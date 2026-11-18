//! Volatile evaluation (#67, #192) through the gRPC protocol.
//!
//! A submit that requests Volatile evaluation (`tolerate_nondeterminism =
//! false`, sent by current clients for `evaluate_volatile_sql: true`) has each
//! Volatile function its Warehouse has a captured rule for rendered to a value.
//! The value is returned in `transformed_nodes_by_query` on `ready_to_execute`,
//! and `dbt state explain` prints it from the persisted decision. The Warehouse
//! still executes the original SQL: the value is telemetry and never enters
//! identity or Reuse qualification.
//!
//! Evidence: the captured Databricks build of a `rand()` model,
//! `fixtures/databricks_submit_copy_controls.jsonl` entries 85 (submit) and 86
//! (Explain); the captured Snowflake first builds of a `current_timestamp()`
//! model, `golden/fixtures/clone_fallback_volatile.jsonl` entries 0 (prod) and
//! 2 (dev) (#193); the captured Snowflake table over a volatile view and data
//! test, `golden/fixtures/explain_volatile_evaluation.jsonl` entries 10, 35
//! and 45, and 16, 24, 37 and 46 (#195).
mod support;

use std::collections::BTreeMap;

use dbt_state_harness::diff::GoldenEntry;
use dbt_state_proto::query_cache as qc;
use support::scenario::{built_at, captured_submit, variant, Scenario, OWN_EPOCH};

const CONTROLS: &str = include_str!("fixtures/databricks_submit_copy_controls.jsonl");
/// The captured submit whose SQL calls `rand()` with evaluation requested.
const RAND_SUBMIT: usize = 85;
/// Hosted's Explain of that decision.
const RAND_EXPLAIN: usize = 86;
/// The submit before it: the same model without `rand()`.
const DETERMINISTIC_SUBMIT: usize = 83;

fn controls() -> Vec<GoldenEntry> {
    CONTROLS
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn captured_request(entry: usize) -> serde_json::Value {
    controls()[entry].request.clone()
}

/// The Target relation the captured submit sends.
fn captured_target() -> String {
    captured_submit(&captured_request(RAND_SUBMIT))
        .build()
        .target_table
        .unwrap()
}

/// The epoch the captured submit reports for its own Target relation.
fn captured_target_epoch() -> i64 {
    let req = captured_submit(&captured_request(RAND_SUBMIT)).build();
    req.tables
        .iter()
        .find(|t| Some(&t.name) == req.target_table.as_ref())
        .and_then(|t| t.last_modified_epoch)
        .unwrap()
}

/// Build the deterministic model over the captured Target relation, so a
/// later submit of changed SQL is a query-changed rebuild of an existing table.
async fn deterministic_history(sc: &Scenario) {
    let deterministic = captured_request(DETERMINISTIC_SUBMIT)["sql"]
        .as_str()
        .unwrap()
        .to_owned();
    sc.build(
        captured_submit(&captured_request(RAND_SUBMIT)).sql(&deterministic),
        captured_target_epoch(),
    )
    .await;
}

/// The rendered values a `ready_to_execute` returned, as
/// target → function → value.
fn rendered(ready: &qc::ReadyToExecuteResponse) -> BTreeMap<String, BTreeMap<String, String>> {
    ready
        .transformed_nodes_by_query
        .iter()
        .map(|(query, node)| {
            (
                query.clone(),
                node.node_to_func
                    .iter()
                    .map(|(f, v)| (f.clone(), v.clone()))
                    .collect(),
            )
        })
        .collect()
}

/// Whether `value` has the captured `RAND()` format: a plain decimal in
/// [0, 1), without an exponent (`0.2867523899434695`).
fn is_captured_rand_format(value: &str) -> bool {
    let digits = value.strip_prefix("0.").unwrap_or_default();
    !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
        && value.parse::<f64>().is_ok_and(|v| (0.0..1.0).contains(&v))
}

/// Replay the captured controls through the RAND submit (entry 85) at their
/// recorded times, correlating each confirm with its local request id.
/// Returns the local response to entry 85.
async fn replay_through_rand_submit(sc: &Scenario) -> qc::ReadyToExecuteResponse {
    let entries = controls();
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let mut exec = qc::execution_client::ExecutionClient::new(sc.channel());
    let mut local_ids = std::collections::HashMap::new();
    for entry in &entries[..RAND_SUBMIT] {
        match entry.method.as_str() {
            "SubmitEnrichedSQL" => {
                let req = captured_submit(&entry.request).build();
                let response = sql
                    .submit_enriched_sql(support::captured_request(req, entry))
                    .await
                    .unwrap()
                    .into_inner();
                let hosted = entry.response["response"]["ready_to_execute"]["request_id"]
                    .as_str()
                    .or(entry.response["response"]["ready_to_clone"]["request_id"].as_str());
                if let (Some(hosted), Some(local)) = (hosted, ready_request_id(&response)) {
                    local_ids.insert(hosted.to_owned(), local);
                }
            }
            "ConfirmExecution" => {
                let mut req: qc::ConfirmExecutionRequest =
                    serde_json::from_value(entry.request.clone()).unwrap();
                req.request_id = local_ids[&req.request_id].clone();
                exec.confirm_execution(support::captured_request(req, entry))
                    .await
                    .unwrap();
            }
            _ => {}
        }
    }
    let entry = &entries[RAND_SUBMIT];
    let response = sql
        .submit_enriched_sql(support::captured_request(
            captured_submit(&entry.request).build(),
            entry,
        ))
        .await
        .unwrap()
        .into_inner();
    support::scenario::into_execute(response)
}

fn ready_request_id(response: &qc::SubmitSqlResponse) -> Option<String> {
    let value = serde_json::to_value(response).unwrap();
    ["ready_to_execute", "ready_to_clone"].iter().find_map(|k| {
        value["response"][k]["request_id"]
            .as_str()
            .map(str::to_owned)
    })
}

async fn explain(sc: &Scenario, id: &str) -> serde_json::Value {
    serde_json::to_value(sc.explain(&[id]).await).unwrap()
}

/// Every line of an Explain response, depth first.
fn texts(explain: &serde_json::Value) -> Vec<String> {
    fn walk(lines: &serde_json::Value, out: &mut Vec<String>) {
        for line in lines.as_array().into_iter().flatten() {
            out.push(line["text"].as_str().unwrap().to_owned());
            walk(&line["children"], out);
        }
    }
    let mut out = Vec::new();
    for message in explain["messages"].as_array().unwrap() {
        walk(&message["explain_lines"], &mut out);
    }
    out
}

/// Whether an Explain response prints any Volatile evaluation line.
fn prints_volatile_lines(explain: &serde_json::Value) -> bool {
    texts(explain).iter().any(|t| {
        t.contains("rendered with")
            || t == "non-deterministic functions detected in query"
            || t.contains("is not deterministic")
    })
}

/// Hosted's entry 86 Explain, for the local decision `id` whose `RAND()`
/// rendered `value`.
fn hosted_rand_explain(id: &str, value: &str) -> serde_json::Value {
    let hosted = serde_json::to_string(&controls()[RAND_EXPLAIN].response)
        .unwrap()
        .replace("0.2867523899434695", value)
        .replace("01a1e094-30e7-7588-b0dd-086c25a0df43", id);
    serde_json::from_str(&hosted).unwrap()
}

#[tokio::test]
async fn databricks_rand_build_returns_its_rendered_value_keyed_by_the_target_as_sent() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("rand");
    deterministic_history(&sc).await;

    let ready = sc
        .expect_execute(captured_submit(&captured_request(RAND_SUBMIT)))
        .await;

    let values = rendered(&ready);
    assert_eq!(
        values.keys().collect::<Vec<_>>(),
        [&captured_target()],
        "keyed by the Target relation as sent"
    );
    let functions = &values[&captured_target()];
    assert_eq!(functions.keys().collect::<Vec<_>>(), ["RAND()"]);
    assert!(
        is_captured_rand_format(&functions["RAND()"]),
        "captured float format, got {:?}",
        functions["RAND()"]
    );
}

#[tokio::test]
async fn databricks_rand_explain_matches_hosted_with_the_returned_value_after_a_restart() {
    let database = std::sync::Arc::new(support::TestDatabase::new("volatile_restart").await);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    let ready = replay_through_rand_submit(&sc).await;
    let id = ready.execution_decision_id.clone().unwrap();
    let value = rendered(&ready)[&captured_target()]["RAND()"].clone();

    assert_eq!(explain(&sc, &id).await, hosted_rand_explain(&id, &value));

    drop(sc);
    drop(server);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    assert_eq!(
        explain(&sc, &id).await,
        hosted_rand_explain(&id, &value),
        "the persisted value prints after a restart"
    );
}

/// Submit `sql` (with evaluation requested unless `tolerate`) as a
/// query-changed rebuild of the captured model, in its own org.
async fn rebuild_with(
    sc: &Scenario,
    sql: &str,
    tolerate: bool,
) -> (qc::ReadyToExecuteResponse, serde_json::Value) {
    deterministic_history(sc).await;
    let ready = sc
        .expect_execute(
            captured_submit(&captured_request(RAND_SUBMIT))
                .sql(sql)
                .tolerate_nondeterminism(tolerate),
        )
        .await;
    let explained = explain(sc, ready.execution_decision_id.as_deref().unwrap()).await;
    (ready, explained)
}

#[tokio::test]
async fn submits_without_a_captured_volatile_call_render_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let input = "cdo_demo_db.ds_20261112a134ef_prod.p153_80615b233ac140e48993d85c06bfc823_inputs";
    let cases = [
        (
            "deterministic control",
            format!("select *, 1 AS noise from {input}"),
            false,
        ),
        (
            "evaluation off",
            format!("select *, rand() AS noise from {input}"),
            true,
        ),
        (
            "uncaptured volatile function",
            format!("select *, uuid() AS noise from {input}"),
            false,
        ),
        (
            "uncaptured clock",
            format!("select *, current_timestamp() AS noise from {input}"),
            false,
        ),
        (
            "seeded rand",
            format!("select *, rand(42) AS noise from {input}"),
            false,
        ),
        (
            "line comment",
            format!("select * from {input} -- rand()"),
            false,
        ),
        (
            "block comment",
            format!("select * /* rand() */ from {input}"),
            false,
        ),
        (
            "string literal",
            format!("select *, 'rand()' AS noise from {input}"),
            false,
        ),
        (
            "quoted identifier",
            format!("select *, `rand`() AS noise from {input}"),
            false,
        ),
    ];
    // Positive control for the same harness: the captured call renders.
    let (ready, explained) = rebuild_with(
        &sc.org("control"),
        &format!("select *, rand() AS noise from {input}"),
        false,
    )
    .await;
    assert!(!ready.transformed_nodes_by_query.is_empty());
    assert!(prints_volatile_lines(&explained));
    for (case, sql, tolerate) in cases {
        let (ready, explained) = rebuild_with(&sc.org(case), &sql, tolerate).await;
        assert!(
            ready.transformed_nodes_by_query.is_empty(),
            "{case}: {:?}",
            ready.transformed_nodes_by_query
        );
        assert!(
            texts(&explained).contains(&"the model query has changed".to_owned()),
            "{case}: the query analysis is printed"
        );
        assert!(!prints_volatile_lines(&explained), "{case}: {explained}");
    }
}

#[tokio::test]
async fn another_organizations_decision_renders_no_values() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let owner = sc.org("owner");
    deterministic_history(&owner).await;
    let ready = owner
        .expect_execute(captured_submit(&captured_request(RAND_SUBMIT)))
        .await;
    let id = ready.execution_decision_id.unwrap();
    assert!(prints_volatile_lines(&explain(&owner, &id).await));

    let other = explain(&sc.org("other"), &id).await;
    assert!(other["messages"].as_array().unwrap().is_empty(), "{other}");
    assert!(!prints_volatile_lines(&other));
}

#[tokio::test]
async fn a_decision_recorded_before_the_migration_prints_no_volatile_lines() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("legacy");
    deterministic_history(&sc).await;
    let ready = sc
        .expect_execute(captured_submit(&captured_request(RAND_SUBMIT)))
        .await;
    let id = ready.execution_decision_id.unwrap();
    let with_values = explain(&sc, &id).await;
    assert!(prints_volatile_lines(&with_values));

    support::legacy::forget_rendered_volatile_values(&server.state, "legacy", &id).await;

    let legacy = explain(&sc, &id).await;
    assert!(!prints_volatile_lines(&legacy), "{legacy}");
    let mut expected = texts(&with_values);
    expected.retain(|t| {
        !t.contains("rendered with")
            && t != "non-deterministic functions detected in query"
            && !t.contains("is not deterministic")
    });
    assert_eq!(
        texts(&legacy),
        expected,
        "the rest of the tree is unchanged"
    );
}

/// A persisted execution with the columns that differ per request removed.
async fn fingerprints(server: &support::TestServer, request_id: &str) -> serde_json::Value {
    let mut row = support::rows::complete_execution(&server.state, request_id).await;
    let object = row.as_object_mut().unwrap();
    for per_request in [
        "id",
        "request_id",
        "execution_decision_id",
        "created_at",
        "display_submitted_at_ms",
    ] {
        object.remove(per_request);
    }
    row
}

#[tokio::test]
async fn a_volatile_model_is_never_reused_or_copied_and_its_values_enter_no_fingerprint() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("volatile");
    let model = captured_submit(&captured_request(RAND_SUBMIT));

    let first = sc.expect_execute(model.clone()).await;
    let second = sc.expect_execute(model.clone()).await;
    let value =
        |ready: &qc::ReadyToExecuteResponse| rendered(ready)[&captured_target()]["RAND()"].clone();
    assert_ne!(value(&first), value(&second), "each decision renders anew");
    assert_eq!(
        fingerprints(&server, &first.request_id).await,
        fingerprints(&server, &second.request_id).await,
        "rendered values enter no persisted identity or fingerprint"
    );

    assert!(
        sc.confirm(&first.request_id, built_at(captured_target_epoch()))
            .await
    );
    let rerun = sc.expect_execute(model.clone()).await;
    assert!(
        !rendered(&rerun).is_empty(),
        "a rerun is rebuilt and rendered"
    );

    let elsewhere = model
        .clone()
        .target("`cdo_demo_db`.`elsewhere`.`p153_80615b233ac140e48993d85c06bfc823_full`")
        .schema("elsewhere")
        .no_tables()
        .table(
            "`cdo_demo_db`.`ds_20261112a134ef_prod`.`p153_80615b233ac140e48993d85c06bfc823_inputs`",
            Some(1794768588000),
        );
    assert_eq!(
        variant(&sc.submit(elsewhere).await),
        "ready_to_execute",
        "never copied into another schema of the namespace"
    );

    // Control: the same history reuses the model when evaluation is off.
    let deterministic = sc.org("deterministic");
    let model = model.tolerate_nondeterminism(true);
    deterministic
        .build(model.clone(), captured_target_epoch())
        .await;
    assert_eq!(
        variant(&deterministic.submit(model).await),
        "skip_execution"
    );
}

// ── Snowflake CURRENT_TIMESTAMP() (#193) ────────────────────────────────────

const SNOWFLAKE_FIRST_BUILDS: &str =
    include_str!("../../../golden/fixtures/clone_fallback_volatile.jsonl");
/// The captured prod and dev first builds of a model calling
/// `current_timestamp()` with evaluation requested.
const SNOWFLAKE_PROD_SUBMIT: usize = 0;
const SNOWFLAKE_DEV_SUBMIT: usize = 2;

fn snowflake_first_builds() -> Vec<GoldenEntry> {
    SNOWFLAKE_FIRST_BUILDS
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// A map as target → function texts, without the values.
fn map_shape(map: &BTreeMap<String, BTreeMap<String, String>>) -> Vec<(String, Vec<String>)> {
    map.iter()
        .map(|(query, functions)| (query.clone(), functions.keys().cloned().collect()))
        .collect()
}

fn hosted_map(entry: &GoldenEntry) -> BTreeMap<String, BTreeMap<String, String>> {
    entry.response["response"]["ready_to_execute"]["transformed_nodes_by_query"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(query, node)| {
            let functions = serde_json::from_value(node["node_to_func"].clone()).unwrap();
            (query.clone(), functions)
        })
        .collect()
}

#[tokio::test]
async fn snowflake_current_timestamp_build_renders_the_replay_clock_in_utc_keyed_as_captured() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let entries = snowflake_first_builds();
    // Each submit's recorded time (the replay clock), in UTC.
    for (entry, at) in [
        (SNOWFLAKE_PROD_SUBMIT, "2026-10-12 21:52:38.707000"),
        (SNOWFLAKE_DEV_SUBMIT, "2026-10-12 21:54:20.470000"),
    ] {
        let entry = &entries[entry];
        let response = sql
            .submit_enriched_sql(support::captured_request(
                captured_submit(&entry.request).build(),
                entry,
            ))
            .await
            .unwrap()
            .into_inner();
        let values = rendered(&support::scenario::into_execute(response));
        let hosted = hosted_map(entry);

        assert_eq!(map_shape(&values), map_shape(&hosted), "hosted's key shape");
        let target = entry.request["target_table"].as_str().unwrap();
        assert_eq!(values[target]["CURRENT_TIMESTAMP()"], at);
    }
}

/// The captured prod submit's request.
fn snowflake_request() -> serde_json::Value {
    snowflake_first_builds()[SNOWFLAKE_PROD_SUBMIT]
        .request
        .clone()
}

fn snowflake_target() -> String {
    snowflake_request()["target_table"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// The captured model's SQL with `columns` in place of its column list.
fn snowflake_sql(columns: &str) -> String {
    format!("\nselect {columns} from CDO_DEMO_DB.prod_state_demo.ce_up_215158")
}

/// The epoch the captured build confirmed for its Target relation (entry 1).
const SNOWFLAKE_BUILT_AT: i64 = 1791841959783;

/// Build a deterministic version of the captured model over its Target
/// relation, so a later submit of other SQL is a query-changed rebuild.
async fn snowflake_history(sc: &Scenario) {
    sc.build(
        captured_submit(&snowflake_request())
            .sql(&snowflake_sql("id, amount"))
            .own_epoch(Some(OWN_EPOCH)),
        SNOWFLAKE_BUILT_AT,
    )
    .await;
}

/// Submit `sql` (with evaluation requested unless `tolerate`) as a
/// query-changed rebuild of the captured Snowflake model.
async fn snowflake_rebuild_with(
    sc: &Scenario,
    sql: &str,
    tolerate: bool,
) -> (qc::ReadyToExecuteResponse, serde_json::Value) {
    snowflake_history(sc).await;
    let ready = sc
        .expect_execute(
            captured_submit(&snowflake_request())
                .sql(sql)
                .own_epoch(Some(OWN_EPOCH))
                .tolerate_nondeterminism(tolerate),
        )
        .await;
    let explained = explain(sc, ready.execution_decision_id.as_deref().unwrap()).await;
    (ready, explained)
}

#[tokio::test]
async fn snowflake_submits_without_a_captured_current_timestamp_call_render_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let cases = [
        (
            "deterministic control",
            snowflake_sql("id, amount, 1 as loaded_at"),
            false,
        ),
        (
            "evaluation off",
            snowflake_sql("id, amount, current_timestamp() as loaded_at"),
            true,
        ),
        (
            "uncaptured precision argument",
            snowflake_sql("id, amount, current_timestamp(3) as loaded_at"),
            false,
        ),
        (
            "a function hosted renders nothing for",
            snowflake_sql("id, amount, seq4() as loaded_at"),
            false,
        ),
        (
            "line comment",
            snowflake_sql("id, amount, 1 as loaded_at") + " -- current_timestamp()",
            false,
        ),
        (
            "block comment",
            snowflake_sql("id, amount, 1 /* current_timestamp() */ as loaded_at"),
            false,
        ),
        (
            "string literal",
            snowflake_sql("id, amount, 'current_timestamp()' as loaded_at"),
            false,
        ),
        (
            "quoted identifier",
            snowflake_sql("id, amount, \"current_timestamp\"() as loaded_at"),
            false,
        ),
        (
            "qualified user function",
            snowflake_sql("id, amount, analytics.current_timestamp() as loaded_at"),
            false,
        ),
    ];
    // Positive control for the same harness: the captured call renders.
    let (ready, explained) = snowflake_rebuild_with(
        &sc.org("control"),
        &snowflake_sql("id, amount, current_timestamp() as loaded_at"),
        false,
    )
    .await;
    assert!(!ready.transformed_nodes_by_query.is_empty());
    assert!(prints_volatile_lines(&explained));
    for (case, sql, tolerate) in cases {
        let (ready, explained) = snowflake_rebuild_with(&sc.org(case), &sql, tolerate).await;
        assert!(
            ready.transformed_nodes_by_query.is_empty(),
            "{case}: {:?}",
            ready.transformed_nodes_by_query
        );
        assert!(
            texts(&explained).contains(&"the model query has changed".to_owned()),
            "{case}: the query analysis is printed"
        );
        assert!(!prints_volatile_lines(&explained), "{case}: {explained}");
    }
}

/// A query-changed rebuild of the captured Snowflake model calling
/// `current_timestamp()`; returns its decision id.
async fn snowflake_volatile_rebuild(sc: &Scenario) -> String {
    let (ready, explained) = snowflake_rebuild_with(
        sc,
        &snowflake_sql("id, amount, current_timestamp() as loaded_at"),
        false,
    )
    .await;
    assert!(prints_volatile_lines(&explained));
    ready.execution_decision_id.unwrap()
}

#[tokio::test]
async fn snowflake_another_organizations_decision_renders_no_values() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let id = snowflake_volatile_rebuild(&sc.org("owner")).await;

    let other = explain(&sc.org("other"), &id).await;
    assert!(other["messages"].as_array().unwrap().is_empty(), "{other}");
}

#[tokio::test]
async fn snowflake_decision_recorded_before_the_migration_prints_no_volatile_lines() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("legacy");
    let id = snowflake_volatile_rebuild(&sc).await;
    let with_values = explain(&sc, &id).await;

    support::legacy::forget_rendered_volatile_values(&server.state, "legacy", &id).await;

    let legacy = explain(&sc, &id).await;
    assert!(!prints_volatile_lines(&legacy), "{legacy}");
    let mut expected = texts(&with_values);
    expected.retain(|t| {
        !t.contains("rendered with")
            && t != "non-deterministic functions detected in query"
            && !t.contains("is not deterministic")
    });
    assert_eq!(
        texts(&legacy),
        expected,
        "the rest of the tree is unchanged"
    );
}

#[tokio::test]
async fn snowflake_volatile_model_is_never_reused_or_copied_and_its_values_enter_no_fingerprint() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("volatile");
    let entries = snowflake_first_builds();
    let model = captured_submit(&snowflake_request()).own_epoch(Some(OWN_EPOCH));
    let value = |ready: &qc::ReadyToExecuteResponse| {
        rendered(ready)[&snowflake_target()]["CURRENT_TIMESTAMP()"].clone()
    };

    let first = sc.expect_execute(model.clone()).await;
    let second = sc.expect_execute(model.clone()).await;
    assert!(!value(&first).is_empty() && !value(&second).is_empty());
    assert_eq!(
        fingerprints(&server, &first.request_id).await,
        fingerprints(&server, &second.request_id).await,
        "rendered values enter no persisted identity or fingerprint"
    );

    assert!(
        sc.confirm(&first.request_id, built_at(SNOWFLAKE_BUILT_AT))
            .await
    );
    let rerun = sc.expect_execute(model.clone()).await;
    assert!(!value(&rerun).is_empty(), "a rerun is rebuilt and rendered");

    // The captured dev build of the same model, in another schema of the
    // namespace: hosted built it, and so does this server.
    assert_eq!(
        variant(
            &sc.submit(captured_submit(&entries[SNOWFLAKE_DEV_SUBMIT].request))
                .await
        ),
        "ready_to_execute",
        "never copied into another schema of the namespace"
    );

    // Control: the same history reuses the model when evaluation is off.
    let deterministic = sc.org("deterministic");
    let model = model.tolerate_nondeterminism(true);
    deterministic.build(model.clone(), SNOWFLAKE_BUILT_AT).await;
    assert_eq!(
        variant(&deterministic.submit(model).await),
        "skip_execution"
    );
}

// ── Snowflake volatile view dependency and data test (#195) ─────────────────

const SNOWFLAKE_EVALUATION: &str =
    include_str!("../../../golden/fixtures/explain_volatile_evaluation.jsonl");
/// `vd`, a table over the view `vv` that calls `current_timestamp()`, with
/// evaluation requested: first build and unchanged rerun.
const VD_FIRST_BUILD: usize = 10;
const VD_RERUN: usize = 35;
/// Hosted's Explain of the `vd` rerun.
const VD_RERUN_EXPLAIN: usize = 45;
/// `vt`, a data test calling `current_timestamp()` with evaluation
/// requested: first execution and unchanged rerun, and their Explains.
const VT_FIRST: usize = 16;
const VT_RERUN: usize = 37;
const VT_FIRST_EXPLAIN: usize = 24;
const VT_RERUN_EXPLAIN: usize = 46;

fn snowflake_evaluation() -> Vec<GoldenEntry> {
    SNOWFLAKE_EVALUATION
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn snowflake_volatile_data_test_and_dependency_render_keyed_as_captured() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = snowflake_evaluation();
    for (case, entry) in [("data test", VT_FIRST), ("view dependency", VD_FIRST_BUILD)] {
        let entry = &entries[entry];
        let ready = sc
            .org(case)
            .expect_execute(captured_submit(&entry.request))
            .await;
        let values = rendered(&ready);
        assert_eq!(
            map_shape(&values),
            map_shape(&hosted_map(entry)),
            "{case}: hosted's key shape"
        );
    }
}

/// Replay the captured run through `through` (inclusive) at the recorded
/// times, correlating each confirm with its local request id. Returns each
/// local `ready_to_execute` by its submit's entry index.
async fn replay_snowflake_evaluation(
    sc: &Scenario,
    through: usize,
) -> BTreeMap<usize, qc::ReadyToExecuteResponse> {
    let entries = snowflake_evaluation();
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let mut exec = qc::execution_client::ExecutionClient::new(sc.channel());
    let mut local_ids = std::collections::HashMap::new();
    let mut executed = BTreeMap::new();
    for (index, entry) in entries[..=through].iter().enumerate() {
        match entry.method.as_str() {
            "SubmitEnrichedSQL" => {
                let response = sql
                    .submit_enriched_sql(support::captured_request(
                        captured_submit(&entry.request).build(),
                        entry,
                    ))
                    .await
                    .unwrap()
                    .into_inner();
                let hosted = entry.response["response"]["ready_to_execute"]["request_id"].as_str();
                if let (Some(hosted), Some(local)) = (hosted, ready_request_id(&response)) {
                    local_ids.insert(hosted.to_owned(), local);
                }
                if variant(&response) == "ready_to_execute" {
                    executed.insert(index, support::scenario::into_execute(response));
                }
            }
            "ConfirmExecution" => {
                let mut req: qc::ConfirmExecutionRequest =
                    serde_json::from_value(entry.request.clone()).unwrap();
                req.request_id = local_ids[&req.request_id].clone();
                exec.confirm_execution(support::captured_request(req, entry))
                    .await
                    .unwrap();
            }
            _ => {}
        }
    }
    executed
}

/// Hosted's Explain `explain` of the captured submit `submit`, for the local
/// decision `local` and the values it rendered.
fn hosted_explain_of(
    entries: &[GoldenEntry],
    submit: usize,
    explain: usize,
    local: &qc::ReadyToExecuteResponse,
) -> serde_json::Value {
    let hosted_id = entries[submit].response["response"]["ready_to_execute"]
        ["execution_decision_id"]
        .as_str()
        .unwrap();
    let mut text = serde_json::to_string(&entries[explain].response)
        .unwrap()
        .replace(hosted_id, local.execution_decision_id.as_deref().unwrap());
    let values = rendered(local);
    for (query, functions) in hosted_map(&entries[submit]) {
        for (function, value) in functions {
            text = text.replace(
                &format!("{function} -> rendered with '{value}'"),
                &format!(
                    "{function} -> rendered with '{}'",
                    values[&query][&function]
                ),
            );
        }
    }
    masked_relative_times(serde_json::from_str(&text).unwrap())
}

/// `explain` with each `last updated: …` line masked. Outside Databricks a
/// data test's upstream listing prints the raw epoch, not hosted's relative
/// time; the conformance replay masks the same slot (`«rel_time»`).
fn masked_relative_times(mut explain: serde_json::Value) -> serde_json::Value {
    fn walk(lines: &mut serde_json::Value) {
        for line in lines.as_array_mut().into_iter().flatten() {
            if line["text"]
                .as_str()
                .is_some_and(|t| t.starts_with("last updated:"))
            {
                line["text"] = "last updated: «rel_time»".into();
            }
            walk(&mut line["children"]);
        }
    }
    for message in explain["messages"].as_array_mut().unwrap() {
        walk(&mut message["explain_lines"]);
    }
    explain
}

/// Replay the captured run through the last of `cases` (submit entry,
/// hosted Explain entry), then require each local Explain to equal hosted's
/// for the values the local decision rendered, before and after a restart.
async fn assert_explains_match_hosted_after_a_restart(database: &str, cases: &[(usize, usize)]) {
    let database = std::sync::Arc::new(support::TestDatabase::new(database).await);
    let server = support::start_server_on_database(database.clone()).await;
    let entries = snowflake_evaluation();
    let through = cases.iter().map(|&(submit, _)| submit).max().unwrap();
    let executed = replay_snowflake_evaluation(&Scenario::connect(&server).await, through).await;
    let expected: Vec<_> = cases
        .iter()
        .map(|&(submit, explain)| hosted_explain_of(&entries, submit, explain, &executed[&submit]))
        .collect();
    let explained = |server: support::TestServer| {
        let executed = &executed;
        async move {
            let sc = Scenario::connect(&server).await;
            let mut out = Vec::new();
            for &(submit, _) in cases {
                let id = executed[&submit].execution_decision_id.as_deref().unwrap();
                out.push(masked_relative_times(explain(&sc, id).await));
            }
            out
        }
    };

    assert_eq!(explained(server).await, expected);

    let server = support::start_server_on_database(database.clone()).await;
    assert_eq!(
        explained(server).await,
        expected,
        "the persisted values print after a restart"
    );
}

#[tokio::test]
async fn snowflake_volatile_data_test_explain_matches_hosted_on_both_runs_after_a_restart() {
    assert_explains_match_hosted_after_a_restart(
        "volatile_test_restart",
        &[(VT_FIRST, VT_FIRST_EXPLAIN), (VT_RERUN, VT_RERUN_EXPLAIN)],
    )
    .await;
}

#[tokio::test]
async fn snowflake_volatile_dependency_explain_matches_hosted_after_a_restart() {
    assert_explains_match_hosted_after_a_restart(
        "volatile_dependency_restart",
        &[(VD_RERUN, VD_RERUN_EXPLAIN)],
    )
    .await;
}

#[tokio::test]
async fn snowflake_volatile_dependency_and_data_test_are_never_reused_or_copied() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = snowflake_evaluation();
    let request = |entry: usize| captured_submit(&entries[entry].request);
    let vv = entries[VD_FIRST_BUILD].request["query_dependencies"][0]["name"]
        .as_str()
        .unwrap()
        .to_owned();
    let vd_built_at = entries[VD_RERUN].request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .unwrap();
    // The captured confirm of the first `vt` execution: passed, 0 failures.
    let vt_results: qc::Struct =
        serde_json::from_value(entries[VT_FIRST + 1].request["execution_results"].clone()).unwrap();
    let vt_passed = || support::scenario::outcome().results(vt_results.clone());
    // `vd` built in the other schema of its namespace over that schema's
    // `vv`, as a dev Target.
    let vd_elsewhere = |tolerate: bool| {
        let dev = serde_json::to_string(&entries[VD_FIRST_BUILD].request)
            .unwrap()
            .replace("PROD_STATE_DEMO", "DEV_STATE_DEMO")
            .replace("prod_state_demo", "dev_state_demo");
        captured_submit(&serde_json::from_str(&dev).unwrap()).tolerate_nondeterminism(tolerate)
    };

    let pending = sc.org("pending");
    let first = pending.expect_execute(request(VD_FIRST_BUILD)).await;
    let second = pending.expect_execute(request(VD_FIRST_BUILD)).await;
    assert!(!rendered(&first).is_empty() && !rendered(&second).is_empty());
    assert_eq!(
        fingerprints(&server, &first.request_id).await,
        fingerprints(&server, &second.request_id).await,
        "rendered values enter no persisted identity or fingerprint"
    );

    let volatile = sc.org("volatile");
    volatile.build(request(VD_FIRST_BUILD), vd_built_at).await;
    let rerun = volatile.expect_execute(request(VD_RERUN)).await;
    assert_eq!(
        map_shape(&rendered(&rerun)),
        [(vv.clone(), vec!["CURRENT_TIMESTAMP()".to_owned()])],
        "the unchanged table over the volatile view is rebuilt and rendered"
    );
    assert!(
        volatile
            .confirm(&rerun.request_id, built_at(vd_built_at))
            .await
    );
    assert_eq!(
        variant(&volatile.submit(vd_elsewhere(false)).await),
        "ready_to_execute",
        "never copied into another schema of the namespace"
    );
    let test = volatile.expect_execute(request(VT_FIRST)).await;
    assert!(volatile.confirm(&test.request_id, vt_passed()).await);
    let test_rerun = volatile.expect_execute(request(VT_RERUN)).await;
    assert_eq!(
        map_shape(&rendered(&test_rerun)),
        [(
            "__target_query__".to_owned(),
            vec!["CURRENT_TIMESTAMP()".to_owned()]
        )],
        "the unchanged data test is executed again and rendered"
    );

    // Control: the same history is reused, and `vd` copied, with evaluation off.
    let tolerated = sc.org("tolerated");
    tolerated
        .build(
            request(VD_FIRST_BUILD).tolerate_nondeterminism(true),
            vd_built_at,
        )
        .await;
    assert_eq!(
        variant(
            &tolerated
                .submit(request(VD_RERUN).tolerate_nondeterminism(true))
                .await
        ),
        "skip_execution"
    );
    assert_eq!(
        variant(&tolerated.submit(vd_elsewhere(true)).await),
        "ready_to_clone"
    );
    tolerated
        .build_with(request(VT_FIRST).tolerate_nondeterminism(true), vt_passed())
        .await;
    assert_eq!(
        variant(
            &tolerated
                .submit(request(VT_RERUN).tolerate_nondeterminism(true))
                .await
        ),
        "skip_execution"
    );
}

#[tokio::test]
async fn dependency_and_data_test_sql_without_a_captured_call_renders_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let entries = snowflake_evaluation();
    let vv_columns = "1 as id, current_timestamp() as ts, 'vv_164840c243b8' as v";
    let vt_condition = "current_timestamp() < '2000-01-01'::timestamp_ltz";
    let cases = [
        (
            "line comment",
            "1 as id, 1 as ts -- current_timestamp()",
            "1 = 1 -- current_timestamp()",
        ),
        (
            "block comment",
            "1 as id, 1 /* current_timestamp() */ as ts",
            "1 /* current_timestamp() */ = 1",
        ),
        (
            "string literal",
            "1 as id, 'current_timestamp()' as ts",
            "'current_timestamp()' = v",
        ),
        (
            "quoted identifier",
            "1 as id, \"current_timestamp\"() as ts",
            "\"current_timestamp\"() < '2000-01-01'::timestamp_ltz",
        ),
    ];
    let with = |entry: usize, find: &str, replace: &str| {
        let (find, replace) = (find.to_owned(), replace.to_owned());
        captured_submit(&entries[entry].request).with(move |r| {
            r.sql = r.sql.replace(&find, &replace);
            for d in &mut r.query_dependencies {
                d.query = d.query.replace(&find, &replace);
            }
        })
    };
    for (case, dependency, condition) in cases {
        for (node, submit) in [
            (
                "view dependency",
                with(VD_FIRST_BUILD, vv_columns, dependency),
            ),
            ("data test", with(VT_FIRST, vt_condition, condition)),
        ] {
            let ready = sc
                .org(&format!("{case} {node}"))
                .expect_execute(submit)
                .await;
            assert!(
                rendered(&ready).is_empty(),
                "{case} {node}: {:?}",
                rendered(&ready)
            );
        }
    }
    // Positive control for the same edits: the captured calls render.
    for (node, submit) in [
        (
            "view dependency",
            with(VD_FIRST_BUILD, vv_columns, vv_columns),
        ),
        ("data test", with(VT_FIRST, vt_condition, vt_condition)),
    ] {
        let ready = sc
            .org(&format!("control {node}"))
            .expect_execute(submit)
            .await;
        assert!(!rendered(&ready).is_empty(), "{node}");
    }

    // Databricks has no capture of a dependency or a data test: only a Target
    // relation's own `rand()` renders.
    let model = captured_submit(&captured_request(RAND_SUBMIT));
    let databricks_dependency = model
        .clone()
        .sql(&format!(
            "select * from {}",
            "`cdo_demo_db`.`ds_20261112a134ef_prod`.`v`"
        ))
        .dependency(
            "`cdo_demo_db`.`ds_20261112a134ef_prod`.`v`",
            "select rand() as noise",
        );
    let databricks_test = support::scenario::data_test("test.databricks.rand")
        .dialect("databricks")
        .sql("select rand() as noise where false")
        .tolerate_nondeterminism(false);
    for (node, submit) in [
        ("Databricks dependency", databricks_dependency),
        ("Databricks data test", databricks_test),
    ] {
        let ready = sc.org(node).expect_execute(submit).await;
        assert!(
            rendered(&ready).is_empty(),
            "{node}: {:?}",
            rendered(&ready)
        );
    }
}

#[tokio::test]
async fn a_volatile_data_test_logic_change_is_uncaptured_and_prints_no_volatile_lines() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("logic change");
    let entries = snowflake_evaluation();
    let vt_results: qc::Struct =
        serde_json::from_value(entries[VT_FIRST + 1].request["execution_results"].clone()).unwrap();
    sc.build_with(
        captured_submit(&entries[VT_FIRST].request),
        support::scenario::outcome().results(vt_results),
    )
    .await;

    let changed = sc
        .expect_execute(
            captured_submit(&entries[VT_RERUN].request)
                .with(|r| r.sql = r.sql.replace("id > 100", "id > 200")),
        )
        .await;
    assert!(!rendered(&changed).is_empty(), "rendered and returned");
    let explained = explain(&sc, changed.execution_decision_id.as_deref().unwrap()).await;
    assert!(!prints_volatile_lines(&explained), "{explained}");
    assert!(
        texts(&explained).contains(&"data freshness analysis".to_owned()),
        "the captured logic-change tree: {explained}"
    );
}
