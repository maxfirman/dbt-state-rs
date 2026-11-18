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
//! 2 (dev) (#193); the captured consumer of a volatile model,
//! `golden/fixtures/explain_volatile_evaluation.jsonl` #191 cell 7 (#196).
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

// ── Snowflake consumer of a volatile model (#196) ───────────────────────────
//
// #191 cell 7, `golden/fixtures/explain_volatile_evaluation.jsonl`: `vm` calls
// every Volatile function with evaluation requested, and `vk` is a table over
// `vm` with a Lag tolerance of an hour and evaluation off. A rebuild of `vm`
// with unchanged SQL is a data change for `vk` (ADR 0007): hosted SKIPs `vk`
// within Lag tolerance, so its identity holds no rendered value.

const VOLATILE_CELLS: &str =
    include_str!("../../../golden/fixtures/explain_volatile_evaluation.jsonl");
const VM_BUILD: usize = 6;
const VM_REBUILD: usize = 33;
const VK_BUILD: usize = 15;
const VK_BUILD_EXPLAIN: usize = 25;
const VK_RERUN: usize = 39;
const VK_RERUN_EXPLAIN: usize = 47;
const HOUR_MS: i64 = 3_600_000;

fn volatile_cells() -> Vec<GoldenEntry> {
    VOLATILE_CELLS
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// When the client sent the captured `entry` (`x-submitted-at-epoch`): the
/// earliest time hosted can have decided it. The proxy's `recorded_at` is
/// later, after hosted answered, and moves "last updated: now" past a second.
fn submitted_at(entry: &GoldenEntry) -> i64 {
    entry
        .metadata
        .iter()
        .find(|(key, _)| key == "x-submitted-at-epoch")
        .and_then(|(_, value)| value.parse().ok())
        .unwrap()
}

/// Submit the captured `submit` when the client sent it, expect a build, and
/// confirm it with its captured confirmation when that one was sent.
async fn build_captured(
    sc: &Scenario,
    entries: &[GoldenEntry],
    submit: usize,
) -> qc::ReadyToExecuteResponse {
    let entry = &entries[submit];
    let ready = sc
        .at(submitted_at(entry))
        .expect_execute(captured_submit(&entry.request))
        .await;
    let hosted_id = &entry.response["response"]["ready_to_execute"]["request_id"];
    let confirmation = entries
        .iter()
        .find(|e| e.method == "ConfirmExecution" && &e.request["request_id"] == hosted_id)
        .unwrap();
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(confirmation.request.clone()).unwrap();
    confirm.request_id = ready.request_id.clone();
    assert!(
        sc.at(submitted_at(confirmation))
            .try_confirm(confirm)
            .await
            .unwrap()
            .success
    );
    ready
}

/// Hosted's captured Explain `entry`, for the local decision `id`.
fn hosted_explain(entries: &[GoldenEntry], entry: usize, id: &str) -> serde_json::Value {
    let hosted = &entries[entry].response;
    let hosted_id = hosted["messages"][0]["execution_decision_id"]
        .as_str()
        .unwrap();
    serde_json::from_str(
        &serde_json::to_string(hosted)
            .unwrap()
            .replace(hosted_id, id),
    )
    .unwrap()
}

/// Cell 7's first builds of `vm` and `vk`. Returns `vk`'s.
async fn first_builds(sc: &Scenario, entries: &[GoldenEntry]) -> qc::ReadyToExecuteResponse {
    build_captured(sc, entries, VM_BUILD).await;
    build_captured(sc, entries, VK_BUILD).await
}

/// The captured decision of a submit `entry`, as hosted explained it.
fn hosted_decision(entries: &[GoldenEntry], entry: usize, variant: &str) -> serde_json::Value {
    entries[entry].response["response"][variant]["explained_decision"].clone()
}

/// Cell 7 up to `vk`'s rerun: both first builds, then a rebuild of `vm`.
/// Returns `vk`'s first build.
async fn consumer_history(sc: &Scenario, entries: &[GoldenEntry]) -> qc::ReadyToExecuteResponse {
    let vk = first_builds(sc, entries).await;
    let vm = build_captured(sc, entries, VM_REBUILD).await;
    assert!(
        !vm.transformed_nodes_by_query.is_empty(),
        "the volatile upstream is rebuilt and rendered"
    );
    vk
}

#[tokio::test]
async fn snowflake_consumer_of_a_volatile_model_skips_within_lag_tolerance_after_its_rebuild() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("cell7");
    let entries = volatile_cells();

    let vk = consumer_history(&sc, &entries).await;
    assert_eq!(
        serde_json::to_value(&vk.explained_decision).unwrap(),
        hosted_decision(&entries, VK_BUILD, "ready_to_execute")
    );
    assert!(vk.transformed_nodes_by_query.is_empty());
    let id = vk.execution_decision_id.unwrap();
    assert_eq!(
        explain(&sc, &id).await,
        hosted_explain(&entries, VK_BUILD_EXPLAIN, &id)
    );

    let rerun = &entries[VK_RERUN];
    let skip = sc
        .at(submitted_at(rerun))
        .expect_skip(captured_submit(&rerun.request))
        .await;
    assert_eq!(
        serde_json::to_value(&skip.explained_decision).unwrap(),
        hosted_decision(&entries, VK_RERUN, "skip_execution"),
        "a SKIP within Lag tolerance, reported stale"
    );
    assert!(skip.transformed_nodes_by_query.is_empty());
    assert_eq!(
        skip.execution_runtime_ms,
        rerun.response["response"]["skip_execution"]["execution_runtime_ms"].as_i64()
    );
    let id = skip.execution_decision_id.unwrap();
    assert_eq!(
        explain(&sc, &id).await,
        hosted_explain(&entries, VK_RERUN_EXPLAIN, &id)
    );
}

/// One Explain line of the first message, by its exact text.
fn line<'a>(explain: &'a serde_json::Value, text: &str) -> Option<&'a serde_json::Value> {
    fn find<'a>(lines: &'a serde_json::Value, text: &str) -> Option<&'a serde_json::Value> {
        lines.as_array()?.iter().find_map(|line| {
            (line["text"] == text)
                .then_some(line)
                .or_else(|| find(&line["children"], text))
        })
    }
    find(&explain["messages"][0]["explain_lines"], text)
}

#[tokio::test]
async fn snowflake_consumer_of_a_volatile_model_rebuilds_after_its_rebuild_outside_lag_tolerance() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("cell7-late");
    let entries = volatile_cells();
    consumer_history(&sc, &entries).await;

    // The captured rerun, an hour later than captured: `vk` was built more
    // than its Lag tolerance ago, and `vm` has changed since.
    let rerun = &entries[VK_RERUN];
    let ready = sc
        .at(submitted_at(rerun) + HOUR_MS)
        .expect_execute(captured_submit(&rerun.request))
        .await;
    assert_eq!(
        ready.explained_decision.as_ref().unwrap().decision_description,
        "model was executed because either its query didn't match or its upstream data is out of date"
    );
    assert!(ready.transformed_nodes_by_query.is_empty());
    let explained = explain(&sc, ready.execution_decision_id.as_deref().unwrap()).await;
    assert!(!prints_volatile_lines(&explained), "{explained}");
    for (text, marker, badge) in [
        ("the model query has not changed", Some(1), Some(5)),
        ("upstream model queries have not changed", Some(1), None),
        ("upstream data is outdated", Some(2), None),
    ] {
        let found = line(&explained, text).unwrap_or_else(|| panic!("{text}: {explained}"));
        assert_eq!(
            (found["marker"].as_i64(), found["badge"].as_i64()),
            (marker, badge),
            "{text}"
        );
    }
    // A data-only rebuild with no equivalent elsewhere prints no Lag
    // tolerance line (hosted, `explain_rebuild_tolerance_line.jsonl` entry 3).
    assert!(
        !texts(&explained)
            .iter()
            .any(|t| t.contains("lag tolerance")),
        "{explained}"
    );
}

#[tokio::test]
async fn snowflake_consumer_of_an_unchanged_volatile_model_skips_outside_lag_tolerance() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("cell7-control");
    let entries = volatile_cells();
    first_builds(&sc, &entries).await;

    // Control for the test above: the same late rerun without `vm`'s
    // rebuild, so `vm` reports the epoch `vk` was built over.
    let rerun = &entries[VK_RERUN];
    let vm = entries[VM_BUILD].request["target_table"].as_str().unwrap();
    let vm_built = entries[VK_BUILD].request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .unwrap();
    let skip = sc
        .at(submitted_at(rerun) + HOUR_MS)
        .expect_skip(captured_submit(&rerun.request).upstream(vm, vm_built))
        .await;
    assert_eq!(
        skip.explained_decision.unwrap().decision_description,
        "model was a no-op because both its query and its upstream data are up to date"
    );
}
