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
//! 2 (dev) (#193).
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
/// recorded times. Returns the local response to entry 85.
async fn replay_through_rand_submit(sc: &Scenario) -> qc::ReadyToExecuteResponse {
    let mut responses = replay(sc, &controls(), RAND_SUBMIT).await;
    support::scenario::into_execute(responses.remove(&RAND_SUBMIT).unwrap())
}

/// Replay the submits and confirms of `entries` through entry `through`, at
/// their recorded times, correlating each confirm with its local request id.
/// Returns the local response to each submit, by entry.
async fn replay(
    sc: &Scenario,
    entries: &[GoldenEntry],
    through: usize,
) -> std::collections::HashMap<usize, qc::SubmitSqlResponse> {
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let mut exec = qc::execution_client::ExecutionClient::new(sc.channel());
    let mut local_ids = std::collections::HashMap::new();
    let mut responses = std::collections::HashMap::new();
    for (index, entry) in entries.iter().enumerate().take(through + 1) {
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
                responses.insert(index, response);
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
    responses
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
async fn snowflake_submits_without_a_captured_volatile_call_render_nothing() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let cases = [
        (
            "deterministic control",
            snowflake_sql("id, amount, 1 as loaded_at"),
            false,
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
            "zipf with a constant generator",
            snowflake_sql("id, amount, zipf(1, 10, 1234) as loaded_at"),
            false,
        ),
        (
            "uncaptured seed",
            snowflake_sql("id, amount, randstr(8, 99) as loaded_at"),
            false,
        ),
        (
            "uncaptured normal arguments",
            snowflake_sql("id, amount, normal(0, 1, 7) as loaded_at"),
            false,
        ),
        (
            "uniform with a constant generator",
            snowflake_sql("id, amount, uniform(1, 100, 1234) as loaded_at"),
            false,
        ),
        (
            "uncaptured parenless current_time",
            snowflake_sql("id, amount, current_time as loaded_at"),
            false,
        ),
        (
            "uncaptured parenless localtime",
            snowflake_sql("id, amount, localtime as loaded_at"),
            false,
        ),
        (
            "a column named like a function that needs parentheses",
            snowflake_sql("id, amount, random as loaded_at"),
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
async fn snowflake_decision_recorded_before_the_evaluation_flag_prints_no_volatile_lines() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("legacy flag");
    let id = snowflake_volatile_rebuild(&sc).await;
    let with_flag = explain(&sc, &id).await;

    support::legacy::forget_volatile_evaluation_flag(&server.state, "legacy flag", &id).await;

    let legacy = explain(&sc, &id).await;
    assert!(!prints_volatile_lines(&legacy), "{legacy}");
    let mut expected = texts(&with_flag);
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

// ── Snowflake model cells 1–4 and 8 (#194) ──────────────────────────────────
//
// Evidence: the #191 capture batch, `golden/fixtures/explain_volatile_evaluation.jsonl`
// (run 1: every function in one model, reruns, the deterministic control and
// evaluation off) and `explain_volatile_evaluation_functions.jsonl` (run 2:
// one function form per model). Both were captured with dbt 2.0.11.

const RUN_1: &str = include_str!("../../../golden/fixtures/explain_volatile_evaluation.jsonl");
const RUN_2: &str =
    include_str!("../../../golden/fixtures/explain_volatile_evaluation_functions.jsonl");

fn entries(fixture: &str) -> Vec<GoldenEntry> {
    fixture
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// The map a captured submit's response returned, on whichever branch.
fn hosted_map_of(entry: &GoldenEntry) -> BTreeMap<String, BTreeMap<String, String>> {
    let response = entry.response["response"].as_object().unwrap();
    let payload = response.values().next().unwrap();
    payload["transformed_nodes_by_query"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(query, node)| {
            let functions = serde_json::from_value(node["node_to_func"].clone()).unwrap();
            (query.clone(), functions)
        })
        .collect()
}

/// The map a local response returned, on whichever branch.
fn local_map(response: &qc::SubmitSqlResponse) -> BTreeMap<String, BTreeMap<String, String>> {
    let value = serde_json::to_value(response).unwrap();
    let payload = value["response"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    payload["transformed_nodes_by_query"]
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(query, node)| {
                    let functions = serde_json::from_value(node["node_to_func"].clone()).unwrap();
                    (query.clone(), functions)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Whether `value` is a lowercase version 4 UUID (`8f162c59-9bda-412e-b5de-d0d22af57bf9`).
fn is_lowercase_v4_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|u| u.get_version_num() == 4)
        && value.len() == 36
        && value == value.to_ascii_lowercase()
}

/// Whether `value` has the captured `RANDOM()` format: a plain decimal in
/// [0, 1) (`0.2496468898543448`).
fn is_captured_random_format(value: &str) -> bool {
    is_captured_rand_format(value)
}

#[tokio::test]
async fn snowflake_every_captured_function_renders_hosted_key_and_value_format() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let mut sql = qc::sql_client::SqlClient::new(sc.channel());
    let run_1 = entries(RUN_1);
    let run_2 = entries(RUN_2);
    // Each submit is decided at its recorded time (the replay clock). The
    // clock values are that instant in UTC, in hosted's format per function.
    let clock: &[(&GoldenEntry, &[(&str, &str)])] = &[
        (
            &run_1[6],
            &[
                ("CURRENT_TIMESTAMP()", "2026-11-17 16:48:51.890000"),
                ("SYSDATE()", "2026-11-17 16:48:51.890000+00:00"),
                ("CURRENT_TIME", "16:48:51.890000"),
                ("CURRENT_DATE", "2026-11-17"),
            ],
        ),
        (
            &run_2[13],
            &[("CURRENT_TIMESTAMP()", "2026-11-17 17:05:56.620000")],
        ),
        (
            &run_2[14],
            &[("CURRENT_TIMESTAMP", "2026-11-17 17:05:56.712000")],
        ),
        (&run_2[15], &[("CURRENT_DATE", "2026-11-17")]),
        (&run_2[16], &[]),
        (&run_2[17], &[("CURRENT_TIME", "17:05:56.783000")]),
        (&run_2[18], &[]),
        (
            &run_2[19],
            &[("CURRENT_TIMESTAMP()", "2026-11-17 17:05:56.805000")],
        ),
        (&run_2[20], &[("CURRENT_TIME", "17:05:56.812000")]),
        (&run_2[21], &[]),
        (&run_2[22], &[]),
        (
            &run_2[23],
            &[("CURRENT_TIMESTAMP()", "2026-11-17 17:05:56.855000")],
        ),
        (
            &run_2[24],
            &[("CURRENT_TIMESTAMP()", "2026-11-17 17:05:56.862000")],
        ),
    ];
    for (entry, clock_values) in clock {
        let request = captured_submit(&entry.request).build();
        let sql_text = request.sql.clone();
        let response = sql
            .submit_enriched_sql(support::captured_request(request, entry))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(variant(&response), "ready_to_execute", "{sql_text}");
        let local = local_map(&response);
        let hosted = hosted_map_of(entry);
        assert_eq!(map_shape(&local), map_shape(&hosted), "{sql_text}");
        for (query, functions) in &local {
            for (function, value) in functions {
                let hosted_value = &hosted[query][function];
                match function.as_str() {
                    "RANDOM()" | "UNIFORM(1, 100, RANDOM())" => {
                        assert!(is_captured_random_format(value), "{function}: {value:?}")
                    }
                    "UUID_STRING()" => {
                        assert!(is_lowercase_v4_uuid(value), "{function}: {value:?}");
                        assert!(is_lowercase_v4_uuid(hosted_value));
                    }
                    // Stable across every captured render.
                    "RANDSTR(8, 1234)" | "NORMAL(0, 1, 1234)" => {
                        assert_eq!(value, hosted_value, "{function}")
                    }
                    _ => {
                        let expected = clock_values
                            .iter()
                            .find(|(f, _)| f == function)
                            .unwrap_or_else(|| panic!("{function} is a clock value"));
                        assert_eq!(value, expected.1, "{function}");
                    }
                }
            }
        }
    }
}

/// A decision payload's fields other than its ids and map, which differ per
/// run.
fn codes(payload: &serde_json::Value) -> serde_json::Value {
    let mut payload = payload.clone();
    let object = payload.as_object_mut().unwrap();
    for per_run in [
        "request_id",
        "execution_decision_id",
        "transformed_nodes_by_query",
    ] {
        object.remove(per_run);
    }
    payload
}

/// The local and hosted response to captured submit `entry`: variant, codes
/// and description, and the map's keys and function texts.
fn assert_matches_hosted(local: &qc::SubmitSqlResponse, entry: &GoldenEntry, case: &str) {
    let local_json = serde_json::to_value(local).unwrap();
    let (variant, hosted) = entry.response["response"]
        .as_object()
        .unwrap()
        .iter()
        .next()
        .unwrap();
    assert_eq!(
        support::scenario::variant(local),
        variant,
        "{case}: {local_json}"
    );
    assert_eq!(
        codes(&local_json["response"][variant]),
        codes(hosted),
        "{case}: {local_json}"
    );
    assert_eq!(
        map_shape(&local_map(local)),
        map_shape(&hosted_map_of(entry)),
        "{case}"
    );
}

#[tokio::test]
async fn snowflake_model_cells_return_hosted_codes_and_maps_on_builds_and_reuses() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let run_1 = entries(RUN_1);
    let responses = replay(&sc, &run_1, 50).await;
    for (entry, case) in [
        (5, "cell 3 deterministic control, first build: empty map"),
        (6, "cell 1 every function, first build"),
        (8, "cell 4 evaluation off, first build: rendered"),
        (7, "a view with evaluation off, first build: rendered"),
        (
            31,
            "cell 3 deterministic control rerun with evaluation on: reused",
        ),
        (32, "a view with evaluation off, rerun: reused and rendered"),
        (33, "cell 2 rerun with evaluation on: rebuilt and rendered"),
        (34, "cell 4 evaluation off, rerun: reused and rendered"),
        (
            39,
            "cell 7 consumer within Lag tolerance: reused, empty map",
        ),
        (
            50,
            "cell 4 evaluation switched off over an evaluation-on build: reused and rendered",
        ),
    ] {
        assert_matches_hosted(&responses[&entry], &run_1[entry], case);
    }
    // Reuse is still refused for SQL whose query dependency definition calls
    // a Volatile function with evaluation requested (cell 5 consumer rerun).
    assert_eq!(variant(&responses[&35]), "ready_to_execute");
}

#[tokio::test]
async fn snowflake_evaluation_off_renders_a_rebuild_but_prints_no_uncaptured_explain_lines() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org("evaluation off");
    let (ready, explained) = snowflake_rebuild_with(
        &sc,
        &snowflake_sql("id, amount, current_timestamp() as loaded_at"),
        true,
    )
    .await;

    assert_eq!(
        map_shape(&rendered(&ready)),
        [(snowflake_target(), vec!["CURRENT_TIMESTAMP()".to_owned()])],
        "rendered with evaluation off, as on hosted's evaluation-off builds"
    );
    // Hosted's Explain of a query-changed rebuild with evaluation off is
    // uncaptured: the query analysis prints, without volatile lines.
    assert!(
        texts(&explained).contains(&"the model query has changed".to_owned()),
        "{explained}"
    );
    assert!(!prints_volatile_lines(&explained), "{explained}");
}

/// Hosted's Explain `explain` of captured submit `submit`, for the local
/// decision `local`: hosted's decision id and rendered values replaced by
/// the local ones.
fn hosted_explain_for(
    explain: &GoldenEntry,
    submit: &GoldenEntry,
    local: &qc::SubmitSqlResponse,
) -> serde_json::Value {
    let local_json = serde_json::to_value(local).unwrap();
    let payload = local_json["response"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    let local_id = payload["execution_decision_id"].as_str().unwrap();
    let hosted_payload = submit.response["response"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap();
    let hosted_id = hosted_payload["execution_decision_id"].as_str().unwrap();
    let mut hosted = serde_json::to_string(&explain.response)
        .unwrap()
        .replace(hosted_id, local_id);
    let local_values = local_map(local);
    for (query, functions) in hosted_map_of(submit) {
        for (function, value) in functions {
            let ours = &local_values[&query][&function];
            hosted = hosted.replace(
                &format!("{function} -> rendered with '{value}'"),
                &format!("{function} -> rendered with '{ours}'"),
            );
        }
    }
    serde_json::from_str(&hosted).unwrap()
}

fn decision_id(response: &qc::SubmitSqlResponse) -> String {
    let value = serde_json::to_value(response).unwrap();
    value["response"]
        .as_object()
        .unwrap()
        .values()
        .next()
        .unwrap()["execution_decision_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

#[tokio::test]
async fn snowflake_model_cells_explain_as_hosted_including_after_a_restart() {
    let database = std::sync::Arc::new(support::TestDatabase::new("volatile_cells").await);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    let run_1 = entries(RUN_1);
    let responses = replay(&sc, &run_1, 50).await;
    // (submit, its captured Explain, case)
    let cases = [
        (6, 19, "cell 1 first build: no volatile lines"),
        (
            8,
            20,
            "cell 4 evaluation off, first build: no volatile lines",
        ),
        (5, 21, "cell 3 first build"),
        (7, 22, "a view with evaluation off, first build"),
        (33, 41, "cell 2 rerun with evaluation on"),
        (34, 42, "cell 4 evaluation off, reused"),
        (31, 43, "cell 3 deterministic control, reused"),
        (32, 44, "a view with evaluation off, reused"),
        (50, 51, "cell 4 evaluation switched off, reused"),
    ];
    let expected: Vec<_> = cases
        .iter()
        .map(|(submit, explained, _)| {
            hosted_explain_for(&run_1[*explained], &run_1[*submit], &responses[submit])
        })
        .collect();
    for ((submit, _, case), expected) in cases.iter().zip(&expected) {
        let id = decision_id(&responses[submit]);
        assert_eq!(&explain(&sc, &id).await, expected, "{case}");
    }

    drop(sc);
    drop(server);
    let server = support::start_server_on_database(database.clone()).await;
    let sc = Scenario::connect(&server).await;
    for ((submit, _, case), expected) in cases.iter().zip(&expected) {
        let id = decision_id(&responses[submit]);
        assert_eq!(
            &explain(&sc, &id).await,
            expected,
            "{case}: the persisted values print after a restart"
        );
    }
}
