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
//! (Explain).
mod support;

use std::collections::BTreeMap;

use dbt_state_harness::diff::GoldenEntry;
use dbt_state_proto::query_cache as qc;
use support::scenario::{captured_submit, Scenario};

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

/// Whether `value` has the captured `RAND()` format: a decimal in [0, 1)
/// (`0.2867523899434695`).
fn is_captured_rand_format(value: &str) -> bool {
    let digits = value.strip_prefix("0.").unwrap_or_default();
    !digits.is_empty()
        && digits.len() <= 17
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
    let response = qc::explain_client::ExplainClient::new(sc.channel())
        .get_explain_messages(qc::GetExplainMessagesRequest {
            execution_decision_ids: vec![id.to_owned()],
        })
        .await
        .unwrap()
        .into_inner();
    serde_json::to_value(response).unwrap()
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
