//! Source-less cached failures through the production router. Hosted wire replay
//! lives in conformance.rs; restart/scope controls here are crafted safety cases.
mod support;

use dbt_state_proto::query_cache as qc;
use support::scenario::{into_speculative_skip, Scenario};

fn failing_capture() -> (qc::SubmitEnrichedSqlRequest, qc::ConfirmExecutionRequest) {
    let rows: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_cached_test_results.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let row = rows
        .iter()
        .filter(|row| row["method"] == "SubmitEnrichedSQL")
        .nth(1)
        .unwrap();
    let id = &row["response"]["response"]["ready_to_execute"]["request_id"];
    let confirm = rows
        .iter()
        .find(|row| row["method"] == "ConfirmExecution" && row["request"]["request_id"] == *id)
        .unwrap();
    (
        serde_json::from_value(row["request"].clone()).unwrap(),
        serde_json::from_value(confirm["request"].clone()).unwrap(),
    )
}

#[tokio::test]
async fn complete_failing_result_survives_restart_without_crossing_uid_org_or_namespace() {
    let (request, mut confirm) = failing_capture();
    assert_eq!(
        serde_json::to_value(&confirm.execution_results).unwrap(),
        serde_json::json!({"fields": {
            "failures": {"kind": {"int_value": 3}},
            "should_error": {"kind": {"bool_value": true}},
            "should_warn": {"kind": {"bool_value": true}}
        }})
    );
    let database = std::sync::Arc::new(support::TestDatabase::new("cached_failure").await);
    let first = support::start_server_on_database(database.clone()).await;
    let scenario = Scenario::connect(&first).await;
    scenario.expect_execute(request.clone()).await;
    drop(first);

    let second = support::start_server_on_database(database.clone()).await;
    let scenario = Scenario::connect(&second).await;
    assert!(matches!(
        scenario.speculative(request.clone()).await.response,
        Some(qc::submit_sql_speculative_response::Response::Undecided(_))
    ));
    confirm.request_id = scenario.execute_id(request.clone()).await;
    let mut execution = qc::execution_client::ExecutionClient::new(scenario.channel());
    assert!(
        execution
            .confirm_execution(confirm.clone())
            .await
            .unwrap()
            .into_inner()
            .success
    );
    drop(second);

    let third = support::start_server_on_database(database).await;
    let scenario = Scenario::connect(&third).await;
    for skip in [
        scenario.expect_skip(request.clone()).await,
        into_speculative_skip(scenario.speculative(request.clone()).await),
    ] {
        assert_eq!(skip.execution_results, confirm.execution_results);
        assert_eq!(skip.execution_runtime_ms, confirm.execution_runtime_ms);
    }
    let mut other_uid = request.clone();
    let uid = "test.other_project.literal_outcome";
    other_uid.dbt_node_state.as_mut().unwrap().node_unique_id = uid.into();
    other_uid
        .labels
        .insert("dbt_node_unique_id".into(), uid.into());
    let mut other_namespace = request.clone();
    other_namespace.table_namespace = Some("independent-connection".into());
    for changed in [other_uid, other_namespace] {
        assert!(matches!(
            scenario.speculative(changed.clone()).await.response,
            Some(qc::submit_sql_speculative_response::Response::Undecided(_))
        ));
        scenario.expect_execute(changed).await;
    }
    let other_org = scenario.org("another-org");
    assert!(matches!(
        other_org.speculative(request.clone()).await.response,
        Some(qc::submit_sql_speculative_response::Response::Undecided(_))
    ));
    other_org.expect_execute(request.clone()).await;
    assert_eq!(
        scenario.expect_skip(request).await.execution_results,
        confirm.execution_results
    );
}

#[test]
fn no_object_runner_rejects_typed_result_and_sql_faults() {
    let output = std::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").unwrap_or_else(|| "python3".into()),
    )
    .arg(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../experiments/databricks_cached_tests_test.py"
    ))
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt and Databricks; no warehouse objects or relation data reads"]
async fn real_dbt_cached_failures_match_independent_literal_results() {
    let server = support::start_server().await;
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_cached_tests.py"
        ))
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(env!("CARGO_BIN_EXE_record-proxy"))
        .arg("--probe")
        .arg(env!("CARGO_BIN_EXE_state-probe"))
        .arg("--schema-lease")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("explicit lease"))
        .arg("--contract")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_cached_tests_contract.json"
        ))
        .arg("--query-identity")
        .arg("20261114b143bb")
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh retained artifacts"));
    let output = command.output().await.unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt and Databricks; no warehouse objects or relation data reads"]
async fn real_dbt_cached_configuration_matches_independent_literal_results() {
    let server = support::start_server().await;
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_cached_config.py"
        ))
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(env!("CARGO_BIN_EXE_record-proxy"))
        .arg("--probe")
        .arg(env!("CARGO_BIN_EXE_state-probe"))
        .arg("--schema-lease")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("explicit lease"))
        .arg("--contract")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_CONFIG_CONTRACT").unwrap_or_else(|| {
                concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/../../experiments/databricks_cached_config_contract.json"
                )
                .into()
            }),
        )
        .arg("--through")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_CONFIG_THROUGH")
                .unwrap_or_else(|| "severity_warn".into()),
        )
        .arg("--query-identity")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_CONFIG_QUERY_IDENTITY")
                .unwrap_or_else(|| "20261114c143cc".into()),
        )
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh retained artifacts"));
    let output = command.output().await.unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn configuration_oracle_distinguishes_raw_flags_from_client_classification() {
    let output = std::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").unwrap_or_else(|| "python3".into()),
    )
    .arg(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../experiments/databricks_cached_config_test.py"
    ))
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
