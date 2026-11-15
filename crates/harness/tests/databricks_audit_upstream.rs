//! #144 bounded real-client table/view audits with genuine upstreams.
mod support;

#[test]
fn audit_campaign_guards_exact_outputs_genuine_sources_and_independent_references() {
    let output = std::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").unwrap_or_else(|| "python3".into()),
    )
    .arg(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../experiments/databricks_audit_upstream_test.py"
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

#[tokio::test]
async fn audit_output_never_replaces_genuine_freshness_or_scope() {
    use dbt_state_proto::query_cache as qc;
    use support::scenario::{model, Scenario};
    let rows: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_audit_upstream.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let submits: Vec<_> = rows
        .iter()
        .filter(|r| r["method"] == "SubmitEnrichedSQL")
        .collect();
    let mut request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[2]["request"].clone()).unwrap();
    let first = &submits[0]["response"]["response"]["ready_to_execute"]["request_id"];
    let mut confirm: qc::ConfirmExecutionRequest = serde_json::from_value(
        rows.iter()
            .find(|r| r["method"] == "ConfirmExecution" && r["request"]["request_id"] == *first)
            .unwrap()["request"]
            .clone(),
    )
    .unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let source = request.tables[0].name.clone();
    let epoch = request.tables[0].last_modified_epoch.unwrap();
    let producer = model(&source)
        .dialect("databricks")
        .namespace(request.table_namespace.as_ref().unwrap())
        .sql("SELECT cast(1 AS INT) AS id, cast(2 AS INT) AS v");
    scenario.build(producer.clone(), epoch).await;
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
    assert_eq!(
        scenario
            .expect_skip(request.clone())
            .await
            .execution_results,
        confirm.execution_results
    );
    request.tables[1].last_modified_epoch = Some(epoch + 100_000);
    assert_eq!(
        scenario
            .expect_skip(request.clone())
            .await
            .execution_results,
        confirm.execution_results
    );
    let mut unknown = request.clone();
    unknown.tables[0].last_modified_epoch = None;
    let mut changed = request.clone();
    changed.tables[0].last_modified_epoch = Some(epoch + 1_000);
    let mut renamed = request.clone();
    renamed.tables[0].name = "cdo_demo_db.prod.different_source".into();
    let mut duplicate = request.clone();
    duplicate.tables[1] = duplicate.tables[0].clone();
    let mut namespace = request.clone();
    namespace.table_namespace = Some("other-connection".into());
    let mut uid = request.clone();
    uid.dbt_node_state
        .as_mut()
        .unwrap()
        .node_unique_id
        .push_str("_other");
    uid.labels.insert(
        "dbt_node_unique_id".into(),
        uid.dbt_node_state.as_ref().unwrap().node_unique_id.clone(),
    );
    let mut extra = request.clone();
    extra.tables.push(qc::TableModifiedInfo {
        name: "cdo_demo_db.audit.second".into(),
        last_modified_epoch: Some(epoch),
    });
    for negative in [unknown, changed, renamed, duplicate, namespace, uid, extra] {
        scenario.expect_execute(negative).await;
    }
    scenario
        .org("other-organization")
        .expect_execute(request.clone())
        .await;
    assert_eq!(
        scenario
            .expect_skip(request.clone())
            .await
            .execution_results,
        confirm.execution_results
    );
    scenario
        .build(
            producer.sql("SELECT cast(1 AS INT) AS id, cast(3 AS INT) AS v"),
            epoch,
        )
        .await;
    scenario.expect_execute(request).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt, exact fresh owned source/audit aliases and observed hosted contract"]
async fn real_dbt_audit_table_view_upstream_failure_recovery() {
    let server = support::start_server().await;
    let artifacts = std::path::PathBuf::from(
        std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh retained artifacts"),
    );
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_audit_upstream.py"
        ))
        .arg("--proxy")
        .arg(env!("CARGO_BIN_EXE_record-proxy"))
        .arg("--probe")
        .arg(env!("CARGO_BIN_EXE_state-probe"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(&artifacts);
    for (flag, variable) in [
        ("--dbt", "DBT_STATE_DATABRICKS_DBT"),
        ("--schema-lease", "DBT_STATE_DATABRICKS_SCHEMA_LEASE"),
        ("--audit-receipt", "DBT_STATE_DATABRICKS_AUDIT_RECEIPT"),
        ("--object-id", "DBT_STATE_DATABRICKS_OBJECT_ID"),
        ("--run-id", "DBT_STATE_DATABRICKS_RUN_ID"),
        ("--contract", "DBT_STATE_DATABRICKS_CONTRACT"),
    ] {
        command
            .arg(flag)
            .arg(std::env::var_os(variable).expect(variable));
    }
    let output = command.output().await.unwrap();
    let capture = std::fs::read_dir(artifacts.join("golden"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .expect("whole native capture");
    support::native_clock::retain(&server, &capture, &artifacts.join("native-clocks.json"))
        .await
        .expect("retain exact audit clocks before State cleanup");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
