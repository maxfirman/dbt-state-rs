//! Identifier acceptance uses the public production router, frozen history,
//! and explicitly opt-in pinned dbt/Databricks physical verification.
mod support;

use dbt_state_proto::query_cache as qc;
use support::legacy::{self, Rows};
use support::scenario::{built_at, Scenario};

#[test]
fn identity_campaign_refuses_prior_objects_and_unleased_names() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_identity_test.py"
        ))
        .output()
        .expect("run credential-free lease boundary checks");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn pre_identifier_history_cold_misses_after_upgrade() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_c6bfa07.json"
    ))
    .unwrap();
    let req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    let old_hash = frozen["rows"][0]["node_sql_hash"].as_str().unwrap();
    assert_eq!(
        old_hash,
        "50c4f6a4586e802fec7d040a7ae2c995581d507ffbe7f80c9d8346061a82ee0a"
    );
    for confirmed_before_upgrade in [true, false] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let id = scenario.execute_id(req.clone()).await;
        if confirmed_before_upgrade {
            assert!(scenario.confirm(&id, built_at(100)).await);
            scenario.expect_skip(req.clone()).await;
        }
        // Preserve the real public build's freshness evidence while restoring
        // independently captured old-server bytes, never today's old-tag hash.
        legacy::rewrite_logic_hashes(&server, Rows::All, Some(old_hash), None).await;
        if !confirmed_before_upgrade {
            assert!(scenario.confirm(&id, built_at(100)).await);
        }
        scenario.expect_execute(req.clone()).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt 2.0.11 and live Databricks; writes exact leased demo names"]
async fn real_dbt_identifiers_match_independent_reference() {
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained identity artifact directory");
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_identity.py"
        ))
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(env!("CARGO_BIN_EXE_record-proxy"))
        .arg("--probe")
        .arg(env!("CARGO_BIN_EXE_state-probe"))
        .arg("--schema-lease")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("explicit lease"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(&artifacts);
    if let Some(identity) = std::env::var_os("DBT_STATE_DATABRICKS_QUERY_IDENTITY") {
        command.arg("--query-identity").arg(identity);
    }
    let output = command.output().await.expect("run identifier acceptance");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn identifier_aliases_share_latest_history_without_crossing_physical_scope() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_c6bfa07.json"
    ))
    .unwrap();
    let original: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    scenario.build(original.clone(), 100).await;
    let mut changed = original.clone();
    changed.target_table = Some("CDO_DEMO_DB.FROZEN_SCHEMA.RESULT".into());
    changed.sql = "select id from cdo_demo_db.frozen_schema.other_input".into();
    changed.tables[1].name = "cdo_demo_db.frozen_schema.other_input".into();
    scenario.build(changed.clone(), 200).await;
    changed.tables[0].last_modified_epoch = Some(200);
    scenario.expect_skip(changed.clone()).await;
    let mut old = original.clone();
    old.tables[0].last_modified_epoch = Some(200);
    scenario.expect_execute(old).await;
    for target in [
        "cdo_demo_db.other_schema.result",
        "other_catalog.frozen_schema.result",
    ] {
        let mut other = changed.clone();
        other.target_table = Some(target.into());
        other.tables[0].name = target.into();
        scenario.expect_execute(other).await;
    }
    let mut namespace = changed.clone();
    namespace.table_namespace = Some("other-connection".into());
    scenario.expect_execute(namespace).await;
    scenario.org("other-org").expect_execute(changed).await;
}

#[tokio::test]
async fn unresolved_identifier_shapes_keep_exact_resolution_context() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_c6bfa07.json"
    ))
    .unwrap();
    let original: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    for sql in [
        "select id from `cdo_demo_db.frozen_schema`.`input`",
        "select id from `cdo_demo_db`.`frozen_schema`.`in``put`",
        "with q as (select id from input) select id from q",
        "select local_function(id) from cdo_demo_db.frozen_schema.input",
        "select (select id from input) from cdo_demo_db.frozen_schema.input",
    ] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let mut req = original.clone();
        req.sql = sql.into();
        scenario.build(req.clone(), 100).await;
        scenario.expect_skip(req.clone()).await;
        req.default_schema = Some("other_schema".into());
        scenario.expect_execute(req).await;
    }
}

#[tokio::test]
async fn upstream_producer_changes_are_resolved_through_identifier_aliases() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_c6bfa07.json"
    ))
    .unwrap();
    let mut consumer: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let mut producer = consumer.clone();
    producer.target_table = Some("cdo_demo_db.frozen_schema.input".into());
    producer.sql = "select id from cdo_demo_db.frozen_schema.seed_input".into();
    producer.tables = vec![qc::TableModifiedInfo {
        name: producer.target_table.clone().unwrap(),
        last_modified_epoch: Some(100),
    }];
    scenario.build(producer.clone(), 100).await;
    consumer.tables[1].last_modified_epoch = Some(100);
    consumer.freshness_tolerance_seconds = i64::MAX;
    scenario.build(consumer.clone(), 200).await;
    consumer.tables[0].last_modified_epoch = Some(200);
    consumer.tables[1].name = "CDO_DEMO_DB.FROZEN_SCHEMA.INPUT".into();
    scenario.expect_skip(consumer.clone()).await;
    producer.tables[0].last_modified_epoch = Some(150);
    scenario.build(producer.clone(), 250).await;
    consumer.tables[1].last_modified_epoch = Some(250);
    scenario.expect_skip(consumer.clone()).await;
    producer.tables[0].last_modified_epoch = Some(250);
    producer.sql = "select id from cdo_demo_db.frozen_schema.changed_seed_input".into();
    scenario.build(producer, 300).await;
    consumer.tables[1].last_modified_epoch = Some(300);
    scenario.expect_execute(consumer).await;
}

#[tokio::test]
async fn recorded_physical_aliases_are_reusable_through_the_same_submit_boundary() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_c6bfa07.json"
    ))
    .unwrap();
    let mut req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let mut record_json = frozen["request"].clone();
    record_json["from_speculative_submit"] = serde_json::json!(false);
    let mut recorded: qc::SqlExecution = serde_json::from_value(record_json).unwrap();
    recorded.target_table = Some("CDO_DEMO_DB.FROZEN_SCHEMA.RESULT".into());
    recorded.tables[0].name = "cdo_demo_db.frozen_schema.result".into();
    recorded.tables[1].name = "CDO_DEMO_DB.FROZEN_SCHEMA.INPUT".into();
    let record = qc::ExecutionRecord {
        input: Some(qc::execution_record::Input::EnrichedSql(recorded)),
        outcome: Some(built_at(100).into()),
    };
    assert_eq!(scenario.record([record]).await.unwrap(), 1);
    req.target_table = Some("cdo_demo_db.frozen_schema.result".into());
    scenario.expect_skip(req).await;
}
