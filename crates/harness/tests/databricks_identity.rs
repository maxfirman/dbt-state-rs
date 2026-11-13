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
    for (bytes, expected_hash) in [
        (
            include_str!("../../../golden/upgrade/databricks_exact_c6bfa07.json"),
            "50c4f6a4586e802fec7d040a7ae2c995581d507ffbe7f80c9d8346061a82ee0a",
        ),
        // This unsupported literal query changes neither SQL nor context:
        // its old hash cold-misses only when the identity version advances.
        (
            include_str!("../../../golden/upgrade/databricks_exact_literal_c6bfa07.json"),
            "a20825eb672c207928f80be6588b5e5ac2924feeec749a6eabf0315b30b1ac62",
        ),
    ] {
        let frozen: serde_json::Value = serde_json::from_str(bytes).unwrap();
        let req: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(frozen["request"].clone()).unwrap();
        let old_hash = frozen["rows"][0]["node_sql_hash"].as_str().unwrap();
        assert_eq!(old_hash, expected_hash);
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

#[tokio::test]
async fn latest_frozen_alias_history_blocks_older_eligible_canonical_rows_at_equal_epochs() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_literal_c6bfa07.json"
    ))
    .unwrap();
    let req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    for pending_before_upgrade in [false, true] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let old_id = uuid::Uuid::new_v4().to_string();
        let row = legacy::FrozenExecution {
            org_id: "local",
            target_table: "CDO_DEMO_DB.FROZEN_SCHEMA.RESULT",
            execution_type: 1,
            dialect: "databricks",
            status: if pending_before_upgrade {
                "pending"
            } else {
                "confirmed"
            },
            request_id: &old_id,
            node_sql_hash: frozen["rows"][0]["node_sql_hash"].as_str(),
            node_template_hash: None,
            node_namespace_logic_hash: None,
            clone_source_table: None,
            table_namespace: Some("frozen-databricks-136"),
            last_modified_epoch: Some(100),
        };
        if pending_before_upgrade {
            legacy::replace_with_frozen_execution(&server, &row).await;
        }
        scenario.build(req.clone(), 100).await;
        scenario.expect_skip(req.clone()).await;
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        if pending_before_upgrade {
            assert!(scenario.confirm(&old_id, built_at(100)).await);
        } else {
            legacy::replace_with_frozen_execution(&server, &row).await;
        }
        let response = scenario.expect_execute(req.clone()).await;
        assert_eq!(
            response.explained_decision.unwrap().skip_rejection_reason,
            Some(8),
            "latest old alias must block reuse while reporting the older eligible canonical build"
        );
    }
}

#[tokio::test]
async fn frozen_alias_scope_and_producer_lookup_preserve_isolation_and_detect_old_publication() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_literal_c6bfa07.json"
    ))
    .unwrap();
    let req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    for (org, namespace, dialect, target) in [
        (
            "other-org",
            "frozen-databricks-136",
            "databricks",
            "CDO_DEMO_DB.FROZEN_SCHEMA.RESULT",
        ),
        (
            "local",
            "other-namespace",
            "databricks",
            "CDO_DEMO_DB.FROZEN_SCHEMA.RESULT",
        ),
        (
            "local",
            "frozen-databricks-136",
            "duckdb",
            "CDO_DEMO_DB.FROZEN_SCHEMA.RESULT",
        ),
        (
            "local",
            "frozen-databricks-136",
            "databricks",
            "CDO_DEMO_DB.OTHER_SCHEMA.RESULT",
        ),
        (
            "local",
            "frozen-databricks-136",
            "databricks",
            "`cdo_demo_db.frozen_schema`.`result`",
        ),
        (
            "local",
            "frozen-databricks-136",
            "databricks",
            "`cdo_demo_db`.`frozen_schema`.`re``sult`",
        ),
    ] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        scenario.build(req.clone(), 100).await;
        legacy::replace_with_frozen_execution(
            &server,
            &legacy::FrozenExecution {
                org_id: org,
                target_table: target,
                execution_type: 1,
                dialect,
                status: "confirmed",
                request_id: &uuid::Uuid::new_v4().to_string(),
                node_sql_hash: frozen["rows"][0]["node_sql_hash"].as_str(),
                node_template_hash: None,
                node_namespace_logic_hash: None,
                clone_source_table: None,
                table_namespace: Some(namespace),
                last_modified_epoch: Some(100),
            },
        )
        .await;
        scenario.expect_skip(req.clone()).await;
    }
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let mut producer = req.clone();
    producer.target_table = Some("`cdo_demo_db`.`frozen_schema`.`input`".into());
    producer.tables = vec![qc::TableModifiedInfo {
        name: producer.target_table.clone().unwrap(),
        last_modified_epoch: Some(100),
    }];
    scenario.build(producer, 100).await;
    let mut consumer = req.clone();
    consumer.sql = "select id from cdo_demo_db.frozen_schema.input".into();
    consumer.tables[1].last_modified_epoch = Some(100);
    consumer.freshness_tolerance_seconds = i64::MAX;
    scenario.build(consumer.clone(), 200).await;
    consumer.tables[0].last_modified_epoch = Some(200);
    scenario.expect_skip(consumer.clone()).await;
    legacy::replace_with_frozen_execution(
        &server,
        &legacy::FrozenExecution {
            org_id: "local",
            target_table: "CDO_DEMO_DB.FROZEN_SCHEMA.INPUT",
            execution_type: 1,
            dialect: "databricks",
            status: "confirmed",
            request_id: &uuid::Uuid::new_v4().to_string(),
            node_sql_hash: frozen["rows"][0]["node_sql_hash"].as_str(),
            node_template_hash: None,
            node_namespace_logic_hash: None,
            clone_source_table: None,
            table_namespace: Some("frozen-databricks-136"),
            last_modified_epoch: Some(100),
        },
    )
    .await;
    scenario.expect_execute(consumer).await;
}

#[tokio::test]
async fn persisted_alias_projection_matches_the_public_warehouse_grammar() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_literal_c6bfa07.json"
    ))
    .unwrap();
    let original: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    for dialect in [
        "databricks",
        "DATABRICKS",
        "spark",
        "spark, normalization_strategy = lowercase",
        "databricks, option = exact",
    ] {
        for alias in [
            "SELECT.I_SCHEMA.TABLE",
            "`SELECT`.I_SCHEMA.`TABLE`",
            "SELECT.`I_SCHEMA`.TABLE",
        ] {
            let server = support::start_server().await;
            let scenario = Scenario::connect(&server).await;
            let mut req = original.clone();
            req.dialect = dialect.into();
            req.target_table = Some("`select`.`I_schema`.`table`".into());
            req.tables[0].name = req.target_table.clone().unwrap();
            scenario.build(req.clone(), 100).await;
            scenario.expect_skip(req.clone()).await;
            legacy::replace_with_frozen_execution(
                &server,
                &legacy::FrozenExecution {
                    org_id: "local",
                    target_table: alias,
                    execution_type: 1,
                    dialect: "databricks",
                    status: "confirmed",
                    request_id: &uuid::Uuid::new_v4().to_string(),
                    node_sql_hash: frozen["rows"][0]["node_sql_hash"].as_str(),
                    node_template_hash: None,
                    node_namespace_logic_hash: None,
                    clone_source_table: None,
                    table_namespace: Some("frozen-databricks-136"),
                    last_modified_epoch: Some(100),
                },
            )
            .await;
            scenario.expect_execute(req).await;
        }
    }
    for unsupported in [
        "cdo_demo_db. frozen_schema.result",
        "`cdo_demo_db`/*comment*/.`frozen_schema`.`result`",
        "`cdo_demo_db`.`frozen_schema`.`re``sult`",
        "`cdo_demo_db.frozen_schema`.`result`",
        "cdo_demo_db.frozen_schema.reſult",
        "cdo_demo_db.frozen_schema.resuİt",
    ] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        scenario.build(original.clone(), 100).await;
        legacy::replace_with_frozen_execution(
            &server,
            &legacy::FrozenExecution {
                org_id: "local",
                target_table: unsupported,
                execution_type: 1,
                dialect: "databricks",
                status: "confirmed",
                request_id: &uuid::Uuid::new_v4().to_string(),
                node_sql_hash: frozen["rows"][0]["node_sql_hash"].as_str(),
                node_template_hash: None,
                node_namespace_logic_hash: None,
                clone_source_table: None,
                table_namespace: Some("frozen-databricks-136"),
                last_modified_epoch: Some(100),
            },
        )
        .await;
        scenario.expect_skip(original.clone()).await;
    }
}

#[tokio::test]
async fn unsupported_dialect_whitespace_retains_exact_family_and_physical_spelling() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_literal_c6bfa07.json"
    ))
    .unwrap();
    let original: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    for dialect in [
        "\tdatabricks\t",
        "\ndatabricks\n",
        "\u{a0}databricks\u{a0}",
        "spark\t, option = exact",
    ] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let mut req = original.clone();
        req.dialect = dialect.into();
        scenario.build(req.clone(), 100).await;
        scenario.expect_skip(req.clone()).await;
        legacy::replace_with_frozen_execution(
            &server,
            &legacy::FrozenExecution {
                org_id: "local",
                target_table: "CDO_DEMO_DB.FROZEN_SCHEMA.RESULT",
                execution_type: 1,
                dialect,
                status: "confirmed",
                request_id: &uuid::Uuid::new_v4().to_string(),
                node_sql_hash: frozen["rows"][0]["node_sql_hash"].as_str(),
                node_template_hash: None,
                node_namespace_logic_hash: None,
                clone_source_table: None,
                table_namespace: Some("frozen-databricks-136"),
                last_modified_epoch: Some(100),
            },
        )
        .await;
        scenario.expect_skip(req.clone()).await;
        let mut ordinary = req;
        ordinary.dialect = "databricks".into();
        scenario.expect_execute(ordinary).await;
    }
}

#[tokio::test]
async fn new_sql_builds_preserve_the_original_target_spelling_for_later_deferral() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_exact_literal_c6bfa07.json"
    ))
    .unwrap();
    let mut req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    req.target_table = Some("CDO_DEMO_DB.FROZEN_SCHEMA.RESULT".into());
    req.tables[0].name = req.target_table.clone().unwrap();
    req.dbt_node_state = Some(qc::DbtNodeState {
        project_id: Some("raw-target-project".into()),
        node_unique_id: "model.raw.result".into(),
        project_name: "raw-project".into(),
        target_name: "prod".into(),
        profile_name: "raw-profile".into(),
        ..Default::default()
    });
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    scenario.build(req.clone(), 100).await;
    scenario.expect_skip(req.clone()).await;
    let resolved = scenario
        .resolve_deferred(qc::ResolveDeferredRelationsRequest {
            project_id: Some("raw-target-project".into()),
            project_name: "raw-project".into(),
            profile_name: "raw-profile".into(),
            target_name: "prod".into(),
            node_unique_ids: vec!["model.raw.result".into()],
        })
        .await;
    assert_eq!(resolved.get("model.raw.result"), req.target_table.as_ref());
}
