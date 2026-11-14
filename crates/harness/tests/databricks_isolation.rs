//! Self-hosted isolation safety; hosted OAuth cannot select an arbitrary org.
mod support;

use support::scenario::{built_at, model, Scenario};

#[test]
fn concurrent_campaign_cleanup_rejects_peer_relations() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_isolation_test.py"
        ))
        .output()
        .expect("run credential-free ownership boundary checks");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn self_hosted_org_family_namespace_and_target_histories_stay_independent() {
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let a = scenario.org("isolation-a");
    let b = scenario.org("isolation-b");
    let request = model("`cdo_demo_db`.`isolation`.`result`")
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("isolation")
        .namespace("connection-a")
        .own_epoch(Some(159))
        .allow_clones(false)
        .without_node_state()
        .sql("select cast(159 as bigint) as id");
    a.build(request.clone(), 159).await;
    a.expect_skip(request.clone()).await;
    b.expect_execute(request.clone()).await;
    b.build(request.clone().sql("select cast(160 as bigint) as id"), 159)
        .await;
    a.expect_skip(request.clone()).await;
    b.expect_execute(request.clone()).await;

    for other in [
        request.clone().namespace("connection-b"),
        request.clone().with(|q| q.table_namespace = None),
        request.clone().dialect("snowflake"),
        request
            .clone()
            .target("`cdo_demo_db`.`other_schema`.`result`"),
        request
            .clone()
            .target("`other_catalog`.`isolation`.`result`"),
    ] {
        a.expect_execute(other.clone()).await;
        let changed = other.sql("select cast(161 as bigint) as id");
        a.build(changed.clone(), 159).await;
        a.expect_skip(changed).await;
        a.expect_skip(request.clone()).await;
    }
    // Missing and empty namespaces share only the absent connection scope.
    let absent = request
        .clone()
        .with(|q| q.table_namespace = None)
        .sql("select cast(161 as bigint) as id");
    a.expect_skip(absent.clone()).await;
    a.expect_skip(absent.with(|q| q.table_namespace = Some(String::new())))
        .await;
}

#[tokio::test]
async fn evidenced_spark_aliases_observe_latest_identity_without_crossing_scopes() {
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    for (index, alias) in ["spark", "spark, normalization_strategy = lowercase"]
        .iter()
        .enumerate()
    {
        let request = model("`cdo_demo_db`.`isolation`.`result`")
            .dialect("databricks")
            .catalog("cdo_demo_db")
            .schema("isolation")
            .namespace(&format!("alias-{index}"))
            .own_epoch(Some(159))
            .allow_clones(false)
            .without_node_state()
            .sql("select cast(159 as bigint) as id");
        scenario.build(request.clone(), 159).await;
        let alias_request = request.clone().dialect(alias);
        scenario.expect_skip(alias_request.clone()).await;
        // A pending different identity never shadows a confirmed result.
        scenario
            .expect_execute(
                alias_request
                    .clone()
                    .sql("select cast(160 as bigint) as id"),
            )
            .await;
        scenario.expect_skip(request.clone()).await;
        let changed = alias_request.sql("select cast(160 as bigint) as id");
        let build = scenario.expect_execute(changed.clone()).await;
        assert!(scenario.confirm(&build.request_id, built_at(159)).await);
        scenario.expect_skip(changed.clone()).await;
        scenario
            .expect_skip(changed.clone().dialect("databricks"))
            .await;
        scenario.expect_execute(request).await;
        scenario
            .expect_execute(changed.clone().namespace("other-connection"))
            .await;
        scenario
            .expect_execute(
                changed
                    .clone()
                    .target("`cdo_demo_db`.`other_schema`.`result`"),
            )
            .await;
        scenario
            .org("another-org")
            .expect_execute(changed.clone())
            .await;
        for unsupported in [
            "spark, normalization_strategy = uppercase",
            "spark, normalization_strategy = lowercase, unknown = x",
            "spark , normalization_strategy = lowercase",
        ] {
            scenario
                .expect_execute(changed.clone().dialect(unsupported))
                .await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt 2.0.11 and Databricks credentials; writes run-owned demo schemas"]
async fn real_dbt_concurrent_runs_and_owned_cleanup_preserve_peer_results() {
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("databricks-isolation-{}", uuid::Uuid::new_v4()))
        });
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML installed"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_isolation.py"
        ))
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt executable"))
        .arg("--proxy")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_PROXY")
                .unwrap_or_else(|| env!("CARGO_BIN_EXE_record-proxy").into()),
        )
        .arg("--probe")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_PROBE")
                .unwrap_or_else(|| env!("CARGO_BIN_EXE_state-probe").into()),
        )
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(&artifacts);
    if let Some(lease) = std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE") {
        command.arg("--schema-lease").arg(lease);
    }
    if let Some(identity) = std::env::var_os("DBT_STATE_DATABRICKS_QUERY_IDENTITY") {
        command.arg("--query-identity").arg(identity);
    }
    let output = command
        .output()
        .await
        .expect("run concurrent Databricks isolation");
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
async fn alias_identity_upgrade_rejects_frozen_v1_and_late_pending_history() {
    use dbt_state_proto::query_cache as qc;
    use support::legacy;
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_alias_identifiers_v1_aa89b6a.json"
    ))
    .unwrap();
    let request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    for pending in [false, true] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let old_id = uuid::Uuid::new_v4().to_string();
        let row = legacy::FrozenExecution {
            org_id: "local",
            target_table: "`cdo_demo_db`.`frozen_alias_159`.`result`",
            execution_type: 1,
            dialect: "databricks",
            status: if pending { "pending" } else { "confirmed" },
            request_id: &old_id,
            node_sql_hash: frozen["rows"][0]["node_sql_hash"].as_str(),
            node_template_hash: None,
            node_namespace_logic_hash: None,
            clone_source_table: None,
            table_namespace: Some("frozen-databricks-alias-159"),
            last_modified_epoch: Some(159),
        };
        legacy::replace_with_frozen_execution(&server, &row).await;
        scenario.expect_execute(request.clone()).await;
        let mut alias = request.clone();
        alias.dialect = "spark".into();
        scenario.build(alias.clone(), 159).await;
        scenario.expect_skip(request.clone()).await;
        if pending {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            assert!(scenario.confirm(&old_id, built_at(159)).await);
            scenario.expect_execute(request.clone()).await;
            scenario.expect_execute(alias).await;
        }
    }
}

#[tokio::test]
async fn rendered_alias_admission_does_not_equate_changed_sql_through_template_identity() {
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let request = model("`cdo_demo_db`.`isolation`.`template_result`")
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("isolation")
        .namespace("template-alias-control")
        .own_epoch(Some(159))
        .allow_clones(false)
        .compare_unrendered_code()
        .body("unchanged-template")
        .sql("select cast(159 as bigint) as id");
    scenario.build(request.clone(), 159).await;
    scenario.expect_skip(request.clone()).await;
    for alias in ["spark", "spark, normalization_strategy = lowercase"] {
        scenario
            .expect_execute(
                request
                    .clone()
                    .dialect(alias)
                    .sql("select cast(160 as bigint) as id"),
            )
            .await;
    }
}

#[tokio::test]
async fn omitted_namespace_reports_query_evidence_without_reusing_another_scope() {
    use dbt_state_proto::query_cache::ModelExecutionType;
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    for kind in [ModelExecutionType::Full, ModelExecutionType::View] {
        let named = model("`cdo_demo_db`.`isolation`.`namespace_diagnostic`")
            .dialect("databricks")
            .catalog("cdo_demo_db")
            .schema("isolation")
            .execution_type(kind)
            .namespace(&format!("named-diagnostic-{}", kind as i32))
            .own_epoch(Some(159))
            .allow_clones(false)
            .without_node_state()
            .sql(&format!("select 159 as id, {} as kind", kind as i32));
        scenario.build(named.clone(), 159).await;
        let absent = named.clone().with(|q| q.table_namespace = None);
        let response = scenario.expect_execute(absent.clone()).await;
        let decision = response.explained_decision.unwrap();
        assert_eq!(decision.skip_rejection_reason, Some(8));
        assert_eq!(decision.clone_rejection_reason, Some(7));
        let empty = absent
            .clone()
            .with(|q| q.table_namespace = Some(String::new()));
        assert_eq!(
            scenario
                .expect_execute(empty)
                .await
                .explained_decision
                .unwrap()
                .skip_rejection_reason,
            Some(6)
        );
        assert_eq!(
            scenario
                .org("other-org")
                .expect_execute(absent.clone())
                .await
                .explained_decision
                .unwrap()
                .skip_rejection_reason,
            Some(6)
        );
        let changed = absent
            .clone()
            .sql(&format!("select 160 as id, {} as kind", kind as i32));
        scenario.build(changed.clone(), 159).await;
        scenario.expect_skip(named).await;
        scenario.expect_skip(changed.clone()).await;
        scenario
            .expect_skip(changed.with(|q| q.table_namespace = Some(String::new())))
            .await;
        assert_eq!(
            scenario
                .expect_execute(absent)
                .await
                .explained_decision
                .unwrap()
                .skip_rejection_reason,
            Some(8)
        );
    }
}

#[tokio::test]
async fn other_target_query_diagnostics_never_grant_copy_or_resurrect_a_source() {
    use dbt_state_proto::query_cache::ModelExecutionType;
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    for kind in [ModelExecutionType::View, ModelExecutionType::Full] {
        let source = model("`cdo_demo_db`.`isolation`.`diagnostic_source`")
            .dialect("databricks")
            .catalog("cdo_demo_db")
            .schema("isolation")
            .execution_type(kind)
            .namespace(&format!("other-name-{}", kind as i32))
            .own_epoch(Some(159))
            .allow_clones(false)
            .without_node_state()
            .sql("select cast(159 as bigint) as id");
        let source = if kind == ModelExecutionType::Full {
            source.clone_chain_depth_limit(0)
        } else {
            source
        };
        scenario.build(source.clone(), 159).await;
        let target = source
            .clone()
            .target("`cdo_demo_db`.`isolation`.`diagnostic_target`");
        let response = scenario.expect_execute(target.clone()).await;
        let decision = response.explained_decision.unwrap();
        assert_eq!(decision.skip_rejection_reason, Some(1));
        assert_eq!(
            decision.clone_rejection_reason,
            Some(if kind == ModelExecutionType::View {
                3
            } else {
                10
            })
        );
        assert_eq!(
            scenario
                .expect_execute(target.clone().namespace("another-connection"))
                .await
                .explained_decision
                .unwrap()
                .skip_rejection_reason,
            Some(6)
        );
        assert_eq!(
            scenario
                .org("another-org")
                .expect_execute(target.clone())
                .await
                .explained_decision
                .unwrap()
                .skip_rejection_reason,
            Some(6)
        );
        assert_eq!(
            scenario
                .expect_execute(target.clone().extra("isolation", "changed"))
                .await
                .explained_decision
                .unwrap()
                .skip_rejection_reason,
            Some(6)
        );
        scenario
            .build(source.clone().sql("select cast(160 as bigint) as id"), 159)
            .await;
        assert_eq!(
            scenario
                .expect_execute(target.clone())
                .await
                .explained_decision
                .unwrap()
                .skip_rejection_reason,
            Some(6)
        );
        // A refused optimization must publish an ordinary build when the
        // caller executes and confirms it, independent of the named source.
        scenario.build(source.clone(), 159).await;
        let refused = scenario.expect_execute(target.clone()).await;
        assert!(scenario.confirm(&refused.request_id, built_at(159)).await);
        scenario.expect_skip(target.clone()).await;
        scenario
            .expect_execute(target.clone().sql("select cast(160 as bigint) as id"))
            .await;
        scenario
            .build(source.sql("select cast(160 as bigint) as id"), 159)
            .await;
        scenario.expect_skip(target).await;
    }
}

#[tokio::test]
async fn empty_namespace_writes_are_reusable_as_the_absent_scope() {
    use support::scenario::{data_test, outcome, seed, test_results};
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let request = model("`cdo_demo_db`.`isolation`.`empty_namespace`")
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("isolation")
        .namespace("")
        .own_epoch(Some(159))
        .allow_clones(false)
        .without_node_state()
        .sql("select cast(159 as bigint) as id");
    scenario.build(request.clone(), 159).await;
    scenario.expect_skip(request.clone()).await;
    scenario
        .expect_skip(request.clone().with(|q| q.table_namespace = None))
        .await;
    let changed = request.clone().sql("select cast(160 as bigint) as id");
    scenario
        .record([changed.record(built_at(159))])
        .await
        .unwrap();
    scenario
        .expect_skip(changed.with(|q| q.table_namespace = None))
        .await;
    scenario.expect_execute(request).await;

    let values = seed("`cdo_demo_db`.`isolation`.`empty_seed`")
        .dialect("databricks")
        .namespace("")
        .own_epoch(Some(159));
    scenario.build_values(values.clone(), built_at(159)).await;
    let mut absent_values: dbt_state_proto::query_cache::SubmitValuesRequest = values.into();
    absent_values.table_namespace = None;
    assert_eq!(
        scenario.values_verdict(absent_values).await,
        "skip_execution"
    );

    let test = data_test("test.isolation.empty_namespace")
        .dialect("databricks")
        .namespace("")
        .project("isolation");
    let pending = scenario.expect_execute(test.clone()).await;
    assert!(
        scenario
            .confirm(&pending.request_id, outcome().results(test_results(0)))
            .await
    );
    scenario
        .expect_skip(test.with(|q| q.table_namespace = None))
        .await;
}

#[tokio::test]
async fn legacy_empty_namespace_rows_and_late_pending_rows_share_latest_physical_scope() {
    use dbt_state_proto::query_cache as qc;
    use support::legacy;
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_empty_namespace_v2_4b38371.json"
    ))
    .unwrap();
    let empty: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    for pending in [false, true] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let old_id = uuid::Uuid::new_v4().to_string();
        legacy::replace_with_frozen_execution(
            &server,
            &legacy::FrozenExecution {
                org_id: "local",
                target_table: "`cdo_demo_db`.`frozen_empty_159`.`result`",
                execution_type: 1,
                dialect: "databricks",
                status: if pending { "pending" } else { "confirmed" },
                request_id: &old_id,
                node_sql_hash: frozen["rows"][0]["node_sql_hash"].as_str(),
                node_template_hash: None,
                node_namespace_logic_hash: None,
                clone_source_table: None,
                table_namespace: Some(""),
                last_modified_epoch: Some(159),
            },
        )
        .await;
        let mut absent = empty.clone();
        absent.table_namespace = None;
        if !pending {
            scenario.expect_skip(empty.clone()).await;
            scenario.expect_skip(absent.clone()).await;
        }
        let mut changed = absent.clone();
        changed.sql = "select cast(160 as bigint) as id".into();
        scenario.build(changed.clone(), 159).await;
        scenario.expect_skip(changed.clone()).await;
        if pending {
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            assert!(scenario.confirm(&old_id, built_at(159)).await);
            scenario.expect_skip(absent).await;
            scenario.expect_execute(changed).await;
        } else {
            scenario.expect_execute(absent).await;
        }
    }
}

#[tokio::test]
async fn absent_scope_definition_is_diagnostic_only_for_a_fresh_named_namespace() {
    use dbt_state_proto::query_cache as qc;
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let absent = model("`cdo_demo_db`.`isolation`.`absent_definition`")
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("isolation")
        .own_epoch(Some(159))
        .allow_clones(false)
        .without_node_state()
        .sql("select cast(159 as bigint) as id");
    scenario.build(absent.clone(), 159).await;
    let named = absent.clone().namespace("different-warehouse-connection");
    let mut explain = qc::explain_client::ExplainClient::new(support::channel(server.addr).await);
    for (request, expected_query) in [
        (named.clone(), ""),
        (
            named.clone().sql("select cast(160 as bigint) as id"),
            "the model query has changed",
        ),
    ] {
        // Hosted permits a cross-scope SKIP in the equal-query case. The
        // explicit self-hosted isolation invariant intentionally still builds.
        let ready = scenario.expect_execute(request).await;
        assert_eq!(
            ready.explained_decision.unwrap().skip_rejection_reason,
            Some(6)
        );
        let messages = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![ready.execution_decision_id.unwrap()],
            })
            .await
            .unwrap()
            .into_inner()
            .messages;
        let query = messages[0]
            .explain_lines
            .iter()
            .find(|line| line.text == "query analysis")
            .unwrap();
        if expected_query.is_empty() {
            assert!(query.children.is_empty());
        } else {
            assert_eq!(query.children[0].text, expected_query);
        }
    }
    let changed_named = named.sql("select cast(160 as bigint) as id");
    scenario.build(changed_named.clone(), 159).await;
    scenario.expect_skip(changed_named).await;
    scenario.expect_skip(absent).await;
}
