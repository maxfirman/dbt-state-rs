//! Local restart is a persistence check, not a claim to restart hosted State.
mod support;

use dbt_state_proto::query_cache as qc;
use support::scenario::{built_at, into_execute, into_skip, model};
use tonic::transport::Channel;

struct Running {
    channel: Channel,
    server: Option<support::TestServer>,
}

impl Running {
    async fn start(database: &std::sync::Arc<support::TestDatabase>) -> Self {
        let server = support::start_server_on_database(database.clone()).await;
        let channel = support::channel(server.addr).await;
        Self {
            channel,
            server: Some(server),
        }
    }

    async fn stop(mut self) {
        // The fixture stops and joins its own runtime, including idle transport
        // tasks. The separate database owner keeps durable State alive.
        drop(self.server.take());
    }

    async fn submit(&self, req: qc::SubmitEnrichedSqlRequest) -> qc::SubmitSqlResponse {
        let response = qc::sql_client::SqlClient::new(self.channel.clone())
            .submit_enriched_sql(req)
            .await
            .unwrap()
            .into_inner();
        let value = serde_json::to_value(&response).unwrap();
        let decision = value["response"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap();
        let id = decision["execution_decision_id"].as_str().unwrap();
        let explanation = qc::explain_client::ExplainClient::new(self.channel.clone())
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![id.into()],
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(explanation.messages.len(), 1);
        assert_eq!(explanation.messages[0].execution_decision_id, id);
        assert!(!explanation.messages[0].explain_lines.is_empty());
        response
    }

    async fn confirm(
        &self,
        req: qc::ConfirmExecutionRequest,
    ) -> Result<qc::ConfirmExecutionResponse, tonic::Status> {
        qc::execution_client::ExecutionClient::new(self.channel.clone())
            .confirm_execution(req)
            .await
            .map(tonic::Response::into_inner)
    }

    async fn record(
        &self,
        records: Vec<qc::ExecutionRecord>,
    ) -> Result<qc::RecordExecutionsResponse, tonic::Status> {
        qc::execution_client::ExecutionClient::new(self.channel.clone())
            .record_executions(qc::RecordExecutionsRequest { records })
            .await
            .map(tonic::Response::into_inner)
    }
}

#[tokio::test]
async fn rejected_record_batch_matches_hosted_complete_status() {
    let server = support::start_server().await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../golden/fixtures/databricks_lifecycle_record.jsonl");
    let entries = dbt_state_harness::diff::load_golden(path.to_str().unwrap()).unwrap();
    let rejection = &entries[0];
    let channel = support::channel(server.addr).await;
    let outcome = dbt_state_harness::diff::replay_rpc(channel.clone(), rejection)
        .await
        .unwrap();
    rejection.compare_outcome(&outcome).unwrap();
    let mut next = entries[1].clone();
    let actual = dbt_state_harness::diff::replay_rpc(channel, &next)
        .await
        .unwrap();
    for field in ["request_id", "execution_decision_id"] {
        let captured = next.response["response"]["ready_to_execute"][field]
            .as_str()
            .unwrap();
        let local = actual.response["response"]["ready_to_execute"][field]
            .as_str()
            .unwrap();
        assert!(
            !captured.is_empty() && !local.is_empty(),
            "present minted IDs must remain present"
        );
        next.response["response"]["ready_to_execute"][field] = local.into();
    }
    next.compare_outcome(&actual).unwrap();
}

#[tokio::test]
async fn duplicate_confirmation_errors_preserve_the_first_published_outcome() {
    let database = std::sync::Arc::new(support::TestDatabase::new("confirmation_retry").await);
    let server = Running::start(&database).await;
    let node = model("`cdo_demo_db`.`owned_protocol`.`duplicate`")
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("owned_protocol")
        .namespace("duplicate-connection")
        .sql("select 158 as id")
        .own_epoch(Some(1000));
    let ready = into_execute(server.submit(node.clone().build()).await);
    assert!(
        server
            .confirm(built_at(1000).runtime(158).confirm(&ready.request_id))
            .await
            .unwrap()
            .success
    );
    for (epoch, runtime) in [(1000, 158), (9999, 999)] {
        let error = server
            .confirm(built_at(epoch).runtime(runtime).confirm(&ready.request_id))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(error.message(), "Database operation failed");
        assert_eq!(
            into_skip(server.submit(node.clone().build()).await).execution_runtime_ms,
            Some(158)
        );
    }
    let unknown = "019f0000-0000-7000-8000-000000000158";
    let error = server
        .confirm(built_at(1000).confirm(unknown))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::NotFound);
    assert_eq!(
        error.message(),
        format!("Execution request with ID {unknown} not found")
    );
    assert_eq!(
        into_skip(server.submit(node.build()).await).execution_runtime_ms,
        Some(158)
    );
    server.stop().await;
}

#[tokio::test]
async fn foreign_confirmation_ids_are_indistinguishable_from_unknown_ids() {
    let database = std::sync::Arc::new(support::TestDatabase::new("confirmation_org").await);
    let server = Running::start(&database).await;
    let node = model("`cdo_demo_db`.`owned_protocol`.`org`")
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("owned_protocol")
        .namespace("org-connection")
        .sql("select 158 as id")
        .own_epoch(Some(1000));
    let ready = into_execute(server.submit(node.clone().build()).await);
    let unknown = "019f0000-0000-7000-8000-000000000158";
    for confirmed in [false, true] {
        if confirmed {
            assert!(
                server
                    .confirm(built_at(1000).runtime(158).confirm(&ready.request_id))
                    .await
                    .unwrap()
                    .success
            );
        }
        for id in [ready.request_id.as_str(), unknown] {
            let mut request = tonic::Request::new(built_at(1000).runtime(999).confirm(id));
            request
                .metadata_mut()
                .insert("x-organization-id", "other-org".parse().unwrap());
            let error = qc::execution_client::ExecutionClient::new(server.channel.clone())
                .confirm_execution(request)
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::NotFound);
            assert_eq!(
                error.message(),
                format!("Execution request with ID {id} not found")
            );
            assert!(error.details().is_empty());
        }
    }
    assert_eq!(
        into_skip(server.submit(node.build()).await).execution_runtime_ms,
        Some(158)
    );
    server.stop().await;
}

#[tokio::test]
async fn confirmation_uuid_spellings_correlate_without_republishing() {
    let database = std::sync::Arc::new(support::TestDatabase::new("confirmation_uuid").await);
    let server = Running::start(&database).await;
    for spelling in ["upper", "simple", "braced"] {
        let node = model("`cdo_demo_db`.`owned_protocol`.`uuid`")
            .dialect("databricks")
            .catalog("cdo_demo_db")
            .schema("owned_protocol")
            .namespace(spelling)
            .sql("select 158 as id")
            .own_epoch(Some(1000));
        let ready = into_execute(server.submit(node.clone().build()).await);
        let alias = match spelling {
            "upper" => ready.request_id.to_uppercase(),
            "simple" => ready.request_id.replace('-', ""),
            _ => format!("{{{}}}", ready.request_id),
        };
        let response = server
            .confirm(built_at(1000).runtime(158).confirm(&alias))
            .await
            .unwrap();
        assert!(response.success);
        assert_eq!(response.request_id, alias);
        let error = server
            .confirm(built_at(2000).runtime(999).confirm(&ready.request_id))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(error.message(), "Database operation failed");
        assert_eq!(
            into_skip(server.submit(node.build()).await).execution_runtime_ms,
            Some(158)
        );
    }
    for malformed in [
        "",
        "does-not-exist",
        "urn:uuid:019f0000-abcd-7000-8000-000000000158",
        " 019f0000-abcd-7000-8000-000000000158 ",
    ] {
        let error = server
            .confirm(built_at(1000).confirm(malformed))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(error.message(), "Database operation failed");
    }
    server.stop().await;
}

#[tokio::test]
async fn rejected_missing_completion_epoch_leaves_the_physical_execution_pending() {
    let database = std::sync::Arc::new(support::TestDatabase::new("missing_completion").await);
    let server = Running::start(&database).await;
    let node = model("`cdo_demo_db`.`owned_protocol`.`pending`")
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("owned_protocol")
        .namespace("pending-connection")
        .sql("select 158 as id")
        .own_epoch(Some(1000));
    let first = into_execute(server.submit(node.clone().build()).await);
    let mut invalid = built_at(1000).runtime(999).confirm(&first.request_id);
    invalid.last_modified_epoch = None;
    let error = server.confirm(invalid).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "Database operation failed");
    let retry = into_execute(server.submit(node.clone().build()).await);
    assert_ne!(first.request_id, retry.request_id);
    assert!(
        server
            .confirm(built_at(1000).runtime(158).confirm(&first.request_id))
            .await
            .unwrap()
            .success
    );
    assert_eq!(
        into_skip(server.submit(node.build()).await).execution_runtime_ms,
        Some(158)
    );
    server.stop().await;
}

#[tokio::test]
async fn rejected_physical_record_outcome_publishes_neither_executions_nor_deferred_relations() {
    let database = std::sync::Arc::new(support::TestDatabase::new("atomic_completion").await);
    let server = Running::start(&database).await;
    let nodes = [
        (
            "`cdo_demo_db`.`owned_protocol`.`atomic_a`",
            "model.lifecycle.atomic_a",
            "select 158 as id",
            158,
        ),
        (
            "`cdo_demo_db`.`owned_protocol`.`atomic_b`",
            "model.lifecycle.atomic_b",
            "select 159 as id",
            159,
        ),
    ]
    .map(|(target, uid, sql, runtime)| {
        (
            model(target)
                .dialect("databricks")
                .catalog("cdo_demo_db")
                .schema("owned_protocol")
                .namespace("atomic-record")
                .unique_id(uid)
                .project("lifecycle-record")
                .target_name("prod")
                .node_state(|state| state.profile_name = "lifecycle".into())
                .sql(sql)
                .own_epoch(Some(1000)),
            runtime,
        )
    });
    let records: Vec<_> = nodes
        .iter()
        .map(|(node, runtime)| node.clone().record(built_at(1000).runtime(*runtime)))
        .collect();
    let mut invalid = records.clone();
    invalid[1].outcome.as_mut().unwrap().last_modified_epoch = None;
    let error = server.record(invalid).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "Database operation failed");
    let mapping = qc::ResolveDeferredRelationsRequest {
        profile_name: "lifecycle".into(),
        project_name: "lifecycle".into(),
        target_name: "prod".into(),
        project_id: Some("lifecycle-record".into()),
        node_unique_ids: vec![
            "model.lifecycle.atomic_a".into(),
            "model.lifecycle.atomic_b".into(),
        ],
    };
    let mut execution = qc::execution_client::ExecutionClient::new(server.channel.clone());
    assert!(execution
        .resolve_deferred_relations(mapping.clone())
        .await
        .unwrap()
        .into_inner()
        .fqn_by_unique_id
        .is_empty());
    for (node, _) in &nodes {
        into_execute(server.submit(node.clone().build()).await);
    }
    assert_eq!(server.record(records).await.unwrap().records_stored, 2);
    for (node, runtime) in nodes {
        assert_eq!(
            into_skip(server.submit(node.build()).await).execution_runtime_ms,
            Some(runtime)
        );
    }
    assert_eq!(
        execution
            .resolve_deferred_relations(mapping)
            .await
            .unwrap()
            .into_inner()
            .fqn_by_unique_id,
        [
            (
                "model.lifecycle.atomic_a".into(),
                "`cdo_demo_db`.`owned_protocol`.`atomic_a`".into()
            ),
            (
                "model.lifecycle.atomic_b".into(),
                "`cdo_demo_db`.`owned_protocol`.`atomic_b`".into()
            )
        ]
        .into()
    );
    server.stop().await;
}

#[tokio::test]
async fn restart_retains_only_confirmed_evidence_and_atomic_record_publication() {
    // This owner outlives every production router, so stopping the router does
    // not delete the database. No history is seeded through internal writes.
    let database = std::sync::Arc::new(support::TestDatabase::new("databricks_restart").await);
    let node = model("`cdo_demo_db`.`owned_protocol`.`recovery`")
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("owned_protocol")
        .namespace("restart-connection")
        .sql("select 158 as id")
        .own_epoch(Some(1000));
    let record_node = node
        .clone()
        .target("`cdo_demo_db`.`owned_protocol`.`recorded`")
        .sql("select 222 as id")
        .own_epoch(Some(2000));
    let record = record_node.clone().record(built_at(2000).runtime(222));
    let first = Running::start(&database).await;
    let abandoned = into_execute(first.submit(node.clone().build()).await);
    first.stop().await;

    let second = Running::start(&database).await;
    let retried = into_execute(second.submit(node.clone().build()).await);
    let error = second
        .record(vec![record.clone(), qc::ExecutionRecord::default()])
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert_eq!(error.message(), "Record at index 1 has no input set");
    assert!(
        second
            .confirm(built_at(1000).runtime(158).confirm(&retried.request_id))
            .await
            .unwrap()
            .success
    );
    second.stop().await;

    let third = Running::start(&database).await;
    assert_eq!(
        into_skip(third.submit(node.clone().build()).await).execution_runtime_ms,
        Some(158)
    );
    // A first confirmation delayed past a newer build must not replace it.
    assert!(
        third
            .confirm(built_at(900).runtime(1).confirm(&abandoned.request_id))
            .await
            .unwrap()
            .success
    );
    for runtime in [1, 999, 158] {
        let error = third
            .confirm(built_at(9999).runtime(runtime).confirm(&retried.request_id))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert_eq!(error.message(), "Database operation failed");
        assert_eq!(
            into_skip(third.submit(node.clone().build()).await).execution_runtime_ms,
            Some(158)
        );
    }
    // The rejected Record did not publish a reusable row across restart.
    into_execute(third.submit(record_node.clone().build()).await);
    assert_eq!(third.record(vec![record]).await.unwrap().records_stored, 1);
    third.stop().await;

    let fourth = Running::start(&database).await;
    assert_eq!(
        into_skip(fourth.submit(record_node.build()).await).execution_runtime_ms,
        Some(222)
    );
    assert_eq!(
        into_skip(fourth.submit(node.build()).await).execution_runtime_ms,
        Some(158)
    );
    fourth.stop().await;
}

fn frozen_execution<'a>(
    snapshot: &'a serde_json::Value,
    status: &'static str,
    epoch: Option<i64>,
) -> support::legacy::FrozenExecution<'a> {
    let row = &snapshot["rows"][0];
    support::legacy::FrozenExecution {
        org_id: row["org_id"].as_str().unwrap(),
        target_table: row["target_table"].as_str().unwrap(),
        execution_type: row["execution_type"].as_i64().unwrap() as i32,
        dialect: row["dialect"].as_str().unwrap(),
        status,
        request_id: row["request_id"].as_str().unwrap(),
        node_sql_hash: row["node_sql_hash"].as_str(),
        node_template_hash: row["node_template_hash"].as_str(),
        node_namespace_logic_hash: row["node_namespace_logic_hash"].as_str(),
        clone_source_table: row["clone_source_table"].as_str(),
        table_namespace: row["table_namespace"].as_str(),
        last_modified_epoch: epoch,
    }
}

#[tokio::test]
async fn current_identity_storage_fault_cannot_be_bypassed_by_external_modification_tolerance() {
    use support::scenario::Scenario;
    for (dialect, catalog, schema, target) in [
        (
            "databricks",
            "cdo_demo_db",
            "fault_lifecycle",
            "`cdo_demo_db`.`fault_lifecycle`.`result`",
        ),
        (
            "snowflake",
            "CDO_DEMO_DB",
            "FAULT_LIFECYCLE",
            "\"CDO_DEMO_DB\".\"FAULT_LIFECYCLE\".\"RESULT\"",
        ),
        (
            "duckdb",
            "warehouse",
            "prod",
            "\"warehouse\".\"prod\".\"result\"",
        ),
    ] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let node = model(target)
            .dialect(dialect)
            .catalog(catalog)
            .schema(schema)
            .namespace("current-completion-fault")
            .sql("select 158 as id")
            .own_epoch(Some(1000));
        scenario.build(node.clone(), 1000).await;
        scenario.expect_skip(node.clone()).await;
        support::fault::forget_completion_epoch(&server, target).await;
        let mut request = node.build();
        request.ignore_external_modifications = true;
        scenario.expect_execute(request).await;
    }
}

#[tokio::test]
async fn current_identity_storage_fault_cannot_supply_an_automatic_copy() {
    use support::scenario::Scenario;
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let source = "\"CDO_DEMO_DB\".\"FAULT_LIFECYCLE\".\"RESULT\"";
    let node = model(source)
        .dialect("snowflake")
        .catalog("CDO_DEMO_DB")
        .schema("FAULT_LIFECYCLE")
        .namespace("current-copy-fault")
        .sql("select 158 as id")
        .own_epoch(Some(1000));
    scenario.build(node.clone(), 1000).await;
    let copy = node
        .target("\"CDO_DEMO_DB\".\"FAULT_COPY\".\"RESULT\"")
        .schema("FAULT_COPY")
        .own_epoch(None);
    let positive = support::scenario::into_clone(scenario.submit(copy.clone()).await);
    assert_eq!(positive.clone_required_last_modified_epoch, Some(1000));
    support::fault::forget_completion_epoch(&server, source).await;
    scenario.expect_execute(copy).await;
}

#[tokio::test]
async fn frozen_unknown_epoch_is_ineligible_even_when_external_modifications_are_ignored() {
    use support::legacy::replace_with_frozen_execution;
    use support::scenario::Scenario;
    for bytes in [
        include_str!("../../../golden/upgrade/databricks_null_epoch_v2_5585f53.json"),
        include_str!("../../../golden/upgrade/snowflake_null_epoch_5585f53.json"),
    ] {
        let snapshot: serde_json::Value = serde_json::from_str(bytes).unwrap();
        let mut request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(snapshot["request"].clone()).unwrap();
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        // This positive control proves the genuinely frozen fingerprint still
        // matches today's submit. A v1/v2 mismatch cannot hide this regression.
        replace_with_frozen_execution(
            &server,
            &frozen_execution(&snapshot, "confirmed", Some(1000)),
        )
        .await;
        let compatibility = scenario.submit(request.clone()).await;
        assert_eq!(
            support::response_variant(&compatibility),
            "skip_execution",
            "frozen known-epoch positive must match current identity for {}",
            request.dialect
        );
        replace_with_frozen_execution(&server, &frozen_execution(&snapshot, "confirmed", None))
            .await;
        request.ignore_external_modifications = true;
        scenario.expect_execute(request).await;
    }
}

#[tokio::test]
async fn frozen_unknown_physical_history_cannot_be_resurrected_by_a_late_known_confirmation() {
    use support::legacy::append_frozen_execution;
    use support::scenario::Scenario;
    let snapshot: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_null_epoch_v2_5585f53.json"
    ))
    .unwrap();
    for status in ["pending", "confirmed"] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let mut request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(snapshot["request"].clone()).unwrap();
        request.sql = "select 157 as id".into();
        let older_pending = scenario.execute_id(request.clone()).await;
        // Only the pre-upgrade setup uses the legacy writer. All observations
        // and new executions use the public production protocol.
        append_frozen_execution(&server, &frozen_execution(&snapshot, status, None)).await;
        assert!(
            scenario
                .confirm(&older_pending, built_at(1000).runtime(158))
                .await
        );
        if status == "confirmed" {
            scenario.expect_execute(request.clone()).await;
        } else {
            assert_eq!(
                scenario
                    .expect_skip(request.clone())
                    .await
                    .execution_runtime_ms,
                Some(158)
            );
        }
        request.sql = "select 159 as id".into();
        let fresh = scenario.execute_id(request.clone()).await;
        assert!(scenario.confirm(&fresh, built_at(2000).runtime(159)).await);
        request.tables[0].last_modified_epoch = Some(2000);
        if status == "confirmed" {
            // Neither confirmation delivery nor a historical Record proves
            // chronology around an unknown physical result: remain a cold miss.
            scenario.expect_execute(request.clone()).await;
        } else {
            assert_eq!(
                scenario
                    .expect_skip(request.clone())
                    .await
                    .execution_runtime_ms,
                Some(159)
            );
        }
        request.sql = "select 157 as id".into();
        scenario.expect_execute(request).await;
    }
}

#[tokio::test]
async fn frozen_unknown_source_cannot_supply_an_automatic_result_copy() {
    use support::legacy::replace_with_frozen_execution;
    use support::scenario::Scenario;
    let snapshot: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/snowflake_null_epoch_5585f53.json"
    ))
    .unwrap();
    let request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(snapshot["copy_request"].clone()).unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    replace_with_frozen_execution(
        &server,
        &frozen_execution(&snapshot, "confirmed", Some(1000)),
    )
    .await;
    let positive = support::scenario::into_clone(scenario.submit(request.clone()).await);
    assert_eq!(positive.clone_required_last_modified_epoch, Some(1000));
    replace_with_frozen_execution(&server, &frozen_execution(&snapshot, "confirmed", None)).await;
    scenario.expect_execute(request).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt and Databricks credentials; writes fresh owned demo relations"]
async fn real_dbt_failure_retry_and_publication_match_independent_results() {
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("durable evidence directory");
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_lifecycle.py"
        ))
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt executable"))
        .arg("--proxy")
        .arg(env!("CARGO_BIN_EXE_record-proxy"))
        .arg("--probe")
        .arg(env!("CARGO_BIN_EXE_state-probe"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--schema-lease")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("ownership lease"))
        .arg("--work-dir")
        .arg(&artifacts);
    if let Some(identity) = std::env::var_os("DBT_STATE_DATABRICKS_QUERY_IDENTITY") {
        command.arg("--query-identity").arg(identity);
    }
    if let Some(run) = std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID") {
        command.arg("--run-id").arg(run);
    }
    let output = command
        .output()
        .await
        .expect("run real dbt lifecycle campaign");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
