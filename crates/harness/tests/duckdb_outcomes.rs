//! Precise lifecycle retries/rejection at the production public protocol seam.
mod support;
use dbt_state_proto::query_cache as qc;
use qc::{execution_client::ExecutionClient, sql_client::SqlClient};
use support::{channel, request_with_org, response_variant, start_server};

fn model() -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        dialect: "duckdb".into(),
        default_catalog: "warehouse".into(),
        default_schema: Some("dev".into()),
        target_table: Some("\"warehouse\".\"dev\".\"totals\"".into()),
        sql: "select 10::bigint as amount".into(),
        execution_type: 1,
        table_namespace: Some("isolated-file-generation".into()),
        tolerate_nondeterminism: true,
        tables: vec![qc::TableModifiedInfo {
            name: "\"warehouse\".\"dev\".\"totals\"".into(),
            last_modified_epoch: Some(10),
        }],
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.recovery.totals".into(),
            node_body_hash: Some("body".into()),
            node_hash: "hash".into(),
            project_id: Some("recovery".into()),
            project_name: "recovery".into(),
            target_name: "dev".into(),
            profile_name: "duckdb".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn ready(response: qc::SubmitSqlResponse) -> String {
    match response.response.unwrap() {
        qc::submit_sql_response::Response::ReadyToExecute(r) => r.request_id,
        other => panic!("expected pending BUILD, got {other:?}"),
    }
}

#[tokio::test]
async fn duckdb_correlated_confirmation_retry_preserves_first_outcome_and_order() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut execution = ExecutionClient::new(ch);
    let req = model();
    let pending = ready(
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
    );
    // Another pending decision still cannot make either result reusable.
    let second = ready(
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
    );
    assert_ne!(pending, second);
    let first = qc::ConfirmExecutionRequest {
        request_id: pending.clone(),
        last_modified_epoch: Some(10),
        table_type: Some("TABLE".into()),
        execution_runtime_ms: Some(123),
        execution_results: Some(qc::Struct {
            fields: [(
                "outcome".into(),
                qc::Value {
                    kind: Some(qc::value::Kind::StringValue("first".into())),
                },
            )]
            .into(),
        }),
        ..Default::default()
    };
    let wrong_org = execution
        .confirm_execution(request_with_org(first.clone(), "other"))
        .await
        .unwrap()
        .into_inner();
    assert!(!wrong_org.success);
    assert_eq!(wrong_org.request_id, pending);
    ready(
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
    );
    let response = execution
        .confirm_execution(first.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(response.success);
    assert_eq!(response.request_id, pending);
    // Retry the same request with conflicting physical epoch/type/runtime.
    // First durable outcome must survive, as seen by the subsequent SKIP.
    let conflicting = qc::ConfirmExecutionRequest {
        last_modified_epoch: Some(999),
        table_type: Some("VIEW".into()),
        execution_runtime_ms: Some(999),
        execution_results: Some(qc::Struct::default()),
        ..first.clone()
    };
    assert!(
        execution
            .confirm_execution(conflicting.clone())
            .await
            .unwrap()
            .into_inner()
            .success
    );
    let response = sql
        .submit_enriched_sql(req.clone())
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::SkipExecution(skip)) = response.response else {
        panic!("duplicate confirm replaced first physical epoch/type")
    };
    assert_eq!(skip.execution_runtime_ms, Some(123));
    assert_eq!(skip.execution_results, first.execution_results);
    // A distinct later outcome supersedes it. Retrying the first confirmation
    // must not refresh confirmed_at and resurrect the old physical state.
    let mut changed = req.clone();
    changed.sql = "select 20::bigint as amount".into();
    let latest = ready(
        sql.submit_enriched_sql(changed.clone())
            .await
            .unwrap()
            .into_inner(),
    );
    execution
        .confirm_execution(qc::ConfirmExecutionRequest {
            request_id: latest,
            ..first
        })
        .await
        .unwrap();
    execution.confirm_execution(conflicting).await.unwrap();
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(changed).await.unwrap().into_inner()),
        "skip_execution"
    );
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
        "ready_to_execute"
    );
}

#[tokio::test]
async fn duckdb_rejected_record_batch_leaves_execution_and_deferral_unpublished() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut execution = ExecutionClient::new(ch);
    let req = model();
    let record = qc::ExecutionRecord {
        input: Some(qc::execution_record::Input::EnrichedSql(qc::SqlExecution {
            dialect: req.dialect.clone(),
            default_catalog: req.default_catalog.clone(),
            default_schema: req.default_schema.clone(),
            target_table: req.target_table.clone(),
            sql: req.sql.clone(),
            execution_type: req.execution_type,
            table_namespace: req.table_namespace.clone(),
            dbt_node_state: req.dbt_node_state.clone(),
            tables: req.tables.clone(),
            ..Default::default()
        })),
        outcome: Some(qc::ExecutionOutcome {
            last_modified_epoch: Some(10),
            table_type: Some("TABLE".into()),
            ..Default::default()
        }),
    };
    let resolve = qc::ResolveDeferredRelationsRequest {
        profile_name: "duckdb".into(),
        target_name: "dev".into(),
        project_name: "recovery".into(),
        project_id: Some("recovery".into()),
        node_unique_ids: vec!["model.recovery.totals".into()],
    };
    let rejected = execution
        .record_executions(qc::RecordExecutionsRequest {
            records: vec![record.clone(), qc::ExecutionRecord::default()],
        })
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::InvalidArgument);
    assert!(execution
        .resolve_deferred_relations(resolve.clone())
        .await
        .unwrap()
        .into_inner()
        .fqn_by_unique_id
        .is_empty());
    ready(
        sql.submit_enriched_sql(req.clone())
            .await
            .unwrap()
            .into_inner(),
    );
    assert_eq!(
        execution
            .record_executions(qc::RecordExecutionsRequest {
                records: vec![record]
            })
            .await
            .unwrap()
            .into_inner()
            .records_stored,
        1
    );
    assert_eq!(
        execution
            .resolve_deferred_relations(resolve)
            .await
            .unwrap()
            .into_inner()
            .fqn_by_unique_id
            .get("model.recovery.totals"),
        req.target_table.as_ref()
    );
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
        "skip_execution"
    );
}
