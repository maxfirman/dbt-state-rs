//! Offline gRPC regressions for physical scope and conservative SQL identity.

#[path = "support.rs"]
mod support;

use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, response_variant, start_server};

fn submit(target: &str, dialect: &str, namespace: &str, sql: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: dialect.into(),
        table_namespace: Some(namespace.into()),
        default_catalog: "db".into(),
        default_schema: Some("schema".into()),
        execution_type: 1,
        sql: sql.into(),
        tolerate_nondeterminism: true,
        tables: vec![qc::TableModifiedInfo {
            name: target.into(),
            last_modified_epoch: Some(100),
        }],
        ..Default::default()
    }
}

async fn build(
    sql: &mut SqlClient<tonic::transport::Channel>,
    exec: &mut ExecutionClient<tonic::transport::Channel>,
    request: qc::SubmitEnrichedSqlRequest,
) {
    let response = sql.submit_enriched_sql(request).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(ready) = response.response.unwrap()
    else {
        panic!("expected a build decision");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: ready.request_id,
        last_modified_epoch: Some(200),
        table_type: Some("TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
}

async fn variant(
    sql: &mut SqlClient<tonic::transport::Channel>,
    request: qc::SubmitEnrichedSqlRequest,
) -> &'static str {
    let response = sql.submit_enriched_sql(request).await.unwrap().into_inner();
    response_variant(&response)
}

#[tokio::test]
async fn same_relation_text_is_independent_across_warehouse_connections() {
    let (addr, _) = start_server().await;
    let channel = channel(addr).await;
    let mut sql = SqlClient::new(channel.clone());
    let mut exec = ExecutionClient::new(channel);
    let target = "DB.S.T";
    let a = submit(target, "snowflake", "account-a", "select 1");
    let b = submit(target, "snowflake", "account-b", "select 2");
    let c = submit(target, "redshift", "account-a", "select 3");

    build(&mut sql, &mut exec, a.clone()).await;
    build(&mut sql, &mut exec, b.clone()).await;
    build(&mut sql, &mut exec, c.clone()).await;
    for request in [a, b, c] {
        assert_eq!(variant(&mut sql, request).await, "skip_execution");
    }
}

#[tokio::test]
async fn bigquery_reuses_exact_sql_but_does_not_clone_unverified_equivalence() {
    let (addr, _) = start_server().await;
    let channel = channel(addr).await;
    let mut sql = SqlClient::new(channel.clone());
    let mut exec = ExecutionClient::new(channel);
    let source = "`project.dataset.source`";
    let target = "`project.dataset.target`";
    let base = submit(source, "bigquery", "project", "select 1 as id");
    build(&mut sql, &mut exec, base.clone()).await;
    assert_eq!(variant(&mut sql, base.clone()).await, "skip_execution");

    let changed_whitespace = submit(source, "bigquery", "project", "SELECT 1 AS id");
    assert_eq!(
        variant(&mut sql, changed_whitespace).await,
        "ready_to_execute"
    );

    let fresh_target = submit(target, "bigquery", "project", "select 1 as id");
    assert_eq!(variant(&mut sql, fresh_target).await, "ready_to_execute");
}
