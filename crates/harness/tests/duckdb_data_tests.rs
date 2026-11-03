//! Scoped DuckDB cached outcomes at the production gRPC seam.
mod support;
use dbt_state_proto::query_cache as qc;
use qc::{execution_client::ExecutionClient, sql_client::SqlClient};
use tonic::Request;

fn request() -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        dialect: "duckdb".into(),
        default_catalog: "warehouse".into(),
        default_schema: Some("dev".into()),
        execution_type: 8,
        tolerate_nondeterminism: true,
        sql: "select count(*) as failures from warehouse.dev.numbers where amount < 0".into(),
        table_namespace: Some("file-generation".into()),
        tables: vec![qc::TableModifiedInfo {
            name: "warehouse.dev.numbers".into(),
            last_modified_epoch: Some(10),
        }],
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "test.same_name.positive".into(),
            project_id: Some("local-project-root-a".into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn org<T>(body: T, organization: &str) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("x-organization-id", organization.parse().unwrap());
    request
}

fn results() -> qc::Struct {
    serde_json::from_value(serde_json::json!({"fields": {
        "failures": {"kind": {"int_value": 0}},
        "should_error": {"kind": {"bool_value": false}},
        "should_warn": {"kind": {"bool_value": false}}
    }}))
    .unwrap()
}

#[tokio::test]
async fn duckdb_cached_tests_require_complete_scoped_confirmed_evidence() {
    let (addr, schema, state) = support::start_server_with_state().await;
    let channel = support::channel(addr).await;
    let mut sql = SqlClient::new(channel.clone());
    let mut execution = ExecutionClient::new(channel);
    let baseline = request();
    // Pending SQL errors / interrupted execution never become cached passes.
    for payload in [None, Some(qc::Struct::default()), Some(results())] {
        let response = sql
            .submit_enriched_sql(org(baseline.clone(), "one"))
            .await
            .unwrap()
            .into_inner();
        let Some(qc::submit_sql_response::Response::ReadyToExecute(build)) = response.response
        else {
            panic!("must execute")
        };
        let pending = sql
            .submit_enriched_sql(org(baseline.clone(), "one"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(support::response_variant(&pending), "ready_to_execute");
        assert!(
            execution
                .confirm_execution(org(
                    qc::ConfirmExecutionRequest {
                        request_id: build.request_id,
                        execution_results: payload.clone(),
                        last_modified_epoch: None,
                        table_type: None,
                        ..Default::default()
                    },
                    "one"
                ))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        let response = sql
            .submit_enriched_sql(org(baseline.clone(), "one"))
            .await
            .unwrap()
            .into_inner();
        if payload == Some(results()) {
            let Some(qc::submit_sql_response::Response::SkipExecution(skip)) = response.response
            else {
                panic!("complete pass reuses: {response:?}")
            };
            assert_eq!(skip.execution_results, payload);
        } else {
            assert_eq!(support::response_variant(&response), "ready_to_execute");
        }
    }
    let mut variants = vec![];
    let mut changed = baseline.clone();
    changed.dbt_node_state.as_mut().unwrap().project_id = Some("local-project-root-b".into());
    variants.push(changed);
    let mut changed = baseline.clone();
    changed.table_namespace = Some("other-file-generation".into());
    variants.push(changed);
    let mut changed = baseline.clone();
    changed.dbt_node_state.as_mut().unwrap().node_unique_id = "test.same_name.other".into();
    variants.push(changed);
    let mut changed = baseline.clone();
    changed.tables[0].last_modified_epoch = None;
    variants.push(changed);
    let mut changed = baseline.clone();
    changed.tables[0].last_modified_epoch = Some(11);
    variants.push(changed);
    let mut changed = baseline.clone();
    changed.tables.push(qc::TableModifiedInfo {
        name: "warehouse.dev.numbers".into(),
        last_modified_epoch: Some(11),
    });
    variants.push(changed);
    let mut changed = baseline.clone();
    changed.tables.clear();
    variants.push(changed);
    for changed in variants {
        let response = sql
            .submit_enriched_sql(org(changed, "one"))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(support::response_variant(&response), "ready_to_execute");
    }
    let response = sql
        .submit_enriched_sql(org(baseline.clone(), "two"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(support::response_variant(&response), "ready_to_execute");
    let response = sql
        .submit_enriched_sql(org(baseline, "one"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(support::response_variant(&response), "skip_execution");
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(state.store.pool())
        .await
        .unwrap();
}
