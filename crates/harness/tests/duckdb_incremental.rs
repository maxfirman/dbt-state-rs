//! Compile-time incremental initialization through the production gRPC router.
mod support;
use dbt_state_proto::query_cache as qc;
use qc::{clone_client::CloneClient, execution_client::ExecutionClient, sql_client::SqlClient};
use support::{channel, response_variant, start_server};

fn submit(schema: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(format!("\"db\".\"{schema}\".\"result\"")),
        dialect: "duckdb".into(),
        default_catalog: "db".into(),
        default_schema: Some(schema.into()),
        table_namespace: Some("incremental-file".into()),
        sql: "select id, amount from db.raw.input".into(),
        execution_type: 5,
        tolerate_nondeterminism: true,
        semantic_extras: [
            ("duckdb_copy_contract".into(), "rows-schema-v1".into()),
            (
                "duckdb_incremental_contract".into(),
                "delete-insert-fixed-v1".into(),
            ),
        ]
        .into(),
        tables: vec![qc::TableModifiedInfo {
            name: "db.raw.input".into(),
            last_modified_epoch: Some(10),
        }],
        dbt_node_state: Some(qc::DbtNodeState {
            node_hash: "duckdb-incremental-init-v1:test".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn register() -> qc::CloneRequest {
    qc::CloneRequest {
        target_table: "\"db\".\"dev\".\"result\"".into(),
        clone_source_table: "\"db\".\"prod\".\"result\"".into(),
        dialect: "duckdb".into(),
        default_catalog: "db".into(),
        execution_type: 5,
        table_namespace: Some("incremental-file".into()),
        clone_source_last_modified_epoch: Some(20),
        clone_source_table_type: Some("DUCKDB CTAS TABLE".into()),
        clone_chain_depth_limit: Some(1),
        labels: [
            (
                "duckdb_incremental_contract".into(),
                "delete-insert-fixed-v1".into(),
            ),
            (
                "duckdb_incremental_identity".into(),
                "duckdb-incremental-init-v1:test".into(),
            ),
        ]
        .into(),
        ..Default::default()
    }
}
#[tokio::test]
async fn incremental_register_requires_current_source_and_adopts_only_matching_logic() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut copies = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);
    assert!(copies
        .register_clone(register())
        .await
        .unwrap()
        .into_inner()
        .unable_to_clone
        .is_some());
    let result = sql
        .submit_enriched_sql(submit("prod"))
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToExecute(build)) = result.response else {
        panic!("BUILD")
    };
    // Pending intent never qualifies as a source.
    assert!(copies
        .register_clone(register())
        .await
        .unwrap()
        .into_inner()
        .unable_to_clone
        .is_some());
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: build.request_id,
        last_modified_epoch: Some(20),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    for case in 0..9 {
        let mut bad = register();
        match case {
            0 => bad.clone_source_last_modified_epoch = None,
            1 => bad.clone_source_last_modified_epoch = Some(19),
            2 => bad.clone_source_last_modified_epoch = Some(21),
            3 => bad.table_namespace = Some("different-file".into()),
            4 => bad.execution_type = 3,
            5 => bad.clone_source_table_type = Some("TABLE".into()),
            6 => bad.clone_chain_depth_limit = Some(0),
            7 => {
                bad.labels.insert(
                    "duckdb_incremental_identity".into(),
                    "duckdb-incremental-init-v1:changed".into(),
                );
            }
            _ => bad.labels.clear(),
        }
        assert!(
            copies
                .register_clone(bad)
                .await
                .unwrap()
                .into_inner()
                .unable_to_clone
                .is_some(),
            "case {case}"
        );
    }
    let response = copies
        .register_clone(register())
        .await
        .unwrap()
        .into_inner();
    let clone = response.ready_to_clone.unwrap();
    assert_eq!(clone.clone_required_last_modified_epoch, Some(20));
    #[allow(deprecated)]
    {
        assert_eq!(
            response.response,
            Some(qc::clone_response::Response::ReadyToCloneV1(clone.clone()))
        );
    }
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: clone.request_id,
        last_modified_epoch: Some(30),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut later = submit("dev");
    later.tables.push(qc::TableModifiedInfo {
        name: later.target_table.clone().unwrap(),
        last_modified_epoch: Some(30),
    });
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(later.clone())
                .await
                .unwrap()
                .into_inner()
        ),
        "skip_execution"
    );
    later.sql = "select id, amount+1 as amount from db.raw.input".into();
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(later).await.unwrap().into_inner()),
        "ready_to_execute"
    );
    // A later incompatible physical write hides the earlier eligible ET5 row.
    let mut overwrite = submit("prod");
    overwrite.execution_type = 1;
    overwrite.allow_clones = Some(false);
    let response = sql
        .submit_enriched_sql(overwrite)
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToExecute(build)) = response.response else {
        panic!("BUILD")
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: build.request_id,
        last_modified_epoch: Some(40),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(copies
        .register_clone(register())
        .await
        .unwrap()
        .into_inner()
        .unable_to_clone
        .is_some());
}
