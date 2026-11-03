//! Conservative table result copying through the production service.
mod support;
use dbt_state_proto::query_cache as qc;
use qc::{execution_client::ExecutionClient, sql_client::SqlClient};
use support::{channel, response_variant, start_server};
fn request(schema: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(format!("\"db\".\"{schema}\".\"result\"")),
        dialect: "duckdb".into(),
        default_catalog: "db".into(),
        default_schema: Some(schema.into()),
        table_namespace: Some("copy-file-generation".into()),
        sql: "select id, amount + 1 as amount from db.raw.input".into(),
        execution_type: 1,
        tolerate_nondeterminism: true,
        semantic_extras: [("duckdb_copy_contract".into(), "rows-schema-v1".into())].into(),
        tables: vec![qc::TableModifiedInfo {
            name: "db.raw.input".into(),
            last_modified_epoch: Some(10),
        }],
        ..Default::default()
    }
}
#[tokio::test]
async fn eligible_table_copy_confirms_then_skips_preserving_physical_inputs() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);
    let prod = sql
        .submit_enriched_sql(request("prod"))
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToExecute(build)) = prod.response else {
        panic!("BUILD required")
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: build.request_id,
        last_modified_epoch: Some(20),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let dev = request("dev");
    for case in 0..10 {
        let mut unsupported = dev.clone();
        // An unrelated physical base input must never become equivalent.
        match case {
            0 => unsupported.sql = unsupported.sql.replace("raw.input", "other.input"),
            1 => unsupported.default_schema = Some("unapproved".into()),
            2 => {
                unsupported.semantic_extras.remove("duckdb_copy_contract");
            }
            3 => unsupported.sql = "select id from input".into(),
            4 => unsupported.allow_clones = Some(false),
            5 => {
                unsupported.clone_table_properties = Some(qc::TableProperties {
                    hours_to_expiration: Some(1),
                    partition_expiration_days: None,
                })
            }
            6 => unsupported.tables.clear(),
            7 => unsupported.tables[0].last_modified_epoch = None,
            8 => unsupported.tables.push(unsupported.tables[0].clone()),
            _ => unsupported.tables.push(qc::TableModifiedInfo {
                name: "db._dbt_state.unresolved".into(),
                last_modified_epoch: None,
            }),
        }
        let result = sql
            .submit_enriched_sql(unsupported)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response_variant(&result), "ready_to_execute");
    }
    let result = sql
        .submit_enriched_sql(dev.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&result), "ready_to_clone");
    let Some(qc::submit_sql_response::Response::ReadyToClone(clone)) = result.response else {
        panic!("copy required")
    };
    assert_eq!(clone.clone_required_last_modified_epoch, Some(20));
    assert_eq!(clone.clone_source, "\"db\".\"prod\".\"result\"");
    assert_eq!(clone.clone_target, dev.target_table.clone().unwrap());
    assert_eq!(clone.clone_sqls, ["CREATE OR REPLACE TABLE \"db\".\"dev\".\"result\" AS SELECT * FROM \"db\".\"prod\".\"result\""]);
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: clone.request_id,
        last_modified_epoch: Some(30),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut later = dev;
    later.tables.push(qc::TableModifiedInfo {
        name: later.target_table.clone().unwrap(),
        last_modified_epoch: Some(30),
    });
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(later).await.unwrap().into_inner()),
        "skip_execution"
    );
}

#[tokio::test]
async fn failed_sql_copy_records_actual_build_and_becomes_an_ordinary_source() {
    let (addr, _) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);
    let prod = sql
        .submit_enriched_sql(request("prod"))
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToExecute(build)) = prod.response else {
        panic!("BUILD required")
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: build.request_id,
        last_modified_epoch: Some(20),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let result = sql
        .submit_enriched_sql(request("dev"))
        .await
        .unwrap()
        .into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToClone(copy)) = result.response else {
        panic!("copy required")
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: copy.request_id,
        failed_to_clone: true,
        last_modified_epoch: Some(30),
        table_type: Some("DUCKDB CTAS TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut next = request("prod");
    next.target_table = Some("\"db\".\"prod\".\"next\"".into());
    let result = sql.submit_enriched_sql(next).await.unwrap().into_inner();
    let Some(qc::submit_sql_response::Response::ReadyToClone(copy)) = result.response else {
        panic!("ordinary fallback should be a valid source")
    };
    // One-hop copies cannot be sources. Selecting the newest ordinary fallback
    // through the public service proves it retained no false copy provenance.
    assert_eq!(copy.clone_source, "\"db\".\"dev\".\"result\"");
    assert_eq!(copy.clone_required_last_modified_epoch, Some(30));
}
