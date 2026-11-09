//! Compile-time incremental initialization through the production gRPC router.
mod support;
use dbt_state_proto::query_cache as qc;
use support::scenario::{
    clone_request, duckdb_ctas_at, duckdb_model, ExecutionType, Scenario, DUCKDB_CTAS_TABLE,
    DUCKDB_INCREMENTAL_CONTRACT,
};
use support::start_server;

const PROD: &str = "\"db\".\"prod\".\"result\"";
const DEV: &str = "\"db\".\"dev\".\"result\"";
const NAMESPACE: &str = "incremental-file";
const IDENTITY_LABEL: &str = "duckdb_incremental_identity";
const IDENTITY: &str = "duckdb-incremental-init-v2:test";

#[tokio::test]
async fn incremental_register_requires_current_source_and_adopts_only_matching_logic() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |schema: &str| {
        duckdb_model(schema)
            .namespace(NAMESPACE)
            .sql("select id, amount from db.raw.input")
            .execution_type(ExecutionType::DeleteInsert)
            .extra(DUCKDB_INCREMENTAL_CONTRACT.0, DUCKDB_INCREMENTAL_CONTRACT.1)
            .node_state(|state| state.node_hash = IDENTITY.into())
    };
    let register = clone_request(PROD, DEV)
        .dialect("duckdb")
        .catalog("db")
        .execution_type(ExecutionType::DeleteInsert)
        .namespace(NAMESPACE)
        .source_epoch(20)
        .source_table_type(DUCKDB_CTAS_TABLE)
        .chain_depth_limit(1)
        .label(DUCKDB_INCREMENTAL_CONTRACT.0, DUCKDB_INCREMENTAL_CONTRACT.1)
        .label(IDENTITY_LABEL, IDENTITY);
    assert!(sc
        .register_clone(register.clone())
        .await
        .unwrap()
        .unable_to_clone
        .is_some());
    let build = sc.execute_id(submit("prod")).await;
    // Pending intent never qualifies as a source.
    assert!(sc
        .register_clone(register.clone())
        .await
        .unwrap()
        .unable_to_clone
        .is_some());
    sc.confirm(&build, duckdb_ctas_at(20)).await;
    for case in 0..9 {
        let bad = match case {
            0 => register
                .clone()
                .with(|req| req.clone_source_last_modified_epoch = None),
            1 => register.clone().source_epoch(19),
            2 => register.clone().source_epoch(21),
            3 => register.clone().namespace("different-file"),
            4 => register.clone().execution_type(ExecutionType::Merge),
            5 => register.clone().source_table_type("TABLE"),
            6 => register.clone().chain_depth_limit(0),
            7 => register
                .clone()
                .label(IDENTITY_LABEL, "duckdb-incremental-init-v2:changed"),
            _ => register.clone().with(|req| req.labels.clear()),
        };
        assert!(
            sc.register_clone(bad)
                .await
                .unwrap()
                .unable_to_clone
                .is_some(),
            "case {case}"
        );
    }
    let response = sc.register_clone(register.clone()).await.unwrap();
    let clone = response.ready_to_clone.clone().unwrap();
    assert_eq!(clone.clone_required_last_modified_epoch, Some(20));
    #[allow(deprecated)]
    {
        assert_eq!(
            response.response,
            Some(qc::clone_response::Response::ReadyToCloneV1(clone.clone()))
        );
    }
    sc.confirm(&clone.request_id, duckdb_ctas_at(30)).await;
    let later = submit("dev").table(DEV, Some(30));
    assert_eq!(sc.verdict(later.clone()).await, "skip_execution");
    let later = later.sql("select id, amount+1 as amount from db.raw.input");
    assert_eq!(sc.verdict(later).await, "ready_to_execute");
    // A later incompatible physical write hides the earlier eligible ET5 row.
    let overwrite = submit("prod")
        .execution_type(ExecutionType::Full)
        .allow_clones(false);
    sc.build_with(overwrite, duckdb_ctas_at(40)).await;
    assert!(sc
        .register_clone(register)
        .await
        .unwrap()
        .unable_to_clone
        .is_some());
}

#[tokio::test]
async fn legacy_incremental_sources_cannot_initialize_new_targets() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let legacy = duckdb_model("prod")
        .namespace(NAMESPACE)
        .sql("select id, amount from db.raw.input")
        .execution_type(ExecutionType::DeleteInsert)
        .extra(DUCKDB_INCREMENTAL_CONTRACT.0, DUCKDB_INCREMENTAL_CONTRACT.1)
        .node_state(|state| state.node_hash = "duckdb-incremental-init-v1:test".into());
    sc.build_with(legacy, duckdb_ctas_at(20)).await;
    let register = clone_request(PROD, DEV)
        .dialect("duckdb")
        .catalog("db")
        .execution_type(ExecutionType::DeleteInsert)
        .namespace(NAMESPACE)
        .source_epoch(20)
        .source_table_type(DUCKDB_CTAS_TABLE)
        .chain_depth_limit(1)
        .label(DUCKDB_INCREMENTAL_CONTRACT.0, DUCKDB_INCREMENTAL_CONTRACT.1)
        .label(IDENTITY_LABEL, IDENTITY);
    let request = register
        .clone()
        .label(IDENTITY_LABEL, "duckdb-incremental-init-v1:test");
    assert!(sc
        .register_clone(request)
        .await
        .unwrap()
        .unable_to_clone
        .is_some());
    assert!(sc
        .register_clone(register)
        .await
        .unwrap()
        .unable_to_clone
        .is_some());
}
