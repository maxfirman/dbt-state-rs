//! Precise lifecycle retries/rejection at the production public protocol seam.
mod support;
use dbt_state_proto::query_cache as qc;
use support::scenario::{model, outcome, Scenario};
use support::start_server;

const TARGET: &str = "\"warehouse\".\"dev\".\"totals\"";

#[tokio::test]
async fn duckdb_correlated_confirmation_retry_preserves_first_outcome_and_order() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = model(TARGET)
        .dialect("duckdb")
        .catalog("warehouse")
        .schema("dev")
        .sql("select 10::bigint as amount")
        .namespace("isolated-file-generation")
        .own_epoch(Some(10))
        .unique_id("model.recovery.totals")
        .project("recovery")
        .target_name("dev")
        .node_state(|state| {
            state.node_body_hash = Some("body".into());
            state.node_hash = "hash".into();
            state.project_name = "recovery".into();
            state.profile_name = "duckdb".into();
        });
    // A pending BUILD.
    let pending = sc.execute_id(req.clone()).await;
    // Another pending decision still cannot make either result reusable.
    let second = sc.execute_id(req.clone()).await;
    assert_ne!(pending, second);
    let first_results = qc::Struct {
        fields: [(
            "outcome".into(),
            qc::Value {
                kind: Some(qc::value::Kind::StringValue("first".into())),
            },
        )]
        .into(),
    };
    let first = outcome()
        .epoch(10)
        .table()
        .runtime(123)
        .results(first_results.clone());
    let wrong_org = sc
        .org("other")
        .try_confirm(first.clone().confirm(&pending))
        .await
        .unwrap_err();
    assert_eq!(wrong_org.code(), tonic::Code::NotFound);
    sc.execute_id(req.clone()).await;
    let response = sc
        .try_confirm(first.clone().confirm(&pending))
        .await
        .unwrap();
    assert!(response.success);
    assert_eq!(response.request_id, pending);
    // Retry the same request with conflicting physical epoch/type/runtime.
    // First durable outcome must survive, as seen by the subsequent SKIP.
    let conflicting = outcome()
        .epoch(999)
        .table_type("VIEW")
        .runtime(999)
        .results(qc::Struct::default());
    let error = sc
        .try_confirm(conflicting.clone().confirm(&pending))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    // A duplicate confirm must not replace the first physical epoch/type.
    let skip = sc.expect_skip(req.clone()).await;
    assert_eq!(skip.execution_runtime_ms, Some(123));
    assert_eq!(skip.execution_results, Some(first_results));
    // A distinct later outcome supersedes it. Retrying the first confirmation
    // must not refresh confirmed_at and resurrect the old physical state.
    let changed = req.clone().sql("select 20::bigint as amount");
    let latest = sc.execute_id(changed.clone()).await;
    sc.confirm(&latest, first).await;
    let error = sc
        .try_confirm(conflicting.confirm(&pending))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(sc.verdict(changed).await, "skip_execution");
    assert_eq!(sc.verdict(req).await, "ready_to_execute");
}

#[tokio::test]
async fn duckdb_rejected_record_batch_leaves_execution_and_deferral_unpublished() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let req = model(TARGET)
        .dialect("duckdb")
        .catalog("warehouse")
        .schema("dev")
        .sql("select 10::bigint as amount")
        .namespace("isolated-file-generation")
        .own_epoch(Some(10))
        .unique_id("model.recovery.totals")
        .project("recovery")
        .target_name("dev")
        .node_state(|state| {
            state.node_body_hash = Some("body".into());
            state.node_hash = "hash".into();
            state.project_name = "recovery".into();
            state.profile_name = "duckdb".into();
        });
    let record = req.record(outcome().epoch(10).table());
    let resolve = qc::ResolveDeferredRelationsRequest {
        profile_name: "duckdb".into(),
        target_name: "dev".into(),
        project_name: "recovery".into(),
        project_id: Some("recovery".into()),
        node_unique_ids: vec!["model.recovery.totals".into()],
    };
    let rejected = sc
        .record([record.clone(), qc::ExecutionRecord::default()])
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::InvalidArgument);
    assert!(sc.resolve_deferred(resolve.clone()).await.is_empty());
    sc.execute_id(req.clone()).await;
    assert_eq!(sc.record([record]).await.unwrap(), 1);
    assert_eq!(
        sc.resolve_deferred(resolve)
            .await
            .get("model.recovery.totals")
            .map(String::as_str),
        Some(TARGET)
    );
    assert_eq!(sc.verdict(req).await, "skip_execution");
}
