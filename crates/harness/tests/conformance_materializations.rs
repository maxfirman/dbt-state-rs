//! Conformance characterizations from the exhaustive live sweep (materializations).
//!
//! These replay real captures from api.state.dbt.com and assert our server
//! reproduces the hosted decision, pinning two behaviours that the official
//! DOCS describe imprecisely:
//!
//!  * VIEW skip-despite-upstream-change — the docs say a view is reused "even if
//!    new data has arrived upstream". Mechanically, the CLIENT achieves this by
//!    sending ONLY the view's own target table in `tables[]` (never upstreams).
//!    Our own-table exclusion then yields `considered == 0` → skip on a logic
//!    match. Verified across all captures: no view submit carries a non-own
//!    upstream. This test pins that a view request with only its own (even
//!    advanced) target still skips.
//!
//!  * CUSTOM MATERIALIZATION reuse — the docs say custom-materialization models
//!    are "always built and never reused". The REAL service DISAGREES: an
//!    unchanged `custom_table` model (execution_type = DBT_CUSTOM = 11) was
//!    SKIPPED on rerun. So treating et=11 like any other node (allowing skip on
//!    an unchanged confirmed match) is the CONFORMANT behaviour; an
//!    always-execute rule would diverge. Pinned by replaying the real capture.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

const CUSTOM_MAT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/custom_materialization_reuse.jsonl"
);
const SNAPSHOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/snapshot_reuse.jsonl"
);

/// Replay the real custom-materialization sequence (et=11): execute → confirm →
/// skip. Our server must reproduce the SKIP on the unchanged rerun, matching the
/// hosted service (NOT force execute as the docs suggest).
#[tokio::test]
async fn custom_materialization_is_reused_like_any_node() {
    let entries = diff::load_golden(CUSTOM_MAT).expect("load custom-mat fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 2, "execute then skip");
    // Both are execution_type = DBT_CUSTOM = 11.
    for s in &submits {
        assert_eq!(
            s.request["execution_type"].as_i64(),
            Some(11),
            "custom materialization is execution_type 11"
        );
    }
    // Hosted contract: execute, then skip on the unchanged rerun.
    assert_eq!(
        diff::decision_variant(&submits[0].response).unwrap(),
        "ready_to_execute"
    );
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "skip_execution"
    );

    // Replay against our server.
    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let r0: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[0].request.clone()).unwrap();
    let resp0 = sql.submit_enriched_sql(r0).await.unwrap().into_inner();
    let rid = match resp0.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        other => panic!("custom-mat first build must execute, got {other:?}"),
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: rid,
        last_modified_epoch: Some(1_791_600_000_000),
        failed_to_clone: false,
        table_type: Some("TABLE".into()),
        execution_results: None,
        execution_runtime_ms: Some(100),
        labels: Default::default(),
    })
    .await
    .unwrap();

    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v = support::response_variant(&sql.submit_enriched_sql(r1).await.unwrap().into_inner());
    assert_eq!(
        v, "skip_execution",
        "an unchanged custom-materialization model must be reused (matches the hosted service; \
         the docs' 'never reused' claim does not hold in practice)"
    );
}

/// A VIEW (execution_type=10) whose request carries ONLY its own target table in
/// `tables[]` (as the real client always sends for views) must skip on a logic
/// match, regardless of the own-target epoch advancing — reproducing the hosted
/// "views reflect new upstream data without a rebuild" behaviour.
#[tokio::test]
async fn view_skips_with_only_own_target_in_tables() {
    use qc::execution_client::ExecutionClient;

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"PROD\".\"STG_V\"";
    let view = |own_epoch: i64| qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.to_string()),
        dialect: "snowflake".to_string(),
        execution_type: 10, // VIEW
        sql: "select 1".to_string(),
        table_namespace: Some("ns".to_string()),
        // Views carry ONLY their own target in tables[] (never upstreams).
        tables: vec![qc::TableModifiedInfo {
            name: target.to_string(),
            last_modified_epoch: Some(own_epoch),
        }],
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.jaffle.stg_v".to_string(),
            node_hash: "vh".to_string(),
            node_body_hash: Some("vh".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Build + confirm the view.
    let r = sql
        .submit_enriched_sql(view(100))
        .await
        .unwrap()
        .into_inner();
    let rid = match r.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        other => panic!("first view build executes, got {other:?}"),
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: rid,
        last_modified_epoch: Some(100),
        failed_to_clone: false,
        table_type: Some("VIEW".into()),
        execution_results: None,
        execution_runtime_ms: Some(10),
        labels: Default::default(),
    })
    .await
    .unwrap();

    // Rerun with the OWN target epoch advanced far beyond any tolerance: the
    // own-table is excluded from freshness, so with no upstreams to compare the
    // view skips on its logic match (mirrors "views reflect new data w/o rebuild").
    let v = support::response_variant(
        &sql.submit_enriched_sql(view(9_999_999_999))
            .await
            .unwrap()
            .into_inner(),
    );
    assert_eq!(
        v, "skip_execution",
        "a view with only its own (advanced) target in tables[] must skip on logic match"
    );
}

/// Snapshot (execution_type = SNAPSHOT = 7): execute → confirm → unchanged skip,
/// reproduced by our generic matching (snapshots are reusable per the docs).
#[tokio::test]
async fn snapshot_is_reused_when_unchanged() {
    let entries = diff::load_golden(SNAPSHOT).expect("load snapshot fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 2, "execute then skip");
    for s in &submits {
        assert_eq!(
            s.request["execution_type"].as_i64(),
            Some(7),
            "SNAPSHOT execution_type"
        );
    }
    assert_eq!(
        diff::decision_variant(&submits[0].response).unwrap(),
        "ready_to_execute"
    );
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "skip_execution"
    );

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let r0: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[0].request.clone()).unwrap();
    let resp0 = sql.submit_enriched_sql(r0).await.unwrap().into_inner();
    let rid = match resp0.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        other => panic!("snapshot first build executes, got {other:?}"),
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: rid,
        last_modified_epoch: Some(1_791_600_000_000),
        failed_to_clone: false,
        table_type: Some("TABLE".into()),
        execution_results: None,
        execution_runtime_ms: Some(10),
        labels: Default::default(),
    })
    .await
    .unwrap();

    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v = support::response_variant(&sql.submit_enriched_sql(r1).await.unwrap().into_inner());
    assert_eq!(v, "skip_execution", "unchanged snapshot must be reused");
}
