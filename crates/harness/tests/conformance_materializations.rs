//! Captured custom/snapshot reuse plus offline external-view modification safety.
//! A view's own object metadata is distinct from upstream data freshness.

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
    support::confirm_captured(&mut exec, &entries, submits[0], rid).await;

    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v = support::response_variant(&sql.submit_enriched_sql(r1).await.unwrap().into_inner());
    assert_eq!(
        v, "skip_execution",
        "an unchanged custom-materialization model must be reused (matches the hosted service; \
         the docs' 'never reused' claim does not hold in practice)"
    );
}

/// An externally modified view requires execution even with no upstreams.
#[tokio::test]
async fn externally_modified_view_rebuilds() {
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
        tolerate_nondeterminism: true,
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

    // Own-target changes invalidate the object independently of upstream policy.
    let v = support::response_variant(
        &sql.submit_enriched_sql(view(9_999_999_999))
            .await
            .unwrap()
            .into_inner(),
    );
    assert_eq!(
        v, "ready_to_execute",
        "an externally modified view cannot reuse recorded logic"
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
    support::confirm_captured(&mut exec, &entries, submits[0], rid).await;

    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v = support::response_variant(&sql.submit_enriched_sql(r1).await.unwrap().into_inner());
    assert_eq!(v, "skip_execution", "unchanged snapshot must be reused");
}
