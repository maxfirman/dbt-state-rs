//! Conformance characterizations: incremental models + seeds (live captures).
//!
//! Incremental (merge, execution_type = MERGE = 3): first incremental-shaped run
//! executes, then an unchanged rerun with fresh upstream SKIPs. node_body_hash
//! is stable across the is_incremental() branch (the hosted semantic fingerprint
//! treats the extra `where … > max()` filter as equivalent); a unique_key change
//! DOES change the body hash → execute (normal path). `--full-refresh` bypasses
//! dbt State entirely on the client (no submit), so there is nothing to
//! reproduce server-side.
//!
//! Seeds (SubmitValues): matched on values_hash. Build → confirm → unchanged
//! skip; a content change moves values_hash → execute.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

const INCR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/incremental_merge_reuse.jsonl"
);
const SEED: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/seed_values_hash.jsonl"
);

async fn confirm(exec: &mut ExecutionClient<tonic::transport::Channel>, rid: String, epoch: i64) {
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: rid,
        last_modified_epoch: Some(epoch),
        failed_to_clone: false,
        table_type: Some("TABLE".into()),
        execution_results: None,
        execution_runtime_ms: Some(10),
        labels: Default::default(),
    })
    .await
    .unwrap();
}

/// Incremental merge model: replay execute → confirm → unchanged skip and assert
/// our server reproduces the skip (execution_type=3 matched like any node).
#[tokio::test]
async fn incremental_merge_reuse_reproduced() {
    let entries = diff::load_golden(INCR).expect("load incremental fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 2, "execute then skip");
    for s in &submits {
        assert_eq!(
            s.request["execution_type"].as_i64(),
            Some(3),
            "MERGE execution_type"
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
        other => panic!("incremental first build executes, got {other:?}"),
    };
    confirm(&mut exec, rid, 1_791_600_000_000).await;

    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v = support::response_variant(&sql.submit_enriched_sql(r1).await.unwrap().into_inner());
    assert_eq!(v, "skip_execution", "unchanged incremental rerun must skip");
}

/// Seed lifecycle via SubmitValues: build → confirm → unchanged skip, then a
/// content change (new values_hash) → execute.
#[tokio::test]
async fn seed_values_hash_lifecycle_reproduced() {
    let entries = diff::load_golden(SEED).expect("load seed fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitValues")
        .collect();
    assert_eq!(submits.len(), 3, "execute, skip, content-change execute");

    let vhash = |i: usize| {
        submits[i].request["values_hash"]
            .as_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(vhash(0), vhash(1), "first two share values_hash");
    assert_ne!(vhash(1), vhash(2), "content change moves values_hash");

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Build #1 (execute) + confirm.
    let s0: qc::SubmitValuesRequest = serde_json::from_value(submits[0].request.clone()).unwrap();
    let r0 = sql.submit_values(s0).await.unwrap().into_inner();
    let rid = match r0.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        other => panic!("seed first build executes, got {other:?}"),
    };
    confirm(&mut exec, rid, 1_791_600_000_000).await;

    // Build #2 (unchanged) → skip.
    let s1: qc::SubmitValuesRequest = serde_json::from_value(submits[1].request.clone()).unwrap();
    let v1 = support::response_variant(&sql.submit_values(s1).await.unwrap().into_inner());
    assert_eq!(
        v1, "skip_execution",
        "unchanged seed must skip (values_hash match)"
    );

    // Build #3 (content changed) → execute.
    let s2: qc::SubmitValuesRequest = serde_json::from_value(submits[2].request.clone()).unwrap();
    let v2 = support::response_variant(&sql.submit_values(s2).await.unwrap().into_inner());
    assert_eq!(
        v2, "ready_to_execute",
        "changed seed content (new values_hash) must execute"
    );
}
