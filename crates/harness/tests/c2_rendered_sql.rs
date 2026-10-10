//! C2 — RENDERED SQL CHANGE (live-captured). The UNSAFE-direction divergence.
//!
//! Captured from api.state.dbt.com: a model `select '{{ env_var("C1_RUN_ID") }}'
//! …`. The client's `node_body_hash` is the UNRENDERED template hash, so it is
//! IDENTICAL across env_var values (`dd10f0f4` for both run1 and run2). But the
//! hosted service compares the RENDERED SQL (default `compare_unrendered_code =
//! false`) and so:
//!   run1 (sql="select 'run1' …")  -> execute -> confirm
//!   run2 (sql="select 'run2' …", SAME body hash) -> EXECUTE (rendered changed)
//!   run2 again (same sql)         -> skip
//!
//! Our server matched only on node_body_hash, so after run1 confirmed it would
//! SKIP run2 — serving the stale run1 output. That is the UNSAFE direction
//! (under-execution). The fix incorporates a hash of the raw `sql` into the
//! match key so a changed rendered SQL forces a rebuild. (This can over-execute
//! on purely-cosmetic SQL edits that the hosted semantic fingerprint would skip
//! — the safe direction, consistent with the documented C1 stance.)

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/rendered_sql_change.jsonl"
);

#[tokio::test]
async fn rendered_sql_change_forces_execute() {
    let entries = diff::load_golden(FIXTURE).expect("load rendered-sql fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 3, "run1, run2, run2-again");

    // Prove the mechanism: identical body hash, different raw sql.
    let body = |i: usize| {
        submits[i].request["dbt_node_state"]["node_body_hash"]
            .as_str()
            .unwrap()
    };
    let sql = |i: usize| submits[i].request["sql"].as_str().unwrap();
    assert_eq!(
        body(0),
        body(1),
        "node_body_hash identical across env_var change"
    );
    assert_ne!(
        sql(0),
        sql(1),
        "raw rendered SQL differs across env_var change"
    );

    // Hosted contract.
    assert_eq!(
        diff::decision_variant(&submits[0].response).unwrap(),
        "ready_to_execute"
    );
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "ready_to_execute"
    );
    assert_eq!(
        diff::decision_variant(&submits[2].response).unwrap(),
        "skip_execution"
    );

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql_c = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // run1 execute + confirm.
    let r0: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[0].request.clone()).unwrap();
    let resp0 = sql_c.submit_enriched_sql(r0).await.unwrap().into_inner();
    let rid = match resp0.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        other => panic!("run1 executes, got {other:?}"),
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

    // run2: SAME body hash, DIFFERENT rendered SQL → MUST execute (not skip).
    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v1 = support::response_variant(&sql_c.submit_enriched_sql(r1).await.unwrap().into_inner());
    assert_eq!(
        v1, "ready_to_execute",
        "a changed rendered SQL (same node_body_hash) must execute — skipping would \
         serve stale output (unsafe)"
    );
}
