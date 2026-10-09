//! C1 — CHARACTERIZED DIVERGENCE: config-only change vs. genuine logic change.
//!
//! Captured live from api.state.dbt.com (jaffle-shop `customers` on Snowflake,
//! `golden/fixtures/c1_config_vs_logic.jsonl`). Four real decisions for the
//! same node, in order:
//!
//!   [0] first build          body=0fbde4f2 cfg=ff8f1fb7 -> ready_to_execute
//!   [1] unchanged rebuild     body=0fbde4f2 cfg=ff8f1fb7 -> skip_execution
//!   [2] config-only change    body=a0a8af93 cfg=39ba728a -> skip_execution  (!)
//!   [3] genuine SQL change     body=22e8207a cfg=ff8f1fb7 -> ready_to_execute
//!
//! The decisive observation: `node_body_hash` CHANGED in BOTH [2] and [3], yet
//! the hosted service SKIPPED [2] and EXECUTED [3]. So the hosted service's
//! logic identity is NOT the client-sent `node_body_hash`. It fingerprints the
//! raw `sql` SEMANTICALLY server-side: a `config(meta=...)` edit ([2]) perturbs
//! the client hash but is semantically-equivalent SQL (skip), while a new
//! column ([3]) is a real semantic change (execute).
//!
//! Our server trusts the client's `node_body_hash` as the match key, which is
//! STRICTER than the hosted service's semantic fingerprint. Consequence: we
//! EXECUTE on config-only changes that the hosted service SKIPs — a measured
//! faithfulness divergence. Reproducing it faithfully needs server-side SQL
//! semantic fingerprinting (a SQL engine), which the project deliberately omits
//! (see docs/overview.md "why the server needs no SQL engine").
//!
//! This test PINS the real decisions as a golden contract AND measures our
//! current (divergent) behavior, so the gap is documented, regression-guarded,
//! and quantified rather than hidden. The divergence is SAFE-DIRECTIONAL: we
//! over-execute (never serve stale data), we just skip less than we could.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/c1_config_vs_logic.jsonl"
);

fn body_hash(e: &diff::GoldenEntry) -> String {
    e.request["dbt_node_state"]["node_body_hash"]
        .as_str()
        .unwrap_or("")
        .to_string()
}

#[tokio::test]
async fn c1_config_vs_logic_change_is_characterized() {
    let entries = diff::load_golden(FIXTURE).expect("load c1 fixture");

    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(
        submits.len(),
        4,
        "fixture must hold the 4 customers submits"
    );

    // Pin the REAL hosted-service decisions (the golden contract).
    let real = |i: usize| diff::decision_variant(&submits[i].response).unwrap();
    assert_eq!(real(0), "ready_to_execute", "[0] first build executes");
    assert_eq!(real(1), "skip_execution", "[1] unchanged rebuild skips");
    assert_eq!(
        real(2),
        "skip_execution",
        "[2] config-only change: hosted service SKIPS despite a changed body hash"
    );
    assert_eq!(
        real(3),
        "ready_to_execute",
        "[3] genuine SQL change: hosted service EXECUTES"
    );

    // The body hash changed in BOTH [2] and [3] — proving it is not the hosted
    // service's discriminator.
    assert_eq!(
        body_hash(submits[0]),
        body_hash(submits[1]),
        "[0]==[1] body"
    );
    assert_ne!(
        body_hash(submits[1]),
        body_hash(submits[2]),
        "[2] body changed"
    );
    assert_ne!(
        body_hash(submits[1]),
        body_hash(submits[3]),
        "[3] body changed"
    );
    assert_ne!(
        body_hash(submits[2]),
        body_hash(submits[3]),
        "[2]!=[3] body"
    );

    // Now MEASURE our server: replay [0]+confirm, then [2] (config-only change).
    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let r0: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[0].request.clone()).unwrap();
    let resp0 = sql.submit_enriched_sql(r0).await.unwrap().into_inner();
    let rid = match resp0.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        other => panic!("[0] should execute for us too, got {other:?}"),
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: rid,
        last_modified_epoch: Some(1_791_600_000_000),
        failed_to_clone: false,
        table_type: Some("TABLE".into()),
        execution_results: None,
        execution_runtime_ms: Some(1000),
        labels: Default::default(),
    })
    .await
    .unwrap();

    // [1] unchanged rebuild: we reproduce the skip (same body hash).
    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v1 = support::response_variant(&sql.submit_enriched_sql(r1).await.unwrap().into_inner());
    assert_eq!(
        v1, "skip_execution",
        "we DO reproduce the unchanged-rebuild skip (body hash matches)"
    );

    // [2] config-only change: the hosted service skips; we EXECUTE (divergence).
    let r2: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[2].request.clone()).unwrap();
    let v2 = support::response_variant(&sql.submit_enriched_sql(r2).await.unwrap().into_inner());

    // [3] genuine logic change: both execute (agreement).
    let r3: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[3].request.clone()).unwrap();
    let v3 = support::response_variant(&sql.submit_enriched_sql(r3).await.unwrap().into_inner());

    eprintln!(
        "C1 CHARACTERIZATION: [2] config-only real=skip_execution ours={v2}; \
         [3] logic-change real=ready_to_execute ours={v3}"
    );

    // Document the current behavior precisely:
    //  - [2] is the DIVERGENCE (safe-directional over-execute).
    //  - [3] is AGREEMENT on the genuine logic change.
    assert_eq!(
        v2, "ready_to_execute",
        "DIVERGENCE: we execute a config-only change the hosted service skips \
         (we key on node_body_hash; it fingerprints SQL semantics server-side)"
    );
    assert_eq!(
        v3, "ready_to_execute",
        "AGREEMENT: a genuine SQL logic change executes on both"
    );
}
