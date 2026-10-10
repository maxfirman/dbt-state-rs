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
//! logic identity is NOT the client-sent `node_body_hash`.
//!
//! VERIFIED MECHANISM (client source + controlled live A/B, not a guess): the
//! service rebuilds iff (a) the WHITESPACE-NORMALIZED rendered `sql` changed, or
//! (b) an allowlisted `semantic_extras` key changed (the client folds a FIXED
//! set — on_schema_change, contract, constraints, unique_key, grants, merge_*,
//! incremental_predicates, event_time, sql_header, lookback, table_format,
//! warehouse keys, __persisted_docs_hash — into semantic_extras), or (c)
//! upstream data is stale. A `config(meta=...)` edit ([2]) changes neither the
//! normalized SQL (only whitespace) nor an allowlisted key, so it SKIPs; a new
//! column ([3]) changes the normalized SQL, so it EXECUTEs. This is NOT a deep
//! semantic/AST fingerprint — just whitespace normalization + a config
//! allowlist, both reproducible without a SQL engine.
//!
//! Our server now matches on exactly that (whitespace-normalized SQL +
//! semantic_extras hash, ignoring node_body_hash), so it REPRODUCES both: it
//! SKIPs the config-only change [2] and EXECUTEs the genuine change [3].
//! (Earlier the match keyed on node_body_hash and over-executed on [2]; that
//! divergence is now eliminated.)

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

    // [2] config-only change (meta): the hosted service skips. With the
    // whitespace-normalized-SQL + semantic_extras match key, we now SKIP too
    // (meta is not an allowlisted semantic_extras key, and the SQL differs only
    // by whitespace) — the divergence is ELIMINATED.
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

    // Both now AGREE with the hosted service:
    //  - [2] config-only (meta) → SKIP (whitespace-normalized SQL unchanged, no
    //    allowlisted semantic_extras change).
    //  - [3] genuine SQL change → EXECUTE.
    assert_eq!(
        v2, "skip_execution",
        "CONFORMANT: a config-only (meta) change skips, matching the hosted service \
         (match key is whitespace-normalized SQL + semantic_extras, not node_body_hash)"
    );
    assert_eq!(
        v3, "ready_to_execute",
        "AGREEMENT: a genuine SQL logic change executes on both"
    );
}
