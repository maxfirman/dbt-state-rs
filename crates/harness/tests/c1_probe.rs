//! Captured config-only and SQL changes. The hosted skips remain golden evidence;
//! our exact dependency-definition fingerprint conservatively rebuilds when
//! SELECT dependency text changes to view DDL. See correctness-handoff.md B/C.
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
    support::confirm_captured(&mut exec, &entries, submits[0], rid).await;

    // [1] hosted skips; changed dependency representation conservatively builds.
    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v1 = support::response_variant(&sql.submit_enriched_sql(r1).await.unwrap().into_inner());
    assert_eq!(
        v1, "ready_to_execute",
        "dependency representation changes conservatively rebuild"
    );

    // [2] retains the same dependency representation gap.
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

    // Pin the local conservative fallback separately from hosted agreement.
    assert_eq!(
        v2, "ready_to_execute",
        "Known dependency representation gap: hosted skips; local builds"
    );
    assert_eq!(
        v3, "ready_to_execute",
        "AGREEMENT: a genuine SQL logic change executes on both"
    );
}
