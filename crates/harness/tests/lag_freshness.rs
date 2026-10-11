//! LAG / FRESHNESS CHARACTERIZATION — verified live against api.state.dbt.com
//! (dbt Fusion client, Snowflake, target `prod_demo`) via the recording proxy.
//!
//! Fixture `golden/fixtures/lag_freshness_own_build.jsonl` captures a model
//! `lp_model` (select * from lp_upstream) with `lag_tolerance = 5m`. The
//! upstream `lp_upstream` physical table was recreated IN PLACE with identical
//! content (`create or replace table … as select 100 as id, 'stable' as val`),
//! which advances its warehouse modification epoch WITHOUT changing any dbt
//! model SQL. That isolates pure DATA freshness from the upstream-logic-change
//! bypass (which rebuilds regardless of tolerance).
//!
//! Captured decisions (lp_model submits):
//!   [3] own-build=22:56:19  upstream=22:55:30 (upstream < own) -> skip_execution
//!   [5] own-build=22:56:19  upstream=16:49:48 (upstream > own) -> ready_to_execute
//!   [6] ConfirmExecution    (records the new own-build epoch)
//!   [9] own-build=16:50:18  upstream=16:49:48 (upstream < own) -> skip_execution
//!
//! KEY FINDING (contradicts the lag_tolerance doc PROSE and the pre-hardening
//! review's "wall-clock build-age gate" hypothesis): the service rebuilds iff
//!   max_upstream_epoch > own_last_build_epoch
//! comparing each upstream against the node's OWN last-build epoch — NOT against
//! the upstream epoch recorded at that build, and NOT against wall clock. A
//! non-zero `freshness_tolerance_seconds` did NOT absorb the gap: entry [5]
//! rebuilt with a 300s (5m) tolerance while the upstream was only ~tens of
//! seconds newer than the build, and `is_stale=false` on every entry
//! (staleness is a separate axis). See docs/correctness-handoff.md section C.
//!
//! The previous implementation compared `current_upstream > recorded_upstream +
//! tolerance`, which could SKIP here (small upstream delta within tolerance) and
//! serve data staler than the hosted service — the UNSAFE direction. The engine
//! now anchors on the own-build epoch (decision.rs::is_stale).

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/lag_freshness_own_build.jsonl"
);

fn own_and_upstream(req: &serde_json::Value) -> (Option<i64>, Option<i64>) {
    let mut own = None;
    let mut up = None;
    for t in req["tables"].as_array().into_iter().flatten() {
        let name = t["name"].as_str().unwrap_or("");
        let epoch = t["last_modified_epoch"].as_i64();
        if name.to_ascii_uppercase().contains("LP_MODEL") {
            own = epoch;
        } else if name.to_ascii_uppercase().contains("LP_UPSTREAM") {
            up = epoch;
        }
    }
    (own, up)
}

/// The hosted contract: upstream newer than own-build rebuilds; upstream older
/// than own-build (even with a 5m tolerance) skips. Pure fixture inspection, no
/// server/DB required.
#[test]
fn hosted_rebuilds_iff_upstream_newer_than_own_build() {
    let entries = diff::load_golden(FIXTURE).expect("load lag fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .filter(|e| e.request["labels"]["dbt_node_name"].as_str() == Some("lp_model"))
        .collect();
    assert_eq!(
        submits.len(),
        3,
        "three lp_model submits: skip, execute, skip"
    );

    for s in &submits {
        let tol = s.request["freshness_tolerance_seconds"]
            .as_i64()
            .unwrap_or(0);
        assert_eq!(tol, 300, "lag_tolerance=5m -> 300s on the wire");
        let (own, up) = own_and_upstream(&s.request);
        let decision = diff::decision_variant(&s.response).unwrap();
        let (own, up) = (own.expect("own epoch"), up.expect("upstream epoch"));
        if up > own {
            assert_eq!(
                decision, "ready_to_execute",
                "upstream newer than own build must rebuild (tolerance does not absorb)"
            );
        } else {
            assert_eq!(
                decision, "skip_execution",
                "upstream not newer than own build must skip"
            );
        }
    }
}

/// Replay the decisive execute entry against our server: with a confirmed row
/// whose own-build epoch is OLDER than the current upstream epoch, the engine
/// must EXECUTE — reproducing the hosted rebuild and not skipping on a
/// within-tolerance upstream delta.
#[tokio::test]
async fn server_reproduces_rebuild_on_upstream_newer_than_build() {
    let entries = diff::load_golden(FIXTURE).expect("load lag fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .filter(|e| e.request["labels"]["dbt_node_name"].as_str() == Some("lp_model"))
        .collect();

    // submits[1] is the decisive execute (upstream > own-build); submits[2] the
    // post-confirm skip (upstream < new own-build).
    let (own1, up1) = own_and_upstream(&submits[1].request);
    assert!(
        up1.unwrap() > own1.unwrap(),
        "fixture precondition: decisive entry has upstream newer than own build"
    );
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "ready_to_execute"
    );

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Establish a confirmed baseline from the decisive submit: execute it, then
    // confirm with the captured outcome (own-build epoch). A later re-submit
    // whose upstream is NOT newer than that build must then skip.
    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let resp1 = sql.submit_enriched_sql(r1).await.unwrap().into_inner();
    let rid = match resp1.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        other => panic!("first submit with no prior history must execute, got {other:?}"),
    };
    support::confirm_captured(&mut exec, &entries, submits[1], rid).await;

    // The post-confirm skip entry: upstream older than the new own-build → skip.
    let r2: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[2].request.clone()).unwrap();
    let v2 = support::response_variant(&sql.submit_enriched_sql(r2).await.unwrap().into_inner());
    assert_eq!(
        v2, "skip_execution",
        "upstream older than own build must skip (reproduces hosted [9])"
    );
}
