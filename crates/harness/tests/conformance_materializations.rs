//! Captured custom/snapshot reuse plus offline external-view modification safety.
//! A view's own object metadata is distinct from upstream data freshness.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use support::scenario::{outcome, view, Scenario};

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
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let mut exec = ExecutionClient::new(sc.channel());

    let r0: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[0].request.clone()).unwrap();
    // The custom-materialization first build must execute.
    let rid = sc.execute_id(r0).await;
    support::confirm_captured(&mut exec, &entries, submits[0], rid).await;

    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v = sc.verdict(r1).await;
    assert_eq!(
        v, "skip_execution",
        "an unchanged custom-materialization model must be reused (matches the hosted service; \
         the docs' 'never reused' claim does not hold in practice)"
    );
}

/// An externally modified view requires execution even with no upstreams.
#[tokio::test]
async fn externally_modified_view_rebuilds() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;

    let target = "\"DB\".\"PROD\".\"STG_V\"";
    // Views carry ONLY their own target in tables[] (never upstreams).
    let stg_v = |own_epoch: i64| {
        view(target)
            .sql("select 1")
            .namespace("ns")
            .own_epoch(Some(own_epoch))
            .unique_id("model.jaffle.stg_v")
            .body("vh")
    };

    // Build + confirm the view.
    sc.build_with(
        stg_v(100),
        outcome().epoch(100).table_type("VIEW").runtime(10),
    )
    .await;

    // Own-target changes invalidate the object independently of upstream policy.
    let v = sc.verdict(stg_v(9_999_999_999)).await;
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

    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let mut exec = ExecutionClient::new(sc.channel());

    let r0: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[0].request.clone()).unwrap();
    // The snapshot first build executes.
    let rid = sc.execute_id(r0).await;
    support::confirm_captured(&mut exec, &entries, submits[0], rid).await;

    let r1: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[1].request.clone()).unwrap();
    let v = sc.verdict(r1).await;
    assert_eq!(v, "skip_execution", "unchanged snapshot must be reused");
}
