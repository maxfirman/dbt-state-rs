//! CLONE FALLBACK — STALE SOURCE IS TOLERATED (review #2 finding 2 / P0,
//! follow-up). Addresses the follow-up review's methodological critique of the
//! earlier `clone_fallback_xenv_fresh.jsonl`: that fixture did not capture the
//! cloned SOURCE's own provenance (its build epoch and the upstream it was built
//! against), so it could not isolate whether a STALE source is clone-eligible.
//!
//! This capture (experiments/clone_eligibility.sh stale_tol via
//! clone_fallback_xenv.sh stale; golden/fixtures/clone_fallback_stale_source.jsonl)
//! is self-contained and records the full causal chain:
//!
//!   1. prod SUBMIT of the source model over CF2_UP@1791823729803 ->
//!      ready_to_execute; CONFIRM lme=1791823732549  (the SOURCE's build epoch).
//!   2. prod upstream is bumped in place (its epoch advances past the source's
//!      build epoch), then the dev SUBMIT over CF2_UP@1791823799116 (NEWER than
//!      the source build 1791823732549 by ~66s) -> ready_to_clone from the
//!      source, clone_required_last_modified_epoch=1791823732549.
//!
//! So the clone SOURCE's data (built at 1791823732549) is STALE relative to the
//! dev submit's CURRENT upstream (1791823799116), and the hosted service STILL
//! clones it ("Cloned from other environment within tolerance"). Hosted
//! TOLERATES source staleness on this axis; it does not freshness-gate the
//! clone candidate.
//!
//! DETERMINATION (docs/correctness-handoff.md §B finding 2): the server's
//! `find_clone_equivalent` also clones here, so it MATCHES hosted — it is NOT
//! over-conservative. Adding the review's implied freshness GATE would make the
//! server reuse LESS than hosted (a conformance regression), so the fallback is
//! intentionally NOT tightened. The exact clone tolerance window and the
//! (timing-sensitive, non-reproducible) OUTSIDE_TIME_TRAVEL_WINDOW rejection
//! remain OPEN — they need a controlled adapter-level capture that this Fusion
//! version does not expose (lag_tolerance / evaluate_volatile_sql are rejected
//! as model-config keys, so per-model tolerance variation is not
//! client-reachable here).

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, start_server};

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/clone_fallback_stale_source.jsonl"
);

/// Pin the hosted truth AND the captured source provenance: the clone source's
/// build epoch is strictly OLDER than the dev submit's current upstream epoch,
/// and hosted still returns ready_to_clone.
#[test]
fn hosted_clones_stale_source_with_captured_provenance() {
    let entries = diff::load_golden(FIXTURE).expect("load stale-source fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    let confirms: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "ConfirmExecution")
        .collect();
    assert_eq!(submits.len(), 2, "prod source build + dev clone");
    assert!(!confirms.is_empty(), "the source confirm is captured");

    // (1) The prod source build executes and confirms with its build epoch.
    assert_eq!(
        diff::decision_variant(&submits[0].response).unwrap(),
        "ready_to_execute",
        "the source builds first in prod"
    );
    let source_build_epoch = confirms[0].request["last_modified_epoch"]
        .as_i64()
        .expect("the source's build epoch is captured in its confirm");

    // (2) The dev submit's current upstream epoch is NEWER than the source build
    // epoch — i.e. the source is STALE relative to the dev's upstream.
    let dev_upstream_epoch = submits[1].request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .expect("dev submit upstream epoch");
    assert!(
        dev_upstream_epoch > source_build_epoch,
        "the clone source (built @{source_build_epoch}) is STALE relative to the \
         dev submit's current upstream (@{dev_upstream_epoch})"
    );

    // (3) Hosted STILL clones the stale source.
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "ready_to_clone",
        "hosted tolerates source staleness and still clones"
    );
    let clone = &submits[1].response["response"]["ready_to_clone"];
    assert_eq!(
        clone["clone_required_last_modified_epoch"].as_i64(),
        Some(source_build_epoch),
        "clone_required_last_modified_epoch equals the stale source's build epoch"
    );
    assert_eq!(
        clone["explained_decision"]["skip_rejection_reason"].as_i64(),
        Some(1),
        "srr=1 TARGET_TABLE_MISMATCH"
    );
}

/// Server reproduction: the server MATCHES hosted — it clones a cross-name
/// equivalent whose build epoch is OLDER than the submit's current upstream
/// (does not freshness-gate the clone candidate). This guards against a
/// regression that would silently tighten `find_clone_equivalent` and reuse
/// LESS than hosted.
#[tokio::test]
async fn server_clones_stale_cross_name_equivalent_like_hosted() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    const NS: &str = "stale-src-ns";
    let source = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"STALE_SRC\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"STALE_SRC\"";
    let up_old = 1_000i64;
    let up_new = 2_000i64; // dev upstream newer than the source build

    let submit = |target: &str, schema: &str, up_epoch: i64| qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: "snowflake".into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: Some(schema.into()),
        execution_type: 1,
        sql: format!("select id, amount from CDO_DEMO_DB.{schema}.stale_up"),
        tolerate_nondeterminism: true,
        freshness_tolerance_seconds: 2700,
        tables: vec![qc::TableModifiedInfo {
            name: format!("\"CDO_DEMO_DB\".\"{}\".\"STALE_UP\"", schema.to_uppercase()),
            last_modified_epoch: Some(up_epoch),
        }],
        query_dependencies: vec![qc::QueryDependency {
            name: format!("\"CDO_DEMO_DB\".\"{}\".\"STALE_UP\"", schema.to_uppercase()),
            query: "select 1 as id, 100 as amount".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            default_schema: schema.into(),
        }],
        table_namespace: Some(NS.into()),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.jaffle.stale_src".into(),
            project_id: Some("p".into()),
            node_body_hash: Some("body".into()),
            ..Default::default()
        }),
        labels: [("dbt_node_name".to_string(), "stale_src".to_string())]
            .into_iter()
            .collect(),
        ..Default::default()
    };

    // Build + confirm the source in prod against the OLD upstream.
    let r0 = sql
        .submit_enriched_sql(submit(source, "prod_state_demo", up_old))
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("source must execute");
    };
    let source_build_epoch = 1_500i64; // > up_old, as a real build would be
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(source_build_epoch),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(100),
        ..Default::default()
    })
    .await
    .unwrap();

    // Dev submit of an absent target whose CURRENT upstream (up_new) is NEWER
    // than the source's build epoch — the source is stale. The server must
    // STILL clone (matching hosted), not execute.
    let rb = sql
        .submit_enriched_sql(submit(dev, "dev_clone_demo", up_new))
        .await
        .unwrap()
        .into_inner();
    let variant = support::response_variant(&rb);
    assert_eq!(
        variant, "ready_to_clone",
        "the server clones a stale cross-name equivalent, matching hosted (no \
         speculative freshness gate on the clone candidate)"
    );
}
