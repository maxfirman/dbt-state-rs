//! DATA-TEST UPSTREAM FRESHNESS (issue #2, finding 1) — live-captured from
//! api.state.dbt.com (Fusion client, Snowflake;
//! experiments/data_test_freshness{,_control}.sh).
//!
//! A dbt data test (execution_type = 8) produces no physical object, so the
//! client confirms it with `ConfirmExecution.last_modified_epoch = None` (only
//! the test results struct). The hosted service nonetheless invalidates a cached
//! test result when the TESTED (upstream) data changed:
//!
//!   baseline : dt_test submit with DT_UP@735547 -> ready_to_execute
//!              confirm lme=None, results {failures:0, should_error/warn:false}
//!   treatment: dt_up recreated in place (identical content, epoch -> 752144);
//!              dt_test submit with DT_UP@752144  -> ready_to_execute
//!              ("data test was executed because ... its query changed")
//!   control  : dt_test submit with DT_UP@752144 (unchanged) -> skip_execution
//!              ("data test was a no-op because both its query and its upstream
//!               data are up to date")
//!
//! The ONLY difference between the treatment and control re-runs is the upstream
//! epoch, isolating the cause: a data test's freshness anchor is the RECORDED
//! UPSTREAM epoch (it has no own-build epoch), compared per input with strict
//! `>`. Golden fixture: golden/fixtures/data_test_upstream_freshness.jsonl.
//!
//! BUG this pins/guards: the decision engine previously returned
//! should_rebuild=false for a data test with no own-build epoch BEFORE comparing
//! upstream epochs, so it SKIPPED and reused a stale cached pass/fail after the
//! tested data changed. The fix anchors a data test's freshness on the recorded
//! upstream epoch.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use support::scenario::{data_test, outcome, test_results, Scenario};
use support::start_server;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/data_test_upstream_freshness.jsonl"
);

const TEST_UID: &str = "test.jaffle_shop.dt_test.deadbeef01";
const PROJECT: &str = "70437463667433";
const NS: &str = "dt-ns";

const UPSTREAM: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"DT_UP\"";

/// End-to-end: a confirmed-passing data test must EXECUTE after its upstream
/// data changed, and SKIP when the upstream is unchanged.
#[tokio::test]
async fn data_test_reruns_when_upstream_data_changed() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // A data-test (execution_type=8) submit over one upstream `DB.S.DT_UP@epoch`.
    // Data tests carry no physical target.
    let test = data_test(TEST_UID)
        .catalog("CDO_DEMO_DB")
        .schema("PROD_STATE_DEMO")
        .sql("select * from CDO_DEMO_DB.PROD_STATE_DEMO.DT_UP where amount < 0")
        .namespace(NS)
        .project(PROJECT)
        .target_name("prod_demo")
        .body("test-body");
    let test_submit = |upstream_epoch: i64| test.clone().upstream(UPSTREAM, upstream_epoch);

    // Baseline: first build of the test over DT_UP@100, then confirm it with no
    // object epoch, only results (just like the client).
    sc.build_with(test_submit(100), outcome().results(test_results(0)))
        .await;

    // CONTROL: same upstream epoch -> the hosted service SKIPs (reuses the pass).
    assert_eq!(
        sc.verdict(test_submit(100)).await,
        "skip_execution",
        "an unchanged data test (same upstream epoch) must skip"
    );

    // TREATMENT: the upstream data changed (newer epoch) -> must EXECUTE, NOT
    // reuse the stale cached pass. This is the finding-1 fix.
    assert_eq!(
        sc.verdict(test_submit(200)).await,
        "ready_to_execute",
        "a data test must re-run when its tested (upstream) data changed"
    );

    // An older-or-equal upstream (clock skew / restore) is still fresh -> skip.
    assert_eq!(
        sc.verdict(test_submit(50)).await,
        "skip_execution",
        "an older upstream epoch is not drift"
    );
}

/// Pin the hosted truth table directly from the live capture: the treatment
/// submit (upstream newer than recorded) EXECUTES; the control submit (upstream
/// equal to recorded) SKIPS; and the test's confirmation carried no object
/// epoch (last_modified_epoch = null).
#[test]
fn hosted_data_test_freshness_shape_is_pinned() {
    let entries = diff::load_golden(FIXTURE).expect("load data-test freshness fixture");

    // The test's ConfirmExecution carries NO object epoch, only results.
    let test_confirms: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| {
            e.method == "ConfirmExecution"
                && e.request["execution_results"].is_object()
                && e.request["execution_results"]["fields"]["failures"].is_object()
        })
        .collect();
    assert!(
        !test_confirms.is_empty(),
        "fixture has a data-test confirmation with results"
    );
    for c in &test_confirms {
        assert!(
            c.request["last_modified_epoch"].is_null(),
            "a data test is confirmed with no object epoch (last_modified_epoch = null)"
        );
    }

    // Among the data-test (execution_type=8) submits, there is at least one
    // EXECUTE whose upstream epoch is strictly greater than an earlier SKIP/
    // baseline upstream epoch, and the equal-epoch submits SKIP.
    let dt_submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| {
            e.method == "SubmitEnrichedSQL" && e.request["execution_type"].as_i64() == Some(8)
        })
        .collect();
    assert!(
        dt_submits.len() >= 3,
        "baseline + treatment + control submits"
    );

    let upstream_epoch = |e: &diff::GoldenEntry| -> i64 {
        e.request["tables"][0]["last_modified_epoch"]
            .as_i64()
            .expect("upstream epoch")
    };
    let variant = |e: &diff::GoldenEntry| diff::decision_variant(&e.response).unwrap();

    // Collect (epoch, variant) pairs.
    let pairs: Vec<(i64, String)> = dt_submits
        .iter()
        .map(|e| (upstream_epoch(e), variant(e)))
        .collect();

    // There is a strictly-newer-upstream EXECUTE and an equal-upstream SKIP.
    let max_epoch = pairs.iter().map(|(ep, _)| *ep).max().unwrap();
    let min_epoch = pairs.iter().map(|(ep, _)| *ep).min().unwrap();
    assert!(
        max_epoch > min_epoch,
        "the fixture advances the upstream epoch"
    );

    // The submit at the OLD epoch (first build) executed; a submit at the NEW
    // epoch BEFORE the re-confirm executed (invalidation); and a submit at the
    // NEW epoch AFTER the re-confirm skipped (control).
    assert!(
        pairs
            .iter()
            .any(|(ep, v)| *ep == max_epoch && v == "ready_to_execute"),
        "a newer-upstream submit EXECUTES (invalidation)"
    );
    assert!(
        pairs
            .iter()
            .any(|(ep, v)| *ep == max_epoch && v == "skip_execution"),
        "an unchanged (same-epoch, re-confirmed) submit SKIPS (control)"
    );
}
