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
//! DETERMINATION, REVISED (#264): the dev submit reads DEV_CLONE_DEMO.CF2_UP,
//! which no recorded build or copy produced, and the source read
//! PROD_STATE_DEMO.CF2_UP. Nothing shows the source's result was computed from
//! the dev input, so the server builds; the capture stays a keyed replay gap
//! (golden/replay_gaps.json). Newer hosted captures build this shape too
//! (copy_source_choice.jsonl entries 10 and 78, #239;
//! explain_upstream_origin_*.jsonl cases C and D, #237), and a native run in
//! which the dev input held other rows served the prod rows from the copy
//! (docs/reports/snowflake-upstream-origin-2026-11-22.md, case D). A
//! same-input equivalent is judged for freshness like the reuse candidate
//! (lag_copy_candidates `sne`). The exact clone tolerance window and the
//! (timing-sensitive, non-reproducible) OUTSIDE_TIME_TRAVEL_WINDOW rejection
//! remain OPEN — they need a controlled adapter-level capture that this Fusion
//! version does not expose (lag_tolerance / evaluate_volatile_sql are rejected
//! as model-config keys, so per-model tolerance variation is not
//! client-reachable here).

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use support::demo::{self, transient_at, DEV_SCHEMA, PROD_SCHEMA};
use support::scenario::Scenario;
use support::start_server;

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

/// Server reproduction of the 2026-10 capture: a cross-name equivalent that
/// read the prod upstream is not copied for a dev submit over the dev
/// upstream, whatever their epochs (#264). Hosted copied it then
/// (`clone_fallback_stale_source.jsonl` entry 2, a keyed replay gap).
#[tokio::test]
async fn server_builds_where_hosted_copied_over_another_dev_input() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    const NS: &str = "stale-src-ns";
    let source = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"STALE_SRC\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"STALE_SRC\"";
    let up_old = 1_000i64;
    let up_new = 2_000i64; // dev upstream newer than the source build

    let submit = |target: &str, schema: &str, up_epoch: i64| {
        let upstream = format!("\"CDO_DEMO_DB\".\"{}\".\"STALE_UP\"", schema.to_uppercase());
        demo::model(target, schema, "model.jaffle.stale_src")
            .sql(&format!(
                "select id, amount from CDO_DEMO_DB.{schema}.stale_up"
            ))
            .upstream(&upstream, up_epoch)
            .dependency_in(
                &upstream,
                "select 1 as id, 100 as amount",
                demo::CATALOG,
                schema,
            )
            .namespace(NS)
    };

    // Build + confirm the source in prod against the OLD upstream.
    let source_build_epoch = 1_500i64; // > up_old, as a real build would be
    sc.build_with(
        submit(source, PROD_SCHEMA, up_old),
        transient_at(source_build_epoch).runtime(100),
    )
    .await;

    // Dev submit of an absent target over the dev upstream, which the source
    // did not read: the source's result says nothing about it.
    let rb = sc.submit(submit(dev, DEV_SCHEMA, up_new)).await;
    let variant = support::response_variant(&rb);
    assert_eq!(
        variant, "ready_to_execute",
        "an equivalent that read another upstream is not copied"
    );
}

/// CLONE ELIGIBILITY under ZERO tolerance and VOLATILE evaluation (review #2 2nd
/// + 3rd follow-up). The nested State config
/// (`state={'lag_tolerance':'0s','evaluate_volatile_sql':true}`) is reachable in
/// dbt 2.0.6; these independent dimensions are characterized with a wire-field
/// VARCHECK asserting the flag actually reached the request.
///
/// LIVE RESULTS (experiments/clone_eligibility.sh):
///   * ZERO tolerance (`freshness_tolerance_seconds=0`, deterministic model): a
///     STALE source STILL returns `ready_to_clone` — zero tolerance does NOT
///     gate the cross-name clone (golden/fixtures/clone_fallback_zero_tolerance.jsonl).
///   * VOLATILE (`tolerate_nondeterminism=false`, GENUINELY volatile model with
///     `current_timestamp()`): hosted does NOT clone — it EXECUTES (srr=6)
///     (golden/fixtures/clone_fallback_volatile.jsonl). But a DETERMINISTIC
///     model carrying the SAME flag (tnd=false) is STILL cloned
///     (experiments/clone_eligibility.sh volatile_det). So the server suppresses
///     the cross-name fallback only when the SQL ACTUALLY contains a volatile
///     function (sql_norm::sql_contains_volatile_function), NOT for every
///     tnd=false request — review #2 4th follow-up P1 #3 corrected the earlier
///     blanket gate.
///
/// REVISED (#264): the zero-tolerance capture's dev submit read the dev
/// upstream, which the copied source did not read; the server builds it (a
/// keyed replay gap). Over the same upstream, a stale equivalent is judged
/// like the reuse candidate, so zero tolerance rebuilds it (lag_tolerance.rs
/// covers the copy within tolerance).
#[tokio::test]
async fn zero_tolerance_rebuilds_a_stale_equivalent_over_the_same_input() {
    assert_eq!(
        clone_eligibility_variant(0, true, false, DevInput::OwnNewer).await,
        "ready_to_execute",
        "an equivalent over another dev input is not copied (#264)"
    );
    assert_eq!(
        clone_eligibility_variant(0, true, false, DevInput::ProdNewer).await,
        "ready_to_execute",
        "zero tolerance rebuilds a stale same-input equivalent"
    );
}

#[tokio::test]
async fn volatile_sql_executes_but_deterministic_clones_under_nondeterminism_flag() {
    // GENUINE volatile SQL (current_timestamp) + tnd=false → EXECUTE: cloning
    // would serve a stale evaluated value (verified live,
    // golden/fixtures/clone_fallback_volatile.jsonl).
    assert_eq!(
        clone_eligibility_variant(2700, false, true, DevInput::ProdCurrent).await,
        "ready_to_execute",
        "a genuinely volatile model must EXECUTE under tolerate_nondeterminism=false"
    );
    // DETERMINISTIC SQL + tnd=false → still CLONE the equivalent: the flag
    // alone does NOT suppress the clone (verified live,
    // experiments/clone_eligibility.sh volatile_det). This is the cell the
    // reviewer flagged — the earlier synthetic test used deterministic SQL yet
    // asserted EXECUTE, conflating the flag with genuine volatility.
    assert_eq!(
        clone_eligibility_variant(2700, false, false, DevInput::ProdCurrent).await,
        "ready_to_clone",
        "a deterministic model clones the equivalent even with \
         tolerate_nondeterminism=false (flag alone does not gate the clone)"
    );
}

/// Which upstream the dev submit of [`clone_eligibility_variant`] reads.
#[derive(Clone, Copy)]
enum DevInput {
    /// The dev upstream, at a newer epoch than the source read prod's.
    OwnNewer,
    /// The prod upstream the source read, at the epoch it read.
    ProdCurrent,
    /// The prod upstream the source read, changed since the source's build.
    ProdNewer,
}

/// Build a prod source over the prod upstream at 1000, confirm it at 1500,
/// then submit an absent dev target reading `dev_input` with the given
/// freshness_tolerance_seconds / tolerate_nondeterminism and (optionally) a
/// GENUINELY volatile projection, returning the decision variant. The
/// cross-name clone fallback is the path under test: with no same-target
/// history the dev submit either clones the prod equivalent or executes.
async fn clone_eligibility_variant(
    freshness_tolerance_seconds: i64,
    tnd: bool,
    volatile_sql: bool,
    dev_input: DevInput,
) -> String {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    const NS: &str = "elig-ns";
    let source = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ELIG\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"ELIG\"";

    // `target` in `schema`, reading the ELIG_UP of `input_schema` at `up_epoch`.
    let submit = |target: &str, schema: &str, input_schema: &str, up_epoch: i64| {
        let upstream = format!(
            "\"CDO_DEMO_DB\".\"{}\".\"ELIG_UP\"",
            input_schema.to_uppercase()
        );
        let sql = if volatile_sql {
            format!("select id, amount, current_timestamp() as loaded_at from CDO_DEMO_DB.{input_schema}.elig_up")
        } else {
            format!("select id, amount from CDO_DEMO_DB.{input_schema}.elig_up")
        };
        demo::model(target, schema, "model.jaffle.elig")
            .sql(&sql)
            .tolerate_nondeterminism(tnd)
            .tolerance_seconds(freshness_tolerance_seconds)
            .upstream(&upstream, up_epoch)
            .dependency_in(
                &upstream,
                "select 1 as id, 100 as amount",
                demo::CATALOG,
                input_schema,
            )
            .namespace(NS)
    };

    sc.build_with(
        submit(source, PROD_SCHEMA, PROD_SCHEMA, 1_000),
        transient_at(1_500).runtime(100),
    )
    .await;

    let dev_submit = match dev_input {
        DevInput::OwnNewer => submit(dev, DEV_SCHEMA, DEV_SCHEMA, 2_000),
        DevInput::ProdCurrent => submit(dev, DEV_SCHEMA, PROD_SCHEMA, 1_000),
        DevInput::ProdNewer => submit(dev, DEV_SCHEMA, PROD_SCHEMA, 2_000),
    };
    let rb = sc.submit(dev_submit).await;
    support::response_variant(&rb).to_string()
}

/// Pin the hosted truth for the ZERO-tolerance and VOLATILE arms directly from
/// their committed captures (review #2 3rd follow-up P1 #3 — the arms must be
/// backed by replayable fixtures, not just synthetic requests).
#[test]
fn hosted_zero_tolerance_clones_stale_source_shape_is_pinned() {
    const F: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_fallback_zero_tolerance.jsonl"
    );
    let entries = diff::load_golden(F).expect("load zero-tol fixture");
    let dev = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .find(|e| e.request["default_schema"].as_str() == Some("dev_clone_demo"))
        .expect("dev submit");
    assert_eq!(
        dev.request["freshness_tolerance_seconds"].as_i64(),
        Some(0),
        "the dev submit carries freshness_tolerance_seconds=0 (zero tolerance)"
    );
    assert_eq!(
        diff::decision_variant(&dev.response).as_deref(),
        Some("ready_to_clone"),
        "zero tolerance still clones a stale source (hosted truth)"
    );
    // The cloned source is NEWER-than? No — assert the staleness: the clone
    // source epoch is OLDER than the dev submit's current upstream.
    let clone = &dev.response["response"]["ready_to_clone"];
    let src_epoch = clone["clone_required_last_modified_epoch"]
        .as_i64()
        .unwrap();
    let up = dev.request["tables"][0]["last_modified_epoch"]
        .as_i64()
        .unwrap();
    assert!(
        up > src_epoch,
        "upstream (@{up}) is newer than the clone source (@{src_epoch})"
    );
}

#[test]
fn hosted_volatile_executes_not_clones_shape_is_pinned() {
    const F: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_fallback_volatile.jsonl"
    );
    let entries = diff::load_golden(F).expect("load volatile fixture");
    let dev = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .find(|e| e.request["default_schema"].as_str() == Some("dev_clone_demo"))
        .expect("dev submit");
    assert_eq!(
        dev.request["tolerate_nondeterminism"].as_bool(),
        Some(false),
        "the dev submit carries tolerate_nondeterminism=false (evaluate_volatile_sql)"
    );
    assert!(
        dev.request["sql"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("current_timestamp"),
        "the model uses a GENUINE volatile function"
    );
    assert_eq!(
        diff::decision_variant(&dev.response).as_deref(),
        Some("ready_to_execute"),
        "a volatile model is NOT cloned — hosted executes (the clone would serve a \
         different evaluated value)"
    );
}
