//! CROSS-ENVIRONMENT CLONE FALLBACK + abstraction aggressiveness
//! (review #2 findings 2a / 3) — live-captured from api.state.dbt.com (Fusion
//! client, Snowflake; experiments/clone_fallback_xenv.sh, dev_clone with
//! `defer_to_target: prod_demo`, UNIQUE per-run names).
//!
//! CHARACTERIZATION ONLY — no server behavior is asserted against the local
//! implementation here; this pins what the HOSTED service does so a future fix
//! has a captured oracle.
//!
//! What the capture establishes (and why no server change was made):
//!   * The SQL-submit cross-name/cross-schema clone fallback DOES fire in the
//!     cross-environment dev_clone flow: a dev build of an absent target returns
//!     `ready_to_clone` (srr=1 TARGET_TABLE_MISMATCH, "an equivalent model
//!     exists under another name so we cloned that one") cloning the equivalent
//!     built under another name/schema, with `clone_required_last_modified_epoch`
//!     = the source's build epoch.
//!   * The equivalent it clones from references a DIFFERENT upstream TABLE name
//!     than the submit (cf2_up_g* vs cf2_up_f*), yet hosted still treats them as
//!     equivalent ("Cloned from other environment within tolerance"). Hosted's
//!     logic/dependency abstraction is therefore MORE aggressive than this
//!     server's schema-only abstraction.
//!
//! Determination (see docs/correctness-handoff.md §A/§B): on the finding-2a/3
//! axes the server is MORE CONSERVATIVE than hosted (hosted reuses aggressively
//! — clones across differing dependency names and tolerates source staleness).
//! Adding a speculative freshness/binding GATE to `find_clone_equivalent` would
//! make the server reuse LESS than hosted, i.e. a CONFORMANCE REGRESSION. Per
//! the live evidence and the repo's do-not-infer rule, the clone fallback is
//! intentionally NOT tightened. The exact hosted abstraction key, clone
//! tolerance, and the (observed, timing-sensitive/non-deterministic)
//! OUTSIDE_TIME_TRAVEL_WINDOW crr=9 rejection remain OPEN pending a controlled
//! adapter-level capture.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/clone_fallback_xenv_fresh.jsonl"
);

/// Pin the hosted cross-environment `ready_to_clone` fallback shape and the
/// cross-dependency-name clone (the finding-3 signal).
#[test]
fn hosted_cross_env_clone_fallback_shape_is_pinned() {
    let entries = diff::load_golden(FIXTURE).expect("load cross-env clone fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 2, "prod build + dev clone-fallback build");

    // First (prod) submit executes (first build in prod).
    assert_eq!(
        diff::decision_variant(&submits[0].response).unwrap(),
        "ready_to_execute",
        "prod build of the equivalent executes first"
    );

    // Second (dev) submit returns ready_to_clone cloning the equivalent.
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "ready_to_clone",
        "dev build of an absent target clones the cross-environment equivalent"
    );
    let clone = &submits[1].response["response"]["ready_to_clone"];
    let ed = &clone["explained_decision"];
    assert_eq!(
        ed["skip_rejection_reason"].as_i64(),
        Some(1),
        "srr=1 TARGET_TABLE_MISMATCH (equivalent under another name)"
    );
    assert_eq!(
        ed["decision_description"].as_str(),
        Some("an equivalent model exists under another name so we cloned that one")
    );
    let source = clone["clone_source"].as_str().unwrap_or("");
    assert!(
        source.contains("CF2_MODEL_"),
        "clones from a differently-named equivalent model: {source}"
    );
    assert!(
        clone["clone_required_last_modified_epoch"]
            .as_i64()
            .is_some(),
        "carries the source's build epoch as clone_required_last_modified_epoch"
    );

    // Finding-3 signal: the dev submit references a DIFFERENT upstream table
    // name than the equivalent it was cloned from, yet hosted still cloned —
    // hosted's abstraction is more aggressive than schema-only. We only pin that
    // the rendered SQL references an upstream whose table name is NOT the
    // clone source's model name (they are distinct physical relations).
    let dev_sql = submits[1].request["sql"].as_str().unwrap_or("");
    assert!(
        dev_sql.to_uppercase().contains("CF2_UP_"),
        "dev submit references its own cf2_up_* upstream: {dev_sql}"
    );
}
