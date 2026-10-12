//! REGISTERCLONE / CLONE-CHAIN PROVENANCE (review #2 finding 4, P0) —
//! live-captured from api.state.dbt.com (Fusion client, Snowflake;
//! experiments/clone_chain_provenance.sh; golden/fixtures/
//! clone_chain_upstream_drift.jsonl).
//!
//! The follow-up review's concern is that a clone inherits the SOURCE's logic
//! identity while a later same-target reuse is anchored on the CLONE's build
//! epoch, so a clone whose data predates the current upstream could be reused
//! unsafely. The live clone chain settles the provenance question:
//!
//!   1. prod builds/holds the source S.
//!   2. dev build of an absent target T CLONES S into T (ready_to_clone),
//!      confirmed with the clone's build epoch (CC_CONFIRM lme).
//!   3. the dev upstream is bumped so it is NEWER than the clone's build epoch.
//!   4. dev re-submits T: the submit now carries T's OWN epoch and the newer
//!      upstream, and the hosted service answers ready_to_execute
//!      ("model was executed because ... its upstream data is out of date").
//!
//! So after a clone, hosted anchors the clone target's freshness on the CLONE's
//! build epoch and REBUILDS when an upstream advances past it — exactly the
//! own-build anchor the engine implements (decision.rs). The clone is NOT reused
//! once the upstream moves past it; a clone is not treated as perpetually fresh.
//! (The ORTHOGONAL fact that a clone may be created from a source whose data is
//! older than the current upstream is hosted-TOLERATED at clone time — see
//! clone_eligibility.rs / finding 2 — and is a separate axis from this post-clone
//! freshness anchor.)
//!
//! This fixture REPLAYS EXACTLY on the server (it is in the conformance corpus),
//! so the server reproduces the whole chain including the step-4 rebuild. These
//! assertions pin the hosted truth so a regression that made a post-clone target
//! perpetually skippable (ignoring upstream drift) would fail.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/clone_chain_upstream_drift.jsonl"
);

/// Pin the hosted clone-chain: dev clones, then a later submit with an upstream
/// NEWER than the clone's build epoch REBUILDS (own-build freshness anchor).
#[test]
fn hosted_clone_rebuilds_when_upstream_advances_past_clone() {
    let entries = diff::load_golden(FIXTURE).expect("load clone-chain fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    let confirms: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "ConfirmExecution")
        .collect();
    // prod source submit, dev clone submit, dev re-submit after bump.
    assert_eq!(submits.len(), 3, "prod + dev-clone + dev-resubmit");
    assert!(confirms.len() >= 2, "clone confirm + rebuild confirm");

    // The dev clone (second submit) is a clone.
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "ready_to_clone",
        "the dev build clones the equivalent"
    );
    // The clone's build epoch is recorded by its confirm.
    let clone_build_epoch = confirms[1].request["last_modified_epoch"]
        .as_i64()
        .expect("clone confirm epoch");

    // The third submit (after the upstream bump) carries an upstream epoch NEWER
    // than the clone's build epoch, and hosted REBUILDS.
    let third = submits[2];
    let max_upstream = third.request["tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["last_modified_epoch"].as_i64())
        .max()
        .expect("some upstream epoch");
    assert!(
        max_upstream > clone_build_epoch,
        "the re-submit's upstream (@{max_upstream}) is newer than the clone build \
         (@{clone_build_epoch})"
    );
    assert_eq!(
        diff::decision_variant(&third.response).unwrap(),
        "ready_to_execute",
        "a post-clone target with an upstream advanced past the clone REBUILDS \
         (clone is not perpetually fresh)"
    );
    let ed = &third.response["response"]["ready_to_execute"]["explained_decision"];
    assert!(
        ed["decision_description"]
            .as_str()
            .unwrap_or("")
            .contains("out of date"),
        "hosted attributes the rebuild to stale upstream data"
    );
}
