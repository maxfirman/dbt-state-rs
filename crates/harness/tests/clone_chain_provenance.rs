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
use dbt_state_proto::query_cache as qc;
use qc::clone_client::CloneClient;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, start_server};

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/clone_chain_upstream_drift.jsonl"
);
const SOURCE_AGE_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/clone_chain_source_age.jsonl"
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

/// REGISTERCLONE SOURCE-AGE CHAIN (review #2 2nd+3rd follow-up, P0 #2) —
/// live-captured (experiments/registerclone_source_age.sh;
/// golden/fixtures/clone_chain_source_age.jsonl). FIVE messages:
///   0 prod SUBMIT (et=2 incremental, empty deps, plain SELECT) -> execute
///   1 CONFIRM lme=1791831369299 (source build @T1)
///   2 RegisterClone source->dev (et=2)
///   3 CONFIRM lme=1791831502677 (clone creation @T3)
///   4 dev SUBMIT (et=2, SQL now carries the INCREMENTAL `where id > (select
///     ... from {{ this }})` predicate; own target @1791831506226, upstream
///     @1791831449030) -> hosted skip_execution.
///
/// HONEST STATUS (3rd follow-up correction): the earlier claim that this chain
/// is reproduced by a synthetic clone-age test was WRONG — the synthetic test
/// used et=3, a plain (non-incremental) SQL, supplied deps, and own-epoch equal
/// to the clone confirm, so it did NOT send these requests. Replaying the EXACT
/// capture below, entries 0-3 match and ENTRY 4 returns local `ready_to_execute`
/// where hosted SKIPs. The cause is a genuine, UNIMPLEMENTED behavior: for an
/// INCREMENTAL model, hosted treats the second-phase SQL (which adds a
/// self-referential `where id > (select ... from {{ this }})` predicate) as the
/// SAME node as the first-phase build for reuse, whereas the server's logic hash
/// sees a different SQL and executes. This is isolated as a known replay gap
/// (incremental self-referential predicate reuse), NOT silently claimed as
/// reproduced.
///
/// SEPARATELY (3rd follow-up correction): the claim "hosted does not validate a
/// RegisterClone source epoch" is RETRACTED — this capture's source epoch
/// (1791831368903) is OLDER than the source's recorded confirmation
/// (1791831369299); it does not exercise the newer/externally-changed source
/// case. Source-content freshness, clone-creation age, and source-identity
/// validation remain SEPARATE OPEN questions; this capture settles none of them
/// beyond its own request.
#[tokio::test]
async fn registerclone_source_age_chain_exact_replay() {
    let entries = diff::load_golden(SOURCE_AGE_FIXTURE).expect("load source-age fixture");
    assert_eq!(entries.len(), 5, "the full 5-message chain");

    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch.clone());

    // Replay entry 0: prod source submit → must EXECUTE (first build). Correlate
    // its request_id to the hosted one for the following confirm.
    let e0 = &entries[0];
    let req0: qc::SubmitEnrichedSqlRequest = serde_json::from_value(e0.request.clone()).unwrap();
    let r0 = sql
        .submit_enriched_sql(support::captured_request(req0, e0))
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(local0) = r0.response.unwrap() else {
        panic!("entry 0: source must execute");
    };

    // Replay entry 1: confirm the source build using the local request_id.
    let mut c1: qc::ConfirmExecutionRequest =
        serde_json::from_value(entries[1].request.clone()).unwrap();
    c1.request_id = local0.request_id;
    assert!(
        exec.confirm_execution(support::captured_request(c1, &entries[1]))
            .await
            .unwrap()
            .into_inner()
            .success,
        "entry 1: source confirm succeeds"
    );

    // Replay entry 2: RegisterClone → ready_to_clone (both mirrors populated).
    let rc: qc::CloneRequest = serde_json::from_value(entries[2].request.clone()).unwrap();
    let clone_resp = clone
        .register_clone(support::captured_request(rc, &entries[2]))
        .await
        .unwrap()
        .into_inner();
    let ready = clone_resp
        .ready_to_clone
        .expect("entry 2: RegisterClone returns ready_to_clone");

    // Replay entry 3: confirm the clone creation.
    let mut c3: qc::ConfirmExecutionRequest =
        serde_json::from_value(entries[3].request.clone()).unwrap();
    c3.request_id = ready.request_id;
    assert!(
        exec.confirm_execution(support::captured_request(c3, &entries[3]))
            .await
            .unwrap()
            .into_inner()
            .success,
        "entry 3: clone confirm succeeds"
    );

    // Replay entry 4: the dev resubmit with the INCREMENTAL predicate.
    let e4 = &entries[4];
    let req4: qc::SubmitEnrichedSqlRequest = serde_json::from_value(e4.request.clone()).unwrap();
    let r4 = sql
        .submit_enriched_sql(support::captured_request(req4, e4))
        .await
        .unwrap()
        .into_inner();
    let local4 = support::response_variant(&r4);
    let hosted4 = diff::decision_variant(&e4.response).unwrap();

    assert_eq!(
        hosted4, "skip_execution",
        "hosted SKIPs entry 4 (captured truth)"
    );
    // KNOWN GAP: the server EXECUTEs here because the incremental self-referential
    // predicate changes the logic hash. Pin the gap EXACTLY so a future fix that
    // models incremental-phase reuse flips this and forces this assertion to be
    // updated (rather than silently claiming parity).
    assert_eq!(
        local4, "ready_to_execute",
        "KNOWN GAP (incremental self-referential predicate reuse UNIMPLEMENTED): \
         the server executes the second-phase incremental submit where hosted \
         reuses. If this changes, the gap is resolved — update the test and docs."
    );
}
