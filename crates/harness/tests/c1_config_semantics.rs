//! C1b — CONFIG-SEMANTICS MATRIX (live-captured from api.state.dbt.com).
//!
//! Follow-up to `c1_probe.rs`. We asked: is the hosted service "clever" about
//! which config changes are skippable — and does a config that mutates
//! warehouse state (pre/post hooks, grants, persist_docs) force a rebuild?
//!
//! We ran a `dbt build` of jaffle-shop `customers` on Snowflake through the
//! recording proxy, applying one config change at a time. The SELECT body is
//! semantically identical in every case (the client renders config OUT of the
//! compiled SQL into the hashes), so the server's own SQL fingerprint is equal
//! across all variants — yet the hosted decisions split by CONFIG KEY:
//!
//!   change                         hosted decision
//!   -----------------------------  ---------------
//!   (unchanged rebuild)            skip
//!   config(tags=[…])               skip
//!   config(meta={…})               skip      (shown in c1_probe)
//!   config(post_hook="ALTER …")    SKIP   <-- warehouse-mutating, still skipped
//!   config(post_hook="GRANT …")    SKIP   <-- idem
//!   config(grants={select:[…]})    execute
//!   config(pre_hook="SELECT …")    execute
//!   config(persist_docs={…})       execute
//!
//! VERIFIED side effect: when the post_hook `ALTER TABLE … SET COMMENT 'probe'`
//! was skipped, the warehouse table comment was NOT changed (it kept the model
//! description). So the hosted service deliberately treats meta/tags/post_hook
//! as non-rebuild-worthy and silently drops the post_hook's warehouse mutation
//! — it is NOT conservatively protecting warehouse state. The split is a
//! per-config-key policy (meta/tags/post_hook = cosmetic; grants/pre_hook/
//! persist_docs = material), applied on top of the server-side SQL semantic
//! fingerprint.
//!
//! Our server keys the match on node_body_hash, so it executes on EVERY one of
//! these (any config edit perturbs the client body hash). This test PINS the
//! real decision sequence as the golden contract and records that our behavior
//! is uniformly ready_to_execute — a safe-directional divergence (over-execute,
//! never stale). Reproducing the hosted policy faithfully would require encoding
//! its per-config-key semantics server-side.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/c1_config_semantics.jsonl"
);

#[tokio::test]
async fn c1_config_semantics_matrix_is_characterized() {
    let entries = diff::load_golden(FIXTURE).expect("load config-semantics fixture");

    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 8, "expected the 8 config-matrix submits");

    let real: Vec<String> = submits
        .iter()
        .map(|e| diff::decision_variant(&e.response).unwrap())
        .collect();

    // The hosted-service golden contract, in experiment order:
    //   [0] first build       -> execute
    //   [1] unchanged rebuild  -> skip
    //   [2] tags               -> skip
    //   [3] post_hook (ALTER)  -> skip   (warehouse-mutating, still skipped)
    //   [4] grants             -> execute
    //   [5] pre_hook           -> execute
    //   [6] post_hook (GRANT)  -> skip
    //   [7] persist_docs       -> execute
    let expected = [
        "ready_to_execute",
        "skip_execution",
        "skip_execution",
        "skip_execution",
        "ready_to_execute",
        "ready_to_execute",
        "skip_execution",
        "ready_to_execute",
    ];
    assert_eq!(
        real, expected,
        "hosted-service config-semantics decision sequence changed from the captured golden"
    );

    // The decisive pair: a warehouse-mutating post_hook SKIPPED ([3]) while a
    // grants change EXECUTED ([4]) — so the hosted service's split is by config
    // KEY semantics, not by "does it touch the warehouse".
    assert_eq!(
        real[3], "skip_execution",
        "post_hook change is skipped by the hosted service"
    );
    assert_eq!(
        real[4], "ready_to_execute",
        "grants change is executed by the hosted service"
    );

    eprintln!(
        "C1b CHARACTERIZATION: hosted per-config-key policy = {real:?}; \
         our server executes on all of these (node_body_hash match)."
    );
}
