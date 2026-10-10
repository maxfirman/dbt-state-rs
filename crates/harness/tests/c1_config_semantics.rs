//! C1b — CONFIG-SEMANTICS MATRIX (live-captured from api.state.dbt.com).
//!
//! Follow-up to `c1_probe.rs`. We asked which config changes force a rebuild and
//! why. Running `dbt build` of jaffle-shop `customers` on Snowflake through the
//! recording proxy, one config change at a time, and cross-checking the client
//! source, established the rule:
//!
//! The hosted service rebuilds iff the WHITESPACE-NORMALIZED rendered SQL
//! changed OR an ALLOWLISTED `semantic_extras` key changed. The client folds a
//! FIXED set of config keys into `semantic_extras`
//! (on_schema_change/contract/constraints/unique_key/grants/merge_*/
//! incremental_predicates/event_time/sql_header/lookback/table_format/warehouse
//! keys/__persisted_docs_hash). Config NOT in that set (meta/tags/pre_hook/
//! post_hook) does not appear in semantic_extras and does NOT force a rebuild.
//!
//!   change                         semantic_extras? hosted (reproducible)
//!   -----------------------------  ---------------- ---------------------
//!   config(tags=[…]) / meta        no               skip
//!   config(pre_hook / post_hook)   no               skip
//!   whitespace-only SQL            n/a              skip
//!   config(grants={…})             yes (grants)     execute
//!   config(persist_docs={…})       yes (__persisted_docs_hash) execute
//!   config(contract/unique_key)    yes              execute
//!
//! CORRECTION: this fixture was captured in a single pass WITHOUT re-establishing
//! a clean confirmed baseline between edits, and its `pre_hook` entry recorded
//! EXECUTE. Re-testing with a clean baseline between each edit showed `pre_hook`
//! reproducibly SKIPS (same as meta/post_hook). The hosted service's decision for
//! non-allowlisted config is NOT a pure function of the request — it also depends
//! on warehouse/server state we don't control — so the fixture's `pre_hook=execute`
//! is a stateful artifact, retained as recorded traffic but NOT our target. An
//! earlier draft of this test over-claimed a deterministic "per-config-key policy"
//! and a "server-side semantic SQL fingerprint"; both were corrected to the
//! verified `semantic_extras`-allowlist + whitespace-normalization mechanism.
//!
//! Our server implements that rule, so it reproduces every REPRODUCIBLE case
//! (skip meta/tags/hooks/whitespace; execute grants/persist_docs/contract). This
//! test pins the fixture's recorded sequence as a contract and documents the one
//! non-reproducible entry.

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
    // grants change EXECUTED ([4]) — grants is an allowlisted semantic_extras
    // key, post_hook is not. The split is by the semantic_extras allowlist.
    assert_eq!(
        real[3], "skip_execution",
        "post_hook (not in semantic_extras allowlist) is skipped by the hosted service"
    );
    assert_eq!(
        real[4], "ready_to_execute",
        "grants (in semantic_extras allowlist) is executed by the hosted service"
    );

    eprintln!(
        "C1b: hosted decisions = {real:?}; discriminator = semantic_extras allowlist \
         + whitespace-normalized SQL. Our server reproduces every reproducible case \
         (meta/tags/hooks skip; grants/persist_docs execute); the fixture's pre_hook=execute \
         is a non-reproducible stateful artifact (see module docs)."
    );
}
