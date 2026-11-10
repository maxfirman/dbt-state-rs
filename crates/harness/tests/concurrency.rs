//! CONCURRENCY & LATE-CONFIRMATION physical-state determinism (review issue #1
//! P1 "Concurrency and physical provenance").
//!
//! The review flags that confirmation ordering may differ from warehouse
//! replacement ordering, and asks to characterize overlapping A/B builds,
//! reversed/late confirmations, and conflicting Record batches. The hosted
//! service's behavior under true concurrency is NOT establishable from a
//! sequential capture (the review says so), so these tests do NOT assert hosted
//! parity. Instead they PIN the server's deterministic rule and make the
//! physical-state invariant explicit:
//!
//!   CURRENT PHYSICAL STATE = the execution with the newest `confirmed_at`
//!   (tie-broken by id). Confirmation order — NOT submission order — defines
//!   which build is treated as the object's current contents. This models "the
//!   last build to finish writing the warehouse object wins", which is the only
//!   provenance signal available without stable warehouse object IDs.
//!
//! These are deterministic offline tests; the open question of stronger
//! object-lineage provenance is documented in docs/correctness-handoff.md.
#[path = "support.rs"]
mod support;

use support::rows;
use support::scenario::{built_at, model, outcome, Scenario};
use support::start_server;

const SRC: &str = "\"DB\".\"S\".\"SRC\"";

// Every submit is a model for `target` with own-target epoch 1 (present) and
// one upstream `SRC` at epoch 100. Its body ("11", "22", "33") is its logic:
// distinct bodies render to distinct SQL projections (not comments, which the
// Snowflake normalizer strips).

/// REVERSED CONFIRMATION ORDER. Two different-logic submits for the SAME target
/// are both issued (both pending), then confirmed in REVERSE order (the
/// first-submitted is confirmed LAST). The current physical state must be the
/// LAST-CONFIRMED build (logic A), not the last-submitted (logic B): a resubmit
/// of A's logic + fresh upstream SKIPS, while B's logic rebuilds.
#[tokio::test]
async fn confirmation_order_defines_current_state_not_submission_order() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let node = model("\"DB\".\"S\".\"T\"").upstream(SRC, 100);
    let (logic_a, logic_b) = (node.clone().body("11"), node.body("22"));

    // Submit A (logic hA) and B (logic hB) — both become pending rows.
    let id_a = sc.execute_id(logic_a.clone()).await;
    let id_b = sc.execute_id(logic_b.clone()).await;

    // Confirm in REVERSE order: B first, then A. A has the newest confirmed_at.
    assert!(sc.confirm(&id_b, built_at(100)).await);
    assert!(sc.confirm(&id_a, built_at(100)).await);

    // Resubmit A's logic with a fresh (<=100) upstream → SKIP (A is current).
    assert_eq!(
        sc.verdict(logic_a).await,
        "skip_execution",
        "the LAST-confirmed build (A) is the current physical state"
    );

    // Resubmit B's logic → EXECUTE (B is no longer the current state).
    assert_eq!(
        sc.verdict(logic_b).await,
        "ready_to_execute",
        "the superseded build (B) must not skip"
    );
}

/// LATE CONFIRMATION. After B is the current state, a LATE confirm of an even
/// older pending row (C, submitted first but confirmed last) makes C the current
/// state deterministically. This bounds the "late confirm" risk: the outcome is
/// always well-defined (newest confirmed_at), never a silent corruption.
#[tokio::test]
async fn late_confirmation_deterministically_becomes_current() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let node = model("\"DB\".\"S\".\"L\"").upstream(SRC, 100);
    let (logic_b, logic_c) = (node.clone().body("22"), node.body("33"));

    // Submit C first (pending, not yet confirmed).
    let id_c = sc.execute_id(logic_c.clone()).await;

    // Build + confirm B meanwhile (B becomes current).
    sc.build(logic_b.clone(), 100).await;
    // B is current → B's logic skips.
    assert_eq!(sc.verdict(logic_b.clone()).await, "skip_execution");

    // Now the LATE confirm of C lands. C has the newest confirmed_at → current.
    sc.confirm(&id_c, built_at(100)).await;
    assert_eq!(
        sc.verdict(logic_c).await,
        "skip_execution",
        "a late confirm deterministically makes its row the current state"
    );
    assert_eq!(
        sc.verdict(logic_b).await,
        "ready_to_execute",
        "B is now superseded by the later-confirmed C"
    );
}

/// SUBMIT↔RECORD fingerprint parity for a type-1 model: a target hydrated via
/// RecordExecutions with the SAME dialect/catalog/schema/sql as a later submit
/// must be reused (the two paths compute the same `node_sql_hash`). Guards
/// against a Submit-vs-Record hashing divergence that would silently rebuild a
/// recorded model.
#[tokio::test]
async fn submit_and_record_agree_on_model_fingerprint() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let node = model("\"DB\".\"S\".\"P\"")
        .catalog("DB")
        .schema("S")
        .sql("select 1 as id from \"DB\".\"S\".\"SRC\"")
        .without_node_state();

    // Hydrate via RecordExecutions, copying only the identity the submit will
    // send (no tables recorded).
    sc.record([node
        .clone()
        .no_tables()
        .record(outcome().epoch(100).table().runtime(5))])
        .await
        .unwrap();

    // A submit with the SAME dialect/catalog/schema/sql must reuse it.
    assert_eq!(
        sc.verdict(node).await,
        "skip_execution",
        "Submit and Record must agree on the model fingerprint (recorded model reused)"
    );
}

/// CONFLICTING CONFIRMED ROWS for the same target. Two different-logic builds
/// for the SAME target are confirmed in sequence (X then Y). Both rows persist
/// (no dedupe) and the LAST-confirmed one (Y) is the current physical state: a
/// submit of Y's logic skips; the superseded X rebuilds. (Submit↔Record
/// fingerprint parity is covered separately in offline_regressions.rs; here we
/// confirm via the submit path so the probe hashes line up deterministically.)
#[tokio::test]
async fn conflicting_confirmed_rows_last_confirmed_is_current() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let target = "\"DB\".\"S\".\"R\"";
    let node = model(target).upstream(SRC, 100);

    // Build + confirm logic X, then logic Y, for the same target.
    for expr in ["11", "22"] {
        sc.build(node.clone().body(expr), 100).await;
    }

    // Both rows persist (no dedupe across conflicting confirmed builds).
    let count = rows::executions(&server)
        .await
        .iter()
        .filter(|r| r.target_table == target && r.is_confirmed())
        .count();
    assert_eq!(count, 2, "conflicting confirmed builds both persist");

    // The LAST-confirmed logic (Y="22") is current → its submit skips; X rebuilds.
    assert_eq!(
        sc.verdict(node.clone().body("22")).await,
        "skip_execution",
        "last-confirmed build is the current physical state"
    );
    assert_eq!(
        sc.verdict(node.body("11")).await,
        "ready_to_execute",
        "the earlier-confirmed (superseded) logic must rebuild"
    );
}
