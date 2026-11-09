//! CLONE HISTORY / PHYSICAL-STATE PROVENANCE regressions (review issue #1, P0).
//!
//! A historical execution must never be treated as evidence of the CURRENT
//! contents of a physical object. Two clone-state lookups violated this:
//!
//!   P0#1 `store::find_clone_equivalent` (SQL-submit cross-name fallback):
//!        it filtered confirmed rows by (namespace, schema-abstracted logic,
//!        execution_type) BEFORE picking the newest per source target, so an
//!        OBSOLETE row for a source that has since been rebuilt with different
//!        logic was still eligible as a clone source. A→B→A: build PROD with
//!        logic A, rebuild PROD with logic B, then a new target submits logic A
//!        — the stale A row made PROD a clone source even though PROD now holds
//!        B. Correct: the source's CURRENT (latest confirmed) logic must match.
//!
//!   P0#2 `store::find_latest_confirmed_target` (clone adoption on submit):
//!        it applied `clone_source_table IS NOT NULL` BEFORE selecting the
//!        latest confirmed row, so a target that was cloned (clone-origin row A)
//!        and later rebuilt normally (ordinary row B) still resolved to the old
//!        clone-origin row A. clone→rebuild→old-logic: the physical target holds
//!        B, but a resubmit of logic A adopted the stale clone-origin row and
//!        could SKIP. Correct: the latest confirmed row for the target wins; it
//!        is adopted only if it is ITSELF clone-origin and logic-compatible.
//!
//! Both scenarios assert the SAFE (physical-state-faithful) outcome: EXECUTE,
//! never reuse of a historical object description.

#[path = "support.rs"]
mod support;

use support::demo::{
    self, clone_probe, clone_probe_sql, transient_at, CLONE_PROBE, DEV_SCHEMA, PROD_SCHEMA,
    RAW_ORDERS as RAW,
};
use support::scenario::{variant, ExecutionType, Scenario};
use support::start_server;

/// Every build below confirms a `TRANSIENT TABLE` built in 1 s.
const BUILD_MS: i64 = 1000;
/// A genuine projection change of the probe's logic.
const ADDED_COLUMN: &str = ", upper(customer_id) as c";

/// P0#1 — A→B→A cross-name clone selection.
///
/// `PROD` built with logic A, then rebuilt with logic B (physical PROD now
/// holds B). A new target `DEV` (absent) submits logic A. PROD's CURRENT logic
/// is B, so there is NO equivalent for logic A under another name — the submit
/// must EXECUTE, not clone the stale A description of PROD.
#[tokio::test]
async fn cross_name_clone_source_must_reflect_current_logic() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let prod = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ORDERS\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"ORDERS\"";
    let raw_epoch = 1_791_320_590_839i64;

    // 1) Build PROD with logic A, confirm.
    sc.build_with(
        clone_probe(prod, PROD_SCHEMA)
            .own_epoch(Some(1_791_743_740_000))
            .upstream(RAW, raw_epoch),
        transient_at(1_791_743_740_000).runtime(BUILD_MS),
    )
    .await;

    // 2) Rebuild PROD with logic B (added column) — physical PROD now holds B.
    sc.build_with(
        clone_probe(prod, PROD_SCHEMA)
            .sql(&clone_probe_sql(PROD_SCHEMA, ADDED_COLUMN))
            .own_epoch(Some(1_791_743_750_000))
            .upstream(RAW, raw_epoch),
        transient_at(1_791_743_750_000).runtime(BUILD_MS),
    )
    .await;

    // 3) New target DEV (absent) submits logic A. PROD's current logic is B, so
    //    no equivalent for A exists under another name: MUST EXECUTE.
    let dev_a = clone_probe(dev, DEV_SCHEMA)
        .own_epoch(Some(1))
        .upstream(RAW, raw_epoch);
    assert_eq!(
        sc.verdict(dev_a).await,
        "ready_to_execute",
        "clone source's CURRENT logic is B, not A: a stale A record must not make \
         PROD a clone source for logic A"
    );
}

/// P0#2 — clone→rebuild→old-logic adoption must not SKIP on stale clone state.
///
/// `DEV` is cloned from `PROD` (clone-origin row, logic A), then rebuilt
/// normally with logic B (physical DEV now holds B). A resubmit of logic A must
/// NOT SKIP by resurrecting the stale clone-origin A row: DEV's CURRENT state is
/// the ordinary B row (not clone-origin), so clone adoption cannot fire on it.
///
/// LIVE-VERIFIED (experiments/p0_fallback_over_existing.sh): the hosted service
/// answers `ready_to_clone` here — an equivalent for logic A still exists under
/// another name (PROD), so it re-clones that equivalent into DEV, EVEN THOUGH
/// DEV already exists holding logic B. The unsafe outcome the P0#2 bug produced
/// was a `skip_execution` (reusing A's description for a target physically
/// holding B); that must never happen. We assert the decision is NOT
/// `skip_execution` and matches the hosted `ready_to_clone`.
#[tokio::test]
async fn clone_adoption_must_not_resurrect_rebuilt_target() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let prod = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let raw_epoch = 1_791_320_590_839i64;
    let prod_epoch = 1_791_743_740_000i64;
    let clone_epoch = 1_791_743_784_000i64;

    // 1) Build the SOURCE in prod with logic A (records source logic for the
    //    clone-origin row to inherit), confirm.
    sc.build_with(
        clone_probe(prod, PROD_SCHEMA)
            .own_epoch(Some(prod_epoch))
            .upstream(RAW, raw_epoch),
        transient_at(prod_epoch).runtime(BUILD_MS),
    )
    .await;

    // 2) RegisterClone(source=prod, target=dev) + confirm → clone-origin row A
    //    (clone_source_table set, inherits logic A).
    let clone_resp = sc
        .register_clone(
            demo::copy(prod, dev)
                .source_epoch(prod_epoch)
                .label("dbt_node_unique_id", CLONE_PROBE),
        )
        .await
        .unwrap();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert!(
        sc.confirm(&ready.request_id, transient_at(clone_epoch))
            .await
    );

    // 3) Rebuild DEV normally with logic B (added column). Physical DEV now
    //    holds B; this ordinary row is the latest confirmed state for DEV.
    sc.build_with(
        clone_probe(dev, DEV_SCHEMA)
            .sql(&clone_probe_sql(DEV_SCHEMA, ADDED_COLUMN))
            .own_epoch(Some(1_791_743_790_000))
            .upstream(RAW, raw_epoch),
        transient_at(1_791_743_790_000).runtime(BUILD_MS),
    )
    .await;

    // 4) Resubmit logic A for DEV. DEV's latest confirmed state is the ordinary
    //    B row (not clone-origin), so the stale clone-origin A row must NOT be
    //    adopted → NO SKIP. Because an equivalent for logic A still exists under
    //    another name (PROD), the hosted-faithful outcome is `ready_to_clone`.
    let dev_a = sc
        .submit(
            clone_probe(dev, DEV_SCHEMA)
                .own_epoch(Some(1_791_743_790_000))
                .upstream(RAW, raw_epoch),
        )
        .await;
    assert_ne!(
        variant(&dev_a),
        "skip_execution",
        "a stale clone-origin row (logic A) must NEVER be resurrected to SKIP a \
         target that physically holds logic B"
    );
    assert_eq!(
        variant(&dev_a),
        "ready_to_clone",
        "LIVE-VERIFIED: an equivalent for logic A exists under another name (PROD), \
         so the hosted service re-clones it into DEV even though DEV exists holding B"
    );
}

// ============================================================================
// FOLLOW-UP (review issue #1, 2026-10-12): select CURRENT physical state before
// ALL metadata filters. The P0 fixes picked the latest confirmed row per source
// before matching logic/provenance, but three lookups still applied some
// metadata filters (execution_type / dialect / namespace / hash-NOT-NULL)
// BEFORE ordering by confirmed_at. A target whose CURRENT state differs on one
// of those columns could still resolve to an older matching row. Each test
// below asserts the SAFE physical-state-faithful outcome.
// ============================================================================

/// Transition 1a — `find_latest_confirmed_target`: clone→rebuild with a CHANGED
/// execution type must not adopt the stale clone-origin row.
///
/// LIVE-VERIFIED (experiments/physical_state_exec_type.sh, golden
/// golden_20261012T102334): a dbt model switched `table`→`incremental` sends
/// execution_type=1 (FULL) then =3 (MERGE) for the SAME physical target in the
/// SAME namespace, so the transition is client-reachable. Hosted timeline on
/// DEV: et=1 submit (absent) → ready_to_clone from PROD (clone-origin et=1 row);
/// et=3 submit → ready_to_execute (DEV now holds an ordinary et=3 build); et=1
/// submit of the same logic again → **ready_to_clone**, NOT a skip reusing the
/// stale et=1 clone-origin row. Hosted treats the et=1 clone-origin row as
/// historical because DEV's current state is the et=3 build.
///
/// The bug: `find_latest_confirmed_target` filtered `execution_type=1` before
/// `ORDER BY confirmed_at`, so it would still find the stale et=1 clone-origin
/// row and (logic matching) could SKIP. The fix picks DEV's latest row first
/// (the et=3 ordinary build), which is neither execution_type=1 nor
/// clone-origin → no adoption → falls through to the cross-name clone fallback.
#[tokio::test]
async fn clone_adoption_must_not_resurrect_row_of_different_execution_type() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let prod = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"PTF_PROBE\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"PTF_PROBE\"";
    let raw_epoch = 1_791_320_590_839i64;
    let prod_epoch = 1_791_743_740_000i64;
    let clone_epoch = 1_791_743_784_000i64;

    // 1) Build the SOURCE in prod as a TABLE (et=1), logic A, confirm.
    sc.build_with(
        clone_probe(prod, PROD_SCHEMA)
            .own_epoch(Some(prod_epoch))
            .upstream(RAW, raw_epoch),
        transient_at(prod_epoch).runtime(BUILD_MS),
    )
    .await;

    // 2) Clone prod→dev (clone-origin row, et=1, inherits logic A), confirm.
    let clone_resp = sc
        .register_clone(
            demo::copy(prod, dev)
                .source_epoch(prod_epoch)
                .label("dbt_node_unique_id", CLONE_PROBE),
        )
        .await
        .unwrap();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert!(
        sc.confirm(&ready.request_id, transient_at(clone_epoch))
            .await
    );

    // 3) Rebuild DEV as an INCREMENTAL merge (et=3), same logic A. DEV's CURRENT
    //    physical state is now an ordinary et=3 build (not clone-origin).
    let dev_a = clone_probe(dev, DEV_SCHEMA)
        .own_epoch(Some(1_791_743_790_000))
        .upstream(RAW, raw_epoch);
    sc.build_with(
        dev_a.clone().execution_type(ExecutionType::Merge),
        transient_at(1_791_743_790_000).runtime(BUILD_MS),
    )
    .await;

    // 4) Re-submit DEV as a TABLE (et=1), logic A again. DEV's latest confirmed
    //    row is the et=3 ordinary build, so the stale et=1 clone-origin row must
    //    NOT be adopted → NO SKIP. Hosted re-clones the equivalent (PROD).
    let dev_a = sc.submit(dev_a).await;
    assert_ne!(
        variant(&dev_a),
        "skip_execution",
        "the stale et=1 clone-origin row must NOT be resurrected to SKIP a target \
         whose current physical state is an et=3 incremental build"
    );
    assert_eq!(
        variant(&dev_a),
        "ready_to_clone",
        "LIVE-VERIFIED: DEV's current state is et=3, so hosted re-clones the \
         equivalent (PROD) rather than reusing the historical et=1 clone-origin row"
    );
}

/// Transition 1b — `find_source_ns_logic_hash`: when the clone SOURCE's newest
/// confirmed row carries NO schema-abstracted logic hash, RegisterClone must
/// inherit NOTHING (not an obsolete hash from an older row).
///
/// Reachability (primary evidence, services.rs): the namespace logic hash is
/// stored only when `!compare_unrendered_code && sql non-empty`. A source first
/// built with rendered SQL (hash present) and later rebuilt via an unrendered
/// comparison submit records a NEWER row with `node_namespace_logic_hash` NULL.
///
/// The bug: `find_source_ns_logic_hash` filtered `node_namespace_logic_hash IS
/// NOT NULL` before ordering, so it skipped the newer NULL-hash row and
/// inherited the stale hash onto the clone-origin row — which could later let a
/// resubmit SKIP against logic the source no longer has. The fix selects the
/// source's latest confirmed row first; if it lacks a hash, inherit none → the
/// clone target carries no logic identity → a later submit EXECUTEs (safe).
///
/// OFFLINE invariant (store determinism); not a hosted-parity claim.
#[tokio::test]
async fn clone_source_logic_inherit_must_use_current_source_state() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let src = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"PROBE_SRC\"";
    let tgt = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"PROBE_TGT\"";
    let raw_epoch = 1_791_320_590_839i64;

    // 1) Build SOURCE with rendered logic A → stores node_namespace_logic_hash.
    sc.build_with(
        clone_probe(src, PROD_SCHEMA)
            .own_epoch(Some(1_791_743_740_000))
            .upstream(RAW, raw_epoch),
        transient_at(1_791_743_740_000).runtime(BUILD_MS),
    )
    .await;

    // 2) Rebuild SOURCE via an UNRENDERED-comparison submit (newer row, NULL
    //    namespace logic hash). This is the source's CURRENT state.
    sc.build_with(
        clone_probe(src, PROD_SCHEMA)
            .own_epoch(Some(1_791_743_760_000))
            .upstream(RAW, raw_epoch)
            .compare_unrendered_code(),
        transient_at(1_791_743_760_000).runtime(BUILD_MS),
    )
    .await;

    // 3) Clone source→target. The source's CURRENT row has no logic hash, so the
    //    clone-origin row must inherit NONE.
    let clone_resp = sc
        .register_clone(
            demo::copy(src, tgt)
                .source_epoch(1_791_743_760_000)
                .label("dbt_node_unique_id", CLONE_PROBE),
        )
        .await
        .unwrap();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert!(
        sc.confirm(&ready.request_id, transient_at(1_791_743_784_000))
            .await
    );

    // 4) Resubmit the clone target with the OLD rendered logic A. Because the
    //    clone-origin row inherited NO logic identity (source's current state
    //    had none), there is no proven logic equivalence → MUST EXECUTE, never
    //    SKIP against a stale inherited hash.
    let tgt_a = clone_probe(tgt, DEV_SCHEMA)
        .own_epoch(Some(1_791_743_784_000))
        .upstream(RAW, raw_epoch);
    assert_ne!(
        sc.verdict(tgt_a).await,
        "skip_execution",
        "a clone whose source's CURRENT state carries no logic hash must not SKIP \
         by inheriting an obsolete hash from an older source row"
    );
}

/// Equal relation text in two adapter namespaces names two different physical
/// objects. A later build in namespace NS2 must not hide a live NS source from
/// the SQL-submit clone lookup. This supersedes the earlier offline assumption
/// that a namespace transition could describe one physical warehouse object.
#[tokio::test]
async fn clone_equivalent_is_scoped_to_source_warehouse() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    const NS2: &str = "aa11bb22cc33dd44ee55ff6600112233";
    let src = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"XNS_SRC\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"XNS_DEV\"";
    let raw_epoch = 1_791_320_590_839i64;

    // 1) Build SOURCE with logic A in namespace NS (older state).
    sc.build_with(
        clone_probe(src, PROD_SCHEMA)
            .own_epoch(Some(1_791_743_740_000))
            .upstream(RAW, raw_epoch),
        transient_at(1_791_743_740_000).runtime(BUILD_MS),
    )
    .await;

    // 2) Build the same textual name in a DIFFERENT warehouse namespace NS2.
    sc.build_with(
        clone_probe(src, PROD_SCHEMA)
            .own_epoch(Some(1_791_743_760_000))
            .upstream(RAW, raw_epoch)
            .namespace(NS2),
        transient_at(1_791_743_760_000).runtime(BUILD_MS),
    )
    .await;

    // 3) The original source remains current in NS, so DEV can clone from it.
    let dev_a = clone_probe(dev, DEV_SCHEMA)
        .own_epoch(Some(1))
        .upstream(RAW, raw_epoch);
    assert_eq!(
        sc.verdict(dev_a).await,
        "ready_to_clone",
        "a same-named object in another warehouse cannot hide the NS clone source"
    );
}

/// A clone inherits source logic from the requested physical namespace. A
/// same-named source in another warehouse does not replace that source's logic.
#[tokio::test]
async fn clone_source_logic_inherits_from_requested_warehouse() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    const NS2: &str = "99887766554433221100ffeeddccbbaa";
    let src = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"NSX_SRC\"";
    let tgt = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"NSX_TGT\"";
    let raw_epoch = 1_791_320_590_839i64;

    // 1) Build SOURCE with logic A in namespace NS (older state → records a
    //    namespace logic hash for A under NS).
    sc.build_with(
        clone_probe(src, PROD_SCHEMA)
            .own_epoch(Some(1_791_743_740_000))
            .upstream(RAW, raw_epoch),
        transient_at(1_791_743_740_000).runtime(BUILD_MS),
    )
    .await;

    // 2) Build different logic under the same textual name in NS2.
    sc.build_with(
        clone_probe(src, PROD_SCHEMA)
            .sql(&clone_probe_sql(PROD_SCHEMA, ", 42 as mark"))
            .own_epoch(Some(1_791_743_760_000))
            .upstream(RAW, raw_epoch)
            .namespace(NS2),
        transient_at(1_791_743_760_000).runtime(BUILD_MS),
    )
    .await;

    // 3) Clone the NS source. The newer NS2 row describes a different object.
    let clone_resp = sc
        .register_clone(
            demo::copy(src, tgt)
                .source_epoch(1_791_743_740_000)
                .label("dbt_node_unique_id", CLONE_PROBE),
        )
        .await
        .unwrap();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert!(
        sc.confirm(&ready.request_id, transient_at(1_791_743_784_000))
            .await
    );

    // 4) The cloned target holds logic A from NS and can reuse it.
    let tgt_a = clone_probe(tgt, DEV_SCHEMA)
        .own_epoch(Some(1))
        .upstream(RAW, raw_epoch);
    assert_eq!(
        sc.verdict(tgt_a).await,
        "skip_execution",
        "a same-named NS2 object cannot erase the original source's logic"
    );
}
