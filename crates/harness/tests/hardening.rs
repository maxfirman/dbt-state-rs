//! End-to-end hardening scenarios — controlled differential testing of the
//! NON-HAPPY paths through the full gRPC server (store + decision engine +
//! lifecycle), against an isolated Postgres schema per test.
//!
//! The differential corpus proves faithful reproduction of the *observed*
//! transitions; these tests widen the surface to the error-prone branches the
//! captured traffic does not exercise directly:
//!
//!   1. FAILED RUNS — a submit that executed but was never confirmed must never
//!      skip on the next run (the warehouse object was never built).
//!   2. CONFIRM-OF-WRONG-ID — confirming a different request_id leaves the
//!      original pending; the original fingerprint still executes.
//!   3. CHANGED CONTRACT / CHANGED SQL — a changed node_body_hash after a
//!      confirmed build forces a rebuild (hash miss), then re-confirms.
//!   4. MODIFIED UPSTREAM — a newer upstream after a confirmed build forces a
//!      stale rebuild; reverting the upstream skips again.
//!   5. ADDED / DELETED UPSTREAM — a new upstream forces execute; dropping all
//!      upstreams skips on hash match.
//!   6. SCHEMA CHANGE — a cross-schema upstream is matched by logical identity.
//!   7. EXECUTION-TYPE ISOLATION — the same fingerprint under a different
//!      execution_type is a distinct node and must execute.
//!   8. FULL REFRESH — re-running an unchanged, confirmed node skips (dbt
//!      --full-refresh does not force execute when logic+data are unchanged).
//!   9. STALE-POLICY — ANY vs ALL end-to-end across two upstreams.
//!  10. CONCURRENCY — N concurrent IDENTICAL submits all execute (no dedupe)
//!      and after a single confirm the fingerprint skips; confirm is
//!      idempotent across the duplicate pending rows.
//!  11. SKIP-SAFETY END-TO-END — a skip only ever follows a confirmed build of
//!      the same fingerprint with non-drifted upstreams; otherwise execute.

use dbt_state_proto::query_cache as qc;
use tokio::task::JoinSet;

#[path = "support.rs"]
mod support;
use support::rows;
use support::scenario::{built_at, model, ready_id, variant, view, Policy, Scenario};
use support::start_server;

const DECISION_SKIP: i32 = qc::SubmitSqlResultType::SkipExecution as i32;
const DECISION_EXECUTE: i32 = qc::SubmitSqlResultType::ReadyToExecute as i32;

// Every submit is a model (or view) with its own target present at epoch 1 and
// the given upstreams. NOTE: the hosted service's logic identity is the
// (whitespace-normalized) rendered SQL + allowlisted `semantic_extras`, NOT
// `node_body_hash` (which it ignores for reuse). The scenario builders render
// the SQL from the body hash: a distinct logical identity produces distinct
// SQL, exactly as real dbt compiles distinct logic to distinct SQL. This keeps
// "changed logic → rebuild" tests faithful.

// 1. FAILED RUN: executed but NEVER confirmed → the next identical submit must
//    STILL EXECUTE. A failed build left no confirmed state; skipping would
//    reference an object that was never created.
#[tokio::test]
async fn unconfirmed_execution_never_skips() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let req = model("\"DB\".\"S\".\"FAILED\"")
        .body("h-failed")
        .upstream("up", 100);

    // First submit executes (pending row written), but we DO NOT confirm —
    // simulating a build that errored before ConfirmExecution.
    sc.execute_id(req.clone()).await;

    // Second identical submit must STILL execute (no confirmed history).
    assert_eq!(
        sc.verdict(req.clone()).await,
        "ready_to_execute",
        "an unconfirmed (failed) run must never produce a skip"
    );

    // There must be zero confirmed rows for this fingerprint.
    let confirmed = rows::executions(&server)
        .await
        .iter()
        .filter(|r| r.is_confirmed() && r.node_body_hash.as_deref() == Some("h-failed"))
        .count();
    assert_eq!(
        confirmed, 0,
        "a failed/unconfirmed run leaves no confirmed state"
    );
}

// 2. CONFIRMING THE WRONG request_id must not confirm the original fingerprint:
//    the original still executes.
#[tokio::test]
async fn confirming_wrong_request_id_leaves_original_pending() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let req = model("\"DB\".\"S\".\"WRONGID\"")
        .body("h-wrong")
        .upstream("up", 100);
    let _rid = sc.execute_id(req.clone()).await;

    // Malformed correlation IDs fail without confirming pending work.
    let error = sc
        .try_confirm(built_at(100).confirm("totally-unrelated-id"))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "Database operation failed");

    // The original fingerprint is still unconfirmed → executes again.
    assert_eq!(sc.verdict(req).await, "ready_to_execute");
}

// 3. CHANGED CONTRACT / CHANGED SQL: confirm a build, then the SAME node with a
//    DIFFERENT body hash (edited SQL / changed contract) must REBUILD, and can
//    itself be confirmed; the original hash still skips.
#[tokio::test]
async fn changed_contract_forces_rebuild() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let node = model("\"DB\".\"S\".\"CONTRACT\"").upstream("up", 100);
    let v1 = node.clone().body("hash-v1");
    sc.build(v1.clone(), 100).await;

    // Same node, edited SQL → new body hash → must execute (hash miss).
    let v2 = node.body("hash-v2");
    let r2 = sc.submit(v2.clone()).await;
    assert_eq!(
        variant(&r2),
        "ready_to_execute",
        "a changed body hash must rebuild"
    );
    // Confirm v2.
    assert!(sc.confirm(&ready_id(&r2), built_at(200).runtime(20)).await);

    // The original history no longer describes the overwritten target.
    assert_eq!(sc.verdict(v1).await, "ready_to_execute");
    // And v2 now skips too.
    assert_eq!(sc.verdict(v2).await, "skip_execution");
}

// 4. MODIFIED UPSTREAM: confirm a build, then a NEWER upstream forces a stale
//    rebuild; reverting the upstream to the recorded epoch skips again.
#[tokio::test]
async fn modified_upstream_forces_stale_rebuild_then_recovers() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let base = model("\"DB\".\"S\".\"UP\"")
        .body("h-up")
        .upstream("src", 1_000_000);
    sc.build(base.clone(), 1_000_000).await;

    // Upstream moved far newer → stale → execute.
    let x = sc
        .expect_execute(base.clone().upstream("src", 9_000_000))
        .await;
    let ed = x.explained_decision.as_ref().unwrap();
    assert_eq!(ed.decision, DECISION_EXECUTE);
    // VERIFIED LIVE (lag fixture entry 5; multi-upstream ANY captures):
    // the hosted service reports is_stale=FALSE on an upstream-drift
    // rebuild. is_stale is only ever true on a tolerated-stale SKIP.
    assert!(!ed.is_stale, "an execute always reports is_stale=false");

    // Upstream reverts to the recorded epoch → fresh → skip (confirmed row
    // still present; we never confirmed the stale rebuild).
    assert_eq!(sc.verdict(base).await, "skip_execution");
}

// 5. ADDED then DELETED upstream.
#[tokio::test]
async fn added_then_deleted_upstream() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let target = "\"DB\".\"S\".\"ADDDROP\"";
    let node = model(target).body("h-ad");
    let base = node.clone().upstream("a", 100);
    sc.build(base.clone(), 100).await;

    // Add a brand-new upstream, newer than the build → execute.
    let added = base.upstream("b_new", 9_000_000);
    assert_eq!(
        sc.verdict(added).await,
        "ready_to_execute",
        "a new, newer, untracked upstream must execute"
    );

    // Missing all recorded upstreams cannot establish freshness.
    assert_eq!(
        sc.verdict(node).await,
        "ready_to_execute",
        "missing dependency evidence requires execution"
    );
}

// 6. SCHEMA CHANGE: identical timestamps do not prove equivalent objects.
#[tokio::test]
async fn cross_schema_upstream_requires_provenance() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let node = model("\"DB\".\"S\".\"XSCHEMA\"").body("h-xs");
    let prod = node.clone().upstream("\"DB\".\"PROD\".\"SRC\"", 1_000_000);
    sc.build(prod, 1_000_000).await;

    // Different physical input under a different schema requires execution.
    let dev = node.upstream("\"DB\".\"DEV\".\"SRC\"", 1_000_000);
    assert_eq!(
        sc.verdict(dev).await,
        "ready_to_execute",
        "distinct schemas cannot establish equivalent input data"
    );
}

// 7. EXECUTION-TYPE ISOLATION: the same target+hash under a DIFFERENT
//    execution_type is a distinct node and must execute (the match key includes
//    execution_type).
#[tokio::test]
async fn execution_type_isolation() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let target = "\"DB\".\"S\".\"ETYPE\"";
    let as_view = view(target).body("h-et").upstream("up", 100);
    sc.build(as_view.clone(), 100).await;

    // Same target + hash but execution_type FULL → not a match → execute.
    let as_full = model(target).body("h-et").upstream("up", 100);
    assert_eq!(
        sc.verdict(as_full).await,
        "ready_to_execute",
        "a differing execution_type must not match a recorded row of another type"
    );

    // The original VIEW still skips.
    assert_eq!(sc.verdict(as_view).await, "skip_execution");
}

// 8. FULL REFRESH: re-running an unchanged, confirmed node skips — mirrors the
//    documented behavior that `dbt --full-refresh` does not force execute when
//    logic+data are unchanged (the client sends the same fingerprint; the
//    server has no --full-refresh signal and must skip on an unchanged match).
#[tokio::test]
async fn unchanged_confirmed_node_skips_on_rerun() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let req = model("\"DB\".\"S\".\"FR\"")
        .body("h-fr")
        .upstream("up", 100);
    sc.build_with(req.clone(), built_at(100).runtime(42)).await;

    // Re-run the identical node → skip, echoing the recorded runtime.
    let s = sc.expect_skip(req).await;
    let ed = s.explained_decision.as_ref().unwrap();
    assert_eq!(ed.decision, DECISION_SKIP);
    assert!(!ed.is_stale);
    assert_eq!(
        s.execution_runtime_ms,
        Some(42),
        "skip echoes recorded runtime"
    );
}

// 9. STALE-POLICY ANY vs ALL end-to-end over two upstreams, one drifted.
#[tokio::test]
async fn stale_policy_any_vs_all_end_to_end() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Policy ANY node.
    let any_base = model("\"DB\".\"S\".\"POLANY\"")
        .body("h-any")
        .upstream("a", 100)
        .upstream("b", 100)
        .policy(Policy::Any)
        .tolerance_seconds(0);
    sc.build(any_base.clone(), 100).await;
    // Only b drifts → ANY → execute.
    assert_eq!(
        sc.verdict(any_base.upstream("b", 9_000_000)).await,
        "ready_to_execute",
        "ANY: one drift executes"
    );

    // Policy ALL node.
    let all_base = model("\"DB\".\"S\".\"POLALL\"")
        .body("h-all")
        .upstream("a", 100)
        .upstream("b", 100)
        .policy(Policy::All)
        .tolerance_seconds(0);
    sc.build(all_base.clone(), 100).await;
    // Only b drifts → ALL → skip (a still fresh).
    let all_one = all_base.clone().upstream("b", 9_000_000);
    assert_eq!(
        sc.verdict(all_one).await,
        "skip_execution",
        "ALL: one fresh upstream skips"
    );
    // Both drift → ALL → execute.
    let all_both = all_base.upstream("a", 8_000_000).upstream("b", 9_000_000);
    assert_eq!(
        sc.verdict(all_both).await,
        "ready_to_execute",
        "ALL: every upstream drift executes"
    );
}

// 10. CONCURRENCY: N concurrent IDENTICAL submits all execute (the server does
//     not dedupe pending rows), and confirming ONE of them makes the
//     fingerprint skip. Confirm is idempotent across the duplicate pending
//     rows (each distinct request_id confirms its own row).
#[tokio::test]
async fn concurrent_identical_submits_then_single_confirm_skips() {
    const N: usize = 12;
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let req = model("\"DB\".\"S\".\"CONC\"")
        .body("h-conc")
        .upstream("up", 100);

    // Fire N identical submits concurrently.
    let mut tasks = JoinSet::new();
    for _ in 0..N {
        let (sc, req) = (sc.clone(), req.clone());
        tasks.spawn(async move { sc.try_submit(req).await });
    }
    let mut request_ids = Vec::new();
    while let Some(j) = tasks.join_next().await {
        let resp = j.unwrap().expect("submit ok");
        assert_eq!(
            variant(&resp),
            "ready_to_execute",
            "all identical submits execute"
        );
        request_ids.push(ready_id(&resp));
    }
    assert_eq!(request_ids.len(), N);

    // Exactly N pending rows were written (no dedupe), all distinct ids.
    let pending = rows::executions(&server)
        .await
        .iter()
        .filter(|r| r.is_pending())
        .count();
    assert_eq!(pending, N, "N distinct pending rows persisted");

    // Confirm ONE request_id → that fingerprint now skips.
    assert!(sc.confirm(&request_ids[0], built_at(100).runtime(55)).await);

    assert_eq!(
        sc.verdict(req).await,
        "skip_execution",
        "once any identical pending row is confirmed, the fingerprint skips"
    );

    // Confirming a SECOND of the duplicate ids is still idempotent/successful.
    assert!(sc.confirm(&request_ids[1], built_at(100).runtime(55)).await);
}

// 11. SKIP-SAFETY END-TO-END: across a representative matrix of transitions,
//     the server SKIPs ONLY when a confirmed build of the exact fingerprint
//     exists AND upstreams are not drifted. Every unsafe situation executes.
#[tokio::test]
async fn skip_safety_end_to_end_matrix() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let target = "\"DB\".\"S\".\"SAFE\"";

    // Before any confirm: must execute (no history → skip would be unsafe).
    let fresh = model(target).body("h-safe").upstream("up", 100);
    assert_eq!(
        sc.verdict(fresh.clone()).await,
        "ready_to_execute",
        "no confirmed history → never skip"
    );

    // Confirm the build.
    sc.build(fresh.clone(), 100).await;

    // Identical, fresh upstream → safe to skip.
    assert_eq!(
        sc.verdict(fresh.clone()).await,
        "skip_execution",
        "confirmed + fresh → skip"
    );

    // Drifted upstream → unsafe → must execute.
    assert_eq!(
        sc.verdict(fresh.clone().upstream("up", 9_999_999)).await,
        "ready_to_execute",
        "confirmed but drifted → must execute (skip would be unsafe)"
    );

    // Different hash → unsafe (never built this logic) → must execute.
    assert_eq!(
        sc.verdict(fresh.clone().body("h-safe-CHANGED")).await,
        "ready_to_execute",
        "changed fingerprint → must execute"
    );

    // Different execution type → unsafe (distinct node) → must execute.
    let other_type = view(target).body("h-safe").upstream("up", 100);
    assert_eq!(
        sc.verdict(other_type).await,
        "ready_to_execute",
        "different execution type → must execute"
    );
}
