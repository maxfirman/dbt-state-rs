//! Robustness integration tests for the dbt State gRPC server.
//!
//! These exercise correctness and resilience properties that the differential
//! corpus does not cover directly:
//!   1. Per-org state isolation (state must not leak across organizations).
//!   2. Graceful handling of near-empty / invalid SubmitEnrichedSQL input and
//!      unknown ConfirmExecution request ids.
//!   3. ConfirmExecution idempotency for a duplicate request_id.
//!   4. Concurrency: many simultaneous distinct submits all succeed.
//!   5. Metadata passthrough: no metadata is required (org defaults to "local").
//!
//! Each test runs against a freshly-created isolated Postgres schema via the
//! shared `support` helpers. Response shapes/values are asserted to match the
//! existing server behavior (decision codes, rejection reasons, etc.).

use dbt_state_proto::query_cache as qc;
use tokio::task::JoinSet;

// Shared isolated-schema server setup and scenario helpers (see tests/support.rs).
#[path = "support.rs"]
mod support;
use support::rows;
use support::scenario::{
    empty_submission, into_execute, into_skip, outcome, ready_id, variant, view, Scenario,
};
use support::start_server;

// Decision/rejection values from the generated proto enums, so we can assert
// the exact response field values stay unchanged.
const DECISION_SKIP_EXECUTION: i32 = qc::SubmitSqlResultType::SkipExecution as i32;
const DECISION_READY_TO_EXECUTE: i32 = qc::SubmitSqlResultType::ReadyToExecute as i32;
const REJECTION_NO_SUITABLE_MATCH_FOUND: i32 = qc::RejectionReason::NoSuitableMatchFound as i32;

/// The object epoch these tests confirm.
const CONFIRMED_EPOCH: i64 = 1_791_498_542_120;

/// Item 1 — ORG ISOLATION: the identical node fingerprint must not leak skip
/// state across organizations. Submit+confirm under org A, then the SAME node
/// under org B must still EXECUTE (B has never seen it); re-submitting under A
/// must SKIP (A's own confirmed history applies).
#[tokio::test]
async fn org_isolation_state_does_not_leak() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let (a, b) = (sc.org("A"), sc.org("B"));

    let node = view("\"DB\".\"S\".\"SHARED\"").body("hash-shared");

    // --- Org A: first submit executes. ---
    let resp_a1 = a.submit(node.clone()).await;
    assert_eq!(
        variant(&resp_a1),
        "ready_to_execute",
        "org A's first submit of a new node must execute"
    );
    let req_id_a = ready_id(&resp_a1);

    // Confirm under org A so A now has skippable history.
    assert!(
        a.confirm(&req_id_a, outcome().epoch(CONFIRMED_EPOCH).runtime(1000))
            .await,
        "confirm under org A must succeed"
    );

    // --- Org B: the IDENTICAL node must still EXECUTE (state must not leak). ---
    let resp_b1 = b.submit(node.clone()).await;
    assert_eq!(
        variant(&resp_b1),
        "ready_to_execute",
        "org B must NOT inherit org A's confirmed state (no cross-org leak)"
    );
    // Response field values must match the standard execute shape.
    {
        let r = into_execute(resp_b1);
        let ed = r.explained_decision.as_ref().expect("explained_decision");
        assert_eq!(ed.decision, DECISION_READY_TO_EXECUTE);
        assert_eq!(
            ed.skip_rejection_reason,
            Some(REJECTION_NO_SUITABLE_MATCH_FOUND)
        );
        assert_eq!(
            ed.clone_rejection_reason,
            Some(REJECTION_NO_SUITABLE_MATCH_FOUND)
        );
        assert!(
            r.execution_decision_id.is_some(),
            "execution_decision_id present"
        );
    }

    // --- Org A again: now SKIP, A's history is intact and unaffected by B. ---
    let resp_a2 = a.submit(node.clone()).await;
    assert_eq!(
        variant(&resp_a2),
        "skip_execution",
        "org A must skip its own previously-confirmed node"
    );
    let s = into_skip(resp_a2);
    let ed = s.explained_decision.as_ref().expect("explained_decision");
    assert_eq!(ed.decision, DECISION_SKIP_EXECUTION);
}

/// Item 2a — INVALID INPUT: a near-empty SubmitEnrichedSQL must NOT panic.
/// With only `dialect` set it has no target, so it is rejected with
/// INVALID_ARGUMENT (#81: only data tests may omit the target). With a target
/// and nothing else it returns Ok with a ready_to_execute (empty-state
/// execute), because there is no confirmed match.
#[tokio::test]
async fn near_empty_submit_executes_without_panic() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let status = sc
        .try_submit(empty_submission("snowflake"))
        .await
        .expect_err("a near-empty submit without a target must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");

    let resp = sc
        .try_submit(empty_submission("snowflake").target("\"DB\".\"S\".\"EMPTY\""))
        .await
        .expect("near-empty submit with a target must return Ok, not error");
    assert_eq!(
        variant(&resp),
        "ready_to_execute",
        "an empty-state submit has no confirmed match and must execute"
    );
    let r = into_execute(resp);
    assert!(!r.request_id.is_empty(), "a pending request_id is issued");
}

/// Captured malformed UUID input is rejected without publishing anything.
#[tokio::test]
async fn confirm_malformed_request_id_returns_captured_database_error() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let resp = sc
        .try_confirm(
            outcome()
                .epoch(CONFIRMED_EPOCH)
                .runtime(123)
                .confirm("does-not-exist"),
        )
        .await
        .expect_err("malformed request ID is rejected");
    assert_eq!(resp.code(), tonic::Code::Internal);
    assert_eq!(resp.message(), "Database operation failed");
}

/// Item 3 — CONFIRM IDEMPOTENCY: calling ConfirmExecution twice for the same
/// request_id must preserve its first immutable publication. The second call
/// returns the captured database error.
#[tokio::test]
async fn confirm_is_idempotent_for_duplicate_request_id() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Execute to create a pending row.
    let req_id = sc
        .execute_id(view("\"DB\".\"S\".\"IDEMP\"").body("hash-idemp"))
        .await;

    // First confirm succeeds.
    assert!(
        sc.confirm(&req_id, outcome().epoch(CONFIRMED_EPOCH).runtime(500))
            .await,
        "first confirm must succeed"
    );

    // A duplicate fails with the captured status, without republishing.
    let error = sc
        .try_confirm(
            outcome()
                .epoch(CONFIRMED_EPOCH)
                .runtime(999)
                .confirm(&req_id),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    assert_eq!(error.message(), "Database operation failed");

    // Exactly one row should still exist for this request_id (no duplication).
    let count = rows::executions(&server)
        .await
        .iter()
        .filter(|r| r.request_id == req_id)
        .count();
    assert_eq!(count, 1, "the request_id row must remain unique");
}

/// Item 4 — CONCURRENCY: fire N=20 concurrent submits for DISTINCT nodes
/// against one server instance. All must succeed and return ready_to_execute,
/// and the DB must end with exactly 20 pending rows.
#[tokio::test]
async fn concurrent_distinct_submits_all_execute() {
    const N: usize = 20;

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Spawn N tasks, each sharing the same underlying channel (cheaply cloned),
    // issuing a submit for a DISTINCT node concurrently.
    let mut tasks = JoinSet::new();
    for i in 0..N {
        let sc = sc.clone();
        tasks.spawn(async move {
            let req = view(&format!("\"DB\".\"S\".\"T{i}\"")).body(&format!("hash-{i}"));
            sc.try_submit(req).await
        });
    }

    let mut executed = 0usize;
    while let Some(joined) = tasks.join_next().await {
        let resp = joined
            .expect("task must not panic")
            .expect("concurrent submit must succeed");
        assert_eq!(
            variant(&resp),
            "ready_to_execute",
            "every distinct new node must execute"
        );
        executed += 1;
    }
    assert_eq!(executed, N, "all {N} submits must return ready_to_execute");

    // The DB must hold exactly N pending rows.
    let count = rows::executions(&server)
        .await
        .iter()
        .filter(|r| r.is_pending())
        .count();
    assert_eq!(count, N, "exactly {N} pending rows must be persisted");
}

/// Item 5 — METADATA PASSTHROUGH: a request with NO metadata must still work;
/// the org defaults to "local". Submit+confirm with no metadata, then an
/// identical no-metadata submit must SKIP (proving it mapped to a stable org).
#[tokio::test]
async fn no_metadata_defaults_to_local_org() {
    let server = start_server().await;
    // No `.org(..)`: plain requests with no metadata set at all.
    let sc = Scenario::connect(&server).await;

    let node = view("\"DB\".\"S\".\"NOMETA\"").body("hash-nometa");

    let resp1 = sc
        .try_submit(node.clone())
        .await
        .expect("no-metadata submit must work");
    assert_eq!(
        variant(&resp1),
        "ready_to_execute",
        "first no-metadata submit executes"
    );
    let req_id = ready_id(&resp1);

    // Confirm (also with no metadata).
    assert!(
        sc.confirm(&req_id, outcome().epoch(CONFIRMED_EPOCH).runtime(42))
            .await
    );

    // Identical no-metadata submit must now SKIP: the default org is stable.
    let resp2 = sc.try_submit(node).await.expect("no-metadata submit 2");
    assert_eq!(
        variant(&resp2),
        "skip_execution",
        "a stable default org ('local') must allow the skip on re-submit"
    );
}
