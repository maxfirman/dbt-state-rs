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

use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

// Shared isolated-schema server setup (see tests/support.rs).
#[path = "support.rs"]
mod support;
use support::{channel, request_with_org, response_variant, schema_pool, start_server};

// Decision/rejection constants mirrored from the server (shared.proto + decision.rs)
// so we can assert the exact response field values stay unchanged.
const DECISION_SKIP_EXECUTION: i64 = 0;
const DECISION_READY_TO_EXECUTE: i64 = 1;
const REJECTION_NO_SUITABLE_MATCH_FOUND: i32 = 6;

/// Build a minimal-but-complete SubmitEnrichedSqlRequest for a model node with
/// the given target table and body hash. Enough fields are populated to drive
/// the SKIP/EXECUTE decision; everything else uses proto defaults.
fn model_submit(target_table: &str, body_hash: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target_table.to_string()),
        tolerate_nondeterminism: true,
        tables: vec![qc::TableModifiedInfo {
            name: target_table.into(),
            last_modified_epoch: Some(1),
        }],
        dialect: "snowflake".to_string(),
        execution_type: 10,
        sql: "select 1".to_string(),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: format!("model.jaffle.{target_table}"),
            target_name: "prod".to_string(),
            project_name: "jaffle".to_string(),
            resource_type: "model".to_string(),
            node_hash: body_hash.to_string(),
            node_body_hash: Some(body_hash.to_string()),
            profile_name: "default".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Build a ConfirmExecutionRequest for the given request_id.
fn confirm_req(request_id: &str, runtime_ms: i64) -> qc::ConfirmExecutionRequest {
    qc::ConfirmExecutionRequest {
        request_id: request_id.to_string(),
        last_modified_epoch: Some(1_791_498_542_120),
        failed_to_clone: false,
        table_type: None,
        execution_results: None,
        execution_runtime_ms: Some(runtime_ms),
        labels: Default::default(),
    }
}

fn ready_request_id(resp: &qc::SubmitSqlResponse) -> String {
    match &resp.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => r.request_id.clone(),
        other => panic!("expected ready_to_execute, got {other:?}"),
    }
}

/// Item 1 — ORG ISOLATION: the identical node fingerprint must not leak skip
/// state across organizations. Submit+confirm under org A, then the SAME node
/// under org B must still EXECUTE (B has never seen it); re-submitting under A
/// must SKIP (A's own confirmed history applies).
#[tokio::test]
async fn org_isolation_state_does_not_leak() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let node = model_submit("\"DB\".\"S\".\"SHARED\"", "hash-shared");

    // --- Org A: first submit executes. ---
    let resp_a1 = sql
        .submit_enriched_sql(request_with_org(node.clone(), "A"))
        .await
        .expect("A submit 1")
        .into_inner();
    assert_eq!(
        response_variant(&resp_a1),
        "ready_to_execute",
        "org A's first submit of a new node must execute"
    );
    let req_id_a = ready_request_id(&resp_a1);

    // Confirm under org A so A now has skippable history.
    assert!(
        exec.confirm_execution(request_with_org(confirm_req(&req_id_a, 1000), "A"))
            .await
            .expect("A confirm")
            .into_inner()
            .success,
        "confirm under org A must succeed"
    );

    // --- Org B: the IDENTICAL node must still EXECUTE (state must not leak). ---
    let resp_b1 = sql
        .submit_enriched_sql(request_with_org(node.clone(), "B"))
        .await
        .expect("B submit 1")
        .into_inner();
    assert_eq!(
        response_variant(&resp_b1),
        "ready_to_execute",
        "org B must NOT inherit org A's confirmed state (no cross-org leak)"
    );
    // Response field values must match the standard execute shape.
    if let Some(qc::submit_sql_response::Response::ReadyToExecute(r)) = &resp_b1.response {
        let ed = r.explained_decision.as_ref().expect("explained_decision");
        assert_eq!(ed.decision as i64, DECISION_READY_TO_EXECUTE);
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
    let resp_a2 = sql
        .submit_enriched_sql(request_with_org(node.clone(), "A"))
        .await
        .expect("A submit 2")
        .into_inner();
    assert_eq!(
        response_variant(&resp_a2),
        "skip_execution",
        "org A must skip its own previously-confirmed node"
    );
    if let Some(qc::submit_sql_response::Response::SkipExecution(s)) = &resp_a2.response {
        let ed = s.explained_decision.as_ref().expect("explained_decision");
        assert_eq!(ed.decision as i64, DECISION_SKIP_EXECUTION);
    }
}

/// Item 2a — INVALID INPUT: a near-empty SubmitEnrichedSQL (only dialect set,
/// no target_table, no dbt_node_state) must NOT panic. It must return Ok with a
/// ready_to_execute (empty-state execute), because there is no confirmed match.
#[tokio::test]
async fn near_empty_submit_executes_without_panic() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch);

    let req = qc::SubmitEnrichedSqlRequest {
        dialect: "snowflake".to_string(),
        ..Default::default()
    };

    let resp = sql
        .submit_enriched_sql(req)
        .await
        .expect("near-empty submit must return Ok, not error")
        .into_inner();
    assert_eq!(
        response_variant(&resp),
        "ready_to_execute",
        "an empty-state submit has no confirmed match and must execute"
    );
    if let Some(qc::submit_sql_response::Response::ReadyToExecute(r)) = &resp.response {
        assert!(!r.request_id.is_empty(), "a pending request_id is issued");
    }
}

/// Item 2b — INVALID INPUT: ConfirmExecution for an unknown request_id must
/// return success=false (and must not error), echoing the request_id back.
#[tokio::test]
async fn confirm_unknown_request_id_returns_false() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut exec = ExecutionClient::new(ch);

    let resp = exec
        .confirm_execution(confirm_req("does-not-exist", 123))
        .await
        .expect("confirm of unknown id must return Ok")
        .into_inner();
    assert!(!resp.success, "unknown request_id must not confirm");
    assert_eq!(resp.request_id, "does-not-exist");
}

/// Item 3 — CONFIRM IDEMPOTENCY: calling ConfirmExecution twice for the same
/// request_id must both return success=true (the row exists and stays
/// confirmed). Verifies the current store.confirm behavior is idempotent.
#[tokio::test]
async fn confirm_is_idempotent_for_duplicate_request_id() {
    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Execute to create a pending row.
    let resp = sql
        .submit_enriched_sql(model_submit("\"DB\".\"S\".\"IDEMP\"", "hash-idemp"))
        .await
        .expect("submit")
        .into_inner();
    let req_id = ready_request_id(&resp);

    // First confirm succeeds.
    let c1 = exec
        .confirm_execution(confirm_req(&req_id, 500))
        .await
        .expect("confirm 1")
        .into_inner();
    assert!(c1.success, "first confirm must succeed");

    // Second confirm for the SAME request_id must also succeed (idempotent).
    let c2 = exec
        .confirm_execution(confirm_req(&req_id, 999))
        .await
        .expect("confirm 2")
        .into_inner();
    assert!(
        c2.success,
        "confirming an already-confirmed request_id must still return true"
    );

    // Exactly one row should still exist for this request_id (no duplication).
    let pool = schema_pool(&schema).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM executions WHERE request_id = $1")
        .bind(&req_id)
        .fetch_one(&pool)
        .await
        .expect("count rows");
    assert_eq!(count, 1, "the request_id row must remain unique");
}

/// Item 4 — CONCURRENCY: fire N=20 concurrent submits for DISTINCT nodes
/// against one server instance. All must succeed and return ready_to_execute,
/// and the DB must end with exactly 20 pending rows.
#[tokio::test]
async fn concurrent_distinct_submits_all_execute() {
    const N: usize = 20;

    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;

    // Spawn N tasks, each sharing the same underlying channel (cheaply cloned),
    // issuing a submit for a DISTINCT node concurrently.
    let mut tasks = JoinSet::new();
    for i in 0..N {
        let mut sql = SqlClient::new(ch.clone());
        tasks.spawn(async move {
            let req = model_submit(&format!("\"DB\".\"S\".\"T{i}\""), &format!("hash-{i}"));
            sql.submit_enriched_sql(req).await
        });
    }

    let mut executed = 0usize;
    while let Some(joined) = tasks.join_next().await {
        let resp = joined
            .expect("task must not panic")
            .expect("concurrent submit must succeed")
            .into_inner();
        assert_eq!(
            response_variant(&resp),
            "ready_to_execute",
            "every distinct new node must execute"
        );
        executed += 1;
    }
    assert_eq!(executed, N, "all {N} submits must return ready_to_execute");

    // The DB must hold exactly N pending rows.
    let pool = schema_pool(&schema).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM executions WHERE status = 'pending'")
        .fetch_one(&pool)
        .await
        .expect("count pending");
    assert_eq!(
        count, N as i64,
        "exactly {N} pending rows must be persisted"
    );
}

/// Item 5 — METADATA PASSTHROUGH: a request with NO metadata must still work;
/// the org defaults to "local". Submit+confirm with no metadata, then an
/// identical no-metadata submit must SKIP (proving it mapped to a stable org).
#[tokio::test]
async fn no_metadata_defaults_to_local_org() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let node = model_submit("\"DB\".\"S\".\"NOMETA\"", "hash-nometa");

    // Plain request, no metadata set at all.
    let resp1 = sql
        .submit_enriched_sql(node.clone())
        .await
        .expect("no-metadata submit must work")
        .into_inner();
    assert_eq!(
        response_variant(&resp1),
        "ready_to_execute",
        "first no-metadata submit executes"
    );
    let req_id = ready_request_id(&resp1);

    // Confirm (also with no metadata).
    assert!(
        exec.confirm_execution(confirm_req(&req_id, 42))
            .await
            .expect("no-metadata confirm")
            .into_inner()
            .success
    );

    // Identical no-metadata submit must now SKIP: the default org is stable.
    let resp2 = sql
        .submit_enriched_sql(node)
        .await
        .expect("no-metadata submit 2")
        .into_inner();
    assert_eq!(
        response_variant(&resp2),
        "skip_execution",
        "a stable default org ('local') must allow the skip on re-submit"
    );
}
