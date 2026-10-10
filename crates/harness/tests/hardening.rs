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

use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

#[path = "support.rs"]
mod support;
use support::{channel, response_variant, schema_pool, start_server};

const ETYPE_FULL: i32 = 1;
const ETYPE_VIEW: i32 = 10;
const DECISION_SKIP: i64 = 0;
const DECISION_EXECUTE: i64 = 1;

/// A minimal model submit with a target, body hash, execution type and
/// upstream tables.
///
/// NOTE: the hosted service's logic identity is the (whitespace-normalized)
/// rendered SQL + allowlisted `semantic_extras`, NOT `node_body_hash` (which it
/// ignores for reuse). So we derive the `sql` field from `body_hash`: a distinct
/// logical identity produces distinct SQL, exactly as real dbt compiles distinct
/// logic to distinct SQL. This keeps "changed logic → rebuild" tests faithful.
fn submit(
    target: &str,
    body_hash: &str,
    execution_type: i32,
    tables: &[(&str, i64)],
) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.to_string()),
        tolerate_nondeterminism: true,
        dialect: "snowflake".to_string(),
        execution_type,
        sql: format!("select 1 as c_{body_hash}"),
        tables: std::iter::once(qc::TableModifiedInfo {
            name: target.into(),
            last_modified_epoch: Some(1),
        })
        .chain(tables.iter().map(|(n, e)| qc::TableModifiedInfo {
            name: n.to_string(),
            last_modified_epoch: Some(*e),
        }))
        .collect(),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: format!("model.jaffle.{target}"),
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

fn with_policy(
    mut req: qc::SubmitEnrichedSqlRequest,
    policy: i32,
    tol_s: i64,
) -> qc::SubmitEnrichedSqlRequest {
    req.stale_upstream_policy = policy;
    req.freshness_tolerance_seconds = tol_s;
    req
}

fn confirm_req(request_id: &str, epoch: i64, runtime_ms: i64) -> qc::ConfirmExecutionRequest {
    qc::ConfirmExecutionRequest {
        request_id: request_id.to_string(),
        last_modified_epoch: Some(epoch),
        failed_to_clone: false,
        table_type: Some("TABLE".to_string()),
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

/// Execute → confirm helper. Returns the request_id used.
async fn execute_and_confirm(
    sql: &mut SqlClient<tonic::transport::Channel>,
    exec: &mut ExecutionClient<tonic::transport::Channel>,
    req: &qc::SubmitEnrichedSqlRequest,
    epoch: i64,
    runtime_ms: i64,
) -> String {
    let resp = sql
        .submit_enriched_sql(req.clone())
        .await
        .expect("submit")
        .into_inner();
    assert_eq!(
        response_variant(&resp),
        "ready_to_execute",
        "first submit must execute"
    );
    let rid = ready_request_id(&resp);
    let c = exec
        .confirm_execution(confirm_req(&rid, epoch, runtime_ms))
        .await
        .expect("confirm")
        .into_inner();
    assert!(c.success, "confirm must succeed");
    rid
}

// 1. FAILED RUN: executed but NEVER confirmed → the next identical submit must
//    STILL EXECUTE. A failed build left no confirmed state; skipping would
//    reference an object that was never created.
#[tokio::test]
async fn unconfirmed_execution_never_skips() {
    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());

    let req = submit(
        "\"DB\".\"S\".\"FAILED\"",
        "h-failed",
        ETYPE_FULL,
        &[("up", 100)],
    );

    // First submit executes (pending row written), but we DO NOT confirm —
    // simulating a build that errored before ConfirmExecution.
    let r1 = sql
        .submit_enriched_sql(req.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&r1), "ready_to_execute");
    let _ = ready_request_id(&r1);

    // Second identical submit must STILL execute (no confirmed history).
    let r2 = sql
        .submit_enriched_sql(req.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&r2),
        "ready_to_execute",
        "an unconfirmed (failed) run must never produce a skip"
    );

    // There must be zero confirmed rows for this fingerprint.
    let pool = schema_pool(&schema).await;
    let confirmed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM executions WHERE status='confirmed' AND node_body_hash=$1",
    )
    .bind("h-failed")
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        confirmed, 0,
        "a failed/unconfirmed run leaves no confirmed state"
    );
}

// 2. CONFIRMING THE WRONG request_id must not confirm the original fingerprint:
//    the original still executes.
#[tokio::test]
async fn confirming_wrong_request_id_leaves_original_pending() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let req = submit(
        "\"DB\".\"S\".\"WRONGID\"",
        "h-wrong",
        ETYPE_FULL,
        &[("up", 100)],
    );
    let r1 = sql
        .submit_enriched_sql(req.clone())
        .await
        .unwrap()
        .into_inner();
    let _rid = ready_request_id(&r1);

    // Confirm a DIFFERENT, unknown id → success=false.
    let c = exec
        .confirm_execution(confirm_req("totally-unrelated-id", 100, 10))
        .await
        .unwrap()
        .into_inner();
    assert!(!c.success, "confirming an unknown id must not succeed");

    // The original fingerprint is still unconfirmed → executes again.
    let r2 = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    assert_eq!(response_variant(&r2), "ready_to_execute");
}

// 3. CHANGED CONTRACT / CHANGED SQL: confirm a build, then the SAME node with a
//    DIFFERENT body hash (edited SQL / changed contract) must REBUILD, and can
//    itself be confirmed; the original hash still skips.
#[tokio::test]
async fn changed_contract_forces_rebuild() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"CONTRACT\"";
    let v1 = submit(target, "hash-v1", ETYPE_FULL, &[("up", 100)]);
    execute_and_confirm(&mut sql, &mut exec, &v1, 100, 10).await;

    // Same node, edited SQL → new body hash → must execute (hash miss).
    let v2 = submit(target, "hash-v2", ETYPE_FULL, &[("up", 100)]);
    let r2 = sql
        .submit_enriched_sql(v2.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&r2),
        "ready_to_execute",
        "a changed body hash must rebuild"
    );
    // Confirm v2.
    let rid2 = ready_request_id(&r2);
    assert!(
        exec.confirm_execution(confirm_req(&rid2, 200, 20))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // The original history no longer describes the overwritten target.
    let r1_again = sql.submit_enriched_sql(v1).await.unwrap().into_inner();
    assert_eq!(response_variant(&r1_again), "ready_to_execute");
    // And v2 now skips too.
    let r2_again = sql.submit_enriched_sql(v2).await.unwrap().into_inner();
    assert_eq!(response_variant(&r2_again), "skip_execution");
}

// 4. MODIFIED UPSTREAM: confirm a build, then a NEWER upstream forces a stale
//    rebuild; reverting the upstream to the recorded epoch skips again.
#[tokio::test]
async fn modified_upstream_forces_stale_rebuild_then_recovers() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"UP\"";
    let base = submit(target, "h-up", ETYPE_FULL, &[("src", 1_000_000)]);
    execute_and_confirm(&mut sql, &mut exec, &base, 1_000_000, 10).await;

    // Upstream moved far newer → stale → execute.
    let moved = submit(target, "h-up", ETYPE_FULL, &[("src", 9_000_000)]);
    let r = sql.submit_enriched_sql(moved).await.unwrap().into_inner();
    match &r.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => {
            let ed = x.explained_decision.as_ref().unwrap();
            assert_eq!(ed.decision as i64, DECISION_EXECUTE);
            assert!(ed.is_stale, "upstream drift must set is_stale=true");
        }
        other => panic!("expected stale execute, got {other:?}"),
    }

    // Upstream reverts to the recorded epoch → fresh → skip (confirmed row
    // still present; we never confirmed the stale rebuild).
    let reverted = submit(target, "h-up", ETYPE_FULL, &[("src", 1_000_000)]);
    let r2 = sql
        .submit_enriched_sql(reverted)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&r2), "skip_execution");
}

// 5. ADDED then DELETED upstream.
#[tokio::test]
async fn added_then_deleted_upstream() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"ADDDROP\"";
    let base = submit(target, "h-ad", ETYPE_FULL, &[("a", 100)]);
    execute_and_confirm(&mut sql, &mut exec, &base, 100, 10).await;

    // Add a brand-new upstream, newer than the build → execute.
    let added = submit(
        target,
        "h-ad",
        ETYPE_FULL,
        &[("a", 100), ("b_new", 9_000_000)],
    );
    let r = sql.submit_enriched_sql(added).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r),
        "ready_to_execute",
        "a new, newer, untracked upstream must execute"
    );

    // Missing all recorded upstreams cannot establish freshness.
    let dropped = submit(target, "h-ad", ETYPE_FULL, &[]);
    let r2 = sql.submit_enriched_sql(dropped).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r2),
        "ready_to_execute",
        "missing dependency evidence requires execution"
    );
}

// 6. SCHEMA CHANGE: identical timestamps do not prove equivalent objects.
#[tokio::test]
async fn cross_schema_upstream_requires_provenance() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"XSCHEMA\"";
    let prod = submit(
        target,
        "h-xs",
        ETYPE_FULL,
        &[("\"DB\".\"PROD\".\"SRC\"", 1_000_000)],
    );
    execute_and_confirm(&mut sql, &mut exec, &prod, 1_000_000, 10).await;

    // Different physical input under a different schema requires execution.
    let dev = submit(
        target,
        "h-xs",
        ETYPE_FULL,
        &[("\"DB\".\"DEV\".\"SRC\"", 1_000_000)],
    );
    let r = sql.submit_enriched_sql(dev).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r),
        "ready_to_execute",
        "distinct schemas cannot establish equivalent input data"
    );
}

// 7. EXECUTION-TYPE ISOLATION: the same target+hash under a DIFFERENT
//    execution_type is a distinct node and must execute (the match key includes
//    execution_type).
#[tokio::test]
async fn execution_type_isolation() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"ETYPE\"";
    let as_view = submit(target, "h-et", ETYPE_VIEW, &[("up", 100)]);
    execute_and_confirm(&mut sql, &mut exec, &as_view, 100, 10).await;

    // Same target + hash but execution_type FULL → not a match → execute.
    let as_full = submit(target, "h-et", ETYPE_FULL, &[("up", 100)]);
    let r = sql.submit_enriched_sql(as_full).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r),
        "ready_to_execute",
        "a differing execution_type must not match a recorded row of another type"
    );

    // The original VIEW still skips.
    let r_view = sql.submit_enriched_sql(as_view).await.unwrap().into_inner();
    assert_eq!(response_variant(&r_view), "skip_execution");
}

// 8. FULL REFRESH: re-running an unchanged, confirmed node skips — mirrors the
//    documented behavior that `dbt --full-refresh` does not force execute when
//    logic+data are unchanged (the client sends the same fingerprint; the
//    server has no --full-refresh signal and must skip on an unchanged match).
#[tokio::test]
async fn unchanged_confirmed_node_skips_on_rerun() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let req = submit("\"DB\".\"S\".\"FR\"", "h-fr", ETYPE_FULL, &[("up", 100)]);
    execute_and_confirm(&mut sql, &mut exec, &req, 100, 42).await;

    // Re-run the identical node → skip, echoing the recorded runtime.
    let r = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    match r.response {
        Some(qc::submit_sql_response::Response::SkipExecution(s)) => {
            let ed = s.explained_decision.as_ref().unwrap();
            assert_eq!(ed.decision as i64, DECISION_SKIP);
            assert!(!ed.is_stale);
            assert_eq!(
                s.execution_runtime_ms,
                Some(42),
                "skip echoes recorded runtime"
            );
        }
        other => panic!("expected skip, got {other:?}"),
    }
}

// 9. STALE-POLICY ANY vs ALL end-to-end over two upstreams, one drifted.
#[tokio::test]
async fn stale_policy_any_vs_all_end_to_end() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Policy ANY node.
    let any_target = "\"DB\".\"S\".\"POLANY\"";
    let any_base = with_policy(
        submit(any_target, "h-any", ETYPE_FULL, &[("a", 100), ("b", 100)]),
        0, // ANY
        0,
    );
    execute_and_confirm(&mut sql, &mut exec, &any_base, 100, 10).await;
    // Only b drifts → ANY → execute.
    let any_drift = with_policy(
        submit(
            any_target,
            "h-any",
            ETYPE_FULL,
            &[("a", 100), ("b", 9_000_000)],
        ),
        0,
        0,
    );
    let ra = sql
        .submit_enriched_sql(any_drift)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&ra),
        "ready_to_execute",
        "ANY: one drift executes"
    );

    // Policy ALL node.
    let all_target = "\"DB\".\"S\".\"POLALL\"";
    let all_base = with_policy(
        submit(all_target, "h-all", ETYPE_FULL, &[("a", 100), ("b", 100)]),
        1, // ALL
        0,
    );
    execute_and_confirm(&mut sql, &mut exec, &all_base, 100, 10).await;
    // Only b drifts → ALL → skip (a still fresh).
    let all_one = with_policy(
        submit(
            all_target,
            "h-all",
            ETYPE_FULL,
            &[("a", 100), ("b", 9_000_000)],
        ),
        1,
        0,
    );
    let rb = sql.submit_enriched_sql(all_one).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&rb),
        "skip_execution",
        "ALL: one fresh upstream skips"
    );
    // Both drift → ALL → execute.
    let all_both = with_policy(
        submit(
            all_target,
            "h-all",
            ETYPE_FULL,
            &[("a", 8_000_000), ("b", 9_000_000)],
        ),
        1,
        0,
    );
    let rc = sql
        .submit_enriched_sql(all_both)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&rc),
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
    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;

    let req = submit(
        "\"DB\".\"S\".\"CONC\"",
        "h-conc",
        ETYPE_FULL,
        &[("up", 100)],
    );

    // Fire N identical submits concurrently.
    let mut tasks = JoinSet::new();
    for _ in 0..N {
        let mut sql = SqlClient::new(ch.clone());
        let req = req.clone();
        tasks.spawn(async move { sql.submit_enriched_sql(req).await });
    }
    let mut request_ids = Vec::new();
    while let Some(j) = tasks.join_next().await {
        let resp = j.unwrap().expect("submit ok").into_inner();
        assert_eq!(
            response_variant(&resp),
            "ready_to_execute",
            "all identical submits execute"
        );
        request_ids.push(ready_request_id(&resp));
    }
    assert_eq!(request_ids.len(), N);

    // Exactly N pending rows were written (no dedupe), all distinct ids.
    let pool = schema_pool(&schema).await;
    let pending: i64 = sqlx::query_scalar("SELECT count(*) FROM executions WHERE status='pending'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(pending, N as i64, "N distinct pending rows persisted");

    // Confirm ONE request_id → that fingerprint now skips.
    let mut exec = ExecutionClient::new(ch.clone());
    assert!(
        exec.confirm_execution(confirm_req(&request_ids[0], 100, 55))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    let mut sql = SqlClient::new(ch);
    let r = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r),
        "skip_execution",
        "once any identical pending row is confirmed, the fingerprint skips"
    );

    // Confirming a SECOND of the duplicate ids is still idempotent/successful.
    assert!(
        exec.confirm_execution(confirm_req(&request_ids[1], 100, 55))
            .await
            .unwrap()
            .into_inner()
            .success
    );
}

// 11. SKIP-SAFETY END-TO-END: across a representative matrix of transitions,
//     the server SKIPs ONLY when a confirmed build of the exact fingerprint
//     exists AND upstreams are not drifted. Every unsafe situation executes.
#[tokio::test]
async fn skip_safety_end_to_end_matrix() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"SAFE\"";

    // Before any confirm: must execute (no history → skip would be unsafe).
    let fresh = submit(target, "h-safe", ETYPE_FULL, &[("up", 100)]);
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(fresh.clone())
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_execute",
        "no confirmed history → never skip"
    );

    // Confirm the build.
    execute_and_confirm(&mut sql, &mut exec, &fresh, 100, 10).await;

    // Identical, fresh upstream → safe to skip.
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(fresh.clone())
                .await
                .unwrap()
                .into_inner()
        ),
        "skip_execution",
        "confirmed + fresh → skip"
    );

    // Drifted upstream → unsafe → must execute.
    let drifted = submit(target, "h-safe", ETYPE_FULL, &[("up", 9_999_999)]);
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(drifted).await.unwrap().into_inner()),
        "ready_to_execute",
        "confirmed but drifted → must execute (skip would be unsafe)"
    );

    // Different hash → unsafe (never built this logic) → must execute.
    let other_hash = submit(target, "h-safe-CHANGED", ETYPE_FULL, &[("up", 100)]);
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(other_hash)
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_execute",
        "changed fingerprint → must execute"
    );

    // Different execution type → unsafe (distinct node) → must execute.
    let other_type = submit(target, "h-safe", ETYPE_VIEW, &[("up", 100)]);
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(other_type)
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_execute",
        "different execution type → must execute"
    );
}
