//! Correctness integration tests surfaced by the review (F2/F3/F6).
//!
//!   F3  — ConfirmExecution semantics: pending-only outcome write, idempotent
//!         success on re-confirm, no silent mutation of recorded history, and
//!         org-scoping of the confirm.
//!   F2  — Match-key SELECTION (the lookup layer, not just `decide`): namespace
//!         vs physical precedence, execution_type isolation on the namespace
//!         path, and deterministic newest-wins tie-break.
//!   F6  — RecordExecutions: atomic all-or-nothing batch (mid-batch failure
//!         rolls back), and duplicate-fingerprint hydration behavior.
//!
//! Each test runs against a freshly-created isolated Postgres schema.

use dbt_state_proto::query_cache as qc;

use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

#[path = "support.rs"]
mod support;
use support::{channel, request_with_org, response_variant, schema_pool, start_server};

const ETYPE_FULL: i32 = 1;
const ETYPE_VIEW: i32 = 10;

fn submit(
    target: &str,
    body_hash: &str,
    execution_type: i32,
    namespace: Option<&str>,
    tables: &[(&str, i64)],
) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.to_string()),
        dialect: "snowflake".to_string(),
        execution_type,
        sql: "select 1".to_string(),
        table_namespace: namespace.map(|s| s.to_string()),
        tables: tables
            .iter()
            .map(|(n, e)| qc::TableModifiedInfo {
                name: n.to_string(),
                last_modified_epoch: Some(*e),
            })
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

// ---------------------------------------------------------------------------
// F3 — ConfirmExecution semantics
// ---------------------------------------------------------------------------

/// A second confirm of the SAME request_id with DIFFERENT outcome values must
/// return success=true (idempotent) but MUST NOT overwrite the recorded
/// last_modified_epoch / runtime from the first confirm. A late/duplicate
/// confirm cannot silently rewrite skippable history.
#[tokio::test]
async fn reconfirm_does_not_mutate_recorded_outcome() {
    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let req = submit(
        "\"DB\".\"S\".\"RECONF\"",
        "h-reconf",
        ETYPE_FULL,
        None,
        &[("up", 100)],
    );
    let r = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    let rid = ready_request_id(&r);

    // First confirm records epoch=1000, runtime=11.
    assert!(
        exec.confirm_execution(confirm_req(&rid, 1000, 11))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // Second confirm with DIFFERENT values must still succeed (idempotent)...
    assert!(
        exec.confirm_execution(confirm_req(&rid, 9999, 99))
            .await
            .unwrap()
            .into_inner()
            .success,
        "re-confirming an already-confirmed id must return success=true"
    );

    // ...but the stored outcome must be the FIRST confirm's values.
    let pool = schema_pool(&schema).await;
    let (epoch, runtime): (Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT last_modified_epoch, execution_runtime_ms FROM executions WHERE request_id=$1",
    )
    .bind(&rid)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        epoch,
        Some(1000),
        "recorded epoch must be stable across re-confirm"
    );
    assert_eq!(
        runtime,
        Some(11),
        "recorded runtime must be stable across re-confirm"
    );
}

/// Confirm is org-scoped: confirming a request_id under the WRONG org must not
/// succeed, and must leave the original (correct-org) pending row unconfirmed.
#[tokio::test]
async fn confirm_is_org_scoped() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let node = submit(
        "\"DB\".\"S\".\"ORGSCOPE\"",
        "h-org",
        ETYPE_FULL,
        None,
        &[("up", 100)],
    );
    let r = sql
        .submit_enriched_sql(request_with_org(node.clone(), "orgA"))
        .await
        .unwrap()
        .into_inner();
    let rid = ready_request_id(&r);

    // Confirm under the WRONG org → must not succeed (row belongs to orgA).
    let wrong = exec
        .confirm_execution(request_with_org(confirm_req(&rid, 1000, 10), "orgB"))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !wrong.success,
        "confirm under a different org must not succeed"
    );

    // The orgA fingerprint is still unconfirmed → re-submit under orgA executes.
    let again = sql
        .submit_enriched_sql(request_with_org(node, "orgA"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&again),
        "ready_to_execute",
        "a cross-org confirm must not confirm the orgA row"
    );
}

// ---------------------------------------------------------------------------
// F2 — Match-key SELECTION (lookup layer)
// ---------------------------------------------------------------------------

/// Namespace match takes precedence over physical target: a confirmed row under
/// a DIFFERENT physical target but the SAME table_namespace + body_hash +
/// execution_type must be reused (cross-environment skip), even though no row
/// matches the new physical target.
#[tokio::test]
async fn namespace_match_wins_over_physical_target() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let ns = "ns-shared";
    // Build + confirm under the PROD physical target.
    let prod = submit(
        "\"DB\".\"PROD\".\"T\"",
        "h-ns",
        ETYPE_FULL,
        Some(ns),
        &[("\"DB\".\"PROD\".\"SRC\"", 100)],
    );
    let r = sql.submit_enriched_sql(prod).await.unwrap().into_inner();
    let rid = ready_request_id(&r);
    assert!(
        exec.confirm_execution(confirm_req(&rid, 100, 10))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // Submit under a DIFFERENT physical target (DEV), same namespace+hash, same
    // logical upstream at the same epoch → must SKIP via namespace reuse.
    let dev = submit(
        "\"DB\".\"DEV\".\"T\"",
        "h-ns",
        ETYPE_FULL,
        Some(ns),
        &[("\"DB\".\"DEV\".\"SRC\"", 100)],
    );
    let r2 = sql.submit_enriched_sql(dev).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r2),
        "skip_execution",
        "same namespace+hash under a new physical target must reuse (skip)"
    );
}

/// Execution-type isolation on the NAMESPACE path: a confirmed row under one
/// execution_type must NOT be reused for a submit with the same namespace+hash
/// but a different execution_type.
#[tokio::test]
async fn namespace_match_respects_execution_type() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let ns = "ns-etype";
    let as_view = submit(
        "\"DB\".\"PROD\".\"T\"",
        "h-et",
        ETYPE_VIEW,
        Some(ns),
        &[("up", 100)],
    );
    let r = sql.submit_enriched_sql(as_view).await.unwrap().into_inner();
    let rid = ready_request_id(&r);
    assert!(
        exec.confirm_execution(confirm_req(&rid, 100, 10))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // Same namespace+hash, different execution_type (FULL) → must execute.
    let as_full = submit(
        "\"DB\".\"DEV\".\"T\"",
        "h-et",
        ETYPE_FULL,
        Some(ns),
        &[("up", 100)],
    );
    let r2 = sql.submit_enriched_sql(as_full).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r2),
        "ready_to_execute",
        "a differing execution_type must not match via the namespace path"
    );
}

/// Deterministic newest-wins tie-break: when two confirmed rows share the same
/// namespace+hash+execution_type (e.g. two environments both built), the lookup
/// resolves to the most recently confirmed row. We confirm an OLD row with a
/// stale upstream epoch, then a NEWER row with a current upstream epoch; a
/// submit matching the current epoch must SKIP (newest row selected), proving
/// the ORDER BY confirmed_at DESC tie-break is honored deterministically.
#[tokio::test]
async fn namespace_tiebreak_selects_newest_confirmed() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let ns = "ns-tie";
    // OLD confirmed row: recorded upstream epoch 100.
    let old = submit(
        "\"DB\".\"A\".\"T\"",
        "h-tie",
        ETYPE_FULL,
        Some(ns),
        &[("\"DB\".\"A\".\"SRC\"", 100)],
    );
    let r_old = sql.submit_enriched_sql(old).await.unwrap().into_inner();
    let rid_old = ready_request_id(&r_old);
    assert!(
        exec.confirm_execution(confirm_req(&rid_old, 100, 10))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // NEWER confirmed row: recorded upstream epoch 5000 (built against newer data).
    let new = submit(
        "\"DB\".\"B\".\"T\"",
        "h-tie",
        ETYPE_FULL,
        Some(ns),
        &[("\"DB\".\"B\".\"SRC\"", 5000)],
    );
    let r_new = sql.submit_enriched_sql(new).await.unwrap().into_inner();
    let rid_new = ready_request_id(&r_new);
    assert!(
        exec.confirm_execution(confirm_req(&rid_new, 5000, 10))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // Submit with upstream epoch 5000: against the NEWEST row this is fresh →
    // SKIP. (Against the OLD row, 5000 > 100 would be drift → execute. So a
    // SKIP here proves the newest row was selected.)
    let probe = submit(
        "\"DB\".\"C\".\"T\"",
        "h-tie",
        ETYPE_FULL,
        Some(ns),
        &[("\"DB\".\"C\".\"SRC\"", 5000)],
    );
    let r = sql.submit_enriched_sql(probe).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r),
        "skip_execution",
        "tie-break must select the newest confirmed row (fresh vs its epoch)"
    );
}

// ---------------------------------------------------------------------------
// F6 — RecordExecutions
// ---------------------------------------------------------------------------

fn enriched_record(target: &str, body_hash: &str, epoch: Option<i64>) -> qc::ExecutionRecord {
    qc::ExecutionRecord {
        outcome: Some(qc::ExecutionOutcome {
            last_modified_epoch: epoch,
            table_type: Some("TABLE".to_string()),
            execution_results: None,
            execution_runtime_ms: Some(10),
        }),
        input: Some(qc::execution_record::Input::EnrichedSql(qc::SqlExecution {
            target_table: Some(target.to_string()),
            dialect: "snowflake".to_string(),
            default_catalog: "DB".to_string(),
            execution_type: ETYPE_FULL,
            sql: "select 1".to_string(),
            tables: vec![],
            query_dependencies: vec![],
            semantic_extras: Default::default(),
            labels: Default::default(),
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: format!("model.jaffle.{target}"),
                node_hash: body_hash.to_string(),
                node_body_hash: Some(body_hash.to_string()),
                ..Default::default()
            }),
            default_schema: None,
            from_speculative_submit: false,
            table_namespace: None,
        })),
    }
}

/// A RecordExecutions batch with a record missing its `input` oneof must fail
/// the whole request (invalid_argument) and persist NOTHING — the batch is
/// atomic all-or-nothing. The valid records in the same batch must not leak in.
#[tokio::test]
async fn record_executions_batch_is_atomic_on_invalid_record() {
    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;
    let mut exec = ExecutionClient::new(ch);

    let good = enriched_record("\"DB\".\"S\".\"GOOD\"", "h-good", Some(1_000));
    // An ExecutionRecord with no `input` oneof — the server must reject the
    // whole batch before inserting anything.
    let bad = qc::ExecutionRecord {
        outcome: Some(qc::ExecutionOutcome {
            last_modified_epoch: Some(2_000),
            table_type: None,
            execution_results: None,
            execution_runtime_ms: None,
        }),
        input: None,
    };

    let res = exec
        .record_executions(qc::RecordExecutionsRequest {
            records: vec![good, bad],
        })
        .await;
    assert!(
        res.is_err(),
        "a batch with an invalid record must be rejected"
    );

    // Nothing from the batch may have persisted.
    let pool = schema_pool(&schema).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM executions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 0,
        "an invalid batch must persist zero rows (atomic rollback)"
    );
}

/// Duplicate-fingerprint hydration: two RecordExecutions for the SAME
/// fingerprint with different recorded epochs both persist (no dedupe), and a
/// subsequent matching submit resolves to the NEWEST confirmed row — so an
/// upstream at the newer recorded epoch is fresh and SKIPs.
#[tokio::test]
async fn record_executions_duplicate_fingerprint_newest_wins() {
    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"DUP\"";
    // First hydrate: recorded epoch 100.
    exec.record_executions(qc::RecordExecutionsRequest {
        records: vec![enriched_record(target, "h-dup", Some(100))],
    })
    .await
    .unwrap();
    // Second hydrate of the SAME fingerprint: recorded epoch 5000.
    exec.record_executions(qc::RecordExecutionsRequest {
        records: vec![enriched_record(target, "h-dup", Some(5000))],
    })
    .await
    .unwrap();

    // Both rows persisted (no dedupe).
    let pool = schema_pool(&schema).await;
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM executions WHERE node_body_hash='h-dup'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 2, "duplicate fingerprints both persist (no dedupe)");

    // A submit with an upstream at epoch 5000: fresh vs the NEWEST recorded row
    // (5000), stale vs the old row (100). SKIP proves newest-wins selection.
    let probe = submit(target, "h-dup", ETYPE_FULL, None, &[("up", 5000)]);
    let r = sql.submit_enriched_sql(probe).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r),
        "skip_execution",
        "lookup must resolve to the newest confirmed row"
    );
}

// ---------------------------------------------------------------------------
// Data-test node identity (execution_type = DBT_DATA_TEST = 8)
// ---------------------------------------------------------------------------

const ETYPE_DATA_TEST: i32 = 8;

/// Build a data-test submit: empty target_table, a body hash shared across all
/// tests of the same generic type, distinguished only by node_unique_id — just
/// like the real dbt client sends. `table_namespace` is present (adapter-level)
/// to prove the fix does not fall back to a namespace+body collision either.
fn test_submit(unique_id: &str, shared_body: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: None,
        dialect: "snowflake".to_string(),
        execution_type: ETYPE_DATA_TEST,
        sql: "select * from x where c is null".to_string(),
        table_namespace: Some("adapter-ns".to_string()),
        tables: vec![],
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: unique_id.to_string(),
            resource_type: "test".to_string(),
            node_hash: shared_body.to_string(),
            node_body_hash: Some(shared_body.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Two DIFFERENT data tests that share a body hash and have an empty target
/// (as all `not_null` tests do) must NOT collide: confirming one must not make
/// the other skip. Guards the review's live-captured C1c finding at the
/// store/handler level, independent of the golden fixture.
#[tokio::test]
async fn data_tests_do_not_collide_on_shared_body_hash() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let shared = "934c4ef4sharednotnullbody";
    let t1 = test_submit("test.proj.not_null_customers_customer_id.aaaa", shared);
    let t2 = test_submit("test.proj.not_null_customers_customer_name.bbbb", shared);

    // Execute + confirm test #1.
    let r1 = sql
        .submit_enriched_sql(t1.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&r1),
        "ready_to_execute",
        "first test executes"
    );
    let rid = ready_request_id(&r1);
    assert!(
        exec.confirm_execution(confirm_req(&rid, 100, 10))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // Test #1 now skips (same unique_id).
    let r1b = sql.submit_enriched_sql(t1).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r1b),
        "skip_execution",
        "same test skips after confirm"
    );

    // Test #2 — different unique_id, SAME body hash, empty target — must EXECUTE,
    // not collide with the confirmed test #1.
    let r2 = sql.submit_enriched_sql(t2).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r2),
        "ready_to_execute",
        "a different data test sharing the body hash must not collide → execute"
    );
}

// ---------------------------------------------------------------------------
// Logic-identity match key: whitespace-normalized SQL + semantic_extras
// (verified mechanism — NOT node_body_hash). See docs/protocol.md C1/C1b.
// ---------------------------------------------------------------------------

fn model_sql(target: &str, sql: &str, body_hash: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.to_string()),
        dialect: "snowflake".to_string(),
        execution_type: ETYPE_FULL,
        sql: sql.to_string(),
        table_namespace: Some("ns".to_string()),
        tables: vec![("up", 100i64)]
            .into_iter()
            .map(|(n, e)| qc::TableModifiedInfo {
                name: n.into(),
                last_modified_epoch: Some(e),
            })
            .collect(),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: format!("model.jaffle.{target}"),
            node_hash: body_hash.into(),
            node_body_hash: Some(body_hash.into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// A whitespace-only SQL change (and even a changed node_body_hash) must SKIP:
/// the hosted service normalizes whitespace and ignores node_body_hash for
/// reuse. Confirms we match on the whitespace-normalized SQL, not the body hash.
#[tokio::test]
async fn whitespace_only_sql_change_skips() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"WS\"";
    // body_hash deliberately DIFFERENT between the two to prove it is not the gate.
    let v1 = model_sql(target, "select 1 as a", "bodyhash-A");
    let r = sql.submit_enriched_sql(v1).await.unwrap().into_inner();
    let rid = ready_request_id(&r);
    assert!(
        exec.confirm_execution(confirm_req(&rid, 100, 10))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // Same SQL but reformatted (extra whitespace) + different body hash → SKIP.
    let v2 = model_sql(target, "select   1   as   a", "bodyhash-B-different");
    let r2 = sql.submit_enriched_sql(v2).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r2),
        "skip_execution",
        "whitespace-only SQL change (even with a changed node_body_hash) must skip"
    );
}

/// A real SQL text change must EXECUTE (normalized SQL differs).
#[tokio::test]
async fn real_sql_change_executes() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"RS\"";
    let v1 = model_sql(target, "select 1 as a", "h");
    let r = sql.submit_enriched_sql(v1).await.unwrap().into_inner();
    let rid = ready_request_id(&r);
    assert!(
        exec.confirm_execution(confirm_req(&rid, 100, 10))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    let v2 = model_sql(target, "select 1 as a, 2 as b", "h"); // same body hash, real SQL change
    let r2 = sql.submit_enriched_sql(v2).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r2),
        "ready_to_execute",
        "a real SQL change must execute"
    );
}

/// Changing an allowlisted `semantic_extras` key (e.g. `grants`) must EXECUTE
/// even when the SQL is unchanged.
#[tokio::test]
async fn semantic_extras_change_executes_sql_unchanged() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"SE\"";
    let mut v1 = model_sql(target, "select 1 as a", "h");
    v1.semantic_extras
        .insert("on_schema_change".into(), "\"ignore\"".into());
    let r = sql
        .submit_enriched_sql(v1.clone())
        .await
        .unwrap()
        .into_inner();
    let rid = ready_request_id(&r);
    assert!(
        exec.confirm_execution(confirm_req(&rid, 100, 10))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // Unchanged (same SQL + same semantic_extras) → skip.
    let r_same = sql.submit_enriched_sql(v1).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r_same),
        "skip_execution",
        "unchanged → skip"
    );

    // Add an allowlisted key (grants) with SQL unchanged → execute.
    let mut v2 = model_sql(target, "select 1 as a", "h");
    v2.semantic_extras
        .insert("on_schema_change".into(), "\"ignore\"".into());
    v2.semantic_extras
        .insert("grants".into(), "{\"select\":[\"PUBLIC\"]}".into());
    let r2 = sql.submit_enriched_sql(v2).await.unwrap().into_inner();
    assert_eq!(
        response_variant(&r2),
        "ready_to_execute",
        "an allowlisted semantic_extras change (grants) must execute even with unchanged SQL"
    );
}
