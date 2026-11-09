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

#[path = "support.rs"]
mod support;
use support::scenario::{
    built_at, data_test, inputless_record, model, outcome, ready_id, test_results, variant, view,
    Scenario,
};
use support::{fault, rows, start_server};

// ---------------------------------------------------------------------------
// F3 — ConfirmExecution semantics
// ---------------------------------------------------------------------------

/// A second confirm of the SAME request_id with DIFFERENT outcome values must
/// return success=true (idempotent) but MUST NOT overwrite the recorded
/// last_modified_epoch / runtime from the first confirm. A late/duplicate
/// confirm cannot silently rewrite skippable history.
#[tokio::test]
async fn reconfirm_does_not_mutate_recorded_outcome() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let req = model("\"DB\".\"S\".\"RECONF\"")
        .body("h-reconf")
        .upstream("up", 100);
    let rid = sc.execute_id(req).await;

    // First confirm records epoch=1000, runtime=11.
    assert!(sc.confirm(&rid, built_at(1000).runtime(11)).await);

    // Second confirm with DIFFERENT values must still succeed (idempotent)...
    assert!(
        sc.confirm(&rid, built_at(9999).runtime(99)).await,
        "re-confirming an already-confirmed id must return success=true"
    );

    // ...but the stored outcome must be the FIRST confirm's values.
    let row = rows::execution(&server, &rid).await.expect("row");
    assert_eq!(
        row.last_modified_epoch,
        Some(1000),
        "recorded epoch must be stable across re-confirm"
    );
    assert_eq!(
        row.execution_runtime_ms,
        Some(11),
        "recorded runtime must be stable across re-confirm"
    );
}

/// Confirm is org-scoped: confirming a request_id under the WRONG org must not
/// succeed, and must leave the original (correct-org) pending row unconfirmed.
#[tokio::test]
async fn confirm_is_org_scoped() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let (org_a, org_b) = (sc.org("orgA"), sc.org("orgB"));

    let node = model("\"DB\".\"S\".\"ORGSCOPE\"")
        .body("h-org")
        .upstream("up", 100);
    let rid = org_a.execute_id(node.clone()).await;

    // Confirm under the WRONG org → must not succeed (row belongs to orgA).
    assert!(
        !org_b.confirm(&rid, built_at(1000)).await,
        "confirm under a different org must not succeed"
    );

    // The orgA fingerprint is still unconfirmed → re-submit under orgA executes.
    assert_eq!(
        org_a.verdict(node).await,
        "ready_to_execute",
        "a cross-org confirm must not confirm the orgA row"
    );
}

// ---------------------------------------------------------------------------
// F2 — Match-key SELECTION (lookup layer)
// ---------------------------------------------------------------------------

/// Namespace identity alone cannot prove data exists at another physical target,
/// so a cross-target submit must NOT SKIP. VERIFIED LIVE: the hosted service
/// instead answers `ready_to_clone` — it clones the equivalent (same namespace +
/// schema-abstracted logic + execution_type) into the submit target rather than
/// blindly reusing it. A clone physically copies the data, so it is safe; a SKIP
/// would not be. See crates/harness/tests/clone_submit_fallback.rs.
#[tokio::test]
async fn namespace_alone_cannot_prove_cross_target_reuse() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let ns = "ns-shared";
    // Build + confirm under the PROD physical target.
    let prod = model("\"DB\".\"PROD\".\"T\"")
        .body("h-ns")
        .namespace(ns)
        .upstream("\"DB\".\"PROD\".\"SRC\"", 100);
    sc.build(prod, 100).await;

    // Submit under a DIFFERENT physical target (DEV), same namespace+logic+type.
    // The equivalent exists under another name → the hosted service CLONES it in
    // (ready_to_clone), it never SKIPs (which would reuse data that isn't there).
    let dev = model("\"DB\".\"DEV\".\"T\"")
        .body("h-ns")
        .namespace(ns)
        .upstream("\"DB\".\"DEV\".\"SRC\"", 100);
    assert_eq!(
        sc.verdict(dev).await,
        "ready_to_clone",
        "cross-name equivalent is cloned in, never skipped",
    );
}

/// Execution-type isolation on the NAMESPACE path: a confirmed row under one
/// execution_type must NOT be reused for a submit with the same namespace+hash
/// but a different execution_type.
#[tokio::test]
async fn namespace_match_respects_execution_type() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let ns = "ns-etype";
    let as_view = view("\"DB\".\"PROD\".\"T\"")
        .body("h-et")
        .namespace(ns)
        .upstream("up", 100);
    sc.build(as_view, 100).await;

    // Same namespace+hash, different execution_type (FULL) → must execute.
    let as_full = model("\"DB\".\"DEV\".\"T\"")
        .body("h-et")
        .namespace(ns)
        .upstream("up", 100);
    assert_eq!(
        sc.verdict(as_full).await,
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
async fn physical_target_selects_newest_confirmed() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let node = model("\"DB\".\"A\".\"T\"")
        .body("h-tie")
        .namespace("ns-tie");
    let src = "\"DB\".\"A\".\"SRC\"";
    // OLD confirmed row: recorded upstream epoch 100.
    sc.build(node.clone().upstream(src, 100), 100).await;

    // NEWER confirmed row: recorded upstream epoch 5000 (built against newer data).
    sc.build(node.clone().upstream(src, 5000), 5000).await;

    // Submit with upstream epoch 5000: against the NEWEST row this is fresh →
    // SKIP. (Against the OLD row, 5000 > 100 would be drift → execute. So a
    // SKIP here proves the newest row was selected.)
    assert_eq!(
        sc.verdict(node.upstream(src, 5000)).await,
        "skip_execution",
        "tie-break must select the newest confirmed row (fresh vs its epoch)"
    );
}

// ---------------------------------------------------------------------------
// F6 — RecordExecutions
// ---------------------------------------------------------------------------

/// A RecordExecutions batch with a record missing its `input` oneof must fail
/// the whole request (invalid_argument) and persist NOTHING — the batch is
/// atomic all-or-nothing. The valid records in the same batch must not leak in.
#[tokio::test]
async fn record_executions_batch_is_atomic_on_invalid_record() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let good = model("\"DB\".\"S\".\"GOOD\"")
        .body("h-good")
        .upstream("up", 1_000)
        .record(built_at(1_000));
    // An ExecutionRecord with no `input` oneof — the server must reject the
    // whole batch before inserting anything.
    let bad = inputless_record(outcome().epoch(2_000));

    let res = sc.record([good, bad]).await;
    assert!(
        res.is_err(),
        "a batch with an invalid record must be rejected"
    );

    // Nothing from the batch may have persisted.
    assert_eq!(
        rows::execution_count(&server).await,
        0,
        "an invalid batch must persist zero rows (atomic rollback)"
    );
}

/// A rejected RecordExecutions batch must not leak LOGICAL (selector/deferral)
/// state either. Review #2 finding 5: `node_states` writes happened per-record
/// inside the loop, BEFORE the whole batch was validated and OUTSIDE the batch
/// transaction, so a valid record followed by an invalid one left the first
/// node resolvable for deferral even though the request returned
/// INVALID_ARGUMENT. A rejected request must have NO persisted side effect.
#[tokio::test]
async fn rejected_record_batch_does_not_leak_node_state() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.org("acme");

    // A valid record whose node_state carries the scope keys (project_id +
    // target_name + target_table) that drive selector/deferral state.
    let good = model("\"DB\".\"S\".\"ROLLED_BACK\"")
        .catalog("DB")
        .schema("S")
        .sql("select 1")
        .no_tables()
        .body("h")
        .unique_id("model.jaffle.rolled_back")
        .project("proj-1")
        .target_name("prod")
        .record(built_at(1_000));
    // An invalid record (no input oneof) — rejects the whole batch.
    let bad = inputless_record(outcome());

    let res = sc.record([good, bad]).await;
    assert!(res.is_err(), "invalid batch must be rejected");

    // Neither the execution ledger NOR the logical node_states may have changed.
    assert_eq!(
        rows::execution_count(&server).await,
        0,
        "no executions persisted from a rejected batch"
    );
    assert_eq!(
        rows::node_states(&server).await.len(),
        0,
        "a rejected batch must not leak selector/deferral node_state"
    );
}

/// FULL CROSS-STORE ATOMICITY (review #2 finding 5, follow-up). A SUCCESSFUL
/// RecordExecutions must not leave the execution ledger and the logical
/// (selector/deferral) state inconsistent. Previously the executions batch
/// COMMITTED and then a best-effort node_states write ran outside the
/// transaction; a node_states failure left a durable execution row with zero
/// logical rows (records_stored=1, empty deferral). Both tables are now written
/// in ONE transaction, so a node_states fault rolls the WHOLE request back.
///
/// This injects a deterministic fault with a `CHECK (false)` constraint on the
/// isolated test schema's `node_states`, then records a single valid execution
/// whose node_state carries the scope keys (so a node_states upsert is
/// attempted). The request must FAIL and leave NEITHER table changed — no
/// partial publish.
#[tokio::test]
async fn node_state_write_failure_rolls_back_whole_record_batch() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.org("acme");

    // Inject a deterministic fault: any node_states write will fail.
    fault::reject_node_state_writes(&server).await;

    // A single valid record whose node_state carries project_id + target_name +
    // node_unique_id, so the server attempts a node_states upsert.
    let rec = model("\"DB\".\"S\".\"ATOMIC\"")
        .catalog("DB")
        .schema("S")
        .sql("select 1")
        .no_tables()
        .body("h")
        .unique_id("model.jaffle.atomic")
        .project("proj-atomic")
        .target_name("prod")
        .record(outcome().epoch(2_000).table().runtime(20));

    let res = sc.record([rec]).await;
    assert!(
        res.is_err(),
        "a node_states write failure must fail the whole RecordExecutions request"
    );

    // The execution row must NOT have been published either (full rollback).
    assert_eq!(
        rows::execution_count(&server).await,
        0,
        "the execution ledger must roll back when the logical-state write fails \
         (no partial publish)"
    );
    assert_eq!(
        rows::node_states(&server).await.len(),
        0,
        "no logical state persisted either"
    );

    // Clean up the injected fault so the shared schema (if reused) is sane.
    fault::accept_node_state_writes(&server).await;
}

/// A SUCCESSFUL RecordExecutions publishes execution AND logical state together
/// (the happy path of the atomic write): the recorded node becomes resolvable
/// for deferral in the same request.
#[tokio::test]
async fn successful_record_batch_publishes_execution_and_node_state_together() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.org("acme");

    let rec = model("\"DB\".\"S\".\"PUB\"")
        .catalog("DB")
        .schema("S")
        .sql("select 1")
        .no_tables()
        .body("h")
        .unique_id("model.jaffle.pub")
        .project("proj-pub")
        .target_name("prod")
        .record(outcome().epoch(3_000).table().runtime(30));

    let stored = sc.record([rec]).await.expect("record succeeds");
    assert_eq!(stored, 1);

    assert_eq!(
        rows::execution_count(&server).await,
        1,
        "the execution is published"
    );
    let states = rows::node_states(&server)
        .await
        .iter()
        .filter(|s| s.node_unique_id == "model.jaffle.pub")
        .count();
    assert_eq!(
        states, 1,
        "the logical state is published in the same request"
    );
}
/// Duplicate-fingerprint hydration: two RecordExecutions for the SAME
/// fingerprint with different recorded epochs both persist (no dedupe), and a
/// subsequent matching submit resolves to the NEWEST confirmed row — so an
/// upstream at the newer recorded epoch is fresh and SKIPs.
#[tokio::test]
async fn record_executions_duplicate_fingerprint_newest_wins() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let node = model("\"DB\".\"S\".\"DUP\"").body("h-dup");
    // First hydrate: recorded epoch 100.
    sc.record([node.clone().upstream("up", 100).record(built_at(100))])
        .await
        .unwrap();
    // Second hydrate of the SAME fingerprint: recorded epoch 5000.
    sc.record([node.clone().upstream("up", 5000).record(built_at(5000))])
        .await
        .unwrap();

    // Both rows persisted (no dedupe).
    let count = rows::executions(&server)
        .await
        .iter()
        .filter(|r| r.node_body_hash.as_deref() == Some("h-dup"))
        .count();
    assert_eq!(count, 2, "duplicate fingerprints both persist (no dedupe)");

    // A submit with an upstream at epoch 5000: fresh vs the NEWEST recorded row
    // (5000), stale vs the old row (100). SKIP proves newest-wins selection.
    assert_eq!(
        sc.verdict(node.upstream("up", 5000)).await,
        "skip_execution",
        "lookup must resolve to the newest confirmed row"
    );
}

// ---------------------------------------------------------------------------
// Data-test node identity (execution_type = DBT_DATA_TEST = 8)
// ---------------------------------------------------------------------------

/// Two DIFFERENT data tests that share a body hash and have an empty target
/// (as all `not_null` tests do) must NOT collide: confirming one must not make
/// the other skip. Guards the review's live-captured C1c finding at the
/// store/handler level, independent of the golden fixture.
///
/// Each data-test submit has an empty target_table and a body hash shared
/// across all tests of the same generic type, distinguished only by
/// node_unique_id — just like the real dbt client sends. `table_namespace` is
/// present (adapter-level) to prove the fix does not fall back to a
/// namespace+body collision either.
#[tokio::test]
async fn data_tests_do_not_collide_on_shared_body_hash() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let t1 = data_test("test.proj.not_null_customers_customer_id.aaaa")
        .sql("select * from x where c is null")
        .body("934c4ef4sharednotnullbody")
        .namespace("adapter-ns");
    let t2 = t1
        .clone()
        .unique_id("test.proj.not_null_customers_customer_name.bbbb");

    // Execute + confirm test #1.
    let r1 = sc.submit(t1.clone()).await;
    assert_eq!(variant(&r1), "ready_to_execute", "first test executes");
    let rid = ready_id(&r1);
    assert!(
        sc.confirm(&rid, built_at(100).results(test_results(0)))
            .await
    );

    // Test #1 now skips (same unique_id).
    assert_eq!(
        sc.verdict(t1).await,
        "skip_execution",
        "same test skips after confirm"
    );

    // Test #2 — different unique_id, SAME body hash, empty target — must EXECUTE,
    // not collide with the confirmed test #1.
    assert_eq!(
        sc.verdict(t2).await,
        "ready_to_execute",
        "a different data test sharing the body hash must not collide → execute"
    );
}

// ---------------------------------------------------------------------------
// Logic-identity match key: whitespace-normalized SQL + semantic_extras
// Captured evidence distinguishes these inputs from the client body hash.
// ---------------------------------------------------------------------------

/// A whitespace-only SQL change (and even a changed node_body_hash) must SKIP:
/// the hosted service normalizes whitespace and ignores node_body_hash for
/// reuse. Confirms we match on the whitespace-normalized SQL, not the body hash.
#[tokio::test]
async fn whitespace_only_sql_change_skips() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let node = model("\"DB\".\"S\".\"WS\"")
        .namespace("ns")
        .upstream("up", 100);
    // body_hash deliberately DIFFERENT between the two to prove it is not the gate.
    let v1 = node.clone().sql("select 1 as a").body("bodyhash-A");
    sc.build(v1, 100).await;

    // Same SQL but reformatted (extra whitespace) + different body hash → SKIP.
    let v2 = node.sql("select   1   as   a").body("bodyhash-B-different");
    assert_eq!(
        sc.verdict(v2).await,
        "skip_execution",
        "whitespace-only SQL change (even with a changed node_body_hash) must skip"
    );
}

/// A real SQL text change must EXECUTE (normalized SQL differs).
#[tokio::test]
async fn real_sql_change_executes() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let node = model("\"DB\".\"S\".\"RS\"")
        .namespace("ns")
        .upstream("up", 100)
        .body("h");
    sc.build(node.clone().sql("select 1 as a"), 100).await;

    // Same body hash, real SQL change.
    let v2 = node.sql("select 1 as a, 2 as b");
    assert_eq!(
        sc.verdict(v2).await,
        "ready_to_execute",
        "a real SQL change must execute"
    );
}

/// Changing an allowlisted `semantic_extras` key (e.g. `grants`) must EXECUTE
/// even when the SQL is unchanged.
#[tokio::test]
async fn semantic_extras_change_executes_sql_unchanged() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let v1 = model("\"DB\".\"S\".\"SE\"")
        .namespace("ns")
        .upstream("up", 100)
        .body("h")
        .sql("select 1 as a")
        .extra("on_schema_change", "\"ignore\"");
    sc.build(v1.clone(), 100).await;

    // Unchanged (same SQL + same semantic_extras) → skip.
    assert_eq!(
        sc.verdict(v1.clone()).await,
        "skip_execution",
        "unchanged → skip"
    );

    // Add an allowlisted key (grants) with SQL unchanged → execute.
    let v2 = v1.extra("grants", "{\"select\":[\"PUBLIC\"]}");
    assert_eq!(
        sc.verdict(v2).await,
        "ready_to_execute",
        "an allowlisted semantic_extras change (grants) must execute even with unchanged SQL"
    );
}
