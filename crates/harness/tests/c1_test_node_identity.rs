//! C1c — DATA-TEST NODE IDENTITY (live-captured from api.state.dbt.com).
//!
//! Column-config exploration finding: dbt data tests are their OWN nodes
//! (execution_type = DBT_DATA_TEST = 8). For these nodes:
//!   * `target_table` is empty,
//!   * `node_body_hash` is IDENTICAL across every test of the same generic type
//!     regardless of the column/table under test — every `not_null` test in the
//!     project hashes to `934c4ef4`, every `unique` to `fc665e00`, etc.,
//!   * the ONLY distinguishing field is `node_unique_id`
//!     (e.g. `test.jaffle_shop.not_null_customers_customer_name.2bf8eaa065`).
//!
//! Captured sequence (fixture `golden/fixtures/c1_test_node_identity.jsonl`):
//!   not_null_customers_customer_id   execute -> confirm -> skip   (body 934c4ef4)
//!   not_null_customers_customer_name execute                      (body 934c4ef4)
//! The hosted service EXECUTED the second test even though its body hash equals
//! the already-confirmed first test, because the `node_unique_id` differs.
//!
//! BUG this pins/guards: our store matched test nodes on
//! (org, target_table|table_namespace, execution_type, node_body_hash). With an
//! empty target and a body hash shared by all same-type tests, every `not_null`
//! test in the org collapses onto ONE match key — so adding a new column test
//! would wrongly SKIP (matching an existing confirmed sibling), silently never
//! running the new assertion. The fix matches test nodes by `node_unique_id`.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/c1_test_node_identity.jsonl"
);

#[tokio::test]
async fn c1_data_test_node_matched_by_unique_id() {
    let entries = diff::load_golden(FIXTURE).expect("load test-node-identity fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 3, "id-execute, id-skip, name-execute");

    // Pin the real decisions + prove the body hashes collide across the two
    // DIFFERENT tests (so body-hash matching alone cannot distinguish them).
    let real = |i: usize| diff::decision_variant(&submits[i].response).unwrap();
    assert_eq!(
        real(0),
        "ready_to_execute",
        "customer_id test first build executes"
    );
    assert_eq!(
        real(1),
        "skip_execution",
        "customer_id test unchanged rebuild skips"
    );
    assert_eq!(
        real(2),
        "ready_to_execute",
        "customer_name test is a NEW node, executes"
    );

    let body = |i: usize| {
        submits[i].request["dbt_node_state"]["node_body_hash"]
            .as_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(
        body(0),
        body(2),
        "both not_null tests share the same body hash"
    );
    let uid = |i: usize| {
        submits[i].request["dbt_node_state"]["node_unique_id"]
            .as_str()
            .unwrap()
            .to_string()
    };
    assert_ne!(
        uid(0),
        uid(2),
        "the two tests differ only by node_unique_id"
    );

    // Replay against our server: execute + confirm the customer_id test, then
    // submit the customer_name test. The hosted service executes it; our server
    // MUST do the same (not skip by colliding on the shared body hash).
    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let id_req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[0].request.clone()).unwrap();
    let r = sql
        .submit_enriched_sql(id_req.clone())
        .await
        .unwrap()
        .into_inner();
    let rid = match r.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        other => panic!("id test should execute first, got {other:?}"),
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: rid,
        last_modified_epoch: Some(1_791_600_000_000),
        failed_to_clone: false,
        table_type: None,
        execution_results: None,
        execution_runtime_ms: Some(100),
        labels: Default::default(),
    })
    .await
    .unwrap();

    // The confirmed id test now skips on rerun (same unique_id).
    let v_id_again =
        support::response_variant(&sql.submit_enriched_sql(id_req).await.unwrap().into_inner());
    assert_eq!(
        v_id_again, "skip_execution",
        "same test (same unique_id) skips after confirm"
    );

    // The NEW customer_name test must EXECUTE (different unique_id), matching the
    // hosted service — NOT skip by colliding on the shared body hash.
    let name_req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[2].request.clone()).unwrap();
    let v_name = support::response_variant(
        &sql.submit_enriched_sql(name_req)
            .await
            .unwrap()
            .into_inner(),
    );
    assert_eq!(
        v_name, "ready_to_execute",
        "a NEW data-test node (distinct node_unique_id, shared body hash) must EXECUTE, \
         not skip by colliding on the shared test body hash"
    );
}
