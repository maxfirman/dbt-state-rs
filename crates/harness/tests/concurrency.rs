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
//! object-lineage provenance is documented in docs/correctness-handoff.md §A.

#[path = "support.rs"]
mod support;

use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, response_variant, schema_pool, start_server};

const ETYPE_FULL: i32 = 1;

/// A submit for `target` with a DISTINCT logic `expr` (a real projection, not a
/// comment — the Snowflake normalizer strips comments), own-target epoch 1
/// (present) and one upstream `src` at `src_epoch`.
fn submit(target: &str, expr: &str, src_epoch: i64) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.to_string()),
        tolerate_nondeterminism: true,
        dialect: "snowflake".to_string(),
        execution_type: ETYPE_FULL,
        sql: format!("select {expr} as id from \"DB\".\"S\".\"SRC\""),
        tables: vec![
            qc::TableModifiedInfo {
                name: target.into(),
                last_modified_epoch: Some(1),
            },
            qc::TableModifiedInfo {
                name: "\"DB\".\"S\".\"SRC\"".into(),
                last_modified_epoch: Some(src_epoch),
            },
        ],
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.p.t".into(),
            node_body_hash: Some(expr.to_string()),
            node_hash: expr.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn confirm_req(request_id: &str, epoch: i64) -> qc::ConfirmExecutionRequest {
    qc::ConfirmExecutionRequest {
        request_id: request_id.to_string(),
        last_modified_epoch: Some(epoch),
        table_type: Some("TABLE".into()),
        execution_runtime_ms: Some(10),
        ..Default::default()
    }
}

fn ready_id(resp: &qc::SubmitSqlResponse) -> String {
    match &resp.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => r.request_id.clone(),
        other => panic!("expected ready_to_execute, got {other:?}"),
    }
}

/// REVERSED CONFIRMATION ORDER. Two different-logic submits for the SAME target
/// are both issued (both pending), then confirmed in REVERSE order (the
/// first-submitted is confirmed LAST). The current physical state must be the
/// LAST-CONFIRMED build (logic A), not the last-submitted (logic B): a resubmit
/// of A's logic + fresh upstream SKIPS, while B's logic rebuilds.
#[tokio::test]
async fn confirmation_order_defines_current_state_not_submission_order() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"T\"";

    // Submit A (logic hA) and B (logic hB) — both become pending rows.
    let ra = sql
        .submit_enriched_sql(submit(target, "11", 100))
        .await
        .unwrap()
        .into_inner();
    let id_a = ready_id(&ra);
    let rb = sql
        .submit_enriched_sql(submit(target, "22", 100))
        .await
        .unwrap()
        .into_inner();
    let id_b = ready_id(&rb);

    // Confirm in REVERSE order: B first, then A. A has the newest confirmed_at.
    assert!(
        exec.confirm_execution(confirm_req(&id_b, 100))
            .await
            .unwrap()
            .into_inner()
            .success
    );
    assert!(
        exec.confirm_execution(confirm_req(&id_a, 100))
            .await
            .unwrap()
            .into_inner()
            .success
    );

    // Resubmit A's logic with a fresh (<=100) upstream → SKIP (A is current).
    let probe_a = sql
        .submit_enriched_sql(submit(target, "11", 100))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&probe_a),
        "skip_execution",
        "the LAST-confirmed build (A) is the current physical state"
    );

    // Resubmit B's logic → EXECUTE (B is no longer the current state).
    let probe_b = sql
        .submit_enriched_sql(submit(target, "22", 100))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&probe_b),
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
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"L\"";
    // Submit C first (pending, not yet confirmed).
    let rc = sql
        .submit_enriched_sql(submit(target, "33", 100))
        .await
        .unwrap()
        .into_inner();
    let id_c = ready_id(&rc);

    // Build + confirm B meanwhile (B becomes current).
    let rb = sql
        .submit_enriched_sql(submit(target, "22", 100))
        .await
        .unwrap()
        .into_inner();
    let id_b = ready_id(&rb);
    exec.confirm_execution(confirm_req(&id_b, 100))
        .await
        .unwrap();
    // B is current → B's logic skips.
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(submit(target, "22", 100))
                .await
                .unwrap()
                .into_inner()
        ),
        "skip_execution"
    );

    // Now the LATE confirm of C lands. C has the newest confirmed_at → current.
    exec.confirm_execution(confirm_req(&id_c, 100))
        .await
        .unwrap();
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(submit(target, "33", 100))
                .await
                .unwrap()
                .into_inner()
        ),
        "skip_execution",
        "a late confirm deterministically makes its row the current state"
    );
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(submit(target, "22", 100))
                .await
                .unwrap()
                .into_inner()
        ),
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
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"P\"";
    let sql_text = "select 1 as id from \"DB\".\"S\".\"SRC\"";

    // Hydrate via RecordExecutions, copying the fields the submit will send.
    exec.record_executions(qc::RecordExecutionsRequest {
        records: vec![qc::ExecutionRecord {
            outcome: Some(qc::ExecutionOutcome {
                last_modified_epoch: Some(100),
                table_type: Some("TABLE".into()),
                execution_results: None,
                execution_runtime_ms: Some(5),
            }),
            input: Some(qc::execution_record::Input::EnrichedSql(qc::SqlExecution {
                target_table: Some(target.into()),
                dialect: "snowflake".into(),
                default_catalog: "DB".into(),
                default_schema: Some("S".into()),
                execution_type: ETYPE_FULL,
                sql: sql_text.into(),
                ..Default::default()
            })),
        }],
    })
    .await
    .unwrap();

    // A submit with the SAME dialect/catalog/schema/sql must reuse it.
    let req = qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: "snowflake".into(),
        default_catalog: "DB".into(),
        default_schema: Some("S".into()),
        execution_type: ETYPE_FULL,
        sql: sql_text.into(),
        tolerate_nondeterminism: true,
        tables: vec![qc::TableModifiedInfo {
            name: target.into(),
            last_modified_epoch: Some(1),
        }],
        ..Default::default()
    };
    assert_eq!(
        response_variant(&sql.submit_enriched_sql(req).await.unwrap().into_inner()),
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
    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"R\"";

    // Build + confirm logic X, then logic Y, for the same target.
    for expr in ["11", "22"] {
        let r = sql
            .submit_enriched_sql(submit(target, expr, 100))
            .await
            .unwrap()
            .into_inner();
        let id = ready_id(&r);
        exec.confirm_execution(confirm_req(&id, 100)).await.unwrap();
    }

    // Both rows persist (no dedupe across conflicting confirmed builds).
    let pool = schema_pool(&schema).await;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM executions WHERE target_table = $1 AND status='confirmed'",
    )
    .bind(target)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2, "conflicting confirmed builds both persist");

    // The LAST-confirmed logic (Y="22") is current → its submit skips; X rebuilds.
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(submit(target, "22", 100))
                .await
                .unwrap()
                .into_inner()
        ),
        "skip_execution",
        "last-confirmed build is the current physical state"
    );
    assert_eq!(
        response_variant(
            &sql.submit_enriched_sql(submit(target, "11", 100))
                .await
                .unwrap()
                .into_inner()
        ),
        "ready_to_execute",
        "the earlier-confirmed (superseded) logic must rebuild"
    );
}
