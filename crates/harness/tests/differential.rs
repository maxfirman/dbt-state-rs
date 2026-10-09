//! Differential tests: replay real golden requests against our server and
//! assert the decisions match the hosted service's observed behavior.
//!
//! Requires a running Postgres (the project's dbt-state-pg container).
//! Set DATABASE_URL or rely on the default localhost:55441 dsn. Each test runs
//! in its own uniquely-named schema so runs are isolated and repeatable.

use dbt_state_harness::diff::{self, GoldenEntry};
use dbt_state_proto::query_cache as qc;

use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

// Shared isolated-schema server setup (see tests/support.rs). Included via
// #[path] so differential.rs and robustness.rs share one copy of the helpers
// while keeping the server/sqlx/tokio deps dev-only.
#[path = "support.rs"]
mod support;
use support::{channel, response_variant, schema_pool, start_server};

const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/golden_20261008T222744.061Z.jsonl"
);

/// Build a SubmitEnrichedSqlRequest from a golden request JSON value.
fn submit_req_from_golden(e: &GoldenEntry) -> qc::SubmitEnrichedSqlRequest {
    serde_json::from_value(e.request.clone()).expect("decode SubmitEnrichedSqlRequest")
}

fn find_first_execute(entries: &[GoldenEntry]) -> &GoldenEntry {
    entries
        .iter()
        .find(|e| {
            e.method == "SubmitEnrichedSQL"
                && diff::decision_variant(&e.response).as_deref() == Some("ready_to_execute")
        })
        .expect("a ready_to_execute golden entry")
}

/// Core causal loop: a never-seen node must EXECUTE; after ConfirmExecution,
/// an identical submit must SKIP. This is the heart of the protocol and is
/// fully reproducible from an empty store.
#[tokio::test]
async fn execute_then_confirm_then_skip() {
    let entries = diff::load_golden(GOLDEN).expect("load golden");
    let exec_entry = find_first_execute(&entries);
    let req = submit_req_from_golden(exec_entry);

    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // 1) First submit of a never-seen fingerprint -> EXECUTE.
    let resp1 = sql
        .submit_enriched_sql(req.clone())
        .await
        .expect("submit 1")
        .into_inner();
    let variant1 = response_variant(&resp1);
    assert_eq!(variant1, "ready_to_execute", "first submit must execute");

    let request_id = match resp1.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => r.request_id,
        _ => panic!("expected ready_to_execute"),
    };
    assert!(!request_id.is_empty());

    // 2) Confirm the execution.
    let confirm = qc::ConfirmExecutionRequest {
        request_id: request_id.clone(),
        last_modified_epoch: Some(1_791_498_542_120),
        failed_to_clone: false,
        table_type: None,
        execution_results: None,
        execution_runtime_ms: Some(3205),
        labels: req.labels.clone(),
    };
    let cresp = exec
        .confirm_execution(confirm)
        .await
        .expect("confirm")
        .into_inner();
    assert!(cresp.success, "confirm must succeed for a known request_id");
    assert_eq!(cresp.request_id, request_id);

    // 3) Identical submit again -> SKIP (no-op), because the fingerprint is now
    //    confirmed and upstream data is unchanged.
    let resp2 = sql
        .submit_enriched_sql(req.clone())
        .await
        .expect("submit 2")
        .into_inner();
    assert_eq!(
        response_variant(&resp2),
        "skip_execution",
        "second identical submit must skip"
    );
}

/// ValidateClientVersion must return is_supported=true, matching the real service.
#[tokio::test]
async fn validate_client_version_supported() {
    use qc::client_validation_client::ClientValidationClient;
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut client = ClientValidationClient::new(ch);
    let resp = client
        .validate_client_version(qc::ValidateClientVersionRequest {
            dbt_run_cache_version: "2.0.6".to_string(),
        })
        .await
        .expect("validate")
        .into_inner();
    assert!(resp.is_supported);
}

/// Item 1: a confirmed row's stored execution_runtime_ms must be echoed back on
/// the subsequent SKIP. Confirm with runtime 3205, then an identical submit
/// skips and reports execution_runtime_ms == 3205.
#[tokio::test]
async fn skip_echoes_confirmed_runtime_ms() {
    let entries = diff::load_golden(GOLDEN).expect("load golden");
    let exec_entry = find_first_execute(&entries);
    let req = submit_req_from_golden(exec_entry);

    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Execute, then confirm with a known runtime.
    let resp1 = sql
        .submit_enriched_sql(req.clone())
        .await
        .expect("submit 1")
        .into_inner();
    let request_id = match resp1.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => r.request_id,
        _ => panic!("expected ready_to_execute"),
    };
    let confirm = qc::ConfirmExecutionRequest {
        request_id: request_id.clone(),
        last_modified_epoch: Some(1_791_498_542_120),
        failed_to_clone: false,
        table_type: None,
        execution_results: None,
        execution_runtime_ms: Some(3205),
        labels: req.labels.clone(),
    };
    assert!(
        exec.confirm_execution(confirm)
            .await
            .expect("confirm")
            .into_inner()
            .success
    );

    // Identical submit -> SKIP with echoed runtime.
    let resp2 = sql
        .submit_enriched_sql(req.clone())
        .await
        .expect("submit 2")
        .into_inner();
    match resp2.response {
        Some(qc::submit_sql_response::Response::SkipExecution(s)) => {
            assert_eq!(
                s.execution_runtime_ms,
                Some(3205),
                "skip must echo the confirmed runtime"
            );
        }
        other => panic!("expected skip_execution, got {other:?}"),
    }
}

/// Item 2: RecordExecutions inserts confirmed rows directly. Record 2
/// executions, assert records_stored==2 and that 2 rows exist in the store.
#[tokio::test]
async fn record_executions_stores_batch() {
    let (addr, schema) = start_server().await;
    let ch = channel(addr).await;
    let mut exec = ExecutionClient::new(ch);

    let mk = |table: &str, hash: &str| qc::ExecutionRecord {
        outcome: Some(qc::ExecutionOutcome {
            last_modified_epoch: Some(1_791_498_000_000),
            table_type: Some("TABLE".to_string()),
            execution_results: None,
            execution_runtime_ms: Some(1500),
        }),
        input: Some(qc::execution_record::Input::EnrichedSql(qc::SqlExecution {
            target_table: Some(table.to_string()),
            dialect: "snowflake".to_string(),
            default_catalog: String::new(),
            execution_type: 10,
            sql: "select 1".to_string(),
            tables: Vec::new(),
            query_dependencies: Vec::new(),
            semantic_extras: Default::default(),
            labels: Default::default(),
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: format!("model.jaffle.{table}"),
                target_name: "prod".to_string(),
                project_name: "jaffle".to_string(),
                resource_type: "model".to_string(),
                node_hash: hash.to_string(),
                node_body_hash: Some(hash.to_string()),
                node_configs_hash: None,
                node_persisted_descriptions_hash: None,
                node_macros_hash: None,
                node_contract_hash: None,
                profile_name: "default".to_string(),
                project_id: None,
                node_database_representation: None,
            }),
            default_schema: None,
            from_speculative_submit: false,
            table_namespace: None,
        })),
    };

    let resp = exec
        .record_executions(qc::RecordExecutionsRequest {
            records: vec![
                mk("\"DB\".\"S\".\"A\"", "ha"),
                mk("\"DB\".\"S\".\"B\"", "hb"),
            ],
        })
        .await
        .expect("record")
        .into_inner();
    assert_eq!(resp.records_stored, 2, "both records must be stored");

    // Assert the rows actually exist and are confirmed.
    let pool = schema_pool(&schema).await;
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM executions WHERE status = 'confirmed'")
            .fetch_one(&pool)
            .await
            .expect("count");
    assert_eq!(count, 2, "two confirmed rows must exist");
}

/// Item 3: SubmitValues (seeds) execute -> confirm -> skip loop, matched on
/// values_hash. First submit of a new values_hash executes; after confirm an
/// identical submit skips.
#[tokio::test]
async fn submit_values_execute_confirm_skip() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let req = qc::SubmitValuesRequest {
        target_table: "\"DB\".\"S\".\"SEED\"".to_string(),
        dialect: "snowflake".to_string(),
        default_catalog: String::new(),
        values_hash: "md5deadbeef".to_string(),
        semantic_extras: Default::default(),
        last_modified_epoch: Some(1_791_498_000_000),
        labels: Default::default(),
        clone_time_travel_limit: None,
        clone_table_properties: None,
        clone_chain_depth_limit: None,
        dbt_node_state: None,
        table_namespace: None,
        allow_clones: None,
        is_defer_to_profile: false,
        defer_enabled: false,
    };

    // 1) never-seen seed -> EXECUTE.
    let resp1 = sql
        .submit_values(req.clone())
        .await
        .expect("submit values 1")
        .into_inner();
    let request_id = match resp1.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => r.request_id,
        other => panic!("expected ready_to_execute, got {other:?}"),
    };
    assert!(!request_id.is_empty());

    // 2) confirm.
    let confirm = qc::ConfirmExecutionRequest {
        request_id: request_id.clone(),
        last_modified_epoch: Some(1_791_498_000_000),
        failed_to_clone: false,
        table_type: None,
        execution_results: None,
        execution_runtime_ms: Some(777),
        labels: Default::default(),
    };
    assert!(
        exec.confirm_execution(confirm)
            .await
            .expect("confirm")
            .into_inner()
            .success
    );

    // 3) identical seed submit -> SKIP, echoing the recorded runtime.
    let resp2 = sql
        .submit_values(req.clone())
        .await
        .expect("submit values 2")
        .into_inner();
    match resp2.response {
        Some(qc::submit_sql_response::Response::SkipExecution(s)) => {
            assert_eq!(s.execution_runtime_ms, Some(777));
        }
        other => panic!("expected skip_execution, got {other:?}"),
    }

    // A different values_hash must still EXECUTE (no match).
    let mut req2 = req.clone();
    req2.values_hash = "md5feedface".to_string();
    let resp3 = sql
        .submit_values(req2)
        .await
        .expect("submit values 3")
        .into_inner();
    assert!(matches!(
        resp3.response,
        Some(qc::submit_sql_response::Response::ReadyToExecute(_))
    ));
}

/// Item 4: trivial services return OK empty responses.
#[tokio::test]
async fn trivial_services_return_empty_ok() {
    use qc::explain_client::ExplainClient;
    use qc::selector_service_client::SelectorServiceClient;

    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;

    let mut explain = ExplainClient::new(ch.clone());
    let msgs = explain
        .get_explain_messages(qc::GetExplainMessagesRequest {
            execution_decision_ids: vec!["x".to_string()],
        })
        .await
        .expect("get_explain_messages")
        .into_inner();
    assert!(msgs.messages.is_empty(), "explain messages must be empty");

    let mut selector = SelectorServiceClient::new(ch);
    let sel = selector
        .get_state_selection(qc::SelectorRequest {
            target: "prod".to_string(),
            selector_criteria: 1,
            project_id: "p".to_string(),
            nodes: Vec::new(),
        })
        .await
        .expect("get_state_selection")
        .into_inner();
    assert!(
        sel.node_unique_ids.is_empty(),
        "state selection must be empty"
    );
}
