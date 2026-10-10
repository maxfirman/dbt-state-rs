//! gRPC service implementations backed by the Postgres store + decision engine.

use tonic::{Request, Response, Status};

use crate::decision::{self, SubmitContext, Verdict};
use crate::query_cache as qc;
use crate::store::{ConfirmedExecution, InputTable, PendingExecution};
use crate::AppState;

use qc::client_validation_server::ClientValidation;
use qc::execution_server::Execution;
use qc::explain_server::Explain;
use qc::selector_service_server::SelectorService;
use qc::sql_server::Sql;

// SubmitSQLResultType values (shared.proto).
const DECISION_SKIP_EXECUTION: i32 = 0;
const DECISION_READY_TO_EXECUTE: i32 = 1;

// ModelExecutionType::VALUES (struct.proto).
const EXECUTION_TYPE_VALUES: i32 = 9;

// ModelExecutionType::DBT_DATA_TEST (shared.proto). Test nodes are keyed by
// node_unique_id, not target_table/node_body_hash (which collide across tests).
const EXECUTION_TYPE_DBT_DATA_TEST: i32 = 8;

/// Build a SKIP (no-op) SubmitSQLResponse, echoing the previously recorded
/// runtime (if any) just like the hosted service.
fn skip_response(
    description: String,
    execution_runtime_ms: Option<i64>,
    execution_decision_id: String,
) -> qc::SubmitSqlResponse {
    let explained = qc::ExplainedDecision {
        decision: DECISION_SKIP_EXECUTION,
        skip_rejection_reason: None,
        clone_rejection_reason: None,
        is_stale: false,
        decision_description: description,
    };
    qc::SubmitSqlResponse {
        response: Some(qc::submit_sql_response::Response::SkipExecution(
            qc::SkipExecutionResponse {
                explained_decision: Some(explained),
                transformed_nodes_by_query: Default::default(),
                execution_results: Some(qc::Struct {
                    fields: Default::default(),
                }),
                execution_runtime_ms,
                execution_decision_id: Some(execution_decision_id),
            },
        )),
    }
}

/// Build a READY_TO_EXECUTE SubmitSQLResponse.
#[allow(deprecated)]
fn execute_response(
    description: String,
    skip_rejection_reason: i32,
    clone_rejection_reason: i32,
    is_stale: bool,
    request_id: String,
    execution_decision_id: String,
) -> qc::SubmitSqlResponse {
    let explained = qc::ExplainedDecision {
        decision: DECISION_READY_TO_EXECUTE,
        skip_rejection_reason: Some(skip_rejection_reason),
        clone_rejection_reason: Some(clone_rejection_reason),
        is_stale,
        decision_description: description,
    };
    qc::SubmitSqlResponse {
        response: Some(qc::submit_sql_response::Response::ReadyToExecute(
            qc::ReadyToExecuteResponse {
                request_id,
                last_modified_query: String::new(),
                explained_decision: Some(explained),
                transformed_nodes_by_query: Default::default(),
                query_hash_metadata_info: None,
                execution_decision_id: Some(execution_decision_id),
            },
        )),
    }
}

// SubmitSQLResultType::READY_TO_CLONE and RejectionReason::FORCED_NOT_ELIGIBLE.
const DECISION_READY_TO_CLONE: i32 = 3;
const REJECTION_FORCED_NOT_ELIGIBLE: i32 = 5;

/// Build a ReadyToCloneResponse matching the hosted service's shape: decision=3
/// (READY_TO_CLONE), skip_rejection_reason=5 (FORCED_NOT_ELIGIBLE), no clone
/// rejection, empty description, empty clone_execution_results struct.
#[allow(deprecated)]
fn ready_to_clone_response(
    request_id: String,
    clone_sqls: Vec<String>,
    clone_source: String,
    clone_target: String,
    execution_decision_id: String,
) -> qc::ReadyToCloneResponse {
    qc::ReadyToCloneResponse {
        request_id,
        last_modified_query: String::new(),
        clone_sqls,
        clone_source,
        clone_target,
        explained_decision: Some(qc::ExplainedDecision {
            decision: DECISION_READY_TO_CLONE,
            skip_rejection_reason: Some(REJECTION_FORCED_NOT_ELIGIBLE),
            clone_rejection_reason: None,
            is_stale: false,
            decision_description: String::new(),
        }),
        transformed_nodes_by_query: Default::default(),
        clone_required_last_modified_epoch: None,
        clone_execution_results: Some(qc::Struct {
            fields: Default::default(),
        }),
        execution_runtime_ms: None,
        execution_decision_id: Some(execution_decision_id),
    }
}

/// Extract the org id from the `x-organization-id` metadata header. Defaults to
/// a sentinel when absent (e.g. insecure local runs that omit it).
fn org_id_of<T>(req: &Request<T>) -> String {
    req.metadata()
        .get("x-organization-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("local")
        .to_string()
}

/// Read a string metadata header, if present and valid UTF-8.
fn meta_str<T>(req: &Request<T>, key: &str) -> Option<String> {
    req.metadata()
        .get(key)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Grouping metadata captured per request for the UI domain model.
#[derive(Clone, Default)]
struct CaptureMeta {
    org_id: String,
    invocation_id: Option<String>,
    session_id: Option<String>,
}

fn capture_meta_of<T>(req: &Request<T>) -> CaptureMeta {
    CaptureMeta {
        org_id: org_id_of(req),
        invocation_id: meta_str(req, "x-dbt-invocation-id"),
        session_id: meta_str(req, "x-session-id"),
    }
}

/// Spawn a best-effort, non-blocking capture of a decision. Never blocks or
/// fails the gRPC response.
fn spawn_capture(state: &AppState, input: crate::capture::CaptureInput) {
    let pool = state.store.pool().clone();
    tokio::spawn(async move {
        crate::capture::capture_decision(&pool, input).await;
    });
}

/// Build + spawn capture for a SubmitEnrichedSQL decision.
fn capture_submit(
    state: &AppState,
    meta: &CaptureMeta,
    req: &qc::SubmitEnrichedSqlRequest,
    response: &qc::SubmitSqlResponse,
    input_tables: &[crate::store::InputTable],
) {
    use crate::capture::{CaptureInput, DecisionKind};
    let ns = req.dbt_node_state.as_ref();
    let (kind, is_stale, request_id, exec_decision_id) = classify_response(response);
    let input = CaptureInput {
        org_id: meta.org_id.clone(),
        external_invocation_id: meta.invocation_id.clone(),
        session_id: meta.session_id.clone(),
        project_external_id: ns.and_then(|s| s.project_id.clone()),
        project_name: ns.map(|s| s.project_name.clone()),
        environment_name: ns.map(|s| s.target_name.clone()),
        profile_name: ns.map(|s| s.profile_name.clone()),
        dialect: Some(req.dialect.clone()),
        database: Some(req.default_catalog.clone()),
        node_unique_id: req.labels.get("dbt_node_unique_id").cloned(),
        node_name: req.labels.get("dbt_node_name").cloned(),
        node_fqn: req.labels.get("dbt_node_fqn").cloned(),
        resource_type: ns.map(|s| s.resource_type.clone()),
        execution_type: req.execution_type,
        decision: kind,
        is_stale,
        decision_description: decision_description_of(response),
        request_id,
        execution_decision_id: exec_decision_id,
        node_body_hash: ns.and_then(|s| s.node_body_hash.clone()),
        values_hash: None,
        table_namespace: req.table_namespace.clone(),
        target_table: req.target_table.clone(),
        default_schema: req.default_schema.clone(),
        clone_source: None,
        clone_sqls: None,
        input_tables: input_tables.to_vec(),
        query_dependencies: req
            .query_dependencies
            .iter()
            .map(|d| d.name.clone())
            .collect(),
        execution_runtime_ms: runtime_of(response),
    };
    let _ = DecisionKind::Build; // keep import used across cfgs
    spawn_capture(state, input);
}

/// Classify a SubmitSQLResponse into (kind, is_stale, request_id, exec_decision_id).
fn classify_response(
    resp: &qc::SubmitSqlResponse,
) -> (
    crate::capture::DecisionKind,
    bool,
    Option<String>,
    Option<String>,
) {
    use crate::capture::DecisionKind;
    match &resp.response {
        Some(qc::submit_sql_response::Response::SkipExecution(s)) => (
            DecisionKind::Skip,
            s.explained_decision
                .as_ref()
                .map(|e| e.is_stale)
                .unwrap_or(false),
            None,
            s.execution_decision_id.clone(),
        ),
        Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => (
            DecisionKind::Build,
            r.explained_decision
                .as_ref()
                .map(|e| e.is_stale)
                .unwrap_or(false),
            Some(r.request_id.clone()),
            r.execution_decision_id.clone(),
        ),
        Some(qc::submit_sql_response::Response::ReadyToClone(c)) => (
            DecisionKind::Clone,
            false,
            Some(c.request_id.clone()),
            c.execution_decision_id.clone(),
        ),
        None => (DecisionKind::Build, false, None, None),
    }
}

fn decision_description_of(resp: &qc::SubmitSqlResponse) -> Option<String> {
    let ed = match &resp.response {
        Some(qc::submit_sql_response::Response::SkipExecution(s)) => s.explained_decision.as_ref(),
        Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => r.explained_decision.as_ref(),
        Some(qc::submit_sql_response::Response::ReadyToClone(c)) => c.explained_decision.as_ref(),
        None => None,
    };
    ed.map(|e| e.decision_description.clone())
        .filter(|s| !s.is_empty())
}

fn runtime_of(resp: &qc::SubmitSqlResponse) -> Option<i64> {
    match &resp.response {
        Some(qc::submit_sql_response::Response::SkipExecution(s)) => s.execution_runtime_ms,
        _ => None,
    }
}

/// Build + spawn capture for a SubmitValues (seed) decision.
fn capture_submit_values(
    state: &AppState,
    meta: &CaptureMeta,
    req: &qc::SubmitValuesRequest,
    response: &qc::SubmitSqlResponse,
) {
    use crate::capture::CaptureInput;
    let ns = req.dbt_node_state.as_ref();
    let (kind, is_stale, request_id, exec_decision_id) = classify_response(response);
    let input = CaptureInput {
        org_id: meta.org_id.clone(),
        external_invocation_id: meta.invocation_id.clone(),
        session_id: meta.session_id.clone(),
        project_external_id: ns.and_then(|s| s.project_id.clone()),
        project_name: ns.map(|s| s.project_name.clone()),
        environment_name: ns.map(|s| s.target_name.clone()),
        profile_name: ns.map(|s| s.profile_name.clone()),
        dialect: Some(req.dialect.clone()),
        database: Some(req.default_catalog.clone()),
        node_unique_id: req.labels.get("dbt_node_unique_id").cloned(),
        node_name: req.labels.get("dbt_node_name").cloned(),
        node_fqn: req.labels.get("dbt_node_fqn").cloned(),
        resource_type: ns.map(|s| s.resource_type.clone()),
        execution_type: EXECUTION_TYPE_VALUES,
        decision: kind,
        is_stale,
        decision_description: decision_description_of(response),
        request_id,
        execution_decision_id: exec_decision_id,
        node_body_hash: ns.and_then(|s| s.node_body_hash.clone()),
        values_hash: if req.values_hash.is_empty() {
            None
        } else {
            Some(req.values_hash.clone())
        },
        table_namespace: req.table_namespace.clone(),
        target_table: Some(req.target_table.clone()),
        default_schema: None,
        clone_source: None,
        clone_sqls: None,
        input_tables: Vec::new(),
        query_dependencies: Vec::new(),
        execution_runtime_ms: runtime_of(response),
    };
    spawn_capture(state, input);
}

fn new_uuid_v7() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Hash of the raw (rendered) SQL the client sends, used alongside
/// `node_body_hash` in the match key. The hosted service compares RENDERED SQL
/// by default (`compare_unrendered_code=false`), so a changed rendered SQL —
/// e.g. a different `env_var` value — must force a rebuild even though the
/// client's `node_body_hash` (an UNRENDERED template hash) is unchanged.
/// Returns `None` for empty SQL (seeds/clones carry none) so those paths retain
/// their prior `NULL` match semantics. See `c2_rendered_sql.rs`.
fn sql_hash_of(sql: &str) -> Option<String> {
    if sql.is_empty() {
        return None;
    }
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(sql.as_bytes());
    Some(hex::encode(h.finalize()))
}

#[derive(Clone)]
pub struct SqlService(pub AppState);

#[tonic::async_trait]
impl Sql for SqlService {
    async fn submit_enriched_sql(
        &self,
        request: Request<qc::SubmitEnrichedSqlRequest>,
    ) -> Result<Response<qc::SubmitSqlResponse>, Status> {
        let org_id = org_id_of(&request);
        let cap_meta = capture_meta_of(&request);
        let req = request.into_inner();

        let target_table = req.target_table.clone().unwrap_or_default();
        let execution_type = req.execution_type;
        let node_body_hash = decision::node_body_hash_of(&req);
        let node_sql_hash = sql_hash_of(&req.sql);
        let input_tables = decision::input_tables_of(&req);
        let node_unique_id = req
            .dbt_node_state
            .as_ref()
            .map(|s| s.node_unique_id.clone())
            .filter(|s| !s.is_empty());

        let confirmed = {
            // DATA TEST nodes (execution_type = DBT_DATA_TEST = 8) carry no
            // per-node body identity: every test of the same generic type shares
            // one node_body_hash and target_table is empty. The hosted service
            // distinguishes them by node_unique_id (verified live — see
            // c1_test_node_identity.rs). Match on node_unique_id for these so a
            // new column test does not wrongly collide with a confirmed sibling.
            if execution_type == EXECUTION_TYPE_DBT_DATA_TEST {
                match node_unique_id.as_deref() {
                    Some(uid) => self
                        .0
                        .store
                        .find_confirmed_by_unique_id(&org_id, uid, execution_type)
                        .await
                        .map_err(db_err)?,
                    // No unique id to key on: fall back to the physical match.
                    None => self
                        .0
                        .store
                        .find_confirmed(
                            &org_id,
                            &target_table,
                            execution_type,
                            node_body_hash.as_deref(),
                            None,
                        )
                        .await
                        .map_err(db_err)?,
                }
            } else {
                // Prefer logical cross-environment matching by table_namespace +
                // node_body_hash (mirrors the hosted service's state reuse across
                // environments). Fall back to physical target_table matching when
                // no namespace is supplied.
                let by_ns = match req.table_namespace.as_deref() {
                    Some(ns) if !ns.is_empty() => self
                        .0
                        .store
                        .find_confirmed_by_namespace(
                            &org_id,
                            ns,
                            execution_type,
                            node_body_hash.as_deref(),
                            node_sql_hash.as_deref(),
                        )
                        .await
                        .map_err(db_err)?,
                    _ => None,
                };
                match by_ns {
                    Some(row) => Some(row),
                    None => self
                        .0
                        .store
                        .find_confirmed(
                            &org_id,
                            &target_table,
                            execution_type,
                            node_body_hash.as_deref(),
                            node_sql_hash.as_deref(),
                        )
                        .await
                        .map_err(db_err)?,
                }
            }
        };

        let ctx = SubmitContext {
            execution_type,
            node_body_hash: node_body_hash.as_deref(),
            input_tables: &input_tables,
            freshness_tolerance_seconds: req.freshness_tolerance_seconds,
            target_table: req.target_table.as_deref(),
            stale_upstream_policy: decision::StaleUpstreamPolicy::from_i32(
                req.stale_upstream_policy,
            ),
        };
        let verdict = decision::decide(&ctx, confirmed.as_ref());

        tracing::info!(
            node = %req.labels.get("dbt_node_name").cloned().unwrap_or_default(),
            execution_type,
            had_match = confirmed.is_some(),
            body_hash = %node_body_hash.clone().unwrap_or_default(),
            verdict = match &verdict { Verdict::Skip{..} => "SKIP", Verdict::Execute{..} => "EXECUTE" },
            "decision"
        );

        let execution_decision_id = new_uuid_v7();
        let response = match verdict {
            Verdict::Skip { description } => skip_response(
                description,
                confirmed.as_ref().and_then(|r| r.execution_runtime_ms),
                execution_decision_id,
            ),
            Verdict::Execute {
                description,
                skip_rejection_reason,
                clone_rejection_reason,
                is_stale,
            } => {
                let request_id = new_uuid_v7();
                // Persist a pending row so the forthcoming ConfirmExecution can
                // finalize this fingerprint into skippable history.
                let pending = PendingExecution {
                    org_id: org_id.clone(),
                    target_table: target_table.clone(),
                    execution_type,
                    node_hash: req.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                    node_body_hash: node_body_hash.clone(),
                    node_sql_hash: node_sql_hash.clone(),
                    node_configs_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_configs_hash.clone()),
                    node_contract_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_contract_hash.clone()),
                    node_unique_id: req
                        .dbt_node_state
                        .as_ref()
                        .map(|s| s.node_unique_id.clone()),
                    table_namespace: req.table_namespace.clone(),
                    dialect: req.dialect.clone(),
                    input_tables: input_tables.clone(),
                    values_hash: None,
                    request_id: request_id.clone(),
                    execution_decision_id: Some(execution_decision_id.clone()),
                };
                self.0
                    .store
                    .insert_pending(&pending)
                    .await
                    .map_err(db_err)?;
                execute_response(
                    description,
                    skip_rejection_reason,
                    clone_rejection_reason,
                    is_stale,
                    request_id,
                    execution_decision_id,
                )
            }
        };

        // Best-effort capture for the UI domain model (non-blocking).
        capture_submit(&self.0, &cap_meta, &req, &response, &input_tables);

        Ok(Response::new(response))
    }

    async fn submit_values(
        &self,
        request: Request<qc::SubmitValuesRequest>,
    ) -> Result<Response<qc::SubmitSqlResponse>, Status> {
        let org_id = org_id_of(&request);
        let cap_meta = capture_meta_of(&request);
        let req = request.into_inner();

        let target_table = req.target_table.clone();
        // execution_type is implicitly VALUES(9) for seeds.
        let execution_type = EXECUTION_TYPE_VALUES;
        let values_hash = if req.values_hash.is_empty() {
            None
        } else {
            Some(req.values_hash.clone())
        };

        let confirmed = self
            .0
            .store
            .find_confirmed_values(
                &org_id,
                &target_table,
                execution_type,
                values_hash.as_deref(),
            )
            .await
            .map_err(db_err)?;

        // Seeds carry no upstream `tables`; the decision is driven purely by a
        // values_hash match. Freshness defaults to fresh with empty inputs.
        //
        // SAFETY NOTE (C8): `freshness_tolerance_seconds = 0` and policy = Any
        // below are inert ONLY because `input_tables` is always empty here
        // (`considered == 0 => not stale`). If seeds ever gain upstream inputs,
        // these hardcodes would silently apply zero tolerance — revisit then.
        let input_tables: Vec<InputTable> = Vec::new();
        debug_assert!(
            input_tables.is_empty(),
            "seed decisions assume no upstream inputs; the tolerance/policy \
             constants below are only safe for an empty input set"
        );
        let ctx = SubmitContext {
            execution_type,
            node_body_hash: None,
            input_tables: &input_tables,
            freshness_tolerance_seconds: 0,
            target_table: Some(target_table.as_str()),
            stale_upstream_policy: decision::StaleUpstreamPolicy::Any,
        };
        let verdict = decision::decide(&ctx, confirmed.as_ref());

        tracing::info!(
            node = %req.labels.get("dbt_node_name").cloned().unwrap_or_default(),
            execution_type,
            had_match = confirmed.is_some(),
            values_hash = %values_hash.clone().unwrap_or_default(),
            verdict = match &verdict { Verdict::Skip{..} => "SKIP", Verdict::Execute{..} => "EXECUTE" },
            "decision(values)"
        );

        let execution_decision_id = new_uuid_v7();
        let response = match verdict {
            Verdict::Skip { description } => skip_response(
                description,
                confirmed.as_ref().and_then(|r| r.execution_runtime_ms),
                execution_decision_id,
            ),
            Verdict::Execute {
                description,
                skip_rejection_reason,
                clone_rejection_reason,
                is_stale,
            } => {
                let request_id = new_uuid_v7();
                let pending = PendingExecution {
                    org_id: org_id.clone(),
                    target_table: target_table.clone(),
                    execution_type,
                    node_hash: req.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                    node_body_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_body_hash.clone()),
                    node_sql_hash: None,
                    node_configs_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_configs_hash.clone()),
                    node_contract_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_contract_hash.clone()),
                    node_unique_id: req
                        .dbt_node_state
                        .as_ref()
                        .map(|s| s.node_unique_id.clone()),
                    table_namespace: req.table_namespace.clone(),
                    dialect: req.dialect.clone(),
                    input_tables: input_tables.clone(),
                    values_hash: values_hash.clone(),
                    request_id: request_id.clone(),
                    execution_decision_id: Some(execution_decision_id.clone()),
                };
                self.0
                    .store
                    .insert_pending(&pending)
                    .await
                    .map_err(db_err)?;
                execute_response(
                    description,
                    skip_rejection_reason,
                    clone_rejection_reason,
                    is_stale,
                    request_id,
                    execution_decision_id,
                )
            }
        };

        capture_submit_values(&self.0, &cap_meta, &req, &response);
        Ok(Response::new(response))
    }

    async fn submit_enriched_sql_speculative(
        &self,
        _request: Request<qc::SubmitEnrichedSqlRequest>,
    ) -> Result<Response<qc::SubmitSqlSpeculativeResponse>, Status> {
        // The hosted service returned `undecided` for every speculative call in
        // our corpus. Match that conservative default.
        Ok(Response::new(qc::SubmitSqlSpeculativeResponse {
            response: Some(qc::submit_sql_speculative_response::Response::Undecided(
                qc::UndecidedResponse {},
            )),
        }))
    }
}

#[derive(Clone)]
pub struct ExecutionService(pub AppState);

#[tonic::async_trait]
impl Execution for ExecutionService {
    async fn confirm_execution(
        &self,
        request: Request<qc::ConfirmExecutionRequest>,
    ) -> Result<Response<qc::ConfirmExecutionResponse>, Status> {
        let org_id = org_id_of(&request);
        let req = request.into_inner();
        let found = self
            .0
            .store
            .confirm(
                &org_id,
                &req.request_id,
                req.last_modified_epoch,
                req.table_type.as_deref(),
                req.execution_runtime_ms,
            )
            .await
            .map_err(db_err)?;

        Ok(Response::new(qc::ConfirmExecutionResponse {
            success: found,
            request_id: req.request_id,
        }))
    }

    async fn record_executions(
        &self,
        request: Request<qc::RecordExecutionsRequest>,
    ) -> Result<Response<qc::RecordExecutionsResponse>, Status> {
        let org_id = org_id_of(&request);
        let req = request.into_inner();

        let mut rows: Vec<ConfirmedExecution> = Vec::with_capacity(req.records.len());
        for record in &req.records {
            let outcome = record.outcome.clone().unwrap_or_default();
            let input = record
                .input
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("ExecutionRecord missing input"))?;

            let row = match input {
                qc::execution_record::Input::EnrichedSql(sql) => ConfirmedExecution {
                    org_id: org_id.clone(),
                    target_table: sql.target_table.clone().unwrap_or_default(),
                    execution_type: sql.execution_type,
                    node_hash: sql.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                    // SQLExecution carries hashes via dbt_node_state (may be absent);
                    // node_body_hash may therefore be null.
                    node_body_hash: sql
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_body_hash.clone()),
                    node_sql_hash: sql_hash_of(&sql.sql),
                    node_configs_hash: sql
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_configs_hash.clone()),
                    node_contract_hash: sql
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_contract_hash.clone()),
                    node_unique_id: sql
                        .dbt_node_state
                        .as_ref()
                        .map(|s| s.node_unique_id.clone()),
                    table_namespace: sql.table_namespace.clone(),
                    dialect: sql.dialect.clone(),
                    input_tables: sql
                        .tables
                        .iter()
                        .map(|t| InputTable {
                            name: t.name.clone(),
                            last_modified_epoch: t.last_modified_epoch.unwrap_or(0),
                        })
                        .collect(),
                    values_hash: None,
                    request_id: new_uuid_v7(),
                    execution_decision_id: Some(new_uuid_v7()),
                    last_modified_epoch: outcome.last_modified_epoch,
                    table_type: outcome.table_type.clone(),
                    execution_runtime_ms: outcome.execution_runtime_ms,
                },
                qc::execution_record::Input::Values(values) => ConfirmedExecution {
                    org_id: org_id.clone(),
                    target_table: values.target_table.clone(),
                    execution_type: EXECUTION_TYPE_VALUES,
                    node_hash: values.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                    node_body_hash: values
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_body_hash.clone()),
                    node_sql_hash: None,
                    node_configs_hash: values
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_configs_hash.clone()),
                    node_contract_hash: values
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_contract_hash.clone()),
                    node_unique_id: values
                        .dbt_node_state
                        .as_ref()
                        .map(|s| s.node_unique_id.clone()),
                    table_namespace: values.table_namespace.clone(),
                    dialect: values.dialect.clone(),
                    input_tables: Vec::new(),
                    values_hash: if values.values_hash.is_empty() {
                        None
                    } else {
                        Some(values.values_hash.clone())
                    },
                    request_id: new_uuid_v7(),
                    execution_decision_id: Some(new_uuid_v7()),
                    last_modified_epoch: outcome.last_modified_epoch,
                    table_type: outcome.table_type.clone(),
                    execution_runtime_ms: outcome.execution_runtime_ms,
                },
            };
            rows.push(row);
        }

        let stored = self
            .0
            .store
            .insert_confirmed_batch(&rows)
            .await
            .map_err(db_err)?;

        Ok(Response::new(qc::RecordExecutionsResponse {
            records_stored: stored,
        }))
    }

    async fn resolve_deferred_relations(
        &self,
        _request: Request<qc::ResolveDeferredRelationsRequest>,
    ) -> Result<Response<qc::ResolveDeferredRelationsResponse>, Status> {
        // Conservative default: resolve nothing (echo back no relations).
        Ok(Response::new(qc::ResolveDeferredRelationsResponse {
            fqn_by_unique_id: Default::default(),
        }))
    }
}

#[derive(Clone)]
pub struct CloneServiceImpl(pub AppState);

#[tonic::async_trait]
impl qc::clone_server::Clone for CloneServiceImpl {
    async fn register_clone(
        &self,
        request: Request<qc::CloneRequest>,
    ) -> Result<Response<qc::CloneResponse>, Status> {
        let org_id = org_id_of(&request);
        let cap_meta = capture_meta_of(&request);
        let req = request.into_inner();

        let request_id = new_uuid_v7();
        let execution_decision_id = new_uuid_v7();

        let clone_sqls = crate::clone::clone_sqls(
            &req.dialect,
            &req.clone_source_table,
            &req.target_table,
            req.clone_source_table_type.as_deref(),
        );

        // Persist a pending row so the subsequent ConfirmExecution finalizes the
        // clone into skippable history, keyed on the clone target.
        let pending = PendingExecution {
            org_id: org_id.clone(),
            target_table: req.target_table.clone(),
            execution_type: req.execution_type,
            node_hash: None,
            node_body_hash: None,
            node_sql_hash: None,
            node_configs_hash: None,
            node_contract_hash: None,
            node_unique_id: req.labels.get("dbt_node_unique_id").cloned(),
            table_namespace: req.table_namespace.clone(),
            dialect: req.dialect.clone(),
            input_tables: Vec::new(),
            values_hash: None,
            request_id: request_id.clone(),
            execution_decision_id: Some(execution_decision_id.clone()),
        };
        self.0
            .store
            .insert_pending(&pending)
            .await
            .map_err(db_err)?;

        // Best-effort capture (non-blocking) of the clone decision.
        {
            use crate::capture::{CaptureInput, DecisionKind};
            spawn_capture(
                &self.0,
                CaptureInput {
                    org_id: org_id.clone(),
                    external_invocation_id: cap_meta.invocation_id.clone(),
                    session_id: cap_meta.session_id.clone(),
                    project_external_id: None,
                    project_name: None,
                    environment_name: None,
                    profile_name: None,
                    dialect: Some(req.dialect.clone()),
                    database: Some(req.default_catalog.clone()),
                    node_unique_id: req.labels.get("dbt_node_unique_id").cloned(),
                    node_name: req.labels.get("dbt_node_name").cloned(),
                    node_fqn: req.labels.get("dbt_node_fqn").cloned(),
                    resource_type: None,
                    execution_type: req.execution_type,
                    decision: DecisionKind::Clone,
                    is_stale: false,
                    decision_description: None,
                    request_id: Some(request_id.clone()),
                    execution_decision_id: Some(execution_decision_id.clone()),
                    node_body_hash: None,
                    values_hash: None,
                    table_namespace: req.table_namespace.clone(),
                    target_table: Some(req.target_table.clone()),
                    default_schema: None,
                    clone_source: Some(req.clone_source_table.clone()),
                    clone_sqls: Some(clone_sqls.clone()),
                    input_tables: Vec::new(),
                    query_dependencies: Vec::new(),
                    execution_runtime_ms: None,
                },
            );
        }

        tracing::info!(
            node = %req.labels.get("dbt_node_name").cloned().unwrap_or_default(),
            source = %req.clone_source_table,
            target = %req.target_table,
            "clone decision: READY_TO_CLONE"
        );

        let ready = ready_to_clone_response(
            request_id,
            clone_sqls,
            req.clone_source_table.clone(),
            req.target_table.clone(),
            execution_decision_id,
        );

        // The real CloneResponse populates both the new `ready_to_clone` field
        // (3) and the deprecated `ready_to_clone_v1` oneof (1) with the same
        // payload.
        #[allow(deprecated)]
        let response = qc::CloneResponse {
            ready_to_clone: Some(ready.clone()),
            unable_to_clone: None,
            response: Some(qc::clone_response::Response::ReadyToCloneV1(ready)),
        };
        Ok(Response::new(response))
    }
}

#[derive(Clone)]
pub struct ClientValidationService;

#[tonic::async_trait]
impl ClientValidation for ClientValidationService {
    async fn validate_client_version(
        &self,
        _request: Request<qc::ValidateClientVersionRequest>,
    ) -> Result<Response<qc::ValidateClientVersionResponse>, Status> {
        Ok(Response::new(qc::ValidateClientVersionResponse {
            is_supported: true,
        }))
    }
}

#[derive(Clone)]
pub struct ExplainService;

#[tonic::async_trait]
impl Explain for ExplainService {
    async fn get_explain_messages(
        &self,
        _request: Request<qc::GetExplainMessagesRequest>,
    ) -> Result<Response<qc::GetExplainMessagesResponse>, Status> {
        // No human-readable explain text generated yet; return an empty list.
        Ok(Response::new(qc::GetExplainMessagesResponse {
            messages: Vec::new(),
        }))
    }

    async fn get_upstream_dependency_changes(
        &self,
        _request: Request<qc::GetUpstreamDependencyChangesRequest>,
    ) -> Result<Response<qc::GetUpstreamDependencyChangesResponse>, Status> {
        Ok(Response::new(qc::GetUpstreamDependencyChangesResponse {
            dependency_changes: Vec::new(),
        }))
    }
}

#[derive(Clone)]
pub struct SelectorServiceImpl;

#[tonic::async_trait]
impl SelectorService for SelectorServiceImpl {
    async fn get_state_selection(
        &self,
        _request: Request<qc::SelectorRequest>,
    ) -> Result<Response<qc::SelectorResponse>, Status> {
        // Conservative default: select nothing.
        Ok(Response::new(qc::SelectorResponse {
            node_unique_ids: Vec::new(),
        }))
    }
}

/// Map a store error to a gRPC status. The detailed sqlx error (which may name
/// internal schema objects, columns, or connection fragments) is logged
/// server-side; the client receives only a generic message to avoid leaking
/// implementation details over the wire.
fn db_err(e: sqlx::Error) -> Status {
    tracing::error!(error = %e, "store error");
    Status::internal("internal store error")
}
