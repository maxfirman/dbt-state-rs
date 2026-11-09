//! gRPC service implementations backed by the Postgres store + decision engine.

use tonic::{Request, Response, Status};

use crate::execution::store_status as db_err;
use crate::query_cache as qc;
use crate::result_copy::{RegisterHistory, RegisterLookup, RegisterRefusal, RegisterRejection};
use crate::submit_decision::SubmitOutcome;
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

/// Build a SKIP (no-op) SubmitSQLResponse, echoing the previously recorded
/// runtime (if any) just like the hosted service.
fn skip_response(
    description: String,
    is_stale: bool,
    execution_runtime_ms: Option<i64>,
    execution_results: Option<qc::Struct>,
    execution_decision_id: String,
) -> qc::SubmitSqlResponse {
    let explained = qc::ExplainedDecision {
        decision: DECISION_SKIP_EXECUTION,
        skip_rejection_reason: None,
        clone_rejection_reason: None,
        is_stale,
        decision_description: description,
    };
    qc::SubmitSqlResponse {
        response: Some(qc::submit_sql_response::Response::SkipExecution(
            qc::SkipExecutionResponse {
                explained_decision: Some(explained),
                transformed_nodes_by_query: Default::default(),
                execution_results: execution_results.or_else(|| Some(qc::Struct::default())),
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
// RejectionReason::CLONE_CHAIN_LIMIT_EXCEEDED. VERIFIED LIVE: when a target
// disallows clones (profile `allow_clones: false`), the Fusion client encodes it
// as `clone_chain_depth_limit = 0` and the hosted service answers RegisterClone
// with `unable_to_clone` carrying this rejection reason + decision=READY_TO_EXECUTE.
const REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED: i32 = 10;

// RejectionReason::TARGET_TABLE_MISMATCH + description for the SQL-submit
// cross-name clone fallback (an equivalent exists under another name).
const REJECTION_TARGET_TABLE_MISMATCH: i32 = 1;
const CLONE_FALLBACK_DESCRIPTION: &str =
    "an equivalent model exists under another name so we cloned that one";
// VERIFIED LIVE (experiments/clone_experiment.sh table --no-clones): when an
// equivalent exists under another name but the submit target forbids clones
// (`clone_chain_depth_limit=0`), the hosted service executes with this exact
// description plus skip_rejection_reason=1 and clone_rejection_reason=10.
const CLONE_CHAIN_LIMIT_EXECUTE_DESCRIPTION: &str =
    "model was executed because the maximum number of nested clones has been reached";

/// Build a SubmitSQLResponse `ready_to_clone` for the cross-name fallback: an
/// equivalent model exists under another physical name, so clone it into the
/// submit target. VERIFIED LIVE shape: decision=3 (READY_TO_CLONE),
/// skip_rejection_reason=1 (TARGET_TABLE_MISMATCH), clone_rejection_reason=null,
/// the equivalent's build epoch as clone_required_last_modified_epoch, its
/// runtime echoed.
#[allow(deprecated)]
fn submit_ready_to_clone_response(
    request_id: String,
    clone_sqls: Vec<String>,
    clone_source: String,
    clone_target: String,
    clone_required_last_modified_epoch: Option<i64>,
    execution_runtime_ms: Option<i64>,
    execution_decision_id: String,
) -> qc::SubmitSqlResponse {
    qc::SubmitSqlResponse {
        response: Some(qc::submit_sql_response::Response::ReadyToClone(
            qc::ReadyToCloneResponse {
                request_id,
                last_modified_query: String::new(),
                clone_sqls,
                clone_source,
                clone_target,
                explained_decision: Some(qc::ExplainedDecision {
                    decision: DECISION_READY_TO_CLONE,
                    skip_rejection_reason: Some(REJECTION_TARGET_TABLE_MISMATCH),
                    clone_rejection_reason: None,
                    is_stale: false,
                    decision_description: CLONE_FALLBACK_DESCRIPTION.to_string(),
                }),
                transformed_nodes_by_query: Default::default(),
                clone_required_last_modified_epoch,
                clone_execution_results: Some(qc::Struct {
                    fields: Default::default(),
                }),
                execution_runtime_ms,
                execution_decision_id: Some(execution_decision_id),
            },
        )),
    }
}

/// Map a Submit decision outcome to the wire. `clone_target` is the submit
/// target a Copy populates.
fn submit_response(outcome: SubmitOutcome, clone_target: &str) -> qc::SubmitSqlResponse {
    match outcome {
        SubmitOutcome::Skip {
            execution_decision_id,
            matched_execution,
            description,
            is_stale,
        } => skip_response(
            description,
            is_stale,
            matched_execution.execution_runtime_ms,
            matched_execution.execution_results,
            execution_decision_id,
        ),
        SubmitOutcome::Execute {
            ids,
            description,
            skip_rejection_reason,
            clone_rejection_reason,
            is_stale,
            ..
        } => execute_response(
            description,
            skip_rejection_reason,
            clone_rejection_reason,
            is_stale,
            ids.request_id,
            ids.execution_decision_id,
        ),
        SubmitOutcome::Copy {
            ids,
            source_table,
            clone_sqls,
            source_last_modified_epoch,
            source_execution_runtime_ms,
        } => submit_ready_to_clone_response(
            ids.request_id,
            clone_sqls,
            source_table,
            clone_target.to_string(),
            source_last_modified_epoch,
            source_execution_runtime_ms,
            ids.execution_decision_id,
        ),
        SubmitOutcome::ChainLimitExecute { ids } => execute_response(
            CLONE_CHAIN_LIMIT_EXECUTE_DESCRIPTION.to_string(),
            REJECTION_TARGET_TABLE_MISMATCH,
            REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED,
            false,
            ids.request_id,
            ids.execution_decision_id,
        ),
    }
}

/// Build an UnableToCloneResponse matching the hosted shape when a clone is
/// rejected: decision=1 (READY_TO_EXECUTE), the clone_rejection_reason, no
/// skip_rejection_reason, empty description. The deprecated top-level oneof is
/// left EMPTY on this path (verified live — only ready_to_clone mirrors it).
fn unable_to_clone_response(
    clone_source: String,
    clone_target: String,
    clone_rejection_reason: i32,
) -> qc::CloneResponse {
    #[allow(deprecated)]
    qc::CloneResponse {
        ready_to_clone: None,
        unable_to_clone: Some(qc::UnableToCloneResponse {
            clone_source,
            clone_target,
            explained_decision: Some(qc::ExplainedDecision {
                decision: DECISION_READY_TO_EXECUTE,
                skip_rejection_reason: None,
                clone_rejection_reason: Some(clone_rejection_reason),
                is_stale: false,
                decision_description: String::new(),
            }),
        }),
        response: None,
    }
}

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
        .filter(|s| !s.is_empty())
        .unwrap_or("local")
        .to_string()
}

/// Resolve the organization id, enforcing presence when the deployment requires
/// it (`AppState::require_org_header`). Returns the scoped org on success, or
/// `UNAUTHENTICATED` when the header is required but absent/empty. This enforces
/// org SCOPING presence — it is NOT identity authentication (verifying the org
/// against a trusted token is deployment work). Local/insecure runs leave the
/// flag off and fall back to the `local` tenant as before.
fn org_id_checked<T>(state: &AppState, req: &Request<T>) -> Result<String, Status> {
    let present = req
        .metadata()
        .get("x-organization-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    if state.require_org_header && !present {
        return Err(Status::unauthenticated(
            "missing required x-organization-id metadata header",
        ));
    }
    Ok(org_id_of(req))
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
    matched_execution_id: Option<i64>,
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
        clone_source: clone_source_of(response),
        clone_sqls: None,
        input_tables: req.tables.iter().map(Into::into).collect(),
        query_dependencies: req
            .query_dependencies
            .iter()
            .map(|d| d.name.clone())
            .collect(),
        execution_runtime_ms: runtime_of(response),
        matched_execution_id,
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

/// The CLONE source relation carried by a `ReadyToClone` response, if any.
///
/// A cross-name CLONE decision returned via `SubmitEnrichedSQL` carries the
/// physical relation it would be cloned FROM in `ReadyToClone.clone_source`
/// (a `String`). `capture_submit` must persist it so the explain CLONE renderer
/// (`explain.rs::clone_tree`) can emit `could be cloned from "<source>"` instead
/// of an empty string. Non-clone variants have no source; an empty string maps
/// to `None` (nothing to render). (F2: previously hard-coded `None` here.)
fn clone_source_of(resp: &qc::SubmitSqlResponse) -> Option<String> {
    match &resp.response {
        Some(qc::submit_sql_response::Response::ReadyToClone(c)) => {
            Some(c.clone_source.clone()).filter(|s| !s.is_empty())
        }
        _ => None,
    }
}

/// Build + spawn capture for a SubmitValues (seed) decision.
fn capture_submit_values(
    state: &AppState,
    meta: &CaptureMeta,
    req: &qc::SubmitValuesRequest,
    response: &qc::SubmitSqlResponse,
    matched_execution_id: Option<i64>,
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
        matched_execution_id,
    };
    spawn_capture(state, input);
}

fn new_uuid_v7() -> String {
    uuid::Uuid::now_v7().to_string()
}

// Preserve the public conversion path used by upgrade regressions.
pub use crate::execution::sql_execution;

#[derive(Clone)]
pub struct SqlService(pub AppState);

#[tonic::async_trait]
impl Sql for SqlService {
    async fn submit_enriched_sql(
        &self,
        request: Request<qc::SubmitEnrichedSqlRequest>,
    ) -> Result<Response<qc::SubmitSqlResponse>, Status> {
        let org_id = org_id_checked(&self.0, &request)?;
        let cap_meta = capture_meta_of(&request);
        let req = request.into_inner();

        let outcome = crate::submit_decision::submit_sql(&self.0.store, &org_id, &req)
            .await
            .map_err(db_err)?;
        // On a SKIP, or a freshness-driven rebuild EXECUTE whose fingerprint
        // still matched, the matched prior `executions` row is the explain
        // freshness anchor (ADR 0005 / ticket #30). `None` for a first-build /
        // fingerprint-miss EXECUTE and for a CLONE.
        let matched_execution_id = outcome.matched_execution().map(|r| r.id);
        let response = submit_response(outcome, req.target_table.as_deref().unwrap_or_default());

        // Best-effort capture for the UI domain model (non-blocking).
        capture_submit(&self.0, &cap_meta, &req, &response, matched_execution_id);
        Ok(Response::new(response))
    }

    async fn submit_values(
        &self,
        request: Request<qc::SubmitValuesRequest>,
    ) -> Result<Response<qc::SubmitSqlResponse>, Status> {
        let org_id = org_id_checked(&self.0, &request)?;
        let cap_meta = capture_meta_of(&request);
        let req = request.into_inner();

        let outcome = crate::submit_decision::submit_values(&self.0.store, &org_id, &req)
            .await
            .map_err(db_err)?;
        let matched_execution_id = outcome.matched_execution().map(|r| r.id);
        let response = submit_response(outcome, &req.target_table);

        capture_submit_values(&self.0, &cap_meta, &req, &response, matched_execution_id);
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
        let org_id = org_id_checked(&self.0, &request)?;
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
                req.execution_results.as_ref(),
                req.failed_to_clone,
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
        let org_id = org_id_checked(&self.0, &request)?;
        let req = request.into_inner();

        let stored = crate::execution::record_batch(&self.0, &org_id, &req.records).await?;

        Ok(Response::new(qc::RecordExecutionsResponse {
            records_stored: stored,
        }))
    }

    async fn resolve_deferred_relations(
        &self,
        request: Request<qc::ResolveDeferredRelationsRequest>,
    ) -> Result<Response<qc::ResolveDeferredRelationsResponse>, Status> {
        // VERIFIED LIVE (experiments/deferral_experiment.sh): the hosted service
        // maps each requested node_unique_id to the PHYSICAL relation it was last
        // built as in the `target_name` (defer-to) env, scoped by project_id —
        // i.e. the submit `target_table` recorded for that (project, env, node).
        // Nodes not built in that env are simply omitted (the empty-map case we
        // first captured was precisely that: nothing built there yet).
        let org_id = org_id_checked(&self.0, &request)?;
        let req = request.into_inner();

        // project_id scopes the recorded node state (same scope as the selector).
        // Without it the defer-to env's node→FQN mapping is unresolvable, so we
        // return an empty map rather than guessing across projects.
        let Some(project_id) = req.project_id.as_deref().filter(|s| !s.is_empty()) else {
            return Ok(Response::new(qc::ResolveDeferredRelationsResponse {
                fqn_by_unique_id: Default::default(),
            }));
        };
        if req.target_name.is_empty() || req.node_unique_ids.is_empty() {
            return Ok(Response::new(qc::ResolveDeferredRelationsResponse {
                fqn_by_unique_id: Default::default(),
            }));
        }

        let fqn_by_unique_id = self
            .0
            .store
            .resolve_deferred_fqns(&org_id, project_id, &req.target_name, &req.node_unique_ids)
            .await
            .map_err(db_err)?;

        Ok(Response::new(qc::ResolveDeferredRelationsResponse {
            fqn_by_unique_id,
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
        let org_id = org_id_checked(&self.0, &request)?;
        let cap_meta = capture_meta_of(&request);
        let req = request.into_inner();

        // Plan the client-requested copy (see `result_copy`): reject ineligible
        // requests before any read, fetch the history the plan needs, then plan.
        let refuse = |refusal| match refusal {
            RegisterRefusal::Reject(rejection) => Ok(Response::new(unable_to_clone_response(
                req.clone_source_table.clone(),
                req.target_table.clone(),
                match rejection {
                    RegisterRejection::ChainLimitExceeded => REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED,
                    RegisterRejection::ForcedNotEligible => REJECTION_FORCED_NOT_ELIGIBLE,
                },
            ))),
            RegisterRefusal::Unsupported(reason) => Err(Status::unimplemented(reason)),
        };
        let history = match crate::result_copy::register_lookup(&req) {
            Err(refusal) => {
                if refusal == RegisterRefusal::Reject(RegisterRejection::ChainLimitExceeded) {
                    tracing::info!(
                        source = %req.clone_source_table,
                        target = %req.target_table,
                        "clone rejected: clone_chain_depth_limit=0 (allow_clones=false)"
                    );
                }
                return refuse(refusal);
            }
            Ok(RegisterLookup::DuckDbIncrementalSource {
                source,
                table_namespace,
                last_modified_epoch,
                identity,
            }) => RegisterHistory::DuckDbIncrementalSource(
                self.0
                    .store
                    .find_duckdb_incremental_source(
                        &org_id,
                        source,
                        table_namespace,
                        last_modified_epoch,
                        identity,
                    )
                    .await
                    .map_err(db_err)?,
            ),
            Ok(RegisterLookup::SourceLogic {
                source,
                table_namespace,
            }) => RegisterHistory::SourceLogic(
                self.0
                    .store
                    .find_source_ns_logic_hash(&org_id, source, table_namespace, &req.dialect)
                    .await
                    .map_err(db_err)?,
            ),
            Ok(RegisterLookup::Nothing) => RegisterHistory::Nothing,
        };
        let plan = match crate::result_copy::plan_register(&req, history) {
            Ok(plan) => plan,
            Err(refusal) => return refuse(refusal),
        };
        let clone_sqls = plan.clone_sqls;
        let request_id = new_uuid_v7();
        let execution_decision_id = new_uuid_v7();

        // Persist a pending row so the subsequent ConfirmExecution finalizes the
        // clone into skippable history, keyed on the clone target.
        crate::execution::register_copy(
            &self.0.store,
            &org_id,
            &req,
            plan.inherited_ns_logic,
            &request_id,
            &execution_decision_id,
        )
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
                    // CLONE registration matches no prior `executions` row to
                    // anchor freshness against (ADR 0005): NULL.
                    matched_execution_id: None,
                },
            );
        }

        tracing::info!(
            node = %req.labels.get("dbt_node_name").cloned().unwrap_or_default(),
            source = %req.clone_source_table,
            target = %req.target_table,
            "clone decision: READY_TO_CLONE"
        );

        let mut ready = ready_to_clone_response(
            request_id,
            clone_sqls,
            req.clone_source_table.clone(),
            req.target_table.clone(),
            execution_decision_id,
        );

        if plan.required_source_epoch.is_some() {
            ready.clone_required_last_modified_epoch = plan.required_source_epoch;
        }

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
pub struct ExplainService(pub AppState);

#[tonic::async_trait]
impl Explain for ExplainService {
    async fn get_explain_messages(
        &self,
        request: Request<qc::GetExplainMessagesRequest>,
    ) -> Result<Response<qc::GetExplainMessagesResponse>, Status> {
        // Org scoping first (AGENTS.md): never re-derive another tenant's
        // decisions. Mirrors SelectorServiceImpl's org_id_checked pattern.
        let org_id = org_id_checked(&self.0, &request)?;
        let req = request.into_inner();

        // Re-derive each requested decision's tree + description on demand from
        // the persisted node_decisions rows (ADR 0005). Untracked ids (absent
        // from the store) produce no message, so the response can be shorter
        // than the request — matching the hosted "omit untracked" behavior.
        //
        // EVENTUAL CONSISTENCY (F3 — intentional, DO NOT make this read
        // synchronous): the `node_decisions` row for an `execution_decision_id`
        // is written by the Submit path's capture, which is a deliberate
        // fire-and-forget async best-effort write (`spawn_capture`; see
        // `capture_submit` and ADR 0005's "capture is best-effort and
        // separate"). So a decision id issued only microseconds earlier may not
        // yet be visible here; such an absent row is simply treated as untracked
        // and omitted (same as a genuinely unknown id). This is by design —
        // coupling this read to the capture write would change Submit semantics
        // for every node. Clients that need the row observe eventual consistency
        // (the harness polls briefly). Do not add retry/polling to the service.
        let rows = self
            .0
            .store
            .fetch_explain_decisions(&org_id, &req.execution_decision_ids)
            .await
            .map_err(db_err)?;

        let mut by_id: std::collections::HashMap<String, crate::explain::ExplainNode> =
            std::collections::HashMap::new();
        for row in rows {
            let Some(edid) = row.execution_decision_id.clone() else {
                continue;
            };
            let Some(decision) = crate::explain::DecisionClass::from_decision_str(&row.decision)
            else {
                // Unknown decision class — skip rather than invent a tree.
                continue;
            };
            let node = crate::explain::ExplainNode {
                execution_decision_id: edid.clone(),
                decision,
                execution_type: row.execution_type,
                decision_description: row.decision_description.clone(),
                is_stale: row.is_stale,
                target_table: row.target_table.clone(),
                clone_source: row.clone_source.clone(),
                input_tables: row.input_tables(),
                // The matched prior `executions` row (ADR 0005 / ticket #30),
                // loaded via `node_decisions.matched_execution_id`. Present for a
                // SKIP/reuse or a freshness-driven rebuild EXECUTE; `None` for a
                // first-build/fingerprint-miss EXECUTE or a CLONE. The generator
                // re-derives the per-upstream FRESH/OUTDATED freshness subtree
                // against it via the shared pure helper, so the explanation
                // matches the decision exactly.
                anchor: row.anchor(),
            };
            by_id.insert(edid, node);
        }

        // Preserve the request order, emitting only tracked ids.
        let messages = req
            .execution_decision_ids
            .iter()
            .filter_map(|id| by_id.get(id))
            .map(crate::explain::explain_entry)
            .collect();

        Ok(Response::new(qc::GetExplainMessagesResponse { messages }))
    }

    async fn get_upstream_dependency_changes(
        &self,
        _request: Request<qc::GetUpstreamDependencyChangesRequest>,
    ) -> Result<Response<qc::GetUpstreamDependencyChangesResponse>, Status> {
        // Intentionally an empty, non-asserted stub. The dbt Fusion CLI never
        // calls this RPC — verified by binary inspection of v2.0.6 AND v2.0.11
        // (zero occurrences of the method name; the only Explain method the
        // client embeds is GetExplainMessages). The upstream-dependency-changes
        // view is a dbt-platform ("Explain tab") surface that does not pass
        // through the recording proxy, so the hosted narrative is UNCAPTURABLE
        // with this harness. Do not invent the line tree before a real capture.
        // See docs/protocol.md § "Explain.GetUpstreamDependencyChanges is
        // CLIENT-UNREACHABLE" and docs/explain-catalog.md § "Unobserved".
        Ok(Response::new(qc::GetUpstreamDependencyChangesResponse {
            dependency_changes: Vec::new(),
        }))
    }
}

#[derive(Clone)]
pub struct SelectorServiceImpl(pub AppState);

#[tonic::async_trait]
impl SelectorService for SelectorServiceImpl {
    async fn get_state_selection(
        &self,
        request: Request<qc::SelectorRequest>,
    ) -> Result<Response<qc::SelectorResponse>, Status> {
        let org_id = org_id_checked(&self.0, &request)?;
        let req = request.into_inner();

        // Decode the criterion. The client only ever sends 1..=10; SC_UNSPECIFIED
        // and out-of-range are rejected rather than guessed.
        let criteria = crate::selector::SelectorCriteria::from_i32(req.selector_criteria)
            .ok_or_else(|| {
                Status::invalid_argument(format!(
                    "unknown selector_criteria {}",
                    req.selector_criteria
                ))
            })?;

        // project_id scopes the comparison state (required by the client).
        if req.project_id.is_empty() {
            return Err(Status::invalid_argument("project_id is required"));
        }

        // Fetch the recorded comparison state for exactly the submitted nodes,
        // scoped by (org, project_id, target env). Nodes absent from the store
        // are `new` (classify treats a missing recorded node accordingly).
        let uids: Vec<String> = req.nodes.iter().map(|n| n.node_unique_id.clone()).collect();
        let recorded = self
            .0
            .store
            .fetch_node_states(&org_id, &req.project_id, &req.target, &uids)
            .await
            .map_err(db_err)?;

        let node_unique_ids = crate::selector::select_matching(criteria, &req.nodes, |uid| {
            recorded.get(uid).cloned()
        });

        Ok(Response::new(qc::SelectorResponse { node_unique_ids }))
    }
}
