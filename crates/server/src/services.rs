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

fn reusable_test_results(results: Option<&qc::Struct>) -> bool {
    use qc::value::Kind;
    let Some(results) = results else {
        return false;
    };
    let failures = matches!(
        results.fields.get("failures").and_then(|v| v.kind.as_ref()),
        Some(Kind::IntValue(_))
    ) || matches!(results.fields.get("failures").and_then(|v| v.kind.as_ref()),
        Some(Kind::DoubleValue(n)) if n.is_finite());
    failures
        && ["should_error", "should_warn"].iter().all(|key| {
            matches!(
                results.fields.get(*key).and_then(|v| v.kind.as_ref()),
                Some(Kind::BoolValue(_))
            )
        })
}

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

/// Older clients publish identity only in labels.
fn node_unique_id(
    state: Option<&qc::DbtNodeState>,
    labels: &std::collections::HashMap<String, String>,
) -> Option<String> {
    state
        .map(|s| s.node_unique_id.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            labels
                .get("dbt_node_unique_id")
                .map(String::as_str)
                .filter(|s| !s.is_empty())
        })
        .map(str::to_owned)
}

fn namespace(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

fn complete_dependencies(req: &qc::SubmitEnrichedSqlRequest) -> bool {
    let mut names = std::collections::HashSet::new();
    req.query_dependencies
        .iter()
        .all(|d| !d.name.trim().is_empty() && !d.query.trim().is_empty() && names.insert(&d.name))
}

fn current_target_matches(
    req: &qc::SubmitEnrichedSqlRequest,
    prev: &crate::store::ExecutionRow,
) -> bool {
    let Some(target) = req.target_table.as_deref().filter(|s| !s.is_empty()) else {
        return false;
    };
    if prev.target_table != target {
        return false;
    }
    let own: Vec<_> = req.tables.iter().filter(|t| t.name == target).collect();
    // Missing/ambiguous target evidence cannot establish that an object exists.
    own.len() == 1
        && own[0].last_modified_epoch.is_some()
        && (req.ignore_external_modifications
            || matches!((own[0].last_modified_epoch, prev.last_modified_epoch), (Some(current), Some(recorded)) if current <= recorded))
}

fn sql_execution(req: &qc::SubmitEnrichedSqlRequest) -> qc::SqlExecution {
    qc::SqlExecution {
        target_table: req.target_table.clone(),
        dialect: req.dialect.clone(),
        default_catalog: req.default_catalog.clone(),
        default_schema: req.default_schema.clone(),
        execution_type: req.execution_type,
        sql: req.sql.clone(),
        tables: req.tables.clone(),
        query_dependencies: req.query_dependencies.clone(),
        semantic_extras: req.semantic_extras.clone(),
        labels: req.labels.clone(),
        dbt_node_state: req.dbt_node_state.clone(),
        table_namespace: req.table_namespace.clone(),
        from_speculative_submit: false,
    }
}

/// Record a node's comparison state for `state:*` selectors from a submitted
/// `dbt_node_state` (latest wins). CHARACTERIZED LIVE: the hosted service
/// records the client-supplied hashes verbatim on every Submit/Record — keyed
/// by the logical (org, project_id, target env, node_unique_id) — regardless of
/// the SKIP/EXECUTE verdict, and the selector compares against them (see
/// `selector.rs`, migration 0009). A no-op when the node_state lacks the scope
/// keys (target_name/project_id) the selector needs; the executions decision
/// ledger is unaffected.
async fn record_node_state(
    state: &AppState,
    org_id: &str,
    node_state: Option<&qc::DbtNodeState>,
    target_table: Option<&str>,
) {
    let Some(ns) = node_state else { return };
    let (Some(project_id), target_name) = (ns.project_id.clone(), ns.target_name.clone()) else {
        return;
    };
    if project_id.is_empty() || target_name.is_empty() || ns.node_unique_id.is_empty() {
        return;
    }
    let upsert = crate::store::NodeStateUpsert {
        org_id: org_id.to_string(),
        project_id,
        target_name,
        node_unique_id: ns.node_unique_id.clone(),
        node_body_hash: ns.node_body_hash.clone(),
        node_configs_hash: ns.node_configs_hash.clone(),
        node_persisted_descriptions_hash: ns.node_persisted_descriptions_hash.clone(),
        node_macros_hash: ns.node_macros_hash.clone(),
        node_contract_hash: ns.node_contract_hash.clone(),
        node_database_representation: ns.node_database_representation.clone(),
        // The physical FQN this node was built as in `target_name` — used to
        // resolve deferral (ResolveDeferredRelations). Empty targets (data
        // tests) record NULL and are not deferrable.
        target_table: target_table.filter(|s| !s.is_empty()).map(str::to_owned),
    };
    if let Err(e) = state.store.upsert_node_state(&upsert).await {
        // Recording selector state must never fail the decision path. Count the
        // failure so it is observable (metric/alert) rather than silently lost:
        // the execution ledger may advance while `state:*` selectors read a
        // stale baseline, and that divergence must be measurable in deployment.
        state
            .selector_write_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(error = %e, "failed to record node state for selectors");
    }
}

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

        // Record the node's comparison state for `state:*` selectors (latest
        // wins), regardless of the SKIP/EXECUTE verdict below.
        record_node_state(
            &self.0,
            &org_id,
            req.dbt_node_state.as_ref(),
            req.target_table.as_deref(),
        )
        .await;

        let target_table = req.target_table.clone().unwrap_or_default();
        let execution_type = req.execution_type;
        let node_body_hash = decision::node_body_hash_of(&req);
        let template = req
            .compare_unrendered_code
            .then(|| node_body_hash.as_deref().unwrap_or(""));
        let node_sql_hash = Some(crate::fingerprint::sql_logic_hash(
            &sql_execution(&req),
            template,
        ));
        // Schema-abstracted cross-environment logic identity (clone adoption).
        // Only meaningful for rendered (non-template) comparison of real SQL.
        let ns_logic_hash = (!req.compare_unrendered_code && !req.sql.trim().is_empty())
            .then(|| crate::fingerprint::namespace_logic_hash(&sql_execution(&req)));
        let input_tables = decision::input_tables_of(&req);
        let unique_id = node_unique_id(req.dbt_node_state.as_ref(), &req.labels);
        let project_id = req
            .dbt_node_state
            .as_ref()
            .and_then(|s| s.project_id.clone());
        let table_namespace = namespace(req.table_namespace.as_deref());

        // Whether the server has ANY confirmed execution describing this exact
        // physical target (regardless of logic/provenance). NOT used to gate the
        // cross-name clone fallback — LIVE-VERIFIED (experiments/
        // p0_fallback_over_existing.sh) that the hosted service answers
        // `ready_to_clone` for an equivalent under another name EVEN WHEN the
        // target already exists with its own confirmed history holding different
        // logic. The gate is purely "does an equivalent exist for THIS logic".
        let candidate = if execution_type == EXECUTION_TYPE_DBT_DATA_TEST {
            match unique_id.as_deref() {
                Some(uid) => self
                    .0
                    .store
                    .find_confirmed_test(
                        &org_id,
                        uid,
                        project_id.as_deref(),
                        table_namespace,
                        node_sql_hash.as_deref(),
                    )
                    .await
                    .map_err(db_err)?,
                None => None,
            }
        } else {
            let strict = self
                .0
                .store
                .find_confirmed(
                    &org_id,
                    &target_table,
                    execution_type,
                    node_sql_hash.as_deref(),
                    table_namespace,
                    &req.dialect,
                )
                .await
                .map_err(db_err)?;
            // CROSS-ENVIRONMENT CLONE ADOPTION. When the strict (exact rendered
            // SQL) match misses, the physical target may still be reusable if it
            // was CLONED from (or is otherwise) the SAME model in the namespace:
            // its schema-abstracted logic identity equals this submit's. This
            // reproduces the hosted post-clone SKIP, which is logic-sensitive
            // (a genuine SQL change still executes). Verified live; see
            // experiments/clone_logic_sensitivity.sh. Freshness still governs
            // via decide(); we only supply the candidate here.
            match strict {
                Some(row) => Some(row),
                None => {
                    // The CURRENT physical state of this target (latest confirmed
                    // row, any provenance). Selecting the latest first guarantees
                    // a stale clone-origin row cannot be resurrected for adoption
                    // after an ordinary rebuild (review issue #1 P0#2): the row
                    // is adopted (→ possible SKIP) only if it is ITSELF the
                    // current state AND clone-origin AND logic-compatible.
                    let latest_for_target = self
                        .0
                        .store
                        .find_latest_confirmed_target(
                            &org_id,
                            &target_table,
                            execution_type,
                            table_namespace,
                            &req.dialect,
                        )
                        .await
                        .map_err(db_err)?;
                    match ns_logic_hash.as_deref() {
                        Some(ns_hash) => latest_for_target
                            .filter(|prev| {
                                // Only CLONE-ORIGIN rows are adopted cross-environment.
                                // A clone establishes a physical object with the
                                // source's logic; a later submit skips when its
                                // schema-abstracted logic matches. Ordinary (non-clone)
                                // rows are NOT adopted here: that would let a genuine
                                // resolution-context change (e.g. default_schema) skip,
                                // which the hosted service rebuilds. Broader
                                // cross-environment reuse without a clone is a separate
                                // provenance feature (see correctness-handoff.md §A).
                                prev.clone_source_table.is_some()
                                    && prev.node_namespace_logic_hash.as_deref() == Some(ns_hash)
                            })
                            .map(|mut prev| {
                                // A clone-origin row records NO upstream inputs (the
                                // clone had none). Adopt the submit's CURRENT inputs
                                // as the recorded set so the freshness check compares
                                // each upstream against the clone's own build epoch
                                // (prev.last_modified_epoch) rather than treating the
                                // empty recorded set as a dependency-set change. The
                                // own-build anchor stays the authoritative clone epoch.
                                if prev.input_tables.is_empty() {
                                    prev.input_tables = input_tables.clone();
                                }
                                prev
                            }),
                        None => None,
                    }
                }
            }
        };
        // Runtime volatile-function evidence is not implemented. Build instead
        // of claiming reuse when evaluation is requested. Empty template/SQL is
        // likewise insufficient. Automatic cross-target cloning remains a gap.
        let confirmed = candidate.filter(|prev| {
            req.tolerate_nondeterminism
                && complete_dependencies(&req)
                && !req.sql.trim().is_empty()
                && (!req.compare_unrendered_code
                    || node_body_hash.as_ref().is_some_and(|s| !s.is_empty()))
                && if execution_type == EXECUTION_TYPE_DBT_DATA_TEST {
                    reusable_test_results(prev.execution_results.as_ref())
                } else {
                    current_target_matches(&req, prev)
                }
        });

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

        // SQL-SUBMIT CROSS-NAME CLONE FALLBACK. When we would EXECUTE (no reuse
        // match) but an EQUIVALENT model (same namespace + schema-abstracted
        // logic) was already built under a DIFFERENT physical name, the hosted
        // service answers `ready_to_clone` to clone that equivalent into this
        // target ("an equivalent model exists under another name so we cloned
        // that one"). VERIFIED LIVE (experiments/clone_experiment.sh table
        // materialization; golden/fixtures/clone_submit_equivalent_fallback.jsonl).
        // Gated to real-SQL submits with a known namespace logic identity, no
        // confirmed reuse candidate, and not seeds/data tests.
        //
        // allow_clones=false: the client encodes a clone-forbidding target as
        // `clone_chain_depth_limit = 0` on the SUBMIT (NOT via `allow_clones`,
        // which stays unset — VERIFIED LIVE, experiments/clone_experiment.sh
        // table --no-clones). When an equivalent WOULD be cloned but clones are
        // forbidden, the hosted service does NOT emit a generic execute: it
        // returns `ready_to_execute` with skip_rejection_reason=1
        // (TARGET_TABLE_MISMATCH), clone_rejection_reason=10
        // (CLONE_CHAIN_LIMIT_EXCEEDED) and the description below. We reproduce
        // that exact shape (fixture clone_submit_allow_clones_false.jsonl).
        let clones_forbidden =
            req.allow_clones == Some(false) || req.clone_chain_depth_limit == Some(0);
        if confirmed.is_none()
            && execution_type != EXECUTION_TYPE_DBT_DATA_TEST
            && execution_type != EXECUTION_TYPE_VALUES
        {
            if let (Some(ns_hash), Some(ns)) = (ns_logic_hash.as_deref(), table_namespace) {
                if let Some(eq) = self
                    .0
                    .store
                    .find_clone_equivalent(&org_id, ns, ns_hash, execution_type, &target_table)
                    .await
                    .map_err(db_err)?
                {
                    // An equivalent exists under another name. If the target
                    // forbids clones, suppress the clone and emit the hosted
                    // CLONE_CHAIN_LIMIT_EXCEEDED execute shape; otherwise clone.
                    if clones_forbidden {
                        let request_id = new_uuid_v7();
                        let execution_decision_id = new_uuid_v7();
                        // Persist an ordinary (non-clone) pending row so the
                        // forthcoming ConfirmExecution records the normal build.
                        let pending = PendingExecution {
                            org_id: org_id.clone(),
                            project_id: req
                                .dbt_node_state
                                .as_ref()
                                .and_then(|s| s.project_id.clone()),
                            target_table: target_table.clone(),
                            execution_type,
                            node_hash: req.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                            node_body_hash: node_body_hash.clone(),
                            node_sql_hash: node_sql_hash.clone(),
                            node_namespace_logic_hash: ns_logic_hash.clone(),
                            node_configs_hash: req
                                .dbt_node_state
                                .as_ref()
                                .and_then(|s| s.node_configs_hash.clone()),
                            node_contract_hash: req
                                .dbt_node_state
                                .as_ref()
                                .and_then(|s| s.node_contract_hash.clone()),
                            node_unique_id: node_unique_id(
                                req.dbt_node_state.as_ref(),
                                &req.labels,
                            ),
                            table_namespace: req.table_namespace.clone(),
                            dialect: req.dialect.clone(),
                            input_tables: input_tables.clone(),
                            values_hash: None,
                            request_id: request_id.clone(),
                            execution_decision_id: Some(execution_decision_id.clone()),
                            clone_source_table: None,
                        };
                        self.0
                            .store
                            .insert_pending(&pending)
                            .await
                            .map_err(db_err)?;
                        let response = execute_response(
                            CLONE_CHAIN_LIMIT_EXECUTE_DESCRIPTION.to_string(),
                            REJECTION_TARGET_TABLE_MISMATCH,
                            REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED,
                            false,
                            request_id,
                            execution_decision_id,
                        );
                        capture_submit(&self.0, &cap_meta, &req, &response, &input_tables);
                        return Ok(Response::new(response));
                    }
                    if let Ok(clone_sqls) = crate::clone::clone_sqls(
                        &req.dialect,
                        &eq.source_table,
                        &target_table,
                        eq.table_type.as_deref(),
                    ) {
                        let request_id = new_uuid_v7();
                        let execution_decision_id = new_uuid_v7();
                        // Persist a pending row keyed on the submit target so the
                        // forthcoming ConfirmExecution finalizes it, inheriting the
                        // equivalent's logic identity (so later submits reuse it).
                        let pending = PendingExecution {
                            org_id: org_id.clone(),
                            project_id: req
                                .dbt_node_state
                                .as_ref()
                                .and_then(|s| s.project_id.clone()),
                            target_table: target_table.clone(),
                            execution_type,
                            node_hash: req.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                            node_body_hash: node_body_hash.clone(),
                            node_sql_hash: node_sql_hash.clone(),
                            node_namespace_logic_hash: ns_logic_hash.clone(),
                            node_configs_hash: req
                                .dbt_node_state
                                .as_ref()
                                .and_then(|s| s.node_configs_hash.clone()),
                            node_contract_hash: req
                                .dbt_node_state
                                .as_ref()
                                .and_then(|s| s.node_contract_hash.clone()),
                            node_unique_id: node_unique_id(
                                req.dbt_node_state.as_ref(),
                                &req.labels,
                            ),
                            table_namespace: req.table_namespace.clone(),
                            dialect: req.dialect.clone(),
                            input_tables: input_tables.clone(),
                            values_hash: None,
                            request_id: request_id.clone(),
                            execution_decision_id: Some(execution_decision_id.clone()),
                            clone_source_table: Some(eq.source_table.clone()),
                        };
                        self.0
                            .store
                            .insert_pending(&pending)
                            .await
                            .map_err(db_err)?;

                        let response = submit_ready_to_clone_response(
                            request_id,
                            clone_sqls,
                            eq.source_table.clone(),
                            target_table.clone(),
                            eq.last_modified_epoch,
                            eq.execution_runtime_ms,
                            execution_decision_id,
                        );
                        capture_submit(&self.0, &cap_meta, &req, &response, &input_tables);
                        return Ok(Response::new(response));
                    }
                }
            }
        }

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
            Verdict::Skip {
                description,
                is_stale,
            } => skip_response(
                description,
                is_stale,
                confirmed.as_ref().and_then(|r| r.execution_runtime_ms),
                confirmed.as_ref().and_then(|r| r.execution_results.clone()),
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
                    project_id: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.project_id.clone()),
                    target_table: target_table.clone(),
                    execution_type,
                    node_hash: req.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                    node_body_hash: node_body_hash.clone(),
                    node_sql_hash: node_sql_hash.clone(),
                    node_namespace_logic_hash: ns_logic_hash.clone(),
                    node_configs_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_configs_hash.clone()),
                    node_contract_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_contract_hash.clone()),
                    node_unique_id: node_unique_id(req.dbt_node_state.as_ref(), &req.labels),
                    table_namespace: req.table_namespace.clone(),
                    dialect: req.dialect.clone(),
                    input_tables: input_tables.clone(),
                    values_hash: None,
                    request_id: request_id.clone(),
                    execution_decision_id: Some(execution_decision_id.clone()),
                    clone_source_table: None,
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
        let org_id = org_id_checked(&self.0, &request)?;
        let cap_meta = capture_meta_of(&request);
        let req = request.into_inner();

        // Record the seed's comparison state for `state:*` selectors.
        record_node_state(
            &self.0,
            &org_id,
            req.dbt_node_state.as_ref(),
            Some(req.target_table.as_str()),
        )
        .await;

        let target_table = req.target_table.clone();
        // execution_type is implicitly VALUES(9) for seeds.
        let execution_type = EXECUTION_TYPE_VALUES;
        let values_hash = if req.values_hash.is_empty() {
            None
        } else {
            Some(req.values_hash.clone())
        };

        let seed_hash = crate::fingerprint::seed_logic_hash(
            &req.dialect,
            &req.default_catalog,
            &req.semantic_extras,
        );
        let confirmed = self.0.store.find_confirmed_values(
            &org_id, &target_table, &req.values_hash, &seed_hash, namespace(req.table_namespace.as_deref()),
        ).await.map_err(db_err)?.filter(|prev| {
            !req.values_hash.is_empty() && req.last_modified_epoch.is_some()
                && matches!((req.last_modified_epoch, prev.last_modified_epoch), (Some(current), Some(recorded)) if current <= recorded)
        });
        let input_tables: Vec<InputTable> = Vec::new();
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
            Verdict::Skip {
                description,
                is_stale,
            } => skip_response(
                description,
                is_stale,
                confirmed.as_ref().and_then(|r| r.execution_runtime_ms),
                confirmed.as_ref().and_then(|r| r.execution_results.clone()),
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
                    project_id: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.project_id.clone()),
                    target_table: target_table.clone(),
                    execution_type,
                    node_hash: req.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                    node_body_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_body_hash.clone()),
                    node_sql_hash: Some(seed_hash.clone()),
                    node_namespace_logic_hash: None,
                    node_configs_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_configs_hash.clone()),
                    node_contract_hash: req
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_contract_hash.clone()),
                    node_unique_id: node_unique_id(req.dbt_node_state.as_ref(), &req.labels),
                    table_namespace: req.table_namespace.clone(),
                    dialect: req.dialect.clone(),
                    input_tables: input_tables.clone(),
                    values_hash: values_hash.clone(),
                    request_id: request_id.clone(),
                    execution_decision_id: Some(execution_decision_id.clone()),
                    clone_source_table: None,
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
                    project_id: sql
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.project_id.clone()),
                    target_table: sql.target_table.clone().unwrap_or_default(),
                    execution_type: sql.execution_type,
                    node_hash: sql.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                    // SQLExecution carries hashes via dbt_node_state (may be absent);
                    // node_body_hash may therefore be null.
                    node_body_hash: sql
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_body_hash.clone()),
                    // Match the submit path's logic key: whitespace-normalized
                    // rendered SQL + allowlisted semantic_extras.
                    node_sql_hash: Some(crate::fingerprint::sql_logic_hash(sql, None)),
                    node_namespace_logic_hash: Some(crate::fingerprint::namespace_logic_hash(sql)),
                    node_configs_hash: sql
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_configs_hash.clone()),
                    node_contract_hash: sql
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_contract_hash.clone()),
                    node_unique_id: node_unique_id(sql.dbt_node_state.as_ref(), &sql.labels),
                    table_namespace: sql.table_namespace.clone(),
                    dialect: sql.dialect.clone(),
                    input_tables: sql
                        .tables
                        .iter()
                        .map(|t| InputTable {
                            name: t.name.clone(),
                            last_modified_epoch: t.last_modified_epoch,
                        })
                        .collect(),
                    values_hash: None,
                    request_id: new_uuid_v7(),
                    execution_decision_id: Some(new_uuid_v7()),
                    last_modified_epoch: outcome.last_modified_epoch,
                    table_type: outcome.table_type.clone(),
                    execution_runtime_ms: outcome.execution_runtime_ms,
                    execution_results: outcome.execution_results.clone(),
                },
                qc::execution_record::Input::Values(values) => ConfirmedExecution {
                    org_id: org_id.clone(),
                    project_id: values
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.project_id.clone()),
                    target_table: values.target_table.clone(),
                    execution_type: EXECUTION_TYPE_VALUES,
                    node_hash: values.dbt_node_state.as_ref().map(|s| s.node_hash.clone()),
                    node_body_hash: values
                        .dbt_node_state
                        .as_ref()
                        .and_then(|s| s.node_body_hash.clone()),
                    node_sql_hash: Some(crate::fingerprint::seed_logic_hash(
                        &values.dialect,
                        &values.default_catalog,
                        &values.semantic_extras,
                    )),
                    node_namespace_logic_hash: None,
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
                    execution_results: outcome.execution_results.clone(),
                },
            };
            rows.push(row);

            // Record each node's comparison state for `state:*` selectors.
            let node_state = match input {
                qc::execution_record::Input::EnrichedSql(sql) => sql.dbt_node_state.as_ref(),
                qc::execution_record::Input::Values(values) => values.dbt_node_state.as_ref(),
            };
            let node_target = match input {
                qc::execution_record::Input::EnrichedSql(sql) => sql.target_table.as_deref(),
                qc::execution_record::Input::Values(values) => Some(values.target_table.as_str()),
            };
            record_node_state(&self.0, &org_id, node_state, node_target).await;
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

        // allow_clones=false: the Fusion client encodes a target that disallows
        // clones as `clone_chain_depth_limit = 0`. VERIFIED LIVE: the hosted
        // service answers `unable_to_clone` with clone_rejection_reason=10
        // (CLONE_CHAIN_LIMIT_EXCEEDED) and decision=READY_TO_EXECUTE, writes no
        // pending history, and leaves the deprecated oneof empty. The client then
        // builds the node normally.
        if req.clone_chain_depth_limit == Some(0) {
            tracing::info!(
                source = %req.clone_source_table,
                target = %req.target_table,
                "clone rejected: clone_chain_depth_limit=0 (allow_clones=false)"
            );
            return Ok(Response::new(unable_to_clone_response(
                req.clone_source_table.clone(),
                req.target_table.clone(),
                REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED,
            )));
        }

        let request_id = new_uuid_v7();
        let execution_decision_id = new_uuid_v7();

        let clone_sqls = crate::clone::clone_sqls(
            &req.dialect,
            &req.clone_source_table,
            &req.target_table,
            req.clone_source_table_type.as_deref(),
        )
        .map_err(Status::unimplemented)?;

        // Inherit the clone SOURCE's schema-abstracted logic identity so a later
        // submit for the clone TARGET can match it (the cloned object has the
        // source's logic). Looked up by the source's physical name within the
        // same namespace. If the source was never recorded here, the clone row
        // carries no logic identity and a later submit falls back to EXECUTE
        // (safe: we never skip without proven logic equivalence).
        let inherited_ns_logic = self
            .0
            .store
            .find_source_ns_logic_hash(
                &org_id,
                &req.clone_source_table,
                namespace(req.table_namespace.as_deref()),
            )
            .await
            .map_err(db_err)?;

        // Persist a pending row so the subsequent ConfirmExecution finalizes the
        // clone into skippable history, keyed on the clone target.
        let pending = PendingExecution {
            org_id: org_id.clone(),
            project_id: None,
            target_table: req.target_table.clone(),
            execution_type: req.execution_type,
            node_hash: None,
            node_body_hash: None,
            node_sql_hash: None,
            node_namespace_logic_hash: inherited_ns_logic,
            node_configs_hash: None,
            node_contract_hash: None,
            node_unique_id: req.labels.get("dbt_node_unique_id").cloned(),
            table_namespace: req.table_namespace.clone(),
            dialect: req.dialect.clone(),
            input_tables: Vec::new(),
            values_hash: None,
            request_id: request_id.clone(),
            execution_decision_id: Some(execution_decision_id.clone()),
            clone_source_table: Some(req.clone_source_table.clone()),
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

/// Map a store error to a gRPC status. The detailed sqlx error (which may name
/// internal schema objects, columns, or connection fragments) is logged
/// server-side; the client receives only a generic message to avoid leaking
/// implementation details over the wire.
fn db_err(e: sqlx::Error) -> Status {
    tracing::error!(error = %e, "store error");
    Status::internal("internal store error")
}
