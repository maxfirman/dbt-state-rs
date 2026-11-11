//! gRPC service implementations backed by the Postgres store + decision engine.

use tonic::{Request, Response, Status};

use crate::decision::{
    DecisionKind, DecisionReason, DecisionRecord, REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED,
    REJECTION_FORCED_NOT_ELIGIBLE,
};
use crate::execution::RecordError;
use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
use crate::result_copy::{RegisterHistory, RegisterLookup, RegisterRefusal, RegisterRejection};
use crate::store::CopySourceQuery;
use crate::submit_decision::SubmitOutcome;
use crate::AppState;

use qc::client_validation_server::ClientValidation;
use qc::execution_server::Execution;
use qc::explain_server::Explain;
use qc::selector_service_server::SelectorService;
use qc::sql_server::Sql;

/// The wire `ExplainedDecision` of a Submit decision record.
fn explained_decision(record: &DecisionRecord) -> qc::ExplainedDecision {
    qc::ExplainedDecision {
        decision: record.class.wire_decision(),
        skip_rejection_reason: record.skip_rejection_reason,
        clone_rejection_reason: record.clone_rejection_reason,
        is_stale: record.is_stale,
        decision_description: record.description.clone(),
    }
}

/// Map a Submit decision outcome to the wire. `clone_target` is the submit
/// target a Copy populates.
///
/// - Skip echoes the matched execution's runtime and cached results, like the
///   hosted service.
/// - Copy (the cross-name fallback, VERIFIED LIVE shape) carries the
///   equivalent's build epoch as `clone_required_last_modified_epoch` and
///   echoes its runtime.
#[allow(deprecated)]
fn submit_response(outcome: SubmitOutcome, clone_target: &str) -> qc::SubmitSqlResponse {
    use qc::submit_sql_response::Response;
    let response = match outcome {
        SubmitOutcome::Skip {
            execution_decision_id,
            matched_execution,
            record,
        } => Response::SkipExecution(qc::SkipExecutionResponse {
            explained_decision: Some(explained_decision(&record)),
            transformed_nodes_by_query: Default::default(),
            execution_results: matched_execution
                .execution_results
                .or_else(|| Some(qc::Struct::default())),
            execution_runtime_ms: matched_execution.execution_runtime_ms,
            execution_decision_id: Some(execution_decision_id),
        }),
        SubmitOutcome::Execute { ids, record, .. }
        | SubmitOutcome::ChainLimitExecute { ids, record, .. } => {
            Response::ReadyToExecute(qc::ReadyToExecuteResponse {
                request_id: ids.request_id,
                last_modified_query: String::new(),
                explained_decision: Some(explained_decision(&record)),
                transformed_nodes_by_query: Default::default(),
                query_hash_metadata_info: None,
                execution_decision_id: Some(ids.execution_decision_id),
            })
        }
        SubmitOutcome::Copy { ids, copy, record } => {
            Response::ReadyToClone(qc::ReadyToCloneResponse {
                request_id: ids.request_id,
                last_modified_query: String::new(),
                clone_sqls: copy.clone_sqls,
                clone_source: copy.source_table,
                clone_target: clone_target.to_string(),
                explained_decision: Some(explained_decision(&record)),
                transformed_nodes_by_query: Default::default(),
                clone_required_last_modified_epoch: copy.source_last_modified_epoch,
                clone_execution_results: Some(qc::Struct {
                    fields: Default::default(),
                }),
                execution_runtime_ms: copy.source_execution_runtime_ms,
                execution_decision_id: Some(ids.execution_decision_id),
            })
        }
    };
    qc::SubmitSqlResponse {
        response: Some(response),
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
                decision: DecisionKind::Build.wire_decision(),
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
            decision: DecisionKind::Clone.wire_decision(),
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

/// Map a store error to a gRPC status. The detailed sqlx error (which may name
/// internal schema objects, columns, or connection fragments) is logged
/// server-side; the client receives only a generic message to avoid leaking
/// implementation details over the wire.
fn store_status(e: sqlx::Error) -> Status {
    tracing::error!(error = %e, "store error");
    Status::internal("internal store error")
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

/// The capture of a Submit decision. The node is described by the
/// submission's node state and labels; the decision is read from the outcome
/// and its decision record, never re-decoded from the wire response. Fields
/// only some submissions carry start empty for the caller to fill.
fn submit_capture(
    meta: &CaptureMeta,
    outcome: &SubmitOutcome,
    node_state: Option<&qc::DbtNodeState>,
    labels: &std::collections::HashMap<String, String>,
    dialect: &str,
    database: &str,
    execution_type: i32,
) -> crate::capture::CaptureInput {
    let record = outcome.record();
    crate::capture::CaptureInput {
        org_id: meta.org_id.clone(),
        external_invocation_id: meta.invocation_id.clone(),
        session_id: meta.session_id.clone(),
        project_external_id: node_state.and_then(|s| s.project_id.clone()),
        project_name: node_state.map(|s| s.project_name.clone()),
        environment_name: node_state.map(|s| s.target_name.clone()),
        profile_name: node_state.map(|s| s.profile_name.clone()),
        dialect: Some(dialect.to_string()),
        database: Some(database.to_string()),
        node_unique_id: labels.get("dbt_node_unique_id").cloned(),
        node_name: labels.get("dbt_node_name").cloned(),
        node_fqn: labels.get("dbt_node_fqn").cloned(),
        resource_type: node_state.map(|s| s.resource_type.clone()),
        execution_type,
        decision: record.class,
        decision_reason: record.reason,
        is_stale: record.is_stale,
        decision_description: Some(record.description.clone()).filter(|s| !s.is_empty()),
        request_id: outcome.ids().map(|ids| ids.request_id.clone()),
        execution_decision_id: Some(outcome.execution_decision_id().to_string()),
        node_body_hash: node_state.and_then(|s| s.node_body_hash.clone()),
        values_hash: None,
        table_namespace: None,
        target_table: None,
        default_schema: None,
        // The physical relation a Copy is made FROM, so the Explain CLONE
        // renderer (`explain.rs::clone_tree`) can emit `could be cloned from
        // "<source>"` (F2); for a build whose copy the submit disallowed, the
        // equivalent it could have been copied from.
        clone_source: match outcome {
            SubmitOutcome::Copy { copy, .. } => {
                Some(copy.source_table.clone()).filter(|s| !s.is_empty())
            }
            SubmitOutcome::ChainLimitExecute { source_table, .. } => {
                Some(source_table.clone()).filter(|s| !s.is_empty())
            }
            _ => None,
        },
        clone_sqls: None,
        input_tables: Vec::new(),
        query_dependencies: Vec::new(),
        // Only a Skip reports the runtime it echoes.
        execution_runtime_ms: match outcome {
            SubmitOutcome::Skip {
                matched_execution, ..
            } => matched_execution.execution_runtime_ms,
            _ => None,
        },
        // On a Skip, or a freshness-driven rebuild Execute whose fingerprint
        // still matched, the matched prior `executions` row is the Explain
        // freshness anchor (ADR 0005 / ticket #30). `None` for a first-build /
        // fingerprint-miss Execute and for a Copy.
        matched_execution_id: record.matched_execution_id,
        freshness_tolerance_ms: Some(record.tolerance_ms).filter(|t| *t > 0),
        build_age_ms: record.build_age_ms,
        skip_rejection_reason: record.skip_rejection_reason,
        clone_rejection_reason: record.clone_rejection_reason,
        unmatched_execution_id: record.unmatched_execution_id,
    }
}

/// Build + spawn capture for a SubmitEnrichedSQL decision.
fn capture_submit(
    state: &AppState,
    meta: &CaptureMeta,
    req: &qc::SubmitEnrichedSqlRequest,
    outcome: &SubmitOutcome,
) {
    let input = crate::capture::CaptureInput {
        table_namespace: req.table_namespace.clone(),
        target_table: req.target_table.clone(),
        default_schema: req.default_schema.clone(),
        // The decision-time inputs with their resolved producers (ADR 0007);
        // the submitted tables when the decision recorded none (a copy).
        input_tables: if outcome.record().input_tables.is_empty() {
            req.tables.iter().map(Into::into).collect()
        } else {
            outcome.record().input_tables.clone()
        },
        query_dependencies: req
            .query_dependencies
            .iter()
            .map(|d| d.name.clone())
            .collect(),
        ..submit_capture(
            meta,
            outcome,
            req.dbt_node_state.as_ref(),
            &req.labels,
            &req.dialect,
            &req.default_catalog,
            req.execution_type,
        )
    };
    spawn_capture(state, input);
}

/// Build + spawn capture for a SubmitValues (seed) decision.
fn capture_submit_values(
    state: &AppState,
    meta: &CaptureMeta,
    req: &qc::SubmitValuesRequest,
    outcome: &SubmitOutcome,
) {
    let input = crate::capture::CaptureInput {
        values_hash: Some(req.values_hash.clone()).filter(|s| !s.is_empty()),
        table_namespace: req.table_namespace.clone(),
        target_table: Some(req.target_table.clone()),
        ..submit_capture(
            meta,
            outcome,
            req.dbt_node_state.as_ref(),
            &req.labels,
            &req.dialect,
            &req.default_catalog,
            ModelExecutionType::Values as i32,
        )
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
        let now_ms = self.0.clock.now_ms(request.metadata());
        let req = request.into_inner();
        // Only data tests have no Target relation. Deciding any other node
        // without one could answer a Result copy into an empty relation name
        // (#81). Hosted fails `""` with InvalidArgument and an unset target
        // with Internal; both are InvalidArgument here.
        if req.execution_type != ModelExecutionType::DbtDataTest as i32
            && req.target_table.as_deref().is_none_or(str::is_empty)
        {
            return Err(Status::invalid_argument(
                "target_table is required for a non-test node",
            ));
        }

        let outcome = crate::submit_decision::submit_sql(&self.0.store, &org_id, &req, now_ms)
            .await
            .map_err(store_status)?;
        // Best-effort capture for the UI domain model (non-blocking).
        capture_submit(&self.0, &cap_meta, &req, &outcome);
        let response = submit_response(outcome, req.target_table.as_deref().unwrap_or_default());
        Ok(Response::new(response))
    }

    async fn submit_values(
        &self,
        request: Request<qc::SubmitValuesRequest>,
    ) -> Result<Response<qc::SubmitSqlResponse>, Status> {
        let org_id = org_id_checked(&self.0, &request)?;
        let cap_meta = capture_meta_of(&request);
        let now_ms = self.0.clock.now_ms(request.metadata());
        let req = request.into_inner();

        let outcome = crate::submit_decision::submit_values(&self.0.store, &org_id, &req, now_ms)
            .await
            .map_err(store_status)?;
        capture_submit_values(&self.0, &cap_meta, &req, &outcome);
        Ok(Response::new(submit_response(outcome, &req.target_table)))
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
        let now_ms = self.0.clock.now_ms(request.metadata());
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
                now_ms,
            )
            .await
            .map_err(store_status)?;

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

        let stored = crate::execution::record_batch(&self.0.store, &org_id, &req.records)
            .await
            .map_err(|e| match e {
                RecordError::MissingInput => {
                    Status::invalid_argument("ExecutionRecord missing input")
                }
                RecordError::Store(e) => store_status(e),
            })?;

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
            .map_err(store_status)?;

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
        let mut source_build = None;
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
                // The incremental contract has pinned the request's execution
                // and table types; the source's current state must match them.
                self.0
                    .store
                    .find_current_copy_source(
                        &org_id,
                        &CopySourceQuery {
                            source,
                            table_namespace,
                            dialect: &req.dialect,
                            execution_type: req.execution_type,
                            last_modified_epoch,
                            table_type: req.clone_source_table_type.as_deref().unwrap_or_default(),
                            node_hash: identity,
                        },
                    )
                    .await
                    .map_err(store_status)?,
            ),
            Ok(RegisterLookup::SourceLogic {
                source,
                table_namespace,
            }) => {
                source_build = self
                    .0
                    .store
                    .find_copy_source_build(&org_id, source, table_namespace, &req.dialect)
                    .await
                    .map_err(store_status)?;
                RegisterHistory::SourceLogic(
                    source_build
                        .as_ref()
                        .and_then(|s| s.node_namespace_logic_hash.clone()),
                )
            }
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
        let evidence = crate::execution::ExecutionEvidence::copy_registration(
            &org_id,
            &req,
            plan.inherited_ns_logic,
            source_build.as_ref(),
        );
        self.0
            .store
            .insert_execution(&evidence.pending(&request_id, &execution_decision_id))
            .await
            .map_err(store_status)?;

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
                    // A client-requested copy populates the target from an
                    // existing result: a Result copy.
                    decision_reason: DecisionReason::ResultCopy,
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
                    freshness_tolerance_ms: None,
                    build_age_ms: None,
                    skip_rejection_reason: None,
                    clone_rejection_reason: None,
                    unmatched_execution_id: None,
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
            .map_err(store_status)?;

        let mut by_id: std::collections::HashMap<String, crate::explain::ExplainNode> =
            std::collections::HashMap::new();
        for row in rows {
            let Some(edid) = row.execution_decision_id.clone() else {
                continue;
            };
            let Some(decision) = DecisionKind::from_stored(&row.decision) else {
                // Unknown decision class — skip rather than invent a tree.
                continue;
            };
            let node = crate::explain::ExplainNode {
                execution_decision_id: edid.clone(),
                decision,
                execution_type: row.execution_type,
                decision_description: row.decision_description.clone(),
                reason: row.decision_reason(),
                is_stale: row.is_stale,
                target_table: row.target_table.clone(),
                clone_source: row.clone_source.clone(),
                input_tables: row.input_tables(),
                tolerance_ms: row.freshness_tolerance_ms.unwrap_or(0),
                build_age_ms: row.build_age_ms,
                skip_rejection_reason: row.skip_rejection_reason,
                // The latest build in scope a rebuild without a matched build
                // did not have the identity of (migration 0017): the SHAPE 7
                // freshness listing is judged against it.
                unmatched: row.unmatched(),
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
        // CLIENT-UNREACHABLE" and docs/explain-catalog.md § "Not rendered".
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
            .map_err(store_status)?;

        let node_unique_ids = crate::selector::select_matching(criteria, &req.nodes, |uid| {
            recorded.get(uid).cloned()
        });

        Ok(Response::new(qc::SelectorResponse { node_unique_ids }))
    }
}
