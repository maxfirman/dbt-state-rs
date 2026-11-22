//! RegisterClone: how one client-requested Result copy is decided and
//! recorded, parallel to [`crate::submit_decision`] for submits.
//!
//! [`register`] refuses what it can before any read
//! ([`result_copy::register_lookup`]), reads the State history that lookup
//! names, plans the copy ([`result_copy::plan_register`]) and writes the
//! pending copy row the client's ConfirmExecution finalizes. The gRPC adapter
//! maps the outcome to the wire and captures it.

use crate::execution::ExecutionEvidence;
use crate::query_cache as qc;
pub(crate) use crate::result_copy::RegisterRejection;
use crate::result_copy::{self, RegisterHistory, RegisterLookup, RegisterRefusal};
use crate::store::{CopySourceQuery, Store};
use crate::submit_decision::DecisionIds;
use dbt_state_auth::PrincipalId;

/// How a RegisterClone request was decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RegisterOutcome {
    /// `ready_to_clone`: a pending copy row keyed on `ids.request_id` was
    /// written for the clone target.
    Registered {
        ids: DecisionIds,
        clone_sqls: Vec<String>,
        /// The source epoch the client must still observe (DuckDB only).
        required_source_epoch: Option<i64>,
    },
    /// `unable_to_clone`: nothing was written; the client builds normally.
    Refused(RegisterRejection),
}

/// Why a RegisterClone request failed.
#[derive(Debug)]
pub(crate) enum RegisterError {
    /// No DDL exists for this copy (`Status::unimplemented`).
    Unsupported(&'static str),
    Store(sqlx::Error),
}

impl From<sqlx::Error> for RegisterError {
    fn from(error: sqlx::Error) -> Self {
        Self::Store(error)
    }
}

/// Decide and record one client-requested copy; its pending row names
/// `principal`.
pub(crate) async fn register(
    store: &Store,
    org_id: &str,
    principal: Option<&PrincipalId>,
    req: &qc::CloneRequest,
) -> Result<RegisterOutcome, RegisterError> {
    let lookup = match result_copy::register_lookup(req) {
        Ok(lookup) => lookup,
        Err(refusal) => {
            if refusal == RegisterRefusal::Reject(RegisterRejection::ChainLimitExceeded) {
                tracing::info!(
                    source = %req.clone_source_table,
                    target = %req.target_table,
                    "clone rejected: clone_chain_depth_limit=0 (allow_clones=false)"
                );
            }
            return refused(refusal);
        }
    };
    let history = read_history(store, org_id, req, lookup).await?;
    let plan = match result_copy::plan_register(req, history) {
        Ok(plan) => plan,
        Err(refusal) => return refused(refusal),
    };
    let ids = DecisionIds::mint();
    // Persist a pending row so the subsequent ConfirmExecution finalizes the
    // clone into skippable history, keyed on the clone target.
    let mut evidence = ExecutionEvidence::copy_registration(
        org_id,
        req,
        plan.inherited_ns_logic,
        plan.source_build.as_ref(),
        plan.direct,
    );
    evidence.principal = principal.cloned();
    store
        .insert_execution(&evidence.pending(&ids.request_id, &ids.execution_decision_id))
        .await?;
    tracing::info!(
        node = %req.labels.get("dbt_node_name").cloned().unwrap_or_default(),
        source = %req.clone_source_table,
        target = %req.target_table,
        "clone decision: READY_TO_CLONE"
    );
    Ok(RegisterOutcome::Registered {
        ids,
        clone_sqls: plan.clone_sqls,
        required_source_epoch: plan.required_source_epoch,
    })
}

fn refused(refusal: RegisterRefusal) -> Result<RegisterOutcome, RegisterError> {
    match refusal {
        RegisterRefusal::Reject(rejection) => Ok(RegisterOutcome::Refused(rejection)),
        RegisterRefusal::Unsupported(reason) => Err(RegisterError::Unsupported(reason)),
    }
}

/// The State history `lookup` names, as the matching [`RegisterHistory`].
async fn read_history(
    store: &Store,
    org_id: &str,
    req: &qc::CloneRequest,
    lookup: RegisterLookup<'_>,
) -> sqlx::Result<RegisterHistory> {
    Ok(match lookup {
        RegisterLookup::DuckDbIncrementalSource {
            source,
            table_namespace,
            last_modified_epoch,
            identity,
        } => RegisterHistory::DuckDbIncrementalSource(
            // The incremental contract has pinned the request's execution
            // and table types; the source's current state must match them.
            store
                .find_current_copy_source(
                    org_id,
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
                .await?,
        ),
        RegisterLookup::DatabricksSource {
            source,
            table_namespace,
        } => RegisterHistory::DatabricksSource(
            store
                .current_direct_copy_source(org_id, source, table_namespace, &req.dialect)
                .await?
                .map(Box::new),
        ),
        RegisterLookup::SourceLogic {
            source,
            table_namespace,
        } => RegisterHistory::SourceLogic(
            store
                .find_copy_source_build(org_id, source, table_namespace, &req.dialect)
                .await?,
        ),
        RegisterLookup::Nothing => RegisterHistory::Nothing,
    })
}
