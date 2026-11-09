//! Prepare trustworthy State history for same-target reuse across warehouses.
//!
//! Fetch the latest execution in the submit's scope (one read) and hand it to
//! the pure `reuse_candidate` selection. The caller retains selector writes,
//! the pure decision, copy selection and outcome recording.

use crate::execution::ExecutionEvidence;
use crate::query_cache as qc;
use crate::reuse_candidate::{self, Selection};
use crate::store::{ExecutionRow, Store};

// ModelExecutionType values (proto/query_cache_protobuf/query_cache/shared.proto).
const EXECUTION_TYPE_DBT_DATA_TEST: i32 = 8;

pub(crate) struct SqlHistory {
    /// The reuse candidate, if any (see `reuse_candidate`).
    pub candidate: Option<ExecutionRow>,
    pub target_has_prior_build: bool,
    pub evidence: ExecutionEvidence,
}

pub(crate) struct ValuesHistory {
    /// The reuse candidate, if any (see `reuse_candidate`).
    pub candidate: Option<ExecutionRow>,
    pub target_has_prior_build: bool,
    pub evidence: ExecutionEvidence,
}

/// Prepare model or data-test history. Models read the target's current
/// physical state; data tests read their latest outcome by scoped UID.
pub(crate) async fn prepare_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
) -> sqlx::Result<SqlHistory> {
    let evidence = ExecutionEvidence::sql_submit(org_id, req);
    let row = evidence.row();
    let table_namespace = namespace(req.table_namespace.as_deref());
    let Selection {
        candidate,
        target_has_prior_build,
    } = if req.execution_type == EXECUTION_TYPE_DBT_DATA_TEST {
        let latest = match row.node_unique_id.as_deref() {
            Some(uid) => {
                store
                    .latest_data_test(
                        org_id,
                        uid,
                        row.project_id.as_deref(),
                        table_namespace,
                        &req.dialect,
                    )
                    .await?
            }
            None => None,
        };
        reuse_candidate::for_data_test(req, row, latest)?
    } else {
        let current = store
            .current_target_state(
                org_id,
                req.target_table.as_deref().unwrap_or_default(),
                table_namespace,
                &req.dialect,
            )
            .await?;
        reuse_candidate::for_model(req, row, current)?
    };
    Ok(SqlHistory {
        candidate,
        target_has_prior_build,
        evidence,
    })
}

/// Seeds read the target's current physical state like models, but compare
/// values/config identity and target evidence (see `reuse_candidate::for_seed`).
pub(crate) async fn prepare_values(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitValuesRequest,
) -> sqlx::Result<ValuesHistory> {
    let evidence = ExecutionEvidence::values_submit(org_id, req);
    let current = store
        .current_target_state(
            org_id,
            &req.target_table,
            namespace(req.table_namespace.as_deref()),
            &req.dialect,
        )
        .await?;
    let Selection {
        candidate,
        target_has_prior_build,
    } = reuse_candidate::for_seed(req, evidence.row(), current)?;
    Ok(ValuesHistory {
        candidate,
        target_has_prior_build,
        evidence,
    })
}

pub(crate) fn namespace(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}
