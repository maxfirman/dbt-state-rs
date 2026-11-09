//! Submit decision: how one SQL or values (seed) submission is decided.
//!
//! Two entry points, [`submit_sql`] and [`submit_values`], each returning one
//! [`SubmitOutcome`]. For a submission the module:
//!
//! 1. records the node's comparison state for `state:*` selectors (best effort);
//! 2. selects a reuse candidate from State history (one scoped read, then the
//!    pure [`reuse_candidate`] selection);
//! 3. SQL only: tries the Result copy fallback, which is considered only when
//!    there is NO reuse candidate and, when it applies, overrides the verdict;
//! 4. otherwise builds the decision context and calls the pure
//!    [`decide`](crate::decision::decide);
//! 5. mints the request and execution decision ids and writes the pending row
//!    for every outcome the client will confirm.
//!
//! The gRPC adapter keeps org and metadata extraction, maps the outcome to the
//! wire (both clone oneofs) and captures it.

use crate::decision::{self, SubmitContext, Verdict};
use crate::execution::{self, ExecutionEvidence};
use crate::query_cache as qc;
use crate::result_copy::{self, SubmitCopyPlan};
use crate::reuse_candidate::{self, Selection};
use crate::store::{ExecutionRow, Store};

// ModelExecutionType values (proto/query_cache_protobuf/query_cache/shared.proto).
const EXECUTION_TYPE_DBT_DATA_TEST: i32 = 8;
const EXECUTION_TYPE_VALUES: i32 = 9;

/// The ids minted for an outcome the client confirms: `request_id` keys the
/// pending row's ConfirmExecution, `execution_decision_id` keys Explain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionIds {
    pub request_id: String,
    pub execution_decision_id: String,
}

impl DecisionIds {
    fn mint() -> Self {
        Self {
            request_id: new_id(),
            execution_decision_id: new_id(),
        }
    }
}

/// How a submission was decided. Every outcome but Skip wrote a pending row
/// keyed on `ids.request_id`.
#[derive(Debug, Clone)]
pub enum SubmitOutcome {
    /// Reuse the matched execution (`skip_execution`). No pending row.
    Skip {
        execution_decision_id: String,
        /// The reuse candidate served; its runtime and cached results are
        /// echoed, and it is the Explain freshness anchor.
        matched_execution: ExecutionRow,
        description: String,
        is_stale: bool,
    },
    /// Build (`ready_to_execute`), with an ordinary pending row.
    Execute {
        ids: DecisionIds,
        /// The reuse candidate when freshness forced the rebuild (the Explain
        /// freshness anchor); `None` when no reuse candidate qualified.
        matched_execution: Option<ExecutionRow>,
        description: String,
        skip_rejection_reason: i32,
        clone_rejection_reason: i32,
        is_stale: bool,
    },
    /// Copy an equivalent built under another name into the submit target
    /// (`ready_to_clone`). The pending row records the copy source.
    Copy {
        ids: DecisionIds,
        source_table: String,
        clone_sqls: Vec<String>,
        source_last_modified_epoch: Option<i64>,
        source_execution_runtime_ms: Option<i64>,
    },
    /// An equivalent exists but the target forbids copies or the copy chain is
    /// at its limit: build with the chain-limit shape and an ordinary pending
    /// row (see [`SubmitCopyPlan::ChainLimitExecute`]).
    ChainLimitExecute { ids: DecisionIds },
}

impl SubmitOutcome {
    pub fn execution_decision_id(&self) -> &str {
        match self {
            Self::Skip {
                execution_decision_id,
                ..
            } => execution_decision_id,
            Self::Execute { ids, .. }
            | Self::Copy { ids, .. }
            | Self::ChainLimitExecute { ids } => &ids.execution_decision_id,
        }
    }

    /// The prior execution this decision was made against, recorded as the
    /// Explain freshness anchor (ADR 0005). Copies match no prior execution.
    pub fn matched_execution(&self) -> Option<&ExecutionRow> {
        match self {
            Self::Skip {
                matched_execution, ..
            } => Some(matched_execution),
            Self::Execute {
                matched_execution, ..
            } => matched_execution.as_ref(),
            Self::Copy { .. } | Self::ChainLimitExecute { .. } => None,
        }
    }
}

/// Decide a `SubmitEnrichedSQL` submission (models, snapshots, data tests...).
pub async fn submit_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
) -> sqlx::Result<SubmitOutcome> {
    record_node_state(
        store,
        org_id,
        req.dbt_node_state.as_ref(),
        req.target_table.as_deref(),
    )
    .await;

    let evidence = ExecutionEvidence::sql_submit(org_id, req);
    let Selection {
        candidate,
        target_has_prior_build,
    } = select_sql(store, org_id, req, &evidence).await?;

    // Precedence: a Result copy is considered only when there is no reuse
    // candidate, and when it applies it overrides the ordinary verdict.
    if let Some(outcome) =
        result_copy_fallback(store, org_id, req, &evidence, candidate.is_some()).await?
    {
        return Ok(outcome);
    }

    let row = evidence.row();
    let ctx = SubmitContext {
        execution_type: req.execution_type,
        input_tables: &row.input_tables,
        target_table: req.target_table.as_deref(),
        stale_upstream_policy: decision::StaleUpstreamPolicy::from_i32(req.stale_upstream_policy),
        target_has_prior_build,
    };
    let verdict = decision::decide(&ctx, candidate.as_ref());
    tracing::info!(
        node = %req.labels.get("dbt_node_name").cloned().unwrap_or_default(),
        execution_type = req.execution_type,
        had_match = candidate.is_some(),
        body_hash = %row.node_body_hash.clone().unwrap_or_default(),
        verdict = verdict_label(&verdict),
        "decision"
    );
    settle(store, &evidence, verdict, candidate).await
}

/// Decide a `SubmitValues` (seed) submission. Seeds have no Result copy
/// fallback and always use the ANY stale-upstream policy.
pub async fn submit_values(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitValuesRequest,
) -> sqlx::Result<SubmitOutcome> {
    record_node_state(
        store,
        org_id,
        req.dbt_node_state.as_ref(),
        Some(req.target_table.as_str()),
    )
    .await;

    let evidence = ExecutionEvidence::values_submit(org_id, req);
    let current = store
        .current_target_state(
            org_id,
            &req.target_table,
            execution::namespace(req.table_namespace.as_deref()),
            &req.dialect,
        )
        .await?;
    let Selection {
        candidate,
        target_has_prior_build,
    } = reuse_candidate::for_seed(req, evidence.row(), current)?;

    let row = evidence.row();
    let ctx = SubmitContext {
        execution_type: EXECUTION_TYPE_VALUES,
        input_tables: &row.input_tables,
        target_table: Some(req.target_table.as_str()),
        stale_upstream_policy: decision::StaleUpstreamPolicy::Any,
        target_has_prior_build,
    };
    let verdict = decision::decide(&ctx, candidate.as_ref());
    tracing::info!(
        node = %req.labels.get("dbt_node_name").cloned().unwrap_or_default(),
        execution_type = EXECUTION_TYPE_VALUES,
        had_match = candidate.is_some(),
        values_hash = %row.values_hash.clone().unwrap_or_default(),
        verdict = verdict_label(&verdict),
        "decision(values)"
    );
    settle(store, &evidence, verdict, candidate).await
}

/// State history selection for a SQL submission. Models read the target's
/// current physical state; data tests read their latest outcome by scoped UID.
async fn select_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
) -> sqlx::Result<Selection> {
    let row = evidence.row();
    let table_namespace = execution::namespace(req.table_namespace.as_deref());
    if req.execution_type == EXECUTION_TYPE_DBT_DATA_TEST {
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
        reuse_candidate::for_data_test(req, row, latest)
    } else {
        let current = store
            .current_target_state(
                org_id,
                req.target_table.as_deref().unwrap_or_default(),
                table_namespace,
                &req.dialect,
            )
            .await?;
        reuse_candidate::for_model(req, row, current)
    }
}

/// The Result copy fallback (see [`result_copy`]): an equivalent model already
/// built under another name is copied in. `None` means the ordinary decision
/// applies: no lookup was warranted, no equivalent exists, or no copy can be
/// produced for it.
async fn result_copy_fallback(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    has_reuse_candidate: bool,
) -> sqlx::Result<Option<SubmitOutcome>> {
    let Some(lookup) = result_copy::submit_lookup(req, evidence.row(), has_reuse_candidate) else {
        return Ok(None);
    };
    let Some(equivalent) = store
        .find_clone_equivalent(
            org_id,
            lookup.table_namespace,
            lookup.node_namespace_logic_hash,
            lookup.execution_type,
            lookup.exclude_target,
            &req.dialect,
        )
        .await?
    else {
        return Ok(None);
    };
    match result_copy::plan_submit(req, equivalent) {
        SubmitCopyPlan::ChainLimitExecute => {
            let ids = DecisionIds::mint();
            // An ordinary (non-copy) pending row: the forthcoming
            // ConfirmExecution records the normal build.
            write_pending(store, evidence, &ids, None).await?;
            Ok(Some(SubmitOutcome::ChainLimitExecute { ids }))
        }
        SubmitCopyPlan::Copy {
            source_table,
            clone_sqls,
            source_last_modified_epoch,
            source_execution_runtime_ms,
        } => {
            let ids = DecisionIds::mint();
            // Keyed on the submit target and inheriting the equivalent's logic
            // identity, so later submits reuse the copy once confirmed.
            write_pending(store, evidence, &ids, Some(&source_table)).await?;
            Ok(Some(SubmitOutcome::Copy {
                ids,
                source_table,
                clone_sqls,
                source_last_modified_epoch,
                source_execution_runtime_ms,
            }))
        }
        SubmitCopyPlan::Unavailable { reason } => {
            tracing::warn!(
                target = %req.target_table.as_deref().unwrap_or_default(),
                reason,
                "equivalent found but no result copy is available; using the ordinary decision"
            );
            Ok(None)
        }
    }
}

/// The shared tail of both entry points: mint ids, write the pending row for
/// an Execute, and carry the reuse candidate as the matched execution.
async fn settle(
    store: &Store,
    evidence: &ExecutionEvidence,
    verdict: Verdict,
    candidate: Option<ExecutionRow>,
) -> sqlx::Result<SubmitOutcome> {
    match verdict {
        Verdict::Skip {
            description,
            is_stale,
        } => Ok(SubmitOutcome::Skip {
            execution_decision_id: new_id(),
            matched_execution: candidate.expect(
                "decide skips only with a reuse candidate (decision_proptest::no_match_always_executes)",
            ),
            description,
            is_stale,
        }),
        Verdict::Execute {
            description,
            skip_rejection_reason,
            clone_rejection_reason,
            is_stale,
        } => {
            let ids = DecisionIds::mint();
            // The forthcoming ConfirmExecution finalizes this identity into
            // reusable State history.
            write_pending(store, evidence, &ids, None).await?;
            Ok(SubmitOutcome::Execute {
                ids,
                matched_execution: candidate,
                description,
                skip_rejection_reason,
                clone_rejection_reason,
                is_stale,
            })
        }
    }
}

async fn write_pending(
    store: &Store,
    evidence: &ExecutionEvidence,
    ids: &DecisionIds,
    copy_source: Option<&str>,
) -> sqlx::Result<()> {
    evidence
        .write_pending(
            store,
            &ids.request_id,
            &ids.execution_decision_id,
            copy_source,
        )
        .await
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
    store: &Store,
    org_id: &str,
    node_state: Option<&qc::DbtNodeState>,
    target_table: Option<&str>,
) {
    let Some(upsert) = execution::node_state_upsert(org_id, node_state, target_table) else {
        return;
    };
    if let Err(e) = store.upsert_node_state(&upsert).await {
        // Recording selector state must never fail the decision path. Count the
        // failure so it is observable (metric/alert) rather than silently lost:
        // the execution ledger may advance while `state:*` selectors read a
        // stale baseline, and that divergence must be measurable in deployment.
        store.count_selector_write_failure();
        tracing::warn!(error = %e, "failed to record node state for selectors");
    }
}

fn verdict_label(verdict: &Verdict) -> &'static str {
    match verdict {
        Verdict::Skip { .. } => "SKIP",
        Verdict::Execute { .. } => "EXECUTE",
    }
}

fn new_id() -> String {
    uuid::Uuid::now_v7().to_string()
}
