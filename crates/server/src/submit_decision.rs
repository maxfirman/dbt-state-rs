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

use crate::decision::{self, DecisionKind, DecisionRecord, SubmitContext};
use crate::execution::{self, ExecutionEvidence};
use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
pub use crate::result_copy::ResultCopy;
use crate::result_copy::{self, CloneEquivalent, EquivalentLookup, SubmitCopyPlan};
use crate::reuse_candidate::{self, Selection};
use crate::store::{CopyCandidateQuery, ExecutionRow, Producer, Store};
use crate::warehouse::Warehouse;
use std::collections::HashMap;

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

/// How a submission was decided. Every outcome carries its
/// [`DecisionRecord`] (class, decision reason, hosted description and
/// rejection codes, `is_stale`, matched execution id). Every outcome but Skip
/// wrote a pending row keyed on `ids.request_id`.
#[derive(Debug, Clone)]
pub enum SubmitOutcome {
    /// Reuse the matched execution (`skip_execution`). No pending row.
    Skip {
        execution_decision_id: String,
        /// The reuse candidate served; its runtime and cached results are
        /// echoed, and it is the Explain freshness anchor.
        matched_execution: ExecutionRow,
        record: DecisionRecord,
    },
    /// Build (`ready_to_execute`), with an ordinary pending row.
    Execute {
        ids: DecisionIds,
        /// The reuse candidate when freshness forced the rebuild (the Explain
        /// freshness anchor); `None` when no reuse candidate qualified.
        matched_execution: Option<ExecutionRow>,
        record: DecisionRecord,
    },
    /// Copy an equivalent built under another name into the submit target
    /// (`ready_to_clone`). The pending row records the copy source.
    Copy {
        ids: DecisionIds,
        copy: ResultCopy,
        record: DecisionRecord,
    },
    /// An equivalent exists but the target forbids copies or the copy chain is
    /// at its limit: build with the chain-limit shape and an ordinary pending
    /// row (see [`SubmitCopyPlan::ChainLimitExecute`]).
    ChainLimitExecute {
        ids: DecisionIds,
        record: DecisionRecord,
    },
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
            | Self::ChainLimitExecute { ids, .. } => &ids.execution_decision_id,
        }
    }

    /// The ids minted for an outcome the client confirms; `None` for a Skip.
    pub fn ids(&self) -> Option<&DecisionIds> {
        match self {
            Self::Skip { .. } => None,
            Self::Execute { ids, .. }
            | Self::Copy { ids, .. }
            | Self::ChainLimitExecute { ids, .. } => Some(ids),
        }
    }

    pub fn record(&self) -> &DecisionRecord {
        match self {
            Self::Skip { record, .. }
            | Self::Execute { record, .. }
            | Self::Copy { record, .. }
            | Self::ChainLimitExecute { record, .. } => record,
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
/// `now_ms` is the server clock (Unix ms) the build age is measured against.
pub async fn submit_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    now_ms: i64,
) -> sqlx::Result<SubmitOutcome> {
    record_node_state(
        store,
        org_id,
        req.dbt_node_state.as_ref(),
        req.target_table.as_deref(),
    )
    .await;

    let mut evidence = ExecutionEvidence::sql_submit(org_id, req);
    resolve_producers(store, &mut evidence).await?;
    let selection = select_sql(store, org_id, req, &evidence).await?;

    // Precedence: a Result copy is considered only when there is no reuse
    // candidate, and when it applies it overrides the ordinary verdict.
    let has_reuse_candidate = selection.candidate.is_some();
    if let Some(outcome) =
        result_copy_fallback(store, org_id, req, &evidence, has_reuse_candidate).await?
    {
        return Ok(outcome);
    }

    decide_and_settle(
        store,
        &evidence,
        selection,
        Freshness {
            target_table: req.target_table.as_deref(),
            stale_upstream_policy: decision::StaleUpstreamPolicy::from_i32(
                req.stale_upstream_policy,
            ),
            tolerance_ms: req.freshness_tolerance_seconds.max(0).saturating_mul(1000),
            now_ms,
        },
        &req.labels,
    )
    .await
}

/// Record, on each input the evidence read, which build in State history
/// produced it (ADR 0007): the input relation's current physical state in the
/// same organization, warehouse family and connection namespace, when the read
/// is not later than that build. The node's own target is not an upstream.
pub(crate) async fn resolve_producers(
    store: &Store,
    evidence: &mut ExecutionEvidence,
) -> sqlx::Result<()> {
    let names: Vec<String> = evidence
        .input_tables
        .iter()
        .filter(|t| t.name != evidence.target_table)
        .map(|t| t.name.clone())
        .collect();
    let builds = store
        .current_target_states(
            &evidence.org_id,
            &names,
            execution::non_blank_namespace(evidence.table_namespace.as_deref()),
            &evidence.dialect,
        )
        .await?;
    let target = evidence.target_table.clone();
    for input in evidence
        .input_tables
        .iter_mut()
        .filter(|t| t.name != target)
    {
        input.producer = Producer::of(builds.get(&input.name), input.last_modified_epoch);
    }
    Ok(())
}

/// The freshness parameters of one submission, distilled at the boundary.
struct Freshness<'a> {
    target_table: Option<&'a str>,
    stale_upstream_policy: decision::StaleUpstreamPolicy,
    tolerance_ms: i64,
    now_ms: i64,
}

/// Decide a `SubmitValues` (seed) submission. Seeds have no Result copy
/// fallback, no Lag tolerance and always use the ANY stale-upstream policy.
pub async fn submit_values(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitValuesRequest,
    now_ms: i64,
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
            execution::non_blank_namespace(req.table_namespace.as_deref()),
            &req.dialect,
        )
        .await?;
    let selection = reuse_candidate::for_seed(req, &evidence, current)?;

    decide_and_settle(
        store,
        &evidence,
        selection,
        Freshness {
            target_table: Some(req.target_table.as_str()),
            stale_upstream_policy: decision::StaleUpstreamPolicy::Any,
            tolerance_ms: 0,
            now_ms,
        },
        &req.labels,
    )
    .await
}

/// State history selection for a SQL submission. Models read the target's
/// current physical state; data tests read their latest outcome by scoped UID.
async fn select_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
) -> sqlx::Result<Selection> {
    let table_namespace = execution::non_blank_namespace(req.table_namespace.as_deref());
    if req.execution_type == ModelExecutionType::DbtDataTest as i32 {
        let latest = match evidence.node_unique_id.as_deref() {
            Some(uid) => {
                store
                    .latest_data_test(
                        org_id,
                        uid,
                        evidence.project_id.as_deref(),
                        table_namespace,
                        &req.dialect,
                    )
                    .await?
            }
            None => None,
        };
        reuse_candidate::for_data_test(req, evidence, latest)
    } else {
        let current = store
            .current_target_state(
                org_id,
                req.target_table.as_deref().unwrap_or_default(),
                table_namespace,
                &req.dialect,
            )
            .await?;
        reuse_candidate::for_model(req, evidence, current)
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
    let Some(lookup) = result_copy::submit_lookup(req, evidence, has_reuse_candidate) else {
        return Ok(None);
    };
    let Some(equivalent) = find_equivalent(store, org_id, req, &lookup).await? else {
        return Ok(None);
    };
    match result_copy::plan_submit(req, equivalent) {
        SubmitCopyPlan::ChainLimitExecute => {
            let ids = DecisionIds::mint();
            // An ordinary (non-copy) pending row: the forthcoming
            // ConfirmExecution records the normal build.
            write_pending(store, evidence, &ids).await?;
            Ok(Some(SubmitOutcome::ChainLimitExecute {
                ids,
                record: DecisionRecord::copy_chain_limit(),
            }))
        }
        SubmitCopyPlan::Copy(copy) => {
            let ids = DecisionIds::mint();
            // Keyed on the submit target and inheriting the equivalent's logic
            // identity, so later submits reuse the copy once confirmed.
            write_pending(store, &evidence.copied_from(&copy.source_table), &ids).await?;
            Ok(Some(SubmitOutcome::Copy {
                ids,
                copy,
                record: DecisionRecord::result_copy(),
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

/// The clone-equivalent lookup: the first current equivalent, in the store's
/// preference order, that the warehouse rules accept as a copy source for the
/// submit target. Where the rules track copy depth, each candidate's copy
/// provenance is walked and a candidate with too long or missing a chain is
/// skipped.
async fn find_equivalent(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    lookup: &EquivalentLookup<'_>,
) -> sqlx::Result<Option<CloneEquivalent>> {
    let warehouse = Warehouse::from_dialect(&req.dialect);
    let candidates = store
        .find_copy_candidates(
            org_id,
            &CopyCandidateQuery {
                table_namespace: lookup.table_namespace,
                node_namespace_logic_hash: lookup.node_namespace_logic_hash,
                execution_type: lookup.execution_type,
                exclude_target: lookup.exclude_target,
                dialect: &req.dialect,
                limit: warehouse.copy_candidate_limit(),
            },
        )
        .await?;
    for candidate in candidates {
        if !warehouse.copy_relations_compatible(&candidate.target_table, lookup.exclude_target) {
            continue;
        }
        let copy_depth = match warehouse.copy_provenance_depth() {
            None => 0,
            Some(max_ancestors) => {
                let depth = store
                    .copy_chain_depth(
                        org_id,
                        lookup.table_namespace,
                        &req.dialect,
                        &candidate.target_table,
                        max_ancestors,
                    )
                    .await?;
                let Some(depth) = depth else {
                    continue;
                };
                depth
            }
        };
        return Ok(Some(CloneEquivalent::new(candidate, copy_depth)));
    }
    Ok(None)
}

/// The step both entry points share once State history is selected (and, for
/// SQL, no Result copy applied): build the decision context from the
/// evidence, decide, log the decision and settle it.
async fn decide_and_settle(
    store: &Store,
    evidence: &ExecutionEvidence,
    selection: Selection,
    freshness: Freshness<'_>,
    labels: &HashMap<String, String>,
) -> sqlx::Result<SubmitOutcome> {
    let Selection {
        candidate,
        target_has_prior_build,
        why_no_candidate,
    } = selection;
    // Build age on the server clock (ADR 0007): the build's ConfirmExecution
    // epoch where epochs are wall-clock milliseconds, the server's receipt of
    // the confirmation where they are not (DuckDB ordinals).
    let build_age_ms = candidate.as_ref().and_then(|c| {
        Warehouse::from_dialect(&evidence.dialect)
            .build_time_ms(c)
            .map(|built| freshness.now_ms.saturating_sub(built))
    });
    let ctx = SubmitContext {
        execution_type: evidence.execution_type,
        input_tables: &evidence.input_tables,
        target_table: freshness.target_table,
        stale_upstream_policy: freshness.stale_upstream_policy,
        target_has_prior_build,
        why_no_candidate,
        tolerance_ms: freshness.tolerance_ms,
        build_age_ms,
    };
    let record = DecisionRecord::decided(&ctx, candidate.as_ref());
    tracing::info!(
        node = %labels.get("dbt_node_name").map(String::as_str).unwrap_or_default(),
        execution_type = evidence.execution_type,
        had_match = candidate.is_some(),
        body_hash = %evidence.node_body_hash.as_deref().unwrap_or_default(),
        values_hash = %evidence.values_hash.as_deref().unwrap_or_default(),
        verdict = verdict_label(&record),
        reason = ?record.reason,
        "decision"
    );
    settle(store, evidence, record, candidate).await
}

/// The shared tail of both entry points: mint ids, write the pending row for
/// an Execute, and carry the reuse candidate as the matched execution.
async fn settle(
    store: &Store,
    evidence: &ExecutionEvidence,
    record: DecisionRecord,
    candidate: Option<ExecutionRow>,
) -> sqlx::Result<SubmitOutcome> {
    match record.class {
        DecisionKind::Skip => Ok(SubmitOutcome::Skip {
            execution_decision_id: new_id(),
            matched_execution: candidate.expect(
                "decide skips only with a reuse candidate (decision_proptest::no_match_always_executes)",
            ),
            record,
        }),
        // `DecisionRecord::decided` is a Skip or a Build, never a Clone.
        DecisionKind::Build | DecisionKind::Clone => {
            let ids = DecisionIds::mint();
            // The forthcoming ConfirmExecution finalizes this identity into
            // reusable State history.
            write_pending(store, evidence, &ids).await?;
            Ok(SubmitOutcome::Execute {
                ids,
                matched_execution: candidate,
                record,
            })
        }
    }
}

async fn write_pending(
    store: &Store,
    evidence: &ExecutionEvidence,
    ids: &DecisionIds,
) -> sqlx::Result<()> {
    store
        .insert_execution(&evidence.pending(&ids.request_id, &ids.execution_decision_id))
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

fn verdict_label(record: &DecisionRecord) -> &'static str {
    match record.class {
        DecisionKind::Skip => "SKIP",
        DecisionKind::Build | DecisionKind::Clone => "EXECUTE",
    }
}

fn new_id() -> String {
    uuid::Uuid::now_v7().to_string()
}
