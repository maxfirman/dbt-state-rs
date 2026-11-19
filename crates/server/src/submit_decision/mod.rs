//! Submit decision: how one SQL or values (seed) submission is decided.
//!
//! Two entry points, [`submit_sql`] and [`submit_values`], each returning one
//! [`SubmitOutcome`], and a third, [`speculative_sql`], which decides the
//! speculative submits it can through the same code as [`submit_sql`]. For a
//! submission the module:
//!
//! 1. records the node's comparison state for `state:*` selectors (best effort);
//! 2. selects a reuse candidate from State history (one scoped read, then the
//!    pure [`reuse_candidate`] selection);
//! 3. tries the Result copy fallback, which is considered only when there is
//!    NO reuse candidate and, when it applies, overrides the verdict (a SQL
//!    submit whose execution type is not copied keeps the verdict and reports
//!    the equivalent in its rejection codes; a seed is copied on Snowflake
//!    only, #91);
//! 4. otherwise builds the decision context and calls the pure
//!    [`decide`](crate::decision::decide);
//! 5. decorates an ordinary SQL rebuild with the refusal diagnostics hosted
//!    reports ([`diagnostics`]);
//! 6. mints the request and execution decision ids and writes the pending row
//!    for every outcome the client will confirm.
//!
//! The rules behind the reads are pure internal seams, unit-tested without a
//! store: which equivalent the copy fallback copies ([`copy_search`]) and
//! which diagnostic asks State history what ([`diagnostics`]). The functions
//! here sequence the reads and writes.
//!
//! The gRPC adapter keeps org and metadata extraction, maps the outcome to the
//! wire (both clone oneofs) and captures it.

mod copy_search;
mod diagnostics;

use crate::decision::{self, DecisionKind, DecisionRecord, SubmitContext};
use crate::execution::{self, ExecutionEvidence};
use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
pub use crate::result_copy::ResultCopy;
use crate::result_copy::{self, CloneEquivalent, EquivalentLookup, SeedCopyTarget, SubmitCopyPlan};
use crate::reuse_candidate::{self, Selection};
use crate::store::{
    CopyCandidate, CopyCandidateQuery, ExecutionRow, Producer, SeedCopyCandidateQuery, Store,
};
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
    pub(crate) fn mint() -> Self {
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
        /// Frozen name for a refused copy's Explain only. Never a candidate.
        refused_copy_source: Option<String>,
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
    /// at its limit: build with the chain-limit or copy-disabled shape and an
    /// ordinary pending row (see [`SubmitCopyPlan::ChainLimitExecute`],
    /// [`SubmitCopyPlan::CopiesDisabled`]). Also a seed load whose copy the
    /// submit disallowed, with the seed's own description
    /// ([`DecisionRecord::with_seed_copy_refused`]).
    ChainLimitExecute {
        ids: DecisionIds,
        record: DecisionRecord,
        /// The equivalent that could have been copied (Explain names it).
        source_table: String,
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

/// What a SQL submit's capture persists for `dbt state explain` (ADR 0005),
/// in the decision's own resolved naming: Explain re-derives its tree from
/// exactly the evidence the decision used.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainEvidence {
    /// Submit instant for Explain display only; never freshness evidence.
    pub display_submitted_at_ms: Option<i64>,
    pub template_comparison: Option<crate::explain::TemplateComparison>,
    pub table_namespace: Option<String>,
    pub target_table: Option<String>,
    pub default_schema: Option<String>,
    /// The decision-time inputs with their resolved producers (ADR 0007);
    /// the submitted inputs when the decision recorded none (a copy), less a
    /// store_failures test's own audit relation, as the decision reads them
    /// (#88, #123).
    pub input_tables: Vec<crate::store::InputTable>,
    pub query_dependencies: Vec<String>,
    /// Whether a view's SQL reads a relation (#117): client 2.0.11 reports
    /// no upstreams for a view, so Explain words its freshness from it.
    pub reads_relation: Option<bool>,
}

impl ExplainEvidence {
    /// `req` is the resolved submit `evidence` was decided on.
    fn of(
        req: &qc::SubmitEnrichedSqlRequest,
        evidence: &ExecutionEvidence,
        outcome: &SubmitOutcome,
    ) -> Self {
        let warehouse = Warehouse::from_dialect(&req.dialect);
        let record = outcome.record();
        let input_tables = if record.reason
            == decision::DecisionReason::NoReuseCandidate(
                decision::NoCandidate::ResolutionContextChanged,
            ) {
            // These are resolved SQL bindings, not guessed freshness metadata.
            // A context-only rebuild explains the changed binding as ignored;
            // its pending execution still retains the original request evidence.
            req.tables
                .iter()
                .filter(|t| Some(&t.name) == req.target_table.as_ref())
                .map(|t| crate::store::InputTable::at(&t.name, t.last_modified_epoch))
                .chain(
                    warehouse
                        .resolved_input(req)
                        .map(|name| crate::store::InputTable::at(&name, None)),
                )
                .collect()
        } else if record.input_tables.is_empty() {
            execution::sql_inputs(&execution::sql_execution(req))
        } else {
            record.input_tables.clone()
        };
        Self {
            display_submitted_at_ms: evidence.display_submitted_at_ms,
            template_comparison: warehouse
                .template_comparison(req, evidence.node_sql_hash.as_deref().unwrap_or_default()),
            table_namespace: req.table_namespace.clone(),
            target_table: req.target_table.clone(),
            default_schema: req.default_schema.clone(),
            input_tables,
            query_dependencies: req
                .query_dependencies
                .iter()
                .map(|d| d.name.clone())
                .collect(),
            reads_relation: (req.execution_type == ModelExecutionType::View as i32)
                .then_some(evidence.sql_reads_relation)
                .flatten(),
        }
    }
}

/// A decided SQL submit with the Explain evidence its capture persists.
#[derive(Debug, Clone)]
pub struct ExplainedOutcome {
    pub outcome: SubmitOutcome,
    pub explain: ExplainEvidence,
    /// The values Volatile evaluation rendered for this decision, returned in
    /// `transformed_nodes_by_query` and persisted for Explain only (ADR 0005,
    /// "Amended by #67"). Rendered after the decision is made, so they never
    /// enter identity, fingerprints, Upstream producers or Reuse
    /// qualification. `None` when nothing was rendered.
    pub rendered: Option<crate::volatile_evaluation::Rendered>,
}

/// Decide a `SubmitEnrichedSQL` submission (models, snapshots, data tests...).
/// `now_ms` is the server clock (Unix ms) the build age is measured against.
pub async fn submit_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    now_ms: i64,
) -> sqlx::Result<SubmitOutcome> {
    let sources = crate::volatile_evaluation::Sources::system(now_ms.saturating_mul(1000));
    Ok(submit_sql_explained(store, org_id, req, &sources)
        .await?
        .outcome)
}

/// [`submit_sql`], with the decision's [`ExplainEvidence`] and the values
/// its Volatile evaluation rendered from `sources`. The build age is
/// measured on the same clock (`sources.now_micros`).
pub async fn submit_sql_explained(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    sources: &crate::volatile_evaluation::Sources,
) -> sqlx::Result<ExplainedOutcome> {
    record_node_state(
        store,
        org_id,
        req.dbt_node_state.as_ref(),
        req.target_table.as_deref(),
    )
    .await;
    let now_ms = sources.now_micros.div_euclid(1000);
    let mut decided = decide_sql(store, org_id, req, now_ms, Mode::Ordinary)
        .await?
        .expect("an ordinary submit is always decided");
    // Volatile evaluation, once decided: a build, a reuse or a Result copy,
    // in the cells the Warehouse was captured rendering in. The chain-limit
    // build has no capture.
    let decision = match &decided.outcome {
        SubmitOutcome::Execute { record, .. } => {
            Some(crate::volatile_evaluation::Decided::Build(record.reason))
        }
        SubmitOutcome::Skip { .. } => Some(crate::volatile_evaluation::Decided::Reuse),
        SubmitOutcome::Copy { .. } => {
            let explain = &decided.explain;
            Some(crate::volatile_evaluation::Decided::Copy {
                target_exists: explain
                    .target_table
                    .as_ref()
                    .is_some_and(|target| explain.input_tables.iter().any(|t| &t.name == target)),
            })
        }
        SubmitOutcome::ChainLimitExecute { .. } => None,
    };
    decided.rendered = decision.and_then(|outcome| {
        crate::volatile_evaluation::evaluate(
            req,
            Warehouse::from_dialect(&req.dialect).volatile_rules(),
            outcome,
            sources,
        )
    });
    Ok(decided)
}

/// Decide a `SubmitEnrichedSQLSpeculative` submission (#103). A speculative
/// request carries whatever upstream epochs the client had already fetched,
/// so only a decision that needs no table evidence at all is made on it: a
/// data test with no Target relation and no reported tables, which the
/// ordinary submit would SKIP. That Skip is returned exactly as the ordinary
/// submit would return it, and its node state is recorded as the ordinary
/// submit records it, because the client sends no ordinary submit after it.
/// Every other submission returns `None` (`undecided`) without writing
/// anything; the client then sends the ordinary submit.
pub async fn speculative_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    now_ms: i64,
) -> sqlx::Result<Option<SubmitOutcome>> {
    Ok(speculative_sql_explained(store, org_id, req, now_ms)
        .await?
        .map(|decided| decided.outcome))
}

/// [`speculative_sql`], with the decision's [`ExplainEvidence`].
pub async fn speculative_sql_explained(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    now_ms: i64,
) -> sqlx::Result<Option<ExplainedOutcome>> {
    // Invalid explicit template evidence must not gain a speculative SKIP
    // that the ordinary adapter rejects. Malformed speculative status policy
    // is uncharacterized; retain the conservative undecided fallback.
    let warehouse = Warehouse::from_dialect(&req.dialect);
    if warehouse.invalid_template_signature(req) || warehouse.invalid_dependency_definition(req) {
        return Ok(None);
    }
    let needs_no_table_evidence = req.execution_type == ModelExecutionType::DbtDataTest as i32
        && req.target_table.as_deref().is_none_or(str::is_empty)
        && req.tables.is_empty();
    if !needs_no_table_evidence {
        return Ok(None);
    }
    let outcome = decide_sql(store, org_id, req, now_ms, Mode::SkipOnly).await?;
    if outcome.is_some() {
        record_node_state(
            store,
            org_id,
            req.dbt_node_state.as_ref(),
            req.target_table.as_deref(),
        )
        .await;
    }
    Ok(outcome)
}

/// Which outcomes a SQL decision may settle on.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Every outcome (the ordinary submit).
    Ordinary,
    /// Only a Skip; any other outcome is abandoned, before anything is
    /// written, at the first point the decision can no longer be a Skip.
    SkipOnly,
}

/// The SQL decision shared by the ordinary and the speculative submit.
/// `None` only under [`Mode::SkipOnly`], for a decision that is not a Skip.
async fn decide_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    now_ms: i64,
    mode: Mode,
) -> sqlx::Result<Option<ExplainedOutcome>> {
    let mut evidence = ExecutionEvidence::sql_submit(org_id, req);
    evidence.display_submitted_at_ms =
        Warehouse::from_dialect(&req.dialect).explain_submit_clock(now_ms);
    let req = Warehouse::from_dialect(&req.dialect).resolve_submit(req);
    let req = req.as_ref();
    resolve_producers(store, &mut evidence).await?;
    let outcome = decide_resolved_sql(store, org_id, req, &evidence, now_ms, mode).await?;
    Ok(outcome.map(|outcome| ExplainedOutcome {
        explain: ExplainEvidence::of(req, &evidence, &outcome),
        outcome,
        rendered: None,
    }))
}

/// [`decide_sql`] for the resolved submit, once its inputs' producers are
/// resolved.
async fn decide_resolved_sql(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    now_ms: i64,
    mode: Mode,
) -> sqlx::Result<Option<SubmitOutcome>> {
    let selection = select_sql(store, org_id, req, evidence).await?;
    // Without a reuse candidate the decision never skips
    // (decision_proptest::no_match_always_executes), and the Result copy
    // fallback below would write a pending row.
    if mode == Mode::SkipOnly && selection.candidate.is_none() {
        return Ok(None);
    }

    // Precedence: with no reuse candidate, a Result copy of an equivalent
    // overrides the ordinary verdict. Not when the target's own table was
    // modified after its build: hosted rebuilds it (VERIFIED LIVE 2026-11-10,
    // lag_copy_candidates `ome`).
    let copy_considered = selection.candidate.is_none()
        && selection.why_no_candidate != Some(decision::NoCandidate::OwnTargetModified);
    let freshness = Freshness {
        sql_reads_relation: evidence.sql_reads_relation,
        target_table: req.target_table.as_deref(),
        stale_upstream_policy: decision::StaleUpstreamPolicy::from_i32(req.stale_upstream_policy),
        tolerance_ms: req.freshness_tolerance_seconds.max(0).saturating_mul(1000),
        now_ms,
        own_target_absent: req
            .target_table
            .as_deref()
            .is_some_and(|t| !req.tables.iter().any(|x| x.name == t)),
    };
    let mut refused_copy = None;
    if copy_considered {
        match result_copy_fallback(
            store,
            org_id,
            req,
            evidence,
            &freshness,
            (None, selection.rejected.as_ref()),
        )
        .await?
        {
            CopyFallback::Decided(outcome) => return Ok(Some(*outcome)),
            CopyFallback::Refused {
                clone_rejection_reason,
                ..
            } => refused_copy = Some(clone_rejection_reason),
            CopyFallback::Ordinary => {}
        }
    }
    let (mut record, candidate) = decide(evidence, &selection, &freshness);
    if mode == Mode::SkipOnly && record.class != DecisionKind::Skip {
        return Ok(None);
    }
    let candidate = candidate.cloned();
    // The target's own reuse candidate must rebuild for freshness, but an
    // equivalent under another name may still be reusable for this submit
    // (VERIFIED LIVE 2026-11-10, lag_copy_candidates `fe`, `st`, `wba`; not
    // when the equivalent is itself stale, `esb`). A submit the Warehouse
    // rules do not copy reports the equivalent in its codes and records the
    // equivalent's build age, which its Explain tree's Lag tolerance line
    // reports (VERIFIED LIVE 2026-11-11, explain_freshness_cells.jsonl
    // `fd_sn`; crafted, explain_rebuild_tolerance_line.jsonl `s2`, `i2`).
    if let (Some(own), true) = (&candidate, rebuilds_for_freshness(&record)) {
        match result_copy_fallback(
            store,
            org_id,
            req,
            evidence,
            &freshness,
            (Some(own), Some(own)),
        )
        .await?
        {
            CopyFallback::Decided(outcome) => {
                log_decision(evidence, outcome.record(), true, &req.labels);
                return Ok(Some(*outcome));
            }
            // Captured for a data-only rebuild; an Upstream producer change
            // keeps the ordinary codes (uncaptured).
            CopyFallback::Refused {
                clone_rejection_reason,
                equivalent_build_age_ms,
            } if record.reason == decision::DecisionReason::UpstreamOutdated => {
                refused_copy = Some(clone_rejection_reason);
                record.build_age_ms = equivalent_build_age_ms;
            }
            CopyFallback::Refused { .. } | CopyFallback::Ordinary => {}
        }
    }
    // An older match of the target is reported ahead of a refused copy's
    // equivalent under another name (VERIFIED LIVE 2026-11-11,
    // explain_freshness_cells.jsonl `fr_*`: incrementals and a snapshot
    // reverted to an older build's logic, with same-logic nodes elsewhere,
    // are answered LATEST_QUERY_HASH_NOT_MATCH / TARGET_TABLE_MATCH). A
    // refused copy otherwise reports its equivalent where a copy would have
    // been answered.
    let binding_change = record.reason
        == decision::DecisionReason::NoReuseCandidate(decision::NoCandidate::InputBindingChanged);
    if binding_change {
        record.build_age_ms = selection
            .unmatched
            .as_ref()
            .and_then(|row| Warehouse::from_dialect(&row.dialect).build_time_ms(row))
            .map(|built| now_ms.saturating_sub(built));
    }
    let older = older_match(store, evidence, &freshness, &record, |row| {
        reuse_candidate::older_build_matches(req, evidence, row)
    })
    .await?;
    let mut candidate = candidate;
    let record = match (older, refused_copy) {
        (Some((reuse, build_age_ms, older)), _) => {
            let mut record = record.with_older_match(reuse, build_age_ms);
            if binding_change {
                record.matched_execution_id = Some(older.id);
                candidate = Some(older);
            }
            record
        }
        (None, Some(clone_rejection_reason)) => record.with_refused_copy(clone_rejection_reason),
        (None, None) => record,
    };
    let scope = diagnostics::Scope {
        org_id,
        req,
        evidence,
        selection: &selection,
        freshness: &freshness,
    };
    let diagnostics::Diagnosed {
        record,
        other_target,
        direct_reference,
    } = diagnostics::decorate(store, &scope, record, candidate.is_some()).await?;
    if let Some(reference) = other_target {
        let ids = DecisionIds::mint();
        write_pending(store, evidence, &ids).await?;
        return Ok(Some(SubmitOutcome::ChainLimitExecute {
            ids,
            record,
            source_table: reference.source_name,
        }));
    }
    log_decision(evidence, &record, candidate.is_some(), &req.labels);
    let (record, candidate) = anchor_rejected(
        record,
        candidate,
        selection.rejected.or(selection.type_changed),
    );
    let record = record.with_unmatched_build(selection.unmatched.as_ref());
    let mut outcome = settle(store, evidence, record, candidate).await?;
    if req.execution_type == ModelExecutionType::Merge as i32 {
        if let (
            Some(reference),
            SubmitOutcome::Execute {
                refused_copy_source,
                ..
            },
        ) = (direct_reference, &mut outcome)
        {
            *refused_copy_source = Some(reference.source_name);
        }
    }
    Ok(Some(outcome))
}

/// A rebuild whose latest build had the workload's identity but whose own
/// target was missing or modified keeps that build as its Explain anchor; so
/// does a rebuild whose latest build had the identity under another
/// execution type (#83).
fn anchor_rejected(
    mut record: DecisionRecord,
    candidate: Option<ExecutionRow>,
    rejected: Option<ExecutionRow>,
) -> (DecisionRecord, Option<ExecutionRow>) {
    match (candidate, rejected) {
        (None, Some(row)) if record.class == DecisionKind::Build => {
            record.matched_execution_id = Some(row.id);
            (record, Some(row))
        }
        (candidate, _) => (record, candidate),
    }
}

/// A rebuild the reuse candidate's freshness required (not its identity).
fn rebuilds_for_freshness(record: &DecisionRecord) -> bool {
    record.class == DecisionKind::Build
        && matches!(
            record.reason,
            decision::DecisionReason::UpstreamOutdated
                | decision::DecisionReason::UpstreamQueryChanged
        )
}

/// The decision context for judging `build` as the reuse candidate: the
/// submit's inputs and freshness parameters, with `build`'s age on the
/// server clock (ADR 0007: the ConfirmExecution epoch where epochs are
/// wall-clock milliseconds, the confirmation's receipt on DuckDB).
fn context<'a>(
    evidence: &'a ExecutionEvidence,
    selection: &Selection,
    freshness: &Freshness<'a>,
    build: Option<&ExecutionRow>,
) -> SubmitContext<'a> {
    let build_age_ms = build.and_then(|c| {
        Warehouse::from_dialect(&evidence.dialect)
            .build_time_ms(c)
            .map(|built| freshness.now_ms.saturating_sub(built))
    });
    SubmitContext {
        execution_type: evidence.execution_type,
        input_tables: &evidence.input_tables,
        sql_reads_relation: freshness.sql_reads_relation,
        target_table: freshness.target_table,
        stale_upstream_policy: freshness.stale_upstream_policy,
        target_has_prior_build: selection.target_has_prior_build,
        why_no_candidate: selection.why_no_candidate,
        tolerance_ms: freshness.tolerance_ms,
        build_age_ms,
        own_target_absent: freshness.own_target_absent,
    }
}

/// The ordinary decision against the selected reuse candidate. A rebuild of
/// an absent own target reports the target-table codes when the latest build
/// it rejected would otherwise be reusable for this submit.
fn decide<'s>(
    evidence: &ExecutionEvidence,
    selection: &'s Selection,
    freshness: &Freshness<'_>,
) -> (DecisionRecord, Option<&'s ExecutionRow>) {
    let candidate = selection.candidate.as_ref();
    let ctx = context(evidence, selection, freshness, candidate);
    let mut record = DecisionRecord::decided(&ctx, candidate);
    if let Some(rejected) = &selection.rejected {
        let ctx = context(evidence, selection, freshness, Some(rejected));
        if decision::reusable(&ctx, rejected).is_some() {
            record = record.with_absent_target_match();
        }
        // Hosted #150: with both a modified Target and an independently
        // advanced source, Databricks MICRO reports the generic rebuild text.
        // Only decorate the description: qualification, freshness, reason and
        // Explain's anchor remain unchanged. `rejected` already passed strict
        // identity and reuse qualification before own-target rejection; copies
        // are outside this captured ordinary-build cell.
        if Warehouse::from_dialect(&evidence.dialect)
            .modified_target_uses_upstream_description(evidence.execution_type)
            && rejected.clone_source_table.is_none()
            && evidence.node_sql_hash.is_some()
            && rejected.node_sql_hash == evidence.node_sql_hash
            && record.reason
                == decision::DecisionReason::NoReuseCandidate(
                    decision::NoCandidate::OwnTargetModified,
                )
            && ctx.tolerance_ms == 0
            && ctx.stale_upstream_policy == decision::StaleUpstreamPolicy::Any
            && rejected.last_modified_epoch.is_some_and(|recorded| {
                recorded > 0
                    && evidence.input_tables.iter().any(|input| {
                        Some(input.name.as_str()) == ctx.target_table
                            && input
                                .last_modified_epoch
                                .is_some_and(|current| current > recorded)
                    })
            })
        {
            let upstreams = decision::upstream_freshness(&ctx, rejected).upstreams;
            if upstreams.iter().all(|input| {
                !input.unprovable
                    && !input.producer_changed
                    && input.anchor_epoch > 0
                    && input.current_epoch.is_some_and(|current| current > 0)
            }) && upstreams.iter().any(|input| {
                input.drifted
                    && input.anchor_epoch > 0
                    && input
                        .current_epoch
                        .is_some_and(|current| current > input.anchor_epoch)
            }) {
                record.description = decision::DESC_EXECUTE.into();
            }
        }
    }
    (record, candidate)
}

/// When a rebuild's target had an OLDER build (overwritten since) with the
/// workload's identity that would still be reusable for this submit: how
/// that build would be reused, and its age. Hosted reports it
/// (LATEST_QUERY_HASH_NOT_MATCH / TARGET_TABLE_MATCH, `is_stale` as that
/// build would be reused, [`DecisionRecord::with_older_match`]). Only the
/// newest such build is judged. First builds, absent targets and own-target
/// edits have none.
async fn older_match(
    store: &Store,
    evidence: &ExecutionEvidence,
    freshness: &Freshness<'_>,
    record: &DecisionRecord,
    matches: impl Fn(&ExecutionRow) -> bool,
) -> sqlx::Result<Option<(decision::Reuse, Option<i64>, ExecutionRow)>> {
    use decision::{DecisionReason as R, NoCandidate as N};
    let applies = record.class == DecisionKind::Build
        && !freshness.own_target_absent
        && matches!(
            record.reason,
            R::UpstreamOutdated
                | R::UpstreamQueryChanged
                | R::DependencySetChanged
                | R::NoReuseCandidate(
                    N::IdentityMismatch
                        | N::UpstreamNamesChanged
                        | N::CopyLogicMismatch
                        | N::InputBindingChanged
                )
        );
    if !applies {
        return Ok(None);
    }
    let history = store
        .target_history(
            &evidence.org_id,
            &evidence.target_table,
            execution::non_blank_namespace(evidence.table_namespace.as_deref()),
            &evidence.dialect,
        )
        .await?;
    let Some(older) = history.into_iter().skip(1).find(|row| matches(row)) else {
        return Ok(None);
    };
    let Ok(older) = older.into_checked(false) else {
        return Ok(None);
    };
    let selection = Selection::of_candidate(older.clone());
    let ctx = context(evidence, &selection, freshness, Some(&older));
    let age = ctx.build_age_ms;
    Ok(decision::reusable(&ctx, &older).map(|reuse| (reuse, age, older)))
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
    sql_reads_relation: Option<bool>,
    target_table: Option<&'a str>,
    stale_upstream_policy: decision::StaleUpstreamPolicy,
    tolerance_ms: i64,
    now_ms: i64,
    /// The submit does not report the node's own target.
    own_target_absent: bool,
}

/// Decide a `SubmitValues` (seed) submission. Seeds have no Lag tolerance and
/// always use the ANY stale-upstream policy. A seed without a reuse
/// candidate may be copied from its load under another name where the
/// Warehouse rules admit it (Snowflake, #91; [`seed_copy_fallback`]).
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
    let latest = current.clone();
    let selection = reuse_candidate::for_seed(req, &evidence, current)?;
    let mut refused_copy = None;
    if let Some(target) = result_copy::seed_copy_target(req, &selection, latest.as_ref()) {
        match seed_copy_fallback(store, org_id, req, &evidence, target).await? {
            SeedCopy::Copied(outcome) => {
                log_decision(&evidence, outcome.record(), false, &req.labels);
                return Ok(*outcome);
            }
            SeedCopy::Refused {
                reason,
                source_table,
            } => refused_copy = Some((reason, source_table, target)),
            SeedCopy::Ordinary => {}
        }
    }

    let freshness = Freshness {
        sql_reads_relation: None,
        target_table: Some(req.target_table.as_str()),
        stale_upstream_policy: decision::StaleUpstreamPolicy::Any,
        tolerance_ms: 0,
        now_ms,
        own_target_absent: req.last_modified_epoch.is_none(),
    };
    let (record, candidate) = decide(&evidence, &selection, &freshness);
    let candidate = candidate.cloned();
    let older = older_match(store, &evidence, &freshness, &record, |row| {
        reuse_candidate::older_seed_matches(req, &evidence, row)
    })
    .await?;
    // An older match of the table is reported ahead of a refused copy, as
    // for a SQL submit.
    let (record, refused_copy) = match older {
        Some((reuse, build_age_ms, _)) => (record.with_older_match(reuse, build_age_ms), None),
        None => (record, refused_copy),
    };
    let (record, candidate) = anchor_rejected(record, candidate, selection.rejected);
    let Some((reason, source_table, target)) = refused_copy else {
        log_decision(&evidence, &record, candidate.is_some(), &req.labels);
        return settle(store, &evidence, record, candidate).await;
    };
    let over = match target {
        SeedCopyTarget::Absent => None,
        SeedCopyTarget::Over(row) => Some(row),
    };
    let record = record.with_seed_copy_refused(reason, over);
    log_decision(&evidence, &record, candidate.is_some(), &req.labels);
    if record.reason != decision::DecisionReason::CopyDisabled {
        return settle(store, &evidence, record, candidate).await;
    }
    // The FULL `copy_disabled` outcome: an ordinary pending row, and the
    // equivalent Explain names.
    let ids = DecisionIds::mint();
    write_pending(store, &evidence, &ids).await?;
    Ok(SubmitOutcome::ChainLimitExecute {
        ids,
        record,
        source_table,
    })
}

/// What the seed copy on submit decided ([`seed_copy_fallback`]).
enum SeedCopy {
    /// The seed is copied (`ready_to_clone`).
    Copied(Box<SubmitOutcome>),
    /// A load to copy exists, `source_table`, but the submit disallows or
    /// forbids copies: the seed is loaded, recording this reason
    /// ([`DecisionRecord::with_seed_copy_refused`]).
    Refused {
        reason: decision::DecisionReason,
        source_table: String,
    },
    /// The ordinary decision applies unchanged.
    Ordinary,
}

/// The seed copy on submit (#91, Snowflake only; see
/// [`result_copy::seed_lookup`]): the newest current load of the seed's
/// reuse identity in another schema of its namespace is copied into the
/// target, under the limits of a FULL copy. Otherwise the ordinary decision
/// applies: no copy is admitted or no such load exists. A submit that
/// disallows copies is loaded with the FULL `copy_disabled` codes and Explain
/// names the load it could have copied (VERIFIED LIVE 2026-11-12,
/// explain_seed_copy_disabled.jsonl; [`SeedCopy::Refused`]).
///
/// A seed has no upstreams, so the copy is not judged for freshness. An
/// External modification of the source after its load is not visible at
/// submit time (docs/correctness-handoff.md), as for a FULL copy.
async fn seed_copy_fallback(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitValuesRequest,
    evidence: &ExecutionEvidence,
    target: SeedCopyTarget<'_>,
) -> sqlx::Result<SeedCopy> {
    let Some(lookup) = result_copy::seed_lookup(req, evidence) else {
        return Ok(SeedCopy::Ordinary);
    };
    let Some(candidate) = seed_copy_source(store, org_id, req, &lookup).await? else {
        return Ok(SeedCopy::Ordinary);
    };
    // Snowflake tracks no copy depth: the source counts as a build, as for a
    // FULL copy.
    let source_table = candidate.target_table.clone();
    match result_copy::plan_seed_submit(req, CloneEquivalent::new(candidate, 0)) {
        SubmitCopyPlan::Copy(copy) => {
            let ids = DecisionIds::mint();
            // Keyed on the submit target with the seed's own identity, so a
            // later submit reuses the copy once confirmed.
            write_pending(store, &evidence.copied_from(&copy.source_table), &ids).await?;
            let over = match target {
                SeedCopyTarget::Absent => None,
                SeedCopyTarget::Over(row) => Some(row),
            };
            Ok(SeedCopy::Copied(Box::new(SubmitOutcome::Copy {
                ids,
                copy,
                record: DecisionRecord::seed_result_copy(over),
            })))
        }
        SubmitCopyPlan::Unavailable { reason } => {
            tracing::warn!(
                target = %req.target_table,
                reason,
                "equivalent seed found but no result copy is available; loading it"
            );
            Ok(SeedCopy::Ordinary)
        }
        SubmitCopyPlan::CopiesDisabled => Ok(SeedCopy::Refused {
            reason: decision::DecisionReason::CopyDisabled,
            source_table,
        }),
        SubmitCopyPlan::ChainLimitExecute => Ok(SeedCopy::Refused {
            reason: decision::DecisionReason::CopyChainLimit,
            source_table,
        }),
        SubmitCopyPlan::Refused { .. } => Ok(SeedCopy::Ordinary),
    }
}

/// The newest current load [`seed_copy_fallback`] may copy: the first
/// candidate the lookup admits. The relation check is by canonical
/// identifier, so it runs after the read; candidates are read a page at a
/// time until one is admitted, because the same values under other object
/// names (never sources) could otherwise fill the page and hide an eligible
/// source (PR #131 review). Admitting a candidate costs no further read.
async fn seed_copy_source(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitValuesRequest,
    lookup: &result_copy::SeedCopyLookup<'_>,
) -> sqlx::Result<Option<CopyCandidate>> {
    let mut offset = 0;
    loop {
        let page = store
            .find_seed_copy_candidates(
                org_id,
                &SeedCopyCandidateQuery {
                    table_namespace: lookup.table_namespace,
                    seed_logic_hash: lookup.seed_logic_hash,
                    values_hash: lookup.values_hash,
                    exclude_target: lookup.target_table,
                    dialect: &req.dialect,
                    limit: COPY_CANDIDATE_LIMIT,
                    offset,
                },
            )
            .await?;
        let read = page.len() as i64;
        if let Some(candidate) = page.into_iter().find(|c| lookup.admits(&c.target_table)) {
            return Ok(Some(candidate));
        }
        if read < COPY_CANDIDATE_LIMIT {
            return Ok(None);
        }
        offset += read;
    }
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
        // A node kind change with unchanged identity is judged against the
        // target's latest build of the workload's own type, when there is one
        // (#83, #90).
        if let Some(latest) = current
            .as_ref()
            .filter(|latest| reuse_candidate::execution_type_changed(req, evidence, latest))
        {
            let history = store
                .target_history(
                    org_id,
                    req.target_table.as_deref().unwrap_or_default(),
                    table_namespace,
                    &req.dialect,
                )
                .await?;
            if let Some(build) = reuse_candidate::latest_build_of_type(req, evidence, history) {
                return reuse_candidate::for_model_over_build_of_type(
                    req,
                    evidence,
                    latest.clone(),
                    build,
                );
            }
        }
        reuse_candidate::for_model(req, evidence, current)
    }
}

/// What the Result copy fallback decided ([`result_copy_fallback`]).
enum CopyFallback {
    /// The submit is answered by the fallback (a copy, or the chain-limit
    /// build).
    Decided(Box<SubmitOutcome>),
    /// An equivalent exists, but this submit's execution type is not copied
    /// (see [`SubmitCopyPlan::Refused`]): the ordinary decision applies, with
    /// TARGET_TABLE_MISMATCH and this clone rejection.
    /// `equivalent_build_age_ms` is the equivalent's build age when it was
    /// judged for freshness (it read the same upstream relations).
    Refused {
        clone_rejection_reason: i32,
        equivalent_build_age_ms: Option<i64>,
    },
    /// The ordinary decision applies unchanged.
    Ordinary,
}

/// The Result copy fallback (see [`result_copy`]): an equivalent model already
/// built under another name is copied in. `own` is the target's reuse
/// candidate when it had one but must rebuild for freshness (the copy then
/// goes over the existing target). [`CopyFallback::Ordinary`] means the
/// ordinary decision applies: no lookup was warranted, no equivalent exists,
/// the equivalent is not fresh enough for this submit, or no copy can be
/// produced for it. An equivalent of a submit the Warehouse rules do not copy
/// is chosen the same way (of any execution type) and reported as
/// [`CopyFallback::Refused`].
///
/// An equivalent that read the same upstream relations as this submit is
/// judged exactly as if it were the reuse candidate, for an existing or a
/// dropped target (VERIFIED LIVE 2026-11-10, lag_copy_candidates: an
/// equivalent whose consumed upstream was since externally modified is not
/// copied; lag_dropped_targets `dset0`/`dset600`). One in another environment reads
/// other relations and is copied without that judgment, as verified earlier
/// (clone_fallback_stale_source, clone_fallback_zero_tolerance).
async fn result_copy_fallback(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    freshness: &Freshness<'_>,
    judgement: (Option<&ExecutionRow>, Option<&ExecutionRow>),
) -> sqlx::Result<CopyFallback> {
    let (own, anchor) = judgement;
    if Warehouse::from_dialect(&req.dialect) == Warehouse::Databricks {
        return databricks_result_copy(store, org_id, req, evidence, freshness, anchor).await;
    }
    let Some(lookup) = result_copy::submit_lookup(req, evidence, false) else {
        return Ok(CopyFallback::Ordinary);
    };
    // The first equivalent, in preference order, that the warehouse rules
    // accept and that qualifies for this submit ([`copy_search`]).
    let mut search = copy_search::CopySearch::new(own);
    let mut chosen = None;
    for candidate in copy_candidates(store, org_id, req, &lookup).await? {
        let Some(equivalent) = eligible_equivalent(store, org_id, req, &lookup, candidate).await?
        else {
            continue;
        };
        let source = store
            .current_target_state(
                org_id,
                &equivalent.source_table,
                Some(lookup.table_namespace),
                &req.dialect,
            )
            .await?;
        match search.offer(evidence, freshness, source) {
            copy_search::Offer::Choose(judged) => {
                chosen = Some((equivalent, judged));
                break;
            }
            copy_search::Offer::Next => continue,
            copy_search::Offer::Stop => break,
        }
    }
    let Some((equivalent, judged)) = chosen else {
        return Ok(CopyFallback::Ordinary);
    };
    let source_table = equivalent.source_table.clone();
    match result_copy::plan_submit(req, equivalent) {
        plan @ (SubmitCopyPlan::ChainLimitExecute | SubmitCopyPlan::CopiesDisabled)
            if own.is_none() =>
        {
            let ids = DecisionIds::mint();
            // An ordinary (non-copy) pending row: the forthcoming
            // ConfirmExecution records the normal build.
            write_pending(store, evidence, &ids).await?;
            let record = match plan {
                SubmitCopyPlan::CopiesDisabled => DecisionRecord::copy_disabled(),
                _ => DecisionRecord::copy_chain_limit(),
            };
            Ok(CopyFallback::Decided(Box::new(
                SubmitOutcome::ChainLimitExecute {
                    ids,
                    record,
                    source_table,
                },
            )))
        }
        SubmitCopyPlan::Copy(copy) => {
            let ids = DecisionIds::mint();
            // Keyed on the submit target and inheriting the equivalent's logic
            // identity, so later submits reuse the copy once confirmed.
            write_pending(store, &evidence.copied_from(&copy.source_table), &ids).await?;
            let record = match judged {
                Some((reuse, build_age_ms)) => DecisionRecord::result_copy_judged(
                    anchor,
                    reuse,
                    freshness.tolerance_ms,
                    build_age_ms,
                    &evidence.input_tables,
                ),
                None => DecisionRecord::result_copy(),
            };
            Ok(CopyFallback::Decided(Box::new(SubmitOutcome::Copy {
                ids,
                copy,
                record,
            })))
        }
        SubmitCopyPlan::ChainLimitExecute | SubmitCopyPlan::CopiesDisabled => {
            Ok(CopyFallback::Ordinary)
        }
        SubmitCopyPlan::Refused {
            clone_rejection_reason,
        } => Ok(CopyFallback::Refused {
            clone_rejection_reason,
            equivalent_build_age_ms: judged.and_then(|(_, build_age_ms)| build_age_ms),
        }),
        SubmitCopyPlan::Unavailable { reason } => {
            tracing::warn!(
                target = %req.target_table.as_deref().unwrap_or_default(),
                reason,
                "equivalent found but no result copy is available; using the ordinary decision"
            );
            Ok(CopyFallback::Ordinary)
        }
    }
}

/// Databricks never takes the generic unjudged cross-input path. Its source
/// is read with a pending-write barrier, and its build inputs are frozen into
/// both the pending copy and its publication proof.
async fn databricks_result_copy(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    freshness: &Freshness<'_>,
    anchor: Option<&ExecutionRow>,
) -> sqlx::Result<CopyFallback> {
    if !crate::databricks_submit_copy::request_admits(req, evidence) {
        return Ok(CopyFallback::Ordinary);
    }
    let Some(lookup) = result_copy::submit_lookup(req, evidence, false) else {
        return Ok(CopyFallback::Ordinary);
    };
    for candidate in copy_candidates(store, org_id, req, &lookup).await? {
        let Some(source) = store
            .current_direct_copy_source(
                org_id,
                &candidate.target_table,
                lookup.table_namespace,
                &req.dialect,
            )
            .await?
        else {
            continue;
        };
        if !crate::copy_lifecycle::source_admits(evidence, &source) {
            continue;
        }
        let Some((reuse, age)) = copy_search::judge_equivalent(evidence, freshness, source.clone())
        else {
            continue;
        };
        let equivalent = CloneEquivalent {
            source_table: source.target_table.clone(),
            execution_type: source.execution_type,
            table_type: None,
            last_modified_epoch: source.last_modified_epoch,
            execution_runtime_ms: source.execution_runtime_ms,
            copy_depth: 0,
        };
        if let SubmitCopyPlan::Copy(copy) = result_copy::plan_submit(req, equivalent) {
            let ids = DecisionIds::mint();
            write_pending(store, &evidence.submit_copied_from(&source), &ids).await?;
            return Ok(CopyFallback::Decided(Box::new(SubmitOutcome::Copy {
                ids,
                copy,
                record: DecisionRecord::result_copy_judged(
                    anchor.or(Some(&source)),
                    reuse,
                    freshness.tolerance_ms,
                    age,
                    &evidence.input_tables,
                ),
            })));
        }
    }
    Ok(CopyFallback::Ordinary)
}

/// How many current equivalents the Result copy fallback considers, newest
/// build first. Each costs a state read (and a provenance walk where copy
/// depth is tracked), so a rebuild in a namespace with many environments
/// does not pay one read per environment. An equivalent past the cap is never
/// copied; the ordinary decision applies instead. A seed copy, which pays no
/// read per candidate, reads candidates in pages of this size
/// ([`seed_copy_source`]).
const COPY_CANDIDATE_LIMIT: i64 = 16;

/// Every current equivalent of the submit, up to [`COPY_CANDIDATE_LIMIT`], in
/// the store's preference order (newest build first). A candidate the lookup
/// does not admit (a rehomed identity matched in another schema) is dropped
/// after the read, so it can only cost an equivalent past the cap.
async fn copy_candidates(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    lookup: &EquivalentLookup<'_>,
) -> sqlx::Result<Vec<CopyCandidate>> {
    let candidates = store
        .find_copy_candidates(
            org_id,
            &CopyCandidateQuery {
                table_namespace: lookup.table_namespace,
                node_namespace_logic_hashes: &lookup.logic_hashes(),
                execution_type: lookup.execution_type,
                exclude_target: lookup.exclude_target,
                dialect: &req.dialect,
                limit: COPY_CANDIDATE_LIMIT,
            },
        )
        .await?;
    Ok(candidates
        .into_iter()
        .filter(|candidate| lookup.admits(candidate))
        .collect())
}

/// `candidate` as a copy source, if the warehouse rules accept it for the
/// submit target. Where the rules track copy depth, its copy provenance is
/// walked and a candidate with too long or missing a chain is rejected.
async fn eligible_equivalent(
    store: &Store,
    org_id: &str,
    req: &qc::SubmitEnrichedSqlRequest,
    lookup: &EquivalentLookup<'_>,
    candidate: CopyCandidate,
) -> sqlx::Result<Option<CloneEquivalent>> {
    let warehouse = Warehouse::from_dialect(&req.dialect);
    if !warehouse.copy_relations_compatible(&candidate.target_table, lookup.exclude_target) {
        return Ok(None);
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
                return Ok(None);
            };
            depth
        }
    };
    Ok(Some(CloneEquivalent::new(candidate, copy_depth)))
}

fn log_decision(
    evidence: &ExecutionEvidence,
    record: &DecisionRecord,
    had_match: bool,
    labels: &HashMap<String, String>,
) {
    tracing::info!(
        node = %labels.get("dbt_node_name").map(String::as_str).unwrap_or_default(),
        execution_type = evidence.execution_type,
        had_match,
        body_hash = %evidence.node_body_hash.as_deref().unwrap_or_default(),
        values_hash = %evidence.values_hash.as_deref().unwrap_or_default(),
        verdict = verdict_label(record),
        reason = ?record.reason,
        "decision"
    );
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
                refused_copy_source: None,
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
