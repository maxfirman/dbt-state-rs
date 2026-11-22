//! Reuse candidate selection: which recorded result, if any, a submit may reuse.
//!
//! The caller fetches the latest execution in scope
//! ([`Store::current_target_state`](crate::store::Store::current_target_state),
//! [`Store::latest_data_test_by_key`](crate::store::Store::latest_data_test_by_key)
//! or [`Store::latest_data_test`](crate::store::Store::latest_data_test)) and
//! passes it in. This module then decides, without I/O:
//!
//! 1. whether that row matches the workload's identity (strict same-target
//!    match, or adoption of a clone-origin row with the same
//!    schema-abstracted logic);
//! 2. whether the workload and its evidence pass reuse qualification.
//!
//! Freshness is NOT assessed here: a reuse candidate goes to
//! [`decision::decide`](crate::decision::decide), which may still rebuild.
//!
//! Only the latest row is ever reused (physical-state invariant, review issue
//! #1 P0#2): an older execution cannot describe a target overwritten by a
//! later build, so nothing older is resurrected when the latest row does not
//! match. One exception to which row a rebuild is *judged* against: when the
//! latest row has the workload's identity under another execution type, the
//! latest build of the workload's own type is (hosted, #83/#90;
//! [`for_model_over_build_of_type`]). It is never reused.

use crate::decision::NoCandidate;
use crate::execution::ExecutionEvidence;
use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
use crate::store::{ExecutionRow, InputTable};

/// The outcome of reuse candidate selection for one submit.
#[derive(Debug)]
pub(crate) struct Selection {
    /// The reuse candidate, if the latest row has the workload's identity and
    /// qualified.
    pub candidate: Option<ExecutionRow>,
    /// Whether the target has any confirmed build in its physical scope,
    /// whatever its identity. Distinguishes a first build ("did not exist")
    /// from a rebuild in the EXECUTE description (review #2 5th follow-up P1 #4).
    pub target_has_prior_build: bool,
    /// Why the latest row in scope is not a reuse candidate. `None` when there
    /// is a candidate or no latest row at all (a first build).
    pub why_no_candidate: Option<NoCandidate>,
    /// The latest row when it has the workload's identity but its own target
    /// is missing or was modified after it: what Explain renders against
    /// (VERIFIED LIVE 2026-11-10, lag_copy_candidates `ome`, `de`). Never
    /// reused.
    pub rejected: Option<ExecutionRow>,
    /// The latest row when it does not have the workload's identity (models
    /// only). Never reused; Explain judges the SHAPE 7 freshness listing
    /// against it (VERIFIED LIVE 2026-11-11, explain_rebuild_reason_gaps.jsonl).
    pub unmatched: Option<ExecutionRow>,
    /// The latest row when it has the workload's identity under another
    /// execution type and no build of the workload's type stands in for it:
    /// the EXECUTION_TYPE_MISMATCH rebuild is explained against it (VERIFIED
    /// LIVE 2026-11-10/11, explain_snapshot_history.jsonl entry 58,
    /// explain_execution_type_change.jsonl). Never reused.
    pub type_changed: Option<ExecutionRow>,
    /// Databricks APPEND/MERGE response/Explain basis only. Selection still
    /// rejects against the submit type's build; this row is never reusable.
    pub transition_display: Option<ExecutionRow>,
    /// The latest row when it has the workload's identity but the submit
    /// (a model) failed Reuse qualification (volatile SQL with Volatile
    /// evaluation requested, among others): Explain judges the rebuild's freshness
    /// listing against it (`explain_volatile_cells.jsonl` entries 189, 190
    /// (#205), 195 and 199 (#206)). Never reused, and never a copy's anchor.
    pub unqualified: Option<ExecutionRow>,
    /// Evidence from the latest matching build refused at Reuse qualification.
    pub rerun_evidence: RerunEvidence,
    /// The latest ordinary build's producer revision for the same rendered
    /// SQL identity, including builds refused for ambiguous Target evidence.
    pub producer_revision: Option<i64>,
}

impl Selection {
    /// `row` as the reuse candidate, for judging it as if selected.
    pub(crate) fn of_candidate(row: ExecutionRow) -> Selection {
        Selection {
            candidate: Some(row),
            target_has_prior_build: true,
            why_no_candidate: None,
            rejected: None,
            unqualified: None,
            unmatched: None,
            type_changed: None,
            transition_display: None,
            rerun_evidence: RerunEvidence::Unverified,
            producer_revision: None,
        }
    }

    /// No confirmed build of the target exists in scope.
    const FIRST_BUILD: Selection = Selection {
        candidate: None,
        target_has_prior_build: false,
        why_no_candidate: None,
        rejected: None,
        unqualified: None,
        unmatched: None,
        type_changed: None,
        transition_display: None,
        rerun_evidence: RerunEvidence::Unverified,
        producer_revision: None,
    };

    /// The outcome for a latest row in scope: the reuse candidate when the
    /// row's identity matched (`identified`) and `rejection` found nothing
    /// against it, otherwise why it is not one.
    /// `unidentified` says why when the row's identity did not match.
    fn of(
        identified: Option<ExecutionRow>,
        unidentified: NoCandidate,
        rejection: impl FnOnce(&ExecutionRow) -> Option<NoCandidate>,
        workload: Workload,
    ) -> Selection {
        let why_no_candidate = match &identified {
            None => Some(unidentified),
            Some(row) => rejection(row),
        };
        let own_target = matches!(
            why_no_candidate,
            Some(NoCandidate::OwnTargetModified | NoCandidate::OwnTargetMissing)
        );
        // Only a model's qualification-refused rebuild is captured explained
        // against the refused build (#205).
        let unqualified_build = why_no_candidate == Some(NoCandidate::FailedReuseQualification)
            && matches!(workload, Workload::Model { .. });
        let (candidate, rejected, unqualified) = match identified {
            Some(row) if why_no_candidate.is_none() => (Some(row), None, None),
            Some(row) if own_target => (None, Some(row), None),
            Some(row) if unqualified_build => (None, None, Some(row)),
            _ => (None, None, None),
        };
        let (target_has_prior_build, unmatched) = match workload {
            Workload::Model { unmatched } => (true, unmatched),
            Workload::DataTest => (false, None),
            Workload::Seed => (true, None),
        };
        // A row with the identity under another type is no unmatched build.
        let (unmatched, type_changed) = match why_no_candidate {
            Some(NoCandidate::ExecutionTypeChanged) => (None, unmatched),
            _ => (unmatched, None),
        };
        Selection {
            candidate,
            target_has_prior_build,
            why_no_candidate,
            rejected,
            unqualified,
            unmatched,
            type_changed,
            transition_display: None,
            rerun_evidence: RerunEvidence::Unverified,
            producer_revision: None,
        }
    }

    fn with_rerun_evidence(mut self, rerun: RerunEvidence) -> Selection {
        if self.why_no_candidate == Some(NoCandidate::FailedReuseQualification) {
            self.rerun_evidence = rerun;
        }
        self
    }
}

/// The workload a [`Selection`] is for, which decides what it keeps of a
/// latest row that is not the reuse candidate.
#[allow(clippy::large_enum_variant)] // A by-value argument, moved into the Selection.
enum Workload {
    /// A model. `unmatched` is the latest row when it does not have the
    /// workload's identity ([`Selection::unmatched`]), and a build refused
    /// at Reuse qualification is kept ([`Selection::unqualified`]).
    Model { unmatched: Option<ExecutionRow> },
    /// A data test: no physical target, so no prior build to report, and a
    /// qualification refusal keeps no anchor.
    DataTest,
    /// A seed.
    Seed,
}

/// Models, views, snapshots and other target-producing SQL submits.
///
/// `current` is the target's current physical state. `evidence` is the
/// submit's identity as built by `ExecutionEvidence::sql_submit`.
pub(crate) fn for_model(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    current: Option<ExecutionRow>,
) -> sqlx::Result<Selection> {
    debug_assert_ne!(req.execution_type, ModelExecutionType::DbtDataTest as i32);
    let Some(current) = current else {
        return Ok(Selection::FIRST_BUILD);
    };
    let transition_display =
        databricks_append_merge_transition(req, evidence, &current).then(|| current.clone());
    // Same identity, other node kind (#83): hosted's EXECUTION_TYPE_MISMATCH,
    // explained against this row. The caller routes a target that also has
    // a build of the workload's type to `for_model_over_build_of_type`.
    // Likewise a RegisterClone copy this submit cannot adopt (#93, #104):
    // another kind with the copy's logic is a node kind change, and other
    // logic is a copy logic mismatch.
    let unidentified = if execution_type_changed(req, evidence, &current) {
        NoCandidate::ExecutionTypeChanged
    } else if is_copy_registration(&current) && !adoptable(req, evidence, &current) {
        NoCandidate::CopyLogicMismatch
    } else if identity_under_other_type(req, evidence, &current) {
        // The identity under another type, but over other upstream names:
        // another identity on hosted (#116).
        NoCandidate::UpstreamNamesChanged
    } else if crate::warehouse::Warehouse::from_dialect(&req.dialect)
        .changed_resolution_context(req, &current)
        .is_some()
    {
        NoCandidate::ResolutionContextChanged
    } else if crate::warehouse::Warehouse::from_dialect(&req.dialect)
        .changed_input_binding(req, &current)
    {
        NoCandidate::InputBindingChanged
    } else {
        NoCandidate::IdentityMismatch
    };
    let mut unmatched = None;
    let identified = if strict_match(req, evidence, &current) {
        // Result-serving path: corrupt cached results surface as an error.
        Some(current.into_checked(true)?)
    } else if adoptable(req, evidence, &current) {
        // CROSS-ENVIRONMENT CLONE ADOPTION. The strict (exact rendered SQL)
        // match missed, but the current state is a CLONE of the same model in
        // this namespace: its schema-abstracted logic identity equals this
        // submit's. This reproduces the hosted post-clone SKIP, which is
        // logic-sensitive (a genuine SQL change still executes). Verified live;
        // see experiments/clone_logic_sensitivity.sh. Adoption never serves
        // cached results (clone-origin rows carry none).
        let mut row = current.into_checked(false)?;
        adopt_current_inputs(&mut row, &evidence.input_tables);
        Some(row)
    } else {
        unmatched = Some(current);
        None
    };
    let rerun = identified
        .as_ref()
        .map(|row| rerun_evidence(req, evidence, row))
        .unwrap_or_default();
    let producer_revision = identified
        .as_ref()
        .and_then(|row| row.retained_producer_revision(evidence.node_sql_hash.as_deref()));
    let mut selected = Selection::of(
        identified,
        unidentified,
        |prev| model_rejection(req, evidence, prev),
        Workload::Model { unmatched },
    )
    .with_rerun_evidence(rerun);
    selected.transition_display = transition_display;
    selected.producer_revision = producer_revision;
    Ok(selected)
}

/// Why a model's build with the workload's identity is not a reuse
/// candidate, if anything is against it: reuse qualification, then its own
/// target's existence and External modifications.
fn model_rejection(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    prev: &ExecutionRow,
) -> Option<NoCandidate> {
    let template_qualified = volatile_sql_compared_by_unrendered_code(req)
        && evidence.node_template_hash.is_some()
        && prev.node_template_hash == evidence.node_template_hash;
    let checked_databricks = crate::copy_lifecycle::ordinary(prev)
        && crate::warehouse::Warehouse::from_dialect(&req.dialect).checked_volatile_reuse(req);
    let qualifies = if template_qualified || checked_databricks {
        sql_evidence_complete(req, evidence)
    } else {
        sql_qualifies(req, evidence)
    };
    if !qualifies {
        Some(NoCandidate::FailedReuseQualification)
    } else {
        match own_target(req, prev) {
            OwnTarget::AsBuilt => None,
            OwnTarget::Modified => Some(NoCandidate::OwnTargetModified),
            OwnTarget::Missing => Some(NoCandidate::OwnTargetMissing),
            OwnTarget::Ambiguous => Some(NoCandidate::OwnTargetChangedOrMissing),
        }
    }
}

/// Whether the target's latest row `latest` has the workload's identity but
/// was built as another execution type (a node kind change with unchanged
/// SQL, or a full refresh recorded as FULL). A client-requested copy has the
/// workload's identity when the workload has the logic the copy inherited
/// ([`copy_logic_matches`]; VERIFIED LIVE 2026-11-11,
/// explain_copy_logic_mismatch.jsonl `kf4`: a table over an append copy with
/// the same SQL and semantic extras is EXECUTION_TYPE_MISMATCH). Any other
/// row must also have recorded the workload's genuine upstream names: hosted
/// treats a changed upstream set as another identity (VERIFIED LIVE
/// 2026-11-11, explain_reason_kind_cells.jsonl `cv`, `vc`: client 2.0.11
/// reports no upstreams for a view, so the same SQL as a custom node over an
/// upstream is not the view's identity, #116). Selection
/// then also needs the target's latest build of the workload's own type
/// ([`latest_build_of_type`]).
pub(crate) fn execution_type_changed(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    latest: &ExecutionRow,
) -> bool {
    latest.execution_type != req.execution_type
        && (same_identity(req, evidence, latest) && same_upstream_names(evidence, latest)
            || is_copy_registration(latest) && copy_logic_matches(req, evidence, latest))
}

/// Bounded native #146 display characterization, not reuse qualification.
fn databricks_append_merge_transition(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    latest: &ExecutionRow,
) -> bool {
    crate::warehouse::Warehouse::from_dialect(&req.dialect)
        == crate::warehouse::Warehouse::Databricks
        && matches!((req.execution_type, latest.execution_type), (2, 3) | (3, 2))
        && !req.compare_unrendered_code
        && evidence
            .node_unique_id
            .as_deref()
            .is_some_and(|uid| !uid.is_empty() && latest.node_unique_id.as_deref() == Some(uid))
        && execution_type_changed(req, evidence, latest)
}

/// Whether `latest` has the workload's identity under another execution type
/// but recorded other genuine upstream names
/// ([`NoCandidate::UpstreamNamesChanged`]).
fn identity_under_other_type(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    latest: &ExecutionRow,
) -> bool {
    latest.execution_type != req.execution_type
        && same_identity(req, evidence, latest)
        && !same_upstream_names(evidence, latest)
}

/// Whether `row` recorded the workload's genuine upstream names: its inputs
/// other than the workload's own target, by exact physical name.
fn same_upstream_names(evidence: &ExecutionEvidence, row: &ExecutionRow) -> bool {
    use std::collections::BTreeSet;
    let names = |tables: &[InputTable]| -> BTreeSet<String> {
        tables
            .iter()
            .map(|t| crate::decision::physical_relation_key(&t.name).to_string())
            .filter(|name| *name != evidence.target_table)
            .collect()
    };
    names(&evidence.input_tables) == names(&row.input_tables)
}

/// The newest build in `history` (the target's builds in scope, newest
/// first) with the workload's identity and execution type.
pub(crate) fn latest_build_of_type(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    history: Vec<ExecutionRow>,
) -> Option<ExecutionRow> {
    history
        .into_iter()
        .find(|row| strict_match(req, evidence, row))
}

/// Selection when the latest row `latest` has the workload's identity under
/// another execution type and `build` is the target's latest build with the
/// workload's identity and type. VERIFIED LIVE 2026-11-10/11
/// (explain_execution_type_change.jsonl `ta`, `mi`;
/// explain_incremental_append_merge_history.jsonl entries 74-75, after a
/// RecordExecutions full refresh): hosted judges the submit against `build`,
/// whose Target relation the newer build modified. That build is then the
/// Explain anchor, as for any build rejected for its own target.
///
/// Only the latest row is ever reused. When nothing is against `build` it
/// would qualify but is not the latest, so the rebuild is the execution
/// type change against `latest` (conservative; uncharacterized).
pub(crate) fn for_model_over_build_of_type(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    latest: ExecutionRow,
    build: ExecutionRow,
) -> sqlx::Result<Selection> {
    debug_assert!(execution_type_changed(req, evidence, &latest));
    let transition_display =
        databricks_append_merge_transition(req, evidence, &latest).then(|| latest.clone());
    let build = build.into_checked(false)?;
    let mut selected = match model_rejection(req, evidence, &build) {
        Some(why) => Selection::of(
            Some(build),
            why,
            |_| Some(why),
            Workload::Model { unmatched: None },
        ),
        None => Selection::of(
            None,
            NoCandidate::ExecutionTypeChanged,
            |_| None,
            Workload::Model {
                unmatched: Some(latest),
            },
        ),
    };
    selected.transition_display = transition_display;
    Ok(selected)
}

/// Data tests: no physical target. `latest` is the latest confirmed run under
/// the test's history key (Snowflake, #200), or, under
/// `compare_unrendered_code` when the key has none, and on every other
/// Warehouse, the latest confirmed outcome for the test's node UID in its
/// project and physical scope ([`data_test_identity_matches`]). The target's
/// existence does not affect a data test's response, so it is not reported.
pub(crate) fn for_data_test(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    latest: Option<ExecutionRow>,
) -> sqlx::Result<Selection> {
    debug_assert_eq!(req.execution_type, ModelExecutionType::DbtDataTest as i32);
    let Some(latest) = latest else {
        return Ok(Selection::FIRST_BUILD);
    };
    let identified = if data_test_identity_matches(evidence, &latest) {
        Some(latest.into_checked(true)?)
    } else {
        None
    };
    let rerun = identified
        .as_ref()
        .map(|row| rerun_evidence(req, evidence, row))
        .unwrap_or_default();
    Ok(Selection::of(
        identified,
        NoCandidate::IdentityMismatch,
        |prev| {
            if !sql_qualifies(req, evidence) {
                Some(NoCandidate::FailedReuseQualification)
            } else if !reusable_test_results(prev.execution_results.as_ref()) {
                Some(NoCandidate::IncompleteCachedTestResults)
            } else {
                None
            }
        },
        Workload::DataTest,
    )
    .with_rerun_evidence(rerun))
}

/// Seeds compare values and seed configuration identity, then require the
/// current target evidence to be no newer than the recorded build. Their
/// inputs stay empty for the pure decision.
pub(crate) fn for_seed(
    req: &qc::SubmitValuesRequest,
    evidence: &ExecutionEvidence,
    current: Option<ExecutionRow>,
) -> sqlx::Result<Selection> {
    let Some(current) = current else {
        return Ok(Selection::FIRST_BUILD);
    };
    let identified = if current.execution_type == ModelExecutionType::Values as i32
        && current.values_hash.as_deref() == Some(req.values_hash.as_str())
        && current.node_sql_hash.is_some()
        && current.node_sql_hash == evidence.node_sql_hash
    {
        Some(current.into_checked(true)?)
    } else {
        None
    };
    Ok(Selection::of(
        identified,
        NoCandidate::IdentityMismatch,
        |prev| {
            if req.values_hash.is_empty() {
                Some(NoCandidate::FailedReuseQualification)
            } else if req.last_modified_epoch.is_none() {
                // VERIFIED LIVE (lag_seed_upstream G): a seed reloaded into a
                // missing table "did not exist".
                Some(NoCandidate::OwnTargetMissing)
            } else if !not_modified_since(req.last_modified_epoch, prev.last_modified_epoch) {
                Some(NoCandidate::OwnTargetChangedOrMissing)
            } else {
                None
            }
        },
        Workload::Seed,
    ))
}

/// Whether an OLDER build of the target (overwritten since) has the workload's
/// strict identity and the workload qualifies for reuse: the first step of
/// reporting a rebuild whose earlier build would still be reusable
/// (LATEST_QUERY_HASH_NOT_MATCH). Freshness is judged by the caller.
pub(crate) fn older_build_matches(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    row: &ExecutionRow,
) -> bool {
    row.clone_source_table.is_none()
        && sql_qualifies(req, evidence)
        && strict_match(req, evidence, row)
}

/// [`older_build_matches`] for a seed.
pub(crate) fn older_seed_matches(
    req: &qc::SubmitValuesRequest,
    evidence: &ExecutionEvidence,
    row: &ExecutionRow,
) -> bool {
    row.execution_type == ModelExecutionType::Values as i32
        && !req.values_hash.is_empty()
        && row.values_hash.as_deref() == Some(req.values_hash.as_str())
        && row.node_sql_hash.is_some()
        && row.node_sql_hash == evidence.node_sql_hash
}

use crate::copy_lifecycle::{adopt_current_inputs, copy_logic_matches, is_copy_registration};

/// A data test's identity. A keyed test (Snowflake, #200) has a run's
/// identity when the run has its history key, or, under
/// `compare_unrendered_code`, its template identity on a run recorded with
/// a key (the unique id's latest run; a generic test's template is shared
/// by every test of its kind, so that alternative stays scoped by unique
/// id). Runs recorded before the key never match. Every other test keeps
/// the dual rendered/template identity ([`identity_matches`]).
fn data_test_identity_matches(evidence: &ExecutionEvidence, row: &ExecutionRow) -> bool {
    match evidence.data_test_key.as_deref() {
        Some(key) => {
            row.data_test_key.as_deref() == Some(key)
                || (evidence.node_template_hash.is_some()
                    && row.data_test_key.is_some()
                    && row.node_template_hash == evidence.node_template_hash)
        }
        None => identity_matches(evidence, row),
    }
}

/// Rendered identity, or (only in `compare_unrendered_code` mode, when the
/// submit carries a template hash) the unrendered-template identity. A
/// template-mode node rebuilds only when BOTH change (verified live; see
/// `template_comparison.rs`, and review #2 finding 6 follow-up for data tests).
fn identity_matches(evidence: &ExecutionEvidence, row: &ExecutionRow) -> bool {
    row.node_sql_hash == evidence.node_sql_hash
        || (evidence.node_template_hash.is_some()
            && row.node_template_hash == evidence.node_template_hash)
}

/// Same target, same execution type and evidenced dialect identity.
/// A row whose recorded schema-abstracted logic differs from the submit's is
/// rejected even when the SQL hash matches: that is the upgrade invalidation
/// for rows written under an older namespace identity.
fn strict_match(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    row: &ExecutionRow,
) -> bool {
    row.execution_type == req.execution_type && same_identity(req, evidence, row)
}

/// [`strict_match`] without the execution type.
fn same_identity(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    row: &ExecutionRow,
) -> bool {
    let ordinary_databricks = crate::warehouse::Warehouse::from_dialect(&req.dialect)
        == crate::warehouse::Warehouse::Databricks
        && crate::copy_lifecycle::ordinary(row);
    let logic_compatible = ordinary_databricks
        || match (
            &row.node_namespace_logic_hash,
            &evidence.node_namespace_logic_hash,
        ) {
            (Some(recorded), Some(current)) => recorded == current,
            _ => true,
        };
    identity_matches(evidence, row)
        && crate::copy_lifecycle::reusable(evidence, row)
        && logic_compatible
        // Alias admission requires the rendered identity itself to match; a
        // template-only match cannot equate uncharacterized dialect options.
        && (row.dialect == req.dialect
            || row.node_sql_hash == evidence.node_sql_hash
                && crate::warehouse::Warehouse::rendered_dialect_aliases(&row.dialect, &req.dialect))
}

/// Only CLONE-ORIGIN rows are adopted across environments. Ordinary rows are
/// not: that would let a genuine resolution-context change (e.g.
/// `default_schema`) skip, which the hosted service rebuilds. Broader
/// cross-environment reuse without a clone is a separate provenance feature
/// (see correctness-handoff.md "Open hosted questions").
///
/// A client-requested copy is also adopted when the submit, read in the copy
/// source's schema by the shared copy lifecycle, has the logic the copy
/// inherited.
fn adoptable(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    row: &ExecutionRow,
) -> bool {
    row.execution_type == req.execution_type && copy_logic_matches(req, evidence, row)
}

/// Reuse qualification shared by models and data tests. With Volatile
/// evaluation requested, SQL whose root or query dependency definitions call
/// a Volatile function (per the Warehouse's detector) builds instead of
/// claiming reuse; deterministic SQL reuses as without it (#191 cell 3,
/// `explain_volatile_evaluation.jsonl` entry 31; #194). Empty SQL or
/// template is likewise insufficient evidence.
fn sql_qualifies(req: &qc::SubmitEnrichedSqlRequest, evidence: &ExecutionEvidence) -> bool {
    (!crate::volatile_evaluation::requested(req) || !calls_volatile_function(req))
        && sql_evidence_complete(req, evidence)
}

/// Maintainer decision 1 on #201 (#206): whether `req` is a submit to a
/// Warehouse that reuses volatile SQL by its unrendered code
/// ([`Warehouse::reuses_volatile_sql_by_unrendered_code`], Snowflake),
/// compared by its unrendered code (`compare_unrendered_code`), whose
/// SQL calls a Volatile function with Volatile evaluation requested. Against
/// a build with the same template identity, a model is then not refused at
/// Reuse qualification for its volatile SQL; the ordinary reuse rules of the
/// dual rendered/template identity apply. Hosted reuses it and explains it by
/// its unrendered code with no volatile lines (`explain_volatile_cells.jsonl`
/// `ecu`, entries 140 and 198). A changed template (the compiled SQL
/// decides), a data test, `compare_unrendered_code` off and every other
/// Warehouse keep the refusal (uncaptured).
///
/// [`Warehouse::reuses_volatile_sql_by_unrendered_code`]: crate::warehouse::Warehouse::reuses_volatile_sql_by_unrendered_code
pub(crate) fn volatile_sql_compared_by_unrendered_code(req: &qc::SubmitEnrichedSqlRequest) -> bool {
    req.compare_unrendered_code
        && crate::warehouse::Warehouse::from_dialect(&req.dialect)
            .reuses_volatile_sql_by_unrendered_code()
        && crate::volatile_evaluation::requested(req)
        && calls_volatile_function(req)
}

/// Whether a Reuse qualification refusal of `req` is the refusal of its
/// volatile SQL alone, compared by its rendered SQL: Volatile evaluation is
/// requested, the root SQL or a query dependency definition calls a
/// Volatile function, `compare_unrendered_code` is off, and the rest of the
/// evidence Reuse qualification requires is complete. Every refusal hosted
/// was captured rendering values for is one (`explain_volatile_cells.jsonl`,
/// `explain_volatile_evaluation.jsonl`); the one capture under
/// `compare_unrendered_code` is a reuse (`ecu`, entry 140). Selects the
/// Volatile evaluation cell only; it decides nothing.
pub(crate) fn refuses_volatile_sql_alone(req: &qc::SubmitEnrichedSqlRequest) -> bool {
    !req.compare_unrendered_code
        && crate::volatile_evaluation::requested(req)
        && calls_volatile_function(req)
        && rendered_sql_evidence_complete(req)
}

/// The workload evidence Reuse qualification requires whatever the SQL
/// calls: complete query dependencies, SQL, and a template body when the
/// template identity is used.
fn sql_evidence_complete(req: &qc::SubmitEnrichedSqlRequest, evidence: &ExecutionEvidence) -> bool {
    rendered_sql_evidence_complete(req)
        && (!req.compare_unrendered_code
            || evidence.node_template_hash.is_none()
            || evidence
                .node_body_hash
                .as_ref()
                .is_some_and(|s| !s.is_empty()))
}

/// Whether the root SQL or a query dependency definition calls a Volatile
/// function. Warehouses without a general detector report SQL as volatile.
/// The checked Databricks FULL shape is admitted by model Reuse qualification.
fn calls_volatile_function(req: &qc::SubmitEnrichedSqlRequest) -> bool {
    crate::warehouse::Warehouse::from_dialect(&req.dialect).has_volatile_sql(
        &req.sql,
        req.query_dependencies.iter().map(|d| d.query.as_str()),
    )
}

/// [`sql_evidence_complete`] without the template body: complete query
/// dependencies and SQL.
fn rendered_sql_evidence_complete(req: &qc::SubmitEnrichedSqlRequest) -> bool {
    complete_dependencies(req) && !req.sql.trim().is_empty()
}

fn complete_dependencies(req: &qc::SubmitEnrichedSqlRequest) -> bool {
    let mut names = std::collections::HashSet::new();
    req.query_dependencies
        .iter()
        .all(|d| !d.name.trim().is_empty() && !d.query.trim().is_empty() && names.insert(&d.name))
}

/// What the submit reports about the node's own target, against the build.
enum OwnTarget {
    /// Present exactly once, with a known epoch no later than the build's
    /// (or `ignore_external_modifications`).
    AsBuilt,
    /// Present once with a known epoch later than the build's: modified after
    /// the cached execution (VERIFIED LIVE 2026-11-10, even 1s later).
    Modified,
    /// Not reported: the table does not exist (VERIFIED LIVE 2026-11-10).
    Missing,
    /// Duplicated, of unknown epoch, or another target.
    Ambiguous,
}

/// What can be established about a rerun of a workload's ordinary build.
/// Used for the bounded Databricks producer transition and rendering cells.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RerunEvidence {
    #[default]
    Unverified,
    Unchanged,
    /// Its Target relation changed with one unchanged input.
    TargetChanged,
    /// Its sole input was externally modified with the Target relation as built.
    InputModifiedExternally,
}

impl RerunEvidence {
    /// A change to the Target relation or the sole input, which stands only
    /// when the Warehouse establishes it
    /// (`Warehouse::changed_volatile_rerun`).
    pub(crate) fn is_changed(self) -> bool {
        matches!(self, Self::TargetChanged | Self::InputModifiedExternally)
    }
}

/// Compare the Target relation and upstreams with the ordinary build whose
/// identity matched (#198, #213).
fn rerun_evidence(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    prev: &ExecutionRow,
) -> RerunEvidence {
    use crate::decision::FreshnessEvidence;

    if !crate::copy_lifecycle::ordinary(prev) {
        return RerunEvidence::Unverified;
    }
    let data_test = req.execution_type == ModelExecutionType::DbtDataTest as i32;
    let own = if data_test {
        if !reusable_test_results(prev.execution_results.as_ref()) {
            return RerunEvidence::Unverified;
        }
        OwnTarget::AsBuilt
    } else {
        own_target(req, prev)
    };
    let inputs = crate::decision::FreshnessInputs {
        execution_type: req.execution_type,
        input_tables: &evidence.input_tables,
        target_table: req.target_table.as_deref().filter(|t| !t.is_empty()),
    };
    let freshness = crate::decision::upstream_freshness(inputs, prev);
    let upstreams_unchanged = match freshness.evidence {
        FreshnessEvidence::NoUpstreams => true,
        FreshnessEvidence::Evaluated => freshness.upstreams.iter().all(|u| {
            !u.unprovable && !u.producer_changed && u.current_epoch == Some(u.anchor_epoch)
        }),
        FreshnessEvidence::DependencySetChanged | FreshnessEvidence::NoBuildAnchor => false,
    };
    match (own, upstreams_unchanged, freshness.upstreams.as_slice()) {
        (OwnTarget::AsBuilt, true, _) => RerunEvidence::Unchanged,
        (OwnTarget::Modified | OwnTarget::Missing, true, [_]) => RerunEvidence::TargetChanged,
        (OwnTarget::AsBuilt, false, _)
            if sole_upstream_modified_externally(&freshness, prev, &evidence.input_tables) =>
        {
            RerunEvidence::InputModifiedExternally
        }
        _ => RerunEvidence::Unverified,
    }
}

/// Whether the one genuine upstream drifted since `prev` because it was
/// modified externally after `prev` read it from a recorded build
/// ([`InputTable::modified_externally_since`]). `freshness` is the upstream
/// freshness of `current`, the inputs read now, against `prev`. The #213
/// rerun cell and its Explain both stand on this situation.
pub(crate) fn sole_upstream_modified_externally(
    freshness: &crate::decision::UpstreamFreshness,
    prev: &ExecutionRow,
    current: &[InputTable],
) -> bool {
    let [upstream] = freshness.upstreams.as_slice() else {
        return false;
    };
    let now = current.iter().find(|t| t.name == upstream.name);
    let recorded = prev.input_tables.iter().find(|t| t.name == upstream.name);
    freshness.evidence == crate::decision::FreshnessEvidence::Evaluated
        && !upstream.unprovable
        && upstream.drifted
        && now
            .zip(recorded)
            .is_some_and(|(now, recorded)| now.modified_externally_since(recorded))
}

/// Own-target existence and external edits, checked separately from upstream
/// freshness. Missing or ambiguous target evidence cannot establish that the
/// object exists.
fn own_target(req: &qc::SubmitEnrichedSqlRequest, prev: &ExecutionRow) -> OwnTarget {
    let Some(target) = req.target_table.as_deref().filter(|s| !s.is_empty()) else {
        return OwnTarget::Ambiguous;
    };
    if prev.target_table != target {
        return OwnTarget::Ambiguous;
    }
    let own: Vec<_> = req.tables.iter().filter(|t| t.name == target).collect();
    if own.is_empty() {
        return OwnTarget::Missing;
    }
    if own.len() != 1 || own[0].last_modified_epoch.is_none() || prev.last_modified_epoch.is_none()
    {
        return OwnTarget::Ambiguous;
    }
    if req.ignore_external_modifications
        || not_modified_since(own[0].last_modified_epoch, prev.last_modified_epoch)
    {
        OwnTarget::AsBuilt
    } else {
        OwnTarget::Modified
    }
}

/// The target's current epoch is known and no newer than the recorded build:
/// no external write since. Unknown epochs never prove that.
fn not_modified_since(current: Option<i64>, recorded: Option<i64>) -> bool {
    matches!((current, recorded), (Some(current), Some(recorded)) if current <= recorded)
}

/// Cached data-test results are complete only with numeric failures and both
/// severity flags.
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use prost::Message;
    use qc::value::Kind;

    const TARGET: &str = "\"DB\".\"DEV\".\"M\"";
    const UPSTREAM: &str = "\"DB\".\"DEV\".\"UP\"";
    const BUILT: i64 = 1_000;

    fn table(name: &str, epoch: i64) -> qc::TableModifiedInfo {
        qc::TableModifiedInfo {
            name: name.into(),
            last_modified_epoch: Some(epoch),
        }
    }

    /// A Snowflake model submit whose own target exists at the recorded epoch.
    fn model_req(sql: &str) -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            execution_type: 1,
            dialect: "snowflake".into(),
            sql: sql.into(),
            target_table: Some(TARGET.into()),
            table_namespace: Some("ns".into()),
            tolerate_nondeterminism: true,
            tables: vec![table(TARGET, BUILT), table(UPSTREAM, 500)],
            ..Default::default()
        }
    }

    fn evidence(req: &qc::SubmitEnrichedSqlRequest) -> ExecutionEvidence {
        ExecutionEvidence::sql_submit("o", req)
    }

    fn results(complete: bool) -> qc::Struct {
        let mut fields = std::collections::BTreeMap::new();
        fields.insert(
            "failures".to_string(),
            qc::Value {
                kind: Some(Kind::IntValue(0)),
            },
        );
        fields.insert(
            "should_error".to_string(),
            qc::Value {
                kind: Some(Kind::BoolValue(false)),
            },
        );
        if complete {
            fields.insert(
                "should_warn".to_string(),
                qc::Value {
                    kind: Some(Kind::BoolValue(false)),
                },
            );
        }
        qc::Struct {
            fields: fields.into_iter().collect(),
        }
    }

    /// The latest row as recorded by a confirmed build of exactly `ev`.
    fn recorded(ev: &ExecutionEvidence) -> ExecutionRow {
        ExecutionRow {
            id: 7,
            org_id: "o".into(),
            target_table: ev.target_table.clone(),
            execution_type: ev.execution_type,
            node_body_hash: ev.node_body_hash.clone(),
            node_sql_hash: ev.node_sql_hash.clone(),
            node_namespace_logic_hash: ev.node_namespace_logic_hash.clone(),
            node_template_hash: ev.node_template_hash.clone(),
            values_hash: ev.values_hash.clone(),
            dialect: ev.dialect.clone(),
            table_namespace: ev.table_namespace.clone(),
            node_unique_id: ev.node_unique_id.clone(),
            last_modified_epoch: Some(BUILT),
            execution_runtime_ms: Some(42),
            status: "confirmed".into(),
            request_id: "r".into(),
            clone_source_table: None,
            data_test_key: ev.data_test_key.clone(),
            ..ExecutionRow::default()
        }
        .with_encoded_evidence(
            serde_json::to_value(&ev.input_tables).unwrap(),
            Some(results(true).encode_to_vec()),
        )
    }

    /// A clone-origin row as written by RegisterClone: inherited logic, no
    /// rendered identity, no recorded inputs, no results.
    fn clone_origin(ev: &ExecutionEvidence) -> ExecutionRow {
        ExecutionRow {
            node_sql_hash: None,
            node_body_hash: None,
            input_tables: Vec::new(),
            execution_results: None,
            clone_source_table: Some("\"DB\".\"PROD\".\"M\"".into()),
            ..recorded(ev)
        }
    }

    fn model(req: &qc::SubmitEnrichedSqlRequest, row: Option<ExecutionRow>) -> Selection {
        for_model(req, &evidence(req), row).expect("no decode error")
    }

    // ---- models: identity -------------------------------------------------

    #[test]
    fn first_build_has_no_candidate_and_no_prior_build() {
        let req = model_req("select id from DB.DEV.UP");
        let s = model(&req, None);
        assert!(s.candidate.is_none());
        assert!(!s.target_has_prior_build);
    }

    #[test]
    fn strict_match_serves_the_recorded_row_with_results() {
        let req = model_req("select id from DB.DEV.UP");
        let row = recorded(&evidence(&req));
        let s = model(&req, Some(row));
        let c = s.candidate.expect("strict match qualifies");
        assert_eq!(c.id, 7);
        assert_eq!(c.execution_results, Some(results(true)));
        assert!(s.target_has_prior_build);
    }

    #[test]
    fn latest_row_that_does_not_match_still_reports_a_prior_build() {
        let req = model_req("select id from DB.DEV.UP");
        let mut row = recorded(&evidence(&req));
        row.node_sql_hash = Some("other".into());
        let s = model(&req, Some(row));
        assert!(s.candidate.is_none());
        assert!(s.target_has_prior_build, "a rebuild, not a first build");
    }

    #[test]
    fn strict_match_requires_same_execution_type_and_exact_dialect() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let mut other_type = recorded(&ev);
        other_type.execution_type = 3;
        assert!(model(&req, Some(other_type)).candidate.is_none());
        let mut other_options = recorded(&ev);
        other_options.dialect = "snowflake, mode=x".into();
        assert!(model(&req, Some(other_options)).candidate.is_none());
    }

    #[test]
    fn template_mode_matches_on_template_when_rendered_sql_differs() {
        let mut req = model_req("select id from DB.DEV.UP");
        req.compare_unrendered_code = true;
        req.dbt_node_state = Some(qc::DbtNodeState {
            node_body_hash: Some("body".into()),
            ..Default::default()
        });
        let ev = evidence(&req);
        assert!(ev.node_template_hash.is_some());
        let mut row = recorded(&ev);
        row.node_sql_hash = Some("rendered-differently".into());
        assert!(model(&req, Some(row.clone())).candidate.is_some());

        // Outside template mode the same row is a miss.
        req.compare_unrendered_code = false;
        assert!(model(&req, Some(row)).candidate.is_none());
    }

    #[test]
    fn stale_namespace_logic_invalidates_a_strict_match() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        assert!(ev.node_namespace_logic_hash.is_some());
        let mut stale = recorded(&ev);
        stale.node_namespace_logic_hash = Some("older-identity".into());
        assert!(model(&req, Some(stale)).candidate.is_none());

        // NULL on the recorded side falls back to the strict hash alone.
        let mut unknown = recorded(&ev);
        unknown.node_namespace_logic_hash = None;
        assert!(model(&req, Some(unknown)).candidate.is_some());
    }

    // ---- models: clone-origin adoption -------------------------------------

    #[test]
    fn clone_origin_row_with_same_logic_is_adopted_with_current_inputs() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let c = model(&req, Some(clone_origin(&ev)))
            .candidate
            .expect("adopted");
        assert_eq!(c.input_tables, ev.input_tables);
        assert!(c.execution_results.is_none());
    }

    #[test]
    fn adoption_keeps_recorded_inputs_when_present() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let mut row = clone_origin(&ev);
        row.input_tables = vec![InputTable {
            name: UPSTREAM.into(),
            last_modified_epoch: Some(1),
            ..Default::default()
        }];
        let c = model(&req, Some(row)).candidate.expect("adopted");
        assert_eq!(c.input_tables.len(), 1);
        assert_eq!(c.input_tables[0].last_modified_epoch, Some(1));
    }

    /// Q20 regression: a copy made by the SQL-submit fallback recorded that
    /// submit's inputs. An EMPTY list means "no upstreams", so a later submit
    /// that has upstreams must see a dependency-set change and rebuild, rather
    /// than having its current inputs substituted (which could SKIP).
    #[test]
    fn sql_submit_copy_with_no_recorded_upstreams_rebuilds_when_upstreams_appear() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let mut row = clone_origin(&ev);
        row.node_sql_hash = Some("rendered-in-another-schema".into());
        let c = model(&req, Some(row)).candidate.expect("adopted");
        assert!(
            c.input_tables.is_empty(),
            "recorded inputs kept as recorded"
        );

        let ctx = crate::decision::SubmitContext {
            execution_type: req.execution_type,
            input_tables: &ev.input_tables,
            sql_reads_relation: None,
            target_table: req.target_table.as_deref(),
            stale_upstream_policy: crate::decision::StaleUpstreamPolicy::Any,
            target_has_prior_build: true,
            why_no_candidate: None,
            tolerance_ms: 0,
            build_age_ms: None,
            own_target_absent: false,
        };
        assert!(matches!(
            crate::decision::decide(&ctx, Some(&c)),
            crate::decision::Verdict::Execute { .. }
        ));
    }

    #[test]
    fn ordinary_rows_are_never_adopted_across_environments() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let mut row = clone_origin(&ev);
        row.clone_source_table = None;
        assert!(model(&req, Some(row)).candidate.is_none());
    }

    #[test]
    fn adoption_requires_matching_logic_identity() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let mut different = clone_origin(&ev);
        different.node_namespace_logic_hash = Some("different".into());
        assert!(model(&req, Some(different)).candidate.is_none());
        let mut unknown = clone_origin(&ev);
        unknown.node_namespace_logic_hash = None;
        assert!(model(&req, Some(unknown)).candidate.is_none());
    }

    /// The dev-clone pre-step's copy of a build that read a fixed `PROD`
    /// source (VERIFIED LIVE 2026-11-11, explain_post_copy_identity.jsonl
    /// `c93sa`): the post-copy submit has the build's SQL text under the dev
    /// default schema. Read in the copy source's schema it has the copy's
    /// logic, so the copy is adopted (#93). A genuine SQL change is not.
    #[test]
    fn copy_registration_is_adopted_by_a_submit_with_the_source_builds_logic() {
        let sql = "select id from DB.PROD.SRC";
        let prod = qc::SubmitEnrichedSqlRequest {
            target_table: Some("\"DB\".\"PROD\".\"M\"".into()),
            default_schema: Some("prod".into()),
            ..model_req(sql)
        };
        let dev = |sql: &str| qc::SubmitEnrichedSqlRequest {
            default_schema: Some("dev".into()),
            tables: vec![table(TARGET, BUILT), table("\"DB\".\"PROD\".\"SRC\"", 500)],
            ..model_req(sql)
        };
        let copy = ExecutionRow {
            target_table: TARGET.into(),
            ..clone_origin(&evidence(&prod))
        };
        let same = dev(sql);
        assert_ne!(
            evidence(&same).node_namespace_logic_hash,
            copy.node_namespace_logic_hash,
            "the request-relative abstraction alone does not match"
        );
        let s = model(&same, Some(copy.clone()));
        assert!(s.candidate.is_some(), "adopted: {:?}", s.why_no_candidate);

        let s = model(&dev("select id, 1 as x from DB.PROD.SRC"), Some(copy));
        assert!(s.candidate.is_none());
        assert_eq!(s.why_no_candidate, Some(NoCandidate::CopyLogicMismatch));
    }

    /// A submit of another node kind over a client-requested copy (VERIFIED
    /// LIVE 2026-11-11, explain_copy_logic_mismatch.jsonl `kf4`, `kv4`,
    /// `kc4`): with the copy's logic, a node kind change (EXECUTION_TYPE_MISMATCH,
    /// judged against the copy); with other logic, including other semantic
    /// extras, a copy logic mismatch. Never reused.
    #[test]
    fn another_node_kind_over_a_copy_registration_is_an_execution_type_change() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let copy = ExecutionRow {
            execution_type: ModelExecutionType::Append as i32,
            ..clone_origin(&ev)
        };
        assert!(execution_type_changed(&req, &ev, &copy));
        let s = model(&req, Some(copy.clone()));
        assert!(s.candidate.is_none());
        assert_eq!(s.why_no_candidate, Some(NoCandidate::ExecutionTypeChanged));
        assert_eq!(s.type_changed.map(|r| r.id), Some(copy.id));

        let view = qc::SubmitEnrichedSqlRequest {
            execution_type: ModelExecutionType::View as i32,
            semantic_extras: [("lookback".to_string(), "1".to_string())].into(),
            ..req
        };
        let s = model(&view, Some(copy.clone()));
        assert!(!execution_type_changed(&view, &evidence(&view), &copy));
        assert_eq!(s.why_no_candidate, Some(NoCandidate::CopyLogicMismatch));
        assert_eq!(s.unmatched.map(|r| r.id), Some(copy.id));
    }

    /// Rehoming applies only to a client-requested copy: a Result copy made on
    /// submit recorded that submit's own identity.
    #[test]
    fn only_a_copy_registration_is_rehomed() {
        let sql = "select id from DB.PROD.SRC";
        let prod = qc::SubmitEnrichedSqlRequest {
            target_table: Some("\"DB\".\"PROD\".\"M\"".into()),
            default_schema: Some("prod".into()),
            ..model_req(sql)
        };
        let dev = qc::SubmitEnrichedSqlRequest {
            default_schema: Some("dev".into()),
            tables: vec![table(TARGET, BUILT), table("\"DB\".\"PROD\".\"SRC\"", 500)],
            ..model_req(sql)
        };
        let mut submit_copy = ExecutionRow {
            target_table: TARGET.into(),
            ..clone_origin(&evidence(&prod))
        };
        submit_copy.node_sql_hash = Some("rendered-in-another-schema".into());
        assert!(model(&dev, Some(submit_copy)).candidate.is_none());
    }

    #[test]
    fn qualification_rejects_insufficient_workload_evidence() {
        let base = model_req("select id from DB.DEV.UP");
        type Mutation = fn(&mut qc::SubmitEnrichedSqlRequest);
        let cases: Vec<(&str, Mutation)> = vec![
            ("volatile evaluation of volatile SQL requested", |r| {
                r.tolerate_nondeterminism = false;
                r.sql = "select id, random() from DB.DEV.UP".into();
            }),
            (
                "volatile evaluation of a volatile dependency requested",
                |r| {
                    r.tolerate_nondeterminism = false;
                    r.query_dependencies = vec![qc::QueryDependency {
                        name: "d".into(),
                        query: "select current_timestamp() as ts".into(),
                        ..Default::default()
                    }];
                },
            ),
            (
                "volatile evaluation requested without a checked detector",
                |r| {
                    r.tolerate_nondeterminism = false;
                    r.dialect = "postgres".into();
                },
            ),
            ("own target missing", |r| {
                r.tables.retain(|t| t.name != TARGET)
            }),
            ("own target duplicated", |r| {
                r.tables.push(table(TARGET, BUILT))
            }),
            ("own target epoch unknown", |r| {
                r.tables[0].last_modified_epoch = None
            }),
            ("externally edited", |r| {
                r.tables[0].last_modified_epoch = Some(BUILT + 1)
            }),
            ("duplicate dependency", |r| {
                let dep = qc::QueryDependency {
                    name: "d".into(),
                    query: "select 1".into(),
                    ..Default::default()
                };
                r.query_dependencies = vec![dep.clone(), dep];
            }),
            ("empty dependency definition", |r| {
                r.query_dependencies = vec![qc::QueryDependency {
                    name: "d".into(),
                    ..Default::default()
                }]
            }),
        ];
        for (why, mutate) in cases {
            let mut req = base.clone();
            mutate(&mut req);
            // The recorded row matches this exact request, so only
            // qualification can reject it.
            let row = recorded(&evidence(&req));
            assert!(model(&req, Some(row)).candidate.is_none(), "{why}");
        }
    }

    /// #191 cell 3 (`explain_volatile_evaluation.jsonl` entry 31): hosted
    /// reuses deterministic SQL with evaluation requested.
    #[test]
    fn deterministic_sql_with_volatile_evaluation_requested_qualifies() {
        let mut req = model_req("select id from DB.DEV.UP");
        req.tolerate_nondeterminism = false;
        req.query_dependencies = vec![qc::QueryDependency {
            name: "d".into(),
            query: "select 1 as id".into(),
            ..Default::default()
        }];
        let row = recorded(&evidence(&req));
        assert!(model(&req, Some(row)).candidate.is_some());
    }

    #[test]
    fn qualification_rejects_empty_sql_and_empty_template_body() {
        let empty = model_req("  ");
        assert!(model(&empty, Some(recorded(&evidence(&empty))))
            .candidate
            .is_none());

        let mut template = model_req("select id from DB.DEV.UP");
        template.compare_unrendered_code = true;
        template.dbt_node_state = Some(qc::DbtNodeState {
            node_body_hash: Some(String::new()),
            ..Default::default()
        });
        let row = recorded(&evidence(&template));
        assert!(model(&template, Some(row)).candidate.is_none());
    }

    #[test]
    fn ignore_external_modifications_accepts_a_newer_own_target() {
        let mut req = model_req("select id from DB.DEV.UP");
        req.tables[0].last_modified_epoch = Some(BUILT + 1);
        req.ignore_external_modifications = true;
        let row = recorded(&evidence(&req));
        assert!(model(&req, Some(row)).candidate.is_some());
    }

    // ---- models: undecoded evidence ----------------------------------------

    #[test]
    fn corrupt_results_fail_only_a_result_serving_match() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let corrupt = vec![0xff, 0xff, 0xff];
        let inputs = serde_json::to_value(&ev.input_tables).unwrap();

        let strict = recorded(&ev).with_encoded_evidence(inputs, Some(corrupt.clone()));
        assert!(for_model(&req, &ev, Some(strict)).is_err());

        let adopted =
            clone_origin(&ev).with_encoded_evidence(serde_json::json!([]), Some(corrupt.clone()));
        assert!(for_model(&req, &ev, Some(adopted))
            .unwrap()
            .candidate
            .is_some());

        let mut miss = recorded(&ev);
        miss.node_sql_hash = Some("other".into());
        let miss = miss.with_encoded_evidence(serde_json::json!("not a list"), Some(corrupt));
        assert!(for_model(&req, &ev, Some(miss))
            .unwrap()
            .candidate
            .is_none());
    }

    // ---- data tests ---------------------------------------------------------

    /// A model calling a Volatile function, with evaluation requested.
    fn volatile_evaluation_req() -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            tolerate_nondeterminism: false,
            ..model_req("select id, current_timestamp() as ts from DB.DEV.UP")
        }
    }

    fn data_test_req() -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            execution_type: ModelExecutionType::DbtDataTest as i32,
            target_table: None,
            tables: vec![table(UPSTREAM, 500)],
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: "test.p.t".into(),
                ..Default::default()
            }),
            ..model_req("select * from DB.DEV.UP where id is null")
        }
    }

    #[test]
    fn data_test_reuses_complete_results_only() {
        let req = data_test_req();
        let ev = evidence(&req);
        let complete = for_data_test(&req, &ev, Some(recorded(&ev))).unwrap();
        assert!(complete.candidate.is_some());
        assert!(!complete.target_has_prior_build);

        let mut partial = recorded(&ev);
        partial.execution_results = Some(results(false));
        assert!(for_data_test(&req, &ev, Some(partial))
            .unwrap()
            .candidate
            .is_none());

        // A Snowflake data test is identified by its history key (#200): a
        // run under another key, or recorded before the key, is not one.
        assert!(ev.data_test_key.is_some());
        let mut changed = recorded(&ev);
        changed.data_test_key = Some("other".into());
        assert!(for_data_test(&req, &ev, Some(changed))
            .unwrap()
            .candidate
            .is_none());
        let mut legacy = recorded(&ev);
        legacy.data_test_key = None;
        assert!(for_data_test(&req, &ev, Some(legacy))
            .unwrap()
            .candidate
            .is_none());
        let mut other_sql = recorded(&ev);
        other_sql.node_sql_hash = Some("other".into());
        assert!(
            for_data_test(&req, &ev, Some(other_sql))
                .unwrap()
                .candidate
                .is_some(),
            "the rendered identity holds the default schema; the key does not"
        );

        // Other Warehouses keep the rendered identity.
        let mut duckdb = req.clone();
        duckdb.dialect = "duckdb".into();
        let ev = evidence(&duckdb);
        assert_eq!(ev.data_test_key, None);
        let mut changed = recorded(&ev);
        changed.node_sql_hash = Some("other".into());
        assert!(for_data_test(&duckdb, &ev, Some(changed))
            .unwrap()
            .candidate
            .is_none());
    }

    #[test]
    fn data_test_has_no_own_target_to_check() {
        // Unlike a model, a data test qualifies without own-target evidence.
        let req = data_test_req();
        let ev = evidence(&req);
        assert!(req.tables.iter().all(|t| t.name != TARGET));
        assert!(for_data_test(&req, &ev, Some(recorded(&ev)))
            .unwrap()
            .candidate
            .is_some());
    }

    // ---- seeds --------------------------------------------------------------

    fn seed_req(epoch: Option<i64>) -> qc::SubmitValuesRequest {
        qc::SubmitValuesRequest {
            target_table: TARGET.into(),
            dialect: "snowflake".into(),
            values_hash: "values".into(),
            table_namespace: Some("ns".into()),
            last_modified_epoch: epoch,
            ..Default::default()
        }
    }

    fn seed_evidence(req: &qc::SubmitValuesRequest) -> ExecutionEvidence {
        ExecutionEvidence::values_submit("o", req)
    }

    #[test]
    fn seed_reuse_requires_identity_and_unchanged_target() {
        let req = seed_req(Some(BUILT));
        let ev = seed_evidence(&req);
        let s = for_seed(&req, &ev, Some(recorded(&ev))).unwrap();
        assert!(s.candidate.is_some());
        assert!(s.target_has_prior_build);

        let newer = seed_req(Some(BUILT + 1));
        assert!(for_seed(&newer, &ev, Some(recorded(&ev)))
            .unwrap()
            .candidate
            .is_none());
        let unknown = seed_req(None);
        assert!(for_seed(&unknown, &ev, Some(recorded(&ev)))
            .unwrap()
            .candidate
            .is_none());

        let mut other_values = recorded(&ev);
        other_values.values_hash = Some("changed".into());
        let s = for_seed(&req, &ev, Some(other_values)).unwrap();
        assert!(s.candidate.is_none());
        assert!(s.target_has_prior_build);

        let none = for_seed(&req, &ev, None).unwrap();
        assert!(none.candidate.is_none() && !none.target_has_prior_build);
    }

    #[test]
    fn seed_with_empty_values_hash_never_reuses() {
        let mut req = seed_req(Some(BUILT));
        req.values_hash = String::new();
        let ev = seed_evidence(&req);
        let mut row = recorded(&ev);
        row.values_hash = Some(String::new());
        assert!(for_seed(&req, &ev, Some(row)).unwrap().candidate.is_none());
    }

    // ---- why the latest row is not a reuse candidate ------------------------

    #[test]
    fn model_selection_reports_why_the_latest_row_is_not_a_candidate() {
        let base = model_req("select id from DB.DEV.UP");
        let why = |req: &qc::SubmitEnrichedSqlRequest, row: Option<ExecutionRow>| {
            model(req, row).why_no_candidate
        };
        assert_eq!(why(&base, None), None, "a first build rejects nothing");
        assert_eq!(why(&base, Some(recorded(&evidence(&base)))), None);

        let mut other = recorded(&evidence(&base));
        other.node_sql_hash = Some("other".into());
        assert_eq!(why(&base, Some(other)), Some(NoCandidate::IdentityMismatch));

        let mut volatile = base.clone();
        volatile.tolerate_nondeterminism = false;
        volatile.sql = "select id, random() from DB.DEV.UP".into();
        let row = recorded(&evidence(&volatile));
        assert_eq!(
            why(&volatile, Some(row)),
            Some(NoCandidate::FailedReuseQualification)
        );

        let mut dropped = base.clone();
        dropped.tables.retain(|t| t.name != TARGET);
        let row = recorded(&evidence(&dropped));
        assert_eq!(
            why(&dropped, Some(row)),
            Some(NoCandidate::OwnTargetMissing)
        );

        let mut no_epoch = base.clone();
        no_epoch.tables[0].last_modified_epoch = None;
        let row = recorded(&evidence(&no_epoch));
        assert_eq!(
            why(&no_epoch, Some(row)),
            Some(NoCandidate::OwnTargetChangedOrMissing)
        );

        // VERIFIED LIVE (2026-11-10): an own target later than its build, even
        // by a second, is modified after the cached execution.
        let mut edited = base.clone();
        edited.tables[0].last_modified_epoch = Some(BUILT + 1);
        let row = recorded(&evidence(&edited));
        assert_eq!(
            why(&edited, Some(row)),
            Some(NoCandidate::OwnTargetModified)
        );
    }

    #[test]
    fn data_test_selection_reports_why_the_latest_row_is_not_a_candidate() {
        let req = data_test_req();
        let ev = evidence(&req);
        let why =
            |row: Option<ExecutionRow>| for_data_test(&req, &ev, row).unwrap().why_no_candidate;
        assert_eq!(why(None), None);
        assert_eq!(why(Some(recorded(&ev))), None);

        let mut partial = recorded(&ev);
        partial.execution_results = Some(results(false));
        assert_eq!(
            why(Some(partial)),
            Some(NoCandidate::IncompleteCachedTestResults)
        );
        let mut changed = recorded(&ev);
        changed.data_test_key = Some("other".into());
        assert_eq!(why(Some(changed)), Some(NoCandidate::IdentityMismatch));

        let mut volatile = data_test_req();
        volatile.tolerate_nondeterminism = false;
        volatile.sql = "select * from DB.DEV.UP where ts > current_timestamp()".into();
        let vev = evidence(&volatile);
        assert_eq!(
            for_data_test(&volatile, &vev, Some(recorded(&vev)))
                .unwrap()
                .why_no_candidate,
            Some(NoCandidate::FailedReuseQualification)
        );
    }

    #[test]
    fn seed_selection_reports_why_the_latest_row_is_not_a_candidate() {
        let req = seed_req(Some(BUILT));
        let ev = seed_evidence(&req);
        let why = |req: &qc::SubmitValuesRequest, row: Option<ExecutionRow>| {
            for_seed(req, &ev, row).unwrap().why_no_candidate
        };
        assert_eq!(why(&req, None), None);
        assert_eq!(why(&req, Some(recorded(&ev))), None);

        let mut other_values = recorded(&ev);
        other_values.values_hash = Some("changed".into());
        assert_eq!(
            why(&req, Some(other_values)),
            Some(NoCandidate::IdentityMismatch)
        );
        assert_eq!(
            why(&seed_req(Some(BUILT + 1)), Some(recorded(&ev))),
            Some(NoCandidate::OwnTargetChangedOrMissing)
        );

        let mut empty = seed_req(Some(BUILT));
        empty.values_hash = String::new();
        let eev = seed_evidence(&empty);
        let mut row = recorded(&eev);
        row.values_hash = Some(String::new());
        assert_eq!(
            for_seed(&empty, &eev, Some(row)).unwrap().why_no_candidate,
            Some(NoCandidate::FailedReuseQualification)
        );
    }

    // ---- models: execution type change (#83, #90) ---------------------------

    /// `row` as built with another execution type (`et`), at `built`.
    fn built_as(row: &ExecutionRow, id: i64, et: i32, built: i64) -> ExecutionRow {
        ExecutionRow {
            id,
            execution_type: et,
            last_modified_epoch: Some(built),
            ..row.clone()
        }
    }

    /// VERIFIED LIVE 2026-11-10/11 (explain_snapshot_history.jsonl entry 58,
    /// explain_execution_type_change.jsonl `ta`, `mi`, `tv`): the latest row
    /// has the workload's identity under another execution type, and no
    /// build of the workload's type stands in for it. Hosted rejects with
    /// EXECUTION_TYPE_MISMATCH and explains against that latest row.
    #[test]
    fn execution_type_change_without_a_build_of_that_type_anchors_on_the_latest_row() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let snapshot = built_as(&recorded(&ev), 7, 7, BUILT);
        assert!(execution_type_changed(&req, &ev, &snapshot));
        assert!(latest_build_of_type(&req, &ev, vec![snapshot.clone()]).is_none());
        let s = model(&req, Some(snapshot));
        assert!(s.candidate.is_none());
        assert_eq!(s.why_no_candidate, Some(NoCandidate::ExecutionTypeChanged));
        assert_eq!(s.type_changed.map(|r| r.id), Some(7));
        assert!(s.unmatched.is_none(), "the latest row has the identity");
        assert!(s.rejected.is_none());
    }

    #[test]
    fn another_identity_is_not_an_execution_type_change() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let mut view = built_as(&recorded(&ev), 8, 10, BUILT);
        view.node_sql_hash = Some("other extras".into());
        assert!(!execution_type_changed(&req, &ev, &view));
        assert!(!execution_type_changed(&req, &ev, &recorded(&ev)));
        let s = model(&req, Some(view));
        assert_eq!(s.why_no_candidate, Some(NoCandidate::IdentityMismatch));
        assert!(s.type_changed.is_none());
        assert_eq!(s.unmatched.map(|r| r.id), Some(8));
    }

    /// VERIFIED LIVE 2026-11-11 (explain_reason_kind_cells.jsonl `cv`, `vc`,
    /// #116): the same SQL as a view and as a custom node over an upstream
    /// is another identity on hosted, because client 2.0.11 reports no
    /// upstreams for the view. The latest row is then no execution type
    /// change in either direction, but an unmatched build; with the same
    /// upstream names it still is one (`vc0`, `cv0`, no upstreams).
    #[test]
    fn other_upstream_names_under_another_type_are_another_identity() {
        let custom = model_req("select id from DB.DEV.UP");
        let custom = qc::SubmitEnrichedSqlRequest {
            execution_type: 11,
            ..custom
        };
        let mut view = custom.clone();
        view.execution_type = 10;
        view.tables.retain(|t| t.name != UPSTREAM);
        let (custom_ev, view_ev) = (evidence(&custom), evidence(&view));

        let view_build = built_as(&recorded(&view_ev), 8, 10, BUILT);
        assert!(!execution_type_changed(&custom, &custom_ev, &view_build));
        let s = model(&custom, Some(view_build));
        assert_eq!(s.why_no_candidate, Some(NoCandidate::UpstreamNamesChanged));
        assert!(s.type_changed.is_none() && s.rejected.is_none());
        assert_eq!(s.unmatched.map(|r| r.id), Some(8), "the Explain anchor");

        let custom_build = built_as(&recorded(&custom_ev), 9, 11, BUILT);
        assert!(!execution_type_changed(&view, &view_ev, &custom_build));
        let s = model(&view, Some(custom_build));
        assert_eq!(s.why_no_candidate, Some(NoCandidate::UpstreamNamesChanged));
        assert_eq!(s.unmatched.map(|r| r.id), Some(9));

        let mut bare_custom = custom.clone();
        bare_custom.tables.retain(|t| t.name != UPSTREAM);
        let bare_ev = evidence(&bare_custom);
        let bare_view = built_as(&recorded(&view_ev), 10, 10, BUILT);
        assert!(execution_type_changed(&bare_custom, &bare_ev, &bare_view));
    }

    /// VERIFIED LIVE 2026-11-11 (explain_execution_type_change.jsonl `ta`,
    /// `mi`; explain_incremental_append_merge_history.jsonl entries 74-75,
    /// where the newer row is a RecordExecutions full refresh): the submit is
    /// judged against the latest build with its identity AND execution type,
    /// whose Target relation the newer build modified.
    #[test]
    fn execution_type_change_is_judged_against_the_latest_build_of_that_type() {
        let mut req = model_req("select id from DB.DEV.UP");
        req.tables[0].last_modified_epoch = Some(3 * BUILT);
        let ev = evidence(&req);
        let older = built_as(&recorded(&ev), 5, 1, BUILT);
        let mut other_identity = built_as(&older, 6, 1, 2 * BUILT);
        other_identity.node_sql_hash = Some("other".into());
        let latest = built_as(&older, 9, 2, 3 * BUILT);
        let history = vec![latest.clone(), other_identity, older];
        let build = latest_build_of_type(&req, &ev, history).expect("a FULL build");
        assert_eq!(build.id, 5, "the newest row with the identity and type");

        let s = for_model_over_build_of_type(&req, &ev, latest.clone(), build.clone()).unwrap();
        assert!(s.candidate.is_none());
        assert_eq!(s.why_no_candidate, Some(NoCandidate::OwnTargetModified));
        assert_eq!(s.rejected.map(|r| r.id), Some(5), "the Explain anchor");
        assert!(s.type_changed.is_none() && s.unmatched.is_none());
        assert!(s.target_has_prior_build);

        let mut dropped = req.clone();
        dropped.tables.retain(|t| t.name != TARGET);
        let s = for_model_over_build_of_type(&dropped, &ev, latest, build).unwrap();
        assert_eq!(s.why_no_candidate, Some(NoCandidate::OwnTargetMissing));
        assert_eq!(s.rejected.map(|r| r.id), Some(5));
    }

    /// Only the latest row is ever reused. An older build of the workload's
    /// type that would otherwise qualify (its Target relation unchanged
    /// since) is not: the rebuild is the execution type change against the
    /// latest row (local, conservative; hosted is uncharacterized).
    #[test]
    fn an_older_build_of_that_type_is_never_reused() {
        let req = model_req("select id from DB.DEV.UP");
        let ev = evidence(&req);
        let older = built_as(&recorded(&ev), 5, 1, BUILT);
        let latest = built_as(&older, 9, 2, BUILT);
        let s = for_model_over_build_of_type(&req, &ev, latest, older).unwrap();
        assert!(s.candidate.is_none());
        assert_eq!(s.why_no_candidate, Some(NoCandidate::ExecutionTypeChanged));
        assert_eq!(s.type_changed.map(|r| r.id), Some(9));
        assert!(s.rejected.is_none());

        let mut volatile = req.clone();
        volatile.tolerate_nondeterminism = false;
        volatile.sql = "select id, random() from DB.DEV.UP".into();
        let ev = evidence(&volatile);
        let older = built_as(&recorded(&ev), 5, 1, BUILT);
        let latest = built_as(&older, 9, 2, BUILT);
        let s = for_model_over_build_of_type(&volatile, &ev, latest, older).unwrap();
        assert_eq!(
            s.why_no_candidate,
            Some(NoCandidate::FailedReuseQualification)
        );
        assert!(s.rejected.is_none() && s.type_changed.is_none());
    }

    // ---- properties ---------------------------------------------------------

    fn arbitrary_row(ev: ExecutionEvidence) -> impl Strategy<Value = ExecutionRow> {
        (
            any::<bool>(),
            any::<bool>(),
            any::<bool>(),
            prop::option::of(0i64..2 * BUILT),
            prop_oneof![Just(1), Just(3)],
        )
            .prop_map(move |(same_sql, same_logic, clone, epoch, et)| {
                let mut row = if clone {
                    clone_origin(&ev)
                } else {
                    recorded(&ev)
                };
                if !same_sql {
                    row.node_sql_hash = Some("other".into());
                }
                if !same_logic {
                    row.node_namespace_logic_hash = Some("other".into());
                }
                row.last_modified_epoch = epoch;
                row.execution_type = et;
                row
            })
    }

    proptest! {
        #[test]
        fn unknown_physical_epoch_never_qualifies_even_with_modification_tolerance(
            current_epoch in any::<i64>(),
            ignore_external in any::<bool>(),
        ) {
            let mut req = model_req("select id from DB.DEV.UP");
            req.tables[0].last_modified_epoch = Some(current_epoch);
            req.ignore_external_modifications = ignore_external;
            let mut row = recorded(&evidence(&req));
            row.last_modified_epoch = Some(current_epoch);
            prop_assert!(model(&req, Some(row.clone())).candidate.is_some(),
                "matching known evidence must qualify before the fault");
            row.last_modified_epoch = None;
            prop_assert!(model(&req, Some(row)).candidate.is_none(),
                "modification tolerance cannot supply missing physical evidence");
        }

        #[test]
        fn prior_build_reported_iff_a_current_row_exists(
            row in prop::option::of(arbitrary_row(evidence(&model_req("select id from DB.DEV.UP")))),
        ) {
            let req = model_req("select id from DB.DEV.UP");
            let expected = row.is_some();
            prop_assert_eq!(model(&req, row).target_has_prior_build, expected);
        }

        /// Exactly one of: a candidate, a reason the latest row was rejected,
        /// or no latest row at all.
        #[test]
        fn rejection_reported_iff_the_latest_row_is_not_a_candidate(
            row in prop::option::of(arbitrary_row(evidence(&model_req("select id from DB.DEV.UP")))),
            tolerate in any::<bool>(),
        ) {
            let mut req = model_req("select id from DB.DEV.UP");
            req.tolerate_nondeterminism = tolerate;
            let had_row = row.is_some();
            let s = model(&req, row);
            prop_assert_eq!(
                s.why_no_candidate.is_some(),
                had_row && s.candidate.is_none()
            );
        }

        #[test]
        fn volatile_sql_with_evaluation_requested_never_reuses(
            row in arbitrary_row(evidence(&volatile_evaluation_req())),
        ) {
            let req = volatile_evaluation_req();
            prop_assert!(model(&req, Some(row)).candidate.is_none());
        }

        #[test]
        fn candidate_is_always_the_current_target_row(
            row in arbitrary_row(evidence(&model_req("select id from DB.DEV.UP"))),
            tolerate in any::<bool>(),
        ) {
            let mut req = model_req("select id from DB.DEV.UP");
            req.tolerate_nondeterminism = tolerate;
            let (id, clone) = (row.id, row.clone_source_table.is_some());
            if let Some(c) = model(&req, Some(row)).candidate {
                prop_assert_eq!(c.id, id);
                prop_assert_eq!(c.target_table.as_str(), TARGET);
                prop_assert_eq!(c.execution_type, req.execution_type);
                if clone {
                    prop_assert!(c.execution_results.is_none(), "adoption never serves results");
                }
            }
        }
    }
}
