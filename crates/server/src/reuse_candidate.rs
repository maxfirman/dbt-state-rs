//! Reuse candidate selection: which recorded result, if any, a submit may reuse.
//!
//! The caller fetches the latest execution in scope
//! ([`Store::current_target_state`](crate::store::Store::current_target_state)
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
use crate::store::{ExecutionRow, InputTable, Producer};

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
}

impl Selection {
    /// `row` as the reuse candidate, for judging it as if selected.
    pub(crate) fn of_candidate(row: ExecutionRow) -> Selection {
        Selection {
            candidate: Some(row),
            target_has_prior_build: true,
            why_no_candidate: None,
            rejected: None,
            unmatched: None,
            type_changed: None,
        }
    }

    /// No confirmed build of the target exists in scope.
    const FIRST_BUILD: Selection = Selection {
        candidate: None,
        target_has_prior_build: false,
        why_no_candidate: None,
        rejected: None,
        unmatched: None,
        type_changed: None,
    };

    /// The outcome for a latest row in scope: the reuse candidate when the
    /// row's identity matched (`identified`) and `rejection` found nothing
    /// against it, otherwise why it is not one.
    /// `unidentified` says why when the row's identity did not match, and
    /// `unmatched` is that row (models only).
    fn of(
        identified: Option<ExecutionRow>,
        unidentified: NoCandidate,
        rejection: impl FnOnce(&ExecutionRow) -> Option<NoCandidate>,
        target_has_prior_build: bool,
        unmatched: Option<ExecutionRow>,
    ) -> Selection {
        let why_no_candidate = match &identified {
            None => Some(unidentified),
            Some(row) => rejection(row),
        };
        let own_target = matches!(
            why_no_candidate,
            Some(NoCandidate::OwnTargetModified | NoCandidate::OwnTargetMissing)
        );
        let (candidate, rejected) = match identified {
            Some(row) if why_no_candidate.is_none() => (Some(row), None),
            Some(row) if own_target => (None, Some(row)),
            _ => (None, None),
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
            unmatched,
            type_changed,
        }
    }
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
    Ok(Selection::of(
        identified,
        unidentified,
        |prev| model_rejection(req, evidence, prev),
        true,
        unmatched,
    ))
}

/// Why a model's build with the workload's identity is not a reuse
/// candidate, if anything is against it: reuse qualification, then its own
/// target's existence and External modifications.
fn model_rejection(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    prev: &ExecutionRow,
) -> Option<NoCandidate> {
    if !sql_qualifies(req, evidence) {
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
    let build = build.into_checked(false)?;
    Ok(match model_rejection(req, evidence, &build) {
        Some(why) => Selection::of(Some(build), why, |_| Some(why), true, None),
        None => Selection::of(
            None,
            NoCandidate::ExecutionTypeChanged,
            |_| None,
            true,
            Some(latest),
        ),
    })
}

/// Data tests: no physical target. `latest` is the latest confirmed outcome
/// for the test's node UID in its project and physical scope. The target's
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
    let identified = if identity_matches(evidence, &latest) {
        Some(latest.into_checked(true)?)
    } else {
        None
    };
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
        false,
        None,
    ))
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
        true,
        None,
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

/// A client-requested copy (RegisterClone) records NO upstream evidence: it
/// is clone-origin, carries no rendered identity and no inputs. When such a
/// row is the freshness anchor, the submit's CURRENT inputs stand in as the
/// recorded set, each recorded at the copy's own build epoch (or at its
/// current epoch, if earlier), so an upstream counts as drifted exactly when
/// it is newer than the copy (clone-age anchor, verified live). Their
/// producers are unrecorded: a copy is never an upstream query change.
///
/// Every other row keeps exactly what it recorded. In particular a copy made
/// by the SQL-submit fallback recorded that submit's inputs; an EMPTY list
/// there means "no upstreams", and a later submit with upstreams must rebuild.
///
/// The single rule for both the decision (here) and its explanation
/// (`ExplainDecisionRow::anchor`), so the two cannot disagree.
pub(crate) fn adopt_current_inputs(anchor: &mut ExecutionRow, current: &[InputTable]) {
    if is_copy_registration(anchor) && anchor.input_tables.is_empty() {
        let built = anchor.last_modified_epoch;
        anchor.input_tables = current
            .iter()
            .map(|t| InputTable {
                name: t.name.clone(),
                last_modified_epoch: match (t.last_modified_epoch, built) {
                    (Some(cur), Some(built)) => Some(cur.min(built)),
                    _ => built,
                },
                producer: Producer::Unrecorded,
            })
            .collect();
    }
}

/// A client-requested copy (RegisterClone): clone-origin with no rendered
/// identity of its own. A Result copy made on submit records the submit's
/// identity and is not one.
fn is_copy_registration(row: &ExecutionRow) -> bool {
    row.clone_source_table.is_some() && row.node_sql_hash.is_none()
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
    let logic_compatible = match (
        &row.node_namespace_logic_hash,
        &evidence.node_namespace_logic_hash,
    ) {
        (Some(recorded), Some(current)) => recorded == current,
        _ => true,
    };
    identity_matches(evidence, row)
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
/// source's schema ([`rehomed_logic_matches`]), has the logic the copy
/// inherited.
fn adoptable(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    row: &ExecutionRow,
) -> bool {
    row.execution_type == req.execution_type && copy_logic_matches(req, evidence, row)
}

/// [`adoptable`] without the execution type: a clone-origin row whose logic
/// identity is the submit's, also read in the copy source's schema for a
/// client-requested copy.
fn copy_logic_matches(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    row: &ExecutionRow,
) -> bool {
    row.dialect == req.dialect
        && row.clone_source_table.is_some()
        && evidence.node_namespace_logic_hash.is_some()
        && (row.node_namespace_logic_hash == evidence.node_namespace_logic_hash
            || is_copy_registration(row) && rehomed_logic_matches(req, row))
}

/// Whether the submit, with its Target relation's schema read as the copy
/// source's schema, has the schema-abstracted logic the copy inherited from
/// its source build (`fingerprint::rehomed_copy_logic_hash`, #93). VERIFIED
/// LIVE 2026-11-11 (explain_post_copy_identity.jsonl): hosted reuses a
/// dev-clone pre-step copy whose post-copy submit reads the source build's
/// fixed prod-schema source, or a view resolved per target over it, which the
/// request-relative abstraction alone does not match.
fn rehomed_logic_matches(req: &qc::SubmitEnrichedSqlRequest, row: &ExecutionRow) -> bool {
    use crate::sql_norm::schema_of_object_name;
    let (Some(from), Some(to)) = (
        req.target_table.as_deref().and_then(schema_of_object_name),
        row.clone_source_table
            .as_deref()
            .and_then(schema_of_object_name),
    ) else {
        return false;
    };
    row.node_namespace_logic_hash.is_some()
        && crate::fingerprint::rehomed_copy_logic_hash(
            &crate::execution::sql_execution(req),
            &from,
            &to,
        ) == row.node_namespace_logic_hash
}

/// Reuse qualification shared by models and data tests. Runtime volatile
/// function evidence is not implemented, so requested volatile evaluation
/// builds instead of claiming reuse. Empty SQL or template is likewise
/// insufficient evidence.
fn sql_qualifies(req: &qc::SubmitEnrichedSqlRequest, evidence: &ExecutionEvidence) -> bool {
    req.tolerate_nondeterminism
        && complete_dependencies(req)
        && !req.sql.trim().is_empty()
        && (!req.compare_unrendered_code
            || evidence
                .node_body_hash
                .as_ref()
                .is_some_and(|s| !s.is_empty()))
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
    if own.len() != 1 || own[0].last_modified_epoch.is_none() {
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
            ("volatile evaluation requested", |r| {
                r.tolerate_nondeterminism = false
            }),
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

        let mut changed = recorded(&ev);
        changed.node_sql_hash = Some("other".into());
        assert!(for_data_test(&req, &ev, Some(changed))
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
        changed.node_sql_hash = Some("other".into());
        assert_eq!(why(Some(changed)), Some(NoCandidate::IdentityMismatch));

        let mut volatile = data_test_req();
        volatile.tolerate_nondeterminism = false;
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
        fn candidate_is_always_the_current_target_row(
            row in arbitrary_row(evidence(&model_req("select id from DB.DEV.UP"))),
            tolerate in any::<bool>(),
        ) {
            let mut req = model_req("select id from DB.DEV.UP");
            req.tolerate_nondeterminism = tolerate;
            let (id, clone) = (row.id, row.clone_source_table.is_some());
            if let Some(c) = model(&req, Some(row)).candidate {
                prop_assert!(tolerate, "volatile evaluation never reuses");
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
