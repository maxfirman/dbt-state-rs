//! Pure freshness policy for a reuse candidate already selected by
//! `reuse_candidate` (identity, own-target evidence, cached outcomes).
//! Exact physical upstream identities and known, unambiguous metadata are
//! required. Each upstream's current epoch is compared with the epoch recorded
//! for it at the candidate's build. A changed Upstream producer always
//! rebuilds; otherwise the ANY/ALL policy decides staleness, and Lag tolerance
//! turns a policy rebuild into a reuse while the build age is under the
//! tolerance (ADR 0007).

use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
use crate::store::{ExecutionRow, InputTable, Producer};

/// stale_upstream_policy values (shared.proto StaleUpstreamPolicy).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleUpstreamPolicy {
    /// ANY upstream drift → stale (default).
    Any,
    /// Only stale when ALL upstreams drift.
    All,
}

impl StaleUpstreamPolicy {
    pub fn from_i32(v: i32) -> Self {
        match v {
            1 => Self::All,
            _ => Self::Any,
        }
    }
}

/// Why reuse candidate selection found no reuse candidate although State
/// history holds a latest row in scope (see `reuse_candidate`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoCandidate {
    /// The latest row does not have the workload's identity.
    IdentityMismatch,
    /// The latest row has the workload's identity but was built as another
    /// node kind (a snapshot run under `--full-refresh` is sent with the wire
    /// `execution_type` FULL; a model whose materialization or incremental
    /// strategy changed), and no build of the workload's type stands in for
    /// it. Hosted rejects it with EXECUTION_TYPE_MISMATCH (2 / 2) and explains
    /// it against that row (#83).
    ExecutionTypeChanged,
    /// The latest row has the workload's identity under another execution
    /// type, but recorded other genuine upstream names than the workload
    /// reports (client 2.0.11 reports none for a view, so the same SQL as a
    /// view and as a custom node over an upstream). Hosted treats the changed
    /// upstream set as another identity: the wire answer of
    /// [`NoCandidate::IdentityMismatch`], with the older-match codes when an
    /// older build of the workload's type would qualify (VERIFIED LIVE
    /// 2026-11-11, explain_reason_kind_cells.jsonl `cv`, `vc`, #116). Unlike
    /// an identity mismatch, Explain says the node query has not changed.
    UpstreamNamesChanged,
    /// The latest row is a client-requested copy (RegisterClone) that this
    /// workload cannot adopt: its inherited schema-abstracted logic is absent
    /// or differs, also with the submit read in the copy source's schema
    /// (#93). This server rebuilds with the same wire answer as
    /// [`NoCandidate::IdentityMismatch`]; no capture backs an Explain tree
    /// for it (#104).
    CopyLogicMismatch,
    /// The workload or its evidence failed reuse qualification.
    FailedReuseQualification,
    /// The node's own target is reported ambiguously or without an epoch.
    /// (Persisted name predates [`NoCandidate::OwnTargetMissing`].)
    OwnTargetChangedOrMissing,
    /// The node's own target is absent from the submit: it does not exist,
    /// although the latest build has the workload's identity.
    OwnTargetMissing,
    /// The node's own target was modified after the recorded build (its
    /// reported epoch is later than the build's).
    OwnTargetModified,
    /// The latest data test outcome has no complete cached results to serve.
    IncompleteCachedTestResults,
}

/// How fresh a reused result is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reuse {
    /// No upstream drift.
    Fresh,
    /// Upstream drift is present but the stale-upstream policy tolerates it
    /// (hosted `is_stale = true`).
    ToleratedStale,
    /// The stale-upstream policy requires a rebuild, but the build is younger
    /// than the node's Lag tolerance (hosted `is_stale = true`).
    WithinLagTolerance,
}

/// Why a submission was decided the way it was. Every decision has exactly
/// one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionReason {
    /// State history holds no latest row for the workload's output in scope.
    FirstBuild,
    /// A latest row exists but is not a reuse candidate.
    NoReuseCandidate(NoCandidate),
    /// The reuse candidate recorded a different genuine upstream set.
    DependencySetChanged,
    /// The reuse candidate has upstreams but no own-build epoch to compare
    /// them against.
    NoBuildAnchor,
    /// An upstream changed after the reuse candidate's build (or its freshness
    /// cannot be proven), beyond what the stale-upstream policy tolerates.
    UpstreamOutdated,
    /// An upstream's contents are no longer the result of the build the reuse
    /// candidate consumed: it was rebuilt with different logic or externally
    /// modified. Rebuilds regardless of policy and Lag tolerance.
    UpstreamQueryChanged,
    /// The reuse candidate is served.
    Reused(Reuse),
    /// An equivalent result under another name is copied into the target.
    ResultCopy,
    /// An equivalent exists, but the copy chain is at its limit, or the
    /// target forbids copies with `clone_chain_depth_limit = 0` alone (client
    /// 2.0.6), so the model builds.
    CopyChainLimit,
    /// An equivalent exists, but the submit disallows copies
    /// (`allow_clones: false`), so the model builds.
    CopyDisabled,
}

impl DecisionReason {
    /// Every decision reason, in declaration order.
    const ALL: [DecisionReason; 20] = [
        Self::FirstBuild,
        Self::NoReuseCandidate(NoCandidate::IdentityMismatch),
        Self::NoReuseCandidate(NoCandidate::ExecutionTypeChanged),
        Self::NoReuseCandidate(NoCandidate::UpstreamNamesChanged),
        Self::NoReuseCandidate(NoCandidate::CopyLogicMismatch),
        Self::NoReuseCandidate(NoCandidate::FailedReuseQualification),
        Self::NoReuseCandidate(NoCandidate::OwnTargetChangedOrMissing),
        Self::NoReuseCandidate(NoCandidate::OwnTargetModified),
        Self::NoReuseCandidate(NoCandidate::OwnTargetMissing),
        Self::NoReuseCandidate(NoCandidate::IncompleteCachedTestResults),
        Self::DependencySetChanged,
        Self::NoBuildAnchor,
        Self::UpstreamOutdated,
        Self::UpstreamQueryChanged,
        Self::Reused(Reuse::Fresh),
        Self::Reused(Reuse::ToleratedStale),
        Self::Reused(Reuse::WithinLagTolerance),
        Self::ResultCopy,
        Self::CopyChainLimit,
        Self::CopyDisabled,
    ];

    /// The stable lowercase string stored in `node_decisions.decision_reason`
    /// (ADR 0006). Persisted state: never reword an existing value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FirstBuild => "first_build",
            Self::NoReuseCandidate(NoCandidate::IdentityMismatch) => {
                "no_reuse_candidate.identity_mismatch"
            }
            Self::NoReuseCandidate(NoCandidate::ExecutionTypeChanged) => {
                "no_reuse_candidate.execution_type_changed"
            }
            Self::NoReuseCandidate(NoCandidate::UpstreamNamesChanged) => {
                "no_reuse_candidate.upstream_names_changed"
            }
            Self::NoReuseCandidate(NoCandidate::CopyLogicMismatch) => {
                "no_reuse_candidate.copy_logic_mismatch"
            }
            Self::NoReuseCandidate(NoCandidate::FailedReuseQualification) => {
                "no_reuse_candidate.failed_reuse_qualification"
            }
            Self::NoReuseCandidate(NoCandidate::OwnTargetChangedOrMissing) => {
                "no_reuse_candidate.own_target_changed_or_missing"
            }
            Self::NoReuseCandidate(NoCandidate::OwnTargetModified) => {
                "no_reuse_candidate.own_target_modified"
            }
            Self::NoReuseCandidate(NoCandidate::OwnTargetMissing) => {
                "no_reuse_candidate.own_target_missing"
            }
            Self::NoReuseCandidate(NoCandidate::IncompleteCachedTestResults) => {
                "no_reuse_candidate.incomplete_cached_test_results"
            }
            Self::DependencySetChanged => "dependency_set_changed",
            Self::NoBuildAnchor => "no_build_anchor",
            Self::UpstreamOutdated => "upstream_outdated",
            Self::UpstreamQueryChanged => "upstream_query_changed",
            Self::Reused(Reuse::Fresh) => "reused.fresh",
            Self::Reused(Reuse::ToleratedStale) => "reused.tolerated_stale",
            Self::Reused(Reuse::WithinLagTolerance) => "reused.within_lag_tolerance",
            Self::ResultCopy => "result_copy",
            Self::CopyChainLimit => "copy_chain_limit",
            Self::CopyDisabled => "copy_disabled",
        }
    }

    /// The reason a stored [`as_str`](Self::as_str) value names; `None` for
    /// anything else.
    pub fn from_stored(stored: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|r| r.as_str() == stored)
    }
}

/// The decision class: what the client is told to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionKind {
    Build,
    Skip,
    Clone,
}

impl DecisionKind {
    /// The value stored in `node_decisions.decision`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Skip => "skip",
            Self::Clone => "clone",
        }
    }

    /// The class a stored [`as_str`](Self::as_str) value names; `None` for
    /// anything else.
    pub fn from_stored(stored: &str) -> Option<Self> {
        [Self::Build, Self::Skip, Self::Clone]
            .into_iter()
            .find(|kind| kind.as_str() == stored)
    }

    /// The wire `decision` int (`SubmitSQLResultType`, shared.proto).
    pub fn wire_decision(self) -> i32 {
        let result_type = match self {
            Self::Skip => qc::SubmitSqlResultType::SkipExecution,
            Self::Build => qc::SubmitSqlResultType::ReadyToExecute,
            Self::Clone => qc::SubmitSqlResultType::ReadyToClone,
        };
        result_type as i32
    }
}

/// Decision verdict with the fields needed to build a SubmitSQLResponse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Skip {
        description: String,
        /// Hosted `is_stale`: true when upstream drift IS present but the node is
        /// still reused (e.g. ALL policy and not every direct parent drifted).
        /// VERIFIED LIVE: a skip serving slightly-stale data reports is_stale=true;
        /// a no-drift skip and every execute report false. See lag_freshness.rs.
        is_stale: bool,
        reason: DecisionReason,
    },
    Execute {
        description: String,
        skip_rejection_reason: i32,
        clone_rejection_reason: i32,
        /// Whether the execute was driven by upstream staleness (vs a hash miss).
        is_stale: bool,
        reason: DecisionReason,
    },
}

impl Verdict {
    pub fn reason(&self) -> DecisionReason {
        match self {
            Self::Skip { reason, .. } | Self::Execute { reason, .. } => *reason,
        }
    }
}

/// Everything a Submit decision tells the client and records for capture:
/// the class, its one reason, the hosted description and rejection codes,
/// `is_stale`, and the prior execution the decision was made against (the
/// Explain freshness anchor). Only the constructors below build one (the
/// private `sealed` field stops struct literals, in this crate too), so the
/// reason always agrees with the wire values.
#[derive(Debug, Clone, PartialEq, Eq)]
// `#[non_exhaustive]` would not stop construction inside this crate.
#[allow(clippy::manual_non_exhaustive)]
pub struct DecisionRecord {
    pub class: DecisionKind,
    pub reason: DecisionReason,
    pub description: String,
    pub skip_rejection_reason: Option<i32>,
    pub clone_rejection_reason: Option<i32>,
    pub is_stale: bool,
    /// `executions.id` of the reuse candidate `decide` evaluated: set on a
    /// Skip and on a freshness-driven Execute, `None` otherwise.
    pub matched_execution_id: Option<i64>,
    /// The freshness context the decision used, persisted for Explain: the
    /// node's Lag tolerance, the reuse candidate's build age, and the
    /// decision-time inputs with their resolved producers. Zero, `None` and
    /// empty for a result copy.
    pub tolerance_ms: i64,
    pub build_age_ms: Option<i64>,
    pub input_tables: Vec<InputTable>,
    /// For a rebuild without a matched build: `executions.id` of the latest
    /// build in scope, which did not have the workload's identity. Explain
    /// judges the SHAPE 7 freshness listing against it (VERIFIED LIVE
    /// 2026-11-11, explain_rebuild_reason_gaps.jsonl `r40q`). `None`
    /// otherwise.
    pub unmatched_execution_id: Option<i64>,
    sealed: (),
}

impl DecisionRecord {
    /// The record of [`decide`] against `candidate`.
    pub fn decided(ctx: &SubmitContext, candidate: Option<&ExecutionRow>) -> Self {
        let matched_execution_id = candidate.map(|c| c.id);
        let (tolerance_ms, build_age_ms, input_tables) = (
            ctx.tolerance_ms,
            ctx.build_age_ms,
            ctx.input_tables.to_vec(),
        );
        match decide(ctx, candidate) {
            Verdict::Skip {
                description,
                is_stale,
                reason,
            } => Self {
                class: DecisionKind::Skip,
                reason,
                description,
                skip_rejection_reason: None,
                clone_rejection_reason: None,
                is_stale,
                matched_execution_id,
                tolerance_ms,
                build_age_ms,
                input_tables,
                unmatched_execution_id: None,
                sealed: (),
            },
            Verdict::Execute {
                description,
                skip_rejection_reason,
                clone_rejection_reason,
                is_stale,
                reason,
            } => Self {
                class: DecisionKind::Build,
                reason,
                description,
                skip_rejection_reason: Some(skip_rejection_reason),
                clone_rejection_reason: Some(clone_rejection_reason),
                is_stale,
                matched_execution_id,
                tolerance_ms,
                build_age_ms,
                input_tables,
                unmatched_execution_id: None,
                sealed: (),
            },
        }
    }

    /// A rebuild while an OLDER build of the target would still be reusable
    /// for this submit (its contents were overwritten since): VERIFIED LIVE
    /// (2026-11-10, lag_copy_candidates `ob*`, lag_seed_upstream H) hosted
    /// reports LATEST_QUERY_HASH_NOT_MATCH / TARGET_TABLE_MATCH, and
    /// `is_stale` as that build would be reused. `build_age_ms` is that
    /// build's age, which a stale SHAPE 7 tree's Lag tolerance line reports
    /// (VERIFIED LIVE 2026-11-11, explain_freshness_cells.jsonl `fr_*`).
    pub fn with_older_match(mut self, older: Reuse, build_age_ms: Option<i64>) -> Self {
        if self.class == DecisionKind::Build {
            self.skip_rejection_reason = Some(REJECTION_LATEST_QUERY_HASH_NOT_MATCH);
            self.clone_rejection_reason = Some(REJECTION_TARGET_TABLE_MATCH);
            self.is_stale = older != Reuse::Fresh;
            self.build_age_ms = build_age_ms;
        }
        self
    }

    /// A rebuild of an absent own target whose latest build has the
    /// workload's identity and would otherwise be reusable for this submit:
    /// VERIFIED LIVE (2026-11-10, lag_copy_candidates `dt`, lag_seed_upstream
    /// G) hosted reports TARGET_TABLE_DOES_NOT_EXIST / TARGET_TABLE_MATCH. When
    /// that build would be stale it reports NO_SUITABLE_MATCH_FOUND (`ddc`,
    /// `dset0`), the [`decide`] default.
    pub fn with_absent_target_match(mut self) -> Self {
        if self.class == DecisionKind::Build
            && self.reason == DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetMissing)
        {
            self.skip_rejection_reason = Some(REJECTION_TARGET_TABLE_DOES_NOT_EXIST);
            self.clone_rejection_reason = Some(REJECTION_TARGET_TABLE_MATCH);
        }
        self
    }

    /// A rebuild of a submit whose equivalent under another name is not
    /// copied, because the Warehouse rules copy only FULL on submit: VERIFIED
    /// LIVE (client 2.0.11, `result_copy::plan_submit`) hosted keeps the
    /// ordinary description and tree and reports TARGET_TABLE_MISMATCH with
    /// `clone_rejection_reason`.
    pub fn with_refused_copy(mut self, clone_rejection_reason: i32) -> Self {
        if self.class == DecisionKind::Build {
            self.skip_rejection_reason = Some(REJECTION_TARGET_TABLE_MISMATCH);
            self.clone_rejection_reason = Some(clone_rejection_reason);
        }
        self
    }

    /// A rebuild with no matched build records the latest build in scope
    /// that did not have the workload's identity (`unmatched`), which Explain
    /// judges its freshness listing against.
    pub(crate) fn with_unmatched_build(mut self, unmatched: Option<&ExecutionRow>) -> Self {
        if self.class == DecisionKind::Build && self.matched_execution_id.is_none() {
            self.unmatched_execution_id = unmatched.map(|build| build.id);
        }
        self
    }

    /// A copy of an equivalent judged reusable for this submit as `source`
    /// (VERIFIED LIVE 2026-11-10, lag_copy_candidates): `is_stale` and the
    /// build age are the equivalent's. `own` is the target's reuse candidate
    /// when the copy goes over an existing target whose build had to rebuild
    /// (Explain: "but it's old, so we will clone from …"); it stays the
    /// Explain anchor.
    pub fn result_copy_judged(
        own: Option<&ExecutionRow>,
        source: Reuse,
        tolerance_ms: i64,
        build_age_ms: Option<i64>,
        input_tables: &[InputTable],
    ) -> Self {
        Self {
            is_stale: source != Reuse::Fresh,
            matched_execution_id: own.map(|c| c.id),
            tolerance_ms,
            build_age_ms,
            input_tables: input_tables.to_vec(),
            ..Self::result_copy()
        }
    }

    /// An equivalent model built under another name is copied in. VERIFIED
    /// LIVE shape: READY_TO_CLONE with TARGET_TABLE_MISMATCH and no clone
    /// rejection.
    pub fn result_copy() -> Self {
        Self {
            class: DecisionKind::Clone,
            reason: DecisionReason::ResultCopy,
            description: RESULT_COPY_DESCRIPTION.to_string(),
            skip_rejection_reason: Some(REJECTION_TARGET_TABLE_MISMATCH),
            clone_rejection_reason: None,
            is_stale: false,
            matched_execution_id: None,
            tolerance_ms: 0,
            build_age_ms: None,
            input_tables: Vec::new(),
            unmatched_execution_id: None,
            sealed: (),
        }
    }

    /// An equivalent seed loaded under another name is copied in (#91,
    /// Snowflake only). VERIFIED LIVE shape (client 2.0.11): that of
    /// [`result_copy`](Self::result_copy) with the seed description, into an
    /// absent table (explain_copy_probes.jsonl entry 12) or over the table's
    /// latest load with other values, `over`, which stays the Explain anchor
    /// ("but it's old", explain_seed_copy_existing.jsonl entry 14).
    pub fn seed_result_copy(over: Option<&ExecutionRow>) -> Self {
        Self {
            description: SEED_RESULT_COPY_DESCRIPTION.to_string(),
            matched_execution_id: over.map(|row| row.id),
            ..Self::result_copy()
        }
    }

    /// A seed load whose copy of an equivalent the submit disallowed
    /// (`allow_clones: false`) or forbade (`clone_chain_depth_limit = 0`)
    /// (#91). Hosted's answer for such a seed is uncaptured, so the wire keeps
    /// the ordinary load (description and codes); only the persisted reason
    /// records why it was not copied, so Explain renders no tree claiming no
    /// other table exists.
    pub fn with_seed_copy_refused(mut self, reason: DecisionReason) -> Self {
        debug_assert!(matches!(
            reason,
            DecisionReason::CopyDisabled | DecisionReason::CopyChainLimit
        ));
        if self.class == DecisionKind::Build {
            self.reason = reason;
        }
        self
    }

    /// An equivalent exists but the copy chain is at its limit, or the target
    /// forbids copies with `clone_chain_depth_limit = 0` alone. VERIFIED LIVE
    /// (client 2.0.6, experiments/clone_experiment.sh table --no-clones): the
    /// hosted service executes with this description, TARGET_TABLE_MISMATCH
    /// and CLONE_CHAIN_LIMIT_EXCEEDED.
    pub fn copy_chain_limit() -> Self {
        Self {
            class: DecisionKind::Build,
            reason: DecisionReason::CopyChainLimit,
            description: COPY_CHAIN_LIMIT_DESCRIPTION.to_string(),
            skip_rejection_reason: Some(REJECTION_TARGET_TABLE_MISMATCH),
            clone_rejection_reason: Some(REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED),
            is_stale: false,
            matched_execution_id: None,
            tolerance_ms: 0,
            build_age_ms: None,
            input_tables: Vec::new(),
            unmatched_execution_id: None,
            sealed: (),
        }
    }

    /// An equivalent exists but the submit disallows copies
    /// (`allow_clones: false`, sent by client 2.0.11 with
    /// `clone_chain_depth_limit = 0`). VERIFIED LIVE 2026-11-11
    /// (explain_copy_logic_mismatch.jsonl `cf4`): the codes of
    /// [`copy_chain_limit`](Self::copy_chain_limit) with another description.
    pub fn copy_disabled() -> Self {
        Self {
            reason: DecisionReason::CopyDisabled,
            description: COPY_DISABLED_DESCRIPTION.to_string(),
            ..Self::copy_chain_limit()
        }
    }
}

pub const DESC_SKIP: &str =
    "model was a no-op because both its query and its upstream data are up to date";
pub const DESC_EXECUTE: &str =
    "model was executed because either its query didn't match or its upstream data is out of date";

const RESULT_COPY_DESCRIPTION: &str =
    "an equivalent model exists under another name so we cloned that one";
pub(crate) const SEED_RESULT_COPY_DESCRIPTION: &str =
    "an equivalent seed exists under another name so we cloned that one";
const COPY_CHAIN_LIMIT_DESCRIPTION: &str =
    "model was executed because the maximum number of nested clones has been reached";
const COPY_DISABLED_DESCRIPTION: &str = "model was executed because cloning is disabled";

// The wire rejection codes (`RejectionReason`, shared.proto) the service sends.
pub const REJECTION_TARGET_TABLE_MISMATCH: i32 = qc::RejectionReason::TargetTableMismatch as i32;
/// An execution type change: the workload's identity was last built under
/// another execution type (`NoCandidate::ExecutionTypeChanged`, see
/// `execute`); and a submit not copied although an equivalent exists, whose
/// newest equivalent has another execution type (see
/// `result_copy::plan_submit`).
pub const REJECTION_EXECUTION_TYPE_MISMATCH: i32 =
    qc::RejectionReason::ExecutionTypeMismatch as i32;
/// A view, snapshot or custom node not copied although an equivalent exists:
/// hosted copies only FULL on submit (see `result_copy::plan_submit`).
pub const REJECTION_EXECUTION_TYPE_NOT_FULL: i32 = qc::RejectionReason::ExecutionTypeNotFull as i32;
/// A client-requested copy the service cannot prove eligible (RegisterClone
/// `unable_to_clone`), and the skip rejection on every `ready_to_clone`.
pub const REJECTION_FORCED_NOT_ELIGIBLE: i32 = qc::RejectionReason::ForcedNotEligible as i32;
pub const REJECTION_NO_SUITABLE_MATCH_FOUND: i32 = qc::RejectionReason::NoSuitableMatchFound as i32;
pub const REJECTION_TARGET_TABLE_DOES_NOT_EXIST: i32 =
    qc::RejectionReason::TargetTableDoesNotExist as i32;
pub const REJECTION_TARGET_TABLE_MATCH: i32 = qc::RejectionReason::TargetTableMatch as i32;
pub const REJECTION_LATEST_QUERY_HASH_NOT_MATCH: i32 =
    qc::RejectionReason::LatestQueryHashNotMatch as i32;
/// VERIFIED LIVE: when a target disallows clones (profile `allow_clones:
/// false`), the Fusion client encodes it as `clone_chain_depth_limit = 0`, and
/// the hosted service answers RegisterClone with `unable_to_clone` carrying
/// this rejection reason and decision=READY_TO_EXECUTE.
pub const REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED: i32 =
    qc::RejectionReason::CloneChainLimitExceeded as i32;

/// A human noun for the node kind, matching the hosted service's wording.
fn node_noun(execution_type: i32) -> &'static str {
    match ModelExecutionType::try_from(execution_type) {
        Ok(ModelExecutionType::Snapshot) => "snapshot",
        Ok(ModelExecutionType::DbtDataTest) => "data test",
        Ok(ModelExecutionType::Values) => "seed",
        _ => "model",
    }
}

/// SKIP description, node-kind and freshness specific (verified live against
/// api.state.dbt.com, 2026-11-10): "both … up to date" when the node has no
/// Lag tolerance or no upstream epoch moved since the build; otherwise
/// "within lag tolerance" while the build is younger than the tolerance;
/// otherwise "at least one upstream dependency is unchanged". A snapshot
/// always says "both … up to date" (explain_snapshot_history.jsonl: an epoch
/// that moved back, and drift within the tolerance).
fn skip_description(execution_type: i32, f: &Freshness, ctx: &SubmitContext) -> String {
    if execution_type == ModelExecutionType::Values as i32 {
        return "seed was a no-op because its data has not changed".to_string();
    }
    let noun = node_noun(execution_type);
    let snapshot = execution_type == ModelExecutionType::Snapshot as i32;
    if snapshot || ctx.tolerance_ms == 0 || f.upstreams_unchanged {
        format!("{noun} was a no-op because both its query and its upstream data are up to date")
    } else if within_lag_tolerance(ctx) {
        format!(
            "{noun} was a no-op because its query is up to date and its data is within lag tolerance"
        )
    } else {
        format!(
            "{noun} was a no-op because its query is up to date and at least one upstream dependency is unchanged"
        )
    }
}

/// Whether the reuse candidate's build is younger than the node's Lag
/// tolerance. An unknown build age is never within it.
fn within_lag_tolerance(ctx: &SubmitContext) -> bool {
    ctx.tolerance_ms > 0 && ctx.build_age_ms.is_some_and(|age| age < ctx.tolerance_ms)
}

/// EXECUTE description, node-kind + reason specific. `had_prior` distinguishes a
/// first build ("did not exist" / "no prior execution") from a rebuild of an
/// existing node ("query didn't match or upstream out of date").
fn execute_description(execution_type: i32, had_prior: bool, reason: DecisionReason) -> String {
    if reason == DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetModified) {
        // VERIFIED LIVE 2026-11-11 (explain_reason_kind_cells.jsonl `vc0`):
        // hosted words a view's own-target modification by its definition.
        if execution_type == ModelExecutionType::View as i32 {
            return "model was executed because the view definition is newer than the cached execution"
                .to_string();
        }
        // VERIFIED LIVE (2026-11-10, lag_tolerance_matrix `ot*_u`).
        return format!(
            "{} was executed because the table was modified after the cached execution",
            node_noun(execution_type)
        );
    }
    match (ModelExecutionType::try_from(execution_type), had_prior) {
        (Ok(ModelExecutionType::Values), false) => {
            "seed was loaded because it did not exist".to_string()
        }
        (Ok(ModelExecutionType::Values), true) => {
            "seed was loaded because its data changed".to_string()
        }
        (Ok(ModelExecutionType::DbtDataTest), _) => {
            "data test was executed because it has no prior execution or its query changed"
                .to_string()
        }
        (_, false) => format!(
            "{} was executed because its table did not exist",
            node_noun(execution_type)
        ),
        (_, true) => format!(
            "{} was executed because either its query didn't match or its upstream data is out of date",
            node_noun(execution_type)
        ),
    }
}

/// Inputs distilled from a SubmitEnrichedSQLRequest for the decision.
///
/// `input_tables` carry each current input's resolved producer (ADR 0007).
/// `tolerance_ms` is the node's Lag tolerance and `build_age_ms` the reuse
/// candidate's build age on the server clock, both computed at the service
/// boundary so this engine stays pure.
pub struct SubmitContext<'a> {
    pub execution_type: i32,
    pub input_tables: &'a [InputTable],
    /// The node's own target table (fully-qualified). Its presence in
    /// `input_tables` is for own-existence/freshness tracking and must NOT be
    /// treated as upstream drift.
    pub target_table: Option<&'a str>,
    pub stale_upstream_policy: StaleUpstreamPolicy,
    /// Whether the server has ANY confirmed prior build of this physical target
    /// (regardless of fingerprint/provenance). Distinguishes a genuine FIRST
    /// build ("did not exist") from a REBUILD whose logic fingerprint no longer
    /// matches ("query didn't match or upstream out of date"): hosted reports
    /// the rebuild rationale when the target existed, even if no fingerprint
    /// matched. Review #2 5th follow-up P1 #4.
    pub target_has_prior_build: bool,
    /// Why reuse candidate selection rejected the latest row in scope. `None`
    /// when there was no latest row (a first build) or a candidate was found.
    pub why_no_candidate: Option<NoCandidate>,
    /// `freshness_tolerance_seconds` in milliseconds (0: none).
    pub tolerance_ms: i64,
    /// Server clock minus the reuse candidate's build time; `None` when there
    /// is no candidate or its build time is unknown.
    pub build_age_ms: Option<i64>,
    /// The submit does not report the node's own target: it does not exist.
    /// A rebuild of an absent target is described as "did not exist".
    pub own_target_absent: bool,
}

/// Decide SKIP vs EXECUTE given the reuse candidate (if any; see
/// `reuse_candidate`).
pub fn decide(ctx: &SubmitContext, candidate: Option<&ExecutionRow>) -> Verdict {
    match candidate {
        // No reuse candidate. The target either
        // never existed (first build → "did not exist") or existed under a
        // different fingerprint (rebuild → "query didn't match / out of date").
        // `target_has_prior_build` carries the physical-existence signal.
        None => execute(
            ctx.execution_type,
            ctx.target_has_prior_build && !ctx.own_target_absent,
            ctx.why_no_candidate
                .map_or(DecisionReason::FirstBuild, DecisionReason::NoReuseCandidate),
        ),
        Some(prev) => {
            let f = evaluate_freshness(ctx, prev);
            match f.rebuild {
                Some(reason) => execute(ctx.execution_type, true, reason),
                // Reuse. is_stale is true whenever upstream drift is present:
                // tolerated by the ALL policy, or within the Lag tolerance.
                None => Verdict::Skip {
                    description: skip_description(ctx.execution_type, &f, ctx),
                    is_stale: f.drift_present,
                    reason: DecisionReason::Reused(f.reuse),
                },
            }
        }
    }
}

/// How fresh `build` would be if reused for this submit, or `None` if the
/// submit would rebuild it. Used to report an older build of the target that
/// would still qualify, and to choose an equivalent to copy (both judged as
/// if `build` were the reuse candidate, with `ctx.build_age_ms` its age).
pub fn reusable(ctx: &SubmitContext, build: &ExecutionRow) -> Option<Reuse> {
    let f = evaluate_freshness(ctx, build);
    f.rebuild.is_none().then_some(f.reuse)
}

/// Result of the freshness evaluation: why to rebuild, if the evidence
/// requires it; otherwise how fresh the reuse is. `drift_present` feeds the
/// hosted is_stale and `upstreams_unchanged` the skip description.
struct Freshness {
    rebuild: Option<DecisionReason>,
    reuse: Reuse,
    drift_present: bool,
    /// Every upstream's current epoch equals its recorded epoch (vacuously
    /// true with no upstreams).
    upstreams_unchanged: bool,
}

fn execute(execution_type: i32, had_prior: bool, reason: DecisionReason) -> Verdict {
    // VERIFIED LIVE (2026-11-10/11, explain_snapshot_history.jsonl entry 58,
    // explain_execution_type_change.jsonl): a build of the workload's identity
    // under another execution type is rejected as EXECUTION_TYPE_MISMATCH.
    let rejection = match reason {
        DecisionReason::NoReuseCandidate(NoCandidate::ExecutionTypeChanged) => {
            REJECTION_EXECUTION_TYPE_MISMATCH
        }
        _ => REJECTION_NO_SUITABLE_MATCH_FOUND,
    };
    Verdict::Execute {
        description: execute_description(execution_type, had_prior, reason),
        skip_rejection_reason: rejection,
        clone_rejection_reason: rejection,
        // VERIFIED LIVE: the hosted service reports is_stale=FALSE on every
        // EXECUTE, including an upstream-drift-driven rebuild (lag fixture entry
        // 5, and the multi-upstream ANY captures). is_stale is only ever true on
        // a tolerated-stale SKIP. The rebuild reason is carried by srr/crr, not
        // is_stale.
        is_stale: false,
        reason,
    }
}

/// Metadata is already warehouse-qualified by clients. Preserve its physical
/// identity exactly: schemas and quoted case distinguish genuine dependencies.
/// Cross-environment mapping needs explicit provenance, not schema stripping.
pub(crate) fn physical_relation_key(name: &str) -> &str {
    name
}

/// The FRESH/OUTDATED verdict for a single genuine upstream parent, together
/// with the two epochs the verdict was derived from. `drifted == true` means
/// OUTDATED (the explain `OUTDATED` badge); `false` means FRESH.
///
/// `anchor_epoch` is the epoch this upstream was compared against with strict
/// `>`: the epoch recorded for it at the reuse candidate's build (ADR 0007).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpstreamVerdict {
    pub name: String,
    pub current_epoch: Option<i64>,
    pub anchor_epoch: i64,
    pub drifted: bool,
    /// True when this upstream's freshness could not be PROVEN (unknown current
    /// or recorded epoch, or an ambiguous recorded baseline). The fold forces
    /// EXECUTE when any upstream is unprovable (an unprovable upstream is
    /// always also `drifted`).
    pub unprovable: bool,
    /// The build that produced this upstream's contents differs from the one
    /// the reuse candidate consumed (ADR 0007): hosted's "query or upstream
    /// query changed". Never set when either side's producer is unrecorded.
    pub producer_changed: bool,
}

/// Why the per-upstream freshness evidence resolved the way it did. These are
/// the mutually-exclusive outcomes `decide` folds into SKIP/EXECUTE and the
/// explain path renders as the "data freshness analysis" branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FreshnessEvidence {
    /// The genuine upstream name set changed (added/removed/ambiguous). The
    /// recorded dependency evidence no longer applies → safety rebuild.
    DependencySetChanged,
    /// No genuine upstreams to compare. A hash/identity match alone is safe to
    /// skip; there is no freshness axis to violate.
    NoUpstreams,
    /// An object-producing node has upstreams but no recorded own-build epoch,
    /// so freshness cannot be proven → safety rebuild.
    NoBuildAnchor,
    /// The upstreams were evaluated against an anchor: per-upstream verdicts and
    /// the considered/drifted counts are populated.
    Evaluated,
}

/// Shared, pure per-upstream freshness reasoning: the single source of truth for
/// the per-upstream FRESH/OUTDATED verdict and the considered/drifted counts.
/// It also detects a changed genuine upstream name set (added/removed/ambiguous)
/// and short-circuits to `DependencySetChanged`; the hosted service surfaces no
/// per-name diff for that case (#32), so the changed names are not retained.
/// Both `decide` (via `evaluate_freshness`) and the explain generator (ADR 0005)
/// call THIS helper, so the decision and its explanation can never drift. Pure:
/// no I/O, no DB, no async.
///
/// This returns the raw per-upstream signals WITHOUT applying the ANY/ALL
/// `stale_upstream_policy`; policy folding into a rebuild/skip is the caller's
/// job (`evaluate_freshness`), so the explain path sees the same per-parent
/// verdicts the policy was applied to.
///
/// The per-`UpstreamVerdict` `name`/`current_epoch` and this struct's
/// `upstreams`/`anchor_epoch`/`considered`/`drifted` are now consumed by the
/// explain generator (ADR 0005, ticket #29/#30 anchor render path) and the
/// decision fold, so the former struct-level dead-code allow is removed.
#[derive(Debug, Clone)]
pub(crate) struct UpstreamFreshness {
    pub evidence: FreshnessEvidence,
    /// Per-genuine-upstream verdicts; populated only when `Evaluated`.
    pub upstreams: Vec<UpstreamVerdict>,
    /// Whether the node is a data test, which has no own build and gates
    /// ANY-style regardless of the configured policy.
    pub data_test: bool,
    /// Number of genuine upstreams considered (len of `upstreams` when Evaluated).
    pub considered: usize,
    /// Number of considered upstreams that drifted (OUTDATED).
    pub drifted: usize,
}

/// What per-upstream freshness is assessed for, all of it recorded with the
/// decision: the node kind, its own target and its decision-time inputs. The
/// Submit decision takes it from its [`SubmitContext`]; Explain takes it from
/// the persisted node decision, so re-derivation needs no decision policy.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FreshnessInputs<'a> {
    pub execution_type: i32,
    pub input_tables: &'a [InputTable],
    pub target_table: Option<&'a str>,
}

impl<'a> From<&SubmitContext<'a>> for FreshnessInputs<'a> {
    fn from(ctx: &SubmitContext<'a>) -> Self {
        Self {
            execution_type: ctx.execution_type,
            input_tables: ctx.input_tables,
            target_table: ctx.target_table,
        }
    }
}

pub(crate) fn upstream_freshness<'a>(
    node: impl Into<FreshnessInputs<'a>>,
    prev: &ExecutionRow,
) -> UpstreamFreshness {
    let node: FreshnessInputs<'a> = node.into();
    let genuine = |name: &str| node.target_table != Some(name);
    let data_test = node.execution_type == ModelExecutionType::DbtDataTest as i32;
    let outcome = |evidence| UpstreamFreshness {
        evidence,
        upstreams: Vec::new(),
        data_test,
        considered: 0,
        drifted: 0,
    };

    // Name-set diff over GENUINE upstreams (the node's own target excluded).
    let current_names: std::collections::BTreeSet<&str> = node
        .input_tables
        .iter()
        .filter(|t| genuine(&t.name))
        .map(|t| t.name.as_str())
        .collect();
    let recorded_names: std::collections::BTreeSet<&str> = prev
        .input_tables
        .iter()
        .filter(|t| genuine(&t.name))
        .map(|t| t.name.as_str())
        .collect();
    let genuine_count = node
        .input_tables
        .iter()
        .filter(|t| genuine(&t.name))
        .count();
    // Missing, additional or ambiguous dependency evidence: the recorded run no
    // longer describes this submit's dependency set → safety rebuild. We only
    // need to KNOW the set changed, not WHICH names changed: #32 captured that
    // the hosted service renders no dependency-set-diff subtree (it surfaces a
    // node/upstream-query change by physical FQN and never lists the
    // added/removed/ambiguous names), so the diff itself has no faithful reader.
    // See docs/explain-catalog.md § "Not rendered" (Dependency-set change).
    let added = !current_names.is_subset(&recorded_names);
    let removed = !recorded_names.is_subset(&current_names);
    // Fewer distinct current names than genuine inputs ⇒ duplicate/ambiguous.
    let ambiguous = current_names.len() != genuine_count;
    if added || removed || ambiguous {
        return outcome(FreshnessEvidence::DependencySetChanged);
    }

    // The genuine upstreams to evaluate (own target excluded). A node with none
    // (a source-less model, or a data test with no input freshness) is never
    // stale on this axis — a hash/identity match alone is safe to skip.
    let genuine_upstreams: Vec<&InputTable> = node
        .input_tables
        .iter()
        .filter(|t| genuine(&t.name))
        .collect();
    if genuine_upstreams.is_empty() {
        return outcome(FreshnessEvidence::NoUpstreams);
    }

    // An object-producing node with no recorded build epoch cannot be shown to
    // exist as built; data tests have no own object and record none.
    if prev.last_modified_epoch.is_none() && !data_test {
        return outcome(FreshnessEvidence::NoBuildAnchor);
    }

    // Per-upstream verdicts against the epoch RECORDED for each upstream at the
    // reuse candidate's build (ADR 0007, verified live 2026-11-10: a change
    // between the build's submit and its confirmation still counts). Evidence
    // that cannot establish freshness (unknown current or recorded epoch, an
    // ambiguous recorded baseline) is OUTDATED and unprovable, which always
    // rebuilds.
    let mut upstreams = Vec::with_capacity(genuine_upstreams.len());
    let mut drifted = 0usize;
    for current in &genuine_upstreams {
        // A confirmed row may record the SAME physical relation more than once
        // (the name-set check above collapses duplicates): we cannot prove
        // which epoch to compare against, so it is unprovable (F1 regression).
        let mut recorded = prev
            .input_tables
            .iter()
            .filter(|t| physical_relation_key(&t.name) == physical_relation_key(&current.name));
        let first_recorded = recorded.next();
        let ambiguous_recorded = recorded.next().is_some();
        let recorded_epoch = first_recorded.and_then(|t| t.last_modified_epoch);
        let (is_drift, unprovable) = match (
            current.last_modified_epoch,
            recorded_epoch,
            ambiguous_recorded,
        ) {
            (Some(cur), Some(rec), false) => (cur > rec, false),
            _ => (true, true),
        };
        let producer_changed = match (first_recorded.map(|t| &t.producer), &current.producer) {
            (Some(Producer::Unrecorded) | None, _) | (_, Producer::Unrecorded) => false,
            (Some(recorded), current) => recorded != current,
        };
        if is_drift {
            drifted += 1;
        }
        upstreams.push(UpstreamVerdict {
            name: current.name.clone(),
            current_epoch: current.last_modified_epoch,
            anchor_epoch: recorded_epoch.unwrap_or(0),
            drifted: is_drift,
            unprovable,
            producer_changed,
        });
    }

    let considered = upstreams.len();
    UpstreamFreshness {
        evidence: FreshnessEvidence::Evaluated,
        upstreams,
        data_test,
        considered,
        drifted,
    }
}

impl UpstreamFreshness {
    /// Every genuine upstream was evaluated and its current epoch equals the
    /// epoch recorded for it at the build: the condition for the Explain
    /// `upstream data is up to date` summary under a per-upstream listing.
    /// `false` with no upstreams or unprovable evidence.
    pub(crate) fn unchanged(&self) -> bool {
        self.evidence == FreshnessEvidence::Evaluated
            && !self.upstreams.is_empty()
            && self
                .upstreams
                .iter()
                .all(|u| u.current_epoch == Some(u.anchor_epoch) && !u.unprovable)
    }
}

/// Fold the shared per-upstream freshness signals into a rebuild/skip decision:
/// an upstream query change rebuilds; otherwise the ANY/ALL
/// `stale_upstream_policy` decides whether the node is stale, and a stale node
/// is still reused while its build is younger than the Lag tolerance. This is
/// the ONLY place policy and tolerance are applied; `upstream_freshness` stays
/// policy-neutral so the explain path can render the same per-parent verdicts.
fn evaluate_freshness(ctx: &SubmitContext, prev: &ExecutionRow) -> Freshness {
    let f = upstream_freshness(ctx, prev);
    let rebuild = |reason| Freshness {
        rebuild: Some(reason),
        reuse: Reuse::Fresh,
        drift_present: true,
        upstreams_unchanged: false,
    };
    match f.evidence {
        // Missing/ambiguous evidence or an unprovable anchor always rebuilds;
        // its is_stale is irrelevant (executes report false).
        FreshnessEvidence::DependencySetChanged => rebuild(DecisionReason::DependencySetChanged),
        FreshnessEvidence::NoBuildAnchor => rebuild(DecisionReason::NoBuildAnchor),
        // Nothing to compare → a hash match alone is safe to skip.
        FreshnessEvidence::NoUpstreams => Freshness {
            rebuild: None,
            reuse: Reuse::Fresh,
            drift_present: false,
            upstreams_unchanged: true,
        },
        FreshnessEvidence::Evaluated => {
            if f.upstreams.iter().any(|u| u.unprovable) {
                return rebuild(DecisionReason::UpstreamOutdated);
            }
            if f.upstreams.iter().any(|u| u.producer_changed) {
                return rebuild(DecisionReason::UpstreamQueryChanged);
            }
            let drift_present = f.drifted > 0;
            // A data test gates ANY-style on its tested data regardless of the
            // configured policy; object-producing nodes honor ANY/ALL.
            let stale = if f.data_test {
                drift_present
            } else {
                match ctx.stale_upstream_policy {
                    StaleUpstreamPolicy::Any => drift_present,
                    StaleUpstreamPolicy::All => f.drifted == f.considered,
                }
            };
            let reuse = if stale {
                if !within_lag_tolerance(ctx) {
                    return rebuild(DecisionReason::UpstreamOutdated);
                }
                Reuse::WithinLagTolerance
            } else if drift_present {
                Reuse::ToleratedStale
            } else {
                Reuse::Fresh
            };
            Freshness {
                rebuild: None,
                reuse,
                drift_present,
                upstreams_unchanged: f
                    .upstreams
                    .iter()
                    .all(|u| u.current_epoch == Some(u.anchor_epoch)),
            }
        }
    }
}

/// The client-reported node body hash, when the request carries node state.
pub fn node_body_hash_of(req: &qc::SubmitEnrichedSqlRequest) -> Option<String> {
    req.dbt_node_state
        .as_ref()
        .and_then(|s| s.node_body_hash.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(tables: &'a [InputTable]) -> SubmitContext<'a> {
        SubmitContext {
            execution_type: 10,
            input_tables: tables,
            target_table: None,
            stale_upstream_policy: StaleUpstreamPolicy::Any,
            target_has_prior_build: false,
            why_no_candidate: None,
            tolerance_ms: 0,
            build_age_ms: None,
            own_target_absent: false,
        }
    }

    fn confirmed(hash: &str, tables: Vec<InputTable>, built: Option<i64>) -> ExecutionRow {
        ExecutionRow {
            id: 1,
            org_id: "o".into(),
            target_table: "t".into(),
            execution_type: 10,
            node_body_hash: Some(hash.into()),
            node_sql_hash: None,
            node_namespace_logic_hash: None,
            table_namespace: Some("ns".into()),
            node_unique_id: None,
            last_modified_epoch: built,
            execution_runtime_ms: None,
            execution_results: None,
            input_tables: tables,
            status: "confirmed".into(),
            request_id: "r".into(),
            clone_source_table: None,
            ..ExecutionRow::default()
        }
    }

    fn tbl(name: &str, epoch: i64) -> InputTable {
        InputTable {
            name: name.into(),
            last_modified_epoch: Some(epoch),
            ..Default::default()
        }
    }

    fn desc_of(v: &Verdict) -> String {
        match v {
            Verdict::Skip { description, .. } => description.clone(),
            Verdict::Execute { description, .. } => description.clone(),
        }
    }

    /// Node-type/reason-specific decision_description strings, pinned to the
    /// exact wording captured live from api.state.dbt.com.
    /// A data test has no physical target, so the server does not look up
    /// target existence for it (`reuse_candidate::for_data_test` reports
    /// `false`). Pin that its EXECUTE response would be the same either way.
    #[test]
    fn data_test_execute_ignores_target_existence() {
        let up = vec![tbl("a", 100)];
        let with = |prior: bool| {
            let mut c = data_test_ctx(&up);
            c.target_has_prior_build = prior;
            decide(&c, None)
        };
        assert_eq!(with(false), with(true));
    }

    #[test]
    fn decision_descriptions_match_hosted_wording() {
        let up = vec![tbl("a", 100)];
        // First build (no prior) → "did not exist" per node kind.
        let first = |et: i32| {
            let mut c = ctx(&up);
            c.execution_type = et;
            desc_of(&decide(&c, None))
        };
        assert_eq!(
            first(1),
            "model was executed because its table did not exist"
        );
        assert_eq!(
            first(10),
            "model was executed because its table did not exist"
        );
        assert_eq!(
            first(7),
            "snapshot was executed because its table did not exist"
        );
        assert_eq!(
            first(8),
            "data test was executed because it has no prior execution or its query changed"
        );
        assert_eq!(first(9), "seed was loaded because it did not exist");

        // Rebuild of an existing node (stale upstream) → "query didn't match …".
        let prev = confirmed("h", vec![tbl("a", 100)], Some(100));
        let drift = vec![tbl("a", 9_000_000)];
        let stale = |et: i32| {
            let mut c = ctx(&drift);
            c.execution_type = et;
            desc_of(&decide(&c, Some(&prev)))
        };
        assert_eq!(
            stale(1),
            "model was executed because either its query didn't match or its upstream data is out of date"
        );
        assert_eq!(
            stale(7),
            "snapshot was executed because either its query didn't match or its upstream data is out of date"
        );
        assert_eq!(stale(9), "seed was loaded because its data changed");

        // Skip (confirmed + fresh) → node-kind no-op wording.
        let fresh = vec![tbl("a", 100)];
        let skip = |et: i32| {
            let mut c = ctx(&fresh);
            c.execution_type = et;
            desc_of(&decide(&c, Some(&prev)))
        };
        assert_eq!(
            skip(1),
            "model was a no-op because both its query and its upstream data are up to date"
        );
        assert_eq!(
            skip(8),
            "data test was a no-op because both its query and its upstream data are up to date"
        );
        assert_eq!(
            skip(7),
            "snapshot was a no-op because both its query and its upstream data are up to date"
        );
        assert_eq!(skip(9), "seed was a no-op because its data has not changed");

        // Own target modified after the build: a view's wording names its
        // definition (VERIFIED LIVE 2026-11-11, explain_reason_kind_cells.jsonl `vc0`).
        let modified = DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetModified);
        assert_eq!(
            execute_description(1, true, modified),
            "model was executed because the table was modified after the cached execution"
        );
        assert_eq!(
            execute_description(7, true, modified),
            "snapshot was executed because the table was modified after the cached execution"
        );
        assert_eq!(
            execute_description(10, true, modified),
            "model was executed because the view definition is newer than the cached execution"
        );
    }

    #[test]
    fn physical_relation_key_preserves_schema() {
        assert_ne!(
            physical_relation_key("\"DB\".\"PROD_SCHEMA\".\"T\""),
            physical_relation_key("\"DB\".\"DEV_SCHEMA\".\"T\"")
        );
        assert_eq!(
            physical_relation_key("\"DB\".\"S\".\"T\""),
            "\"DB\".\"S\".\"T\""
        );
        assert_eq!(physical_relation_key("schema.table"), "schema.table");
    }

    /// Different physical upstreams require explicit cross-environment provenance.
    #[test]
    fn cross_environment_upstream_requires_explicit_mapping() {
        let recorded = vec![tbl("\"DB\".\"PROD\".\"CUSTOMERS\"", 1_000_000)];
        let current = vec![tbl("\"DB\".\"DEV\".\"CUSTOMERS\"", 500_000)]; // dev, older
        let prev = confirmed("h", recorded, Some(1_000_000));
        let v = decide(&ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "different physical inputs must not share evidence implicitly"
        );
    }

    #[test]
    fn no_history_executes_not_stale() {
        let v = decide(&ctx(&[]), None);
        match v {
            Verdict::Execute { is_stale, .. } => assert!(!is_stale),
            _ => panic!("expected execute"),
        }
    }

    #[test]
    fn match_with_fresh_inputs_skips() {
        let tables = vec![tbl("a", 100)];
        let prev = confirmed("h", tables.clone(), Some(100));
        let v = decide(&ctx(&tables), Some(&prev));
        assert!(matches!(v, Verdict::Skip { .. }));
    }

    #[test]
    fn match_with_newer_upstream_executes_stale() {
        let recorded = vec![tbl("a", 100)];
        let current = vec![tbl("a", 500)];
        let prev = confirmed("h", recorded, Some(100));
        let v = decide(&ctx(&current), Some(&prev));
        match v {
            // Upstream drift under ANY rebuilds; the hosted service reports
            // is_stale=FALSE on the execute (verified live, lag fixture entry 5).
            Verdict::Execute { is_stale, .. } => {
                assert!(!is_stale, "an execute always reports is_stale=false")
            }
            _ => panic!("expected execute"),
        }
    }

    #[test]
    fn self_table_freshness_change_still_skips() {
        let target = "\"DB\".\"S\".\"T\"";
        let recorded = vec![tbl(target, 100)];
        let current = vec![tbl(target, 999)];
        let mut prev = confirmed("h", recorded, Some(100));
        prev.target_table = target.to_string();
        let mut c = ctx(&current);
        c.target_table = Some(target);
        let v = decide(&c, Some(&prev));
        assert!(
            matches!(v, Verdict::Skip { .. }),
            "self-table change must skip"
        );
    }

    /// Drift is against the epoch RECORDED for the upstream at the build (ADR
    /// 0007), and the Lag tolerance absorbs it only while the build is younger
    /// than the tolerance. The 2026-10-11 captures where a 300s tolerance did
    /// not absorb a 120s gap were External modifications of a consumed build:
    /// a changed producer, which rebuilds regardless of tolerance
    /// (`changed_producer_rebuilds_regardless_of_policy_and_tolerance` below;
    /// ADR 0007).
    #[test]
    fn upstream_drift_executes_once_the_build_outgrows_the_tolerance() {
        // Recorded at 1_000_000; the upstream is now 1000ms newer.
        let recorded = vec![tbl("a", 1_000_000)];
        let current = vec![tbl("a", 1_001_000)];
        let prev = confirmed("h", recorded, Some(1_000_000));
        let executes = |c: &SubmitContext| {
            matches!(
                decide(c, Some(&prev)),
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            )
        };
        assert!(executes(&ctx(&current)), "tolerance 0");
        assert!(
            executes(&tolerant(&current, 300, 300)),
            "build as old as the tolerance"
        );
        assert!(
            !executes(&tolerant(&current, 300, 120)),
            "build younger than the tolerance"
        );
    }

    #[test]
    fn policy_any_executes_if_one_upstream_drifts() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // b drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::Any;
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Execute { .. }));
    }

    #[test]
    fn policy_all_skips_if_one_upstream_fresh() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // only b drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        // ALL policy: stale only if every upstream drifted; a is fresh → skip
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }));
    }

    #[test]
    fn policy_all_executes_if_all_upstreams_drift() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 900), tbl("b", 999)]; // both drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Execute { .. }));
    }

    // ---- is_stale reporting (VERIFIED LIVE, multi-upstream ALL policy) -----
    //
    // Live truth table (api.state.dbt.com, Fusion client, Snowflake, 2 upstreams
    // lp_up_a + lp_up_b, lag_experiment.sh):
    //   ANY, no drift          -> skip,    is_stale=false
    //   ANY, upstream drift     -> execute, is_stale=false
    //   ALL, one-of-two drift   -> skip,    is_stale=TRUE   (tolerated staleness)
    //   ALL, all drift          -> execute, is_stale=false
    // i.e. is_stale is true ONLY on a reuse that knowingly serves slightly stale
    // data: drift present but tolerated by the ALL policy or within the Lag
    // tolerance. It is NEVER set on an execute. See docs/protocol.md
    // and crates/harness/tests/lag_freshness.rs.

    /// ALL policy, only one of two parents drifted → SKIP with is_stale=TRUE.
    #[test]
    fn all_policy_partial_drift_skips_with_is_stale_true() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // only b drifted
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        match decide(&c, Some(&prev)) {
            Verdict::Skip { is_stale, .. } => {
                assert!(
                    is_stale,
                    "ALL-policy tolerated-staleness skip reports is_stale=true"
                )
            }
            _ => panic!("expected skip"),
        }
    }

    /// A no-drift skip reports is_stale=FALSE (both ANY and ALL).
    #[test]
    fn no_drift_skip_reports_is_stale_false() {
        let tables = vec![tbl("a", 100), tbl("b", 100)];
        let prev = confirmed("h", tables.clone(), Some(100));
        for policy in [StaleUpstreamPolicy::Any, StaleUpstreamPolicy::All] {
            let mut c = ctx(&tables);
            c.stale_upstream_policy = policy;
            match decide(&c, Some(&prev)) {
                Verdict::Skip { is_stale, .. } => {
                    assert!(!is_stale, "a fresh (no-drift) skip is not stale")
                }
                _ => panic!("expected skip"),
            }
        }
    }

    /// Under ANY policy, drift drives an EXECUTE and is_stale is FALSE (never set
    /// on an execute, even though drift is present).
    #[test]
    fn any_policy_drift_executes_with_is_stale_false() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)];
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::Any;
        assert!(matches!(
            decide(&c, Some(&prev)),
            Verdict::Execute {
                is_stale: false,
                ..
            }
        ));
    }

    // ---- Non-happy-path scenarios (hardening) ----------------------------

    /// MISSING TARGET / NO HISTORY: a confirmed row with `last_modified_epoch =
    /// None` (never recorded a build time) is not a safe baseline. A current
    /// upstream the recorded run never saw has no baseline at all → treat as
    /// drift → EXECUTE. The warehouse object may not exist, so we must not skip.
    #[test]
    fn unseen_upstream_without_baseline_executes() {
        let recorded: Vec<InputTable> = vec![]; // recorded run saw no inputs
        let current = vec![tbl("brand_new", 500)];
        let prev = confirmed("h", recorded, None); // no build epoch baseline
        let v = decide(&ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "an upstream with no recorded baseline must execute, not skip"
        );
    }

    /// FRESHNESS BOUNDARY (verified): drift is a strict `>` comparison against
    /// the OWN BUILD epoch, not the recorded-upstream epoch, and tolerance does
    /// not shift it. Upstream == own-build is fresh (skip); own-build + 1ms is
    /// stale (execute). A large tolerance does not move this boundary.
    #[test]
    fn freshness_boundary_is_own_build_strict_greater_than() {
        let prev = confirmed("h", vec![tbl("a", 1_000_000)], Some(1_000_000));

        // Upstream exactly equal to own build: NOT drift → skip (even with tol).
        let at = vec![tbl("a", 1_000_000)];
        let c = ctx(&at);
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }),
            "upstream equal to own build must skip"
        );

        // One ms newer than own build: drift → execute, tolerance notwithstanding.
        let beyond = vec![tbl("a", 1_000_001)];
        let c2 = ctx(&beyond);
        assert!(
            matches!(
                decide(&c2, Some(&prev)),
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            ),
            "upstream one ms newer than own build must execute even with 300s tolerance"
        );
    }

    /// ADR 0007 (VERIFIED LIVE 2026-11-10, `cdb_m`, lag_multi entry 11): an
    /// upstream drifts when it is newer than the epoch RECORDED for it at the
    /// build, even if it is older than the build's (client-stamped) confirm
    /// epoch.
    #[test]
    fn drift_is_against_the_recorded_upstream_epoch() {
        let target = "\"DB\".\"S\".\"M\"";
        let prev = confirmed("h", vec![tbl("a", 990_000)], Some(1_016_000));
        let current = vec![tbl(target, 1_000_000), tbl("a", 1_010_000)];
        let mut c = ctx(&current);
        c.target_table = Some(target);
        assert!(matches!(
            decide(&c, Some(&prev)),
            Verdict::Execute {
                is_stale: false,
                reason: DecisionReason::UpstreamOutdated,
                ..
            }
        ));
        // An upstream older than its recorded epoch (a client-stamp artifact)
        // is not drift.
        let current = vec![tbl(target, 1_000_000), tbl("a", 989_700)];
        let mut c = ctx(&current);
        c.target_table = Some(target);
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }));
    }

    fn tolerant<'a>(tables: &'a [InputTable], tol_s: i64, age_s: i64) -> SubmitContext<'a> {
        let mut c = ctx(tables);
        c.tolerance_ms = tol_s * 1000;
        c.build_age_ms = Some(age_s * 1000);
        c
    }

    const WITHIN: &str =
        "model was a no-op because its query is up to date and its data is within lag tolerance";
    const ATLEAST1: &str = "model was a no-op because its query is up to date and at least one upstream dependency is unchanged";

    /// VERIFIED LIVE (`recy_m`, `reco_m`, `recz_m`, `bacfwd_m`): a rebuild
    /// the policy requires is a reuse while the build is younger than the Lag
    /// tolerance, and a rebuild once it is not. A forward-dated build has a
    /// negative age and is within.
    #[test]
    fn lag_tolerance_gates_the_policy_rebuild_by_build_age() {
        let prev = confirmed("h", vec![tbl("a", 100)], Some(100));
        let current = vec![tbl("a", 5_000)];
        let skip = decide(&tolerant(&current, 600, 7), Some(&prev));
        assert_eq!(
            skip,
            Verdict::Skip {
                description: WITHIN.into(),
                is_stale: true,
                reason: DecisionReason::Reused(Reuse::WithinLagTolerance),
            }
        );
        assert!(matches!(
            decide(&tolerant(&current, 20, 34), Some(&prev)),
            Verdict::Execute {
                reason: DecisionReason::UpstreamOutdated,
                ..
            }
        ));
        assert!(matches!(
            decide(&tolerant(&current, 60, -98), Some(&prev)),
            Verdict::Skip { .. }
        ));
        // Unknown build age is never within the tolerance.
        let mut c = tolerant(&current, 600, 0);
        c.build_age_ms = None;
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Execute { .. }));
    }

    /// VERIFIED LIVE (`lk*`, `ext*`, `soe*`): a changed producer is an upstream
    /// query change and rebuilds regardless of ALL policy and tolerance.
    /// Unrecorded producers never count.
    #[test]
    fn changed_producer_rebuilds_regardless_of_policy_and_tolerance() {
        let with = |name: &str, epoch, producer| InputTable {
            producer,
            ..tbl(name, epoch)
        };
        let built = Producer::Build("sql:u0".into());
        let recorded = vec![
            with("a", 100, built.clone()),
            with("b", 100, Producer::NoBuild),
        ];
        let prev = confirmed("h", recorded, Some(100));
        let current = vec![
            with("a", 5_000, Producer::NoBuild),
            with("b", 100, Producer::NoBuild),
        ];
        let mut c = tolerant(&current, 600, 4);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        assert!(matches!(
            decide(&c, Some(&prev)),
            Verdict::Execute {
                is_stale: false,
                reason: DecisionReason::UpstreamQueryChanged,
                ..
            }
        ));
        // The same producer is plain data drift: ALL with one of two drifted
        // reuses.
        let current = vec![with("a", 5_000, built), with("b", 100, Producer::NoBuild)];
        let mut c = tolerant(&current, 600, 4);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        assert!(matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }));
        // An unrecorded side never counts.
        let legacy = confirmed("h", vec![tbl("a", 100), tbl("b", 100)], Some(100));
        let current = vec![with("a", 5_000, Producer::NoBuild), tbl("b", 100)];
        let mut c = tolerant(&current, 600, 4);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        assert!(matches!(decide(&c, Some(&legacy)), Verdict::Skip { .. }));
    }

    /// VERIFIED LIVE (`dsdec_*`, `dsall_m`, `pao_m`): the skip description.
    #[test]
    fn skip_description_follows_tolerance_and_build_age() {
        let prev = confirmed("h", vec![tbl("a", 1_000), tbl("b", 1_000)], Some(1_000));
        let desc = |current: &[InputTable], tol, age, policy| {
            let mut c = tolerant(current, tol, age);
            c.stale_upstream_policy = policy;
            match decide(&c, Some(&prev)) {
                Verdict::Skip {
                    description,
                    is_stale,
                    ..
                } => (description, is_stale),
                other => panic!("expected skip, got {other:?}"),
            }
        };
        let any = StaleUpstreamPolicy::Any;
        let all = StaleUpstreamPolicy::All;
        let unchanged = [tbl("a", 1_000), tbl("b", 1_000)];
        let decreased = [tbl("a", 700), tbl("b", 1_000)];
        let one_drifted = [tbl("a", 9_000), tbl("b", 1_000)];
        assert_eq!(desc(&unchanged, 300, 5, any), (DESC_SKIP.into(), false));
        assert_eq!(desc(&decreased, 0, 2, any), (DESC_SKIP.into(), false));
        assert_eq!(desc(&decreased, 20, 2, any), (WITHIN.into(), false));
        assert_eq!(desc(&decreased, 20, 25, any), (ATLEAST1.into(), false));
        assert_eq!(desc(&one_drifted, 0, 2, all), (DESC_SKIP.into(), true));
        assert_eq!(desc(&one_drifted, 300, 80, all), (WITHIN.into(), true));
        assert_eq!(desc(&one_drifted, 20, 25, all), (ATLEAST1.into(), true));
    }

    /// The SKIP description of a snapshot judged against `prev` (two
    /// upstreams at 1 000).
    fn snapshot_skip_desc(
        current: &[InputTable],
        tol: i64,
        age: i64,
        policy: StaleUpstreamPolicy,
    ) -> (String, bool) {
        let prev = confirmed("h", vec![tbl("a", 1_000), tbl("b", 1_000)], Some(1_000));
        let mut c = tolerant(current, tol, age);
        c.execution_type = ModelExecutionType::Snapshot as i32;
        c.stale_upstream_policy = policy;
        match decide(&c, Some(&prev)) {
            Verdict::Skip {
                description,
                is_stale,
                ..
            } => (description, is_stale),
            other => panic!("expected skip, got {other:?}"),
        }
    }

    const SNAPSHOT_BOTH: &str =
        "snapshot was a no-op because both its query and its upstream data are up to date";

    /// VERIFIED LIVE (2026-11-10, explain_snapshot_history.jsonl entries
    /// 29-31, 40, 43): a snapshot SKIP reads "both … up to date" for an epoch
    /// that moved back and for drift within its Lag tolerance, where a model
    /// says "within lag tolerance".
    #[test]
    fn snapshot_skip_description_does_not_vary_with_freshness() {
        let decreased = [tbl("a", 700), tbl("b", 1_000)];
        let one_drifted = [tbl("a", 9_000), tbl("b", 1_000)];
        let any = StaleUpstreamPolicy::Any;
        assert_eq!(
            snapshot_skip_desc(&decreased, 2_700, 9, any),
            (SNAPSHOT_BOTH.into(), false)
        );
        assert_eq!(
            snapshot_skip_desc(&one_drifted, 2_700, 26, any),
            (SNAPSHOT_BOTH.into(), true)
        );
    }

    /// LOCAL CHOICE, not hosted evidence: the ALL-policy snapshot reuse past
    /// its Lag tolerance was never observed, and client 2.0.11 never sends a
    /// snapshot the ALL policy (it reads `state` config from models only,
    /// #104). The server keeps the observed snapshot wording for it;
    /// docs/correctness-handoff.md "Snapshot SKIP wording past the Lag
    /// tolerance".
    #[test]
    fn unobserved_all_policy_snapshot_skip_past_tolerance_keeps_the_observed_wording() {
        let one_drifted = [tbl("a", 9_000), tbl("b", 1_000)];
        assert_eq!(
            snapshot_skip_desc(&one_drifted, 20, 25, StaleUpstreamPolicy::All),
            (SNAPSHOT_BOTH.into(), true)
        );
    }

    /// Different schema input cannot match recorded physical evidence.
    #[test]
    fn upstream_schema_change_requires_execution() {
        let recorded = vec![tbl("\"DB\".\"PROD\".\"CUSTOMERS\"", 1_000_000)];
        let current = vec![tbl("\"DB\".\"DEV\".\"CUSTOMERS\"", 5_000_000)]; // newer, diff schema
        let prev = confirmed("h", recorded, Some(1_000_000));
        let v = decide(&ctx(&current), Some(&prev));
        assert!(
            matches!(
                v,
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            ),
            "a different physical input requires execution"
        );
    }

    /// Missing previously recorded input metadata cannot establish freshness.
    #[test]
    fn dropped_all_upstreams_requires_execution() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current: Vec<InputTable> = vec![]; // all upstreams removed this run
        let prev = confirmed("h", recorded, Some(100));
        assert!(
            matches!(decide(&ctx(&current), Some(&prev)), Verdict::Execute { .. }),
            "missing dependency evidence requires execution"
        );
    }

    /// ADDED UPSTREAM: a brand-new upstream not in the recorded run, newer than
    /// the recorded build, falls back to the recorded build epoch and is drift
    /// → EXECUTE under ANY. The new dependency's data was never incorporated.
    #[test]
    fn added_newer_upstream_executes_under_any() {
        let recorded = vec![tbl("a", 100)];
        let current = vec![tbl("a", 100), tbl("new_dep", 10_000_000)];
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::Any;
        assert!(
            matches!(
                decide(&c, Some(&prev)),
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            ),
            "a new, newer, untracked upstream must execute"
        );
    }

    /// CHANGED CONTRACT / CHANGED SQL: a different body hash means the store
    /// never returns a matching confirmed row for this fingerprint; the engine
    /// sees `confirmed = None` and must EXECUTE as a hash miss (is_stale=false,
    /// rejection NO_SUITABLE_MATCH_FOUND).
    #[test]
    fn changed_contract_is_a_hash_miss_execute() {
        // A changed contract changes the identity, so no reuse candidate exists.
        let v = decide(&ctx(&[tbl("a", 100)]), None);
        match v {
            Verdict::Execute {
                is_stale,
                skip_rejection_reason,
                clone_rejection_reason,
                ..
            } => {
                assert!(!is_stale, "a hash miss is not staleness-driven");
                assert_eq!(skip_rejection_reason, REJECTION_NO_SUITABLE_MATCH_FOUND);
                assert_eq!(clone_rejection_reason, REJECTION_NO_SUITABLE_MATCH_FOUND);
            }
            _ => panic!("expected execute"),
        }
    }

    /// Zero tolerance with any positive drift must execute (the common
    /// "freshness must be exact" configuration).
    #[test]
    fn zero_tolerance_any_drift_executes() {
        let prev = confirmed("h", vec![tbl("a", 100)], Some(100));
        let current = vec![tbl("a", 101)]; // 1ms newer
        let c = ctx(&current);
        assert!(matches!(
            decide(&c, Some(&prev)),
            Verdict::Execute {
                is_stale: false,
                ..
            }
        ));
    }

    /// OLDER upstream than our own build (clock skew / restore) must NOT be
    /// treated as drift — only an upstream strictly newer than the own-build
    /// epoch forces a rebuild.
    #[test]
    fn older_upstream_than_recorded_skips() {
        let prev = confirmed("h", vec![tbl("a", 1_000_000)], Some(1_000_000));
        let current = vec![tbl("a", 10)]; // older than recorded
        assert!(matches!(
            decide(&ctx(&current), Some(&prev)),
            Verdict::Skip { .. }
        ));
    }

    // ---- DATA-TEST upstream freshness (VERIFIED LIVE, issue #2 finding 1) ----
    //
    // Live truth (api.state.dbt.com, Fusion client, Snowflake;
    // experiments/data_test_freshness{,_control}.sh): a data test
    // (execution_type = 8) is confirmed with NO own object epoch
    // (ConfirmExecution.last_modified_epoch = None, only test results). A later
    // submit whose upstream input epoch is NEWER than the epoch recorded for
    // that upstream at the test's last confirmation EXECUTES ("data test was
    // executed because it has no prior execution or its query changed"); an
    // identical re-submit with the SAME upstream epoch SKIPS ("…a no-op because
    // both its query and its upstream data are up to date"). So a data test's
    // freshness anchor is the RECORDED UPSTREAM epoch (it has no own-build
    // epoch), compared per input with strict `>`.
    // Golden fixture: golden/fixtures/data_test_upstream_freshness.jsonl.

    fn data_test_ctx<'a>(tables: &'a [InputTable]) -> SubmitContext<'a> {
        SubmitContext {
            execution_type: ModelExecutionType::DbtDataTest as i32,
            input_tables: tables,
            target_table: None,
            stale_upstream_policy: StaleUpstreamPolicy::Any,
            target_has_prior_build: false,
            why_no_candidate: None,
            tolerance_ms: 0,
            build_age_ms: None,
            own_target_absent: false,
        }
    }

    /// A data test confirmed with no own epoch must EXECUTE when its upstream is
    /// newer than the epoch recorded at its last confirmation (VERIFIED LIVE).
    #[test]
    fn data_test_executes_when_upstream_newer_than_recorded() {
        let recorded = vec![tbl("DB.S.INPUT", 100)];
        let mut prev = confirmed("h", recorded, None); // tests have no build epoch
        prev.execution_type = ModelExecutionType::DbtDataTest as i32;
        let current = vec![tbl("DB.S.INPUT", 200)]; // upstream advanced
        let v = decide(&data_test_ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "a data test must re-run when its upstream data changed, even with no own epoch"
        );
    }

    /// Boundary: upstream exactly equal to the recorded epoch is fresh → SKIP
    /// (VERIFIED LIVE control arm).
    #[test]
    fn data_test_skips_when_upstream_equal_to_recorded() {
        let recorded = vec![tbl("DB.S.INPUT", 200)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ModelExecutionType::DbtDataTest as i32;
        let current = vec![tbl("DB.S.INPUT", 200)]; // unchanged
        let v = decide(&data_test_ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Skip { .. }),
            "an unchanged data test (same upstream epoch) must skip"
        );
    }

    /// An older-or-equal upstream is still fresh (clock skew / restore): only a
    /// strictly newer upstream re-runs the test.
    #[test]
    fn data_test_skips_when_upstream_older_than_recorded() {
        let recorded = vec![tbl("DB.S.INPUT", 200)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ModelExecutionType::DbtDataTest as i32;
        let current = vec![tbl("DB.S.INPUT", 50)];
        assert!(matches!(
            decide(&data_test_ctx(&current), Some(&prev)),
            Verdict::Skip { .. }
        ));
    }

    /// A data test whose upstream carries an UNKNOWN (None) epoch cannot be
    /// proven fresh → EXECUTE (we never skip on unprovable freshness).
    #[test]
    fn data_test_executes_when_upstream_epoch_unknown() {
        let recorded = vec![tbl("DB.S.INPUT", 200)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ModelExecutionType::DbtDataTest as i32;
        let current = vec![InputTable {
            name: "DB.S.INPUT".into(),
            last_modified_epoch: None,
            ..Default::default()
        }];
        assert!(matches!(
            decide(&data_test_ctx(&current), Some(&prev)),
            Verdict::Execute { .. }
        ));
    }

    /// A data test with NO genuine upstream inputs (e.g. a test over a seed or a
    /// source-less assertion) still skips on a pure identity match — there is no
    /// upstream freshness axis to violate.
    #[test]
    fn data_test_without_upstreams_still_skips_on_match() {
        let mut prev = confirmed("h", vec![], None);
        prev.execution_type = ModelExecutionType::DbtDataTest as i32;
        let current: Vec<InputTable> = vec![];
        assert!(matches!(
            decide(&data_test_ctx(&current), Some(&prev)),
            Verdict::Skip { .. }
        ));
    }

    /// A data test over MULTIPLE upstreams re-runs if ANY one drifted past its
    /// recorded epoch (tests gate on their tested data, ANY-style).
    #[test]
    fn data_test_executes_if_any_upstream_drifts() {
        let recorded = vec![tbl("DB.S.A", 100), tbl("DB.S.B", 100)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ModelExecutionType::DbtDataTest as i32;
        let current = vec![tbl("DB.S.A", 100), tbl("DB.S.B", 999)]; // B drifted
        assert!(matches!(
            decide(&data_test_ctx(&current), Some(&prev)),
            Verdict::Execute { .. }
        ));
    }

    /// EPOCH MAGNITUDE INVARIANT: every `last_modified_epoch` (upstream tables,
    /// recorded build time, ConfirmExecution) is in MILLISECONDS — as sent by
    /// the real client and seen in the golden fixtures (13-digit values
    /// ~1.79e12). Freshness compares the upstream epoch directly against the
    /// own-build epoch in those native millisecond units; a 1ms difference is
    /// decisive. This pins that the comparison is not accidentally scaled or
    /// unit-mixed. `freshness_tolerance_seconds` is deliberately irrelevant here
    /// (verified live: it does not absorb upstream drift).
    #[test]
    fn freshness_compares_millisecond_epochs_against_own_build() {
        // Realistic millisecond epoch (matches golden fixture magnitudes).
        let base = 1_791_320_539_084i64;
        let prev = confirmed("h", vec![tbl("a", base)], Some(base));

        // Upstream equal to own build (to the ms) → fresh, even with tolerance.
        let at = vec![tbl("a", base)];
        let c = ctx(&at);
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Skip { .. }),
            "upstream equal to own build (ms) is fresh"
        );

        // One millisecond newer than own build → stale, tolerance notwithstanding.
        let beyond = vec![tbl("a", base + 1)];
        let c2 = ctx(&beyond);
        assert!(
            matches!(
                decide(&c2, Some(&prev)),
                Verdict::Execute {
                    is_stale: false,
                    ..
                }
            ),
            "upstream 1ms newer than own build is stale regardless of tolerance"
        );
    }

    // ---- Shared pure helper: `upstream_freshness` (#28) --------------------
    //
    // These characterize the STRUCTURE the explain generator renders (per-ADR
    // 0005): the per-upstream FRESH/OUTDATED verdict, the considered/drifted
    // counts, and the dependency-set-change short-circuit. `decide` and the
    // explain path both call this ONE helper, so its returned struct is the
    // single source of truth for the "data freshness analysis" lines. The
    // proptests in decision_proptest.rs remain the regression oracle for
    // `decide` itself.

    /// Clean per-upstream evaluation: one fresh, one outdated, against the
    /// node's OWN build epoch (strict `>`). Counts and per-upstream verdicts are
    /// exposed; the clean (unchanged) name set reaches `Evaluated`.
    #[test]
    fn upstream_freshness_reports_per_upstream_verdicts_and_counts() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![tbl("a", 100), tbl("b", 999)]; // b drifted
        let prev = confirmed("h", recorded, Some(100));
        let f = upstream_freshness(&ctx(&current), &prev);

        // A clean, unchanged name set does not short-circuit to
        // DependencySetChanged; it reaches the per-upstream evaluation.
        assert!(matches!(f.evidence, FreshnessEvidence::Evaluated));
        assert!(!f.data_test);
        assert_eq!(f.considered, 2);
        assert_eq!(f.drifted, 1);

        // Per-upstream FRESH/OUTDATED verdicts are returned, not discarded.
        let a = f.upstreams.iter().find(|u| u.name == "a").unwrap();
        let b = f.upstreams.iter().find(|u| u.name == "b").unwrap();
        assert!(!a.drifted, "a (epoch 100 == own build) is FRESH");
        assert!(b.drifted, "b (epoch 999 > own build 100) is OUTDATED");
        assert_eq!(a.current_epoch, Some(100));
        assert_eq!(b.current_epoch, Some(999));
    }

    /// A changed genuine upstream name set short-circuits to
    /// `DependencySetChanged` regardless of HOW it changed (added, removed, or
    /// duplicate/ambiguous). #32 captured that the hosted service surfaces no
    /// per-name diff for this case, so the helper no longer retains WHICH names
    /// changed — only that the set is dirty drives the rebuild.
    #[test]
    fn upstream_freshness_reports_name_set_change() {
        let is_set_change = |rec: Vec<InputTable>, cur: &[InputTable]| {
            let prev = confirmed("h", rec, Some(100));
            let f = upstream_freshness(&ctx(cur), &prev);
            matches!(f.evidence, FreshnessEvidence::DependencySetChanged)
        };

        // ADDED: a current upstream the recorded run never saw.
        assert!(
            is_set_change(vec![tbl("a", 100)], &[tbl("a", 100), tbl("new_dep", 100)]),
            "an added genuine upstream is a dependency-set change"
        );
        // REMOVED: a recorded upstream absent from the current submit.
        assert!(
            is_set_change(vec![tbl("a", 100), tbl("gone", 100)], &[tbl("a", 100)]),
            "a removed genuine upstream is a dependency-set change"
        );
        // AMBIGUOUS: the same current name appears twice (distinct count < inputs).
        assert!(
            is_set_change(vec![tbl("a", 100)], &[tbl("a", 100), tbl("a", 200)]),
            "a duplicate/ambiguous current upstream is a dependency-set change"
        );
    }

    /// No genuine upstreams → the helper reports `NoUpstreams`: a hash match
    /// alone is safe to skip, nothing to render on the freshness axis.
    #[test]
    fn upstream_freshness_reports_no_upstreams() {
        let prev = confirmed("h", vec![], Some(100));
        let f = upstream_freshness(&ctx(&[]), &prev);
        assert!(matches!(f.evidence, FreshnessEvidence::NoUpstreams));
        assert_eq!(f.considered, 0);
        assert_eq!(f.drifted, 0);
    }

    /// An object-producing node with upstreams but no recorded own-build epoch
    /// cannot prove freshness → `NoBuildAnchor`.
    #[test]
    fn upstream_freshness_reports_missing_build_anchor() {
        let recorded = vec![tbl("a", 100)];
        let current = vec![tbl("a", 100)];
        let prev = confirmed("h", recorded, None); // no own-build epoch
        let f = upstream_freshness(&ctx(&current), &prev);
        assert!(matches!(f.evidence, FreshnessEvidence::NoBuildAnchor));
    }

    /// A data test (no own-build epoch) anchors on the RECORDED UPSTREAM epoch
    /// per input; a drifted upstream is OUTDATED against its own recorded
    /// baseline, not an own-build epoch.
    #[test]
    fn upstream_freshness_data_test_anchors_on_recorded_upstream() {
        let recorded = vec![tbl("DB.S.INPUT", 100)];
        let mut prev = confirmed("h", recorded, None);
        prev.execution_type = ModelExecutionType::DbtDataTest as i32;
        let current = vec![tbl("DB.S.INPUT", 200)]; // newer than recorded 100
        let mut c = ctx(&current);
        c.execution_type = ModelExecutionType::DbtDataTest as i32;
        let f = upstream_freshness(&c, &prev);

        assert!(matches!(f.evidence, FreshnessEvidence::Evaluated));
        assert_eq!(f.considered, 1);
        assert_eq!(f.drifted, 1);
        let u = &f.upstreams[0];
        assert_eq!(
            u.anchor_epoch, 100,
            "data test anchors on recorded upstream"
        );
        assert!(u.drifted, "200 > recorded 100 is OUTDATED");
    }

    /// REGRESSION (behavior parity): under the ALL policy, an upstream whose
    /// CURRENT epoch is unknown cannot be proven fresh. The pre-extraction
    /// engine short-circuited to a rebuild on the first such upstream REGARDLESS
    /// of policy (it never reached the ANY/ALL fold). The extracted helper must
    /// preserve that: an unprovable upstream forces EXECUTE even when a sibling
    /// is fresh and the policy is ALL (which would otherwise require every
    /// parent to drift). This is the exact edge the `tables_strategy` proptests
    /// never generate (they only emit `Some` epochs), so it is pinned here.
    #[test]
    fn unprovable_current_epoch_executes_under_all_policy() {
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let current = vec![
            tbl("a", 100), // fresh
            InputTable {
                name: "b".into(),
                last_modified_epoch: None, // unknown → unprovable
                ..Default::default()
            },
        ];
        let prev = confirmed("h", recorded, Some(100));
        let mut c = ctx(&current);
        c.stale_upstream_policy = StaleUpstreamPolicy::All;
        assert!(
            matches!(decide(&c, Some(&prev)), Verdict::Execute { .. }),
            "an unprovable upstream epoch must execute regardless of ALL policy"
        );
    }

    // ---- DUPLICATE RECORDED UPSTREAM (ambiguous baseline) — F1 regression ----
    //
    // The pre-extraction engine (`main`) rejected a confirmed baseline that
    // recorded MORE THAN ONE counterpart for a single current upstream (same
    // `physical_relation_key`) in BOTH `evaluate_freshness` (object nodes) and
    // `data_test_upstream_freshness`: `matches.next().is_some()` after the first
    // match forced a safety rebuild — the recorded baseline is ambiguous, so we
    // cannot prove which epoch to anchor against and must not skip. The #28
    // refactor replaced that per-upstream uniqueness check with a single
    // `.find()` (takes the FIRST recorded row), silently anchoring on whichever
    // duplicate comes first — a skip where `main` rebuilt (unsafe reuse). The
    // name-set diff does NOT catch this: `recorded_names` is a BTreeSet so the
    // duplicate collapses to one entry and `ambiguous` only measures the CURRENT
    // side. These pin the restored behavior for an object node AND a data test.

    /// OBJECT NODE: a confirmed row recording two physical counterparts for the
    /// same upstream (`a@200`, `a@50`) is an ambiguous baseline. A clean current
    /// submit (`a@100`, older than both) must REBUILD, not anchor on the first
    /// recorded row and SKIP. (`main` parity: `evaluate_freshness` uniqueness.)
    #[test]
    fn duplicate_recorded_upstream_executes_object_node() {
        let recorded = vec![tbl("a", 200), tbl("a", 50)]; // same name twice
        let current = vec![tbl("a", 100)]; // clean single current upstream
        let prev = confirmed("h", recorded, Some(100));
        let v = decide(&ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "a duplicate recorded upstream is an ambiguous baseline → rebuild"
        );
    }

    /// DATA TEST: the same ambiguity on a data test (no own-build epoch, which
    /// anchors per-upstream on the RECORDED baseline). Two recorded `a` rows
    /// mean no single provable baseline → EXECUTE. (`main` parity:
    /// `data_test_upstream_freshness` uniqueness.)
    #[test]
    fn duplicate_recorded_upstream_executes_data_test() {
        let recorded = vec![tbl("DB.S.A", 200), tbl("DB.S.A", 50)]; // same name twice
        let mut prev = confirmed("h", recorded, None); // data tests have no build epoch
        prev.execution_type = ModelExecutionType::DbtDataTest as i32;
        let current = vec![tbl("DB.S.A", 100)];
        let v = decide(&data_test_ctx(&current), Some(&prev));
        assert!(
            matches!(v, Verdict::Execute { .. }),
            "a data test with a duplicate recorded upstream has no provable baseline → rebuild"
        );
    }

    /// The ambiguity is surfaced on the shared helper itself: the matched
    /// upstream is `unprovable` and `drifted`, so the fold forces EXECUTE. This
    /// pins the mechanism (not just the folded verdict) the explain path reads.
    #[test]
    fn upstream_freshness_marks_duplicate_recorded_unprovable() {
        let recorded = vec![tbl("a", 200), tbl("a", 50)];
        let current = vec![tbl("a", 100)];
        let prev = confirmed("h", recorded, Some(100));
        let f = upstream_freshness(&ctx(&current), &prev);

        assert!(matches!(f.evidence, FreshnessEvidence::Evaluated));
        assert_eq!(f.considered, 1);
        let u = &f.upstreams[0];
        assert!(
            u.unprovable,
            "a duplicate recorded counterpart makes the upstream unprovable"
        );
        assert!(u.drifted, "an unprovable upstream is OUTDATED");
    }

    // ---- Decision reason ----------------------------------------------------

    /// `decide` names why it reused or rebuilt: the reuse candidate selection
    /// miss when there is no candidate, otherwise the freshness evidence.
    #[test]
    fn decide_reports_one_reason_per_situation() {
        let reason = |c: &SubmitContext, prev: Option<&ExecutionRow>| decide(c, prev).reason();
        let up = vec![tbl("a", 100)];
        let prev = confirmed("h", up.clone(), Some(100));

        // No reuse candidate: no latest row at all, or a rejected one.
        assert_eq!(reason(&ctx(&up), None), DecisionReason::FirstBuild);
        for why in [
            NoCandidate::IdentityMismatch,
            NoCandidate::FailedReuseQualification,
            NoCandidate::OwnTargetChangedOrMissing,
            NoCandidate::IncompleteCachedTestResults,
        ] {
            let mut c = ctx(&up);
            c.target_has_prior_build = true;
            c.why_no_candidate = Some(why);
            assert_eq!(reason(&c, None), DecisionReason::NoReuseCandidate(why));
        }

        // A reuse candidate: the freshness evidence decides.
        assert_eq!(
            reason(&ctx(&up), Some(&prev)),
            DecisionReason::Reused(Reuse::Fresh)
        );
        assert_eq!(
            reason(&ctx(&[]), Some(&confirmed("h", vec![], Some(100)))),
            DecisionReason::Reused(Reuse::Fresh),
            "no upstreams to compare"
        );
        let added = vec![tbl("a", 100), tbl("new_dep", 100)];
        assert_eq!(
            reason(&ctx(&added), Some(&prev)),
            DecisionReason::DependencySetChanged
        );
        assert_eq!(
            reason(&ctx(&up), Some(&confirmed("h", up.clone(), None))),
            DecisionReason::NoBuildAnchor
        );
        let drifted = vec![tbl("a", 999)];
        assert_eq!(
            reason(&ctx(&drifted), Some(&prev)),
            DecisionReason::UpstreamOutdated
        );
        let unknown = vec![InputTable {
            name: "a".into(),
            last_modified_epoch: None,
            ..Default::default()
        }];
        assert_eq!(
            reason(&ctx(&unknown), Some(&prev)),
            DecisionReason::UpstreamOutdated,
            "an unprovable upstream is outdated"
        );

        // ALL policy with one of two parents drifted: reused, knowingly stale.
        let recorded = vec![tbl("a", 100), tbl("b", 100)];
        let partial = vec![tbl("a", 100), tbl("b", 999)];
        let two = confirmed("h", recorded, Some(100));
        let mut all = ctx(&partial);
        all.stale_upstream_policy = StaleUpstreamPolicy::All;
        assert_eq!(
            reason(&all, Some(&two)),
            DecisionReason::Reused(Reuse::ToleratedStale)
        );
    }

    /// A decided record carries the verdict's wire values, its reason, and the
    /// reuse candidate it was decided against.
    #[test]
    fn decided_record_carries_the_verdict_and_the_matched_execution() {
        let up = vec![tbl("a", 100)];
        let prev = confirmed("h", up.clone(), Some(100));

        let skip = DecisionRecord::decided(&ctx(&up), Some(&prev));
        assert_eq!(skip.class, DecisionKind::Skip);
        assert_eq!(skip.reason, DecisionReason::Reused(Reuse::Fresh));
        assert_eq!(
            skip.description,
            "model was a no-op because both its query and its upstream data are up to date"
        );
        assert_eq!(
            (skip.skip_rejection_reason, skip.clone_rejection_reason),
            (None, None)
        );
        assert!(!skip.is_stale);
        assert_eq!(skip.matched_execution_id, Some(1));

        let drifted = vec![tbl("a", 999)];
        let rebuild = DecisionRecord::decided(&ctx(&drifted), Some(&prev));
        assert_eq!(rebuild.class, DecisionKind::Build);
        assert_eq!(rebuild.reason, DecisionReason::UpstreamOutdated);
        assert_eq!(
            (
                rebuild.skip_rejection_reason,
                rebuild.clone_rejection_reason
            ),
            (Some(6), Some(6))
        );
        assert_eq!(
            rebuild.matched_execution_id,
            Some(1),
            "the freshness anchor"
        );

        let first = DecisionRecord::decided(&ctx(&up), None);
        assert_eq!(first.class, DecisionKind::Build);
        assert_eq!(first.reason, DecisionReason::FirstBuild);
        assert_eq!(
            first.description,
            "model was executed because its table did not exist"
        );
        assert_eq!(first.matched_execution_id, None);
    }

    /// Every decision reason has one stable lowercase string, the value
    /// persisted on the node decision, and reads back as itself.
    #[test]
    fn decision_reasons_persist_as_stable_strings() {
        let pinned = [
            (DecisionReason::FirstBuild, "first_build"),
            (
                DecisionReason::NoReuseCandidate(NoCandidate::IdentityMismatch),
                "no_reuse_candidate.identity_mismatch",
            ),
            (
                DecisionReason::NoReuseCandidate(NoCandidate::ExecutionTypeChanged),
                "no_reuse_candidate.execution_type_changed",
            ),
            (
                DecisionReason::NoReuseCandidate(NoCandidate::UpstreamNamesChanged),
                "no_reuse_candidate.upstream_names_changed",
            ),
            (
                DecisionReason::NoReuseCandidate(NoCandidate::CopyLogicMismatch),
                "no_reuse_candidate.copy_logic_mismatch",
            ),
            (
                DecisionReason::NoReuseCandidate(NoCandidate::FailedReuseQualification),
                "no_reuse_candidate.failed_reuse_qualification",
            ),
            (
                DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetChangedOrMissing),
                "no_reuse_candidate.own_target_changed_or_missing",
            ),
            (
                DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetModified),
                "no_reuse_candidate.own_target_modified",
            ),
            (
                DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetMissing),
                "no_reuse_candidate.own_target_missing",
            ),
            (
                DecisionReason::NoReuseCandidate(NoCandidate::IncompleteCachedTestResults),
                "no_reuse_candidate.incomplete_cached_test_results",
            ),
            (
                DecisionReason::DependencySetChanged,
                "dependency_set_changed",
            ),
            (DecisionReason::NoBuildAnchor, "no_build_anchor"),
            (DecisionReason::UpstreamOutdated, "upstream_outdated"),
            (
                DecisionReason::UpstreamQueryChanged,
                "upstream_query_changed",
            ),
            (DecisionReason::Reused(Reuse::Fresh), "reused.fresh"),
            (
                DecisionReason::Reused(Reuse::ToleratedStale),
                "reused.tolerated_stale",
            ),
            (
                DecisionReason::Reused(Reuse::WithinLagTolerance),
                "reused.within_lag_tolerance",
            ),
            (DecisionReason::ResultCopy, "result_copy"),
            (DecisionReason::CopyChainLimit, "copy_chain_limit"),
            (DecisionReason::CopyDisabled, "copy_disabled"),
        ];
        for (reason, stored) in pinned {
            assert_eq!(reason.as_str(), stored);
            assert_eq!(DecisionReason::from_stored(stored), Some(reason));
        }
        assert_eq!(DecisionReason::from_stored("no_reuse_candidate"), None);
        assert_eq!(DecisionReason::from_stored("FIRST_BUILD"), None);
    }

    /// One definition of the decision class: its stored string, read back by
    /// Explain, and its wire `decision` int (SubmitSQLResultType).
    #[test]
    fn decision_kinds_are_stored_and_sent_as_pinned() {
        let pinned = [
            (DecisionKind::Skip, "skip", 0),
            (DecisionKind::Build, "build", 1),
            (DecisionKind::Clone, "clone", 3),
        ];
        for (kind, stored, wire) in pinned {
            assert_eq!(kind.as_str(), stored);
            assert_eq!(DecisionKind::from_stored(stored), Some(kind));
            assert_eq!(kind.wire_decision(), wire);
        }
        assert_eq!(DecisionKind::from_stored("execute"), None);
        assert_eq!(DecisionKind::from_stored("SKIP"), None);
    }

    #[test]
    fn rejection_codes_are_the_wire_values() {
        assert_eq!(
            [
                REJECTION_TARGET_TABLE_MISMATCH,
                REJECTION_FORCED_NOT_ELIGIBLE,
                REJECTION_NO_SUITABLE_MATCH_FOUND,
                REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED,
                REJECTION_TARGET_TABLE_DOES_NOT_EXIST,
                REJECTION_TARGET_TABLE_MATCH,
                REJECTION_LATEST_QUERY_HASH_NOT_MATCH,
            ],
            [1, 5, 6, 10, 4, 7, 8]
        );
    }

    /// VERIFIED LIVE (2026-11-10, lag_copy_candidates `dt_m`, lag_seed_upstream
    /// G, lag_dropped_targets `ddc`): an absent target whose latest build still
    /// has the workload's identity is described as a first build. It reports
    /// TARGET_TABLE_DOES_NOT_EXIST / TARGET_TABLE_MATCH only when that build
    /// would otherwise be reusable (the caller judges it), else 6/6.
    #[test]
    fn missing_own_target_reports_target_table_codes() {
        let mut c = ctx(&[]);
        c.execution_type = ModelExecutionType::Full as i32;
        c.target_has_prior_build = true;
        c.own_target_absent = true;
        c.why_no_candidate = Some(NoCandidate::OwnTargetMissing);
        let record = DecisionRecord::decided(&c, None);
        assert_eq!(
            record.description,
            "model was executed because its table did not exist"
        );
        assert_eq!(
            record.reason,
            DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetMissing)
        );
        assert!(!record.is_stale);
        assert_eq!(
            (record.skip_rejection_reason, record.clone_rejection_reason),
            (
                Some(REJECTION_NO_SUITABLE_MATCH_FOUND),
                Some(REJECTION_NO_SUITABLE_MATCH_FOUND)
            ),
            "a stale latest build"
        );
        let matched = record.clone().with_absent_target_match();
        assert_eq!(
            (
                matched.skip_rejection_reason,
                matched.clone_rejection_reason
            ),
            (
                Some(REJECTION_TARGET_TABLE_DOES_NOT_EXIST),
                Some(REJECTION_TARGET_TABLE_MATCH)
            )
        );
        assert_eq!(matched.description, record.description);

        // Any other rebuild is left alone.
        let other = DecisionRecord::decided(&ctx(&[]), None);
        assert_eq!(other.clone().with_absent_target_match(), other);
    }

    /// VERIFIED LIVE (2026-11-10, lag_copy_candidates `ob*`): a rebuild while
    /// an older build would still be reusable reports
    /// LATEST_QUERY_HASH_NOT_MATCH / TARGET_TABLE_MATCH, with `is_stale` as
    /// that build would be reused. A reuse is left alone.
    #[test]
    fn older_match_overrides_only_a_rebuild() {
        let c = ctx(&[]);
        let rebuild = DecisionRecord::decided(&c, None);
        for (older, stale) in [
            (Reuse::Fresh, false),
            (Reuse::WithinLagTolerance, true),
            (Reuse::ToleratedStale, true),
        ] {
            let r = rebuild.clone().with_older_match(older, Some(32_000));
            assert_eq!(r.class, DecisionKind::Build);
            assert_eq!(r.reason, rebuild.reason, "the reason is the rebuild's");
            assert_eq!(r.description, rebuild.description);
            assert_eq!(
                r.skip_rejection_reason,
                Some(REJECTION_LATEST_QUERY_HASH_NOT_MATCH)
            );
            assert_eq!(r.clone_rejection_reason, Some(REJECTION_TARGET_TABLE_MATCH));
            assert_eq!(r.is_stale, stale, "{older:?}");
            assert_eq!(r.build_age_ms, Some(32_000), "the older build's age");
        }

        let tables = [tbl("a", 100)];
        let prev = confirmed("h", tables.to_vec(), Some(100));
        let skip = DecisionRecord::decided(&ctx(&tables), Some(&prev));
        assert_eq!(skip.class, DecisionKind::Skip);
        assert_eq!(
            skip.clone()
                .with_older_match(Reuse::WithinLagTolerance, Some(32_000)),
            skip
        );
    }

    /// A judged result copy keeps the copy's wire shape and carries the
    /// equivalent's staleness and build age, anchored on the target's own
    /// build when it copies over an existing target.
    #[test]
    fn judged_result_copy_carries_the_equivalents_freshness() {
        let tables = [tbl("a", 100)];
        let own = confirmed("h", tables.to_vec(), Some(100));
        let over = DecisionRecord::result_copy_judged(
            Some(&own),
            Reuse::WithinLagTolerance,
            600_000,
            Some(5_000),
            &tables,
        );
        assert_eq!(over.class, DecisionKind::Clone);
        assert_eq!(over.reason, DecisionReason::ResultCopy);
        assert_eq!(over.description, RESULT_COPY_DESCRIPTION);
        assert_eq!(
            over.skip_rejection_reason,
            Some(REJECTION_TARGET_TABLE_MISMATCH)
        );
        assert_eq!(over.clone_rejection_reason, None);
        assert!(over.is_stale);
        assert_eq!(over.matched_execution_id, Some(own.id));
        assert_eq!(
            (over.tolerance_ms, over.build_age_ms),
            (600_000, Some(5_000))
        );
        assert_eq!(over.input_tables, tables);

        let fresh = DecisionRecord::result_copy_judged(None, Reuse::Fresh, 0, None, &[]);
        assert!(!fresh.is_stale);
        assert_eq!(fresh.matched_execution_id, None);
        assert_eq!(fresh, DecisionRecord::result_copy());
    }

    /// `reusable` judges a build as if it were the reuse candidate.
    #[test]
    fn reusable_judges_a_build_as_the_reuse_candidate() {
        let prev = confirmed("h", vec![tbl("a", 100)], Some(100));
        let unchanged = [tbl("a", 100)];
        let drifted = [tbl("a", 900)];
        assert_eq!(reusable(&ctx(&unchanged), &prev), Some(Reuse::Fresh));
        assert_eq!(reusable(&ctx(&drifted), &prev), None);
        let mut within = ctx(&drifted);
        within.tolerance_ms = 600;
        within.build_age_ms = Some(10);
        assert_eq!(reusable(&within, &prev), Some(Reuse::WithinLagTolerance));
        within.build_age_ms = Some(600);
        assert_eq!(
            reusable(&within, &prev),
            None,
            "age == tolerance is too old"
        );
    }
}

#[cfg(test)]
#[path = "decision_proptest.rs"]
mod decision_proptest;
