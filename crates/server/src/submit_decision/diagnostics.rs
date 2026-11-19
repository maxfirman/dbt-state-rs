//! Refusal diagnostics: how hosted words an ordinary SQL rebuild it did not
//! optimize. Each diagnostic only decorates the decision record (codes,
//! description, sometimes the reason); none qualifies a reuse candidate, a
//! copy source or an Explain freshness anchor.
//!
//! Each rule is a pure question builder (`*_query`), deciding from the
//! decision so far whether State history must be asked, and with which
//! [`Matcher`]. [`Query::ask`] and [`OtherTargetQuery::ask`] are the only
//! reads. [`decorate`] runs them in hosted's precedence order.

use super::{context, Freshness};
use crate::decision::{self, DecisionKind, DecisionReason, DecisionRecord, NoCandidate};
use crate::execution::{self, ExecutionEvidence};
use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
use crate::result_copy;
use crate::reuse_candidate::{self, Selection};
use crate::store::{
    ExecutionRow, Producer, QueryDiagnosticNamespaces, QueryDiagnosticReference, Store,
};
use crate::warehouse::Warehouse;

/// What the diagnostics read about one submit.
pub(super) struct Scope<'a> {
    pub org_id: &'a str,
    /// The resolved submit.
    pub req: &'a qc::SubmitEnrichedSqlRequest,
    pub evidence: &'a ExecutionEvidence,
    pub selection: &'a Selection,
    pub freshness: &'a Freshness<'a>,
}

/// Which recorded builds under another name a diagnostic is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Matcher {
    /// An older build with the submit's identity that read only its own
    /// Target relation.
    SourceLessOlderBuild,
    /// #151: a same-query snapshot with a known epoch that read exactly the
    /// submit's genuine upstreams.
    SnapshotPeer,
    /// The checked submit-copy refusal (`result_copy::submit_refusal_matches`).
    SubmitRefusal,
    /// #152: the direct-copy refusal (`result_copy::direct_refusal_matches`).
    DirectRefusal,
}

impl Matcher {
    fn matches(self, scope: &Scope<'_>, row: &ExecutionRow) -> bool {
        let (req, evidence) = (scope.req, scope.evidence);
        match self {
            Self::SourceLessOlderBuild => {
                reuse_candidate::older_build_matches(req, evidence, row)
                    && row
                        .input_tables
                        .iter()
                        .all(|input| input.name == row.target_table)
            }
            Self::SnapshotPeer => {
                reuse_candidate::older_build_matches(req, evidence, row)
                    && row.last_modified_epoch.is_some_and(|epoch| epoch > 0)
                    && row
                        .input_tables
                        .iter()
                        .filter(|input| input.name != row.target_table)
                        .eq(evidence
                            .input_tables
                            .iter()
                            .filter(|input| input.name != evidence.target_table))
            }
            Self::SubmitRefusal => result_copy::submit_refusal_matches(req, evidence, row),
            Self::DirectRefusal => result_copy::direct_refusal_matches(req, evidence, row),
        }
    }
}

/// The current build under another name, in `namespaces`, that `matcher`
/// admits: the relation a refused optimization names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OtherTargetQuery<'a> {
    pub namespaces: QueryDiagnosticNamespaces<'a>,
    pub matcher: Matcher,
}

impl OtherTargetQuery<'_> {
    pub(super) async fn ask(
        self,
        store: &Store,
        scope: &Scope<'_>,
    ) -> sqlx::Result<Option<QueryDiagnosticReference>> {
        store
            .other_target_query_diagnostic(
                scope.org_id,
                &scope.evidence.target_table,
                &scope.req.dialect,
                self.namespaces,
                |row| self.matcher.matches(scope, row),
            )
            .await
    }
}

/// A yes/no State history question a diagnostic asks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Query<'a> {
    /// Whether the Target relation has a build matching
    /// [`Matcher::SourceLessOlderBuild`].
    TargetHistory,
    /// Whether another name has a current build [`OtherTargetQuery`] admits.
    OtherTarget(OtherTargetQuery<'a>),
    /// Whether another name in the submit's namespace has a build matching
    /// [`Matcher::SourceLessOlderBuild`].
    OtherTargetHistory,
    /// Whether the absent scope's current query definition changed.
    AbsentScopeDefinition,
}

impl Query<'_> {
    pub(super) async fn ask(self, store: &Store, scope: &Scope<'_>) -> sqlx::Result<bool> {
        let (org_id, req, evidence) = (scope.org_id, scope.req, scope.evidence);
        match self {
            Self::TargetHistory => {
                store
                    .has_target_query_diagnostic(
                        org_id,
                        &evidence.target_table,
                        &req.dialect,
                        |row| Matcher::SourceLessOlderBuild.matches(scope, row),
                    )
                    .await
            }
            Self::OtherTarget(query) => Ok(query.ask(store, scope).await?.is_some()),
            Self::OtherTargetHistory => {
                store
                    .has_other_target_history_diagnostic(
                        org_id,
                        &evidence.target_table,
                        &req.dialect,
                        req.table_namespace.as_deref(),
                        |row| Matcher::SourceLessOlderBuild.matches(scope, row),
                    )
                    .await
            }
            Self::AbsentScopeDefinition => {
                store
                    .absent_scope_query_definition_changed(
                        org_id,
                        &evidence.target_table,
                        &req.dialect,
                        req.execution_type,
                        evidence.node_sql_hash.as_deref(),
                    )
                    .await
            }
        }
    }
}

/// The decorated decision. `other_target` names the build a FULL submit's
/// other-target diagnostic found: the submit then builds with the
/// chain-limit outcome shape, and no later diagnostic applies.
/// `direct_reference` names #152's refused direct copy source.
pub(super) struct Diagnosed {
    pub record: DecisionRecord,
    pub other_target: Option<QueryDiagnosticReference>,
    pub direct_reference: Option<QueryDiagnosticReference>,
}

/// Decorate `record` with every diagnostic that applies, in hosted's
/// precedence order. `has_candidate`: the decision kept a reuse candidate
/// (or the older match it reports).
pub(super) async fn decorate(
    store: &Store,
    scope: &Scope<'_>,
    mut record: DecisionRecord,
    has_candidate: bool,
) -> sqlx::Result<Diagnosed> {
    if let Some(query) = target_history_query(scope, &record, has_candidate) {
        if query.ask(store, scope).await? {
            record = record.with_namespace_query_diagnostic(!scope.freshness.own_target_absent);
        }
    }
    let other_target = match other_target_query(scope, &record, has_candidate) {
        Some(query) => query.ask(store, scope).await?,
        None => None,
    };
    if other_target.is_some() {
        record = record.with_other_target_query_diagnostic(scope.req.execution_type);
    }
    if let Some(reference) =
        other_target.filter(|_| scope.req.execution_type == ModelExecutionType::Full as i32)
    {
        return Ok(Diagnosed {
            record,
            other_target: Some(reference),
            direct_reference: None,
        });
    }
    if let Some(query) = other_target_history_query(scope, &record, has_candidate) {
        if query.ask(store, scope).await? {
            record = record.with_absent_target_history_diagnostic();
        }
    }
    if let Some(query) = absent_scope_query(scope, &record, has_candidate) {
        if query.ask(store, scope).await? {
            record = record.with_changed_absent_scope_definition_diagnostic();
        }
    }
    let mut record = incremental_transition_display(scope, record);
    microbatch_existing_target(scope, &mut record);
    if let Some(query) = snapshot_refusal_query(scope, &record) {
        if query.ask(store, scope).await? {
            record = record.with_refused_copy(decision::REJECTION_EXECUTION_TYPE_NOT_FULL);
        }
    }
    if let Some(query) = submit_refusal_query(scope, &record) {
        if query.ask(store, scope).await? {
            record = record.with_refused_copy(decision::REJECTION_NO_SUITABLE_MATCH_FOUND);
        }
    }
    let direct_reference = match direct_refusal_query(scope, &record) {
        Some(query) => query.ask(store, scope).await?,
        None => None,
    };
    if direct_reference.is_some() {
        record = direct_refusal(scope.req, record);
    }
    Ok(Diagnosed {
        record,
        other_target: None,
        direct_reference,
    })
}

/// A rebuild with no reuse candidate whose Target relation had an older
/// source-less build of the submit's identity.
pub(super) fn target_history_query(
    scope: &Scope<'_>,
    record: &DecisionRecord,
    has_candidate: bool,
) -> Option<Query<'static>> {
    (!has_candidate
        && record.class == DecisionKind::Build
        && Warehouse::from_dialect(&scope.req.dialect).permits_target_query_diagnostic(scope.req)
        && matches!(
            record.reason,
            DecisionReason::FirstBuild
                | DecisionReason::ExistingTargetWithoutHistory
                | DecisionReason::NoReuseCandidate(
                    NoCandidate::IdentityMismatch | NoCandidate::OwnTargetMissing
                )
        ))
    .then_some(Query::TargetHistory)
}

/// A first build (or one over an existing target without history) whose
/// query a build under another name already ran. Without a namespace every
/// namespace is searched.
pub(super) fn other_target_query<'a>(
    scope: &Scope<'a>,
    record: &DecisionRecord,
    has_candidate: bool,
) -> Option<OtherTargetQuery<'a>> {
    let applies = !has_candidate
        && record.class == DecisionKind::Build
        && matches!(
            record.reason,
            DecisionReason::FirstBuild | DecisionReason::ExistingTargetWithoutHistory
        )
        && Warehouse::from_dialect(&scope.req.dialect)
            .permits_other_target_query_diagnostic(scope.req);
    applies.then(|| OtherTargetQuery {
        namespaces: match scope.req.table_namespace.as_deref() {
            None => QueryDiagnosticNamespaces::All,
            Some(namespace) => {
                QueryDiagnosticNamespaces::Exact(execution::non_blank_namespace(Some(namespace)))
            }
        },
        matcher: Matcher::SourceLessOlderBuild,
    })
}

/// A first build of an absent target whose source-less query was built
/// under another name in its namespace.
pub(super) fn other_target_history_query(
    scope: &Scope<'_>,
    record: &DecisionRecord,
    has_candidate: bool,
) -> Option<Query<'static>> {
    (!has_candidate
        && record.reason == DecisionReason::FirstBuild
        && Warehouse::from_dialect(&scope.req.dialect)
            .permits_absent_target_history_diagnostic(scope.req))
    .then_some(Query::OtherTargetHistory)
}

/// A rebuild over an existing target without history whose absent scope's
/// query definition changed.
pub(super) fn absent_scope_query(
    scope: &Scope<'_>,
    record: &DecisionRecord,
    has_candidate: bool,
) -> Option<Query<'static>> {
    (!has_candidate
        && record.reason == DecisionReason::ExistingTargetWithoutHistory
        && Warehouse::from_dialect(&scope.req.dialect)
            .permits_absent_scope_definition_diagnostic(scope.req))
    .then_some(Query::AbsentScopeDefinition)
}

/// #151: a fresh same-query snapshot under another name affects only
/// refusal codes. Never promote that relation to a reuse/copy candidate or
/// replace the submit's own freshness/Explain anchor.
pub(super) fn snapshot_refusal_query<'a>(
    scope: &Scope<'a>,
    record: &DecisionRecord,
) -> Option<Query<'a>> {
    let evidence = scope.evidence;
    (record.class == DecisionKind::Build
        && matches!(
            record.reason,
            DecisionReason::FirstBuild | DecisionReason::UpstreamOutdated
        )
        && Warehouse::from_dialect(&scope.req.dialect)
            .permits_snapshot_refusal_diagnostic(scope.req)
        && evidence
            .input_tables
            .iter()
            .filter(|input| input.name != evidence.target_table)
            .all(|input| input.producer == Producer::NoBuild))
    .then_some(Query::OtherTarget(OtherTargetQuery {
        namespaces: QueryDiagnosticNamespaces::Exact(scope.req.table_namespace.as_deref()),
        matcher: Matcher::SnapshotPeer,
    }))
}

/// A rebuild whose checked submit-copy scope found an equivalent it cannot copy.
pub(super) fn submit_refusal_query<'a>(
    scope: &Scope<'a>,
    record: &DecisionRecord,
) -> Option<Query<'a>> {
    (record.class == DecisionKind::Build
        && !matches!(
            record.reason,
            DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetModified)
        )
        && result_copy::submit_refusal_scope(scope.req, scope.evidence))
    .then_some(Query::OtherTarget(OtherTargetQuery {
        namespaces: QueryDiagnosticNamespaces::Exact(scope.req.table_namespace.as_deref()),
        matcher: Matcher::SubmitRefusal,
    }))
}

/// #152: an upstream-outdated rebuild in the direct-copy refusal scope.
pub(super) fn direct_refusal_query<'a>(
    scope: &Scope<'a>,
    record: &DecisionRecord,
) -> Option<OtherTargetQuery<'a>> {
    (record.class == DecisionKind::Build
        && record.reason == DecisionReason::UpstreamOutdated
        && result_copy::direct_refusal_scope(scope.req, scope.evidence))
    .then_some(OtherTargetQuery {
        namespaces: QueryDiagnosticNamespaces::Exact(scope.req.table_namespace.as_deref()),
        matcher: Matcher::DirectRefusal,
    })
}

/// #152's refusal shape: a MERGE reports the chain limit, other kinds the
/// not-FULL rejection.
pub(super) fn direct_refusal(
    req: &qc::SubmitEnrichedSqlRequest,
    record: DecisionRecord,
) -> DecisionRecord {
    if req.execution_type == ModelExecutionType::Merge as i32 {
        let mut record = record.with_refused_copy(decision::REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED);
        record.description = decision::COPY_CHAIN_LIMIT_DESCRIPTION.into();
        record.reason = DecisionReason::CopyChainLimit;
        record
    } else {
        record.with_refused_copy(decision::REJECTION_EXECUTION_TYPE_NOT_FULL)
    }
}

/// Decorate only an already forced BUILD, after all eligibility, freshness and
/// copy decisions. Unknown epochs/producers, other policies/types and external
/// target modifications retain the ordinary conservative result.
pub(super) fn incremental_transition_display(
    scope: &Scope<'_>,
    record: DecisionRecord,
) -> DecisionRecord {
    let (evidence, selection, freshness) = (scope.evidence, scope.selection, scope.freshness);
    let Some(latest) = selection.transition_display.as_ref() else {
        return record;
    };
    if record.class != DecisionKind::Build
        || !matches!(
            record.reason,
            DecisionReason::NoReuseCandidate(
                NoCandidate::ExecutionTypeChanged | NoCandidate::OwnTargetModified
            )
        )
        || freshness.tolerance_ms != 0
        || freshness.stale_upstream_policy != decision::StaleUpstreamPolicy::Any
        || latest.clone_source_table.is_some()
        || evidence
            .input_tables
            .iter()
            .chain(&latest.input_tables)
            .filter(|input| input.name != evidence.target_table)
            .any(|input| input.producer != Producer::NoBuild)
        || !latest.last_modified_epoch.is_some_and(|epoch| {
            epoch > 0
                && evidence.input_tables.iter().any(|t| {
                    t.name == evidence.target_table && t.last_modified_epoch == Some(epoch)
                })
        })
    {
        return record;
    }
    let ctx = context(evidence, selection, freshness, Some(latest));
    let upstreams = decision::upstream_freshness(&ctx, latest).upstreams;
    if upstreams.is_empty()
        || upstreams.iter().any(|up| {
            up.unprovable
                || up.producer_changed
                || up.anchor_epoch <= 0
                || !up.current_epoch.is_some_and(|epoch| epoch > 0)
        })
    {
        return record;
    }
    record.with_incremental_transition_display(latest, upstreams.iter().any(|up| up.drifted))
}

/// #150: a failed native batch can leave a physical target without a
/// confirmed State build. Report its known existence, without inventing a
/// query/freshness anchor or admitting the partial result for reuse.
pub(super) fn microbatch_existing_target(scope: &Scope<'_>, record: &mut DecisionRecord) {
    let (req, evidence, freshness) = (scope.req, scope.evidence, scope.freshness);
    if Warehouse::from_dialect(&req.dialect) == Warehouse::Databricks
        && req.execution_type == ModelExecutionType::Microbatch as i32
        && record.class == DecisionKind::Build
        && record.reason == DecisionReason::FirstBuild
        && !scope.selection.target_has_prior_build
        && !req.compare_unrendered_code
        && req.query_dependencies.is_empty()
        && req
            .table_namespace
            .as_ref()
            .is_some_and(|ns| !ns.is_empty())
        && freshness.sql_reads_relation == Some(true)
        && freshness.tolerance_ms == 0
        && freshness.stale_upstream_policy == decision::StaleUpstreamPolicy::Any
        && !freshness.own_target_absent
        && evidence.input_tables.len() == 2
        && evidence
            .input_tables
            .iter()
            .filter(|input| input.name != evidence.target_table)
            .count()
            == 1
        && evidence.input_tables.iter().all(|input| {
            input.last_modified_epoch.is_some_and(|epoch| epoch > 0)
                && (input.name == evidence.target_table || input.producer == Producer::NoBuild)
        })
    {
        record.reason = DecisionReason::ExistingTargetWithoutHistory;
        record.description = decision::DESC_EXECUTE.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: &str = "c.s.t";

    fn first_build() -> Selection {
        Selection {
            candidate: None,
            target_has_prior_build: false,
            why_no_candidate: None,
            rejected: None,
            unqualified: None,
            unmatched: None,
            type_changed: None,
            transition_display: None,
        }
    }

    /// A Databricks literal submit: the scope every query diagnostic needs.
    fn literal(execution_type: ModelExecutionType) -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            sql: "select 1 as id".into(),
            dialect: "databricks".into(),
            default_catalog: "c".into(),
            target_table: Some(TARGET.into()),
            execution_type: execution_type as i32,
            allow_clones: Some(true),
            clone_chain_depth_limit: Some(0),
            ..Default::default()
        }
    }

    fn freshness() -> Freshness<'static> {
        Freshness {
            sql_reads_relation: Some(false),
            target_table: Some(TARGET),
            stale_upstream_policy: decision::StaleUpstreamPolicy::Any,
            tolerance_ms: 0,
            now_ms: 0,
            own_target_absent: true,
        }
    }

    /// Run `check` against `req`'s scope and its ordinary first-build record.
    fn with_scope<T>(
        req: &qc::SubmitEnrichedSqlRequest,
        check: impl FnOnce(&Scope<'_>, DecisionRecord) -> T,
    ) -> T {
        let evidence = ExecutionEvidence::sql_submit("o", req);
        let selection = first_build();
        let freshness = freshness();
        let scope = Scope {
            org_id: "o",
            req,
            evidence: &evidence,
            selection: &selection,
            freshness: &freshness,
        };
        let record =
            DecisionRecord::decided(&context(&evidence, &selection, &freshness, None), None);
        check(&scope, record)
    }

    #[test]
    fn a_first_build_asks_for_its_targets_source_less_history_only_without_a_candidate() {
        with_scope(&literal(ModelExecutionType::Full), |scope, record| {
            assert_eq!(record.reason, DecisionReason::FirstBuild);
            assert_eq!(
                target_history_query(scope, &record, false),
                Some(Query::TargetHistory)
            );
            assert_eq!(target_history_query(scope, &record, true), None);
        });
    }

    #[test]
    fn a_namespaced_submit_never_asks_for_its_targets_history() {
        let req = qc::SubmitEnrichedSqlRequest {
            table_namespace: Some("ns".into()),
            ..literal(ModelExecutionType::Full)
        };
        with_scope(&req, |scope, record| {
            assert_eq!(target_history_query(scope, &record, false), None);
        });
    }

    #[test]
    fn the_other_target_query_searches_every_namespace_only_when_the_submit_names_none() {
        with_scope(&literal(ModelExecutionType::Full), |scope, record| {
            assert_eq!(
                other_target_query(scope, &record, false),
                Some(OtherTargetQuery {
                    namespaces: QueryDiagnosticNamespaces::All,
                    matcher: Matcher::SourceLessOlderBuild,
                })
            );
        });
        let req = qc::SubmitEnrichedSqlRequest {
            table_namespace: Some("ns".into()),
            ..literal(ModelExecutionType::Full)
        };
        with_scope(&req, |scope, record| {
            assert_eq!(
                other_target_query(scope, &record, false),
                Some(OtherTargetQuery {
                    namespaces: QueryDiagnosticNamespaces::Exact(Some("ns")),
                    matcher: Matcher::SourceLessOlderBuild,
                })
            );
        });
    }

    #[test]
    fn a_snowflake_submit_asks_no_query_diagnostic() {
        let req = qc::SubmitEnrichedSqlRequest {
            dialect: "snowflake".into(),
            ..literal(ModelExecutionType::Full)
        };
        with_scope(&req, |scope, record| {
            assert_eq!(target_history_query(scope, &record, false), None);
            assert_eq!(other_target_query(scope, &record, false), None);
            assert_eq!(other_target_history_query(scope, &record, false), None);
            assert_eq!(absent_scope_query(scope, &record, false), None);
        });
    }

    #[test]
    fn a_refused_direct_merge_copy_reports_the_chain_limit() {
        with_scope(&literal(ModelExecutionType::Merge), |_, record| {
            let merge = direct_refusal(&literal(ModelExecutionType::Merge), record.clone());
            assert_eq!(merge.reason, DecisionReason::CopyChainLimit);
            assert_eq!(merge.description, decision::COPY_CHAIN_LIMIT_DESCRIPTION);
            assert_eq!(
                merge.clone_rejection_reason,
                Some(decision::REJECTION_CLONE_CHAIN_LIMIT_EXCEEDED)
            );
            let snapshot = direct_refusal(&literal(ModelExecutionType::Snapshot), record);
            assert_eq!(snapshot.reason, DecisionReason::FirstBuild);
            assert_eq!(
                snapshot.clone_rejection_reason,
                Some(decision::REJECTION_EXECUTION_TYPE_NOT_FULL)
            );
        });
    }
}
