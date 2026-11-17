//! Which equivalent under another name a SQL submit's Result copy fallback
//! copies. The fallback offers each eligible equivalent, in the store's
//! preference order (newest build first), with the equivalent's current
//! physical state; [`CopySearch::offer`] decides, without reading.

use super::{context, Freshness};
use crate::decision;
use crate::execution::ExecutionEvidence;
use crate::reuse_candidate::Selection;
use crate::store::ExecutionRow;

/// How a chosen equivalent would be reused for this submit, and its build
/// age: `None` when it was copied unjudged (another environment).
pub(super) type Judged = Option<(decision::Reuse, Option<i64>)>;

/// The search over the equivalents of one submit.
///
/// The first equivalent, in preference order, that qualifies for this
/// submit is chosen. Only a same-upstream equivalent rejected on freshness
/// lets the search go on to the next one (VERIFIED LIVE 2026-11-10,
/// lag_copy_candidates `sne`: the newest equivalent was stale and hosted
/// copied an older, current one). After such a rejection the search accepts
/// only another same-upstream equivalent: copying an unjudged one from
/// another environment instead is uncharacterized.
pub(super) struct CopySearch {
    own_target_has_candidate: bool,
    rejected_on_freshness: bool,
}

/// What the search does with one offered equivalent.
#[derive(Debug, PartialEq)]
pub(super) enum Offer {
    /// Copy this equivalent.
    Choose(Judged),
    /// Pass over it and offer the next one.
    Next,
    /// End the search without a copy; the ordinary decision applies.
    Stop,
}

impl CopySearch {
    /// `own`: the target's reuse candidate when it had one but must rebuild
    /// for freshness (the copy then goes over the existing target).
    pub(super) fn new(own: Option<&ExecutionRow>) -> Self {
        Self {
            own_target_has_candidate: own.is_some(),
            rejected_on_freshness: false,
        }
    }

    /// Offer an eligible equivalent with its current physical state, `None`
    /// when that state vanished since the candidates were read.
    pub(super) fn offer(
        &mut self,
        evidence: &ExecutionEvidence,
        freshness: &Freshness<'_>,
        source: Option<ExecutionRow>,
    ) -> Offer {
        match source {
            Some(source) if reads_same_upstreams(evidence, &source) => {
                match judge_equivalent(evidence, freshness, source) {
                    Some(judged) => Offer::Choose(Some(judged)),
                    None => {
                        self.rejected_on_freshness = true;
                        Offer::Next
                    }
                }
            }
            // An equivalent in another environment is copied unjudged, but
            // only as the first choice for an absent target: a copy over an
            // existing target is only characterized for an equivalent in the
            // same environment.
            Some(_) if !self.own_target_has_candidate && !self.rejected_on_freshness => {
                Offer::Choose(None)
            }
            // Otherwise, or if the equivalent's current state vanished between
            // the two reads, the ordinary decision applies.
            Some(_) | None => Offer::Stop,
        }
    }
}

/// How fresh a same-upstream equivalent would be if reused for this submit,
/// with its build age, judged exactly as if it were the reuse candidate; or
/// `None` when this submit would rebuild it.
pub(super) fn judge_equivalent(
    evidence: &ExecutionEvidence,
    freshness: &Freshness<'_>,
    mut source: ExecutionRow,
) -> Option<(decision::Reuse, Option<i64>)> {
    // The submit's upstream evidence, as read by the equivalent.
    let upstreams: Vec<_> = evidence
        .input_tables
        .iter()
        .filter(|t| t.name != evidence.target_table)
        .cloned()
        .collect();
    crate::copy_lifecycle::adopt_current_inputs(&mut source, &upstreams);
    let selection = Selection::of_candidate(source.clone());
    let mut ctx = context(evidence, &selection, freshness, Some(&source));
    ctx.input_tables = &upstreams;
    ctx.target_table = Some(source.target_table.as_str());
    decision::reusable(&ctx, &source).map(|reuse| (reuse, ctx.build_age_ms))
}

/// Whether `source` recorded exactly the genuine upstream relations this
/// submit reads, however each is spelled
/// ([`crate::copy_lifecycle::canonical_reads`]): a relation it shares with
/// the submit is then judged for drift, never copied unjudged as if it were
/// another environment's (PR #127 review).
fn reads_same_upstreams(evidence: &ExecutionEvidence, source: &ExecutionRow) -> bool {
    use crate::copy_lifecycle::canonical_reads;
    // A client-requested copy records none; it reads what its submit reads.
    source.input_tables.is_empty() && source.clone_source_table.is_some()
        || canonical_reads(&source.input_tables, &source.target_table, None)
            == canonical_reads(&evidence.input_tables, &evidence.target_table, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_cache as qc;
    use crate::store::{InputTable, Producer};

    const DEV: &str = "cdo.dev.orders";
    const PROD: &str = "cdo.prod.orders";
    const RAW: &str = "cdo.raw.orders";
    const OTHER_RAW: &str = "cdo.raw_ci.orders";
    const NOW_MS: i64 = 10_000_000;

    fn read(name: &str, epoch: i64) -> InputTable {
        InputTable {
            producer: Producer::NoBuild,
            ..InputTable::at(name, Some(epoch))
        }
    }

    /// A dev submit reading `RAW` at epoch 200, its own target absent.
    fn evidence() -> ExecutionEvidence {
        let mut evidence = ExecutionEvidence::sql_submit(
            "o",
            &qc::SubmitEnrichedSqlRequest {
                sql: format!("select * from {RAW}"),
                dialect: "snowflake".into(),
                default_catalog: "cdo".into(),
                target_table: Some(DEV.into()),
                table_namespace: Some("ns".into()),
                execution_type: qc::ModelExecutionType::Full as i32,
                tables: vec![qc::TableModifiedInfo {
                    name: RAW.into(),
                    last_modified_epoch: Some(200),
                }],
                ..Default::default()
            },
        );
        for input in &mut evidence.input_tables {
            input.producer = Producer::NoBuild;
        }
        evidence
    }

    fn freshness() -> Freshness<'static> {
        Freshness {
            sql_reads_relation: Some(true),
            target_table: Some(DEV),
            stale_upstream_policy: decision::StaleUpstreamPolicy::Any,
            tolerance_ms: 0,
            now_ms: NOW_MS,
            own_target_absent: true,
        }
    }

    /// The current build of an equivalent under `PROD` that read `input`.
    fn equivalent(input: InputTable) -> ExecutionRow {
        ExecutionRow {
            id: 7,
            target_table: PROD.into(),
            execution_type: qc::ModelExecutionType::Full as i32,
            table_namespace: Some("ns".into()),
            last_modified_epoch: Some(NOW_MS - 1_000),
            input_tables: vec![input],
            status: "confirmed".into(),
            ..ExecutionRow::default()
        }
    }

    #[test]
    fn a_same_upstream_equivalent_is_judged_as_the_reuse_candidate() {
        let mut search = CopySearch::new(None);
        let offer = search.offer(&evidence(), &freshness(), Some(equivalent(read(RAW, 200))));
        assert!(matches!(offer, Offer::Choose(Some(_))), "{offer:?}");
    }

    #[test]
    fn a_stale_same_upstream_equivalent_passes_to_the_next_same_upstream_one() {
        let mut search = CopySearch::new(None);
        let (evidence, freshness) = (evidence(), freshness());
        let stale = search.offer(&evidence, &freshness, Some(equivalent(read(RAW, 100))));
        assert_eq!(stale, Offer::Next);
        let current = search.offer(&evidence, &freshness, Some(equivalent(read(RAW, 200))));
        assert!(matches!(current, Offer::Choose(Some(_))), "{current:?}");
    }

    #[test]
    fn after_a_freshness_rejection_another_environment_is_never_copied() {
        let mut search = CopySearch::new(None);
        let (evidence, freshness) = (evidence(), freshness());
        assert_eq!(
            search.offer(&evidence, &freshness, Some(equivalent(read(RAW, 100)))),
            Offer::Next
        );
        assert_eq!(
            search.offer(&evidence, &freshness, Some(equivalent(read(OTHER_RAW, 50)))),
            Offer::Stop
        );
    }

    #[test]
    fn another_environment_is_copied_unjudged_only_into_an_absent_target() {
        let (evidence, freshness) = (evidence(), freshness());
        let other = || Some(equivalent(read(OTHER_RAW, 50)));
        assert_eq!(
            CopySearch::new(None).offer(&evidence, &freshness, other()),
            Offer::Choose(None)
        );
        let own = equivalent(read(RAW, 100));
        assert_eq!(
            CopySearch::new(Some(&own)).offer(&evidence, &freshness, other()),
            Offer::Stop
        );
    }

    #[test]
    fn an_equivalent_whose_state_vanished_ends_the_search() {
        assert_eq!(
            CopySearch::new(None).offer(&evidence(), &freshness(), None),
            Offer::Stop
        );
    }
}
