//! Which equivalent under another name a SQL submit's Result copy fallback
//! copies. The fallback offers each eligible equivalent, in the store's
//! preference order (newest build first), with the equivalent's current
//! physical state; [`offer`] decides, without reading.

use super::{context, Freshness};
use crate::decision;
use crate::execution::ExecutionEvidence;
use crate::reuse_candidate::Selection;
use crate::sql_norm::canonical_identifiers;
use crate::stand_in::InputRead;
use crate::store::{ExecutionRow, InputTable};

/// How a chosen equivalent would be reused for this submit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct Judged {
    pub reuse: decision::Reuse,
    /// The equivalent's build age, when its build time is known.
    pub build_age_ms: Option<i64>,
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

/// Offer an eligible equivalent with its current physical state, `None`
/// when that state vanished since the candidates were read. `reads` are the
/// submit's inputs with their Stand-ins ([`crate::stand_in::input_reads`]).
///
/// An equivalent qualifies only when the physical inputs it recorded at
/// build time are the ones this submit reads ([`reads_as_recorded`]). It is
/// then judged exactly as if it were the reuse candidate, and the first one
/// fresh enough is chosen; a stale one passes the search on to the next
/// (VERIFIED LIVE 2026-11-10, lag_copy_candidates `sne`: the newest
/// equivalent was stale and hosted copied an older, current one). An
/// equivalent that read other relations is passed over: its result was
/// computed from inputs nothing shows this submit reads (#264). Hosted
/// builds a dev downstream over an independently built dev upstream
/// (`explain_upstream_origin_*.jsonl`, cases C and D) and copies a source
/// that read the destination's upstream over one that read another relation
/// of the same name (`copy_source_choice.jsonl`, #239).
pub(super) fn offer(
    evidence: &ExecutionEvidence,
    freshness: &Freshness<'_>,
    reads: &[InputRead],
    source: Option<ExecutionRow>,
) -> Offer {
    let Some(source) = source else {
        return Offer::Stop;
    };
    match reads_as_recorded(evidence, reads, &source) {
        Some(upstreams) => match judge_equivalent(evidence, freshness, &upstreams, source) {
            Some(judged) => Offer::Choose(judged),
            None => Offer::Next,
        },
        None => Offer::Next,
    }
}

/// How fresh an equivalent would be if reused for this submit, judged
/// exactly as if it were the reuse candidate over `upstreams`, the submit's
/// upstream reads as the equivalent recorded them; or `None` when this
/// submit would rebuild it.
pub(super) fn judge_equivalent(
    evidence: &ExecutionEvidence,
    freshness: &Freshness<'_>,
    upstreams: &[InputTable],
    mut source: ExecutionRow,
) -> Option<Judged> {
    crate::copy_lifecycle::adopt_current_inputs(&mut source, upstreams);
    let selection = Selection::of_candidate(source.clone());
    let mut ctx = context(evidence, &selection, freshness, Some(&source));
    ctx.input_tables = upstreams;
    ctx.target_table = Some(source.target_table.as_str());
    decision::reusable(&ctx, &source).map(|reuse| Judged {
        reuse,
        build_age_ms: ctx.build_age_ms,
    })
}

/// Whether `input` is the submit's Target relation, however it is spelled.
pub(super) fn is_target_relation(evidence: &ExecutionEvidence, input: &InputTable) -> bool {
    canonical_identifiers(&input.name) == canonical_identifiers(&evidence.target_table)
}

/// The submit's upstream reads (every input but its Target relation), when
/// `source` recorded exactly the physical relations they are, however each
/// is spelled ([`crate::copy_lifecycle::canonical_reads`]); `None` when it
/// read others. An input read through a copy this server recorded is the
/// relation `source` read when `source` read the copy's source build: it is
/// then given as its Stand-in, judged at that build's epoch. Otherwise each
/// input is itself, and a relation the source shares with the submit is
/// judged for drift (PR #127 review).
fn reads_as_recorded(
    evidence: &ExecutionEvidence,
    reads: &[InputRead],
    source: &ExecutionRow,
) -> Option<Vec<InputTable>> {
    use crate::copy_lifecycle::canonical_reads;
    let recorded = recorded_reads(source)?;
    let was_recorded = |input: &InputTable| recorded.contains(&canonical_identifiers(&input.name));
    let upstreams: Vec<&InputRead> = reads
        .iter()
        .filter(|read| !is_target_relation(evidence, &read.input))
        .collect();
    // A Stand-in for a relation the submit also reads directly would make
    // two reads one: such a copy stands for itself.
    let read_directly = |stand_in: &InputTable| {
        let name = canonical_identifiers(&stand_in.name);
        upstreams
            .iter()
            .any(|read| canonical_identifiers(&read.input.name) == name)
    };
    let upstreams: Vec<InputTable> = upstreams
        .iter()
        .map(|read| match &read.stand_in {
            Some(stand_in)
                if !was_recorded(&read.input)
                    && was_recorded(stand_in)
                    && !read_directly(stand_in) =>
            {
                stand_in.clone()
            }
            _ => read.input.clone(),
        })
        .collect();
    (canonical_reads(&upstreams, &source.target_table, None) == recorded).then_some(upstreams)
}

/// The physical relations `source` recorded reading at build time, other
/// than its Target relation, canonical and sorted. A client-requested copy
/// records no reads of its own: it holds the build it was copied from, so it
/// has the reads that build recorded (`clone_source_inputs`); `None` when
/// they were not recorded (no confirmed source build was read).
fn recorded_reads(source: &ExecutionRow) -> Option<Vec<Vec<String>>> {
    use crate::copy_lifecycle::{canonical_reads, is_copy_registration};
    if is_copy_registration(source) && source.input_tables.is_empty() {
        let copied: Vec<InputTable> = source
            .clone_source_inputs
            .as_ref()?
            .iter()
            .map(|name| InputTable::at(name, None))
            .collect();
        return Some(canonical_reads(&copied, &source.target_table, None));
    }
    Some(canonical_reads(
        &source.input_tables,
        &source.target_table,
        None,
    ))
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
    const DEV_UP: &str = "cdo.dev.up";
    const PROD_UP: &str = "cdo.prod.up";
    const NOW_MS: i64 = 10_000_000;

    fn read(name: &str, epoch: i64) -> InputTable {
        InputTable {
            producer: Producer::NoBuild,
            ..InputTable::at(name, Some(epoch))
        }
    }

    fn built(name: &str, epoch: i64, producer: &str) -> InputTable {
        InputTable {
            producer: Producer::Build(producer.into()),
            ..InputTable::at(name, Some(epoch))
        }
    }

    /// `name`, read through a copy as the build of `PROD_UP` at `epoch`.
    fn stand_in(name: &str, epoch: i64) -> InputTable {
        InputTable {
            read_copy: Some(name.into()),
            ..built(PROD_UP, epoch, "prod-up")
        }
    }

    /// A dev submit reading `input`, its own target absent.
    fn evidence_reading(input: InputTable) -> ExecutionEvidence {
        let mut evidence = ExecutionEvidence::sql_submit(
            "o",
            &qc::SubmitEnrichedSqlRequest {
                sql: format!("select * from {}", input.name),
                dialect: "snowflake".into(),
                default_catalog: "cdo".into(),
                target_table: Some(DEV.into()),
                table_namespace: Some("ns".into()),
                execution_type: qc::ModelExecutionType::Full as i32,
                tables: vec![qc::TableModifiedInfo {
                    name: input.name.clone(),
                    last_modified_epoch: input.last_modified_epoch,
                }],
                ..Default::default()
            },
        );
        evidence.input_tables = vec![input];
        evidence
    }

    /// A dev submit reading `RAW` at epoch 200.
    fn evidence() -> ExecutionEvidence {
        evidence_reading(read(RAW, 200))
    }

    /// The submit's inputs, the first read through a copy as `stand_in`.
    fn reads(evidence: &ExecutionEvidence, stand_in: Option<InputTable>) -> Vec<InputRead> {
        let mut stand_in = stand_in;
        evidence
            .input_tables
            .iter()
            .map(|input| InputRead {
                input: input.clone(),
                stand_in: stand_in.take(),
            })
            .collect()
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
        let evidence = evidence();
        let offer = offer(
            &evidence,
            &freshness(),
            &reads(&evidence, None),
            Some(equivalent(read(RAW, 200))),
        );
        assert!(matches!(offer, Offer::Choose(_)), "{offer:?}");
    }

    #[test]
    fn a_stale_same_upstream_equivalent_passes_to_the_next_one() {
        let (evidence, freshness) = (evidence(), freshness());
        let stale = offer(
            &evidence,
            &freshness,
            &reads(&evidence, None),
            Some(equivalent(read(RAW, 100))),
        );
        assert_eq!(stale, Offer::Next);
        let current = offer(
            &evidence,
            &freshness,
            &reads(&evidence, None),
            Some(equivalent(read(RAW, 200))),
        );
        assert!(matches!(current, Offer::Choose(_)), "{current:?}");
    }

    #[test]
    fn an_equivalent_that_read_another_relation_is_passed_over() {
        let (evidence, freshness) = (evidence(), freshness());
        assert_eq!(
            offer(
                &evidence,
                &freshness,
                &reads(&evidence, None),
                Some(equivalent(read(OTHER_RAW, 200)))
            ),
            Offer::Next
        );
    }

    #[test]
    fn an_independently_built_upstream_is_another_input() {
        // Case D of #237: the dev upstream was built in dev, not copied.
        let evidence = evidence_reading(built(DEV_UP, 600, "dev-up"));
        let prod = equivalent(built(PROD_UP, 500, "prod-up"));
        assert_eq!(
            offer(&evidence, &freshness(), &reads(&evidence, None), Some(prod)),
            Offer::Next
        );
    }

    #[test]
    fn an_upstream_copied_from_the_build_the_equivalent_read_is_judged_as_that_build() {
        let evidence = evidence_reading(built(DEV_UP, 600, "dev-copy"));
        let prod = equivalent(built(PROD_UP, 500, "prod-up"));
        let offer = offer(
            &evidence,
            &freshness(),
            &reads(&evidence, Some(stand_in(DEV_UP, 500))),
            Some(prod),
        );
        assert!(matches!(offer, Offer::Choose(_)), "{offer:?}");
    }

    #[test]
    fn a_stand_in_proves_freshness_only_at_the_epoch_the_equivalent_recorded() {
        let evidence = evidence_reading(built(DEV_UP, 600, "dev-copy"));
        let freshness = freshness();
        for recorded in [499, 501] {
            let prod = equivalent(built(PROD_UP, recorded, "prod-up"));
            assert_eq!(
                offer(
                    &evidence,
                    &freshness,
                    &reads(&evidence, Some(stand_in(DEV_UP, 500))),
                    Some(prod)
                ),
                Offer::Next,
                "recorded at {recorded}"
            );
        }
    }

    #[test]
    fn an_equivalent_that_read_the_copy_itself_is_judged_on_the_copy() {
        let evidence = evidence_reading(built(DEV_UP, 600, "dev-copy"));
        let dev_reader = equivalent(built(DEV_UP, 600, "dev-copy"));
        let offer = offer(
            &evidence,
            &freshness(),
            &reads(&evidence, Some(stand_in(DEV_UP, 500))),
            Some(dev_reader),
        );
        assert!(matches!(offer, Offer::Choose(_)), "{offer:?}");
    }

    #[test]
    fn a_stand_in_for_a_relation_the_submit_also_reads_stands_for_itself() {
        let mut evidence = evidence_reading(built(DEV_UP, 600, "dev-copy"));
        evidence.input_tables.push(built(PROD_UP, 500, "prod-up"));
        let prod = equivalent(built(PROD_UP, 500, "prod-up"));
        assert_eq!(
            offer(
                &evidence,
                &freshness(),
                &reads(&evidence, Some(stand_in(DEV_UP, 500))),
                Some(prod)
            ),
            Offer::Next
        );
    }

    /// A client-requested copy under `PROD` of a build that recorded
    /// reading `copied` (`None`: no source build was read).
    fn copy_registration(copied: Option<&str>) -> ExecutionRow {
        ExecutionRow {
            input_tables: Vec::new(),
            clone_source_table: Some("cdo.ci.orders".into()),
            clone_source_inputs: copied.map(|name| vec![name.to_string()]),
            ..equivalent(read(RAW, 200))
        }
    }

    #[test]
    fn a_copy_registration_has_the_reads_of_the_build_it_was_copied_from() {
        let evidence = evidence();
        let reads = reads(&evidence, None);
        let same = offer(
            &evidence,
            &freshness(),
            &reads,
            Some(copy_registration(Some(RAW))),
        );
        assert!(matches!(same, Offer::Choose(_)), "{same:?}");
        for copied in [Some(OTHER_RAW), None] {
            assert_eq!(
                offer(
                    &evidence,
                    &freshness(),
                    &reads,
                    Some(copy_registration(copied))
                ),
                Offer::Next,
                "copied build read {copied:?}"
            );
        }
    }

    #[test]
    fn an_equivalent_whose_state_vanished_ends_the_search() {
        let evidence = evidence();
        assert_eq!(
            offer(&evidence, &freshness(), &reads(&evidence, None), None),
            Offer::Stop
        );
    }
}
