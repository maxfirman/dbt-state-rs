//! Which equivalent under another name a SQL submit's Result copy fallback
//! copies. The fallback offers each eligible equivalent, in the store's
//! preference order (newest build first), with the equivalent's current
//! physical state; [`offer`] decides, without reading.

use super::{context, Freshness};
use crate::decision;
use crate::execution::ExecutionEvidence;
use crate::query_cache as qc;
use crate::reuse_candidate::Selection;
use crate::sql_norm::canonical_identifiers;
use crate::stand_in::InputRead;
use crate::store::{ExecutionRow, InputTable};
use crate::warehouse::Warehouse;

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
}

/// Offer an eligible equivalent, `candidate` (its current physical state),
/// with `held`, the build whose result it holds (itself, or for a copy the
/// build it was copied from, resolved by the caller). `reads` are the
/// submit's inputs with their Stand-ins ([`crate::stand_in::input_reads`]).
///
/// An equivalent qualifies only when the build it holds was computed from
/// the physical inputs this submit reads ([`reads_as_recorded`]). It is then
/// judged exactly as if it were the reuse candidate, over the input epochs
/// that build recorded and with its own build age (a copy's age is its copy
/// time: hosted copied a copy within Lag tolerance whose source build was
/// older, lag_copy_candidates `wba`). The first one fresh enough is chosen;
/// a stale one passes the search on to the next
/// (VERIFIED LIVE 2026-11-10, lag_copy_candidates `sne`: the newest
/// equivalent was stale and hosted copied an older, current one). An
/// equivalent computed from other inputs is passed over (#264). Hosted
/// builds a dev downstream over an independently built dev upstream
/// (`explain_upstream_origin_*.jsonl`, cases C and D) and copies a source
/// that read the destination's upstream over one that read another relation
/// of the same name (`copy_source_choice.jsonl`, #239).
pub(super) fn offer(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    freshness: &Freshness<'_>,
    reads: &[InputRead],
    candidate: ExecutionRow,
    held: &ExecutionRow,
) -> Offer {
    let Some(upstreams) = reads_as_recorded(req, evidence, reads, held) else {
        return Offer::Next;
    };
    let judged = ExecutionRow {
        input_tables: held.input_tables.clone(),
        ..candidate
    };
    match judge_equivalent(evidence, freshness, &upstreams, judged) {
        Some(judged) => Offer::Choose(judged),
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

/// The submit's upstream reads (every input but its own Target relation),
/// when `held`, a build, recorded at build time exactly the physical
/// relations they are, however each is spelled
/// ([`crate::copy_lifecycle::canonical_reads`]); `None` otherwise:
///
/// - Each input is itself, and a relation `held` shares with the submit is
///   judged for drift (PR #127 review). `held`'s own Target relation is a
///   genuine input of the submit when the submit reads it, never filtered.
/// - An input read through a copy this server recorded is given as its
///   Stand-in when `held` did not record the input but recorded the
///   Stand-in's relation at exactly the Stand-in's epoch: it then holds the
///   build `held` read. A copy of a relation the submit also reads directly
///   stands for itself. With a Stand-in, the submit's SQL with each copy
///   read as its source must have `held`'s logic, so each reference still
///   reads what `held`'s did (a Snowflake schema-abstracted identity only).
/// - A submit that may read a relation but reports no upstream reads proves
///   nothing about them, and is not copied.
pub(super) fn reads_as_recorded(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    reads: &[InputRead],
    held: &ExecutionRow,
) -> Option<Vec<InputTable>> {
    use crate::copy_lifecycle::canonical_reads;
    let recorded = canonical_reads(&held.input_tables, &held.target_table, None);
    let recorded_at = |input: &InputTable| {
        let name = canonical_identifiers(&input.name);
        held.input_tables
            .iter()
            .filter(|t| canonical_identifiers(&t.name) == name)
            .map(|t| t.last_modified_epoch)
            .collect::<Vec<_>>()
    };
    let was_recorded = |input: &InputTable| recorded.contains(&canonical_identifiers(&input.name));
    let upstreams: Vec<&InputRead> = reads
        .iter()
        .filter(|read| !is_target_relation(evidence, &read.input))
        .collect();
    if upstreams.is_empty() && !proves_no_upstreams(req, evidence) {
        return None;
    }
    let read_directly = |stand_in: &InputTable| {
        let name = canonical_identifiers(&stand_in.name);
        upstreams
            .iter()
            .any(|read| canonical_identifiers(&read.input.name) == name)
    };
    let mut stood_in = Vec::new();
    let upstreams: Vec<InputTable> = upstreams
        .iter()
        .map(|read| match &read.stand_in {
            Some(stand_in)
                if !was_recorded(&read.input)
                    && recorded_at(stand_in) == [stand_in.last_modified_epoch]
                    && !read_directly(stand_in) =>
            {
                stood_in.push((read.input.name.as_str(), stand_in.name.as_str()));
                stand_in.clone()
            }
            _ => read.input.clone(),
        })
        .collect();
    let same_reads = canonical_reads(&upstreams, &evidence.target_table, None) == recorded;
    (same_reads && (stood_in.is_empty() || keeps_bindings(req, &stood_in, held)))
        .then_some(upstreams)
}

/// Whether a submit that reports no upstream reads reads none: its SQL reads
/// no base relation, or the Warehouse rules never copy it (a kind that only
/// reports its equivalent in its codes; client 2.0.11 reports no reads for
/// a view).
fn proves_no_upstreams(req: &qc::SubmitEnrichedSqlRequest, evidence: &ExecutionEvidence) -> bool {
    evidence.sql_reads_relation == Some(false)
        || reads_no_base_relation(req)
        || !Warehouse::from_dialect(&req.dialect)
            .submit_copy_admits(req.execution_type, &req.semantic_extras)
}

/// Whether every relation the submit's SQL names is one of its query
/// dependencies, each read through its definition (which its copy logic
/// identity binds to the name), and those definitions in turn read no
/// relation but other query dependencies. A relation named in one or two
/// parts other than through the default catalog, and SQL that does not
/// parse, count as a base read.
fn reads_no_base_relation(req: &qc::SubmitEnrichedSqlRequest) -> bool {
    let warehouse = Warehouse::from_dialect(&req.dialect);
    let catalog = crate::sql_norm::canonical_identifier(&req.default_catalog);
    let dependencies: Vec<(Vec<String>, &str)> = req
        .query_dependencies
        .iter()
        .map(|d| (canonical_identifiers(&d.name), d.query.as_str()))
        .collect();
    let mut seen = vec![false; dependencies.len()];
    let mut pending = vec![req.sql.as_str()];
    while let Some(sql) = pending.pop() {
        if warehouse.sql_reads_relation(sql) == Some(false) {
            continue;
        }
        let Some(names) = crate::sql_norm::relation_names(sql) else {
            return false;
        };
        for mut name in names {
            if name.len() == 2 && !catalog.is_empty() {
                name.insert(0, catalog.clone());
            }
            let Some(i) = dependencies.iter().position(|(dep, _)| *dep == name) else {
                return false;
            };
            if !std::mem::replace(&mut seen[i], true) {
                pending.push(dependencies[i].1);
            }
        }
    }
    true
}

/// Whether the submit's SQL, each copy in `stood_in` (copy, source) read as
/// its source and its Target relation's schema read as `held`'s, has the
/// copy logic identity of `held`: each reference then reads the relation
/// `held`'s did, so two copies swapped between references differ.
fn keeps_bindings(
    req: &qc::SubmitEnrichedSqlRequest,
    stood_in: &[(&str, &str)],
    held: &ExecutionRow,
) -> bool {
    use crate::sql_norm::schema_of_object_name;
    let substitute = |parts: &[String]| {
        stood_in
            .iter()
            .find(|(copy, _)| canonical_identifiers(copy) == parts)
            .map(|(_, source)| canonical_identifiers(source))
    };
    let Some(sql) = crate::sql_norm::substitute_relations(&req.sql, &substitute) else {
        return false;
    };
    let mut read_as_source = crate::execution::sql_execution(req);
    read_as_source.sql = sql;
    let from = req.target_table.as_deref().and_then(schema_of_object_name);
    let to = schema_of_object_name(&held.target_table);
    let (Some(from), Some(to)) = (from, to) else {
        return false;
    };
    crate::analyzed_workload::identity::rehomed_copy_logic_hash(&read_as_source, &from, &to)
        .is_some_and(|hash| held.node_namespace_logic_hash.as_deref() == Some(hash.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Producer;

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

    /// A FULL Snowflake submit into `target` of `sql` over `inputs`.
    fn request(target: &str, sql: &str, inputs: &[InputTable]) -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            sql: sql.into(),
            dialect: "snowflake".into(),
            default_catalog: "cdo".into(),
            target_table: Some(target.into()),
            table_namespace: Some("ns".into()),
            execution_type: qc::ModelExecutionType::Full as i32,
            tables: inputs
                .iter()
                .map(|t| qc::TableModifiedInfo {
                    name: t.name.clone(),
                    last_modified_epoch: t.last_modified_epoch,
                })
                .collect(),
            ..Default::default()
        }
    }

    /// A dev submit selecting from each of `inputs`, its own target absent.
    struct Submit {
        req: qc::SubmitEnrichedSqlRequest,
        evidence: ExecutionEvidence,
    }

    fn submit(inputs: Vec<InputTable>) -> Submit {
        submit_sql(&join_sql(&inputs), inputs)
    }

    fn submit_sql(sql: &str, inputs: Vec<InputTable>) -> Submit {
        let req = request(DEV, sql, &inputs);
        let mut evidence = ExecutionEvidence::sql_submit("o", &req);
        evidence.input_tables = inputs;
        Submit { req, evidence }
    }

    fn join_sql(inputs: &[InputTable]) -> String {
        let names: Vec<&str> = inputs.iter().map(|t| t.name.as_str()).collect();
        format!("select * from {}", names.join(" cross join "))
    }

    impl Submit {
        /// The submit's inputs, the first read through a copy as `stand_in`.
        fn reads(&self, stand_in: Option<InputTable>) -> Vec<InputRead> {
            let mut stand_in = stand_in;
            self.evidence
                .input_tables
                .iter()
                .map(|input| InputRead {
                    input: input.clone(),
                    stand_in: stand_in.take(),
                })
                .collect()
        }

        fn offer(
            &self,
            freshness: &Freshness<'_>,
            reads: &[InputRead],
            held: ExecutionRow,
        ) -> Offer {
            offer(
                &self.req,
                &self.evidence,
                freshness,
                reads,
                held.clone(),
                &held,
            )
        }
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

    /// The current build under `PROD` of `sql` over `inputs`.
    fn build_of(sql: &str, inputs: Vec<InputTable>) -> ExecutionRow {
        let req = request(PROD, sql, &inputs);
        ExecutionRow {
            id: 7,
            target_table: PROD.into(),
            execution_type: qc::ModelExecutionType::Full as i32,
            table_namespace: Some("ns".into()),
            node_namespace_logic_hash: Some(
                crate::analyzed_workload::identity::namespace_logic_hash(
                    &crate::execution::sql_execution(&req),
                ),
            ),
            last_modified_epoch: Some(NOW_MS - 1_000),
            input_tables: inputs,
            status: "confirmed".into(),
            ..ExecutionRow::default()
        }
    }

    /// The current build under `PROD` that read `input`.
    fn equivalent(input: InputTable) -> ExecutionRow {
        build_of(&join_sql(std::slice::from_ref(&input)), vec![input])
    }

    #[test]
    fn a_same_upstream_equivalent_is_judged_as_the_reuse_candidate() {
        let dev = submit(vec![read(RAW, 200)]);
        let offer = dev.offer(&freshness(), &dev.reads(None), equivalent(read(RAW, 200)));
        assert!(matches!(offer, Offer::Choose(_)), "{offer:?}");
    }

    #[test]
    fn a_stale_same_upstream_equivalent_passes_to_the_next_one() {
        let dev = submit(vec![read(RAW, 200)]);
        let stale = dev.offer(&freshness(), &dev.reads(None), equivalent(read(RAW, 100)));
        assert_eq!(stale, Offer::Next);
    }

    #[test]
    fn an_equivalent_that_read_another_relation_is_passed_over() {
        let dev = submit(vec![read(RAW, 200)]);
        assert_eq!(
            dev.offer(
                &freshness(),
                &dev.reads(None),
                equivalent(read(OTHER_RAW, 200))
            ),
            Offer::Next
        );
    }

    #[test]
    fn an_independently_built_upstream_is_another_input() {
        // Case D of #237: the dev upstream was built in dev, not copied.
        let dev = submit(vec![built(DEV_UP, 600, "dev-up")]);
        let prod = equivalent(built(PROD_UP, 500, "prod-up"));
        assert_eq!(dev.offer(&freshness(), &dev.reads(None), prod), Offer::Next);
    }

    #[test]
    fn an_upstream_copied_from_the_build_the_equivalent_read_is_judged_as_that_build() {
        let dev = submit(vec![built(DEV_UP, 600, "dev-copy")]);
        let prod = equivalent(built(PROD_UP, 500, "prod-up"));
        let offer = dev.offer(&freshness(), &dev.reads(Some(stand_in(DEV_UP, 500))), prod);
        assert!(matches!(offer, Offer::Choose(_)), "{offer:?}");
    }

    #[test]
    fn review266_stand_in_epoch_is_a_qualification_before_lag() {
        let dev = submit(vec![built(DEV_UP, 600, "dev-copy")]);
        let reads = dev.reads(Some(stand_in(DEV_UP, 500)));
        let mut freshness = freshness();
        // Lag tolerance does not stand in for the exact epoch (review of #266).
        freshness.tolerance_ms = 60_000;
        for recorded in [499, 501] {
            let prod = equivalent(built(PROD_UP, recorded, "prod-up"));
            assert_eq!(
                dev.offer(&freshness, &reads, prod),
                Offer::Next,
                "recorded at {recorded}"
            );
        }
    }

    #[test]
    fn an_equivalent_that_read_the_copy_itself_is_judged_on_the_copy() {
        let dev = submit(vec![built(DEV_UP, 600, "dev-copy")]);
        let dev_reader = equivalent(built(DEV_UP, 600, "dev-copy"));
        let offer = dev.offer(
            &freshness(),
            &dev.reads(Some(stand_in(DEV_UP, 500))),
            dev_reader,
        );
        assert!(matches!(offer, Offer::Choose(_)), "{offer:?}");
    }

    #[test]
    fn a_stand_in_for_a_relation_the_submit_also_reads_stands_for_itself() {
        let dev = submit(vec![
            built(DEV_UP, 600, "dev-copy"),
            built(PROD_UP, 500, "prod-up"),
        ]);
        let prod = equivalent(built(PROD_UP, 500, "prod-up"));
        assert_eq!(
            dev.offer(&freshness(), &dev.reads(Some(stand_in(DEV_UP, 500))), prod),
            Offer::Next
        );
    }

    #[test]
    fn copies_swapped_between_references_keep_their_bindings() {
        // Prod projects P from `P cross join Q`; dev reads A (a copy of Q)
        // then B (a copy of P): the same relations, bound the other way.
        let (p, q, a, b) = ("cdo.p.u", "cdo.q.u", "cdo.a.u", "cdo.b.u");
        let sql =
            |left: &str, right: &str| format!("select l.v from {left} l cross join {right} r");
        let prod = build_of(&sql(p, q), vec![built(p, 100, "p"), built(q, 200, "q")]);
        let stood = |copy: &str, source: &str, epoch, producer: &str| InputRead {
            input: built(copy, epoch + 1_000, "copy"),
            stand_in: Some(InputTable {
                read_copy: Some(copy.into()),
                ..built(source, epoch, producer)
            }),
        };
        let swapped = submit_sql(
            &sql(a, b),
            vec![built(a, 1_200, "copy"), built(b, 1_100, "copy")],
        );
        let reads = [stood(a, q, 200, "q"), stood(b, p, 100, "p")];
        assert_eq!(
            swapped.offer(&freshness(), &reads, prod.clone()),
            Offer::Next
        );
        // Control: bound as prod's were, the copies are prod's inputs.
        let aligned = submit_sql(
            &sql(b, a),
            vec![built(b, 1_100, "copy"), built(a, 1_200, "copy")],
        );
        let reads = [stood(b, p, 100, "p"), stood(a, q, 200, "q")];
        let offer = aligned.offer(&freshness(), &reads, prod);
        assert!(matches!(offer, Offer::Choose(_)), "{offer:?}");
    }

    #[test]
    fn the_equivalents_own_target_is_a_genuine_input_of_the_submit() {
        let sql = format!("select id + 1 as id from {PROD}");
        let dev = submit_sql(&sql, vec![built(PROD, 200, "prod-m")]);
        let prod = build_of(&sql, vec![built(PROD, 100, "prod-m-0")]);
        assert_eq!(dev.offer(&freshness(), &dev.reads(None), prod), Offer::Next);
    }

    #[test]
    fn no_reported_reads_prove_nothing_for_sql_that_reads_a_relation() {
        let sql = format!("select * from {RAW}");
        let mut dev = submit_sql(&sql, Vec::new());
        dev.evidence.sql_reads_relation = Some(true);
        let prod = build_of(&sql, Vec::new());
        assert_eq!(dev.offer(&freshness(), &[], prod.clone()), Offer::Next);
        dev.evidence.sql_reads_relation = None;
        assert_eq!(dev.offer(&freshness(), &[], prod), Offer::Next);
        // Control: SQL that reads no relation has no upstreams to prove.
        let mut constant = submit_sql("select 1 as id", Vec::new());
        constant.evidence.sql_reads_relation = Some(false);
        let offer = constant.offer(&freshness(), &[], build_of("select 1 as id", Vec::new()));
        assert!(matches!(offer, Offer::Choose(_)), "{offer:?}");
    }
}
