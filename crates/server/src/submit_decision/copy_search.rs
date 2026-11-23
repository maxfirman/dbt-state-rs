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
    /// Copy this equivalent. `inputs` are what the copy records: the
    /// inputs of the build it holds ([`held_inputs`]).
    Choose {
        judged: Judged,
        inputs: Vec<InputTable>,
    },
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
/// that build recorded, with that build's Target relation (not the
/// candidate's) set aside, and with the candidate's own build age (a copy's
/// age is its copy time: hosted copied a copy within Lag tolerance whose
/// source build was older, lag_copy_candidates `wba`). The first one fresh
/// enough is chosen; a stale one passes the search on to the next
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
    let Some(aligned) = reads_as_recorded(req, evidence, reads, held) else {
        return Offer::Next;
    };
    let upstreams: Vec<InputTable> = aligned.iter().map(|a| a.as_recorded.clone()).collect();
    let judged = ExecutionRow {
        input_tables: held.input_tables.clone(),
        ..candidate
    };
    match judge_equivalent(evidence, freshness, &upstreams, judged, &held.target_table) {
        Some(judged) => Offer::Choose {
            judged,
            inputs: held_inputs(evidence, held, &aligned),
        },
        None => Offer::Next,
    }
}

/// How fresh an equivalent would be if reused for this submit, judged
/// exactly as if it were the reuse candidate over `upstreams`, the submit's
/// upstream reads as the equivalent recorded them, with `recorded_target`
/// (the Target relation of the build that recorded its inputs) set aside;
/// or `None` when this submit would rebuild it.
pub(super) fn judge_equivalent(
    evidence: &ExecutionEvidence,
    freshness: &Freshness<'_>,
    upstreams: &[InputTable],
    mut source: ExecutionRow,
    recorded_target: &str,
) -> Option<Judged> {
    crate::copy_lifecycle::adopt_current_inputs(&mut source, upstreams);
    let selection = Selection::of_candidate(source.clone());
    let mut ctx = context(evidence, &selection, freshness, Some(&source));
    ctx.input_tables = upstreams;
    ctx.target_table = Some(recorded_target);
    decision::reusable(&ctx, &source).map(|reuse| Judged {
        reuse,
        build_age_ms: ctx.build_age_ms,
    })
}

/// Whether `input` is the submit's Target relation, however it is spelled.
pub(super) fn is_target_relation(evidence: &ExecutionEvidence, input: &InputTable) -> bool {
    canonical_identifiers(&input.name) == canonical_identifiers(&evidence.target_table)
}

/// One of the submit's upstream reads, with the relation the held build
/// recorded it as: itself, or its Stand-in.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct AlignedRead {
    pub read: InputTable,
    pub as_recorded: InputTable,
}

impl AlignedRead {
    fn stood_in(&self) -> bool {
        self.read != self.as_recorded
    }
}

/// The submit's upstream reads (every input but its own Target relation),
/// each aligned with the relation `held`, a build, recorded at build time,
/// when they are exactly the physical relations `held` recorded, however
/// each is spelled ([`crate::copy_lifecycle::canonical_reads`]); `None`
/// otherwise:
///
/// - Each input is itself, and a relation `held` shares with the submit is
///   judged for drift (PR #127 review). `held`'s own Target relation is a
///   genuine input of the submit when the submit reads it, never filtered.
/// - An input read through a copy this server recorded is given as its
///   Stand-in when `held` did not record the input but recorded the
///   Stand-in's relation at exactly the Stand-in's epoch: it then holds the
///   build `held` read. A copy of a relation the submit also reads directly
///   stands for itself. With a Stand-in, the submit's SQL and query
///   dependency definitions with each copy read as its source must have
///   `held`'s logic, so each reference still reads what `held`'s did (a
///   Snowflake schema-abstracted identity only).
/// - On Snowflake, a submit the Warehouse rules copy must report every base
///   relation its SQL reads ([`sql_reads_are_reported`]): a read nothing
///   reports proves nothing. DuckDB's checked copy graph already binds each
///   physical input exactly.
pub(super) fn reads_as_recorded(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    reads: &[InputRead],
    held: &ExecutionRow,
) -> Option<Vec<AlignedRead>> {
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
    if !sql_reads_are_reported(req, evidence, &upstreams) {
        return None;
    }
    let read_directly = |stand_in: &InputTable| {
        let name = canonical_identifiers(&stand_in.name);
        upstreams
            .iter()
            .any(|read| canonical_identifiers(&read.input.name) == name)
    };
    let aligned: Vec<AlignedRead> = upstreams
        .iter()
        .map(|read| AlignedRead {
            read: read.input.clone(),
            as_recorded: match &read.stand_in {
                Some(stand_in)
                    if !was_recorded(&read.input)
                        && recorded_at(stand_in) == [stand_in.last_modified_epoch]
                        && !read_directly(stand_in) =>
                {
                    stand_in.clone()
                }
                _ => read.input.clone(),
            },
        })
        .collect();
    let as_recorded: Vec<InputTable> = aligned.iter().map(|a| a.as_recorded.clone()).collect();
    let same_reads = canonical_reads(&as_recorded, &evidence.target_table, None) == recorded;
    let stood_in: Vec<(&str, &str)> = aligned
        .iter()
        .filter(|a| a.stood_in())
        .map(|a| (a.read.name.as_str(), a.as_recorded.name.as_str()))
        .collect();
    (same_reads && (stood_in.is_empty() || keeps_bindings(req, &stood_in, held))).then_some(aligned)
}

/// The inputs a Result copy of `held` into the submit's Target relation
/// records (#264): the submit's own Target relation entries as reported,
/// then each upstream in the submit's spelling, at the epoch and with the
/// Upstream producer `held` recorded for it, or as currently read when it is
/// read through a copy of the build `held` read (`aligned`). The copy holds
/// `held`'s rows, so its next submit is judged for drift against what those
/// rows were computed from, and its Lag tolerance runs out as theirs would.
pub(super) fn held_inputs(
    evidence: &ExecutionEvidence,
    held: &ExecutionRow,
    aligned: &[AlignedRead],
) -> Vec<InputTable> {
    let own = evidence
        .input_tables
        .iter()
        .filter(|t| is_target_relation(evidence, t))
        .cloned();
    let upstreams = aligned.iter().map(|a| {
        if a.stood_in() {
            return a.read.clone();
        }
        let name = canonical_identifiers(&a.read.name);
        held.input_tables
            .iter()
            .find(|t| canonical_identifiers(&t.name) == name)
            .map(|t| InputTable {
                name: a.read.name.clone(),
                ..t.clone()
            })
            .unwrap_or_else(|| a.read.clone())
    });
    own.chain(upstreams).collect()
}

/// Whether `copy`, a Result copy that is the submit's Reuse candidate,
/// recorded for each upstream exactly what [`held_inputs`] gives for the
/// build it holds: no copy claims fresher inputs than its rows were
/// computed from. A copy an earlier server recorded at the epochs its
/// submit read, when the build it holds read older ones, does not.
pub(super) fn records_held_inputs(
    evidence: &ExecutionEvidence,
    copy: &ExecutionRow,
    held: &ExecutionRow,
    aligned: &[AlignedRead],
) -> bool {
    use std::collections::BTreeMap;
    let key = |inputs: &mut dyn Iterator<Item = &InputTable>| {
        inputs
            .filter(|t| canonical_identifiers(&t.name) != canonical_identifiers(&copy.target_table))
            .map(|t| {
                (
                    canonical_identifiers(&t.name),
                    (t.last_modified_epoch, t.producer.clone()),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let derived = held_inputs(evidence, held, aligned);
    key(&mut copy.input_tables.iter()) == key(&mut derived.iter())
}

/// Whether the submit reports every base relation its SQL reads, on
/// Snowflake, for a submit the Warehouse rules copy. A base relation is one
/// the root SQL, or the definition of a query dependency it reads, names in
/// FROM or JOIN and that is no query dependency itself (a view read through
/// its definition, which the copy logic identity binds to its name); a CTE
/// reference or a view's declared name is none. A read of the submit's own
/// Target relation is never covered: its prior contents differ between the
/// two Target relations. SQL this cannot resolve proves nothing. A kind
/// that only reports its equivalent in its codes is not copied, so its
/// reads need no proof (client 2.0.11 reports no reads for a view).
fn sql_reads_are_reported(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    upstreams: &[&InputRead],
) -> bool {
    let warehouse = Warehouse::from_dialect(&req.dialect);
    if warehouse != Warehouse::Snowflake
        || !warehouse.submit_copy_admits(req.execution_type, &req.semantic_extras)
    {
        return true;
    }
    let Some(base) = base_relations(req) else {
        return false;
    };
    let target = canonical_identifiers(&evidence.target_table);
    base.iter().all(|relation| {
        *relation != target
            && upstreams
                .iter()
                .any(|read| canonical_identifiers(&read.input.name) == *relation)
    })
}

/// The base relations of `req` ([`sql_reads_are_reported`]), each in three
/// canonical parts, a name in fewer parts resolved in the default catalog
/// and schema of the SQL that names it; `None` when a name cannot be
/// resolved or the SQL does not parse.
fn base_relations(req: &qc::SubmitEnrichedSqlRequest) -> Option<Vec<Vec<String>>> {
    use sqlparser::dialect::SnowflakeDialect;
    let canonical = |raw: &str| crate::sql_norm::canonical_identifier(raw);
    let dependencies: Vec<Vec<String>> = req
        .query_dependencies
        .iter()
        .map(|d| canonical_identifiers(&d.name))
        .collect();
    let mut seen = vec![false; dependencies.len()];
    let root_schema = req.default_schema.clone().unwrap_or_default();
    let mut pending = vec![(req.sql.as_str(), req.default_catalog.as_str(), root_schema)];
    let mut base = Vec::new();
    while let Some((sql, catalog, schema)) = pending.pop() {
        for mut name in crate::sql_identity::relations_in(sql, &SnowflakeDialect {})? {
            if name.len() == 2 && !catalog.is_empty() {
                name.insert(0, canonical(catalog));
            } else if name.len() == 1 && !catalog.is_empty() && !schema.is_empty() {
                name.insert(0, canonical(&schema));
                name.insert(0, canonical(catalog));
            }
            if name.len() != 3 {
                return None;
            }
            match dependencies.iter().position(|dep| *dep == name) {
                Some(i) => {
                    if !std::mem::replace(&mut seen[i], true) {
                        let d = &req.query_dependencies[i];
                        pending.push((
                            d.query.as_str(),
                            d.default_catalog.as_str(),
                            d.default_schema.clone(),
                        ));
                    }
                }
                None => base.push(name),
            }
        }
    }
    Some(base)
}

/// Whether the submit's SQL and query dependency definitions, each copy in
/// `stood_in` (copy, source) read as its source and its Target relation's
/// schema read as `held`'s, have the copy logic identity of `held`: each
/// reference then reads the relation `held`'s did, so two copies swapped
/// between references, in the root SQL or in a view's definition, differ.
fn keeps_bindings(
    req: &qc::SubmitEnrichedSqlRequest,
    stood_in: &[(&str, &str)],
    held: &ExecutionRow,
) -> bool {
    use crate::sql_norm::{schema_of_object_name, substitute_relations};
    let substitute = |parts: &[String]| {
        stood_in
            .iter()
            .find(|(copy, _)| canonical_identifiers(copy) == parts)
            .map(|(_, source)| canonical_identifiers(source))
    };
    let mut read_as_source = crate::execution::sql_execution(req);
    let Some(sql) = substitute_relations(&read_as_source.sql, &substitute) else {
        return false;
    };
    read_as_source.sql = sql;
    for dependency in &mut read_as_source.query_dependencies {
        let Some(query) = substitute_relations(&dependency.query, &substitute) else {
            return false;
        };
        dependency.query = query;
    }
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
        assert!(matches!(offer, Offer::Choose { .. }), "{offer:?}");
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
        assert!(matches!(offer, Offer::Choose { .. }), "{offer:?}");
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
        assert!(matches!(offer, Offer::Choose { .. }), "{offer:?}");
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
        assert!(matches!(offer, Offer::Choose { .. }), "{offer:?}");
    }

    #[test]
    fn copies_swapped_inside_a_view_definition_keep_their_bindings() {
        let (p, q, a, b) = ("cdo.p.u", "cdo.q.u", "cdo.a.u", "cdo.b.u");
        let view = "cdo.zzz.v";
        let with_view = |mut req: qc::SubmitEnrichedSqlRequest, left: &str, right: &str| {
            req.sql = format!("select * from {view}");
            req.query_dependencies = vec![qc::QueryDependency {
                name: view.into(),
                query: format!(
                    "create or replace view v as select l.v from {left} l cross join {right} r"
                ),
                default_catalog: "cdo".into(),
                default_schema: "zzz".into(),
            }];
            req
        };
        let prod_inputs = vec![built(p, 100, "p"), built(q, 200, "q")];
        let mut prod = build_of("", prod_inputs.clone());
        prod.node_namespace_logic_hash =
            Some(crate::analyzed_workload::identity::namespace_logic_hash(
                &crate::execution::sql_execution(&with_view(request(PROD, "", &prod_inputs), p, q)),
            ));
        let stood = |copy: &str, source: &str, epoch, producer: &str| InputRead {
            input: built(copy, epoch + 1_000, "copy"),
            stand_in: Some(InputTable {
                read_copy: Some(copy.into()),
                ..built(source, epoch, producer)
            }),
        };
        let reads = [stood(a, q, 200, "q"), stood(b, p, 100, "p")];
        for (left, right, copied) in [(a, b, false), (b, a, true)] {
            let mut dev = submit(vec![built(a, 1_200, "copy"), built(b, 1_100, "copy")]);
            dev.req = with_view(dev.req, left, right);
            let offer = dev.offer(&freshness(), &reads, prod.clone());
            assert_eq!(
                matches!(offer, Offer::Choose { .. }),
                copied,
                "{left} {right}: {offer:?}"
            );
        }
    }

    #[test]
    fn every_base_relation_the_sql_reads_must_be_reported() {
        let (local, fixed) = ("cdo.dev.u", "cdo.aaa.fixed");
        let sql = format!("select l.v from {local} l cross join {fixed} f");
        let partial = submit_sql(&sql, vec![read(fixed, 100)]);
        let prod = build_of(&sql.replace("dev", "prod"), vec![read(fixed, 100)]);
        assert_eq!(
            partial.offer(&freshness(), &partial.reads(None), prod),
            Offer::Next
        );
    }

    #[test]
    fn a_read_of_its_own_target_relation_is_never_copied() {
        let sql = format!("select m.id + s.v as id from {DEV} m cross join {RAW} s");
        let dev = submit_sql(&sql, vec![read(DEV, 10), read(RAW, 50)]);
        let prod = build_of(
            &sql.replace("dev", "prod"),
            vec![read(PROD, 5), read(RAW, 50)],
        );
        assert_eq!(dev.offer(&freshness(), &dev.reads(None), prod), Offer::Next);
    }

    #[test]
    fn cte_references_and_view_declarations_are_not_base_relations() {
        let view = |query: &str| {
            let mut req = request(
                DEV,
                "with c as (select v from cdo.zzz.v) select v from c",
                &[],
            );
            req.query_dependencies = vec![qc::QueryDependency {
                name: "cdo.zzz.v".into(),
                query: query.into(),
                default_catalog: "cdo".into(),
                default_schema: "zzz".into(),
            }];
            base_relations(&req)
        };
        assert_eq!(view("select 1 as v"), Some(Vec::new()));
        assert_eq!(
            view("create or replace view v as (select 1 as v)"),
            Some(Vec::new())
        );
        assert_eq!(
            view("create or replace view v as select v from raw"),
            Some(vec![vec!["CDO".into(), "ZZZ".into(), "RAW".into()]])
        );
    }

    #[test]
    fn a_copy_records_the_inputs_of_the_build_it_holds() {
        // Copied within Lag tolerance after the input moved on: the copy
        // records the epoch its rows were computed from.
        let dev = submit(vec![read(RAW, 2_500)]);
        let mut freshness = freshness();
        freshness.tolerance_ms = 60_000;
        let prod = equivalent(read(RAW, 100));
        let Offer::Choose { inputs, .. } = dev.offer(&freshness, &dev.reads(None), prod.clone())
        else {
            panic!("copied within tolerance");
        };
        assert_eq!(inputs, vec![read(RAW, 100)]);
        let aligned = reads_as_recorded(&dev.req, &dev.evidence, &dev.reads(None), &prod).unwrap();
        let recorded = |inputs| ExecutionRow {
            target_table: DEV.into(),
            input_tables: inputs,
            ..prod.clone()
        };
        assert!(records_held_inputs(
            &dev.evidence,
            &recorded(inputs),
            &prod,
            &aligned
        ));
        // A copy recorded at the epoch its submit read claims fresher rows.
        assert!(!records_held_inputs(
            &dev.evidence,
            &recorded(vec![read(RAW, 2_500)]),
            &prod,
            &aligned
        ));
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
        assert!(matches!(offer, Offer::Choose { .. }), "{offer:?}");
    }
}
