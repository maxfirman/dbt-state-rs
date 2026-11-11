//! Result copy planning: when a target is populated by copying an existing
//! result instead of executing its transformation (wire name: CLONE).
//!
//! Two entry paths, each a pure two-step interface around one store lookup:
//!
//! * **Submit fallback** (`SubmitEnrichedSQL`): no reuse candidate qualified,
//!   but an equivalent model (same namespace + schema-abstracted logic) is
//!   already built under another physical name. [`submit_lookup`] says whether
//!   to look for one; [`plan_submit`] turns the equivalent found into a plan.
//!   Only the execution types the Warehouse rules admit are copied (FULL, and
//!   DuckDB's checked subset); any other submit builds and reports the
//!   equivalent in its rejection codes, as hosted 2.0.11 does.
//! * **Client-requested copy** (`RegisterClone`): the client names the source.
//!   [`register_lookup`] rejects ineligible requests or says what to look up;
//!   [`plan_register`] turns the lookup result into a plan.
//!
//! The Submit decision module (submit fallback) and the gRPC adapter
//! (client-requested copy) perform the lookup and write the pending row.
//! Everything that decides the outcome is here; warehouse-specific questions
//! (DDL, relation checks, copy-chain limits, source evidence, the incremental
//! contract) are answered by the [`Warehouse`] rules, never by testing the
//! dialect string here.

use crate::execution::ExecutionEvidence;
use crate::fingerprint::RehomedLogic;
use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
use crate::store::{CopyCandidate, InputTable};
use crate::warehouse::{IncrementalCopyContract, Warehouse};

// ── Submit fallback ─────────────────────────────────────────────────────────

/// What to look up for the submit fallback: a current equivalent of this
/// logic in this namespace under any other physical target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EquivalentLookup<'a> {
    pub table_namespace: &'a str,
    pub node_namespace_logic_hash: &'a str,
    /// The submit's logic read in each other schema it names, with its Target
    /// relation's schema read as that schema
    /// (`fingerprint::rehomed_copy_logic_hashes`, #107). Each admits only a
    /// candidate whose Target relation is in its schema and, when the submit
    /// may be copied, that read what the submit so read reads
    /// ([`Self::admits`]).
    pub rehomed: Vec<RehomedReading>,
    /// The equivalent's execution type: the submit's own for a submit the
    /// Warehouse rules let copy; `None` (any) for one they do not, which
    /// builds with the codes of whichever equivalent hosted would have found
    /// ([`plan_submit`]).
    pub execution_type: Option<i32>,
    pub exclude_target: &'a str,
}

/// The submit read in another schema ([`RehomedLogic`]), with the upstream
/// relations it reads so read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RehomedReading {
    pub logic: RehomedLogic,
    /// The submit's recorded upstream reads with its Target relation's schema
    /// read as `logic.schema` ([`canonical_reads`]). `None` when they do not
    /// read a relation in each schema the reading names: they cannot then
    /// show that a build resolved its references in those schemas.
    pub reads: Option<Vec<Vec<String>>>,
}

impl RehomedReading {
    /// `logic`, read by a submit that recorded `input_tables` and builds
    /// `own_target`, whose schema `from` is read as `logic.schema`.
    pub(crate) fn new(
        logic: RehomedLogic,
        input_tables: &[InputTable],
        own_target: &str,
        from: &str,
    ) -> Self {
        let from = crate::sql_norm::canonical_schema_key(from);
        let reads = canonical_reads(input_tables, own_target, Some((&from, &logic.schema)));
        let schemas: std::collections::BTreeSet<&str> = reads
            .iter()
            .filter(|name| name.len() == 3)
            .map(|name| name[1].as_str())
            .collect();
        let covered = logic.named.iter().all(|s| schemas.contains(s.as_str()));
        Self {
            reads: covered.then_some(reads),
            logic,
        }
    }
}

/// `input_tables` other than `own_target`, each as its canonical name
/// components (`sql_norm::canonical_identifier`), sorted and deduplicated;
/// with `rehome = Some((from, to))`, a three-part name in canonical schema
/// `from` is read in `to`.
fn canonical_reads(
    input_tables: &[InputTable],
    own_target: &str,
    rehome: Option<(&str, &str)>,
) -> Vec<Vec<String>> {
    use crate::sql_norm::{canonical_identifier, split_object_name_components};
    let canonical = |name: &str| -> Vec<String> {
        split_object_name_components(name)
            .iter()
            .map(|c| canonical_identifier(c))
            .collect()
    };
    let own = canonical(own_target);
    let mut reads: Vec<Vec<String>> = input_tables
        .iter()
        .map(|t| canonical(&t.name))
        .filter(|name| *name != own)
        .map(|mut name| {
            if let (Some((from, to)), 3) = (rehome, name.len()) {
                if name[1] == from {
                    name[1] = to.to_owned();
                }
            }
            name
        })
        .collect();
    reads.sort();
    reads.dedup();
    reads
}

impl EquivalentLookup<'_> {
    /// Every logic identity a candidate may have. A reading that could admit
    /// no candidate ([`Self::admits`]) is not looked up.
    pub(crate) fn logic_hashes(&self) -> Vec<&str> {
        std::iter::once(self.node_namespace_logic_hash)
            .chain(
                self.rehomed
                    .iter()
                    .filter(|r| self.reading_may_admit(r))
                    .map(|r| r.logic.hash.as_str()),
            )
            .collect()
    }

    /// Whether a lookup that may copy has the reads to verify `reading`.
    fn reading_may_admit(&self, reading: &RehomedReading) -> bool {
        self.execution_type.is_none() || reading.reads.is_some()
    }

    /// Whether `candidate` is an equivalent: it has the submit's logic, or
    /// the submit's logic read in the schema of the candidate's Target
    /// relation. The submit so read is the candidate's build request with one
    /// schema substituted, each dependency name still bound to its definition
    /// (AGENTS.md invariant 4). A rehomed identity matched by a candidate in
    /// another schema is not one: that candidate read other relations.
    ///
    /// The identity abstracts the schemas a build resolved its references
    /// in, and a Target relation's schema does not show them: a build into
    /// the prod schema may have resolved them in another one. So when the
    /// submit may be copied, the candidate must also have recorded exactly
    /// the upstream reads of the submit so read, which name each schema that
    /// reading names (PR #127 review). A lookup that only reports codes
    /// builds either way and admits on the Target relation's schema.
    pub(crate) fn admits(&self, candidate: &CopyCandidate) -> bool {
        use crate::sql_norm::{canonical_schema_key, schema_of_object_name};
        let Some(hash) = candidate.node_namespace_logic_hash.as_deref() else {
            return false;
        };
        if hash == self.node_namespace_logic_hash {
            return true;
        }
        let Some(schema) = schema_of_object_name(&candidate.target_table) else {
            return false;
        };
        let schema = canonical_schema_key(&schema);
        self.rehomed.iter().any(|r| {
            r.logic.hash == hash
                && r.logic.schema == schema
                && (self.execution_type.is_none()
                    || r.reads.as_ref().is_some_and(|reads| {
                        *reads
                            == canonical_reads(
                                &candidate.input_tables,
                                &candidate.target_table,
                                None,
                            )
                    }))
        })
    }
}

/// Whether the submit fallback applies, and if so what to look up.
///
/// Gated to real-SQL submits with a known namespace logic identity and no
/// reuse candidate, excluding seeds and data tests. VERIFIED LIVE
/// (experiments/clone_experiment.sh table materialization;
/// golden/fixtures/clone_submit_equivalent_fallback.jsonl).
///
/// Only the execution types [`Warehouse::submit_copy_admits`] are copied; the
/// others still look for an equivalent, of any execution type, because
/// hosted reports it in the rejection codes of the build (see
/// [`SubmitCopyPlan::Refused`]).
///
/// Volatility: when volatile evaluation is requested
/// (`tolerate_nondeterminism = false`) AND the root SQL or any supplied
/// dependency definition genuinely contains a volatile function, a copy would
/// serve a different evaluated value, so hosted EXECUTES (srr=6)
/// (experiments/clone_eligibility.sh volatile,
/// golden/fixtures/clone_fallback_volatile.jsonl). A deterministic model with
/// the same flag still copies (experiments/clone_eligibility.sh volatile_det,
/// review #2 4th follow-up P1 #3). The parse-based detector
/// (`sql_norm::request_has_volatile_sql`) is a conservative suppression gate,
/// not full volatile-evaluation conformance (golden/replay_gaps.json).
pub(crate) fn submit_lookup<'a>(
    req: &'a qc::SubmitEnrichedSqlRequest,
    evidence: &'a ExecutionEvidence,
    has_reuse_candidate: bool,
) -> Option<EquivalentLookup<'a>> {
    let ns_logic_hash = evidence.node_namespace_logic_hash.as_deref();
    let warehouse = Warehouse::from_dialect(&req.dialect);
    let volatile_blocks_copy = !(warehouse.copy_identity_skips_volatility_gate()
        && ns_logic_hash.is_some())
        && !req.tolerate_nondeterminism
        && warehouse.has_volatile_sql(
            &req.sql,
            req.query_dependencies.iter().map(|d| d.query.as_str()),
        );
    if has_reuse_candidate
        || volatile_blocks_copy
        || req.execution_type == ModelExecutionType::DbtDataTest as i32
        || req.execution_type == ModelExecutionType::Values as i32
    {
        return None;
    }
    let copies = warehouse.submit_copy_admits(req.execution_type, &req.semantic_extras);
    let exclude_target = req.target_table.as_deref().unwrap_or_default();
    let table_namespace = crate::execution::non_blank_namespace(req.table_namespace.as_deref())?;
    let node_namespace_logic_hash = ns_logic_hash?;
    let rehomed = crate::sql_norm::schema_of_object_name(exclude_target)
        .map(|from| {
            crate::fingerprint::rehomed_copy_logic_hashes(
                &crate::execution::sql_execution(req),
                &from,
            )
            .into_iter()
            .map(|logic| RehomedReading::new(logic, &evidence.input_tables, exclude_target, &from))
            .collect()
        })
        .unwrap_or_default();
    Some(EquivalentLookup {
        table_namespace,
        node_namespace_logic_hash,
        rehomed,
        execution_type: copies.then_some(req.execution_type),
        exclude_target,
    })
}

/// A current equivalent found for the submit fallback, ready to plan.
#[derive(Debug, Clone)]
pub(crate) struct CloneEquivalent {
    pub source_table: String,
    pub execution_type: i32,
    pub table_type: Option<String>,
    pub last_modified_epoch: Option<i64>,
    pub execution_runtime_ms: Option<i64>,
    /// Copy ancestors behind the source (0: the source is a build). Only
    /// walked where [`Warehouse::copy_provenance_depth`] requires it.
    pub copy_depth: usize,
}

impl CloneEquivalent {
    pub(crate) fn new(candidate: CopyCandidate, copy_depth: usize) -> Self {
        Self {
            source_table: candidate.target_table,
            execution_type: candidate.execution_type,
            table_type: candidate.table_type,
            last_modified_epoch: candidate.last_modified_epoch,
            execution_runtime_ms: candidate.execution_runtime_ms,
            copy_depth,
        }
    }
}

/// A Result copy of an equivalent into the submit target: the source, the
/// statements that copy it, and the source build evidence the `ready_to_clone`
/// response echoes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultCopy {
    pub source_table: String,
    pub clone_sqls: Vec<String>,
    pub source_last_modified_epoch: Option<i64>,
    pub source_execution_runtime_ms: Option<i64>,
}

/// The submit fallback's outcome once an equivalent exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubmitCopyPlan {
    /// Copy the equivalent into the submit target (`ready_to_clone`).
    Copy(ResultCopy),
    /// An equivalent would be copied, but the target forbids copies with
    /// `clone_chain_depth_limit = 0` alone, or the copy chain is already at
    /// its limit. Hosted executes with TARGET_TABLE_MISMATCH /
    /// CLONE_CHAIN_LIMIT_EXCEEDED and a fixed description (VERIFIED LIVE,
    /// client 2.0.6, experiments/clone_experiment.sh table --no-clones;
    /// fixture clone_submit_allow_clones_false.jsonl).
    ChainLimitExecute,
    /// An equivalent would be copied, but the submit disallows copies
    /// (`allow_clones: false`). Hosted executes with the codes of
    /// [`SubmitCopyPlan::ChainLimitExecute`] and `cloning is disabled`
    /// (VERIFIED LIVE 2026-11-11, client 2.0.11, which sends it with
    /// `clone_chain_depth_limit = 0`: explain_copy_logic_mismatch.jsonl `cf4`).
    CopiesDisabled,
    /// An equivalent exists, but the Warehouse rules do not copy this
    /// submit's execution type ([`Warehouse::submit_copy_admits`]). Hosted
    /// 2.0.11 builds it with its ordinary description and tree, and reports
    /// TARGET_TABLE_MISMATCH with this clone rejection (VERIFIED LIVE, see
    /// [`refused_copy_rejection`]).
    Refused { clone_rejection_reason: i32 },
    /// No copy can be produced for this equivalent. The submit falls back to
    /// the ordinary decision.
    Unavailable { reason: &'static str },
}

/// The clone rejection hosted 2.0.11 reports when it builds a submit it does
/// not copy although `equivalent_type`'s equivalent exists under another
/// name. VERIFIED LIVE: EXECUTION_TYPE_NOT_FULL for a view or snapshot
/// (explain_copy_probes.jsonl entries 13-14) and a custom node
/// (explain_custom_copy.jsonl entry 9); for an incremental,
/// NO_SUITABLE_MATCH_FOUND when the newest equivalent has the submit's
/// execution type and EXECUTION_TYPE_MISMATCH when it has another
/// (explain_incremental_copy_over.jsonl entries 37-41: the append and
/// delete+insert submits' newest equivalents were the insert_overwrite and
/// merge prod builds with the same SQL and extras). A view, snapshot or
/// custom node whose equivalent has another execution type is not captured;
/// it is given the incremental rule's EXECUTION_TYPE_MISMATCH.
fn refused_copy_rejection(submit_type: i32, equivalent_type: i32) -> i32 {
    use crate::decision::{
        REJECTION_EXECUTION_TYPE_MISMATCH, REJECTION_EXECUTION_TYPE_NOT_FULL,
        REJECTION_NO_SUITABLE_MATCH_FOUND,
    };
    let incremental = matches!(
        ModelExecutionType::try_from(submit_type),
        Ok(ModelExecutionType::Append
            | ModelExecutionType::Merge
            | ModelExecutionType::InsertOverwrite
            | ModelExecutionType::DeleteInsert
            | ModelExecutionType::Microbatch)
    );
    if equivalent_type != submit_type {
        REJECTION_EXECUTION_TYPE_MISMATCH
    } else if incremental {
        REJECTION_NO_SUITABLE_MATCH_FOUND
    } else {
        REJECTION_EXECUTION_TYPE_NOT_FULL
    }
}

/// Plan the submit fallback for an equivalent found by [`submit_lookup`].
///
/// A submit whose execution type the Warehouse rules do not copy is refused
/// first: it is never copied, whatever the target allows.
///
/// The forbidden/chain-limit check runs only once an equivalent EXISTS:
/// without one, hosted answers the ordinary execute, not the chain-limit
/// shape. The client encodes a copy-forbidding target as
/// `clone_chain_depth_limit = 0` on the submit, not via `allow_clones`
/// (VERIFIED LIVE, experiments/clone_experiment.sh table --no-clones).
pub(crate) fn plan_submit(
    req: &qc::SubmitEnrichedSqlRequest,
    equivalent: CloneEquivalent,
) -> SubmitCopyPlan {
    let warehouse = Warehouse::from_dialect(&req.dialect);
    if !warehouse.submit_copy_admits(req.execution_type, &req.semantic_extras) {
        return SubmitCopyPlan::Refused {
            clone_rejection_reason: refused_copy_rejection(
                req.execution_type,
                equivalent.execution_type,
            ),
        };
    }
    if req.allow_clones == Some(false) {
        return SubmitCopyPlan::CopiesDisabled;
    }
    let forbidden = req.clone_chain_depth_limit == Some(0);
    let chain_limit_reached = warehouse
        .copy_chain_depth_limit(req.clone_chain_depth_limit)
        .is_some_and(|limit| equivalent.copy_depth >= limit);
    if forbidden || chain_limit_reached {
        return SubmitCopyPlan::ChainLimitExecute;
    }
    let ddl = warehouse
        .result_copy_ddl(
            &equivalent.source_table,
            req.target_table.as_deref().unwrap_or_default(),
            equivalent.table_type.as_deref(),
            req.clone_table_properties.as_ref(),
        )
        .and_then(|sqls| {
            warehouse
                .copy_source_evidence(equivalent.last_modified_epoch)
                .map(|()| sqls)
        });
    match ddl {
        Ok(clone_sqls) => SubmitCopyPlan::Copy(ResultCopy {
            source_table: equivalent.source_table,
            clone_sqls,
            source_last_modified_epoch: equivalent.last_modified_epoch,
            source_execution_runtime_ms: equivalent.execution_runtime_ms,
        }),
        Err(reason) => SubmitCopyPlan::Unavailable { reason },
    }
}

// ── Client-requested copy ───────────────────────────────────────────────────

/// Why a client-requested copy is refused (`unable_to_clone`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisterRejection {
    /// The target forbids copies (`clone_chain_depth_limit = 0`).
    ChainLimitExceeded,
    /// The request cannot be proven eligible (DuckDB contract or source
    /// evidence missing).
    ForcedNotEligible,
}

/// Why a client-requested copy will not be registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisterRefusal {
    /// Answered with `unable_to_clone`; the client builds normally.
    Reject(RegisterRejection),
    /// No DDL exists for this copy (`Status::unimplemented`).
    Unsupported(&'static str),
}

/// What a client-requested copy needs from history before it can be planned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegisterLookup<'a> {
    /// DuckDB: the source must be the current, confirmed incremental build at
    /// exactly this epoch and initialization identity.
    DuckDbIncrementalSource {
        source: &'a str,
        table_namespace: &'a str,
        last_modified_epoch: i64,
        identity: &'a str,
    },
    /// Snowflake: the source's logic identity, inherited by the copy so a
    /// later submit for the target can match it.
    SourceLogic {
        source: &'a str,
        table_namespace: Option<&'a str>,
    },
    /// Other warehouses inherit no logic identity.
    Nothing,
}

/// The lookup result matching the [`RegisterLookup`] that was requested.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RegisterHistory {
    /// `None`: no eligible source. `Some(logic)`: eligible, carrying the
    /// source's logic identity (itself optional).
    DuckDbIncrementalSource(Option<Option<String>>),
    SourceLogic(Option<String>),
    Nothing,
}

/// A client-requested copy that will be registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegisterPlan {
    pub clone_sqls: Vec<String>,
    /// The source's logic identity, stamped on the copy's pending row. Without
    /// it a later submit for the target falls back to EXECUTE (safe: no skip
    /// without proven logic equivalence).
    pub inherited_ns_logic: Option<String>,
    /// DuckDB only: the source epoch the client must still observe.
    pub required_source_epoch: Option<i64>,
}

/// Refuse what can be refused before any history read, or say what to read.
///
/// `clone_chain_depth_limit = 0` encodes a target that disallows copies.
/// VERIFIED LIVE: hosted answers `unable_to_clone` with
/// CLONE_CHAIN_LIMIT_EXCEEDED, writes no pending history and the client builds
/// normally.
///
/// Unlike Snowflake, DuckDB accepts this pre-compilation path only for the
/// pinned incremental contract and a physically current source. The later SQL
/// submit must still match the inherited logic/config hash. DuckDB checks its
/// source before DDL (an ineligible source is refused, not unsupported);
/// every other warehouse checks DDL before reading history.
pub(crate) fn register_lookup(
    req: &qc::CloneRequest,
) -> Result<RegisterLookup<'_>, RegisterRefusal> {
    if req.clone_chain_depth_limit == Some(0) {
        return Err(RegisterRefusal::Reject(
            RegisterRejection::ChainLimitExceeded,
        ));
    }
    let warehouse = Warehouse::from_dialect(&req.dialect);
    match warehouse.incremental_copy_contract(req) {
        IncrementalCopyContract::NotRequired => {}
        IncrementalCopyContract::Missing => {
            return Err(RegisterRefusal::Reject(
                RegisterRejection::ForcedNotEligible,
            ))
        }
        IncrementalCopyContract::Declared { identity } => {
            return match (
                crate::execution::non_blank_namespace(req.table_namespace.as_deref()),
                req.clone_source_last_modified_epoch,
            ) {
                (Some(ns), Some(epoch)) => Ok(RegisterLookup::DuckDbIncrementalSource {
                    source: &req.clone_source_table,
                    table_namespace: ns,
                    last_modified_epoch: epoch,
                    identity,
                }),
                _ => Err(RegisterRefusal::Reject(
                    RegisterRejection::ForcedNotEligible,
                )),
            };
        }
    }
    register_ddl(req)?;
    if warehouse.supports_namespace_reuse() {
        return Ok(RegisterLookup::SourceLogic {
            source: &req.clone_source_table,
            table_namespace: crate::execution::non_blank_namespace(req.table_namespace.as_deref()),
        });
    }
    Ok(RegisterLookup::Nothing)
}

/// Plan a client-requested copy from the history its lookup returned.
pub(crate) fn plan_register(
    req: &qc::CloneRequest,
    history: RegisterHistory,
) -> Result<RegisterPlan, RegisterRefusal> {
    let (inherited_ns_logic, required_source_epoch) = match history {
        RegisterHistory::DuckDbIncrementalSource(None) => {
            return Err(RegisterRefusal::Reject(
                RegisterRejection::ForcedNotEligible,
            ))
        }
        RegisterHistory::DuckDbIncrementalSource(Some(logic)) => {
            (logic, req.clone_source_last_modified_epoch)
        }
        RegisterHistory::SourceLogic(logic) => (logic, None),
        RegisterHistory::Nothing => (None, None),
    };
    Ok(RegisterPlan {
        clone_sqls: register_ddl(req)?,
        inherited_ns_logic,
        required_source_epoch,
    })
}

fn register_ddl(req: &qc::CloneRequest) -> Result<Vec<String>, RegisterRefusal> {
    Warehouse::from_dialect(&req.dialect)
        .result_copy_ddl(
            &req.clone_source_table,
            &req.target_table,
            req.clone_source_table_type.as_deref(),
            req.table_properties.as_ref(),
        )
        .map_err(RegisterRefusal::Unsupported)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEV: &str = "\"DB\".\"DEV\".\"M\"";
    const PROD: &str = "\"DB\".\"PROD\".\"M\"";
    const DUCK_DEV: &str = "\"local\".\"dev\".\"m\"";
    const DUCK_PROD: &str = "\"local\".\"prod\".\"m\"";

    fn submit(dialect: &str, sql: &str) -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            execution_type: 1,
            dialect: dialect.into(),
            sql: sql.into(),
            target_table: Some(DEV.into()),
            table_namespace: Some("ns".into()),
            tolerate_nondeterminism: true,
            ..Default::default()
        }
    }

    fn evidence(req: &qc::SubmitEnrichedSqlRequest) -> ExecutionEvidence {
        let mut ev = ExecutionEvidence::sql_submit("o", req);
        // Fix the logic identity so DuckDB cases do not depend on its parser.
        ev.node_namespace_logic_hash = Some("logic".into());
        ev
    }

    fn equivalent(source: &str, depth: usize, epoch: Option<i64>) -> CloneEquivalent {
        CloneEquivalent {
            source_table: source.into(),
            execution_type: 1,
            table_type: Some("TABLE".into()),
            last_modified_epoch: epoch,
            execution_runtime_ms: Some(9),
            copy_depth: depth,
        }
    }

    fn duck_submit(limit: Option<i64>) -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            target_table: Some(DUCK_DEV.into()),
            clone_chain_depth_limit: limit,
            ..submit("duckdb", "select 1 as id")
        }
    }

    fn duck_equivalent(depth: usize, epoch: Option<i64>) -> CloneEquivalent {
        CloneEquivalent {
            table_type: Some("DUCKDB CTAS TABLE".into()),
            ..equivalent(DUCK_PROD, depth, epoch)
        }
    }

    // ---- submit_lookup ------------------------------------------------------

    #[test]
    fn lookup_targets_other_names_with_the_same_logic_in_the_namespace() {
        let req = submit("snowflake", "select id from DB.DEV.UP");
        let ev = evidence(&req);
        assert_eq!(
            submit_lookup(&req, &ev, false),
            Some(EquivalentLookup {
                table_namespace: "ns",
                node_namespace_logic_hash: "logic",
                rehomed: vec![],
                execution_type: Some(1),
                exclude_target: DEV,
            })
        );
    }

    /// A submit that reads a fixed schema other than its Target relation's
    /// also looks for its logic read in that schema (#107,
    /// explain_post_copy_identity.jsonl entries 18-19), with the upstream
    /// reads of the submit so read.
    #[test]
    fn lookup_reads_the_submit_in_each_other_schema_it_names() {
        let req = qc::SubmitEnrichedSqlRequest {
            default_schema: Some("DEV".into()),
            tables: vec![
                table("\"DB\".\"PROD\".\"UP\""),
                table("DB.dev.v"),
                table(DEV),
            ],
            ..submit("snowflake", "select id from DB.PROD.UP")
        };
        let ev = evidence(&req);
        let lookup = submit_lookup(&req, &ev, false).expect("lookup");
        let prod = qc::SqlExecution {
            target_table: Some(PROD.into()),
            default_schema: Some("PROD".into()),
            ..crate::execution::sql_execution(&req)
        };
        let reads = |names: &[[&str; 3]]| -> Vec<Vec<String>> {
            names
                .iter()
                .map(|n| n.iter().map(|c| c.to_string()).collect())
                .collect()
        };
        assert_eq!(
            lookup.rehomed,
            vec![RehomedReading {
                logic: RehomedLogic {
                    schema: "PROD".into(),
                    hash: crate::fingerprint::namespace_logic_hash(&prod),
                    named: vec!["PROD".into()],
                },
                reads: Some(reads(&[["DB", "PROD", "UP"], ["DB", "PROD", "V"]])),
            }]
        );
        assert_eq!(
            lookup.logic_hashes(),
            vec!["logic", lookup.rehomed[0].logic.hash.as_str()]
        );
    }

    /// Reads that do not read a relation in each schema the reading names
    /// cannot verify it: a lookup that may copy does not look it up, and one
    /// that only reports codes does.
    #[test]
    fn reads_that_name_too_few_schemas_verify_no_reading() {
        let req = qc::SubmitEnrichedSqlRequest {
            default_schema: Some("DEV".into()),
            tables: vec![table(DEV)],
            ..submit("snowflake", "select id from DB.PROD.UP")
        };
        let ev = evidence(&req);
        let copying = submit_lookup(&req, &ev, false).expect("lookup");
        assert_eq!(copying.rehomed.len(), 1);
        assert_eq!(copying.rehomed[0].reads, None);
        assert_eq!(copying.logic_hashes(), vec!["logic"]);
        let codes_only = EquivalentLookup {
            execution_type: None,
            ..copying.clone()
        };
        assert_eq!(codes_only.logic_hashes().len(), 2);
    }

    fn table(name: &str) -> qc::TableModifiedInfo {
        qc::TableModifiedInfo {
            name: name.into(),
            ..Default::default()
        }
    }

    fn candidate(target: &str, logic: Option<&str>) -> CopyCandidate {
        CopyCandidate {
            target_table: target.into(),
            execution_type: 1,
            table_type: None,
            last_modified_epoch: None,
            execution_runtime_ms: None,
            node_namespace_logic_hash: logic.map(Into::into),
            input_tables: vec![],
        }
    }

    fn reading_in_prod(reads: Option<Vec<Vec<String>>>) -> RehomedReading {
        RehomedReading {
            logic: RehomedLogic {
                schema: "PROD".into(),
                hash: "in-prod".into(),
                named: vec!["PROD".into()],
            },
            reads,
        }
    }

    const PROD_UP: &str = "\"DB\".\"PROD\".\"UP\"";

    fn read(name: &str) -> InputTable {
        InputTable::at(name, Some(1))
    }

    /// The submit's own logic admits a candidate in any schema; the logic
    /// read in a schema admits only a candidate whose Target relation is in
    /// that schema (spelled either way). One elsewhere read other relations.
    #[test]
    fn a_rehomed_identity_admits_only_a_candidate_in_its_schema() {
        let lookup = EquivalentLookup {
            table_namespace: "ns",
            node_namespace_logic_hash: "own",
            rehomed: vec![reading_in_prod(Some(canonical_reads(
                &[read(PROD_UP)],
                DEV,
                None,
            )))],
            execution_type: Some(1),
            exclude_target: DEV,
        };
        const CI: &str = "\"DB\".\"CI\".\"M\"";
        let read_up = |target: &str, logic: Option<&str>| CopyCandidate {
            input_tables: vec![read(PROD_UP), read(target)],
            ..candidate(target, logic)
        };
        assert!(lookup.admits(&candidate(CI, Some("own"))));
        assert!(lookup.admits(&read_up(PROD, Some("in-prod"))));
        assert!(lookup.admits(&read_up("DB.prod.M", Some("in-prod"))));
        assert!(!lookup.admits(&read_up(CI, Some("in-prod"))), "elsewhere");
        assert!(
            !lookup.admits(&read_up("\"DB\".\"prod\".\"M\"", Some("in-prod"))),
            "a quoted lowercase schema is another schema"
        );
        assert!(!lookup.admits(&read_up(PROD, Some("other"))));
        assert!(!lookup.admits(&read_up(PROD, None)));
    }

    /// PR #127 review: a candidate in the reading's schema that resolved its
    /// references elsewhere (its build read `CI.UP`) has the reading's logic
    /// but read other relations, and a submit that may be copied does not
    /// admit it; nor one that recorded no reads, nor any candidate when the
    /// submit's reads cannot verify the reading. A lookup that only reports
    /// codes admits on the Target relation's schema.
    #[test]
    fn a_copying_rehomed_identity_admits_only_a_candidate_that_read_the_reading() {
        let reads = canonical_reads(&[read(PROD_UP)], DEV, None);
        let lookup = EquivalentLookup {
            table_namespace: "ns",
            node_namespace_logic_hash: "own",
            rehomed: vec![reading_in_prod(Some(reads))],
            execution_type: Some(1),
            exclude_target: DEV,
        };
        let reading = |names: &[&str]| CopyCandidate {
            input_tables: names.iter().map(|n| read(n)).collect(),
            ..candidate(PROD, Some("in-prod"))
        };
        assert!(
            lookup.admits(&reading(&["DB.prod.up"])),
            "spelled either way"
        );
        assert!(
            !lookup.admits(&reading(&["\"DB\".\"CI\".\"UP\""])),
            "read ci"
        );
        assert!(!lookup.admits(&reading(&[])), "recorded no reads");
        assert!(
            !lookup.admits(&reading(&[PROD_UP, "\"DB\".\"PROD\".\"OTHER\""])),
            "read more"
        );
        let unverified = EquivalentLookup {
            rehomed: vec![reading_in_prod(None)],
            ..lookup.clone()
        };
        assert!(!unverified.admits(&reading(&[PROD_UP])));
        let codes_only = EquivalentLookup {
            execution_type: None,
            ..unverified
        };
        assert!(codes_only.admits(&reading(&["\"DB\".\"CI\".\"UP\""])));
    }

    /// The submit's reads in its Target relation's schema are read in the
    /// reading's schema; its own Target relation is not a read.
    #[test]
    fn a_reading_reads_the_submits_own_schema_relations_in_its_schema() {
        let logic = RehomedLogic {
            schema: "PROD".into(),
            hash: "in-prod".into(),
            named: vec!["PROD".into(), "RAW".into()],
        };
        let tables = [read("db.dev.v"), read("\"DB\".\"RAW\".\"R\""), read(DEV)];
        let reading = RehomedReading::new(logic.clone(), &tables, DEV, "\"DEV\"");
        let expected: Vec<Vec<String>> = vec![
            vec!["DB".into(), "PROD".into(), "V".into()],
            vec!["DB".into(), "RAW".into(), "R".into()],
        ];
        assert_eq!(reading.reads, Some(expected));
        let uncovered = RehomedReading::new(logic, &tables[..1], DEV, "DEV");
        assert_eq!(uncovered.reads, None, "no read in RAW");
    }

    #[test]
    fn lookup_is_skipped_when_the_fallback_cannot_apply() {
        let base = submit("snowflake", "select id from DB.DEV.UP");
        let ev = evidence(&base);
        assert_eq!(
            submit_lookup(&base, &ev, true),
            None,
            "a reuse candidate exists"
        );

        type Mutation = fn(&mut qc::SubmitEnrichedSqlRequest);
        let cases: Vec<(&str, Mutation)> = vec![
            ("data test", |r| r.execution_type = 8),
            ("seed", |r| r.execution_type = 9),
            ("no namespace", |r| r.table_namespace = None),
            ("empty namespace", |r| {
                r.table_namespace = Some(String::new())
            }),
        ];
        for (why, mutate) in cases {
            let mut req = base.clone();
            mutate(&mut req);
            assert_eq!(submit_lookup(&req, &ev, false), None, "{why}");
        }

        let mut no_logic = ev.clone();
        no_logic.node_namespace_logic_hash = None;
        assert_eq!(
            submit_lookup(&base, &no_logic, false),
            None,
            "no logic identity"
        );
    }

    #[test]
    fn volatile_sql_blocks_the_lookup_only_when_evaluation_is_requested() {
        let mut volatile = submit("snowflake", "select current_timestamp() as ts");
        let ev = evidence(&volatile);
        assert!(
            submit_lookup(&volatile, &ev, false).is_some(),
            "volatility tolerated"
        );
        volatile.tolerate_nondeterminism = false;
        assert!(
            submit_lookup(&volatile, &ev, false).is_none(),
            "volatile evaluation"
        );

        let mut deterministic = submit("snowflake", "select id from DB.DEV.UP");
        deterministic.tolerate_nondeterminism = false;
        let ev = evidence(&deterministic);
        assert!(
            submit_lookup(&deterministic, &ev, false).is_some(),
            "deterministic"
        );

        let mut dependency = submit("snowflake", "select id from DB.DEV.V");
        dependency.tolerate_nondeterminism = false;
        dependency.query_dependencies = vec![qc::QueryDependency {
            name: "DB.DEV.V".into(),
            query: "select current_timestamp() as id".into(),
            ..Default::default()
        }];
        let ev = evidence(&dependency);
        assert!(
            submit_lookup(&dependency, &ev, false).is_none(),
            "volatile dependency"
        );
    }

    #[test]
    fn duckdb_with_a_logic_identity_bypasses_the_volatility_gate() {
        let mut req = duck_submit(None);
        req.tolerate_nondeterminism = false;
        let ev = evidence(&req);
        assert!(submit_lookup(&req, &ev, false).is_some());
        let mut no_logic = ev.clone();
        no_logic.node_namespace_logic_hash = None;
        assert!(submit_lookup(&req, &no_logic, false).is_none());
    }

    #[test]
    fn only_a_full_submit_looks_for_an_equivalent_of_its_own_type() {
        // A copy-eligible submit looks for an equivalent of its own execution
        // type; any other kind looks for the equivalent hosted reports, of any
        // execution type (explain_incremental_copy_over.jsonl).
        for (execution_type, wanted) in [
            (1, Some(1)),
            (2, None),
            (3, None),
            (4, None),
            (5, None),
            (6, None),
            (7, None),
            (10, None),
            (11, None),
        ] {
            let mut req = submit("snowflake", "select id from DB.DEV.UP");
            req.execution_type = execution_type;
            let ev = evidence(&req);
            assert_eq!(
                submit_lookup(&req, &ev, false).map(|l| l.execution_type),
                Some(wanted),
                "execution type {execution_type}"
            );
        }
    }

    // ---- plan_submit --------------------------------------------------------

    fn typed_equivalent(execution_type: i32) -> CloneEquivalent {
        CloneEquivalent {
            execution_type,
            ..equivalent(PROD, 0, Some(5))
        }
    }

    #[test]
    fn non_full_submits_build_with_the_hosted_rejection_codes() {
        // (submit type, equivalent type, clone rejection). VERIFIED LIVE,
        // client 2.0.11: view 10 and snapshot 7 (explain_copy_probes.jsonl
        // entries 13-14) and custom 11 (explain_custom_copy.jsonl entry 9) get
        // EXECUTION_TYPE_NOT_FULL; an incremental whose equivalent has its own
        // type gets NO_SUITABLE_MATCH_FOUND, and one whose equivalent has
        // another type EXECUTION_TYPE_MISMATCH
        // (explain_incremental_copy_over.jsonl entries 37-41).
        let cases = [
            (10, 10, 3),
            (7, 7, 3),
            (11, 11, 3),
            (3, 3, 6),
            (4, 4, 6),
            (6, 6, 6),
            (2, 4, 2),
            (5, 3, 2),
        ];
        for (submit_type, equivalent_type, clone_rejection_reason) in cases {
            let mut req = submit("snowflake", "select id from DB.DEV.UP");
            req.execution_type = submit_type;
            assert_eq!(
                plan_submit(&req, typed_equivalent(equivalent_type)),
                SubmitCopyPlan::Refused {
                    clone_rejection_reason
                },
                "submit {submit_type}, equivalent {equivalent_type}"
            );
        }
    }

    #[test]
    fn duckdb_keeps_its_checked_delete_insert_copy() {
        let mut req = duck_submit(None);
        req.execution_type = ModelExecutionType::DeleteInsert as i32;
        req.semantic_extras.insert(
            "duckdb_incremental_contract".into(),
            "delete-insert-fixed-v1".into(),
        );
        let ev = evidence(&req);
        assert_eq!(
            submit_lookup(&req, &ev, false).map(|l| l.execution_type),
            Some(Some(ModelExecutionType::DeleteInsert as i32))
        );
        let equivalent = CloneEquivalent {
            execution_type: req.execution_type,
            ..duck_equivalent(0, Some(5))
        };
        assert!(matches!(
            plan_submit(&req, equivalent),
            SubmitCopyPlan::Copy(_)
        ));
    }

    #[test]
    fn snowflake_equivalent_is_copied_into_the_submit_target() {
        let req = submit("snowflake", "select id from DB.DEV.UP");
        let SubmitCopyPlan::Copy(ResultCopy {
            source_table,
            clone_sqls,
            source_last_modified_epoch,
            source_execution_runtime_ms,
        }) = plan_submit(&req, equivalent(PROD, 0, Some(5)))
        else {
            panic!("expected a copy");
        };
        assert_eq!(source_table, PROD);
        assert_eq!(clone_sqls.len(), 1);
        assert!(clone_sqls[0].contains(&format!("CREATE OR REPLACE TABLE {DEV}")));
        assert!(clone_sqls[0].contains(&format!("CLONE {PROD}")));
        assert_eq!(source_last_modified_epoch, Some(5));
        assert_eq!(source_execution_runtime_ms, Some(9));
    }

    #[test]
    fn copy_forbidding_target_executes_with_the_chain_limit_shape() {
        let mut req = submit("snowflake", "select id from DB.DEV.UP");
        req.clone_chain_depth_limit = Some(0);
        assert_eq!(
            plan_submit(&req, equivalent(PROD, 0, Some(5))),
            SubmitCopyPlan::ChainLimitExecute
        );
    }

    /// `allow_clones: false` (client 2.0.11 sends it with limit 0): the same
    /// codes, another description (VERIFIED LIVE 2026-11-11,
    /// explain_copy_logic_mismatch.jsonl `cf4`).
    #[test]
    fn disallowed_copies_execute_with_the_copy_disabled_shape() {
        for limit in [None, Some(0)] {
            let mut req = submit("snowflake", "select id from DB.DEV.UP");
            req.allow_clones = Some(false);
            req.clone_chain_depth_limit = limit;
            assert_eq!(
                plan_submit(&req, equivalent(PROD, 0, Some(5))),
                SubmitCopyPlan::CopiesDisabled,
                "limit {limit:?}"
            );
        }
    }

    #[test]
    fn duckdb_copy_depth_is_capped_at_two_ancestors() {
        // (requested limit, source depth, copies?)
        let cases = [
            (None, 0, true),
            (None, 1, true),
            (None, 2, false),
            (Some(1), 0, true),
            (Some(1), 1, false),
            (Some(5), 1, true),
            (Some(5), 2, false),
        ];
        for (limit, depth, copies) in cases {
            let plan = plan_submit(&duck_submit(limit), duck_equivalent(depth, Some(5)));
            assert_eq!(
                matches!(plan, SubmitCopyPlan::Copy(_)),
                copies,
                "limit {limit:?}, depth {depth}: {plan:?}"
            );
            if !copies {
                assert_eq!(plan, SubmitCopyPlan::ChainLimitExecute);
            }
        }
    }

    #[test]
    fn copy_is_unavailable_without_ddl_or_source_evidence() {
        assert!(matches!(
            plan_submit(&duck_submit(None), duck_equivalent(0, None)),
            SubmitCopyPlan::Unavailable { .. }
        ));
        let mut req = submit("snowflake", "select id from DB.DEV.UP");
        req.clone_table_properties = Some(qc::TableProperties {
            hours_to_expiration: Some(1),
            ..Default::default()
        });
        assert!(matches!(
            plan_submit(&req, equivalent(PROD, 0, Some(5))),
            SubmitCopyPlan::Unavailable { .. }
        ));
    }

    // ---- client-requested copy ---------------------------------------------

    fn register(dialect: &str) -> qc::CloneRequest {
        qc::CloneRequest {
            dialect: dialect.into(),
            execution_type: 1,
            clone_source_table: PROD.into(),
            target_table: DEV.into(),
            table_namespace: Some("ns".into()),
            clone_source_table_type: Some("TABLE".into()),
            ..Default::default()
        }
    }

    fn duck_register() -> qc::CloneRequest {
        let mut req = qc::CloneRequest {
            execution_type: ModelExecutionType::DeleteInsert as i32,
            clone_source_table: DUCK_PROD.into(),
            target_table: DUCK_DEV.into(),
            clone_source_last_modified_epoch: Some(7),
            clone_source_table_type: Some("DUCKDB CTAS TABLE".into()),
            ..register("duckdb")
        };
        req.labels.insert(
            "duckdb_incremental_contract".into(),
            "delete-insert-fixed-v1".into(),
        );
        req.labels.insert(
            "duckdb_incremental_identity".into(),
            "duckdb-incremental-init-v2:abc".into(),
        );
        req
    }

    #[test]
    fn copy_forbidding_target_is_rejected_before_any_lookup() {
        for mut req in [register("snowflake"), duck_register()] {
            req.clone_chain_depth_limit = Some(0);
            assert_eq!(
                register_lookup(&req),
                Err(RegisterRefusal::Reject(
                    RegisterRejection::ChainLimitExceeded
                ))
            );
        }
    }

    #[test]
    fn snowflake_inherits_the_source_logic() {
        let req = register("snowflake");
        assert_eq!(
            register_lookup(&req),
            Ok(RegisterLookup::SourceLogic {
                source: PROD,
                table_namespace: Some("ns"),
            })
        );
        let plan = plan_register(&req, RegisterHistory::SourceLogic(Some("logic".into())))
            .expect("registered copy");
        assert_eq!(plan.inherited_ns_logic.as_deref(), Some("logic"));
        assert_eq!(plan.required_source_epoch, None);
        assert_eq!(plan.clone_sqls.len(), 1);

        let mut blank = register("snowflake");
        blank.table_namespace = Some(String::new());
        assert_eq!(
            register_lookup(&blank),
            Ok(RegisterLookup::SourceLogic {
                source: PROD,
                table_namespace: None,
            })
        );
    }

    #[test]
    fn other_warehouses_inherit_nothing() {
        let req = register("bigquery");
        assert_eq!(register_lookup(&req), Ok(RegisterLookup::Nothing));
        let plan = plan_register(&req, RegisterHistory::Nothing).expect("registered copy");
        assert_eq!(plan.inherited_ns_logic, None);
        assert_eq!(
            register_lookup(&register("redshift")),
            Err(RegisterRefusal::Unsupported(
                "clone DDL is not implemented for this dialect"
            ))
        );
    }

    #[test]
    fn duckdb_requires_the_pinned_incremental_contract() {
        let req = duck_register();
        assert_eq!(
            register_lookup(&req),
            Ok(RegisterLookup::DuckDbIncrementalSource {
                source: DUCK_PROD,
                table_namespace: "ns",
                last_modified_epoch: 7,
                identity: "duckdb-incremental-init-v2:abc",
            })
        );

        type Mutation = fn(&mut qc::CloneRequest);
        let cases: Vec<(&str, Mutation)> = vec![
            ("not incremental", |r| r.execution_type = 1),
            ("no namespace", |r| r.table_namespace = None),
            ("no source epoch", |r| {
                r.clone_source_last_modified_epoch = None
            }),
            ("no identity", |r| {
                r.labels.remove("duckdb_incremental_identity");
            }),
            ("other contract", |r| {
                r.labels
                    .insert("duckdb_incremental_contract".into(), "merge".into());
            }),
            ("old identity version", |r| {
                r.labels.insert(
                    "duckdb_incremental_identity".into(),
                    "duckdb-incremental-init-v1:abc".into(),
                );
            }),
            ("not inspected as CTAS", |r| {
                r.clone_source_table_type = Some("BASE TABLE".into())
            }),
        ];
        for (why, mutate) in cases {
            let mut req = duck_register();
            mutate(&mut req);
            assert_eq!(
                register_lookup(&req),
                Err(RegisterRefusal::Reject(
                    RegisterRejection::ForcedNotEligible
                )),
                "{why}"
            );
        }
    }

    #[test]
    fn unsupported_ddl_is_reported_before_any_history_read_except_for_duckdb() {
        let mut req = register("snowflake");
        req.table_properties = Some(qc::TableProperties {
            hours_to_expiration: Some(1),
            ..Default::default()
        });
        assert!(matches!(
            register_lookup(&req),
            Err(RegisterRefusal::Unsupported(_))
        ));

        // DuckDB checks its source first: an ineligible source is refused even
        // when no DDL could be generated for it.
        let mut duck = duck_register();
        duck.target_table = "\"other_file\".\"dev\".\"m\"".into();
        assert!(matches!(
            register_lookup(&duck),
            Ok(RegisterLookup::DuckDbIncrementalSource { .. })
        ));
        assert_eq!(
            plan_register(&duck, RegisterHistory::DuckDbIncrementalSource(None)),
            Err(RegisterRefusal::Reject(
                RegisterRejection::ForcedNotEligible
            ))
        );
        assert!(matches!(
            plan_register(&duck, RegisterHistory::DuckDbIncrementalSource(Some(None))),
            Err(RegisterRefusal::Unsupported(_))
        ));
    }

    #[test]
    fn duckdb_copy_needs_a_current_eligible_source() {
        let req = duck_register();
        assert_eq!(
            plan_register(&req, RegisterHistory::DuckDbIncrementalSource(None)),
            Err(RegisterRefusal::Reject(
                RegisterRejection::ForcedNotEligible
            ))
        );
        let plan = plan_register(
            &req,
            RegisterHistory::DuckDbIncrementalSource(Some(Some("logic".into()))),
        )
        .expect("registered copy");
        assert_eq!(plan.inherited_ns_logic.as_deref(), Some("logic"));
        assert_eq!(plan.required_source_epoch, Some(7));
        assert_eq!(
            plan.clone_sqls,
            vec![format!(
                "CREATE OR REPLACE TABLE {DUCK_DEV} AS SELECT * FROM {DUCK_PROD}"
            )]
        );
    }
}
