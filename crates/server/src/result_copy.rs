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
//! * **Seed copy on submit** (`SubmitValues`, Snowflake only, #91): the one
//!   exception to "FULL only" on submit. [`seed_copy_target`] says whether a
//!   seed without a reuse candidate may be copied, [`seed_lookup`] what to
//!   look for (the seed's reuse identity in another schema), and
//!   [`plan_seed_submit`] turns the load found into a plan under the same
//!   limits as a FULL copy.
//!
//! The Submit decision module (submit fallback) and the gRPC adapter
//! (client-requested copy) perform the lookup and write the pending row.
//! Everything that decides the outcome is here; warehouse-specific questions
//! (DDL, relation checks, copy-chain limits, source evidence, the incremental
//! contract) are answered by the [`Warehouse`] rules, never by testing the
//! dialect string here.

use crate::analyzed_workload::identity::RehomedLogic;
use crate::decision::NoCandidate;
use crate::execution::ExecutionEvidence;
use crate::query_cache as qc;
use crate::query_cache::ModelExecutionType;
use crate::reuse_candidate::Selection;
#[cfg(test)]
use crate::store::InputTable;
use crate::store::{CopyCandidate, ExecutionRow};
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
    /// (`analyzed_workload::identity::rehomed_copy_logic_hashes`, #107). Each admits only a
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
    /// read as `logic.schema` ([`crate::copy_lifecycle::canonical_reads`]). `None` when they do not
    /// read a relation in each schema the reading names: they cannot then
    /// show that a build resolved its references in those schemas.
    pub reads: Option<Vec<Vec<String>>>,
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
/// (`sql_norm::request_has_volatile_sql`) is a conservative suppression gate;
/// the values Volatile evaluation reports are rendered separately, after the
/// decision (`volatile_evaluation`).
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
            crate::analyzed_workload::identity::rehomed_copy_logic_hashes(
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
    /// A seed keeps its ordinary load description with these codes
    /// (2026-11-12, explain_seed_copy_disabled.jsonl).
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
    plan_copy(
        &CopyRequest {
            dialect: &req.dialect,
            execution_type: req.execution_type,
            semantic_extras: &req.semantic_extras,
            allow_clones: req.allow_clones,
            clone_chain_depth_limit: req.clone_chain_depth_limit,
            target_table: req.target_table.as_deref().unwrap_or_default(),
            clone_table_properties: req.clone_table_properties.as_ref(),
        },
        equivalent,
    )
}

/// What [`plan_copy`] reads from a submit: the same fields of a SQL submit
/// and of a seed's `SubmitValues`.
struct CopyRequest<'a> {
    dialect: &'a str,
    execution_type: i32,
    semantic_extras: &'a std::collections::HashMap<String, String>,
    allow_clones: Option<bool>,
    clone_chain_depth_limit: Option<i64>,
    target_table: &'a str,
    clone_table_properties: Option<&'a qc::TableProperties>,
}

/// The one submit copy plan, for SQL submits and seeds alike.
fn plan_copy(req: &CopyRequest<'_>, equivalent: CloneEquivalent) -> SubmitCopyPlan {
    let warehouse = Warehouse::from_dialect(req.dialect);
    if !warehouse.submit_copy_admits(req.execution_type, req.semantic_extras) {
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
            req.target_table,
            equivalent.table_type.as_deref(),
            req.clone_table_properties,
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

// ── Seed copy on submit (#91) ───────────────────────────────────────────────

/// Where a seed submit without a reuse candidate may be copied
/// ([`seed_copy_target`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SeedCopyTarget<'a> {
    /// The seed's table does not exist.
    Absent,
    /// The seed's table exists, and its latest load in scope (this row) has
    /// other values or seed config. The copy replaces it.
    Over(&'a ExecutionRow),
}

/// Whether a seed submit may be answered with a copy, and over what. A seed
/// with a reuse candidate is reused. Otherwise it is copied into its absent
/// table (VERIFIED LIVE 2026-11-10/11, client 2.0.11:
/// explain_copy_probes.jsonl entry 12, explain_seed_copy_existing.jsonl entry
/// 6), or over its existing table when that table's latest load in scope is a
/// seed load with other values (explain_seed_copy_existing.jsonl entry 14).
///
/// It is not copied over a table modified after a load with its values:
/// hosted SKIPs that seed, serving the modified table (entry 13), and this
/// server reloads it (AGENTS.md reuse invariant 3, #129). Nor over an
/// existing table without a seed load in scope, which is uncaptured. `latest`
/// is the target's current physical state, as selection read it.
pub(crate) fn seed_copy_target<'a>(
    req: &qc::SubmitValuesRequest,
    selection: &Selection,
    latest: Option<&'a ExecutionRow>,
) -> Option<SeedCopyTarget<'a>> {
    if selection.candidate.is_some() {
        return None;
    }
    if req.last_modified_epoch.is_none() {
        return Some(SeedCopyTarget::Absent);
    }
    match (selection.why_no_candidate, latest) {
        (Some(NoCandidate::IdentityMismatch), Some(row))
            if row.execution_type == ModelExecutionType::Values as i32 =>
        {
            Some(SeedCopyTarget::Over(row))
        }
        _ => None,
    }
}

/// What a seed copy looks for: the latest load of each other Target relation
/// in the submit's scope (organization, warehouse family, `table_namespace`)
/// with the seed's reuse identity, its `values_hash` and its seed logic hash
/// (dialect, catalog and every `semantic_extras` entry the client sends,
/// `analyzed_workload::identity::seed_logic_hash`). Only its schema may differ from the
/// target ([`SeedCopyLookup::admits`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeedCopyLookup<'a> {
    pub table_namespace: &'a str,
    pub seed_logic_hash: &'a str,
    pub values_hash: &'a str,
    pub target_table: &'a str,
}

/// Whether to look for a seed to copy, and its identity. Only where
/// [`Warehouse::submit_copy_admits`] admits a seed (Snowflake): DuckDB and
/// every other dialect reload it. A seed without values or a namespace is
/// never copied.
pub(crate) fn seed_lookup<'a>(
    req: &'a qc::SubmitValuesRequest,
    evidence: &'a ExecutionEvidence,
) -> Option<SeedCopyLookup<'a>> {
    let warehouse = Warehouse::from_dialect(&req.dialect);
    if !warehouse.submit_copy_admits(ModelExecutionType::Values as i32, &req.semantic_extras)
        || req.values_hash.is_empty()
    {
        return None;
    }
    Some(SeedCopyLookup {
        table_namespace: crate::execution::non_blank_namespace(req.table_namespace.as_deref())?,
        seed_logic_hash: evidence.node_sql_hash.as_deref()?,
        values_hash: &req.values_hash,
        target_table: &req.target_table,
    })
}

impl SeedCopyLookup<'_> {
    /// Whether `source` names the same seed relation in another schema: the
    /// same database and object, another schema (maintainer decision
    /// 2026-11-11, #91), each part compared as Snowflake resolves it
    /// ([`crate::sql_norm::canonical_identifier`]).
    pub(crate) fn admits(&self, source: &str) -> bool {
        let parts = |name: &str| -> Vec<String> {
            crate::sql_norm::split_object_name_components(name)
                .iter()
                .map(|p| crate::sql_norm::canonical_identifier(p))
                .collect()
        };
        let (source, target) = (parts(source), parts(self.target_table));
        source.len() == 3
            && target.len() == 3
            && source[0] == target[0]
            && source[2] == target[2]
            && source[1] != target[1]
    }
}

/// Plan a seed copy from the load [`seed_lookup`] found, under the limits of
/// a FULL copy: `allow_clones: false` and a copy-forbidding target do not
/// copy, and the DDL follows the source's recorded table type.
pub(crate) fn plan_seed_submit(
    req: &qc::SubmitValuesRequest,
    equivalent: CloneEquivalent,
) -> SubmitCopyPlan {
    plan_copy(
        &CopyRequest {
            dialect: &req.dialect,
            execution_type: ModelExecutionType::Values as i32,
            semantic_extras: &req.semantic_extras,
            allow_clones: req.allow_clones,
            clone_chain_depth_limit: req.clone_chain_depth_limit,
            target_table: &req.target_table,
            clone_table_properties: req.clone_table_properties.as_ref(),
        },
        equivalent,
    )
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

use crate::copy_lifecycle::DirectCopyProvenance;

#[cfg(test)]
fn direct_copy_provenance(req: &qc::SqlExecution) -> Option<DirectCopyProvenance> {
    crate::analyzed_workload::analyze(
        req,
        crate::analyzed_workload::Purpose::Submit {
            compare_unrendered_code: false,
        },
    )
    .direct_provenance
}

/// #152's observed is_defer_to_profile + chain-zero wire scope. This proves
/// only a refused-optimization display, never a candidate or copy grant.
pub(crate) fn direct_refusal_scope(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
) -> bool {
    Warehouse::from_dialect(&req.dialect) == Warehouse::Databricks
        && req.is_defer_to_profile
        && !req.compare_unrendered_code
        && req.tolerate_nondeterminism
        && !req.ignore_external_modifications
        && req.allow_clones == Some(true)
        && req.clone_chain_depth_limit == Some(0)
        && req.freshness_tolerance_seconds == 0
        && req.stale_upstream_policy == 0
        && req
            .table_namespace
            .as_deref()
            .is_some_and(|ns| !ns.trim().is_empty())
        && evidence
            .direct_copy_provenance
            .as_ref()
            .is_some_and(|p| p.version == 2)
        && req.semantic_extras.get("unique_key").map(String::as_str) == Some("\"id\"")
        && match req.execution_type {
            3 => {
                req.semantic_extras.len() == 3
                    && req.semantic_extras.get("lookback").map(String::as_str) == Some("1")
                    && req
                        .semantic_extras
                        .get("on_schema_change")
                        .map(String::as_str)
                        == Some("\"ignore\"")
            }
            7 => req.semantic_extras.len() == 1,
            _ => false,
        }
}

/// #153's measured ordinary MERGE submit reports a fresh equivalent as 1/6.
/// Reuse the exact direct-input diagnostic proof, without granting any copy.
fn submit_refusal_reading(
    req: &qc::SubmitEnrichedSqlRequest,
) -> Option<qc::SubmitEnrichedSqlRequest> {
    if req.execution_type != ModelExecutionType::Merge as i32
        || req.is_defer_to_profile
        || req.clone_chain_depth_limit != Some(1)
    {
        return None;
    }
    let mut reading = req.clone();
    reading.is_defer_to_profile = true;
    reading.clone_chain_depth_limit = Some(0);
    Some(reading)
}

pub(crate) fn submit_refusal_scope(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
) -> bool {
    submit_refusal_reading(req).is_some_and(|reading| direct_refusal_scope(&reading, evidence))
}

pub(crate) fn submit_refusal_matches(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    row: &crate::store::ExecutionRow,
) -> bool {
    submit_refusal_reading(req)
        .is_some_and(|reading| direct_refusal_matches(&reading, evidence, row))
}

pub(crate) fn direct_refusal_matches(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    row: &crate::store::ExecutionRow,
) -> bool {
    let Some(context) = row
        .direct_copy_provenance
        .as_ref()
        .filter(|p| p.version == 2)
    else {
        return false;
    };
    if !direct_refusal_scope(req, evidence)
        || row.org_id != evidence.org_id
        || row.table_namespace != evidence.table_namespace
        || row.dialect != req.dialect
        || row.execution_type != req.execution_type
        || row.clone_source_table.is_some()
        || row.status != "confirmed"
        || row.last_modified_epoch.is_none_or(|e| e <= 0)
        || evidence.node_unique_id.is_none()
        || row.node_unique_id != evidence.node_unique_id
    {
        return false;
    }
    let physical = |name: &str| Warehouse::physical_scope_key(&req.dialect, name);
    let current: Vec<_> = evidence
        .input_tables
        .iter()
        .filter(|i| physical(&i.name) != physical(&evidence.target_table))
        .collect();
    if physical(&row.target_table) == physical(&evidence.target_table) {
        return false;
    }
    let recorded: Vec<_> = row
        .input_tables
        .iter()
        .filter(|i| physical(&i.name) != physical(&row.target_table))
        .collect();
    if current.len() != 1
        || recorded.len() != 1
        || physical(&current[0].name) != physical(&recorded[0].name)
        || current[0].last_modified_epoch.is_none_or(|e| e <= 0)
        || current[0].last_modified_epoch != recorded[0].last_modified_epoch
        || current[0].producer != crate::store::Producer::NoBuild
        || recorded[0].producer != crate::store::Producer::NoBuild
    {
        return false;
    }
    // The direct-input gate proves every read fully qualified. Compare this
    // exact rendered SQL in the other row's retained actual context. Client
    // config hashes stay mandatory for adoption; this is only a refusal code.
    let mut reading = crate::execution::sql_execution(req);
    reading.default_catalog = context.default_catalog.clone();
    reading.default_schema = context.default_schema.clone();
    reading.target_table = Some(row.target_table.clone());
    row.node_sql_hash.as_deref()
        == Some(crate::analyzed_workload::identity::sql_logic_hash(&reading, None).as_str())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectRegistration {
    pub provenance: DirectCopyProvenance,
    pub proof: crate::analyzed_workload::query::CopyProof,
    pub inputs: Vec<crate::store::InputTable>,
    pub runtime_ms: Option<i64>,
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
    /// Databricks direct pre-copy requires the current scoped source build.
    DatabricksSource {
        source: &'a str,
        table_namespace: &'a str,
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
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RegisterHistory {
    /// `None`: no eligible source. `Some(logic)`: eligible, carrying the
    /// source's logic identity (itself optional).
    DuckDbIncrementalSource(Option<Option<String>>),
    /// The source's current confirmed build, if any.
    SourceLogic(Option<crate::store::CopySourceBuild>),
    DatabricksSource(Option<Box<crate::store::ExecutionRow>>),
    Nothing,
}

/// A client-requested copy that will be registered.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RegisterPlan {
    pub direct: Option<DirectRegistration>,
    /// What the copy inherits from its source's current build (Snowflake).
    pub source_build: Option<crate::store::CopySourceBuild>,
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
    if warehouse == Warehouse::Databricks {
        return match (
            crate::execution::non_blank_namespace(req.table_namespace.as_deref()),
            req.clone_source_last_modified_epoch,
            req.labels
                .get("dbt_node_unique_id")
                .filter(|uid| !uid.is_empty()),
            req.execution_type,
        ) {
            (Some(namespace), Some(_), Some(_), 3 | 7) => Ok(RegisterLookup::DatabricksSource {
                source: &req.clone_source_table,
                table_namespace: namespace,
            }),
            _ => Err(RegisterRefusal::Reject(
                RegisterRejection::ForcedNotEligible,
            )),
        };
    }
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
    let (mut direct, mut source_build) = (None, None);
    let (inherited_ns_logic, required_source_epoch) = match history {
        RegisterHistory::DuckDbIncrementalSource(None) => {
            return Err(RegisterRefusal::Reject(
                RegisterRejection::ForcedNotEligible,
            ))
        }
        RegisterHistory::DuckDbIncrementalSource(Some(logic)) => {
            (logic, req.clone_source_last_modified_epoch)
        }
        RegisterHistory::SourceLogic(build) => {
            let logic = build
                .as_ref()
                .and_then(|build| build.node_namespace_logic_hash.clone());
            source_build = build;
            (logic, None)
        }
        RegisterHistory::DatabricksSource(source) => {
            direct = crate::copy_lifecycle::qualify_registration(req, source.as_deref())?;
            // This establishes permission to attempt the warehouse DDL, not
            // post-copy SQL equivalence or freshness. No automatic copy identity.
            (None, None)
        }
        RegisterHistory::Nothing => (None, None),
    };
    Ok(RegisterPlan {
        direct,
        source_build,
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
    use crate::copy_lifecycle::canonical_reads;

    #[test]
    fn direct_refusal_diagnostic_requires_fresh_scoped_rendered_evidence() {
        let req = qc::SubmitEnrichedSqlRequest {
            dialect: "databricks".into(),
            execution_type: 3,
            sql: "select * from cdo_demo_db.prod.inputs".into(),
            target_table: Some("`cdo_demo_db`.`prod`.`output`".into()),
            default_catalog: "cdo_demo_db".into(),
            default_schema: Some("prod".into()),
            table_namespace: Some("ns".into()),
            is_defer_to_profile: true,
            tolerate_nondeterminism: true,
            allow_clones: Some(true),
            clone_chain_depth_limit: Some(0),
            semantic_extras: [
                ("unique_key".into(), "\"id\"".into()),
                ("lookback".into(), "1".into()),
                ("on_schema_change".into(), "\"ignore\"".into()),
            ]
            .into(),
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: "model.project.output".into(),
                node_configs_hash: Some("a".repeat(32)),
                node_body_hash: Some("b".repeat(32)),
                resource_type: "model".into(),
                ..Default::default()
            }),
            tables: vec![qc::TableModifiedInfo {
                name: "`cdo_demo_db`.`prod`.`inputs`".into(),
                last_modified_epoch: Some(10),
            }],
            ..Default::default()
        };
        let mut evidence = ExecutionEvidence::sql_submit("o", &req);
        evidence.input_tables[0].producer = crate::store::Producer::NoBuild;
        let mut dev = req.clone();
        dev.default_schema = Some("dev".into());
        dev.target_table = Some("`cdo_demo_db`.`dev`.`output`".into());
        dev.dbt_node_state.as_mut().unwrap().node_configs_hash = Some("c".repeat(32));
        let dev_evidence = ExecutionEvidence::sql_submit("o", &dev);
        let row = ExecutionRow {
            org_id: "o".into(),
            status: "confirmed".into(),
            target_table: dev_evidence.target_table,
            dialect: "databricks".into(),
            execution_type: 3,
            table_namespace: Some("ns".into()),
            node_unique_id: evidence.node_unique_id.clone(),
            node_sql_hash: dev_evidence.node_sql_hash,
            direct_copy_provenance: dev_evidence.direct_copy_provenance,
            last_modified_epoch: Some(20),
            input_tables: evidence.input_tables.clone(),
            ..seed_load()
        };
        assert!(direct_refusal_matches(&req, &evidence, &row));
        for fault in [
            "uid",
            "namespace",
            "org",
            "epoch",
            "producer",
            "other-input",
            "sql",
            "version",
            "type",
            "pending",
            "own-target",
        ] {
            let mut row = row.clone();
            match fault {
                "uid" => row.node_unique_id = Some("model.other.output".into()),
                "namespace" => row.table_namespace = Some("other".into()),
                "org" => row.org_id = "other".into(),
                "epoch" => row.input_tables[0].last_modified_epoch = None,
                "producer" => row.input_tables[0].producer = crate::store::Producer::Unrecorded,
                "other-input" => row.input_tables[0].name = "cdo_demo_db.other.inputs".into(),
                "sql" => row.node_sql_hash = Some("other-rendered-query".into()),
                "version" => row.direct_copy_provenance.as_mut().unwrap().version = 255,
                "type" => row.execution_type = 7,
                "pending" => row.status = "pending".into(),
                "own-target" => row.target_table = evidence.target_table.clone(),
                _ => unreachable!(),
            }
            assert!(!direct_refusal_matches(&req, &evidence, &row), "{fault}");
        }
        let mut unsupported = evidence.clone();
        unsupported.direct_copy_provenance = None;
        assert!(!direct_refusal_matches(&req, &unsupported, &row));
        for flag in [
            "defer",
            "limit",
            "template",
            "namespace",
            "lag",
            "policy",
            "external",
        ] {
            let mut req = req.clone();
            match flag {
                "defer" => req.is_defer_to_profile = false,
                "limit" => req.clone_chain_depth_limit = Some(1),
                "template" => req.compare_unrendered_code = true,
                "namespace" => req.table_namespace = None,
                "lag" => req.freshness_tolerance_seconds = 1,
                "policy" => req.stale_upstream_policy = 1,
                "external" => req.ignore_external_modifications = true,
                _ => unreachable!(),
            }
            assert!(!direct_refusal_scope(&req, &evidence), "{flag}");
        }
    }

    #[test]
    fn direct_chain_diagnostic_requires_known_current_identity() {
        for case in [
            "measured",
            "unknown_epoch",
            "zero_epoch",
            "missing_uid",
            "empty_uid",
            "other_uid",
            "other_type",
            "larger_limit",
        ] {
            let mut source = ExecutionRow {
                execution_type: 3,
                node_unique_id: Some("model.project.out".into()),
                last_modified_epoch: Some(30),
                clone_source_table: Some("cdo.prod.out".into()),
                ..seed_load()
            };
            let mut request = qc::CloneRequest {
                dialect: "databricks".into(),
                execution_type: 3,
                clone_source_last_modified_epoch: Some(30),
                clone_chain_depth_limit: Some(1),
                labels: [("dbt_node_unique_id".into(), "model.project.out".into())].into(),
                ..Default::default()
            };
            match case {
                "unknown_epoch" => {
                    source.last_modified_epoch = None;
                    request.clone_source_last_modified_epoch = None;
                }
                "zero_epoch" => {
                    source.last_modified_epoch = Some(0);
                    request.clone_source_last_modified_epoch = Some(0);
                }
                "missing_uid" => {
                    source.node_unique_id = None;
                    request.labels.clear();
                }
                "empty_uid" => {
                    source.node_unique_id = Some("".into());
                    request
                        .labels
                        .insert("dbt_node_unique_id".into(), "".into());
                }
                "other_uid" => {
                    request
                        .labels
                        .insert("dbt_node_unique_id".into(), "model.other.out".into());
                }
                "other_type" => request.execution_type = 7,
                "larger_limit" => request.clone_chain_depth_limit = Some(2),
                _ => {}
            }
            assert_eq!(
                plan_register(
                    &request,
                    RegisterHistory::DatabricksSource(Some(Box::new(source)))
                ),
                Err(RegisterRefusal::Reject(if case == "measured" {
                    RegisterRejection::ChainLimitExceeded
                } else {
                    RegisterRejection::ForcedNotEligible
                })),
                "{case}"
            );
        }
    }

    #[test]
    fn direct_copy_from_rebuilt_source_requires_only_its_genuine_input_evidence() {
        let source_name = "`cdo_demo_db`.`prod`.`output`";
        let provenance = DirectCopyProvenance {
            version: 2,
            published: false,
            identity: crate::analyzed_workload::query::CopyProof {
                version: 2,
                identity: "core".into(),
                bindings: vec![],
                effects: "checked".into(),
            }
            .for_origin("databricks-direct-copy-v2"),
            default_catalog: "cdo_demo_db".into(),
            default_schema: Some("prod".into()),
            source_execution_id: None,
            source_epoch: None,
            source_physical_key: None,
        };
        let source = ExecutionRow {
            id: 7,
            target_table: source_name.into(),
            dialect: "databricks".into(),
            execution_type: 3,
            node_unique_id: Some("model.project.output".into()),
            node_sql_hash: Some("rendered".into()),
            direct_copy_provenance: Some(provenance),
            analyzed_copy_proof: Some(crate::analyzed_workload::query::CopyProof {
                version: 2,
                identity: "core".into(),
                bindings: vec![],
                effects: "checked".into(),
            }),
            last_modified_epoch: Some(30),
            input_tables: vec![
                crate::store::InputTable {
                    name: "`cdo_demo_db`.`prod`.`inputs`".into(),
                    last_modified_epoch: Some(7),
                    producer: crate::store::Producer::NoBuild,
                },
                // An incremental's own observed target is deliberately not a
                // resolved producer or a genuine upstream of its copied result.
                crate::store::InputTable {
                    name: source_name.into(),
                    last_modified_epoch: Some(20),
                    producer: crate::store::Producer::Unrecorded,
                },
            ],
            ..seed_load()
        };
        let request = qc::CloneRequest {
            dialect: "databricks".into(),
            execution_type: 3,
            clone_source_table: source_name.into(),
            target_table: "`cdo_demo_db`.`dev`.`output`".into(),
            clone_source_last_modified_epoch: Some(30),
            labels: [("dbt_node_unique_id".into(), "model.project.output".into())].into(),
            ..Default::default()
        };
        for own_spelling in [source_name, "CDO_DEMO_DB.PROD.OUTPUT"] {
            let mut current = source.clone();
            current.input_tables[1].name = own_spelling.into();
            let plan = plan_register(
                &request,
                RegisterHistory::DatabricksSource(Some(Box::new(current))),
            )
            .unwrap();
            let direct = plan
                .direct
                .expect("rebuilt source retains adoption evidence");
            assert_eq!(direct.inputs.len(), 1);
            assert_eq!(direct.inputs[0].name, source.input_tables[0].name);
        }
        for unknown in [false, true] {
            let mut source = source.clone();
            if unknown {
                source.input_tables[0].last_modified_epoch = None;
            } else {
                source.input_tables[0].producer = crate::store::Producer::Unrecorded;
            }
            assert!(plan_register(
                &request,
                RegisterHistory::DatabricksSource(Some(Box::new(source)))
            )
            .unwrap()
            .direct
            .is_none());
        }
    }

    #[test]
    fn direct_copy_uses_shared_proof_and_exact_physical_inputs() {
        let req = qc::SqlExecution {
            dialect: "databricks".into(),
            execution_type: 3,
            sql: "select * from cdo_demo_db.prod.inputs".into(),
            default_catalog: "cdo_demo_db".into(),
            default_schema: Some("prod".into()),
            target_table: Some("`cdo_demo_db`.`prod`.`output`".into()),
            tables: vec![qc::TableModifiedInfo {
                name: "`cdo_demo_db`.`prod`.`inputs`".into(),
                last_modified_epoch: Some(7),
            }],
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: "model.project.output".into(),
                resource_type: "model".into(),
                node_body_hash: Some("a".repeat(32)),
                node_configs_hash: Some("b".repeat(32)),
                ..Default::default()
            }),
            ..Default::default()
        };
        let source = direct_copy_provenance(&req).expect("native plain three-part source");
        let mut dev = req.clone();
        dev.default_schema = Some("dev".into());
        dev.target_table = Some("`cdo_demo_db`.`dev`.`output`".into());
        assert_eq!(
            direct_copy_provenance(&dev).unwrap().identity,
            source.identity
        );
        for sql in [
            "select * from inputs",
            "select * from other.prod.inputs",
            "select * from cdo_demo_db.prod.inputs where id=1",
            "select * from cdo_demo_db.prod.inputs join cdo_demo_db.prod.other using(id)",
            "select * from cdo_demo_db.prod.inputs; select 1",
            "select * from cdo_demo_db.prod.\"inputs\"",
            "select * from `cdo_demo_db.prod`.`inputs`",
            "select * from cdo_demo_db.prod.inputs()",
            "select * from cdo_demo_db.prod.inputs-1",
        ] {
            dev.sql = sql.into();
            assert!(
                direct_copy_provenance(&dev).is_none(),
                "unsupported or different physical input: {sql}"
            );
        }
    }

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
                    hash: crate::analyzed_workload::identity::namespace_logic_hash(&prod),
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

    // ---- seed copy on submit (#91) -----------------------------------------

    const DEV_SEED: &str = "\"DB\".\"DEV\".\"S\"";
    const PROD_SEED: &str = "\"DB\".\"PROD\".\"S\"";

    fn seed_submit(dialect: &str) -> qc::SubmitValuesRequest {
        qc::SubmitValuesRequest {
            target_table: DEV_SEED.into(),
            dialect: dialect.into(),
            default_catalog: "DB".into(),
            values_hash: "v1".into(),
            table_namespace: Some("ns".into()),
            ..Default::default()
        }
    }

    fn seed_equivalent(table_type: &str) -> CloneEquivalent {
        CloneEquivalent {
            execution_type: ModelExecutionType::Values as i32,
            table_type: Some(table_type.into()),
            ..equivalent(PROD_SEED, 0, Some(5))
        }
    }

    /// Snowflake is the one dialect that copies a seed on submit (maintainer
    /// decision 2026-11-11, #91); DuckDB and every other dialect reload it.
    #[test]
    fn only_snowflake_looks_for_a_seed_to_copy() {
        for (dialect, looks) in [
            ("snowflake", true),
            (" Snowflake ", true),
            ("duckdb", false),
            ("bigquery", false),
            ("databricks", false),
            ("redshift", false),
            ("postgres", false),
        ] {
            let req = seed_submit(dialect);
            let ev = ExecutionEvidence::values_submit("o", &req);
            assert_eq!(seed_lookup(&req, &ev).is_some(), looks, "{dialect}");
        }
    }

    #[test]
    fn seed_lookup_needs_values_and_a_namespace() {
        type Mutation = fn(&mut qc::SubmitValuesRequest);
        let cases: Vec<(&str, Mutation)> = vec![
            ("no values hash", |r| r.values_hash = String::new()),
            ("no namespace", |r| r.table_namespace = None),
            ("empty namespace", |r| {
                r.table_namespace = Some(String::new())
            }),
        ];
        for (why, mutate) in cases {
            let mut req = seed_submit("snowflake");
            mutate(&mut req);
            let ev = ExecutionEvidence::values_submit("o", &req);
            assert_eq!(seed_lookup(&req, &ev), None, "{why}");
        }
    }

    /// The copy identity is the seed's reuse identity: its values and every
    /// seed config entry the client sends.
    #[test]
    fn seed_lookup_matches_values_and_seed_config() {
        let req = seed_submit("snowflake");
        let ev = ExecutionEvidence::values_submit("o", &req);
        let lookup = seed_lookup(&req, &ev).unwrap();
        assert_eq!(lookup.values_hash, "v1");
        assert_eq!(lookup.table_namespace, "ns");
        assert_eq!(Some(lookup.seed_logic_hash), ev.node_sql_hash.as_deref());

        let mut typed = seed_submit("snowflake");
        typed
            .semantic_extras
            .insert("column_types".into(), "{\"id\": \"varchar\"}".into());
        let typed_ev = ExecutionEvidence::values_submit("o", &typed);
        assert_ne!(
            seed_lookup(&typed, &typed_ev).unwrap().seed_logic_hash,
            lookup.seed_logic_hash,
            "other column types are another seed identity"
        );
    }

    #[test]
    fn a_seed_copy_source_differs_from_the_target_in_its_schema_only() {
        let req = seed_submit("snowflake");
        let ev = ExecutionEvidence::values_submit("o", &req);
        let lookup = seed_lookup(&req, &ev).unwrap();
        assert!(lookup.admits(PROD_SEED));
        for source in [
            DEV_SEED,
            "\"DB\".dev.\"S\"",
            "\"DB\".\"PROD\".\"OTHER\"",
            "\"DB2\".\"PROD\".\"S\"",
            "\"PROD\".\"S\"",
            "",
        ] {
            assert!(!lookup.admits(source), "{source}");
        }
    }

    /// The copy DDL follows the source's recorded table type, as for a FULL
    /// copy (VERIFIED LIVE: explain_copy_probes.jsonl entry 12, a TRANSIENT
    /// prod seed).
    #[test]
    fn snowflake_seed_copy_follows_the_source_table_type() {
        let req = seed_submit("snowflake");
        for (table_type, kind) in [("TRANSIENT TABLE", "TRANSIENT TABLE"), ("TABLE", "TABLE")] {
            let SubmitCopyPlan::Copy(copy) = plan_seed_submit(&req, seed_equivalent(table_type))
            else {
                panic!("{table_type}: expected a copy");
            };
            assert_eq!(
                copy.clone_sqls,
                [format!(
                    "\n            CREATE OR REPLACE {kind} {DEV_SEED}\n            \
                     CLONE {PROD_SEED}\n            COPY GRANTS"
                )]
            );
            assert_eq!(copy.source_table, PROD_SEED);
            assert_eq!(copy.source_last_modified_epoch, Some(5));
            assert_eq!(copy.source_execution_runtime_ms, Some(9));
        }
    }

    /// `allow_clones`, the copy chain limit and table properties apply as for
    /// a FULL copy.
    #[test]
    fn seed_copy_limits_apply_as_for_full_copies() {
        let mut disallowed = seed_submit("snowflake");
        disallowed.allow_clones = Some(false);
        disallowed.clone_chain_depth_limit = Some(0);
        assert_eq!(
            plan_seed_submit(&disallowed, seed_equivalent("TABLE")),
            SubmitCopyPlan::CopiesDisabled
        );
        let mut forbidden = seed_submit("snowflake");
        forbidden.clone_chain_depth_limit = Some(0);
        assert_eq!(
            plan_seed_submit(&forbidden, seed_equivalent("TABLE")),
            SubmitCopyPlan::ChainLimitExecute
        );
        let mut expiring = seed_submit("snowflake");
        expiring.clone_table_properties = Some(qc::TableProperties {
            hours_to_expiration: Some(1),
            ..Default::default()
        });
        assert!(matches!(
            plan_seed_submit(&expiring, seed_equivalent("TABLE")),
            SubmitCopyPlan::Unavailable { .. }
        ));
    }

    #[test]
    fn duckdb_never_plans_a_seed_copy() {
        let req = qc::SubmitValuesRequest {
            target_table: DUCK_DEV.into(),
            ..seed_submit("duckdb")
        };
        let equivalent = CloneEquivalent {
            execution_type: ModelExecutionType::Values as i32,
            ..duck_equivalent(0, Some(5))
        };
        assert!(matches!(
            plan_seed_submit(&req, equivalent),
            SubmitCopyPlan::Refused { .. }
        ));
    }

    fn seed_selection(
        why_no_candidate: Option<NoCandidate>,
        candidate: Option<ExecutionRow>,
    ) -> Selection {
        Selection {
            candidate,
            target_has_prior_build: why_no_candidate.is_some(),
            why_no_candidate,
            rejected: None,
            unmatched: None,
            type_changed: None,
            transition_display: None,
            unqualified: None,
        }
    }

    fn seed_load() -> ExecutionRow {
        ExecutionRow {
            id: 7,
            execution_type: ModelExecutionType::Values as i32,
            ..Default::default()
        }
    }

    /// A seed is copied into its absent table (VERIFIED LIVE:
    /// explain_copy_probes.jsonl entry 12) and over its table whose latest
    /// load has other values (explain_seed_copy_existing.jsonl entry 14). It
    /// is not copied over a table modified after a load with its values:
    /// hosted SKIPs that one (entry 13, #129).
    #[test]
    fn a_seed_is_copied_into_an_absent_table_or_over_other_values() {
        let absent = seed_submit("snowflake");
        let mut existing = seed_submit("snowflake");
        existing.last_modified_epoch = Some(10);
        let load = seed_load();

        let first = seed_selection(None, None);
        assert_eq!(
            seed_copy_target(&absent, &first, None),
            Some(SeedCopyTarget::Absent)
        );
        let dropped = seed_selection(Some(NoCandidate::OwnTargetMissing), None);
        assert_eq!(
            seed_copy_target(&absent, &dropped, Some(&load)),
            Some(SeedCopyTarget::Absent)
        );
        let other_values = seed_selection(Some(NoCandidate::IdentityMismatch), None);
        assert_eq!(
            seed_copy_target(&existing, &other_values, Some(&load)),
            Some(SeedCopyTarget::Over(&load))
        );

        let modified = seed_selection(Some(NoCandidate::OwnTargetChangedOrMissing), None);
        assert_eq!(seed_copy_target(&existing, &modified, Some(&load)), None);
        let reusable = seed_selection(None, Some(load.clone()));
        assert_eq!(seed_copy_target(&existing, &reusable, Some(&load)), None);
        // An existing table without a load in scope, or whose latest row is
        // not a seed load: uncaptured, so the seed is reloaded.
        assert_eq!(seed_copy_target(&existing, &first, None), None);
        let model = ExecutionRow {
            execution_type: ModelExecutionType::Full as i32,
            ..seed_load()
        };
        assert_eq!(
            seed_copy_target(&existing, &other_values, Some(&model)),
            None
        );
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
        let plan = plan_register(
            &req,
            RegisterHistory::SourceLogic(Some(crate::store::CopySourceBuild {
                node_namespace_logic_hash: Some("logic".into()),
                execution_runtime_ms: None,
                input_names: Vec::new(),
            })),
        )
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
