//! Shared Result copy evidence lifecycle. Protocol lookup/planning stays in
//! result_copy; source snapshots and adoption live here. Legacy origin formats
//! remain distinct because direct DDL permission is weaker than adoption proof.
use crate::analyzed_workload::identity::RehomedLogic;
use crate::execution::ExecutionEvidence;
use crate::query_cache as qc;
use crate::result_copy::{
    DirectRegistration, EquivalentLookup, RegisterRefusal, RegisterRejection, RehomedReading,
};
use crate::store::CopyCandidate;
use crate::store::{CopySourceBuild, ExecutionRow, InputTable, Producer};
use crate::warehouse::Warehouse;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Provenance {
    pub version: u8,
    pub source_execution_id: i64,
    pub source_epoch: i64,
    pub source_physical_key: String,
    /// Set atomically at Confirm, only while the recorded source stays current.
    pub published: bool,
}

fn physical(name: &str) -> String {
    Warehouse::physical_scope_key("databricks", name)
}

fn source_inputs(row: &ExecutionRow, dialect: &str) -> Vec<InputTable> {
    let own = Warehouse::physical_scope_key(dialect, &row.target_table);
    row.input_tables
        .iter()
        .filter(|t| Warehouse::physical_scope_key(dialect, &t.name) != own)
        .cloned()
        .collect()
}

pub(crate) fn inputs(row: &ExecutionRow) -> Vec<InputTable> {
    source_inputs(row, "databricks")
}

/// The inputs `build` recorded, less its own Target relation: what the rows
/// it wrote were computed from, as it named them.
pub(crate) fn upstreams_of(build: &ExecutionRow) -> Vec<InputTable> {
    source_inputs(build, &build.dialect)
}

fn complete_inputs(inputs: &[InputTable]) -> bool {
    inputs.iter().all(|t| {
        t.last_modified_epoch.is_some_and(|e| e > 0) && !matches!(t.producer, Producer::Unrecorded)
    })
}

/// A source frozen at planning, never reconstructed from the adopter's reads.
struct SourceSnapshot {
    id: i64,
    epoch: Option<i64>,
    key: String,
    inputs: Vec<InputTable>,
    runtime_ms: Option<i64>,
}
impl SourceSnapshot {
    fn new(source: &ExecutionRow, dialect: &str, source_table: &str) -> Self {
        Self {
            id: source.id,
            epoch: source.last_modified_epoch,
            key: Warehouse::physical_scope_key(dialect, source_table),
            inputs: source_inputs(source, dialect),
            runtime_ms: source.execution_runtime_ms,
        }
    }
    fn submit_provenance(&self) -> Provenance {
        Provenance {
            version: 2,
            source_execution_id: self.id,
            source_epoch: self.epoch.expect("qualified source epoch"),
            source_physical_key: self.key.clone(),
            published: false,
        }
    }
}

/// DDL may be granted without this proof. Missing/unknown legacy proof only
/// prevents adoption, preserving the direct registration protocol contract.
pub(crate) fn freeze_direct(
    req: &qc::CloneRequest,
    source: &ExecutionRow,
) -> Option<DirectRegistration> {
    let mut provenance = source.direct_copy_provenance.clone()?;
    let snapshot = SourceSnapshot::new(source, &req.dialect, &req.clone_source_table);
    if provenance.version != 2
        || source
            .analyzed_copy_proof
            .as_ref()
            .is_none_or(|p| p.version != 2)
        || source
            .analyzed_copy_proof
            .as_ref()
            .is_none_or(|p| provenance.identity != p.for_origin("databricks-direct-copy-v2"))
        || snapshot.epoch.is_none_or(|e| e <= 0)
        || !complete_inputs(&snapshot.inputs)
    {
        return None;
    }
    provenance.source_execution_id = Some(snapshot.id);
    provenance.source_epoch = snapshot.epoch;
    provenance.source_physical_key = Some(snapshot.key);
    Some(DirectRegistration {
        provenance,
        proof: source
            .analyzed_copy_proof
            .clone()
            .expect("qualified v2 proof"),
        inputs: snapshot.inputs,
        runtime_ms: snapshot.runtime_ms,
    })
}

/// Ordinary builds have no copy-origin marker, even when their automatic-copy
/// eligibility hash is populated. A retained proof is also an origin marker.
pub(crate) fn ordinary(row: &ExecutionRow) -> bool {
    row.clone_source_table.is_none() && row.submit_copy_provenance.is_none()
}

/// This is exact freshness, not a copy-age anchor. Unknown epochs/producers,
/// a different input binding and every copy source are conservatively refused.
pub(crate) fn source_admits(evidence: &ExecutionEvidence, source: &ExecutionRow) -> bool {
    let current: Vec<_> = evidence
        .input_tables
        .iter()
        .filter(|t| physical(&t.name) != physical(&evidence.target_table))
        .collect();
    let recorded = inputs(source);
    crate::execution::non_blank_namespace(evidence.table_namespace.as_deref()).is_some()
        && source.org_id == evidence.org_id
        && source.table_namespace == evidence.table_namespace
        && Warehouse::from_dialect(&source.dialect).family_key(&source.dialect)
            == Warehouse::from_dialect(&evidence.dialect).family_key(&evidence.dialect)
        && source.execution_type == evidence.execution_type
        && source.status == "confirmed"
        && ordinary(source)
        && source.last_modified_epoch.is_some_and(|e| e > 0)
        && source.node_namespace_logic_hash == evidence.node_namespace_logic_hash
        && source.node_namespace_logic_hash.is_some()
        && source.node_unique_id == evidence.node_unique_id
        && physical(&source.target_table) != physical(&evidence.target_table)
        && source
            .analyzed_copy_proof
            .as_ref()
            .is_some_and(|p| p.version == 2)
        && source.analyzed_copy_proof == evidence.analyzed_copy_proof
        && matching_inputs(&current, &recorded)
}

fn matching_inputs(current: &[&InputTable], recorded: &[InputTable]) -> bool {
    fn keyed<'a>(
        inputs: impl Iterator<Item = &'a InputTable>,
    ) -> Option<std::collections::BTreeMap<String, (i64, &'a Producer)>> {
        let mut out = std::collections::BTreeMap::new();
        for t in inputs {
            let epoch = t.last_modified_epoch.filter(|e| *e > 0)?;
            if matches!(t.producer, Producer::Unrecorded) {
                return None;
            }
            let entry = (epoch, &t.producer);
            if out
                .insert(physical(&t.name), entry)
                .is_some_and(|old| old != entry)
            {
                return None;
            }
        }
        Some(out)
    }
    let Some(current) = keyed(current.iter().copied()) else {
        return false;
    };
    keyed(recorded.iter()).is_some_and(|recorded| recorded == current)
}

/// An invalid publication cannot regain reuse via its strict SQL hash or the
/// generic namespace-adoption path. Direct registrations are checked by their
/// distinct origin proof in `direct_copy_matches`.
pub(crate) fn reusable(evidence: &ExecutionEvidence, row: &ExecutionRow) -> bool {
    if Warehouse::from_dialect(&row.dialect) != Warehouse::Databricks || ordinary(row) {
        return true;
    }
    if row.clone_source_table.is_none() {
        return false;
    }
    if row.node_sql_hash.is_none() {
        return true;
    }
    row.submit_copy_provenance.as_ref().is_some_and(|p| {
        p.version == 2
            && p.published
            && p.source_epoch > 0
            && p.source_execution_id > 0
            && row
                .clone_source_table
                .as_ref()
                .is_some_and(|s| physical(s) == p.source_physical_key)
    }) && row
        .analyzed_copy_proof
        .as_ref()
        .is_some_and(|p| p.version == 2)
        && row.analyzed_copy_proof == evidence.analyzed_copy_proof
        && evidence.node_namespace_logic_hash.is_some()
        && evidence.node_namespace_logic_hash == row.node_namespace_logic_hash
        && evidence.node_unique_id == row.node_unique_id
}

/// Immutable direct-copy evidence, distinct from automatic namespace identity.
/// Unknown versions and legacy NULL cannot authorize adoption. Source context
/// is retained for the exact rendered refusal diagnostic.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct DirectCopyProvenance {
    pub version: u8,
    #[serde(default)]
    pub published: bool,
    pub identity: String,
    pub default_catalog: String,
    pub default_schema: Option<String>,
    pub source_execution_id: Option<i64>,
    pub source_epoch: Option<i64>,
    pub source_physical_key: Option<String>,
}

/// Frame the shared, once-computed query proof for the distinct direct origin.
pub(crate) fn direct_provenance(
    req: &qc::SqlExecution,
    proof: Option<&crate::analyzed_workload::query::CopyProof>,
) -> Option<DirectCopyProvenance> {
    if !matches!(req.execution_type, 3 | 7) {
        return None;
    }
    let proof = proof.filter(|p| p.version == 2)?;
    Some(DirectCopyProvenance {
        version: 2,
        published: false,
        identity: proof.for_origin("databricks-direct-copy-v2"),
        default_catalog: req.default_catalog.clone(),
        default_schema: req.default_schema.clone(),
        source_execution_id: None,
        source_epoch: None,
        source_physical_key: None,
    })
}

pub(crate) fn direct_copy_matches(
    evidence: &ExecutionEvidence,
    row: &crate::store::ExecutionRow,
) -> bool {
    let (Some(current), Some(recorded)) = (
        &evidence.direct_copy_provenance,
        &row.direct_copy_provenance,
    ) else {
        return false;
    };
    recorded.version == 2
        && current.version == 2
        && recorded.published
        && row.submit_copy_provenance.is_none()
        && row.node_sql_hash.is_none()
        && row
            .analyzed_copy_proof
            .as_ref()
            .is_some_and(|p| p.version == 2)
        && row.analyzed_copy_proof == evidence.analyzed_copy_proof
        && recorded.source_execution_id.is_some_and(|id| id > 0)
        && recorded.source_epoch.is_some_and(|e| e > 0)
        && recorded.source_physical_key.as_ref().is_some_and(|key| {
            row.clone_source_table
                .as_ref()
                .is_some_and(|name| physical(name) == *key)
        })
        && recorded.identity == current.identity
        && row.node_unique_id.is_some()
        && row.node_unique_id == evidence.node_unique_id
        && complete_inputs(&row.input_tables)
}

impl ExecutionEvidence {
    /// A client-requested copy (RegisterClone) of `req.clone_source_table` into
    /// `req.target_table`. It inherits the source's schema-abstracted logic
    /// identity, when the source has one, so later submits reuse the copy once
    /// confirmed. From the source's current build, when it was read
    /// ([`CopySourceBuild`]), it also inherits the runtime, which hosted
    /// reports when it reuses the copy, and the names of the upstreams the
    /// build recorded, for Explain (#93). A copy has no rendered SQL, node
    /// state or recorded inputs of its own.
    pub(crate) fn copy_registration(
        org_id: &str,
        req: &qc::CloneRequest,
        inherited_ns_logic: Option<String>,
        source: Option<&CopySourceBuild>,
        direct: Option<DirectRegistration>,
    ) -> Self {
        let mut evidence = Self::node(
            org_id,
            req.target_table.clone(),
            req.dialect.clone(),
            req.table_namespace.clone(),
            req.execution_type,
            None,
        );
        evidence.node_namespace_logic_hash = inherited_ns_logic;
        evidence.inherited_runtime_ms = source.and_then(|s| s.execution_runtime_ms);
        evidence.clone_source_inputs = source.map(|s| s.input_names.clone());
        // The build the copy is made from (#200, #271): the source's current
        // build, while the request reports the source relation as that build
        // left it, at a known epoch no later than the build's confirmation
        // (the rule for a Target relation as built, AGENTS.md reuse
        // invariant 3), or reports no epoch. The client reads the source's
        // warehouse metadata epoch, slightly before the confirmation
        // (explain_register_clone_input_origin.jsonl: 1795864672228 against
        // the build's 1795864672607); a later epoch is a modification since.
        evidence.clone_source_execution_id = source
            .filter(|s| {
                req.clone_source_last_modified_epoch
                    .is_none_or(|epoch| s.last_modified_epoch.is_some_and(|built| epoch <= built))
            })
            .map(|s| s.execution_id);
        evidence.node_unique_id = req.labels.get("dbt_node_unique_id").cloned();
        evidence.clone_source_table = Some(req.clone_source_table.clone());
        if let Some(direct) = direct {
            evidence.direct_copy_provenance = Some(direct.provenance);
            evidence.analyzed_copy_proof = Some(direct.proof);
            evidence.input_tables = direct.inputs;
            evidence.inherited_runtime_ms = direct.runtime_ms;
        }
        evidence
    }

    /// The same evidence for `copy`, a Result copy into this target, made
    /// from its source's confirmed build (#200).
    pub(crate) fn copied_from(&self, copy: &crate::result_copy::ResultCopy) -> Self {
        // The copy keeps the submit's physical binding: it is unused while
        // the row is a copy (a copy is judged by the build it holds), and
        // describes the fallback build when the copy fails.
        let mut evidence = self.clone();
        evidence.clone_source_table = Some(copy.source_table.clone());
        evidence.clone_source_execution_id = Some(copy.source_execution_id);
        evidence
    }

    pub(crate) fn submit_copied_from(&self, source: &ExecutionRow) -> Self {
        let snapshot = SourceSnapshot::new(source, &source.dialect, &source.target_table);
        let provenance = snapshot.submit_provenance();
        let mut evidence = self.clone();
        evidence.clone_source_table = Some(source.target_table.clone());
        evidence.input_tables = snapshot.inputs;
        evidence.raw_input_tables = None;
        evidence.submit_copy_provenance = Some(provenance);
        evidence
    }
}

/// A client-requested copy (RegisterClone) records NO upstream evidence: it
/// is clone-origin, carries no rendered identity and no inputs. This
/// stand-in set is the submit's CURRENT inputs, each recorded at the copy's
/// own build epoch (or at its current epoch, if earlier), so an upstream
/// counts as drifted exactly when it is newer than the copy (the clone-age
/// anchor). Their producers are unrecorded: a copy is never an upstream
/// query change.
///
/// The copy time proves nothing about the inputs of the rows a copy holds,
/// so a copy is never reused over this set: the submit decision replaces it
/// with the inputs of the build the copy holds, or refuses the copy
/// (`submit_decision::judge_registered_copy`, #271), and Explain reads the
/// inputs the decision judged it with (`ExplainDecisionRow::anchor`). This
/// set only describes a copy the decision did not judge for freshness (one
/// it could not adopt, or whose own Target relation it rejected), and
/// decisions recorded before #271.
///
/// Every other row keeps exactly what it recorded. In particular a copy made
/// by the SQL-submit fallback recorded that submit's inputs; an EMPTY list
/// there means "no upstreams", and a later submit with upstreams must rebuild.
pub(crate) fn adopt_current_inputs(anchor: &mut ExecutionRow, current: &[InputTable]) {
    if registered_without_inputs(anchor) && anchor.input_tables.is_empty() {
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
                read_copy: None,
            })
            .collect();
    }
}

/// A client-requested copy (RegisterClone): clone-origin with no rendered
/// identity of its own. A Result copy made on submit records the submit's
/// identity and is not one.
pub(crate) fn is_copy_registration(row: &ExecutionRow) -> bool {
    row.clone_source_table.is_some() && row.node_sql_hash.is_none()
}

/// A client-requested copy that recorded no inputs of its own: every one but
/// a Databricks direct registration, which froze its source's
/// ([`freeze_direct`]).
pub(crate) fn registered_without_inputs(row: &ExecutionRow) -> bool {
    is_copy_registration(row) && row.direct_copy_provenance.is_none()
}

/// Copy adoption without the execution type: a clone-origin row whose logic
/// identity is the submit's, also read in the copy source's schema for a
/// client-requested copy.
pub(crate) fn copy_logic_matches(
    req: &qc::SubmitEnrichedSqlRequest,
    evidence: &ExecutionEvidence,
    row: &ExecutionRow,
) -> bool {
    if row.dialect == req.dialect
        && row.clone_source_table.is_some()
        && direct_copy_matches(evidence, row)
    {
        return true;
    }
    if crate::warehouse::Warehouse::from_dialect(&req.dialect)
        == crate::warehouse::Warehouse::Databricks
    {
        return row.dialect == req.dialect
            && row.node_sql_hash.is_some()
            && reusable(evidence, row)
            && row.clone_source_table.is_some();
    }
    row.dialect == req.dialect
        && row.clone_source_table.is_some()
        && evidence.node_namespace_logic_hash.is_some()
        && (row.node_namespace_logic_hash == evidence.node_namespace_logic_hash
            || is_copy_registration(row) && rehomed_logic_matches(req, row))
}

/// Whether the submit, with its Target relation's schema read as the copy
/// source's schema, has the schema-abstracted logic the copy inherited from
/// its source build (`analyzed_workload::identity::rehomed_copy_logic_hash`, #93). VERIFIED
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
        && crate::analyzed_workload::identity::rehomed_copy_logic_hash(
            &crate::execution::sql_execution(req),
            &from,
            &to,
        ) == row.node_namespace_logic_hash
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
/// `from` is read in `to`. Two spellings of one relation read the same, so
/// the submit fallback judges a candidate admitted on its reads as a
/// same-upstream equivalent (PR #127 review).
pub(crate) fn canonical_reads(
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

pub(crate) fn qualify_registration(
    req: &qc::CloneRequest,
    source: Option<&ExecutionRow>,
) -> Result<Option<DirectRegistration>, RegisterRefusal> {
    let same_source = source.is_some_and(|source| {
        source.execution_type == req.execution_type
            && source.last_modified_epoch == req.clone_source_last_modified_epoch
            && source.node_unique_id.as_deref()
                == req.labels.get("dbt_node_unique_id").map(String::as_str)
    });
    // The captured native reverse-copy attempt reaches a source that
    // is already a copy, at limit1. Preserve the refusal and report its
    // measured cause; larger limits still gain no chain permission.
    if same_source
        && req.clone_chain_depth_limit == Some(1)
        && req.clone_source_last_modified_epoch.is_some_and(|e| e > 0)
        && source.is_some_and(|s| {
            s.clone_source_table.is_some()
                && s.node_unique_id
                    .as_deref()
                    .is_some_and(|uid| !uid.is_empty())
        })
    {
        return Err(RegisterRefusal::Reject(
            RegisterRejection::ChainLimitExceeded,
        ));
    }
    let eligible = same_source
        && source.is_some_and(|source| ordinary(source) && source.node_sql_hash.is_some());
    if !eligible {
        return Err(RegisterRefusal::Reject(
            RegisterRejection::ForcedNotEligible,
        ));
    }
    let direct = source.and_then(|source| freeze_direct(req, source));
    Ok(direct)
}
