//! Execution evidence and publication shared by Submit, Record and result copies.
//! Keep identity construction here; `submit_decision` selects candidates and writes
//! Submit's pending rows (its selector writes are best effort), decision remains
//! pure, and the gRPC adapter owns wire responses. Record publishes its complete
//! batch and selector state in one transaction.
//!
//! [`ExecutionEvidence`] is the only way to build an [`ExecutionWrite`], the one
//! write type for pending (Submit, RegisterClone) and confirmed
//! (RecordExecutions) rows, so identity fields are set the same way for each.

use crate::query_cache as qc;
use crate::store::{logic_identity, CopySourceBuild, InputTable, Producer, Store};
use std::collections::HashMap;

/// A blank `table_namespace` is read as absent when scoping State history
/// lookups.
pub(crate) fn non_blank_namespace(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

/// Older clients publish identity only in labels.
fn node_unique_id(
    state: Option<&qc::DbtNodeState>,
    labels: &std::collections::HashMap<String, String>,
) -> Option<String> {
    state
        .map(|s| s.node_unique_id.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            labels
                .get("dbt_node_unique_id")
                .map(String::as_str)
                .filter(|s| !s.is_empty())
        })
        .map(str::to_owned)
}

/// Whether `name` is the data test's own audit relation, which a
/// `store_failures` test writes and the client then reports in `tables`
/// (#88). Client 2.0.11 (dbt-fusion `collect_table_modified_infos`) always
/// adds the node's own relation (`node.database()`.`node.schema()`.alias) to
/// `tables` when it exists; a data test's relation is where it stores its
/// failures. Like an incremental's read of its own target, that is the test's
/// own write, not an upstream: hosted neither rebuilds for it nor lists it
/// (VERIFIED LIVE 2026-11-10, explain_data_test_history.jsonl).
///
/// Recognized only from the request: a data test whose semantic extras set
/// `store_failures: true`, and a relation that is exactly
/// `default_catalog`.`default_schema`.`dbt_node_name` under the warehouse's
/// identifier rules (Snowflake only). Any other table still counts, so a
/// changed dependency set still rebuilds. `default_schema` is the test's own
/// schema, also when a schema macro puts it outside the target schema
/// (VERIFIED LIVE 2026-11-11, explain_store_failures_audit_schema.jsonl, #106).
/// A test with a configured alias is not named by the request: no request
/// field carries the alias (explain_store_failures_audit_alias.jsonl). Its
/// audit relation is recognized by elimination instead
/// ([`unnamed_audit_relation`], #123, ADR 0008).
///
/// The request does not carry the alias, so a relation the test reads can
/// have exactly that name (a test `orders` aliased `orders_audit` that reads
/// `orders`). The audit relation is the test's output and never appears in its
/// SQL, so a name the test's SQL or one of its query dependencies mentions is
/// always kept as an input.
fn is_own_audit_relation(req: &qc::SqlExecution, name: &str) -> bool {
    if !stores_failures(req) {
        return false;
    }
    let (Some(schema), Some(test)) = (
        req.default_schema.as_deref(),
        req.labels.get("dbt_node_name"),
    ) else {
        return false;
    };
    !mentioned_by_test(req, test)
        && crate::warehouse::Warehouse::from_dialect(&req.dialect).names_relation(
            name,
            &req.default_catalog,
            schema,
            test,
        )
}

/// The audit relation of a `store_failures` test with a configured alias,
/// recognized by elimination (#123, ADR 0008): the one reported table in the
/// test's own schema (`default_catalog`.`default_schema`, under the
/// warehouse's identifier rules, Snowflake only) that neither the test's SQL
/// nor any of its query dependencies mentions. Client 2.0.11 reports a table
/// only when the test's SQL or a view definition it sends as a query
/// dependency names it, or when it is the node's own relation
/// (dbt-fusion `collect_table_modified_infos`), so that table is the test's
/// own write. Hosted SKIPs the unchanged rerun
/// (explain_store_failures_audit_alias.jsonl entries 9, 10).
///
/// `None` when two or more reported tables qualify (counting one the
/// exact-name rule recognizes): the request is then ambiguous and every
/// table stays an input.
fn unnamed_audit_relation(req: &qc::SqlExecution) -> Option<&str> {
    if !stores_failures(req) {
        return None;
    }
    let schema = req.default_schema.as_deref()?;
    let warehouse = crate::warehouse::Warehouse::from_dialect(&req.dialect);
    let mut qualifying = req.tables.iter().filter(|t| {
        warehouse
            .relation_in_schema(&t.name, &req.default_catalog, schema)
            // An identifier with an embedded quote is spelled doubled in SQL,
            // which the mention check does not read; it stays an input.
            .is_some_and(|id| !id.is_empty() && !id.contains('"') && !mentioned_by_test(req, &id))
    });
    match (qualifying.next(), qualifying.next()) {
        (Some(only), None) => Some(only.name.as_str()),
        _ => None,
    }
}

/// Whether `req` is a data test whose semantic extras set
/// `store_failures: true`.
fn stores_failures(req: &qc::SqlExecution) -> bool {
    req.execution_type == qc::ModelExecutionType::DbtDataTest as i32
        && req
            .semantic_extras
            .get("store_failures")
            .and_then(|v| serde_json::from_str::<bool>(v).ok())
            == Some(true)
}

/// Whether the test's SQL or any of its query dependencies mentions
/// `identifier` ([`mentions_identifier`]).
fn mentioned_by_test(req: &qc::SqlExecution, identifier: &str) -> bool {
    std::iter::once(req.sql.as_str())
        .chain(req.query_dependencies.iter().map(|d| d.query.as_str()))
        .any(|sql| mentions_identifier(sql, identifier))
}

/// The reported tables that are the execution's inputs: all of them but a
/// `store_failures` test's own audit relation (#88, #123).
pub(crate) fn sql_inputs(req: &qc::SqlExecution) -> Vec<InputTable> {
    let unnamed = unnamed_audit_relation(req);
    req.tables
        .iter()
        .filter(|t| !is_own_audit_relation(req, &t.name) && Some(t.name.as_str()) != unnamed)
        .map(InputTable::from)
        .collect()
}

/// Whether `text` contains `identifier` as a whole word, ignoring ASCII case.
/// This over-approximates a reference (a comment or string literal also
/// counts), which only keeps a relation as an input.
fn mentions_identifier(text: &str, identifier: &str) -> bool {
    let is_word = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    let (text, identifier) = (text.to_lowercase(), identifier.to_lowercase());
    !identifier.is_empty()
        && text.match_indices(&identifier).any(|(at, _)| {
            let before = text[..at].chars().next_back();
            let after = text[at + identifier.len()..].chars().next();
            !before.is_some_and(is_word) && !after.is_some_and(is_word)
        })
}

/// The identity and inputs of one execution, as submitted, recorded or
/// registered as a copy. Reuse candidate selection and Result copy planning
/// read it; [`ExecutionEvidence::pending`] and [`ExecutionEvidence::confirmed`]
/// turn it into the row to write. Only the constructors below build it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionEvidence {
    pub(crate) org_id: String,
    pub(crate) project_id: Option<String>,
    pub(crate) target_table: String,
    /// Original SQL target spelling for persistence/deferral/DDL. Decision
    /// evidence uses the Warehouse's physical spelling independently.
    pub(crate) raw_target_table: Option<String>,
    pub(crate) execution_type: i32,
    pub(crate) node_hash: Option<String>,
    pub(crate) node_body_hash: Option<String>,
    pub(crate) node_sql_hash: Option<String>,
    pub(crate) node_namespace_logic_hash: Option<String>,
    /// The unrendered-template logic hash, set only in `compare_unrendered_code`
    /// mode (NULL otherwise). Reuse matches when EITHER `node_sql_hash` (rendered)
    /// OR `node_template_hash` (template) equals the submit's.
    pub(crate) node_template_hash: Option<String>,
    pub(crate) node_configs_hash: Option<String>,
    pub(crate) node_contract_hash: Option<String>,
    pub(crate) node_unique_id: Option<String>,
    pub(crate) table_namespace: Option<String>,
    pub(crate) dialect: String,
    pub(crate) input_tables: Vec<InputTable>,
    pub(crate) values_hash: Option<String>,
    /// Set only for copy-origin rows: the physical source this target is
    /// copied from. NULL for ordinary SQL/seed executions.
    pub(crate) clone_source_table: Option<String>,
    /// A client-requested copy's inherited source build runtime, written on
    /// its pending row ([`ExecutionEvidence::copy_registration`]).
    pub(crate) inherited_runtime_ms: Option<i64>,
    /// A client-requested copy's inherited source build upstream names.
    pub(crate) clone_source_inputs: Option<Vec<String>>,
    sealed: (),
}

/// A row to write to `executions`: pending (Submit, RegisterClone) or
/// confirmed (RecordExecutions). Only [`ExecutionEvidence`] builds one.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionWrite {
    pub(crate) evidence: ExecutionEvidence,
    pub(crate) request_id: String,
    pub(crate) execution_decision_id: String,
    pub(crate) status: WriteStatus,
    sealed: (),
}

/// Whether a written row awaits its ConfirmExecution or carries its outcome.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WriteStatus {
    Pending,
    Confirmed(qc::ExecutionOutcome),
}

impl WriteStatus {
    /// The `executions.status` value.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Confirmed(_) => "confirmed",
        }
    }

    pub(crate) fn outcome(&self) -> Option<&qc::ExecutionOutcome> {
        match self {
            Self::Pending => None,
            Self::Confirmed(outcome) => Some(outcome),
        }
    }
}

impl ExecutionEvidence {
    pub(crate) fn sql_submit(org_id: &str, req: &qc::SubmitEnrichedSqlRequest) -> Self {
        // Template mode retains rendered identity and additionally matches the template.
        let raw_target = req.target_table.clone();
        let req = crate::warehouse::Warehouse::from_dialect(&req.dialect).resolve_submit(req);
        let body = crate::decision::node_body_hash_of(&req);
        let mut evidence = Self::sql(
            org_id,
            &sql_execution(&req),
            req.compare_unrendered_code
                .then(|| body.as_deref().unwrap_or("")),
            !req.compare_unrendered_code && !req.sql.trim().is_empty(),
        );
        evidence.raw_target_table = raw_target;
        evidence
    }

    pub(crate) fn sql_record(org_id: &str, req: &qc::SqlExecution) -> Self {
        // Record has no template comparison flag and computes the supplied SQL identity.
        let raw_target = req.target_table.clone();
        let req = crate::warehouse::Warehouse::from_dialect(&req.dialect).resolve_record(req);
        let mut evidence = Self::sql(org_id, &req, None, true);
        evidence.raw_target_table = raw_target;
        evidence
    }

    fn sql(org_id: &str, req: &qc::SqlExecution, template: Option<&str>, copy: bool) -> Self {
        Self {
            node_unique_id: node_unique_id(req.dbt_node_state.as_ref(), &req.labels),
            node_sql_hash: Some(crate::fingerprint::sql_logic_hash(req, None)),
            node_namespace_logic_hash: copy
                .then(|| crate::fingerprint::copy_logic_hash(req))
                .flatten(),
            node_template_hash: template
                .map(|body| crate::fingerprint::sql_logic_hash(req, Some(body))),
            input_tables: sql_inputs(req),
            ..Self::node(
                org_id,
                req.target_table.clone().unwrap_or_default(),
                req.dialect.clone(),
                req.table_namespace.clone(),
                req.execution_type,
                req.dbt_node_state.as_ref(),
            )
        }
    }

    fn node(
        org_id: &str,
        target_table: String,
        dialect: String,
        table_namespace: Option<String>,
        execution_type: i32,
        state: Option<&qc::DbtNodeState>,
    ) -> Self {
        Self {
            org_id: org_id.to_owned(),
            target_table,
            raw_target_table: None,
            dialect,
            table_namespace,
            execution_type,
            project_id: state.and_then(|s| s.project_id.clone()),
            node_hash: state.map(|s| s.node_hash.clone()),
            node_body_hash: state.and_then(|s| s.node_body_hash.clone()),
            node_configs_hash: state.and_then(|s| s.node_configs_hash.clone()),
            node_contract_hash: state.and_then(|s| s.node_contract_hash.clone()),
            node_unique_id: state.map(|s| s.node_unique_id.clone()),
            node_sql_hash: None,
            node_namespace_logic_hash: None,
            node_template_hash: None,
            input_tables: Vec::new(),
            values_hash: None,
            clone_source_table: None,
            inherited_runtime_ms: None,
            clone_source_inputs: None,
            sealed: (),
        }
    }

    pub(crate) fn values_submit(org_id: &str, req: &qc::SubmitValuesRequest) -> Self {
        Self {
            // Submit supports older clients publishing UID only in labels.
            node_unique_id: node_unique_id(req.dbt_node_state.as_ref(), &req.labels),
            ..Self::values_record(
                org_id,
                &qc::ValuesExecution {
                    target_table: req.target_table.clone(),
                    dialect: req.dialect.clone(),
                    default_catalog: req.default_catalog.clone(),
                    semantic_extras: req.semantic_extras.clone(),
                    values_hash: req.values_hash.clone(),
                    dbt_node_state: req.dbt_node_state.clone(),
                    table_namespace: req.table_namespace.clone(),
                    ..Default::default()
                },
            )
        }
    }

    pub(crate) fn values_record(org_id: &str, req: &qc::ValuesExecution) -> Self {
        Self {
            node_sql_hash: Some(crate::fingerprint::seed_logic_hash(
                &req.dialect,
                &req.default_catalog,
                &req.semantic_extras,
            )),
            values_hash: (!req.values_hash.is_empty()).then(|| req.values_hash.clone()),
            ..Self::node(
                org_id,
                req.target_table.clone(),
                req.dialect.clone(),
                req.table_namespace.clone(),
                qc::ModelExecutionType::Values as i32,
                req.dbt_node_state.as_ref(),
            )
        }
    }

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
    ) -> Self {
        Self {
            node_namespace_logic_hash: inherited_ns_logic,
            inherited_runtime_ms: source.and_then(|s| s.execution_runtime_ms),
            clone_source_inputs: source.map(|s| s.input_names.clone()),
            node_unique_id: req.labels.get("dbt_node_unique_id").cloned(),
            clone_source_table: Some(req.clone_source_table.clone()),
            ..Self::node(
                org_id,
                req.target_table.clone(),
                req.dialect.clone(),
                req.table_namespace.clone(),
                req.execution_type,
                None,
            )
        }
    }

    /// The same evidence for a Result copy of `source_table` into this target.
    pub(crate) fn copied_from(&self, source_table: &str) -> Self {
        Self {
            clone_source_table: Some(source_table.to_owned()),
            ..self.clone()
        }
    }

    /// The pending row for a decision the client will confirm: keyed on the
    /// minted `request_id`, finalized by its ConfirmExecution.
    pub(crate) fn pending(&self, request_id: &str, execution_decision_id: &str) -> ExecutionWrite {
        ExecutionWrite {
            evidence: self.clone(),
            request_id: request_id.to_owned(),
            execution_decision_id: execution_decision_id.to_owned(),
            status: WriteStatus::Pending,
            sealed: (),
        }
    }

    /// A confirmed row carrying its recorded outcome (RecordExecutions). It has
    /// no Submit, so its ids are minted here.
    pub(crate) fn confirmed(self, outcome: qc::ExecutionOutcome) -> ExecutionWrite {
        ExecutionWrite {
            evidence: self,
            request_id: uuid::Uuid::now_v7().to_string(),
            execution_decision_id: uuid::Uuid::now_v7().to_string(),
            status: WriteStatus::Confirmed(outcome),
            sealed: (),
        }
    }
}

/// Build the `NodeStateUpsert` for a node's selector/deferral comparison state,
/// or `None` when the node_state lacks the scope keys (project_id/target_name/
/// node_unique_id) the selector needs. Pure — performs no I/O.
pub(crate) fn node_state_upsert(
    org_id: &str,
    node_state: Option<&qc::DbtNodeState>,
    target_table: Option<&str>,
) -> Option<crate::store::NodeStateUpsert> {
    let ns = node_state?;
    let (project_id, target_name) = (ns.project_id.clone()?, ns.target_name.clone());
    if project_id.is_empty() || target_name.is_empty() || ns.node_unique_id.is_empty() {
        return None;
    }
    Some(crate::store::NodeStateUpsert {
        org_id: org_id.to_string(),
        project_id,
        target_name,
        node_unique_id: ns.node_unique_id.clone(),
        node_body_hash: ns.node_body_hash.clone(),
        node_configs_hash: ns.node_configs_hash.clone(),
        node_persisted_descriptions_hash: ns.node_persisted_descriptions_hash.clone(),
        node_macros_hash: ns.node_macros_hash.clone(),
        node_contract_hash: ns.node_contract_hash.clone(),
        node_database_representation: ns.node_database_representation.clone(),
        // The physical FQN this node was built as in `target_name` — used to
        // resolve deferral (ResolveDeferredRelations). Empty targets (data
        // tests) record NULL and are not deferrable.
        target_table: target_table.filter(|s| !s.is_empty()).map(str::to_owned),
    })
}

/// Why a RecordExecutions batch was not published. Nothing is written either
/// way.
#[derive(Debug)]
pub(crate) enum RecordError {
    /// A record has no `input` (the batch is invalid).
    MissingInput,
    Store(sqlx::Error),
}

/// Publish a RecordExecutions batch: every record's confirmed row and its
/// selector/deferral node state, in one transaction. Returns the rows stored.
pub(crate) async fn record_batch(
    store: &Store,
    org_id: &str,
    records: &[qc::ExecutionRecord],
) -> Result<u32, RecordError> {
    let mut rows: Vec<ExecutionWrite> = Vec::with_capacity(records.len());
    // Collect selector/deferral state upserts to apply in the SAME
    // transaction as the executions insert. Review #2 finding 5: writing
    // node_states per-record inside this loop (before full validation and
    // outside the batch transaction) leaked logical state when a later
    // record was invalid or the batch insert failed, and committing the
    // executions batch BEFORE a best-effort node_states write left a durable
    // execution row with no logical state on a node_states failure. Both are
    // now written atomically (insert_confirmed_batch_with_node_states): a
    // rejected request or any write failure has NO persisted side effect.
    let mut node_state_upserts: Vec<crate::store::NodeStateUpsert> = Vec::new();
    let mut batch: Vec<(ExecutionEvidence, qc::ExecutionOutcome)> =
        Vec::with_capacity(records.len());
    for record in records {
        let outcome = record.outcome.clone().unwrap_or_default();
        let input = record.input.as_ref().ok_or(RecordError::MissingInput)?;

        let evidence = match input {
            qc::execution_record::Input::EnrichedSql(sql) => {
                ExecutionEvidence::sql_record(org_id, sql)
            }
            qc::execution_record::Input::Values(values) => {
                ExecutionEvidence::values_record(org_id, values)
            }
        };
        batch.push((evidence, outcome));

        // Collect each node's comparison state for `state:*` selectors; the
        // actual write happens in the SAME transaction as the executions
        // insert so a rejected/failed batch leaves no logical state behind.
        let node_state = match input {
            qc::execution_record::Input::EnrichedSql(sql) => sql.dbt_node_state.as_ref(),
            qc::execution_record::Input::Values(values) => values.dbt_node_state.as_ref(),
        };
        let node_target = match input {
            qc::execution_record::Input::EnrichedSql(sql) => sql.target_table.as_deref(),
            qc::execution_record::Input::Values(values) => Some(values.target_table.as_str()),
        };
        if let Some(upsert) = node_state_upsert(org_id, node_state, node_target) {
            node_state_upserts.push(upsert);
        }
    }

    // Producers are resolved against State history AND the batch itself: a
    // consumer recorded alongside the upstream build it read records that
    // build, as the batch's builds are not yet in history (ADR 0007).
    let built_in_batch = batch_builds(&batch);
    for (mut evidence, outcome) in batch {
        crate::submit_decision::resolve_producers(store, &mut evidence)
            .await
            .map_err(RecordError::Store)?;
        let scope = BatchScope::of(&evidence);
        let target = evidence.target_table.clone();
        for input in evidence
            .input_tables
            .iter_mut()
            .filter(|t| t.name != target)
        {
            if let Some(builds) = built_in_batch.get(&(input.name.clone(), scope.clone())) {
                input.producer = batch_producer(builds, input.last_modified_epoch);
            }
        }
        rows.push(evidence.confirmed(outcome));
    }

    // Write the executions batch AND the selector/deferral node_states in a
    // SINGLE transaction (review #2 finding 5, full atomicity): a successful
    // RecordExecutions publishes execution and logical state consistently,
    // and a node_states write failure rolls the WHOLE request back (no
    // partial publish, no durable execution row without its logical state).
    let stored = store
        .insert_confirmed_batch_with_node_states(&rows, &node_state_upserts)
        .await
        .map_err(RecordError::Store)?;

    Ok(stored)
}

/// The physical scope a producer is resolved in: connection namespace and
/// warehouse family (the organization is the batch's).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BatchScope {
    table_namespace: Option<String>,
    warehouse_family: String,
}

impl BatchScope {
    fn of(evidence: &ExecutionEvidence) -> Self {
        Self {
            table_namespace: non_blank_namespace(evidence.table_namespace.as_deref())
                .map(str::to_owned),
            warehouse_family: crate::warehouse::Warehouse::from_dialect(&evidence.dialect)
                .family_key(&evidence.dialect),
        }
    }
}

/// A relation's builds in a batch: logic identity and build epoch, in record
/// order.
type BatchBuilds = Vec<(String, Option<i64>)>;

/// Each relation built in a batch, by name and scope: the logic identity and
/// build epoch of every record of it, in record order.
fn batch_builds(
    batch: &[(ExecutionEvidence, qc::ExecutionOutcome)],
) -> HashMap<(String, BatchScope), BatchBuilds> {
    let mut builds: HashMap<_, Vec<_>> = HashMap::new();
    for (e, outcome) in batch.iter().filter(|(e, _)| !e.target_table.is_empty()) {
        let Some(identity) = logic_identity(
            e.node_sql_hash.as_deref(),
            e.node_namespace_logic_hash.as_deref(),
            e.values_hash.as_deref(),
        ) else {
            continue;
        };
        builds
            .entry((e.target_table.clone(), BatchScope::of(e)))
            .or_default()
            .push((identity, outcome.last_modified_epoch));
    }
    builds
}

/// Which of a relation's builds in the batch produced the contents a consumer
/// read at `read`: the earliest build not older than the read (the last
/// record among equal epochs). None of them when the read is later than every
/// build, or unknown: the contents were modified outside dbt, as a later
/// submit would also conclude ([`Producer::of`]).
fn batch_producer(builds: &[(String, Option<i64>)], read: Option<i64>) -> Producer {
    let Some(read) = read else {
        return Producer::NoBuild;
    };
    builds
        .iter()
        .rev()
        .filter_map(|(identity, built)| built.filter(|b| *b >= read).map(|b| (b, identity)))
        .min_by_key(|(built, _)| *built)
        .map_or(Producer::NoBuild, |(_, identity)| {
            Producer::Build(identity.clone())
        })
}

/// Convert a `SubmitEnrichedSqlRequest` into the `SqlExecution` the fingerprint
/// functions consume. Exposed so an upgrade regression can compute a frozen
/// prior-version fingerprint for the exact request (review #2 9th follow-up P2).
pub fn sql_execution(req: &qc::SubmitEnrichedSqlRequest) -> qc::SqlExecution {
    qc::SqlExecution {
        target_table: req.target_table.clone(),
        dialect: req.dialect.clone(),
        default_catalog: req.default_catalog.clone(),
        default_schema: req.default_schema.clone(),
        execution_type: req.execution_type,
        sql: req.sql.clone(),
        tables: req.tables.clone(),
        query_dependencies: req.query_dependencies.clone(),
        semantic_extras: req.semantic_extras.clone(),
        labels: req.labels.clone(),
        dbt_node_state: req.dbt_node_state.clone(),
        table_namespace: req.table_namespace.clone(),
        from_speculative_submit: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_cache::ModelExecutionType;

    const TARGET: &str = "\"DB\".\"DEV\".\"M\"";
    const UPSTREAM: &str = "\"DB\".\"DEV\".\"UP\"";

    fn node_state() -> qc::DbtNodeState {
        qc::DbtNodeState {
            node_unique_id: "model.p.m".into(),
            target_name: "dev".into(),
            node_hash: "node".into(),
            node_body_hash: Some("body".into()),
            node_configs_hash: Some("configs".into()),
            node_contract_hash: Some("contract".into()),
            project_id: Some("project".into()),
            ..Default::default()
        }
    }

    /// A Snowflake model submission with one upstream input.
    fn sql_submit() -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            target_table: Some(TARGET.into()),
            dialect: "snowflake".into(),
            default_catalog: "DB".into(),
            default_schema: Some("DEV".into()),
            execution_type: ModelExecutionType::Full as i32,
            sql: "select * from \"DB\".\"DEV\".\"UP\"".into(),
            tables: vec![qc::TableModifiedInfo {
                name: UPSTREAM.into(),
                last_modified_epoch: Some(500),
            }],
            dbt_node_state: Some(node_state()),
            table_namespace: Some("ns".into()),
            ..Default::default()
        }
    }

    #[test]
    fn sql_submit_pending_row_carries_the_submitted_identity() {
        let row = ExecutionEvidence::sql_submit("org", &sql_submit()).pending("req-1", "dec-1");

        assert_eq!(row.request_id, "req-1");
        assert_eq!(row.execution_decision_id, "dec-1");
        assert_eq!(row.status, WriteStatus::Pending);
        let e = &row.evidence;
        assert_eq!(e.org_id, "org");
        assert_eq!(e.target_table, TARGET);
        assert_eq!(e.dialect, "snowflake");
        assert_eq!(e.table_namespace.as_deref(), Some("ns"));
        assert_eq!(e.execution_type, ModelExecutionType::Full as i32);
        assert_eq!(e.project_id.as_deref(), Some("project"));
        assert_eq!(e.node_unique_id.as_deref(), Some("model.p.m"));
        assert_eq!(e.node_hash.as_deref(), Some("node"));
        assert_eq!(e.node_body_hash.as_deref(), Some("body"));
        assert_eq!(e.node_configs_hash.as_deref(), Some("configs"));
        assert_eq!(e.node_contract_hash.as_deref(), Some("contract"));
        assert!(e.node_sql_hash.is_some(), "rendered SQL identity");
        assert!(
            e.node_namespace_logic_hash.is_some(),
            "Snowflake records the schema-abstracted logic identity"
        );
        assert_eq!(e.node_template_hash, None, "no template comparison");
        assert_eq!(
            e.input_tables,
            vec![InputTable {
                name: UPSTREAM.into(),
                last_modified_epoch: Some(500),
                ..Default::default()
            }]
        );
        assert_eq!(e.values_hash, None);
        assert_eq!(e.clone_source_table, None);
    }

    #[test]
    fn sql_submit_in_template_mode_records_the_template_identity_and_no_copy_identity() {
        let mut req = sql_submit();
        req.compare_unrendered_code = true;
        let e = ExecutionEvidence::sql_submit("org", &req);

        assert!(e.node_sql_hash.is_some(), "rendered identity is retained");
        assert!(e.node_template_hash.is_some());
        assert_ne!(e.node_template_hash, e.node_sql_hash);
        assert_eq!(e.node_namespace_logic_hash, None);
    }

    #[test]
    fn sql_submit_reads_the_unique_id_from_labels_for_older_clients() {
        let mut req = sql_submit();
        req.dbt_node_state = None;
        req.labels
            .insert("dbt_node_unique_id".into(), "model.p.legacy".into());
        let e = ExecutionEvidence::sql_submit("org", &req);

        assert_eq!(e.node_unique_id.as_deref(), Some("model.p.legacy"));
        assert_eq!(e.project_id, None);
        assert_eq!(e.node_hash, None);
    }

    const SOURCE: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T75SRC\"";
    const AUDIT: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T75S\"";

    /// A `store_failures` singular data test as client 2.0.11 submits it once
    /// its audit relation exists (explain_data_test_history.jsonl entry 18):
    /// no target, the test's database and schema as the defaults, and the
    /// audit relation reported next to the tested source.
    fn store_failures_test(tables: &[&str]) -> qc::SubmitEnrichedSqlRequest {
        qc::SubmitEnrichedSqlRequest {
            target_table: None,
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            default_schema: Some("prod_state_demo".into()),
            execution_type: ModelExecutionType::DbtDataTest as i32,
            sql: "select id from CDO_DEMO_DB.prod_state_demo.t75src where id < 0".into(),
            tables: tables
                .iter()
                .map(|name| qc::TableModifiedInfo {
                    name: (*name).into(),
                    last_modified_epoch: Some(500),
                })
                .collect(),
            semantic_extras: [
                ("store_failures".to_string(), "true".to_string()),
                ("store_failures_as".to_string(), "\"table\"".to_string()),
            ]
            .into(),
            labels: [("dbt_node_name".to_string(), "t75s".to_string())].into(),
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: "test.p.t75s".into(),
                ..node_state()
            }),
            ..Default::default()
        }
    }

    fn input_names(e: &ExecutionEvidence) -> Vec<&str> {
        e.input_tables.iter().map(|t| t.name.as_str()).collect()
    }

    /// The audit relation is the test's own write, not something it reads
    /// (#88): neither the submitted nor the recorded evidence lists it.
    #[test]
    fn store_failures_test_audit_relation_is_not_an_input() {
        let req = store_failures_test(&[SOURCE, AUDIT]);
        let submitted = ExecutionEvidence::sql_submit("org", &req);
        assert_eq!(input_names(&submitted), vec![SOURCE]);
        let recorded = ExecutionEvidence::sql_record("org", &sql_execution(&req));
        assert_eq!(input_names(&recorded), vec![SOURCE]);
    }

    /// Unquoted identifiers fold to upper case on Snowflake, so the test's
    /// lower-case name and schema name the upper-case quoted relation; an
    /// unquoted reported name folds the same way.
    #[test]
    fn audit_relation_matches_under_snowflake_identifier_folding() {
        let unquoted = "cdo_demo_db.prod_state_demo.t75s";
        let req = store_failures_test(&[SOURCE, unquoted]);
        assert_eq!(
            input_names(&ExecutionEvidence::sql_submit("org", &req)),
            vec![SOURCE]
        );
    }

    /// Only the test's own relation is excluded by the exact-name rule (#88).
    /// The same name in another schema or database, or the same name for a
    /// test that does not store its failures, a model or another warehouse,
    /// still counts as an input. A lookalike in the test's own schema is the
    /// unnamed-table rule's case (#123, tests below).
    #[test]
    fn relations_resembling_the_audit_relation_still_count() {
        let lookalikes = [
            "\"CDO_DEMO_DB\".\"OTHER_SCHEMA\".\"T75S\"",
            "\"OTHER_DB\".\"PROD_STATE_DEMO\".\"T75S\"",
            "\"PROD_STATE_DEMO\".\"T75S\"",
            "\"X\".\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T75S\"",
        ];
        for name in lookalikes {
            let e = ExecutionEvidence::sql_submit("org", &store_failures_test(&[SOURCE, name]));
            assert_eq!(input_names(&e), vec![SOURCE, name], "{name}");
        }

        type Edit = fn(&mut qc::SubmitEnrichedSqlRequest);
        let variants: [(&str, Edit); 5] = [
            ("store_failures false", |r| {
                r.semantic_extras
                    .insert("store_failures".into(), "false".into());
            }),
            ("store_failures unset", |r| {
                r.semantic_extras.remove("store_failures");
            }),
            ("a model", |r| {
                r.execution_type = ModelExecutionType::Full as i32;
            }),
            ("another warehouse", |r| r.dialect = "databricks".into()),
            ("no default schema", |r| r.default_schema = None),
        ];
        for (what, edit) in variants {
            let mut req = store_failures_test(&[SOURCE, AUDIT]);
            edit(&mut req);
            let e = ExecutionEvidence::sql_submit("org", &req);
            assert_eq!(input_names(&e), vec![SOURCE, AUDIT], "{what}");
        }

        // Without the test's name the exact rule cannot match, so its audit
        // relation is only an unnamed table in its schema (#123).
        let mut req = store_failures_test(&[SOURCE, AUDIT]);
        req.labels.remove("dbt_node_name");
        assert_eq!(inputs_of(&req), [SOURCE]);
    }

    /// The audit relation's name is the test's database, schema and name only
    /// when no alias is configured, and the request does not carry the alias.
    /// A same-named relation the test reads, directly or through a view it
    /// reads, is therefore a genuine input and stays one.
    #[test]
    fn a_relation_the_test_reads_is_never_its_audit_relation() {
        // Codex review on #109: a test named `orders` with alias
        // `orders_audit` that reads `orders` reports the upstream under
        // exactly the name the audit relation would have without the alias.
        let upstream = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ORDERS\"";
        let reads = [
            "select id from CDO_DEMO_DB.prod_state_demo.orders where id < 0",
            "select id from \"ORDERS\" where id < 0",
            "select id from prod_state_demo.Orders where id < 0",
        ];
        for sql in reads {
            let mut req = store_failures_test(&[upstream]);
            req.labels.insert("dbt_node_name".into(), "orders".into());
            req.sql = sql.into();
            let e = ExecutionEvidence::sql_submit("org", &req);
            assert_eq!(input_names(&e), vec![upstream], "{sql}");
            let recorded = ExecutionEvidence::sql_record("org", &sql_execution(&req));
            assert_eq!(input_names(&recorded), vec![upstream], "{sql}");
        }

        // Read through a view: the client reports the view's upstream in
        // `tables` and the view's definition as a query dependency.
        let mut req = store_failures_test(&[upstream]);
        req.labels.insert("dbt_node_name".into(), "orders".into());
        req.sql = "select id from CDO_DEMO_DB.prod_state_demo.orders_v where id < 0".into();
        req.query_dependencies = vec![qc::QueryDependency {
            name: "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ORDERS_V\"".into(),
            query: "select * from CDO_DEMO_DB.prod_state_demo.orders".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            default_schema: "prod_state_demo".into(),
        }];
        assert_eq!(
            input_names(&ExecutionEvidence::sql_submit("org", &req)),
            vec![upstream],
            "read through a view"
        );

        // The name only as part of a longer identifier is not a read.
        let mut req = store_failures_test(&[SOURCE, AUDIT]);
        req.sql = "select id from CDO_DEMO_DB.prod_state_demo.t75src where t75s_id < 0".into();
        assert_eq!(
            input_names(&ExecutionEvidence::sql_submit("org", &req)),
            vec![SOURCE]
        );
    }

    /// The test's schema when a schema macro puts its audit relation outside
    /// the target schema (dbt's default `<target>_dbt_test__audit`).
    const AUDIT_SCHEMA: &str = "prod_state_demo_dbt_test__audit";
    const SEPARATE_AUDIT: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO_DBT_TEST__AUDIT\".\"T75S\"";

    /// Client 2.0.11 sends the test's own schema as `default_schema`, also when
    /// that is a separate audit schema (#106,
    /// explain_store_failures_audit_schema.jsonl entries 10 and 11), so the
    /// audit relation there is named exactly by the request.
    #[test]
    fn audit_relation_in_a_separate_audit_schema_is_not_an_input() {
        let mut req = store_failures_test(&[SOURCE, SEPARATE_AUDIT]);
        req.default_schema = Some(AUDIT_SCHEMA.into());
        assert_eq!(
            input_names(&ExecutionEvidence::sql_submit("org", &req)),
            vec![SOURCE]
        );
        assert_eq!(
            input_names(&ExecutionEvidence::sql_record("org", &sql_execution(&req))),
            vec![SOURCE]
        );
    }

    /// With the audit schema as `default_schema`, the test's name in any other
    /// schema (the target schema included) is not its audit relation, and a
    /// relation of its name in the audit schema that its SQL reads stays an
    /// input.
    #[test]
    fn with_a_separate_audit_schema_lookalikes_and_reads_still_count() {
        let mut req = store_failures_test(&[SOURCE, AUDIT]);
        req.default_schema = Some(AUDIT_SCHEMA.into());
        assert_eq!(
            input_names(&ExecutionEvidence::sql_submit("org", &req)),
            vec![SOURCE, AUDIT]
        );

        let mut req = store_failures_test(&[SOURCE, SEPARATE_AUDIT]);
        req.default_schema = Some(AUDIT_SCHEMA.into());
        req.sql = "select id from CDO_DEMO_DB.prod_state_demo_dbt_test__audit.t75s".into();
        assert_eq!(
            input_names(&ExecutionEvidence::sql_submit("org", &req)),
            vec![SOURCE, SEPARATE_AUDIT]
        );
    }

    /// The audit relation under a configured alias (`<test>_aud`), as client
    /// 2.0.11 reports it (explain_store_failures_audit_alias.jsonl entry 9).
    const ALIASED: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T75S_AUD\"";

    /// Input names of the submitted and the recorded evidence, which must
    /// agree.
    fn inputs_of(req: &qc::SubmitEnrichedSqlRequest) -> Vec<String> {
        let submitted = ExecutionEvidence::sql_submit("org", req);
        let recorded = ExecutionEvidence::sql_record("org", &sql_execution(req));
        assert_eq!(input_names(&submitted), input_names(&recorded));
        input_names(&submitted)
            .into_iter()
            .map(Into::into)
            .collect()
    }

    /// A configured `alias` names the audit relation, and no request field
    /// carries it (#106, explain_store_failures_audit_alias.jsonl). The one
    /// reported table in the test's own schema that neither its SQL nor a
    /// query dependency mentions is its audit relation, and is not an input
    /// (#123, ADR 0008), so the unchanged rerun SKIPs as hosted does.
    #[test]
    fn an_aliased_audit_relation_is_not_an_input() {
        assert_eq!(
            inputs_of(&store_failures_test(&[SOURCE, ALIASED])),
            [SOURCE]
        );
        // Snowflake identifier folding, as for the #88 rule.
        let unquoted = "cdo_demo_db.prod_state_demo.t75s_aud";
        assert_eq!(
            inputs_of(&store_failures_test(&[SOURCE, unquoted])),
            [SOURCE]
        );
        // The audit relation is reported whichever kind it is.
        let mut view = store_failures_test(&[SOURCE, ALIASED]);
        view.semantic_extras
            .insert("store_failures_as".into(), "\"view\"".into());
        assert_eq!(inputs_of(&view), [SOURCE]);
    }

    /// Two or more reported tables qualify: the request is ambiguous, so none
    /// is dropped. That includes a relation named exactly as the #88 audit
    /// relation, which that rule still drops.
    #[test]
    fn two_unnamed_tables_in_the_test_schema_are_both_kept() {
        let other = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T75S_OTHER\"";
        assert_eq!(
            inputs_of(&store_failures_test(&[SOURCE, ALIASED, other])),
            [SOURCE, ALIASED, other]
        );
        assert_eq!(
            inputs_of(&store_failures_test(&[SOURCE, AUDIT, ALIASED])),
            [SOURCE, ALIASED],
            "the exact audit relation is dropped, the other unnamed table kept"
        );
        assert_eq!(
            inputs_of(&store_failures_test(&[SOURCE, ALIASED, ALIASED])),
            [SOURCE, ALIASED, ALIASED],
            "a table reported twice is two qualifying entries"
        );
    }

    /// Only a table in the test's own schema can be its audit relation.
    #[test]
    fn an_unnamed_table_in_another_schema_is_kept() {
        for name in [
            "\"CDO_DEMO_DB\".\"OTHER_SCHEMA\".\"T75S_AUD\"",
            "\"OTHER_DB\".\"PROD_STATE_DEMO\".\"T75S_AUD\"",
            "\"PROD_STATE_DEMO\".\"T75S_AUD\"",
            "\"X\".\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T75S_AUD\"",
            "\"CDO_DEMO_DB\".\"prod_state_demo\".\"T75S_AUD\"",
        ] {
            assert_eq!(
                inputs_of(&store_failures_test(&[SOURCE, name])),
                [SOURCE, name],
                "{name}"
            );
        }
        let mut no_schema = store_failures_test(&[SOURCE, ALIASED]);
        no_schema.default_schema = None;
        assert_eq!(inputs_of(&no_schema), [SOURCE, ALIASED]);
    }

    /// A table the test's SQL or one of its query dependencies mentions is
    /// something the test reads, never its audit relation.
    #[test]
    fn an_unnamed_table_rule_keeps_a_table_the_test_names() {
        let named = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T75S_AUD\"";
        for sql in [
            "select id from t75s_aud where id < 0",
            "select id from \"T75S_AUD\" where id < 0",
            "select id from prod_state_demo.T75S_Aud where id < 0",
            "select id from CDO_DEMO_DB.\"PROD_STATE_DEMO\".\"T75S_AUD\" where id < 0",
        ] {
            let mut req = store_failures_test(&[named]);
            req.sql = sql.into();
            assert_eq!(inputs_of(&req), [named], "{sql}");
        }

        // Read through a view: the client reports the view's upstream in
        // `tables` and the view's definition as a query dependency.
        let mut req = store_failures_test(&[named]);
        req.sql = "select id from CDO_DEMO_DB.prod_state_demo.t75s_v where id < 0".into();
        req.query_dependencies = vec![qc::QueryDependency {
            name: "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T75S_V\"".into(),
            query: "select * from \"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T75S_AUD\"".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            default_schema: "prod_state_demo".into(),
        }];
        assert_eq!(inputs_of(&req), [named], "read through a view");
    }

    /// The rule applies only to a Snowflake data test that stores its
    /// failures.
    #[test]
    fn an_unnamed_table_is_kept_outside_a_snowflake_store_failures_test() {
        type Edit = fn(&mut qc::SubmitEnrichedSqlRequest);
        let variants: [(&str, Edit); 5] = [
            ("store_failures false", |r| {
                r.semantic_extras
                    .insert("store_failures".into(), "false".into());
            }),
            ("store_failures unset", |r| {
                r.semantic_extras.remove("store_failures");
            }),
            ("a model", |r| {
                r.execution_type = ModelExecutionType::Full as i32;
                r.target_table = Some("\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"M\"".into());
            }),
            ("DuckDB", |r| {
                r.dialect = "duckdb".into();
                r.default_catalog = "warehouse".into();
                r.default_schema = Some("main".into());
                r.tables = [
                    "\"warehouse\".\"main\".\"src\"",
                    "\"warehouse\".\"main\".\"t75s_aud\"",
                ]
                .iter()
                .map(|name| qc::TableModifiedInfo {
                    name: (*name).into(),
                    last_modified_epoch: Some(500),
                })
                .collect();
                r.sql = "select id from \"warehouse\".\"main\".\"src\" where id < 0".into();
            }),
            ("another warehouse", |r| r.dialect = "databricks".into()),
        ];
        for (what, edit) in variants {
            let mut req = store_failures_test(&[SOURCE, ALIASED]);
            edit(&mut req);
            let expected: Vec<String> = req.tables.iter().map(|t| t.name.clone()).collect();
            assert_eq!(inputs_of(&req), expected, "{what}");
        }
    }

    fn outcome() -> qc::ExecutionOutcome {
        qc::ExecutionOutcome {
            last_modified_epoch: Some(1_000),
            table_type: Some("TABLE".into()),
            execution_results: None,
            execution_runtime_ms: Some(42),
        }
    }

    #[test]
    fn sql_record_confirmed_row_has_the_submit_identity_and_its_outcome() {
        let submit = sql_submit();
        let row =
            ExecutionEvidence::sql_record("org", &sql_execution(&submit)).confirmed(outcome());

        assert_eq!(row.status, WriteStatus::Confirmed(outcome()));
        assert!(!row.request_id.is_empty());
        assert!(!row.execution_decision_id.is_empty());
        assert_ne!(row.request_id, row.execution_decision_id);
        let e = &row.evidence;
        assert_eq!(e.target_table, TARGET);
        assert_eq!(e.execution_type, ModelExecutionType::Full as i32);
        assert_eq!(e.node_unique_id.as_deref(), Some("model.p.m"));
        assert_eq!(e.project_id.as_deref(), Some("project"));
        assert_eq!(e.input_tables.len(), 1);
        assert_eq!(e.clone_source_table, None);
        // Recording a build is the same evidence as submitting it, so a later
        // submit of the same node reuses it.
        assert_eq!(*e, ExecutionEvidence::sql_submit("org", &submit));
    }

    #[test]
    fn sql_record_with_empty_sql_still_records_its_copy_identity() {
        // Record computes the supplied SQL identity; only Submit drops the copy
        // identity for an empty body.
        let mut submit = sql_submit();
        submit.sql = String::new();
        let recorded = ExecutionEvidence::sql_record("org", &sql_execution(&submit));
        let submitted = ExecutionEvidence::sql_submit("org", &submit);

        assert!(recorded.node_namespace_logic_hash.is_some());
        assert_eq!(submitted.node_namespace_logic_hash, None);
    }

    fn values_submit() -> qc::SubmitValuesRequest {
        qc::SubmitValuesRequest {
            target_table: "\"DB\".\"DEV\".\"SEED\"".into(),
            dialect: "snowflake".into(),
            default_catalog: "DB".into(),
            values_hash: "values".into(),
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: "seed.p.s".into(),
                ..node_state()
            }),
            table_namespace: Some("ns".into()),
            ..Default::default()
        }
    }

    fn values_record(req: &qc::SubmitValuesRequest) -> qc::ValuesExecution {
        qc::ValuesExecution {
            target_table: req.target_table.clone(),
            dialect: req.dialect.clone(),
            default_catalog: req.default_catalog.clone(),
            values_hash: req.values_hash.clone(),
            semantic_extras: req.semantic_extras.clone(),
            labels: req.labels.clone(),
            dbt_node_state: req.dbt_node_state.clone(),
            table_namespace: req.table_namespace.clone(),
        }
    }

    #[test]
    fn values_submit_pending_row_carries_the_seed_identity() {
        let row =
            ExecutionEvidence::values_submit("org", &values_submit()).pending("req-1", "dec-1");

        assert_eq!(row.request_id, "req-1");
        assert_eq!(row.execution_decision_id, "dec-1");
        assert_eq!(row.status, WriteStatus::Pending);
        let e = &row.evidence;
        assert_eq!(e.org_id, "org");
        assert_eq!(e.target_table, "\"DB\".\"DEV\".\"SEED\"");
        assert_eq!(e.execution_type, ModelExecutionType::Values as i32);
        assert_eq!(e.dialect, "snowflake");
        assert_eq!(e.table_namespace.as_deref(), Some("ns"));
        assert_eq!(e.values_hash.as_deref(), Some("values"));
        assert!(e.node_sql_hash.is_some(), "seed load identity");
        assert_eq!(e.node_namespace_logic_hash, None);
        assert_eq!(e.node_template_hash, None);
        assert_eq!(e.node_unique_id.as_deref(), Some("seed.p.s"));
        assert_eq!(e.project_id.as_deref(), Some("project"));
        assert_eq!(e.node_body_hash.as_deref(), Some("body"));
        assert!(e.input_tables.is_empty());
        assert_eq!(e.clone_source_table, None);
    }

    #[test]
    fn values_submit_reads_the_unique_id_from_labels_for_older_clients() {
        let mut req = values_submit();
        req.dbt_node_state = None;
        req.labels
            .insert("dbt_node_unique_id".into(), "seed.p.legacy".into());

        let submitted = ExecutionEvidence::values_submit("org", &req);
        assert_eq!(submitted.node_unique_id.as_deref(), Some("seed.p.legacy"));
        // Record takes the unique id from the node state only.
        let recorded = ExecutionEvidence::values_record("org", &values_record(&req));
        assert_eq!(recorded.node_unique_id, None);
    }

    #[test]
    fn values_record_confirmed_row_has_the_submit_identity_and_its_outcome() {
        let submit = values_submit();
        let row =
            ExecutionEvidence::values_record("org", &values_record(&submit)).confirmed(outcome());

        assert_eq!(row.status, WriteStatus::Confirmed(outcome()));
        assert!(!row.request_id.is_empty());
        assert_ne!(row.request_id, row.execution_decision_id);
        let e = &row.evidence;
        assert_eq!(e.execution_type, ModelExecutionType::Values as i32);
        assert_eq!(e.values_hash.as_deref(), Some("values"));
        assert_eq!(e.node_unique_id.as_deref(), Some("seed.p.s"));
        assert_eq!(*e, ExecutionEvidence::values_submit("org", &submit));
    }

    #[test]
    fn values_with_an_empty_values_hash_record_no_values_identity() {
        let mut req = values_submit();
        req.values_hash = String::new();

        assert_eq!(
            ExecutionEvidence::values_submit("org", &req).values_hash,
            None
        );
        assert_eq!(
            ExecutionEvidence::values_record("org", &values_record(&req)).values_hash,
            None
        );
    }

    fn clone_request() -> qc::CloneRequest {
        qc::CloneRequest {
            target_table: TARGET.into(),
            dialect: "snowflake".into(),
            default_catalog: "DB".into(),
            execution_type: ModelExecutionType::Full as i32,
            clone_source_table: "\"DB\".\"PROD\".\"M\"".into(),
            labels: [("dbt_node_unique_id".to_string(), "model.p.m".to_string())].into(),
            table_namespace: Some("ns".into()),
            ..Default::default()
        }
    }

    #[test]
    fn copy_registration_pending_row_inherits_the_logic_identity_and_records_the_source() {
        let source = CopySourceBuild {
            node_namespace_logic_hash: Some("logic".into()),
            execution_runtime_ms: Some(3826),
            input_names: vec!["\"DB\".\"PROD\".\"UP\"".into()],
        };
        let row = ExecutionEvidence::copy_registration(
            "org",
            &clone_request(),
            Some("logic".into()),
            Some(&source),
        )
        .pending("req-1", "dec-1");

        assert_eq!(row.request_id, "req-1");
        assert_eq!(row.execution_decision_id, "dec-1");
        assert_eq!(row.status, WriteStatus::Pending);
        let e = &row.evidence;
        assert_eq!(e.org_id, "org");
        assert_eq!(e.target_table, TARGET);
        assert_eq!(e.dialect, "snowflake");
        assert_eq!(e.table_namespace.as_deref(), Some("ns"));
        assert_eq!(e.execution_type, ModelExecutionType::Full as i32);
        assert_eq!(e.node_namespace_logic_hash.as_deref(), Some("logic"));
        assert_eq!(
            e.clone_source_table.as_deref(),
            Some("\"DB\".\"PROD\".\"M\"")
        );
        // Hosted reports the source build's runtime when it reuses the copy (#93).
        assert_eq!(e.inherited_runtime_ms, Some(3826));
        assert_eq!(
            e.clone_source_inputs.as_deref(),
            Some(&["\"DB\".\"PROD\".\"UP\"".to_string()][..])
        );
        assert_eq!(e.node_unique_id.as_deref(), Some("model.p.m"));
        // A copy has no rendered SQL, node state, recorded inputs or values.
        assert_eq!(e.node_sql_hash, None);
        assert_eq!(e.node_template_hash, None);
        assert_eq!(e.node_body_hash, None);
        assert_eq!(e.node_hash, None);
        assert_eq!(e.project_id, None);
        assert_eq!(e.node_configs_hash, None);
        assert_eq!(e.node_contract_hash, None);
        assert!(e.input_tables.is_empty());
        assert_eq!(e.values_hash, None);
    }

    #[test]
    fn copy_registration_without_source_logic_inherits_nothing() {
        let e = ExecutionEvidence::copy_registration("org", &clone_request(), None, None);

        assert_eq!(e.node_namespace_logic_hash, None);
        assert_eq!(e.inherited_runtime_ms, None);
        assert_eq!(e.clone_source_inputs, None);
        assert_eq!(
            e.clone_source_table.as_deref(),
            Some("\"DB\".\"PROD\".\"M\"")
        );
    }
}
