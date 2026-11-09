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
use crate::store::{InputTable, Store};

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

/// The identity and inputs of one execution, as submitted, recorded or
/// registered as a copy. Reuse candidate selection and Result copy planning
/// read it; [`ExecutionEvidence::pending`] and [`ExecutionEvidence::confirmed`]
/// turn it into the row to write. Only the constructors below build it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExecutionEvidence {
    pub(crate) org_id: String,
    pub(crate) project_id: Option<String>,
    pub(crate) target_table: String,
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
        let body = crate::decision::node_body_hash_of(req);
        Self::sql(
            org_id,
            &sql_execution(req),
            req.compare_unrendered_code
                .then(|| body.as_deref().unwrap_or("")),
            !req.compare_unrendered_code && !req.sql.trim().is_empty(),
        )
    }

    pub(crate) fn sql_record(org_id: &str, req: &qc::SqlExecution) -> Self {
        // Record has no template comparison flag and computes the supplied SQL identity.
        Self::sql(org_id, req, None, true)
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
            input_tables: req.tables.iter().map(InputTable::from).collect(),
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
    /// confirmed. A copy has no rendered SQL, node state or recorded inputs.
    pub(crate) fn copy_registration(
        org_id: &str,
        req: &qc::CloneRequest,
        inherited_ns_logic: Option<String>,
    ) -> Self {
        Self {
            node_namespace_logic_hash: inherited_ns_logic,
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
        let row = evidence.confirmed(outcome);
        rows.push(row);

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
        let row =
            ExecutionEvidence::copy_registration("org", &clone_request(), Some("logic".into()))
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
        let e = ExecutionEvidence::copy_registration("org", &clone_request(), None);

        assert_eq!(e.node_namespace_logic_hash, None);
        assert_eq!(
            e.clone_source_table.as_deref(),
            Some("\"DB\".\"PROD\".\"M\"")
        );
    }
}
