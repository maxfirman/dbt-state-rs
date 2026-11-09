//! Execution evidence and publication shared by Submit, Record and result copies.
//! Keep identity construction here; `submit_decision` selects candidates and writes
//! Submit's pending rows (its selector writes are best effort), decision remains
//! pure, and the gRPC adapter owns wire responses. Record publishes its complete
//! batch and selector state in one transaction.

use crate::query_cache as qc;
use crate::store::{ConfirmedExecution, InputTable, PendingExecution, Store};
use crate::AppState;
use tonic::Status;

/// A blank `table_namespace` is read as absent when scoping State history
/// lookups.
pub(crate) fn namespace(value: Option<&str>) -> Option<&str> {
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

pub(crate) struct ExecutionEvidence(PendingExecution);

impl ExecutionEvidence {
    pub(crate) fn row(&self) -> &PendingExecution {
        &self.0
    }

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
        let mut evidence = Self::node(
            org_id,
            req.target_table.clone().unwrap_or_default(),
            req.dialect.clone(),
            req.table_namespace.clone(),
            req.execution_type,
            req.dbt_node_state.as_ref(),
        );
        let row = &mut evidence.0;
        row.node_unique_id = node_unique_id(req.dbt_node_state.as_ref(), &req.labels);
        row.node_sql_hash = Some(crate::fingerprint::sql_logic_hash(req, None));
        row.node_namespace_logic_hash = copy
            .then(|| crate::fingerprint::copy_logic_hash(req))
            .flatten();
        row.node_template_hash =
            template.map(|body| crate::fingerprint::sql_logic_hash(req, Some(body)));
        row.input_tables = req.tables.iter().map(InputTable::from).collect();
        evidence
    }

    fn node(
        org_id: &str,
        target_table: String,
        dialect: String,
        table_namespace: Option<String>,
        execution_type: i32,
        state: Option<&qc::DbtNodeState>,
    ) -> Self {
        Self(PendingExecution {
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
            request_id: String::new(),
            execution_decision_id: None,
        })
    }

    pub(crate) fn values_submit(org_id: &str, req: &qc::SubmitValuesRequest) -> Self {
        let mut evidence = Self::values_record(
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
        );
        // Submit supports older clients publishing UID only in labels.
        evidence.0.node_unique_id = node_unique_id(req.dbt_node_state.as_ref(), &req.labels);
        evidence
    }

    pub(crate) fn values_record(org_id: &str, req: &qc::ValuesExecution) -> Self {
        let mut evidence = Self::node(
            org_id,
            req.target_table.clone(),
            req.dialect.clone(),
            req.table_namespace.clone(),
            9,
            req.dbt_node_state.as_ref(),
        );
        evidence.0.node_sql_hash = Some(crate::fingerprint::seed_logic_hash(
            &req.dialect,
            &req.default_catalog,
            &req.semantic_extras,
        ));
        evidence.0.values_hash = (!req.values_hash.is_empty()).then(|| req.values_hash.clone());
        evidence
    }

    pub(crate) async fn write_pending(
        &self,
        store: &Store,
        request_id: &str,
        decision_id: &str,
        copy_source: Option<&str>,
    ) -> sqlx::Result<()> {
        let mut pending = self.0.clone();
        pending.request_id = request_id.to_owned();
        pending.execution_decision_id = Some(decision_id.to_owned());
        pending.clone_source_table = copy_source.map(str::to_owned);
        store.insert_pending(&pending).await?;
        Ok(())
    }

    fn confirmed(self, outcome: qc::ExecutionOutcome) -> ConfirmedExecution {
        let p = self.0;
        ConfirmedExecution {
            org_id: p.org_id,
            project_id: p.project_id,
            target_table: p.target_table,
            execution_type: p.execution_type,
            node_hash: p.node_hash,
            node_body_hash: p.node_body_hash,
            node_sql_hash: p.node_sql_hash,
            node_namespace_logic_hash: p.node_namespace_logic_hash,
            node_configs_hash: p.node_configs_hash,
            node_contract_hash: p.node_contract_hash,
            node_unique_id: p.node_unique_id,
            table_namespace: p.table_namespace,
            dialect: p.dialect,
            input_tables: p.input_tables,
            values_hash: p.values_hash,
            request_id: uuid::Uuid::now_v7().to_string(),
            execution_decision_id: Some(uuid::Uuid::now_v7().to_string()),
            node_template_hash: p.node_template_hash,
            last_modified_epoch: outcome.last_modified_epoch,
            table_type: outcome.table_type,
            execution_runtime_ms: outcome.execution_runtime_ms,
            execution_results: outcome.execution_results,
        }
    }
}

pub(crate) async fn register_copy(
    store: &Store,
    org_id: &str,
    req: &qc::CloneRequest,
    inherited_ns_logic: Option<String>,
    request_id: &str,
    execution_decision_id: &str,
) -> sqlx::Result<()> {
    let pending = PendingExecution {
        org_id: org_id.to_owned(),
        project_id: None,
        target_table: req.target_table.clone(),
        execution_type: req.execution_type,
        node_hash: None,
        node_body_hash: None,
        node_sql_hash: None,
        node_namespace_logic_hash: inherited_ns_logic,
        node_configs_hash: None,
        node_contract_hash: None,
        node_unique_id: req.labels.get("dbt_node_unique_id").cloned(),
        table_namespace: req.table_namespace.clone(),
        dialect: req.dialect.clone(),
        input_tables: Vec::new(),
        values_hash: None,
        request_id: request_id.to_owned(),
        execution_decision_id: Some(execution_decision_id.to_owned()),
        clone_source_table: Some(req.clone_source_table.clone()),
        node_template_hash: None,
    };
    store.insert_pending(&pending).await?;
    Ok(())
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

pub(crate) async fn record_batch(
    state: &AppState,
    org_id: &str,
    records: &[qc::ExecutionRecord],
) -> Result<u32, Status> {
    let mut rows: Vec<ConfirmedExecution> = Vec::with_capacity(records.len());
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
        let input = record
            .input
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("ExecutionRecord missing input"))?;

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
    let stored = state
        .store
        .insert_confirmed_batch_with_node_states(&rows, &node_state_upserts)
        .await
        .map_err(store_status)?;

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

/// Map a store error to a gRPC status. The detailed sqlx error (which may name
/// internal schema objects, columns, or connection fragments) is logged
/// server-side; the client receives only a generic message to avoid leaking
/// implementation details over the wire.
pub(crate) fn store_status(e: sqlx::Error) -> Status {
    tracing::error!(error = %e, "store error");
    Status::internal("internal store error")
}
