//! Postgres-backed state store.

use crate::execution::ExecutionWrite;
use crate::query_cache as qc;
use prost::Message;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

mod history;

/// One upstream input table's freshness, as sent in SubmitEnrichedSQLRequest.tables.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputTable {
    pub name: String,
    pub last_modified_epoch: Option<i64>,
}

impl From<&qc::TableModifiedInfo> for InputTable {
    fn from(t: &qc::TableModifiedInfo) -> Self {
        Self {
            name: t.name.clone(),
            last_modified_epoch: t.last_modified_epoch,
        }
    }
}

/// A stored execution: the one read type for the `executions` table. State
/// history reads, reuse candidate selection and the Explain anchor all read
/// rows through [`ExecutionRow::COLUMNS`] and its `FromRow` mapping, so a new
/// column is added in one place.
///
/// Recorded inputs and cached results are decoded when the row is read, but a
/// decode failure stays on the row instead of failing the read: corrupt
/// evidence on a row that is never served cannot fail a request. A caller that
/// relies on that evidence takes the row through [`ExecutionRow::into_checked`].
/// Fields that failed to decode read as empty.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecutionRow {
    pub id: i64,
    pub org_id: String,
    pub target_table: String,
    pub execution_type: i32,
    pub node_body_hash: Option<String>,
    pub node_sql_hash: Option<String>,
    pub node_namespace_logic_hash: Option<String>,
    /// Unrendered-template logic hash (NULL outside `compare_unrendered_code`).
    pub node_template_hash: Option<String>,
    pub values_hash: Option<String>,
    pub dialect: String,
    pub table_namespace: Option<String>,
    pub node_unique_id: Option<String>,
    pub last_modified_epoch: Option<i64>,
    pub execution_runtime_ms: Option<i64>,
    pub execution_results: Option<qc::Struct>,
    pub input_tables: Vec<InputTable>,
    pub status: String,
    pub request_id: String,
    /// Set only for clone-origin rows: the physical source this target was
    /// copied from. Enables clone adoption on a later submit.
    pub clone_source_table: Option<String>,
    /// Why recorded inputs or cached results could not be decoded, if they
    /// could not. Raised by [`ExecutionRow::into_checked`].
    pub(crate) undecodable: Undecodable,
}

/// Decode failures of an [`ExecutionRow`]'s stored encodings.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Undecodable {
    input_tables: Option<String>,
    execution_results: Option<String>,
}

impl ExecutionRow {
    /// The columns the `FromRow` mapping reads.
    pub(crate) const COLUMNS: &'static str = "id, org_id, target_table, execution_type, \
        node_body_hash, node_sql_hash, node_namespace_logic_hash, node_template_hash, \
        values_hash, dialect, table_namespace, node_unique_id, last_modified_epoch, \
        execution_runtime_ms, execution_results, input_tables, status, request_id, \
        clone_source_table";

    /// Decode the stored encodings of the recorded inputs (JSONB) and cached
    /// results (protobuf bytes) onto the row, keeping any failure for
    /// [`ExecutionRow::into_checked`].
    pub(crate) fn with_encoded_evidence(
        mut self,
        input_tables: serde_json::Value,
        execution_results: Option<Vec<u8>>,
    ) -> Self {
        match serde_json::from_value::<Vec<InputTable>>(input_tables) {
            Ok(tables) => self.input_tables = tables,
            Err(e) => {
                self.input_tables = Vec::new();
                self.undecodable.input_tables = Some(e.to_string());
            }
        }
        match execution_results
            .map(|bytes| qc::Struct::decode(bytes.as_slice()))
            .transpose()
        {
            Ok(results) => self.execution_results = results,
            Err(e) => {
                self.execution_results = None;
                self.undecodable.execution_results = Some(e.to_string());
            }
        }
        self
    }

    /// The row with its recorded inputs, and with `with_results` its cached
    /// results, proven decodable: a decode failure surfaces here as an error.
    /// With `with_results = false` the cached results are dropped unchecked
    /// (paths that never serve them).
    pub(crate) fn into_checked(mut self, with_results: bool) -> sqlx::Result<Self> {
        let decode_error = |e: String| sqlx::Error::Decode(e.into());
        if let Some(e) = self.undecodable.input_tables.take() {
            return Err(decode_error(e));
        }
        match self.undecodable.execution_results.take() {
            Some(e) if with_results => return Err(decode_error(e)),
            _ if !with_results => self.execution_results = None,
            _ => {}
        }
        Ok(self)
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for ExecutionRow {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        let read = ExecutionRow {
            id: row.try_get("id")?,
            org_id: row.try_get("org_id")?,
            target_table: row.try_get("target_table")?,
            execution_type: row.try_get("execution_type")?,
            node_body_hash: row.try_get("node_body_hash")?,
            node_sql_hash: row.try_get("node_sql_hash")?,
            node_namespace_logic_hash: row.try_get("node_namespace_logic_hash")?,
            node_template_hash: row.try_get("node_template_hash")?,
            values_hash: row.try_get("values_hash")?,
            dialect: row.try_get("dialect")?,
            table_namespace: row.try_get("table_namespace")?,
            node_unique_id: row.try_get("node_unique_id")?,
            last_modified_epoch: row.try_get("last_modified_epoch")?,
            execution_runtime_ms: row.try_get("execution_runtime_ms")?,
            status: row.try_get("status")?,
            request_id: row.try_get("request_id")?,
            clone_source_table: row.try_get("clone_source_table")?,
            ..ExecutionRow::default()
        };
        Ok(read.with_encoded_evidence(
            row.try_get("input_tables")?,
            row.try_get("execution_results")?,
        ))
    }
}

/// A current confirmed state with the requested logic under another physical
/// name: a candidate source for a result copy.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct CopyCandidate {
    pub target_table: String,
    pub table_type: Option<String>,
    pub last_modified_epoch: Option<i64>,
    pub execution_runtime_ms: Option<i64>,
}

/// What [`Store::find_copy_candidates`] selects. `limit` is the warehouse
/// rules' candidate limit (`None`: every candidate).
#[derive(Debug, Clone, Copy)]
pub struct CopyCandidateQuery<'a> {
    pub table_namespace: &'a str,
    pub node_namespace_logic_hash: &'a str,
    pub execution_type: i32,
    pub exclude_target: &'a str,
    pub dialect: &'a str,
    pub limit: Option<i64>,
}

/// The current state a client-requested copy's source must have
/// ([`Store::find_current_copy_source`]): a confirmed build (not itself a
/// copy) of this execution type, table type and node hash at this epoch.
#[derive(Debug, Clone, Copy)]
pub struct CopySourceQuery<'a> {
    pub source: &'a str,
    pub table_namespace: &'a str,
    pub dialect: &'a str,
    pub execution_type: i32,
    pub last_modified_epoch: i64,
    pub table_type: &'a str,
    pub node_hash: &'a str,
}

/// A node's recorded comparison state for `state:*` selectors, keyed by the
/// logical (org, project_id, target env, node_unique_id). Mirrors the client's
/// `DbtNodeData` hashes; the hosted service records them verbatim and the
/// selector compares against them (verified live — see `selector.rs`).
#[derive(Debug, Clone)]
pub struct NodeStateUpsert {
    pub org_id: String,
    pub project_id: String,
    pub target_name: String,
    pub node_unique_id: String,
    pub node_body_hash: Option<String>,
    pub node_configs_hash: Option<String>,
    pub node_persisted_descriptions_hash: Option<String>,
    pub node_macros_hash: Option<String>,
    pub node_contract_hash: Option<String>,
    pub node_database_representation: Option<String>,
    /// The physical relation this node was built as in `target_name`, used to
    /// resolve `ResolveDeferredRelations`. The submit/record `target_table`
    /// (a quoted 3-part FQN). NULL for data tests (empty target).
    pub target_table: Option<String>,
}

#[derive(Debug)]
pub struct Store {
    pool: PgPool,
    /// Count of best-effort `node_states` writes that FAILED (DB error). The
    /// selector comparison store is written best-effort on every Submit so a
    /// store outage never fails the SKIP/EXECUTE/CLONE decision path — but a
    /// silent failure would let the execution ledger advance while `state:*`
    /// selectors read a stale baseline. This counter makes that observable
    /// (surface it as a metric/alert in deployment) and is asserted by the
    /// `selector_state_write_failures_are_observable` offline test. It is NOT a
    /// correctness signal for a single request; it is an operational health gauge.
    selector_write_failures: std::sync::atomic::AtomicU64,
}

impl Store {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            selector_write_failures: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Current count of failed best-effort selector-state writes.
    pub fn selector_write_failures(&self) -> u64 {
        self.selector_write_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn count_selector_write_failure(&self) {
        self.selector_write_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Insert one execution row (Submit and RegisterClone pending rows).
    pub(crate) async fn insert_execution(&self, row: &ExecutionWrite) -> sqlx::Result<()> {
        Self::insert_row(&self.pool, row).await
    }

    /// Insert a batch of fully-confirmed executions AND their selector/deferral
    /// `node_states` upserts in a SINGLE transaction. Either everything commits
    /// or nothing does: a failure writing either table rolls back BOTH.
    ///
    /// Review #2 finding 5 (full atomicity): previously the executions batch
    /// committed and THEN best-effort `node_states` upserts ran outside the
    /// transaction, so a logical-state write failure left a durable execution
    /// row with no corresponding selector/deferral state (`records_stored=1`,
    /// zero logical rows, empty deferral resolution). Writing both in one
    /// transaction makes a successful `RecordExecutions` publish execution and
    /// logical state consistently — a `node_states` fault now rolls the whole
    /// request back and surfaces as an error, rather than a silent partial
    /// publish. Returns the number of execution rows inserted.
    pub(crate) async fn insert_confirmed_batch_with_node_states(
        &self,
        records: &[ExecutionWrite],
        node_states: &[NodeStateUpsert],
    ) -> sqlx::Result<u32> {
        let mut tx = self.pool.begin().await?;
        for row in records {
            Self::insert_row(&mut *tx, row).await?;
        }
        for n in node_states {
            Self::upsert_node_state_tx(&mut tx, n).await?;
        }
        tx.commit().await?;
        Ok(records.len() as u32)
    }

    /// Insert one execution row: pending rows carry no outcome, confirmed rows
    /// carry theirs and are stamped `confirmed_at`.
    async fn insert_row<'e>(
        executor: impl sqlx::PgExecutor<'e>,
        row: &ExecutionWrite,
    ) -> sqlx::Result<()> {
        let e = &row.evidence;
        let outcome = row.status.outcome();
        let input_tables = serde_json::to_value(&e.input_tables).unwrap_or_default();
        sqlx::query(
            r#"
            INSERT INTO executions (
                org_id, target_table, execution_type, node_hash, node_body_hash,
                node_configs_hash, node_contract_hash, node_unique_id, table_namespace,
                dialect, input_tables, values_hash, status, request_id,
                execution_decision_id, node_sql_hash, project_id, node_namespace_logic_hash,
                clone_source_table, node_template_hash, last_modified_epoch, table_type,
                execution_runtime_ms, execution_results, confirmed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
                      $21,$22,$23,$24,CASE WHEN $25 THEN now() END)
            "#,
        )
        .bind(&e.org_id)
        .bind(&e.target_table)
        .bind(e.execution_type)
        .bind(&e.node_hash)
        .bind(&e.node_body_hash)
        .bind(&e.node_configs_hash)
        .bind(&e.node_contract_hash)
        .bind(&e.node_unique_id)
        .bind(&e.table_namespace)
        .bind(&e.dialect)
        .bind(input_tables)
        .bind(&e.values_hash)
        .bind(row.status.as_str())
        .bind(&row.request_id)
        .bind(&row.execution_decision_id)
        .bind(&e.node_sql_hash)
        .bind(&e.project_id)
        .bind(&e.node_namespace_logic_hash)
        .bind(&e.clone_source_table)
        .bind(&e.node_template_hash)
        .bind(outcome.and_then(|o| o.last_modified_epoch))
        .bind(outcome.and_then(|o| o.table_type.as_deref()))
        .bind(outcome.and_then(|o| o.execution_runtime_ms))
        .bind(
            outcome
                .and_then(|o| o.execution_results.as_ref())
                .map(Message::encode_to_vec),
        )
        .bind(outcome.is_some())
        .execute(executor)
        .await?;
        Ok(())
    }

    /// Mark a pending execution confirmed, recording the outcome. Semantics
    /// (pinned by tests, mirrors the hosted service's idempotent confirm):
    ///
    ///   * The outcome fields are written ONLY on the pending→confirmed
    ///     transition. A `request_id` that is already confirmed is NOT mutated —
    ///     the first confirm's recorded `last_modified_epoch`/runtime is stable,
    ///     so a late/duplicate confirm cannot silently rewrite skippable history.
    ///   * Returns `true` when the row exists for this (org, request_id) — whether
    ///     it was just confirmed OR was already confirmed (idempotent success).
    ///   * Returns `false` only when no such row exists (unknown request_id).
    ///
    /// Scoped by `org_id` to honor the global org-isolation invariant, even
    /// though `request_id` is a unique server-minted UUID.
    #[allow(clippy::too_many_arguments)] // Mirrors the correlated wire outcome fields.
    pub async fn confirm(
        &self,
        org_id: &str,
        request_id: &str,
        last_modified_epoch: Option<i64>,
        table_type: Option<&str>,
        execution_runtime_ms: Option<i64>,
        execution_results: Option<&qc::Struct>,
        failed_to_clone: bool,
    ) -> sqlx::Result<bool> {
        // Transition pending→confirmed, writing the outcome exactly once.
        let affected = sqlx::query(
            r#"
            UPDATE executions
            SET status = 'confirmed',
                last_modified_epoch = $3,
                table_type = $4,
                execution_runtime_ms = $5,
                execution_results = $6,
                clone_source_table = CASE WHEN $7 THEN NULL ELSE clone_source_table END,
                node_namespace_logic_hash = CASE
                    WHEN $7 AND node_sql_hash IS NULL THEN NULL
                    ELSE node_namespace_logic_hash END,
                confirmed_at = now()
            WHERE org_id = $1
              AND request_id = $2
              AND status = 'pending'
            "#,
        )
        .bind(org_id)
        .bind(request_id)
        .bind(last_modified_epoch)
        .bind(table_type)
        .bind(execution_runtime_ms)
        .bind(execution_results.map(Message::encode_to_vec))
        .bind(failed_to_clone)
        .execute(&self.pool)
        .await?
        .rows_affected();

        if affected > 0 {
            return Ok(true);
        }

        // No pending row updated: either already confirmed (idempotent success)
        // or genuinely unknown. Distinguish without mutating the outcome.
        let exists: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT id FROM executions
            WHERE org_id = $1 AND request_id = $2
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(request_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(exists.is_some())
    }

    /// Upsert a node's comparison state for `state:*` selectors (latest wins),
    /// keyed by the logical (org, project_id, target env, node_unique_id). Called
    /// on every Submit/Record that carries a `dbt_node_state`, so a real build in
    /// the defer-to env seeds the state a later selector reads. See
    /// `selector.rs` and migration 0009.
    pub async fn upsert_node_state(&self, n: &NodeStateUpsert) -> sqlx::Result<()> {
        let mut tx = self.pool.begin().await?;
        Self::upsert_node_state_tx(&mut tx, n).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Upsert a node's comparison state inside an open transaction (shared by
    /// the standalone `upsert_node_state` and the atomic record batch).
    async fn upsert_node_state_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        n: &NodeStateUpsert,
    ) -> sqlx::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO node_states (
                org_id, project_id, target_name, node_unique_id,
                node_body_hash, node_configs_hash, node_persisted_descriptions_hash,
                node_macros_hash, node_contract_hash, node_database_representation,
                target_table, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, now())
            ON CONFLICT (org_id, project_id, target_name, node_unique_id) DO UPDATE SET
                node_body_hash = EXCLUDED.node_body_hash,
                node_configs_hash = EXCLUDED.node_configs_hash,
                node_persisted_descriptions_hash = EXCLUDED.node_persisted_descriptions_hash,
                node_macros_hash = EXCLUDED.node_macros_hash,
                node_contract_hash = EXCLUDED.node_contract_hash,
                node_database_representation = EXCLUDED.node_database_representation,
                target_table = EXCLUDED.target_table,
                updated_at = now()
            "#,
        )
        .bind(&n.org_id)
        .bind(&n.project_id)
        .bind(&n.target_name)
        .bind(&n.node_unique_id)
        .bind(&n.node_body_hash)
        .bind(&n.node_configs_hash)
        .bind(&n.node_persisted_descriptions_hash)
        .bind(&n.node_macros_hash)
        .bind(&n.node_contract_hash)
        .bind(&n.node_database_representation)
        .bind(&n.target_table)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Fetch the recorded comparison state for the given node ids within one
    /// (org, project_id, target env) scope. Nodes with no row are absent from
    /// the map (the selector treats them as `new`). Scoped by `org_id` per the
    /// global org-isolation invariant.
    pub async fn fetch_node_states(
        &self,
        org_id: &str,
        project_id: &str,
        target_name: &str,
        node_unique_ids: &[String],
    ) -> sqlx::Result<std::collections::HashMap<String, crate::selector::RecordedNode>> {
        use crate::selector::RecordedNode;
        type Row = (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT node_unique_id, node_body_hash, node_configs_hash,
                   node_persisted_descriptions_hash, node_macros_hash,
                   node_contract_hash, node_database_representation
            FROM node_states
            WHERE org_id = $1 AND project_id = $2 AND target_name = $3
              AND node_unique_id = ANY($4)
            "#,
        )
        .bind(org_id)
        .bind(project_id)
        .bind(target_name)
        .bind(node_unique_ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(uid, body, configs, pd, macros, contract, repr)| {
                (
                    uid,
                    RecordedNode {
                        node_body_hash: body,
                        node_configs_hash: configs,
                        node_persisted_descriptions_hash: pd,
                        node_macros_hash: macros,
                        node_contract_hash: contract,
                        node_database_representation: repr,
                    },
                )
            })
            .collect())
    }

    /// Resolve each requested `node_unique_id` to the PHYSICAL relation it was
    /// last built as in the `target_name` env (for `ResolveDeferredRelations`),
    /// scoped by (org, project_id, target env). Only nodes with a recorded
    /// physical `target_table` are returned; absent/data-test nodes are simply
    /// omitted from the map (the client treats an unresolved node as not
    /// deferrable), matching the hosted empty-when-unknown behavior.
    ///
    /// VERIFIED LIVE (experiments/deferral_experiment.sh): the hosted service
    /// returns `{ node_unique_id -> "CATALOG"."SCHEMA"."OBJECT" }` for upstreams
    /// built in the defer-to env; the FQN equals the submit `target_table`.
    pub async fn resolve_deferred_fqns(
        &self,
        org_id: &str,
        project_id: &str,
        target_name: &str,
        node_unique_ids: &[String],
    ) -> sqlx::Result<std::collections::HashMap<String, String>> {
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(
            r#"
            SELECT node_unique_id, target_table
            FROM node_states
            WHERE org_id = $1 AND project_id = $2 AND target_name = $3
              AND node_unique_id = ANY($4)
              AND target_table IS NOT NULL
            "#,
        )
        .bind(org_id)
        .bind(project_id)
        .bind(target_name)
        .bind(node_unique_ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .filter_map(|(uid, fqn)| fqn.map(|f| (uid, f)))
            .collect())
    }

    /// Fetch the persisted `node_decisions` rows for the requested
    /// `execution_decision_id`s, scoped by `org_id`, for `GetExplainMessages`
    /// re-derivation (ADR 0005). Returns at most one row per id (the capture
    /// writes one decision per id); ids absent from the store (untracked nodes)
    /// are simply omitted — the caller builds no message for them. Org scoping
    /// is mandatory (AGENTS.md): never read another tenant's decisions.
    pub async fn fetch_explain_decisions(
        &self,
        org_id: &str,
        execution_decision_ids: &[String],
    ) -> sqlx::Result<Vec<ExplainDecisionRow>> {
        if execution_decision_ids.is_empty() {
            return Ok(Vec::new());
        }
        // DISTINCT ON keeps one row per id (the latest by id) in case a best-
        // effort capture wrote more than once for the same decision id.
        let mut rows: Vec<ExplainDecisionRow> = sqlx::query_as(
            r#"
            SELECT DISTINCT ON (execution_decision_id)
                   execution_decision_id, decision, execution_type,
                   decision_description, is_stale, target_table, clone_source,
                   input_tables, matched_execution_id, decision_reason
            FROM node_decisions
            WHERE org_id = $1
              AND execution_decision_id = ANY($2)
            ORDER BY execution_decision_id, id DESC
            "#,
        )
        .bind(org_id)
        .bind(execution_decision_ids)
        .fetch_all(&self.pool)
        .await?;

        // The matched `executions` anchors (ADR 0005 / ticket #30): the
        // freshness-subtree anchor the explain generator re-derives against,
        // read as complete rows. Org-scoped like the decisions (AGENTS.md).
        let matched: Vec<i64> = rows.iter().filter_map(|r| r.matched_execution_id).collect();
        if matched.is_empty() {
            return Ok(rows);
        }
        let anchors: Vec<ExecutionRow> = sqlx::query_as(&format!(
            "SELECT {} FROM executions WHERE org_id = $1 AND id = ANY($2)",
            ExecutionRow::COLUMNS
        ))
        .bind(org_id)
        .bind(&matched)
        .fetch_all(&self.pool)
        .await?;
        let anchors: std::collections::HashMap<i64, ExecutionRow> =
            anchors.into_iter().map(|a| (a.id, a)).collect();
        for row in &mut rows {
            row.matched_execution = row
                .matched_execution_id
                .and_then(|id| anchors.get(&id).cloned());
        }
        Ok(rows)
    }
}

/// A `node_decisions` row projected for explain re-derivation (ADR 0005), with
/// its matched `executions` anchor (ticket #30) when one was recorded.
#[derive(sqlx::FromRow)]
pub struct ExplainDecisionRow {
    pub execution_decision_id: Option<String>,
    pub decision: String,
    pub execution_type: i32,
    pub decision_description: Option<String>,
    pub is_stale: bool,
    pub target_table: Option<String>,
    pub clone_source: Option<String>,
    pub input_tables: serde_json::Value,
    /// The matched prior `executions` row id (NULL when the decision matched no
    /// prior build — first-build/fingerprint-miss EXECUTE or CLONE).
    pub matched_execution_id: Option<i64>,
    /// The decision reason's stored string (ADR 0006); NULL only for a row
    /// written before it was persisted.
    pub decision_reason: Option<String>,
    /// The complete `executions` row `matched_execution_id` names, loaded by
    /// [`Store::fetch_explain_decisions`].
    #[sqlx(skip)]
    pub matched_execution: Option<ExecutionRow>,
}

impl ExplainDecisionRow {
    /// The persisted decision reason. `None` for a NULL or unknown stored
    /// value, either of which renders no Explain tree.
    pub fn decision_reason(&self) -> Option<crate::decision::DecisionReason> {
        self.decision_reason
            .as_deref()
            .and_then(crate::decision::DecisionReason::from_stored)
    }

    /// Decode the JSONB `input_tables` column into the typed shape.
    pub fn input_tables(&self) -> Vec<InputTable> {
        serde_json::from_value::<Vec<InputTable>>(self.input_tables.clone()).unwrap_or_default()
    }

    /// The matched `executions` anchor row (ADR 0005 / ticket #30) for the
    /// explain freshness subtree, when the decision recorded one. `None` when
    /// no prior build was matched (the EXECUTE first-build / CLONE cases ADR
    /// 0005 leaves NULL), in which case the generator renders the
    /// anchor-independent tree.
    pub fn anchor(&self) -> Option<ExecutionRow> {
        let mut anchor = self.matched_execution.clone()?;
        // Re-derive against the same recorded inputs the decision used: the
        // persisted decision-time inputs stand in exactly when the decision
        // substituted the submit's inputs (one shared rule).
        crate::reuse_candidate::adopt_current_inputs(&mut anchor, &self.input_tables());
        Some(anchor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decision_row(anchor_node_sql_hash: Option<&str>) -> ExplainDecisionRow {
        ExplainDecisionRow {
            execution_decision_id: Some("d".into()),
            decision: "skip".into(),
            execution_type: 1,
            decision_description: None,
            is_stale: false,
            target_table: Some("T".into()),
            clone_source: None,
            input_tables: serde_json::json!([{ "name": "UP", "last_modified_epoch": 5 }]),
            matched_execution_id: Some(1),
            decision_reason: Some("reused.fresh".into()),
            matched_execution: Some(ExecutionRow {
                id: 1,
                org_id: "o".into(),
                target_table: "T".into(),
                execution_type: 1,
                node_sql_hash: anchor_node_sql_hash.map(str::to_owned),
                last_modified_epoch: Some(10),
                input_tables: Vec::new(),
                status: "confirmed".into(),
                clone_source_table: Some("PROD.T".into()),
                ..ExecutionRow::default()
            }),
        }
    }

    /// Explain re-derives against the same recorded inputs the decision used
    /// (`reuse_candidate::adopt_current_inputs`): substituted for a
    /// client-requested copy, kept as recorded for a SQL-submit copy.
    #[test]
    fn explain_anchor_applies_the_decision_input_rule() {
        let register_clone = decision_row(None).anchor().unwrap();
        assert_eq!(register_clone.input_tables.len(), 1);
        assert_eq!(register_clone.input_tables[0].name, "UP");

        let sql_copy = decision_row(Some("h")).anchor().unwrap();
        assert!(sql_copy.input_tables.is_empty());
    }
}
