//! Postgres-backed state store.

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

/// A row in the executions table.
#[derive(Debug, Clone)]
pub struct ExecutionRow {
    pub id: i64,
    pub org_id: String,
    pub target_table: String,
    pub execution_type: i32,
    pub node_body_hash: Option<String>,
    pub node_sql_hash: Option<String>,
    pub node_namespace_logic_hash: Option<String>,
    pub table_namespace: Option<String>,
    pub node_unique_id: Option<String>,
    pub last_modified_epoch: Option<i64>,
    pub execution_runtime_ms: Option<i64>,
    pub execution_results: Option<qc::Struct>,
    pub input_tables: Vec<InputTable>,
    pub status: String,
    pub request_id: String,
    /// Set only for clone-origin rows (from RegisterClone): the physical source
    /// this target was cloned from. Enables clone adoption on a later submit.
    pub clone_source_table: Option<String>,
}

/// Parameters captured when a ready_to_execute verdict is issued (pending row).
#[derive(Debug, Clone)]
pub struct PendingExecution {
    pub org_id: String,
    pub project_id: Option<String>,
    pub target_table: String,
    pub execution_type: i32,
    pub node_hash: Option<String>,
    pub node_body_hash: Option<String>,
    pub node_sql_hash: Option<String>,
    pub node_namespace_logic_hash: Option<String>,
    pub node_configs_hash: Option<String>,
    pub node_contract_hash: Option<String>,
    pub node_unique_id: Option<String>,
    pub table_namespace: Option<String>,
    pub dialect: String,
    pub input_tables: Vec<InputTable>,
    pub values_hash: Option<String>,
    pub request_id: String,
    pub execution_decision_id: Option<String>,
    /// Set only for clone-origin rows (from RegisterClone): the physical source
    /// this target was cloned from. NULL for ordinary SQL/seed executions.
    pub clone_source_table: Option<String>,
    /// The unrendered-template logic hash, set only in `compare_unrendered_code`
    /// mode (NULL otherwise). Reuse matches when EITHER `node_sql_hash` (rendered)
    /// OR `node_template_hash` (template) equals the submit's.
    pub node_template_hash: Option<String>,
}

/// A fully-formed confirmed execution to insert directly (RecordExecutions
/// bypass path). Carries the recorded outcome inline — no Submit/Confirm.
#[derive(Debug, Clone)]
pub struct ConfirmedExecution {
    pub org_id: String,
    pub project_id: Option<String>,
    pub target_table: String,
    pub execution_type: i32,
    pub node_hash: Option<String>,
    pub node_body_hash: Option<String>,
    pub node_sql_hash: Option<String>,
    pub node_namespace_logic_hash: Option<String>,
    pub node_configs_hash: Option<String>,
    pub node_contract_hash: Option<String>,
    pub node_unique_id: Option<String>,
    pub table_namespace: Option<String>,
    pub dialect: String,
    pub input_tables: Vec<InputTable>,
    pub values_hash: Option<String>,
    pub request_id: String,
    pub execution_decision_id: Option<String>,
    // recorded outcome
    pub last_modified_epoch: Option<i64>,
    pub table_type: Option<String>,
    pub execution_runtime_ms: Option<i64>,
    pub execution_results: Option<qc::Struct>,
    /// Unrendered-template logic hash (NULL outside `compare_unrendered_code`).
    pub node_template_hash: Option<String>,
}

/// A confirmed "equivalent under another name" to clone from (SQL-submit
/// cross-name fallback). Source physical name + warehouse type + build epoch.
#[derive(Debug, Clone)]
pub struct CloneEquivalent {
    pub source_table: String,
    pub table_type: Option<String>,
    pub last_modified_epoch: Option<i64>,
    pub execution_runtime_ms: Option<i64>,
    pub duckdb_copy_depth: usize,
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
}

impl Store {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Insert a pending execution row for a ready_to_execute verdict.
    pub async fn insert_pending(&self, p: &PendingExecution) -> sqlx::Result<i64> {
        let input_tables = serde_json::to_value(&p.input_tables).unwrap_or_default();
        let rec = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO executions (
                org_id, target_table, execution_type, node_hash, node_body_hash,
                node_configs_hash, node_contract_hash, node_unique_id, table_namespace,
                dialect, input_tables, values_hash, status, request_id, execution_decision_id,
                node_sql_hash, project_id, node_namespace_logic_hash, clone_source_table,
                node_template_hash
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'pending',$13,$14,$15,$16,$17,$18,$19)
            RETURNING id
            "#,
        )
        .bind(&p.org_id)
        .bind(&p.target_table)
        .bind(p.execution_type)
        .bind(&p.node_hash)
        .bind(&p.node_body_hash)
        .bind(&p.node_configs_hash)
        .bind(&p.node_contract_hash)
        .bind(&p.node_unique_id)
        .bind(&p.table_namespace)
        .bind(&p.dialect)
        .bind(input_tables)
        .bind(&p.values_hash)
        .bind(&p.request_id)
        .bind(&p.execution_decision_id)
        .bind(&p.node_sql_hash)
        .bind(&p.project_id)
        .bind(&p.node_namespace_logic_hash)
        .bind(&p.clone_source_table)
        .bind(&p.node_template_hash)
        .fetch_one(&self.pool)
        .await?;
        Ok(rec)
    }

    /// Insert a batch of fully-confirmed executions atomically. Any failure
    /// rolls back the whole batch. Returns the number of rows inserted.
    pub async fn insert_confirmed_batch(
        &self,
        records: &[ConfirmedExecution],
    ) -> sqlx::Result<u32> {
        let mut tx = self.pool.begin().await?;
        Self::insert_confirmed_rows(&mut tx, records).await?;
        tx.commit().await?;
        Ok(records.len() as u32)
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
    pub async fn insert_confirmed_batch_with_node_states(
        &self,
        records: &[ConfirmedExecution],
        node_states: &[NodeStateUpsert],
    ) -> sqlx::Result<u32> {
        let mut tx = self.pool.begin().await?;
        Self::insert_confirmed_rows(&mut tx, records).await?;
        for n in node_states {
            Self::upsert_node_state_tx(&mut tx, n).await?;
        }
        tx.commit().await?;
        Ok(records.len() as u32)
    }

    /// Insert confirmed execution rows into an open transaction (shared by the
    /// executions-only and executions+node_states batch methods).
    async fn insert_confirmed_rows(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        records: &[ConfirmedExecution],
    ) -> sqlx::Result<()> {
        for c in records {
            let input_tables = serde_json::to_value(&c.input_tables).unwrap_or_default();
            sqlx::query(
                r#"
                INSERT INTO executions (
                    org_id, target_table, execution_type, node_hash, node_body_hash,
                    node_configs_hash, node_contract_hash, node_unique_id, table_namespace,
                    dialect, input_tables, values_hash, status, request_id,
                    execution_decision_id, last_modified_epoch, table_type,
                    execution_runtime_ms, confirmed_at, node_sql_hash, project_id, execution_results,
                    node_namespace_logic_hash, node_template_hash
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'confirmed',$13,$14,$15,$16,$17,now(),$18,$19,$20,$21,$22)
                "#,
            )
            .bind(&c.org_id)
            .bind(&c.target_table)
            .bind(c.execution_type)
            .bind(&c.node_hash)
            .bind(&c.node_body_hash)
            .bind(&c.node_configs_hash)
            .bind(&c.node_contract_hash)
            .bind(&c.node_unique_id)
            .bind(&c.table_namespace)
            .bind(&c.dialect)
            .bind(input_tables)
            .bind(&c.values_hash)
            .bind(&c.request_id)
            .bind(&c.execution_decision_id)
            .bind(c.last_modified_epoch)
            .bind(&c.table_type)
            .bind(c.execution_runtime_ms)
            .bind(&c.node_sql_hash)
            .bind(&c.project_id)
            .bind(c.execution_results.as_ref().map(Message::encode_to_vec))
            .bind(&c.node_namespace_logic_hash)
            .bind(&c.node_template_hash)
            .execute(&mut **tx)
            .await?;
        }
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
        let rows: Vec<ExplainDecisionRow> = sqlx::query_as(
            r#"
            SELECT DISTINCT ON (execution_decision_id)
                   execution_decision_id, decision, execution_type,
                   decision_description, is_stale, target_table, clone_source,
                   input_tables
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
        Ok(rows)
    }
}

/// A `node_decisions` row projected for explain re-derivation (ADR 0005).
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
}

impl ExplainDecisionRow {
    /// Decode the JSONB `input_tables` column into the typed shape.
    pub fn input_tables(&self) -> Vec<InputTable> {
        serde_json::from_value::<Vec<InputTable>>(self.input_tables.clone()).unwrap_or_default()
    }
}

#[derive(sqlx::FromRow)]
struct RawRow {
    id: i64,
    org_id: String,
    target_table: String,
    execution_type: i32,
    node_body_hash: Option<String>,
    node_sql_hash: Option<String>,
    node_namespace_logic_hash: Option<String>,
    table_namespace: Option<String>,
    node_unique_id: Option<String>,
    last_modified_epoch: Option<i64>,
    execution_runtime_ms: Option<i64>,
    execution_results: Option<Vec<u8>>,
    input_tables: serde_json::Value,
    status: String,
    request_id: String,
    clone_source_table: Option<String>,
}

impl TryFrom<RawRow> for ExecutionRow {
    type Error = sqlx::Error;
    fn try_from(r: RawRow) -> Result<Self, Self::Error> {
        let input_tables = serde_json::from_value::<Vec<InputTable>>(r.input_tables)
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        let execution_results = r
            .execution_results
            .map(|bytes| qc::Struct::decode(bytes.as_slice()))
            .transpose()
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok(ExecutionRow {
            id: r.id,
            org_id: r.org_id,
            target_table: r.target_table,
            execution_type: r.execution_type,
            node_body_hash: r.node_body_hash,
            node_sql_hash: r.node_sql_hash,
            node_namespace_logic_hash: r.node_namespace_logic_hash,
            table_namespace: r.table_namespace,
            node_unique_id: r.node_unique_id,
            last_modified_epoch: r.last_modified_epoch,
            execution_runtime_ms: r.execution_runtime_ms,
            execution_results,
            input_tables,
            status: r.status,
            request_id: r.request_id,
            clone_source_table: r.clone_source_table,
        })
    }
}
