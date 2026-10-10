//! Postgres-backed state store.

use crate::query_cache as qc;
use prost::Message;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

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
    pub table_namespace: Option<String>,
    pub node_unique_id: Option<String>,
    pub last_modified_epoch: Option<i64>,
    pub execution_runtime_ms: Option<i64>,
    pub execution_results: Option<qc::Struct>,
    pub input_tables: Vec<InputTable>,
    pub status: String,
    pub request_id: String,
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
    pub node_configs_hash: Option<String>,
    pub node_contract_hash: Option<String>,
    pub node_unique_id: Option<String>,
    pub table_namespace: Option<String>,
    pub dialect: String,
    pub input_tables: Vec<InputTable>,
    pub values_hash: Option<String>,
    pub request_id: String,
    pub execution_decision_id: Option<String>,
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

    /// Select current physical state before checking logic, kind and namespace.
    /// An older matching execution cannot describe a target overwritten later.
    pub async fn find_confirmed(
        &self,
        org_id: &str,
        target_table: &str,
        execution_type: i32,
        node_sql_hash: Option<&str>,
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash, node_sql_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id
            FROM (
                SELECT * FROM executions
                WHERE org_id = $1 AND target_table = $2 AND status = 'confirmed'
                ORDER BY confirmed_at DESC NULLS LAST, id DESC LIMIT 1
            ) current_execution
            WHERE execution_type = $3
              AND node_sql_hash IS NOT DISTINCT FROM $4
              AND ($5 IS NULL OR table_namespace = $5)
              AND dialect = $6
            "#,
        )
        .bind(org_id)
        .bind(target_table)
        .bind(execution_type)
        .bind(node_sql_hash)
        .bind(table_namespace)
        .bind(dialect)
        .fetch_optional(&self.pool)
        .await?;
        row.map(TryInto::try_into).transpose()
    }

    /// Tests have no physical target. Scope identity by project and namespace,
    /// then compare the latest outcome's logic rather than resurrecting history.
    pub async fn find_confirmed_test(
        &self,
        org_id: &str,
        node_unique_id: &str,
        project_id: Option<&str>,
        table_namespace: Option<&str>,
        node_sql_hash: Option<&str>,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash, node_sql_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id
            FROM (
                SELECT * FROM executions
                WHERE org_id = $1 AND node_unique_id = $2 AND execution_type = 8
                  AND project_id IS NOT DISTINCT FROM $3
                  AND table_namespace IS NOT DISTINCT FROM $4
                  AND status = 'confirmed'
                ORDER BY confirmed_at DESC NULLS LAST, id DESC LIMIT 1
            ) current_execution
            WHERE node_sql_hash IS NOT DISTINCT FROM $5
            "#,
        )
        .bind(org_id)
        .bind(node_unique_id)
        .bind(project_id)
        .bind(table_namespace)
        .bind(node_sql_hash)
        .fetch_optional(&self.pool)
        .await?;
        row.map(TryInto::try_into).transpose()
    }

    /// Seed configuration participates in the logic hash alongside data bytes.
    pub async fn find_confirmed_values(
        &self,
        org_id: &str,
        target_table: &str,
        values_hash: &str,
        node_sql_hash: &str,
        table_namespace: Option<&str>,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash, node_sql_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id
            FROM (
                SELECT * FROM executions
                WHERE org_id = $1 AND target_table = $2 AND status = 'confirmed'
                ORDER BY confirmed_at DESC NULLS LAST, id DESC LIMIT 1
            ) current_execution
            WHERE execution_type = 9 AND values_hash = $3 AND node_sql_hash = $4
              AND ($5 IS NULL OR table_namespace = $5)
            "#,
        )
        .bind(org_id)
        .bind(target_table)
        .bind(values_hash)
        .bind(node_sql_hash)
        .bind(table_namespace)
        .fetch_optional(&self.pool)
        .await?;
        row.map(TryInto::try_into).transpose()
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
                node_sql_hash, project_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'pending',$13,$14,$15,$16)
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
        for c in records {
            let input_tables = serde_json::to_value(&c.input_tables).unwrap_or_default();
            sqlx::query(
                r#"
                INSERT INTO executions (
                    org_id, target_table, execution_type, node_hash, node_body_hash,
                    node_configs_hash, node_contract_hash, node_unique_id, table_namespace,
                    dialect, input_tables, values_hash, status, request_id,
                    execution_decision_id, last_modified_epoch, table_type,
                    execution_runtime_ms, confirmed_at, node_sql_hash, project_id, execution_results
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'confirmed',$13,$14,$15,$16,$17,now(),$18,$19,$20)
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
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(records.len() as u32)
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
    pub async fn confirm(
        &self,
        org_id: &str,
        request_id: &str,
        last_modified_epoch: Option<i64>,
        table_type: Option<&str>,
        execution_runtime_ms: Option<i64>,
        execution_results: Option<&qc::Struct>,
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
}

#[derive(sqlx::FromRow)]
struct RawRow {
    id: i64,
    org_id: String,
    target_table: String,
    execution_type: i32,
    node_body_hash: Option<String>,
    node_sql_hash: Option<String>,
    table_namespace: Option<String>,
    node_unique_id: Option<String>,
    last_modified_epoch: Option<i64>,
    execution_runtime_ms: Option<i64>,
    execution_results: Option<Vec<u8>>,
    input_tables: serde_json::Value,
    status: String,
    request_id: String,
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
            table_namespace: r.table_namespace,
            node_unique_id: r.node_unique_id,
            last_modified_epoch: r.last_modified_epoch,
            execution_runtime_ms: r.execution_runtime_ms,
            execution_results,
            input_tables,
            status: r.status,
            request_id: r.request_id,
        })
    }
}
