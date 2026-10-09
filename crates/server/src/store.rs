//! Postgres-backed state store.

use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// One upstream input table's freshness, as sent in SubmitEnrichedSQLRequest.tables.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InputTable {
    pub name: String,
    pub last_modified_epoch: i64,
}

/// A row in the executions table.
#[derive(Debug, Clone)]
pub struct ExecutionRow {
    pub id: i64,
    pub org_id: String,
    pub target_table: String,
    pub execution_type: i32,
    pub node_body_hash: Option<String>,
    pub table_namespace: Option<String>,
    pub last_modified_epoch: Option<i64>,
    pub execution_runtime_ms: Option<i64>,
    pub input_tables: Vec<InputTable>,
    pub status: String,
    pub request_id: String,
}

/// Parameters captured when a ready_to_execute verdict is issued (pending row).
#[derive(Debug, Clone)]
pub struct PendingExecution {
    pub org_id: String,
    pub target_table: String,
    pub execution_type: i32,
    pub node_hash: Option<String>,
    pub node_body_hash: Option<String>,
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
    pub target_table: String,
    pub execution_type: i32,
    pub node_hash: Option<String>,
    pub node_body_hash: Option<String>,
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

    /// Find the most recent CONFIRMED execution matching the node fingerprint.
    pub async fn find_confirmed(
        &self,
        org_id: &str,
        target_table: &str,
        execution_type: i32,
        node_body_hash: Option<&str>,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash,
                   table_namespace, last_modified_epoch, execution_runtime_ms,
                   input_tables, status, request_id
            FROM executions
            WHERE org_id = $1
              AND target_table = $2
              AND execution_type = $3
              AND status = 'confirmed'
              AND node_body_hash IS NOT DISTINCT FROM $4
            ORDER BY confirmed_at DESC NULLS LAST, id DESC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(target_table)
        .bind(execution_type)
        .bind(node_body_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    /// Cross-environment match: find the most recent CONFIRMED execution with
    /// the same logical identity (`table_namespace` + `node_body_hash` +
    /// `execution_type`) regardless of physical target/schema. This mirrors the
    /// hosted service, which reuses a node built in one environment (e.g. prod)
    /// to skip the same logical node in another (e.g. dev) — deferral/state
    /// reuse. Scoped to the org. Only used when `table_namespace` is present.
    pub async fn find_confirmed_by_namespace(
        &self,
        org_id: &str,
        table_namespace: &str,
        execution_type: i32,
        node_body_hash: Option<&str>,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash,
                   table_namespace, last_modified_epoch, execution_runtime_ms,
                   input_tables, status, request_id
            FROM executions
            WHERE org_id = $1
              AND table_namespace = $2
              AND execution_type = $3
              AND status = 'confirmed'
              AND node_body_hash IS NOT DISTINCT FROM $4
            ORDER BY confirmed_at DESC NULLS LAST, id DESC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(table_namespace)
        .bind(execution_type)
        .bind(node_body_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    /// Find the most recent CONFIRMED seed execution matching the values_hash.
    /// Analogous to `find_confirmed` but keyed on `values_hash` (seeds carry no
    /// SQL body fingerprint).
    pub async fn find_confirmed_values(
        &self,
        org_id: &str,
        target_table: &str,
        execution_type: i32,
        values_hash: Option<&str>,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash,
                   table_namespace, last_modified_epoch, execution_runtime_ms,
                   input_tables, status, request_id
            FROM executions
            WHERE org_id = $1
              AND target_table = $2
              AND execution_type = $3
              AND status = 'confirmed'
              AND values_hash IS NOT DISTINCT FROM $4
            ORDER BY confirmed_at DESC NULLS LAST, id DESC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(target_table)
        .bind(execution_type)
        .bind(values_hash)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    /// Insert a pending execution row for a ready_to_execute verdict.
    pub async fn insert_pending(&self, p: &PendingExecution) -> sqlx::Result<i64> {
        let input_tables = serde_json::to_value(&p.input_tables).unwrap_or_default();
        let rec = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO executions (
                org_id, target_table, execution_type, node_hash, node_body_hash,
                node_configs_hash, node_contract_hash, node_unique_id, table_namespace,
                dialect, input_tables, values_hash, status, request_id, execution_decision_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'pending',$13,$14)
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
                    execution_runtime_ms, confirmed_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'confirmed',$13,$14,$15,$16,$17,now())
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
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(records.len() as u32)
    }

    /// Mark a pending execution confirmed, recording the outcome. Returns true
    /// if a matching pending row existed.
    pub async fn confirm(
        &self,
        request_id: &str,
        last_modified_epoch: Option<i64>,
        table_type: Option<&str>,
        execution_runtime_ms: Option<i64>,
    ) -> sqlx::Result<bool> {
        let affected = sqlx::query(
            r#"
            UPDATE executions
            SET status = 'confirmed',
                last_modified_epoch = $2,
                table_type = $3,
                execution_runtime_ms = $4,
                confirmed_at = now()
            WHERE request_id = $1
            "#,
        )
        .bind(request_id)
        .bind(last_modified_epoch)
        .bind(table_type)
        .bind(execution_runtime_ms)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(affected > 0)
    }
}

#[derive(sqlx::FromRow)]
struct RawRow {
    id: i64,
    org_id: String,
    target_table: String,
    execution_type: i32,
    node_body_hash: Option<String>,
    table_namespace: Option<String>,
    last_modified_epoch: Option<i64>,
    execution_runtime_ms: Option<i64>,
    input_tables: serde_json::Value,
    status: String,
    request_id: String,
}

impl From<RawRow> for ExecutionRow {
    fn from(r: RawRow) -> Self {
        let input_tables =
            serde_json::from_value::<Vec<InputTable>>(r.input_tables).unwrap_or_default();
        ExecutionRow {
            id: r.id,
            org_id: r.org_id,
            target_table: r.target_table,
            execution_type: r.execution_type,
            node_body_hash: r.node_body_hash,
            table_namespace: r.table_namespace,
            last_modified_epoch: r.last_modified_epoch,
            execution_runtime_ms: r.execution_runtime_ms,
            input_tables,
            status: r.status,
            request_id: r.request_id,
        }
    }
}
