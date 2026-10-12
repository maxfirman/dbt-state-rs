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
}

/// A confirmed "equivalent under another name" to clone from (SQL-submit
/// cross-name fallback). Source physical name + warehouse type + build epoch.
#[derive(Debug, Clone)]
pub struct CloneEquivalent {
    pub source_table: String,
    pub table_type: Option<String>,
    pub last_modified_epoch: Option<i64>,
    pub execution_runtime_ms: Option<i64>,
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
                   node_namespace_logic_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id, clone_source_table
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
                   node_namespace_logic_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id, clone_source_table
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
                   node_namespace_logic_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id, clone_source_table
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

    /// The newest confirmed CLONE-ORIGIN execution for a physical target. A
    /// clone-origin row (RegisterClone) carries the physical object's build
    /// epoch and an INHERITED `node_namespace_logic_hash` (from the clone
    /// source), which the caller compares against the submit's abstracted logic.
    /// Restricted to clone-origin rows so ordinary executions (and any corrupt
    /// unrelated rows) are never materialized by the cross-environment fallback.
    pub async fn find_latest_confirmed_target(
        &self,
        org_id: &str,
        target_table: &str,
        execution_type: i32,
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash, node_sql_hash,
                   node_namespace_logic_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id, clone_source_table
            FROM executions
            WHERE org_id = $1 AND target_table = $2 AND status = 'confirmed'
              AND execution_type = $3
              AND ($4 IS NULL OR table_namespace = $4)
              AND dialect = $5
              AND clone_source_table IS NOT NULL
            ORDER BY confirmed_at DESC NULLS LAST, id DESC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(target_table)
        .bind(execution_type)
        .bind(table_namespace)
        .bind(dialect)
        .fetch_optional(&self.pool)
        .await?;
        row.map(TryInto::try_into).transpose()
    }

    /// The newest confirmed execution in a namespace carrying a given
    /// schema-abstracted logic identity. Used at RegisterClone to inherit the
    /// clone SOURCE's logic identity onto the clone-origin row, so a later
    /// submit for the clone target can match its (schema-abstracted) logic.
    pub async fn find_confirmed_ns_logic(
        &self,
        org_id: &str,
        table_namespace: &str,
        node_namespace_logic_hash: &str,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash, node_sql_hash,
                   node_namespace_logic_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id, clone_source_table
            FROM executions
            WHERE org_id = $1 AND table_namespace = $2
              AND node_namespace_logic_hash = $3 AND status = 'confirmed'
            ORDER BY confirmed_at DESC NULLS LAST, id DESC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(table_namespace)
        .bind(node_namespace_logic_hash)
        .fetch_optional(&self.pool)
        .await?;
        row.map(TryInto::try_into).transpose()
    }

    /// Find a confirmed "equivalent model under another name" to clone FROM.
    ///
    /// VERIFIED LIVE: a SQL submit for a target that does not yet exist, when an
    /// equivalent model (same `table_namespace` + schema-abstracted logic) was
    /// already built under a DIFFERENT physical target, is answered with
    /// `ready_to_clone` cloning that equivalent into the submit target
    /// ("an equivalent model exists under another name so we cloned that one").
    /// Returns the source's physical name, warehouse table type and build epoch
    /// so the service can emit the clone DDL + `clone_required_last_modified_epoch`.
    pub async fn find_clone_equivalent(
        &self,
        org_id: &str,
        table_namespace: &str,
        node_namespace_logic_hash: &str,
        execution_type: i32,
        exclude_target: &str,
    ) -> sqlx::Result<Option<CloneEquivalent>> {
        type CloneEquivRow = (String, Option<String>, Option<i64>, Option<i64>);
        let row: Option<CloneEquivRow> = sqlx::query_as(
            r#"
            SELECT target_table, table_type, last_modified_epoch, execution_runtime_ms
            FROM executions
            WHERE org_id = $1 AND table_namespace = $2
              AND node_namespace_logic_hash = $3 AND status = 'confirmed'
              AND execution_type = $4
              AND target_table <> $5
            ORDER BY confirmed_at DESC NULLS LAST, id DESC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(table_namespace)
        .bind(node_namespace_logic_hash)
        .bind(execution_type)
        .bind(exclude_target)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(source_table, table_type, last_modified_epoch, execution_runtime_ms)| {
                CloneEquivalent {
                    source_table,
                    table_type,
                    last_modified_epoch,
                    execution_runtime_ms,
                }
            },
        ))
    }

    /// Look up the clone source's confirmed logic identity by its physical
    /// target name within a namespace. Returns the inherited
    /// `node_namespace_logic_hash` to stamp on a clone-origin row.
    pub async fn find_source_ns_logic_hash(
        &self,
        org_id: &str,
        source_table: &str,
        table_namespace: Option<&str>,
    ) -> sqlx::Result<Option<String>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash, node_sql_hash,
                   node_namespace_logic_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id, clone_source_table
            FROM executions
            WHERE org_id = $1 AND target_table = $2 AND status = 'confirmed'
              AND ($3 IS NULL OR table_namespace = $3)
              AND node_namespace_logic_hash IS NOT NULL
            ORDER BY confirmed_at DESC NULLS LAST, id DESC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(source_table)
        .bind(table_namespace)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row
            .map(TryInto::try_into)
            .transpose()?
            .and_then(|r: ExecutionRow| r.node_namespace_logic_hash))
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
                node_sql_hash, project_id, node_namespace_logic_hash, clone_source_table
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'pending',$13,$14,$15,$16,$17,$18)
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
                    execution_runtime_ms, confirmed_at, node_sql_hash, project_id, execution_results,
                    node_namespace_logic_hash
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'confirmed',$13,$14,$15,$16,$17,now(),$18,$19,$20,$21)
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

    /// Upsert a node's comparison state for `state:*` selectors (latest wins),
    /// keyed by the logical (org, project_id, target env, node_unique_id). Called
    /// on every Submit/Record that carries a `dbt_node_state`, so a real build in
    /// the defer-to env seeds the state a later selector reads. See
    /// `selector.rs` and migration 0009.
    pub async fn upsert_node_state(&self, n: &NodeStateUpsert) -> sqlx::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO node_states (
                org_id, project_id, target_name, node_unique_id,
                node_body_hash, node_configs_hash, node_persisted_descriptions_hash,
                node_macros_hash, node_contract_hash, node_database_representation,
                updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
            ON CONFLICT (org_id, project_id, target_name, node_unique_id) DO UPDATE SET
                node_body_hash = EXCLUDED.node_body_hash,
                node_configs_hash = EXCLUDED.node_configs_hash,
                node_persisted_descriptions_hash = EXCLUDED.node_persisted_descriptions_hash,
                node_macros_hash = EXCLUDED.node_macros_hash,
                node_contract_hash = EXCLUDED.node_contract_hash,
                node_database_representation = EXCLUDED.node_database_representation,
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
        .execute(&self.pool)
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
