//! Named helpers for State the current protocol cannot produce: rows persisted
//! by an older server (legacy fingerprints, frozen pre-fix rows), corrupt
//! encodings, and node state captured from the hosted service. With
//! `fault.rs`, this is the only code that writes `executions` or `node_states`
//! with raw SQL; everything else seeds State through the protocol
//! (`scenario::Scenario::build` / `record`). Each helper is tied to the
//! columns of migrations 0001–0014, so a schema change is edited here once.

use super::rows::Db;

/// Which `executions` rows a rewrite touches.
#[derive(Clone, Copy, Debug)]
pub enum Rows<'a> {
    All,
    Confirmed,
    Target(&'a str),
    ConfirmedTarget(&'a str),
}

impl Rows<'_> {
    fn target(&self) -> Option<&str> {
        match self {
            Rows::Target(t) | Rows::ConfirmedTarget(t) => Some(t),
            Rows::All | Rows::Confirmed => None,
        }
    }

    fn confirmed_only(&self) -> bool {
        matches!(self, Rows::Confirmed | Rows::ConfirmedTarget(_))
    }

    /// Run `UPDATE executions SET <set>` over these rows. The filter binds `$1`
    /// (target) and `$2` (confirmed only); `set` binds `values` from `$3`.
    /// Returns the number of rows touched.
    async fn update(&self, db: &impl Db, set: &str, values: &[Option<&str>]) -> u64 {
        let sql = format!(
            "UPDATE executions SET {set} \
             WHERE ($1::text IS NULL OR target_table = $1) \
               AND (NOT $2 OR status = 'confirmed')"
        );
        let mut query = sqlx::query(&sql)
            .bind(self.target())
            .bind(self.confirmed_only());
        for value in values {
            query = query.bind(*value);
        }
        query
            .execute(db.pool())
            .await
            .expect("rewrite executions")
            .rows_affected()
    }
}

/// Legacy fingerprint: replace the stored rendered-SQL identity with `hash`,
/// as an older server version (or fingerprint version tag) would have stored.
pub async fn rewrite_sql_hash(db: &impl Db, rows: Rows<'_>, hash: &str) -> u64 {
    rows.update(db, "node_sql_hash = $3", &[Some(hash)]).await
}

/// Legacy fingerprints: replace both the rendered-SQL and the template identity.
pub async fn rewrite_logic_hashes(
    db: &impl Db,
    rows: Rows<'_>,
    sql_hash: Option<&str>,
    template_hash: Option<&str>,
) -> u64 {
    rows.update(
        db,
        "node_sql_hash = $3, node_template_hash = $4",
        &[sql_hash, template_hash],
    )
    .await
}

/// Legacy fingerprint: replace the stored schema-abstracted logic identity.
pub async fn rewrite_namespace_logic_hash(db: &impl Db, rows: Rows<'_>, hash: &str) -> u64 {
    rows.update(db, "node_namespace_logic_hash = $3", &[Some(hash)])
        .await
}

/// Corrupt State: every row's `input_tables` becomes a JSON object, which no
/// server version writes.
pub async fn corrupt_input_tables(db: &impl Db) {
    sqlx::query("UPDATE executions SET input_tables = '{}'::jsonb")
        .execute(db.pool())
        .await
        .expect("corrupt input_tables");
}

/// Corrupt State: every row gets valid (empty) inputs but undecodable cached
/// `execution_results` bytes.
pub async fn corrupt_execution_results(db: &impl Db) {
    sqlx::query(
        "UPDATE executions SET input_tables = '[]'::jsonb, execution_results = decode('ff','hex')",
    )
    .execute(db.pool())
    .await
    .expect("corrupt execution_results");
}

/// A row exactly as a frozen older server persisted it (an upgrade fixture).
#[derive(Clone, Debug)]
pub struct FrozenExecution<'a> {
    pub org_id: &'a str,
    pub target_table: &'a str,
    pub execution_type: i32,
    pub dialect: &'a str,
    /// `pending` or `confirmed` (confirmed rows get `confirmed_at = now()`).
    pub status: &'a str,
    pub request_id: &'a str,
    pub node_sql_hash: Option<&'a str>,
    pub node_template_hash: Option<&'a str>,
    pub node_namespace_logic_hash: Option<&'a str>,
    pub clone_source_table: Option<&'a str>,
    pub table_namespace: Option<&'a str>,
    pub last_modified_epoch: Option<i64>,
}

/// Replace every row for the frozen row's target with the frozen row verbatim.
pub async fn replace_with_frozen_execution(db: &impl Db, row: &FrozenExecution<'_>) {
    sqlx::query("DELETE FROM executions WHERE target_table = $1")
        .bind(row.target_table)
        .execute(db.pool())
        .await
        .expect("clear frozen target");
    sqlx::query(
        "INSERT INTO executions \
         (org_id, target_table, execution_type, dialect, status, request_id, \
          node_sql_hash, node_template_hash, node_namespace_logic_hash, \
          clone_source_table, table_namespace, last_modified_epoch, confirmed_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, \
                 CASE WHEN $5 = 'confirmed' THEN now() ELSE NULL END)",
    )
    .bind(row.org_id)
    .bind(row.target_table)
    .bind(row.execution_type)
    .bind(row.dialect)
    .bind(row.status)
    .bind(row.request_id)
    .bind(row.node_sql_hash)
    .bind(row.node_template_hash)
    .bind(row.node_namespace_logic_hash)
    .bind(row.clone_source_table)
    .bind(row.table_namespace)
    .bind(row.last_modified_epoch)
    .execute(db.pool())
    .await
    .expect("insert frozen execution");
}

/// A node's comparison state as the hosted service recorded it.
#[derive(Clone, Debug, Default)]
pub struct CapturedNodeState {
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

/// Seed (latest wins) a node state captured from the hosted service, without
/// an execution: the selector compares against it directly.
pub async fn seed_captured_node_state(db: &impl Db, state: &CapturedNodeState) {
    sqlx::query(
        r#"
        INSERT INTO node_states (
            org_id, project_id, target_name, node_unique_id,
            node_body_hash, node_configs_hash, node_persisted_descriptions_hash,
            node_macros_hash, node_contract_hash, node_database_representation
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (org_id, project_id, target_name, node_unique_id) DO UPDATE SET
            node_body_hash = EXCLUDED.node_body_hash,
            node_configs_hash = EXCLUDED.node_configs_hash,
            node_persisted_descriptions_hash = EXCLUDED.node_persisted_descriptions_hash,
            node_macros_hash = EXCLUDED.node_macros_hash,
            node_contract_hash = EXCLUDED.node_contract_hash,
            node_database_representation = EXCLUDED.node_database_representation
        "#,
    )
    .bind(&state.org_id)
    .bind(&state.project_id)
    .bind(&state.target_name)
    .bind(&state.node_unique_id)
    .bind(&state.node_body_hash)
    .bind(&state.node_configs_hash)
    .bind(&state.node_persisted_descriptions_hash)
    .bind(&state.node_macros_hash)
    .bind(&state.node_contract_hash)
    .bind(&state.node_database_representation)
    .execute(db.pool())
    .await
    .expect("seed node_states");
}
