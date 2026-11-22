//! Named helpers for State the current protocol cannot produce: rows persisted
//! by an older server (legacy fingerprints, frozen pre-fix rows), corrupt
//! encodings, and node state captured from the hosted service. With
//! `fault.rs`, this is the only code that writes `executions` or `node_states`
//! with raw SQL; everything else seeds State through the protocol
//! (`scenario::Scenario::build` / `record`). Selected-field helpers retain their
//! original limited semantics; the complete-row importer validates historical
//! export columns separately.

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

/// Selected fields from a frozen older-server row, not a complete row export.
/// Omitted columns use current defaults; confirmation receipt time is synthetic.
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

/// Replace target history with the selected frozen fields. This is not a
/// verbatim historical-row restoration; prefer `import_complete_execution`
/// for upgrade lifecycle tests. Do not use where capture rows reference history.
pub async fn replace_with_frozen_execution(db: &impl Db, row: &FrozenExecution<'_>) {
    sqlx::query("DELETE FROM executions WHERE target_table = $1")
        .bind(row.target_table)
        .execute(db.pool())
        .await
        .expect("clear frozen target");
    append_frozen_execution(db, row).await;
}

/// Append a frozen pre-upgrade row without deleting correlated pending work.
/// Used to characterize old unknown-epoch history through later public calls.
pub async fn append_frozen_execution(db: &impl Db, row: &FrozenExecution<'_>) {
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

/// Import a complete old `row_to_json(executions)` export. Unlike
/// `FrozenExecution`, this preserves every exported semantic field and time.
/// Returns (old surrogate id, newly allocated surrogate id).
pub async fn import_complete_execution(db: &impl Db, row: &serde_json::Value) -> (i64, i64) {
    // Source 3021560 exported these 34 columns. Reject subsets rather than
    // silently claiming that defaults are historical evidence. Additional exported
    // columns are imported too; only columns absent from that historical schema
    // use current migration defaults.
    const OLD_COLUMNS: &[&str] = &[
        "id",
        "org_id",
        "target_table",
        "execution_type",
        "node_hash",
        "node_body_hash",
        "node_configs_hash",
        "node_contract_hash",
        "node_unique_id",
        "table_namespace",
        "dialect",
        "last_modified_epoch",
        "table_type",
        "execution_runtime_ms",
        "input_tables",
        "status",
        "request_id",
        "execution_decision_id",
        "created_at",
        "confirmed_at",
        "values_hash",
        "node_sql_hash",
        "execution_results",
        "project_id",
        "node_namespace_logic_hash",
        "clone_source_table",
        "node_template_hash",
        "warehouse_family",
        "clone_source_inputs",
        "physical_target_key",
        "node_profile_name",
        "node_project_name",
        "node_target_name",
        "display_submitted_at_ms",
    ];
    let object = row.as_object().expect("complete old row object");
    assert!(
        object.len() >= OLD_COLUMNS.len(),
        "complete historical export"
    );
    assert!(OLD_COLUMNS.iter().all(|key| object.contains_key(*key)));
    let old_id = row["id"].as_i64().expect("old surrogate id");
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT attname::text FROM pg_attribute WHERE attrelid = 'executions'::regclass \
         AND attnum > 0 AND NOT attisdropped AND attgenerated = '' AND attname <> 'id' \
         ORDER BY attnum",
    )
    .fetch_all(db.pool())
    .await
    .expect("execution schema columns");
    let imported: Vec<_> = columns
        .iter()
        .filter(|c| object.contains_key(c.as_str()))
        .cloned()
        .collect();
    // Only the surrogate PK and the two generated lookup keys may be omitted.
    assert_eq!(
        imported.len(),
        object.len() - 3,
        "all historical columns recognized"
    );
    let names = imported
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "INSERT INTO executions ({names}) SELECT {names} \
         FROM jsonb_populate_record(NULL::executions, $1) RETURNING id"
    );
    let new_id: i64 = sqlx::query_scalar(&sql)
        .bind(row)
        .fetch_one(db.pool())
        .await
        .expect("import complete historical execution");
    // Validate the setup, including original timestamps, bytes, inputs and
    // nulls. Generated lookup values remain in the fixture as provenance but
    // are computed by current migrations; transport UUIDs are not remapped.
    let restored: serde_json::Value = sqlx::query_scalar(
        "SELECT (SELECT jsonb_object_agg(key, value) FROM jsonb_each(to_jsonb(e)) \
         WHERE key = ANY($2)) FROM executions e WHERE id = $1",
    )
    .bind(new_id)
    .bind(&imported)
    .fetch_one(db.pool())
    .await
    .expect("validate imported row");
    let expected: serde_json::Map<_, _> = imported
        .iter()
        .map(|key| (key.clone(), row[key].clone()))
        .collect();
    assert_eq!(
        restored,
        serde_json::Value::Object(expected),
        "historical semantic fields preserved"
    );
    (old_id, new_id)
}

/// Restore chronological complete rows, remapping only surrogate source IDs.
/// Producer logic identities, epochs, context, UUIDs and every other semantic
/// field remain verbatim. A referenced source must precede its copy.
pub async fn import_complete_history(db: &impl Db, rows: &[serde_json::Value]) {
    let mut ids = std::collections::HashMap::new();
    for original in rows {
        let mut row = original.clone();
        for field in ["direct_copy_provenance", "submit_copy_provenance"] {
            if let Some(source_id) = row[field]["source_execution_id"].as_i64() {
                row[field]["source_execution_id"] = serde_json::json!(ids
                    .get(&source_id)
                    .expect("import original source before copy"));
            }
        }
        let (old, new) = import_complete_execution(db, &row).await;
        assert!(
            ids.insert(old, new).is_none(),
            "one historical row per surrogate ID"
        );
    }
}

/// A decision recorded before migration 0027: no rendered Volatile values.
/// Waits for the decision's best-effort capture first.
pub async fn forget_rendered_volatile_values(db: &impl Db, org_id: &str, decision_id: &str) {
    forget_decision_column(db, org_id, decision_id, "rendered_volatile_values").await;
}

/// A decision recorded before migration 0028: rendered values without the
/// evaluation flag. Waits for the decision's best-effort capture first.
pub async fn forget_volatile_evaluation_flag(db: &impl Db, org_id: &str, decision_id: &str) {
    forget_decision_column(db, org_id, decision_id, "volatile_evaluation_requested").await;
}

/// Set one nullable `node_decisions` column of the captured decision to NULL,
/// as before the migration that added it.
async fn forget_decision_column(db: &impl Db, org_id: &str, decision_id: &str, column: &str) {
    super::rows::wait_for_decision_capture(db, org_id, decision_id).await;
    let updated = sqlx::query(&format!(
        "UPDATE node_decisions SET {column} = NULL \
         WHERE org_id = $1 AND execution_decision_id = $2"
    ))
    .bind(org_id)
    .bind(decision_id)
    .execute(db.pool())
    .await
    .unwrap_or_else(|e| panic!("forget {column}: {e}"))
    .rows_affected();
    assert_eq!(updated, 1, "exactly the captured decision");
}

/// Rows recorded before #200: no data test has a history key and no copy
/// records the build it was made from. Returns the number of rows touched.
pub async fn forget_data_test_history_keys(db: &impl Db) -> u64 {
    Rows::All
        .update(
            db,
            "data_test_key = NULL, clone_source_execution_id = NULL",
            &[],
        )
        .await
}

/// Migrate `database` only up to migration 0029 and record State history as
/// a server from before Principals were recorded (#226) did: a confirmed
/// execution and a decision in `org`, and a row in the reserved `actors`
/// table that migration 0030 drops. Start a server on `database` afterwards
/// to upgrade it.
pub async fn record_history_before_principals(database: &super::TestDatabase, org: &str) {
    let pool = sqlx::PgPool::connect(&database.scoped_dsn())
        .await
        .expect("connect");
    let mut before = sqlx::migrate!("../server/migrations");
    before.migrations = before
        .migrations
        .iter()
        .filter(|migration| migration.version < 30)
        .cloned()
        .collect::<Vec<_>>()
        .into();
    before.run(&pool).await.expect("migrate to 0029");
    for statement in [
        "INSERT INTO executions (org_id, target_table, execution_type, dialect, status, request_id) \
         VALUES ($1, 'DB.S.T', 1, 'snowflake', 'confirmed', 'old-request')",
        "INSERT INTO projects (org_id, name) VALUES ($1, 'p')",
        "INSERT INTO environments (org_id, project_id, name) SELECT $1, id, 'prod' FROM projects",
        "INSERT INTO node_decisions (org_id, environment_id, execution_type, decision, \
                                     execution_decision_id, actor_id) \
         SELECT $1, id, 1, 'build', 'old-decision', 7 FROM environments",
        "INSERT INTO actors (org_id, kind) VALUES ($1, 'user')",
    ] {
        sqlx::query(statement)
            .bind(org)
            .execute(&pool)
            .await
            .expect(statement);
    }
    pool.close().await;
}
