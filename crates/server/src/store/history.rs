//! Scoped State history reads. Physical reuse always selects current state before
//! eligibility; data tests use logical UID scope, and provenance walks historical
//! state at the child's submission time. Keep these distinctions explicit.

use super::*;
use crate::query_cache::ModelExecutionType;
use sqlx::{Postgres, QueryBuilder};

/// A single target or all targets within the same physical warehouse scope.
/// Callers append eligibility conditions outside the limited/distinct selection.
/// This builds one query, preserving each caller's existing read/error ordering.
fn current_physical_state<'a>(
    org_id: &'a str,
    namespace: Option<&'a str>,
    dialect: &str,
    target: Option<&'a str>,
) -> QueryBuilder<'a, Postgres> {
    let mut query = QueryBuilder::new("WITH current_state AS (");
    query.push(if target.is_some() {
        "SELECT * FROM executions"
    } else {
        "SELECT DISTINCT ON (target_table) * FROM executions"
    });
    query
        .push(" WHERE org_id = ")
        .push_bind(org_id)
        .push(" AND status = 'confirmed' AND warehouse_family = ")
        .push_bind(crate::warehouse::Warehouse::from_dialect(dialect).family_key(dialect))
        .push(" AND table_namespace IS NOT DISTINCT FROM ")
        .push_bind(namespace);
    if let Some(target) = target {
        query.push(" AND target_table = ").push_bind(target);
    }
    query.push(" ORDER BY ");
    if target.is_none() {
        query.push("target_table, ");
    }
    query.push("confirmed_at DESC NULLS LAST, id DESC");
    if target.is_some() {
        query.push(" LIMIT 1");
    }
    query.push(") ");
    query
}

impl Store {
    /// The CURRENT physical state of a target: its latest confirmed execution
    /// within the org, warehouse family and connection namespace, whatever its
    /// provenance, execution type or dialect options. `None` means the target
    /// has no confirmed build in this scope.
    ///
    /// PHYSICAL-STATE INVARIANT: an older execution cannot describe a target
    /// overwritten later, so identity, provenance and eligibility are checked on
    /// THIS row only (see `reuse_candidate`), never by filtering before the
    /// latest-row selection (review issue #1 P0#2).
    pub async fn current_target_state(
        &self,
        org_id: &str,
        target_table: &str,
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let mut query =
            current_physical_state(org_id, table_namespace, dialect, Some(target_table));
        query
            .push("SELECT ")
            .push(ExecutionRow::COLUMNS)
            .push(" FROM current_state");
        query
            .build_query_as::<ExecutionRow>()
            .fetch_optional(&self.pool)
            .await
    }

    /// Data tests have no physical target. Their latest confirmed outcome is
    /// scoped by node UID, project, namespace and warehouse family; identity is
    /// compared on that row rather than resurrecting older history.
    pub async fn latest_data_test(
        &self,
        org_id: &str,
        node_unique_id: &str,
        project_id: Option<&str>,
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        sqlx::query_as::<_, ExecutionRow>(&format!(
            r#"
            SELECT {} FROM executions
            WHERE org_id = $1 AND node_unique_id = $2 AND execution_type = $6
              AND project_id IS NOT DISTINCT FROM $3
              AND table_namespace IS NOT DISTINCT FROM $4
              AND warehouse_family = $5
              AND status = 'confirmed'
            ORDER BY confirmed_at DESC NULLS LAST, id DESC LIMIT 1
            "#,
            ExecutionRow::COLUMNS
        ))
        .bind(org_id)
        .bind(node_unique_id)
        .bind(project_id)
        .bind(table_namespace)
        .bind(crate::warehouse::Warehouse::from_dialect(dialect).family_key(dialect))
        .bind(ModelExecutionType::DbtDataTest as i32)
        .fetch_optional(&self.pool)
        .await
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
        dialect: &str,
    ) -> sqlx::Result<Option<CloneEquivalent>> {
        // PHYSICAL-STATE INVARIANT: a candidate source may only be cloned if its
        // CURRENT physical contents match the requested logic. We reduce to the
        // latest confirmed row PER target in this physical warehouse scope
        // (DISTINCT ON), then filter the current states by logic and execution
        // type. Another connection's same-named target is a different object.
        type CloneEquivRow = (String, Option<String>, Option<i64>, Option<i64>);
        let mut query = current_physical_state(org_id, Some(table_namespace), dialect, None);
        query.push("SELECT target_table, table_type, last_modified_epoch, execution_runtime_ms FROM current_state WHERE node_namespace_logic_hash = ")
            .push_bind(node_namespace_logic_hash).push(" AND execution_type = ").push_bind(execution_type)
            .push(" AND target_table <> ").push_bind(exclude_target)
            .push(" ORDER BY last_modified_epoch DESC NULLS LAST, target_table");
        if crate::warehouse::Warehouse::from_dialect(dialect).family_key(dialect) != "duckdb" {
            query.push(" LIMIT 1");
        }
        let rows = query
            .build_query_as::<CloneEquivRow>()
            .fetch_all(&self.pool)
            .await?;
        let duckdb = dialect.eq_ignore_ascii_case("duckdb");
        let target = crate::sql_identity::duckdb_relation(exclude_target);
        for (source_table, table_type, last_modified_epoch, execution_runtime_ms) in rows {
            let depth = if duckdb {
                let Some(source) = crate::sql_identity::duckdb_relation(&source_table) else {
                    continue;
                };
                if target.as_ref().is_none_or(|t| {
                    t.catalog() != source.catalog() || t.schema() == source.schema()
                }) {
                    continue;
                }
                let Some(depth) = self
                    .duckdb_copy_depth(org_id, table_namespace, &source_table)
                    .await?
                else {
                    continue;
                };
                depth
            } else {
                0
            };
            return Ok(Some(CloneEquivalent {
                source_table,
                table_type,
                last_modified_epoch,
                execution_runtime_ms,
                duckdb_copy_depth: depth,
            }));
        }
        Ok(None)
    }

    /// Follow immutable provenance at each copy's submission time. Later
    /// rebuilds of an ancestor cannot shorten an already completed copy chain.
    /// Missing history and more than two ancestors remain ineligible.
    async fn duckdb_copy_depth(
        &self,
        org_id: &str,
        namespace: &str,
        target: &str,
    ) -> sqlx::Result<Option<usize>> {
        let mut name = target.to_owned();
        let mut cutoff: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut child_id: Option<i64> = None;
        for depth in 0..=2 {
            let row: Option<(i64, Option<String>, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
                "SELECT id, clone_source_table, created_at FROM executions WHERE org_id=$1 AND table_namespace=$2 AND warehouse_family='duckdb' AND target_table=$3 AND status='confirmed' AND ($4::timestamptz IS NULL OR confirmed_at <= $4) AND ($5::bigint IS NULL OR id < $5) ORDER BY confirmed_at DESC NULLS LAST, id DESC LIMIT 1"
            ).bind(org_id).bind(namespace).bind(&name).bind(cutoff).bind(child_id).fetch_optional(&self.pool).await?;
            let Some((id, source, created_at)) = row else {
                return Ok(None);
            };
            match source {
                None => return Ok(Some(depth)),
                Some(source) => {
                    name = source;
                    cutoff = Some(created_at);
                    child_id = Some(id);
                }
            }
        }
        Ok(None)
    }

    /// Compile-time DuckDB initialization needs current, confirmed ET5 source
    /// provenance. Select physical history first; a later incompatible write
    /// must hide an older eligible source.
    pub async fn find_duckdb_incremental_source(
        &self,
        org_id: &str,
        source: &str,
        namespace: &str,
        epoch: i64,
        identity: &str,
    ) -> sqlx::Result<Option<Option<String>>> {
        let mut query = current_physical_state(org_id, Some(namespace), "duckdb", Some(source));
        query.push("SELECT node_namespace_logic_hash FROM current_state WHERE execution_type = ")
            .push_bind(ModelExecutionType::DeleteInsert as i32)
            .push(" AND last_modified_epoch = ")
            .push_bind(epoch).push(" AND table_type = 'DUCKDB CTAS TABLE' AND clone_source_table IS NULL AND node_hash = ")
            .push_bind(identity);
        let row = query
            .build_query_as::<(Option<String>,)>()
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.0))
    }

    /// Look up the clone source's confirmed logic identity by its physical
    /// target name within a namespace. Returns the inherited
    /// `node_namespace_logic_hash` to stamp on a clone-origin row.
    pub async fn find_source_ns_logic_hash(
        &self,
        org_id: &str,
        source_table: &str,
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<Option<String>> {
        // Select the latest source row within the requested physical warehouse
        // scope. The namespace identifies the warehouse connection; a same-named
        // relation in another namespace cannot overwrite this source. A NULL
        // hash (for example template mode or empty SQL) inherits nothing.
        let mut query =
            current_physical_state(org_id, table_namespace, dialect, Some(source_table));
        query
            .push("SELECT ")
            .push(ExecutionRow::COLUMNS)
            .push(" FROM current_state");
        let row = query
            .build_query_as::<ExecutionRow>()
            .fetch_optional(&self.pool)
            .await?;
        // Corrupt recorded evidence on the source row fails the lookup.
        Ok(row
            .map(|r| r.into_checked(true))
            .transpose()?
            .and_then(|r| r.node_namespace_logic_hash))
    }
}
