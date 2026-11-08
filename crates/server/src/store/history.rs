//! Scoped State history reads. Physical reuse always selects current state before
//! eligibility; data tests use logical UID scope, and provenance walks historical
//! state at the child's submission time. Keep these distinctions explicit.

use super::*;
use sqlx::{Postgres, QueryBuilder};

const EXECUTION_COLUMNS: &str = "id, org_id, target_table, execution_type, node_body_hash, node_sql_hash, node_namespace_logic_hash, table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms, execution_results, input_tables, status, request_id, clone_source_table";
const PHYSICAL_COLUMNS: &str = "id, org_id, target_table, execution_type, node_body_hash, node_sql_hash, node_namespace_logic_hash, table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms, NULL::bytea AS execution_results, input_tables, status, request_id, clone_source_table";

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
    /// Select current physical state within the warehouse connection before
    /// checking logic, execution type and dialect options.
    /// An older matching execution cannot describe a target overwritten later.
    ///
    /// `node_template_hash` is the submit's unrendered-template identity, set
    /// only in `compare_unrendered_code` mode. When present, a row matches if
    /// EITHER its recorded rendered hash equals `node_sql_hash` OR its recorded
    /// template hash equals `node_template_hash` — the documented contract that
    /// a `compare_unrendered_code` node rebuilds ONLY when BOTH the rendered SQL
    /// and the unrendered template change (verified live; see
    /// `template_comparison.rs`). When `node_template_hash` is NULL (ordinary
    /// rendered comparison) only `node_sql_hash` is compared, as before.
    #[allow(clippy::too_many_arguments)]
    pub async fn find_confirmed(
        &self,
        org_id: &str,
        target_table: &str,
        execution_type: i32,
        node_sql_hash: Option<&str>,
        table_namespace: Option<&str>,
        dialect: &str,
        node_template_hash: Option<&str>,
        node_namespace_logic_hash: Option<&str>,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let mut query =
            current_physical_state(org_id, table_namespace, dialect, Some(target_table));
        query
            .push("SELECT ")
            .push(EXECUTION_COLUMNS)
            .push(" FROM current_state WHERE execution_type = ")
            .push_bind(execution_type)
            .push(" AND (node_sql_hash IS NOT DISTINCT FROM ")
            .push_bind(node_sql_hash)
            .push(" OR (")
            .push_bind(node_template_hash)
            .push("::text IS NOT NULL AND node_template_hash IS NOT DISTINCT FROM ")
            .push_bind(node_template_hash)
            .push("))")
            // Retain upgrade invalidation in addition to the strict dependency binding.
            .push(" AND (node_namespace_logic_hash IS NULL OR ")
            .push_bind(node_namespace_logic_hash)
            .push("::text IS NULL OR node_namespace_logic_hash IS NOT DISTINCT FROM ")
            .push_bind(node_namespace_logic_hash)
            .push(") AND dialect = ")
            .push_bind(dialect);
        let row = query
            .build_query_as::<RawRow>()
            .fetch_optional(&self.pool)
            .await?;
        row.map(TryInto::try_into).transpose()
    }

    /// Tests have no physical target. Scope identity by project and warehouse,
    /// then compare the latest outcome's logic rather than resurrecting history.
    ///
    /// `node_template_hash` is the submit's unrendered-template identity, set
    /// only in `compare_unrendered_code` mode. When present, a row matches if
    /// EITHER its recorded rendered hash equals `node_sql_hash` OR its recorded
    /// template hash equals `node_template_hash` — the same dual-identity
    /// contract applied to models in `find_confirmed` (a cuc node rebuilds ONLY
    /// when BOTH the rendered SQL and the template change). Review #2 finding 6
    /// FOLLOW-UP: the data-test lookup previously compared only `node_sql_hash`,
    /// so a cuc data test with an unchanged template but a changed interpolated
    /// rendered literal wrongly EXECUTED where the hosted service reuses it.
    #[allow(clippy::too_many_arguments)] // Explicit independent identity/scope dimensions.
    pub async fn find_confirmed_test(
        &self,
        org_id: &str,
        node_unique_id: &str,
        project_id: Option<&str>,
        table_namespace: Option<&str>,
        node_sql_hash: Option<&str>,
        node_template_hash: Option<&str>,
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
                WHERE org_id = $1 AND node_unique_id = $2 AND execution_type = 8
                  AND project_id IS NOT DISTINCT FROM $3
                  AND table_namespace IS NOT DISTINCT FROM $4
                  AND warehouse_family = $7
                  AND status = 'confirmed'
                ORDER BY confirmed_at DESC NULLS LAST, id DESC LIMIT 1
            ) current_execution
            WHERE (
                node_sql_hash IS NOT DISTINCT FROM $5
                OR ($6 IS NOT NULL AND node_template_hash IS NOT DISTINCT FROM $6)
            )
            "#,
        )
        .bind(org_id)
        .bind(node_unique_id)
        .bind(project_id)
        .bind(table_namespace)
        .bind(node_sql_hash)
        .bind(node_template_hash)
        .bind(crate::warehouse::Warehouse::from_dialect(dialect).family_key(dialect))
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
        dialect: &str,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let mut query =
            current_physical_state(org_id, table_namespace, dialect, Some(target_table));
        query
            .push("SELECT ")
            .push(EXECUTION_COLUMNS)
            .push(" FROM current_state WHERE execution_type = 9 AND values_hash = ")
            .push_bind(values_hash)
            .push(" AND node_sql_hash = ")
            .push_bind(node_sql_hash);
        let row = query
            .build_query_as::<RawRow>()
            .fetch_optional(&self.pool)
            .await?;
        row.map(TryInto::try_into).transpose()
    }

    /// The newest confirmed execution describing a physical target — the
    /// CURRENT physical state — for the cross-environment clone-adoption path.
    ///
    /// PHYSICAL-STATE INVARIANT: a historical execution must never be treated as
    /// evidence of the current contents of a physical object. The clone-origin
    /// marker (`clone_source_table IS NOT NULL`) is therefore NOT part of the
    /// selection: if a cloned target was later rebuilt normally, that ordinary
    /// rebuild is the current state and this returns it. The CALLER then adopts
    /// the row only if it is itself clone-origin AND its inherited
    /// schema-abstracted logic matches the submit (see
    /// `history::prepare_sql`). Selecting the latest row first and
    /// filtering afterwards guarantees a stale clone-origin row cannot be
    /// resurrected once the object has been overwritten by a non-clone build.
    ///
    /// `execution_results` is intentionally NOT selected (returned as NULL):
    /// this lookup feeds only clone adoption, which never serves a row's cached
    /// results (clone-origin rows carry none, and the caller overwrites
    /// `input_tables`). Decoding a corrupt-but-irrelevant results blob here
    /// would spuriously fail an otherwise cold-miss submit; the result-serving
    /// queries (`find_confirmed`, `find_confirmed_test`, `find_confirmed_values`)
    /// still surface corruption as an INTERNAL error.
    pub async fn find_latest_confirmed_target(
        &self,
        org_id: &str,
        target_table: &str,
        execution_type: i32,
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        // PHYSICAL-STATE INVARIANT (review issue #1 follow-up): select the
        // target's CURRENT state within the physical warehouse scope (latest
        // confirmed row, any provenance/type/dialect option) FIRST, then check
        // execution_type and exact dialect. A build of another type or dialect
        // option in the SAME warehouse cannot expose stale clone history.
        let mut query =
            current_physical_state(org_id, table_namespace, dialect, Some(target_table));
        // Physical evidence does not serve cached results; irrelevant corrupt bytes
        // must not fail clone adoption. Result-serving queries still decode them.
        query
            .push("SELECT ")
            .push(PHYSICAL_COLUMNS)
            .push(" FROM current_state WHERE execution_type = ")
            .push_bind(execution_type)
            .push(" AND dialect = ")
            .push_bind(dialect);
        let row = query
            .build_query_as::<RawRow>()
            .fetch_optional(&self.pool)
            .await?;
        row.map(TryInto::try_into).transpose()
    }

    /// True if ANY confirmed execution exists for this target in its physical
    /// warehouse scope (regardless of execution_type or fingerprint). This
    /// is the PHYSICAL-EXISTENCE signal used to distinguish a genuine first
    /// build from a rebuild whose logic fingerprint no longer matches — hosted
    /// reports the rebuild rationale ("query didn't match or upstream out of
    /// date") when the target existed. Review #2 5th follow-up P1 #4.
    pub async fn target_has_confirmed_history(
        &self,
        org_id: &str,
        target_table: &str,
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<bool> {
        let exists: Option<i32> = sqlx::query_scalar(
            r#"SELECT 1 FROM executions
               WHERE org_id = $1 AND target_table = $2 AND status = 'confirmed'
                 AND warehouse_family = $3
                 AND table_namespace IS NOT DISTINCT FROM $4
               LIMIT 1"#,
        )
        .bind(org_id)
        .bind(target_table)
        .bind(crate::warehouse::Warehouse::from_dialect(dialect).family_key(dialect))
        .bind(table_namespace)
        .fetch_optional(&self.pool)
        .await?;
        Ok(exists.is_some())
    }

    /// The newest confirmed execution in a namespace carrying a given
    /// schema-abstracted logic identity. This historical accessor does not prove
    /// current physical contents; reuse paths use the current-state lookups below.
    pub async fn find_confirmed_ns_logic(
        &self,
        org_id: &str,
        table_namespace: &str,
        node_namespace_logic_hash: &str,
        dialect: &str,
    ) -> sqlx::Result<Option<ExecutionRow>> {
        let row = sqlx::query_as::<_, RawRow>(
            r#"
            SELECT id, org_id, target_table, execution_type, node_body_hash, node_sql_hash,
                   node_namespace_logic_hash,
                   table_namespace, node_unique_id, last_modified_epoch, execution_runtime_ms,
                   execution_results, input_tables, status, request_id, clone_source_table
            FROM executions
            WHERE org_id = $1 AND table_namespace = $2
              AND warehouse_family = $4
              AND node_namespace_logic_hash = $3 AND status = 'confirmed'
            ORDER BY confirmed_at DESC NULLS LAST, id DESC
            LIMIT 1
            "#,
        )
        .bind(org_id)
        .bind(table_namespace)
        .bind(node_namespace_logic_hash)
        .bind(crate::warehouse::Warehouse::from_dialect(dialect).family_key(dialect))
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
        query.push("SELECT node_namespace_logic_hash FROM current_state WHERE execution_type = 5 AND last_modified_epoch = ")
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
            .push(EXECUTION_COLUMNS)
            .push(" FROM current_state");
        let row = query
            .build_query_as::<RawRow>()
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .map(TryInto::try_into)
            .transpose()?
            .and_then(|r: ExecutionRow| r.node_namespace_logic_hash))
    }
}
