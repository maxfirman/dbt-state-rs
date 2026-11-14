//! Scoped State history reads. Physical reuse always selects current state before
//! eligibility; data tests use logical UID scope, and provenance walks historical
//! state at the child's submission time. Keep these distinctions explicit.

use super::*;
use crate::query_cache::ModelExecutionType;
use crate::warehouse::Warehouse;
use sqlx::{Postgres, QueryBuilder};
use std::collections::HashMap;

/// Namespace range for diagnostic-only query evidence. `All` is used only
/// for an omitted namespace's rejection/Explain contract, never physical reuse.
pub(crate) enum QueryDiagnosticNamespaces<'a> {
    All,
    Exact(Option<&'a str>),
}

/// A name for a refused optimization's explanation. This intentionally cannot
/// carry cached results, epochs, producers or execution IDs into a candidate.
pub(crate) struct QueryDiagnosticReference {
    pub source_name: String,
}

// Physical completion order is independent of confirmation delivery order.
// A confirmed legacy row without an epoch is deliberately a permanent cold
// miss: neither a late confirmation nor historical Record delivery can prove
// that it replaced the unknown result. Pending rows are filtered out first.
const PHYSICAL_ORDER: &str =
    "last_modified_epoch DESC NULLS FIRST, confirmed_at DESC NULLS LAST, id DESC";

/// The physical warehouse family a dialect's history is scoped by; matches the
/// generated `executions.warehouse_family` column.
fn warehouse_family(dialect: &str) -> String {
    crate::warehouse::Warehouse::from_dialect(dialect).family_key(dialect)
}

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
        "SELECT DISTINCT ON (physical_target_key) * FROM executions"
    });
    query
        .push(" WHERE org_id = ")
        .push_bind(org_id)
        .push(" AND status = 'confirmed' AND warehouse_family = ")
        .push_bind(warehouse_family(dialect))
        .push(" AND NULLIF(table_namespace, '') IS NOT DISTINCT FROM ")
        .push_bind(crate::execution::non_blank_namespace(namespace));
    if let Some(target) = target {
        query
            .push(" AND physical_target_key = ")
            .push_bind(Warehouse::physical_scope_key(dialect, target));
    }
    query.push(" ORDER BY ");
    if target.is_none() {
        query.push("physical_target_key, ");
    }
    query.push(PHYSICAL_ORDER);
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

    /// Every confirmed build of a target in its physical scope, newest first
    /// (the first row is [`Store::current_target_state`]). Older rows never
    /// describe the target's current contents; they are read only to report
    /// which earlier build would have qualified (rejection codes), and to find
    /// the build of the workload's own execution type that an execution type
    /// change is judged against (never reused).
    pub async fn target_history(
        &self,
        org_id: &str,
        target_table: &str,
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<Vec<ExecutionRow>> {
        let mut query = QueryBuilder::new("SELECT ");
        query
            .push(ExecutionRow::COLUMNS)
            .push(" FROM executions WHERE org_id = ")
            .push_bind(org_id)
            .push(" AND status = 'confirmed' AND warehouse_family = ")
            .push_bind(warehouse_family(dialect))
            .push(" AND NULLIF(table_namespace, '') IS NOT DISTINCT FROM ")
            .push_bind(crate::execution::non_blank_namespace(table_namespace))
            .push(" AND physical_target_key = ")
            .push_bind(Warehouse::physical_scope_key(dialect, target_table))
            .push(" ORDER BY ")
            .push(PHYSICAL_ORDER)
            .push(" LIMIT 50");
        query
            .build_query_as::<ExecutionRow>()
            .fetch_all(&self.pool)
            .await
    }

    /// Diagnostic query evidence for one physical name across namespaces.
    /// Returns only a boolean: these rows must never become reuse/copy
    /// candidates, producer evidence, or freshness anchors.
    pub(crate) async fn has_target_query_diagnostic(
        &self,
        org_id: &str,
        target_table: &str,
        dialect: &str,
        matches: impl Fn(&ExecutionRow) -> bool,
    ) -> sqlx::Result<bool> {
        let mut query = QueryBuilder::new("SELECT ");
        query
            .push(ExecutionRow::COLUMNS)
            .push(" FROM executions WHERE org_id = ")
            .push_bind(org_id)
            .push(" AND status = 'confirmed' AND warehouse_family = ")
            .push_bind(warehouse_family(dialect))
            .push(" AND physical_target_key = ")
            .push_bind(Warehouse::physical_scope_key(dialect, target_table))
            .push(" ORDER BY confirmed_at DESC NULLS LAST, id DESC LIMIT 50");
        let rows = query
            .build_query_as::<ExecutionRow>()
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| row.into_checked(false).ok())
            .any(|row| matches(&row)))
    }

    /// Current query evidence under another physical name, only for rejection
    /// diagnostics. Latest physical state is selected before the identity
    /// predicate. The return type contains a name, never an execution row.
    pub(crate) async fn other_target_query_diagnostic(
        &self,
        org_id: &str,
        target_table: &str,
        dialect: &str,
        namespaces: QueryDiagnosticNamespaces<'_>,
        matches: impl Fn(&ExecutionRow) -> bool,
    ) -> sqlx::Result<Option<QueryDiagnosticReference>> {
        let mut query = QueryBuilder::new("WITH diagnostic_current AS (SELECT DISTINCT ON (NULLIF(table_namespace, ''), physical_target_key) * FROM executions WHERE org_id = ");
        query
            .push_bind(org_id)
            .push(" AND status = 'confirmed' AND warehouse_family = ")
            .push_bind(warehouse_family(dialect));
        if let QueryDiagnosticNamespaces::Exact(namespace) = namespaces {
            query
                .push(" AND NULLIF(table_namespace, '') IS NOT DISTINCT FROM ")
                .push_bind(crate::execution::non_blank_namespace(namespace));
        }
        query
            .push(" ORDER BY NULLIF(table_namespace, ''), physical_target_key, ")
            .push(PHYSICAL_ORDER)
            .push(") SELECT ")
            .push(ExecutionRow::COLUMNS)
            .push(" FROM diagnostic_current WHERE physical_target_key <> ")
            .push_bind(Warehouse::physical_scope_key(dialect, target_table))
            .push(" ORDER BY confirmed_at DESC NULLS LAST, id DESC LIMIT 50");
        let rows = query
            .build_query_as::<ExecutionRow>()
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| row.into_checked(false).ok())
            .find(|row| matches(row))
            .map(|row| QueryDiagnosticReference {
                source_name: row.raw_target_table.unwrap_or(row.target_table),
            }))
    }

    /// Last definition in the absent namespace, solely for the explanation of
    /// a fresh named scope. Never return its execution as physical evidence.
    pub(crate) async fn absent_scope_query_definition_changed(
        &self,
        org_id: &str,
        target_table: &str,
        dialect: &str,
        execution_type: i32,
        current_hash: Option<&str>,
    ) -> sqlx::Result<bool> {
        let mut query = current_physical_state(org_id, None, dialect, Some(target_table));
        query
            .push("SELECT ")
            .push(ExecutionRow::COLUMNS)
            .push(" FROM current_state");
        let row = query
            .build_query_as::<ExecutionRow>()
            .fetch_optional(&self.pool)
            .await?;
        Ok(row
            .filter(|row| row.execution_type == execution_type)
            .is_some_and(|row| {
                row.node_sql_hash
                    .as_deref()
                    .zip(current_hash)
                    .is_some_and(|(prior, current)| prior != current)
            }))
    }

    /// The CURRENT physical state of each named target (see
    /// [`Store::current_target_state`]), keyed by target. Targets with no
    /// confirmed build in this scope are absent. Used to resolve which build
    /// produced each input a submit reads (ADR 0007).
    pub async fn current_target_states(
        &self,
        org_id: &str,
        targets: &[String],
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<HashMap<String, ExecutionRow>> {
        if targets.is_empty() {
            return Ok(HashMap::new());
        }
        let requested: HashMap<_, _> = targets
            .iter()
            .map(|name| (Warehouse::physical_scope_key(dialect, name), name.clone()))
            .collect();
        let keys: Vec<_> = targets
            .iter()
            .map(|name| Warehouse::physical_scope_key(dialect, name))
            .collect();
        let mut query = QueryBuilder::new("SELECT DISTINCT ON (physical_target_key) ");
        query
            .push(ExecutionRow::COLUMNS)
            .push(" FROM executions WHERE org_id = ")
            .push_bind(org_id)
            .push(" AND status = 'confirmed' AND warehouse_family = ")
            .push_bind(warehouse_family(dialect))
            .push(" AND NULLIF(table_namespace, '') IS NOT DISTINCT FROM ")
            .push_bind(crate::execution::non_blank_namespace(table_namespace))
            .push(" AND physical_target_key = ANY(")
            .push_bind(keys)
            .push(") ORDER BY physical_target_key, ")
            .push(PHYSICAL_ORDER);
        let rows = query
            .build_query_as::<ExecutionRow>()
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let key = Warehouse::physical_scope_key(dialect, &r.target_table);
                (
                    requested
                        .get(&key)
                        .cloned()
                        .unwrap_or_else(|| r.target_table.clone()),
                    r,
                )
            })
            .collect())
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
              AND NULLIF(table_namespace, '') IS NOT DISTINCT FROM $4
              AND warehouse_family = $5
              AND status = 'confirmed'
            ORDER BY confirmed_at DESC NULLS LAST, id DESC LIMIT 1
            "#,
            ExecutionRow::COLUMNS
        ))
        .bind(org_id)
        .bind(node_unique_id)
        .bind(project_id)
        .bind(crate::execution::non_blank_namespace(table_namespace))
        .bind(warehouse_family(dialect))
        .bind(ModelExecutionType::DbtDataTest as i32)
        .fetch_optional(&self.pool)
        .await
    }

    /// Current seed loads with a seed's reuse identity under other names
    /// (#91): the latest confirmed row of each other target in the physical
    /// scope, kept only when it is a seed with these values and this seed
    /// logic hash. As for [`Store::find_copy_candidates`], a load since
    /// superseded in its own target is never returned.
    pub async fn find_seed_copy_candidates(
        &self,
        org_id: &str,
        q: &SeedCopyCandidateQuery<'_>,
    ) -> sqlx::Result<Vec<CopyCandidate>> {
        let mut query = current_physical_state(org_id, Some(q.table_namespace), q.dialect, None);
        query
            .push("SELECT target_table, execution_type, table_type, last_modified_epoch, execution_runtime_ms, node_namespace_logic_hash, input_tables FROM current_state WHERE last_modified_epoch IS NOT NULL AND physical_target_key <> '' AND execution_type = ")
            .push_bind(ModelExecutionType::Values as i32)
            .push(" AND node_sql_hash = ")
            .push_bind(q.seed_logic_hash)
            .push(" AND values_hash = ")
            .push_bind(q.values_hash)
            .push(" AND physical_target_key <> ")
            .push_bind(Warehouse::physical_scope_key(q.dialect,q.exclude_target))
            .push(" ORDER BY last_modified_epoch DESC NULLS LAST, target_table LIMIT ")
            .push_bind(q.limit)
            .push(" OFFSET ")
            .push_bind(q.offset);
        query
            .build_query_as::<CopyCandidate>()
            .fetch_all(&self.pool)
            .await
    }

    /// Find confirmed "equivalent models under another name" to clone FROM, in
    /// preference order (newest build first), with any of the query's logic
    /// identities. Which logic identity admits which candidate is for the
    /// caller (`result_copy::EquivalentLookup::admits`).
    ///
    /// VERIFIED LIVE: a SQL submit for a target that does not yet exist, when an
    /// equivalent model (same `table_namespace` + schema-abstracted logic) was
    /// already built under a DIFFERENT physical target, is answered with
    /// `ready_to_clone` cloning that equivalent into the submit target
    /// ("an equivalent model exists under another name so we cloned that one").
    /// Returns each source's physical name, warehouse table type and build epoch
    /// so the service can emit the clone DDL + `clone_required_last_modified_epoch`.
    /// Which candidates qualify beyond that is for the warehouse rules.
    pub async fn find_copy_candidates(
        &self,
        org_id: &str,
        q: &CopyCandidateQuery<'_>,
    ) -> sqlx::Result<Vec<CopyCandidate>> {
        // PHYSICAL-STATE INVARIANT: a candidate source may only be cloned if its
        // CURRENT physical contents match the requested logic. We reduce to the
        // latest confirmed row PER target in this physical warehouse scope
        // (DISTINCT ON), then filter the current states by logic and (when asked)
        // execution type. Another connection's same-named target is a different object.
        let mut query = current_physical_state(org_id, Some(q.table_namespace), q.dialect, None);
        query.push("SELECT target_table, execution_type, table_type, last_modified_epoch, execution_runtime_ms, node_namespace_logic_hash, input_tables FROM current_state WHERE last_modified_epoch IS NOT NULL AND physical_target_key <> '' AND node_namespace_logic_hash = ANY(")
            .push_bind(
                q.node_namespace_logic_hashes
                    .iter()
                    .map(|h| h.to_string())
                    .collect::<Vec<String>>(),
            )
            .push(")");
        if let Some(execution_type) = q.execution_type {
            query
                .push(" AND execution_type = ")
                .push_bind(execution_type);
        }
        query
            .push(" AND physical_target_key <> ")
            .push_bind(Warehouse::physical_scope_key(q.dialect, q.exclude_target))
            .push(" ORDER BY last_modified_epoch DESC NULLS LAST, target_table LIMIT ")
            .push_bind(q.limit);
        query
            .build_query_as::<CopyCandidate>()
            .fetch_all(&self.pool)
            .await
    }

    /// The copy-chain depth behind `target`: how many copy ancestors its
    /// current state has (0: a build). `None` when history is missing or the
    /// chain has more than `max_ancestors` ancestors.
    ///
    /// Follows immutable provenance at each copy's submission time. Later
    /// rebuilds of an ancestor cannot shorten an already completed copy chain.
    pub async fn copy_chain_depth(
        &self,
        org_id: &str,
        namespace: &str,
        dialect: &str,
        target: &str,
        max_ancestors: usize,
    ) -> sqlx::Result<Option<usize>> {
        let family = warehouse_family(dialect);
        let mut name = target.to_owned();
        let mut cutoff: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut child_id: Option<i64> = None;
        for depth in 0..=max_ancestors {
            // The head is current physical state. Ancestors remain immutable
            // snapshots bounded by their child's submission, not current rows.
            let order = if depth == 0 {
                PHYSICAL_ORDER
            } else {
                "confirmed_at DESC NULLS LAST, id DESC"
            };
            let row: Option<(i64, Option<String>, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
                &format!("SELECT id, clone_source_table, created_at FROM executions WHERE org_id=$1 AND NULLIF(table_namespace, '') IS NOT DISTINCT FROM $2 AND warehouse_family=$6 AND physical_target_key=$3 AND status='confirmed' AND ($4::timestamptz IS NULL OR confirmed_at <= $4) AND ($5::bigint IS NULL OR id < $5) ORDER BY {order} LIMIT 1")
            ).bind(org_id).bind(crate::execution::non_blank_namespace(Some(namespace))).bind(Warehouse::physical_scope_key(dialect,&name)).bind(cutoff).bind(child_id).bind(&family).fetch_optional(&self.pool).await?;
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

    /// A client-requested copy's source must be current: select physical
    /// history first, so a later incompatible write hides an older eligible
    /// source. `None`: no eligible source. `Some(logic)`: eligible, with the
    /// source's (optional) logic identity.
    pub async fn find_current_copy_source(
        &self,
        org_id: &str,
        q: &CopySourceQuery<'_>,
    ) -> sqlx::Result<Option<Option<String>>> {
        let mut query =
            current_physical_state(org_id, Some(q.table_namespace), q.dialect, Some(q.source));
        query
            .push("SELECT node_namespace_logic_hash FROM current_state WHERE execution_type = ")
            .push_bind(q.execution_type)
            .push(" AND last_modified_epoch = ")
            .push_bind(q.last_modified_epoch)
            .push(" AND table_type = ")
            .push_bind(q.table_type)
            .push(" AND clone_source_table IS NULL AND node_hash = ")
            .push_bind(q.node_hash);
        let row = query
            .build_query_as::<(Option<String>,)>()
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.0))
    }

    /// The clone source's current confirmed build, by its physical target name
    /// within a namespace: what a client-requested copy of it inherits
    /// ([`CopySourceBuild`]). `None` when the source has no confirmed build in
    /// scope.
    pub async fn find_copy_source_build(
        &self,
        org_id: &str,
        source_table: &str,
        table_namespace: Option<&str>,
        dialect: &str,
    ) -> sqlx::Result<Option<CopySourceBuild>> {
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
            .filter(|row| row.last_modified_epoch.is_some() && !row.target_table.is_empty())
            .map(|r| r.into_checked(true))
            .transpose()?
            .map(CopySourceBuild::of))
    }
}

/// What a client-requested copy (RegisterClone) inherits from its source's
/// current confirmed build (#93, VERIFIED LIVE 2026-11-11,
/// explain_post_copy_identity.jsonl).
#[derive(Debug, Clone, PartialEq)]
pub struct CopySourceBuild {
    /// Its schema-abstracted logic identity, which a later submit must have to
    /// adopt the copy.
    pub node_namespace_logic_hash: Option<String>,
    /// Its runtime: hosted reports the source build's runtime when it reuses
    /// the copy, although the copy's confirmation reports none.
    pub execution_runtime_ms: Option<i64>,
    /// The upstream relations it recorded, by physical name. `dbt state
    /// explain` gives a FRESH verdict only to an upstream the source recorded.
    pub input_names: Vec<String>,
}

impl CopySourceBuild {
    /// A source that is itself a client-requested copy passes on what it
    /// inherited.
    fn of(source: ExecutionRow) -> Self {
        let input_names = match &source.clone_source_inputs {
            Some(inherited) if source.node_sql_hash.is_none() => inherited.clone(),
            _ => source
                .input_tables
                .iter()
                .filter(|t| t.name != source.target_table)
                .map(|t| t.name.clone())
                .collect(),
        };
        Self {
            node_namespace_logic_hash: source.node_namespace_logic_hash,
            execution_runtime_ms: source.execution_runtime_ms,
            input_names,
        }
    }
}
