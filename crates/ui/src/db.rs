//! Read-only data access for the UI.
#![allow(dead_code)] // read models expose the full domain surface; not every field is shown yet

//! server writes to (the UI never mutates state in Phase 1).

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Connect a small read pool. `DATABASE_URL` defaults to the local dev dsn.
pub async fn connect() -> anyhow::Result<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://dbtstate:dbtstate@localhost:55441/dbtstate".to_string());
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await?;
    Ok(pool)
}

// ---- Read models -----------------------------------------------------------

#[derive(Debug, sqlx::FromRow)]
pub struct OverviewTotals {
    pub built: i64,
    pub reused: i64,
    pub cloned: i64,
    pub invocations: i64,
    pub projects: i64,
    pub environments: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ProjectRow {
    pub id: i64,
    pub name: String,
    pub dialect: Option<String>,
    pub environment_count: i64,
    pub built: i64,
    pub reused: i64,
    pub cloned: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct EnvironmentRow {
    pub id: i64,
    pub name: String,
    pub profile_name: Option<String>,
    pub dialect: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub dbt_state_enabled: bool,
    pub is_deferrable: bool,
    pub last_seen_at: chrono::DateTime<chrono::Utc>,
    pub built: i64,
    pub reused: i64,
    pub cloned: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct InvocationRow {
    pub id: i64,
    pub external_invocation_id: Option<String>,
    pub project_name: String,
    pub environment_name: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub built_count: i32,
    pub reused_count: i32,
    pub cloned_count: i32,
}

#[derive(Debug, sqlx::FromRow)]
pub struct DecisionRow {
    pub id: i64,
    pub node_name: Option<String>,
    pub node_unique_id: Option<String>,
    pub node_fqn: Option<String>,
    pub resource_type: Option<String>,
    pub decision: String,
    pub is_stale: bool,
    pub decision_description: Option<String>,
    pub target_table: Option<String>,
    pub node_body_hash: Option<String>,
    pub clone_source: Option<String>,
    pub execution_runtime_ms: Option<i64>,
    pub input_tables: serde_json::Value,
    pub query_dependencies: serde_json::Value,
    pub clone_sqls: Option<serde_json::Value>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

// ---- Queries ---------------------------------------------------------------

pub async fn overview_totals(pool: &PgPool) -> sqlx::Result<OverviewTotals> {
    sqlx::query_as::<_, OverviewTotals>(
        r#"
        SELECT
            COALESCE(SUM(CASE WHEN decision = 'build' THEN 1 ELSE 0 END), 0)::bigint AS built,
            COALESCE(SUM(CASE WHEN decision = 'skip'  THEN 1 ELSE 0 END), 0)::bigint AS reused,
            COALESCE(SUM(CASE WHEN decision = 'clone' THEN 1 ELSE 0 END), 0)::bigint AS cloned,
            (SELECT count(*) FROM invocations)::bigint AS invocations,
            (SELECT count(*) FROM projects)::bigint AS projects,
            (SELECT count(*) FROM environments)::bigint AS environments
        FROM node_decisions
        "#,
    )
    .fetch_one(pool)
    .await
}

/// Daily built-vs-reused counts for the last `days` days (for the chart).
pub async fn daily_built_reused(
    pool: &PgPool,
    days: i64,
) -> sqlx::Result<Vec<(chrono::NaiveDate, i64, i64)>> {
    let rows = sqlx::query_as::<_, (chrono::NaiveDate, i64, i64)>(
        r#"
        SELECT created_at::date AS day,
               COALESCE(SUM(CASE WHEN decision = 'build' THEN 1 ELSE 0 END), 0)::bigint AS built,
               COALESCE(SUM(CASE WHEN decision IN ('skip','clone') THEN 1 ELSE 0 END), 0)::bigint AS reused
        FROM node_decisions
        WHERE created_at >= now() - ($1 || ' days')::interval
        GROUP BY day
        ORDER BY day
        "#,
    )
    .bind(days.to_string())
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn projects(pool: &PgPool) -> sqlx::Result<Vec<ProjectRow>> {
    sqlx::query_as::<_, ProjectRow>(
        r#"
        SELECT p.id, p.name, p.dialect,
               (SELECT count(*) FROM environments e WHERE e.project_id = p.id)::bigint AS environment_count,
               COALESCE(SUM(CASE WHEN d.decision='build' THEN 1 ELSE 0 END),0)::bigint AS built,
               COALESCE(SUM(CASE WHEN d.decision='skip'  THEN 1 ELSE 0 END),0)::bigint AS reused,
               COALESCE(SUM(CASE WHEN d.decision='clone' THEN 1 ELSE 0 END),0)::bigint AS cloned
        FROM projects p
        LEFT JOIN environments e ON e.project_id = p.id
        LEFT JOIN node_decisions d ON d.environment_id = e.id
        GROUP BY p.id, p.name, p.dialect
        ORDER BY p.name
        "#,
    )
    .fetch_all(pool)
    .await
}

pub async fn environments_for_project(
    pool: &PgPool,
    project_id: i64,
) -> sqlx::Result<Vec<EnvironmentRow>> {
    sqlx::query_as::<_, EnvironmentRow>(
        r#"
        SELECT e.id, e.name, e.profile_name, e.dialect, e.database, e."schema",
               e.dbt_state_enabled, e.is_deferrable, e.last_seen_at,
               COALESCE(SUM(CASE WHEN d.decision='build' THEN 1 ELSE 0 END),0)::bigint AS built,
               COALESCE(SUM(CASE WHEN d.decision='skip'  THEN 1 ELSE 0 END),0)::bigint AS reused,
               COALESCE(SUM(CASE WHEN d.decision='clone' THEN 1 ELSE 0 END),0)::bigint AS cloned
        FROM environments e
        LEFT JOIN node_decisions d ON d.environment_id = e.id
        WHERE e.project_id = $1
        GROUP BY e.id
        ORDER BY e.name
        "#,
    )
    .bind(project_id)
    .fetch_all(pool)
    .await
}

pub async fn project_name(pool: &PgPool, project_id: i64) -> sqlx::Result<Option<String>> {
    sqlx::query_scalar::<_, String>("SELECT name FROM projects WHERE id = $1")
        .bind(project_id)
        .fetch_optional(pool)
        .await
}

pub async fn recent_invocations(pool: &PgPool, limit: i64) -> sqlx::Result<Vec<InvocationRow>> {
    sqlx::query_as::<_, InvocationRow>(
        r#"
        SELECT i.id, i.external_invocation_id, p.name AS project_name, e.name AS environment_name,
               i.started_at, i.built_count, i.reused_count, i.cloned_count
        FROM invocations i
        JOIN environments e ON e.id = i.environment_id
        JOIN projects p ON p.id = e.project_id
        ORDER BY i.started_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
}

pub async fn invocation(pool: &PgPool, id: i64) -> sqlx::Result<Option<InvocationRow>> {
    sqlx::query_as::<_, InvocationRow>(
        r#"
        SELECT i.id, i.external_invocation_id, p.name AS project_name, e.name AS environment_name,
               i.started_at, i.built_count, i.reused_count, i.cloned_count
        FROM invocations i
        JOIN environments e ON e.id = i.environment_id
        JOIN projects p ON p.id = e.project_id
        WHERE i.id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

pub async fn decisions_for_invocation(
    pool: &PgPool,
    invocation_id: i64,
) -> sqlx::Result<Vec<DecisionRow>> {
    sqlx::query_as::<_, DecisionRow>(
        r#"
        SELECT id, node_name, node_unique_id, node_fqn, resource_type, decision,
               is_stale, decision_description, target_table, node_body_hash,
               clone_source, execution_runtime_ms, input_tables, query_dependencies, clone_sqls, created_at
        FROM node_decisions
        WHERE invocation_id = $1
        ORDER BY created_at, id
        "#,
    )
    .bind(invocation_id)
    .fetch_all(pool)
    .await
}

/// Latest decision per node for an environment (for the catalog / lineage view).
pub async fn latest_decisions_for_environment(
    pool: &PgPool,
    environment_id: i64,
) -> sqlx::Result<Vec<DecisionRow>> {
    sqlx::query_as::<_, DecisionRow>(
        r#"
        SELECT DISTINCT ON (node_unique_id)
               id, node_name, node_unique_id, node_fqn, resource_type, decision,
               is_stale, decision_description, target_table, node_body_hash,
               clone_source, execution_runtime_ms, input_tables, query_dependencies, clone_sqls, created_at
        FROM node_decisions
        WHERE environment_id = $1 AND node_unique_id IS NOT NULL
        ORDER BY node_unique_id, created_at DESC
        "#,
    )
    .bind(environment_id)
    .fetch_all(pool)
    .await
}
