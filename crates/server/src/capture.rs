//! Best-effort capture of the dbt Platform-style domain model for the UI.
//!
//! On each decision the server records organization → project → environment →
//! invocation → node_decision so the UI can present an audit log and
//! cross-project/environment state. This is **additive and non-blocking**: the
//! gRPC decision path never waits on or fails because of capture. Callers spawn
//! `capture_decision` on the runtime and ignore its result.

use sqlx::PgPool;

use crate::store::InputTable;

/// The decision class, as stored in `node_decisions.decision`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecisionKind {
    Build,
    Skip,
    Clone,
}

impl DecisionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Skip => "skip",
            Self::Clone => "clone",
        }
    }
}

/// Everything needed to record one decision. Built by the service from the
/// request metadata + the computed verdict.
#[derive(Debug, Clone)]
pub struct CaptureInput {
    // identity / grouping
    pub org_id: String,
    pub external_invocation_id: Option<String>,
    pub session_id: Option<String>,
    pub project_external_id: Option<String>,
    pub project_name: Option<String>,
    pub environment_name: Option<String>,
    pub profile_name: Option<String>,
    pub dialect: Option<String>,
    pub database: Option<String>,
    // node
    pub node_unique_id: Option<String>,
    pub node_name: Option<String>,
    pub node_fqn: Option<String>,
    pub resource_type: Option<String>,
    pub execution_type: i32,
    // decision
    pub decision: DecisionKind,
    pub is_stale: bool,
    pub decision_description: Option<String>,
    pub request_id: Option<String>,
    pub execution_decision_id: Option<String>,
    pub node_body_hash: Option<String>,
    pub values_hash: Option<String>,
    pub table_namespace: Option<String>,
    pub target_table: Option<String>,
    pub default_schema: Option<String>,
    pub clone_source: Option<String>,
    pub clone_sqls: Option<Vec<String>>,
    pub input_tables: Vec<InputTable>,
    pub execution_runtime_ms: Option<i64>,
}

/// Record a decision. Best-effort: logs a warning on failure, never panics.
pub async fn capture_decision(pool: &PgPool, input: CaptureInput) {
    if let Err(e) = capture_inner(pool, &input).await {
        tracing::warn!(error = %e, "state capture failed (non-fatal)");
    }
}

async fn capture_inner(pool: &PgPool, i: &CaptureInput) -> sqlx::Result<()> {
    // 1. Organization (idempotent upsert on external_id).
    sqlx::query(
        r#"
        INSERT INTO organizations (external_id)
        VALUES ($1)
        ON CONFLICT (external_id) DO NOTHING
        "#,
    )
    .bind(&i.org_id)
    .execute(pool)
    .await?;

    // 2. Project (keyed by org + name). Updated external_id/dialect if newly known.
    let project_name = i
        .project_name
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let project_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO projects (org_id, external_id, name, dialect)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (org_id, name) DO UPDATE
            SET external_id = COALESCE(EXCLUDED.external_id, projects.external_id),
                dialect     = COALESCE(EXCLUDED.dialect, projects.dialect),
                updated_at  = now()
        RETURNING id
        "#,
    )
    .bind(&i.org_id)
    .bind(&i.project_external_id)
    .bind(&project_name)
    .bind(&i.dialect)
    .fetch_one(pool)
    .await?;

    // 3. Environment (keyed by project + name).
    let env_name = i
        .environment_name
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let environment_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO environments (org_id, project_id, name, profile_name, dialect, database, "schema")
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (project_id, name) DO UPDATE
            SET profile_name = COALESCE(EXCLUDED.profile_name, environments.profile_name),
                dialect      = COALESCE(EXCLUDED.dialect, environments.dialect),
                database     = COALESCE(EXCLUDED.database, environments.database),
                "schema"     = COALESCE(EXCLUDED."schema", environments."schema"),
                last_seen_at = now()
        RETURNING id
        "#,
    )
    .bind(&i.org_id)
    .bind(project_id)
    .bind(&env_name)
    .bind(&i.profile_name)
    .bind(&i.dialect)
    .bind(&i.database)
    .bind(&i.default_schema)
    .fetch_one(pool)
    .await?;

    // 4. Invocation (grouped by external_invocation_id). May be absent; when so
    //    we still create a row keyed on a synthesized id so decisions attach.
    let invocation_key = i
        .external_invocation_id
        .clone()
        .unwrap_or_else(|| format!("session:{}", i.session_id.as_deref().unwrap_or("unknown")));
    let (built, reused, cloned) = match i.decision {
        DecisionKind::Build => (1, 0, 0),
        DecisionKind::Skip => (0, 1, 0),
        DecisionKind::Clone => (0, 0, 1),
    };
    let invocation_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO invocations (
            org_id, environment_id, external_invocation_id, session_id,
            built_count, reused_count, cloned_count
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (org_id, external_invocation_id) DO UPDATE
            SET last_seen_at  = now(),
                built_count   = invocations.built_count + EXCLUDED.built_count,
                reused_count  = invocations.reused_count + EXCLUDED.reused_count,
                cloned_count  = invocations.cloned_count + EXCLUDED.cloned_count
        RETURNING id
        "#,
    )
    .bind(&i.org_id)
    .bind(environment_id)
    .bind(&invocation_key)
    .bind(&i.session_id)
    .bind(built)
    .bind(reused)
    .bind(cloned)
    .fetch_one(pool)
    .await?;

    // 5. Append the node decision (the audit log row).
    let clone_sqls = i
        .clone_sqls
        .as_ref()
        .map(|s| serde_json::to_value(s).unwrap_or(serde_json::Value::Null));
    let input_tables = serde_json::to_value(&i.input_tables).unwrap_or_default();
    sqlx::query(
        r#"
        INSERT INTO node_decisions (
            org_id, invocation_id, environment_id, node_unique_id, node_name,
            node_fqn, resource_type, execution_type, decision, is_stale,
            decision_description, request_id, execution_decision_id,
            node_body_hash, values_hash, table_namespace, target_table,
            default_schema, default_catalog, dialect, clone_source, clone_sqls, input_tables,
            execution_runtime_ms
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
                $19,$20,$21,$22,$23,$24)
        "#,
    )
    .bind(&i.org_id)
    .bind(invocation_id)
    .bind(environment_id)
    .bind(&i.node_unique_id)
    .bind(&i.node_name)
    .bind(&i.node_fqn)
    .bind(&i.resource_type)
    .bind(i.execution_type)
    .bind(i.decision.as_str())
    .bind(i.is_stale)
    .bind(&i.decision_description)
    .bind(&i.request_id)
    .bind(&i.execution_decision_id)
    .bind(&i.node_body_hash)
    .bind(&i.values_hash)
    .bind(&i.table_namespace)
    .bind(&i.target_table)
    .bind(&i.default_schema)
    .bind(&i.database)
    .bind(&i.dialect)
    .bind(&i.clone_source)
    .bind(clone_sqls)
    .bind(input_tables)
    .bind(i.execution_runtime_ms)
    .execute(pool)
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Executor;

    fn dsn() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://dbtstate:dbtstate@localhost:55441/dbtstate".to_string())
    }

    async fn schema_pool() -> (PgPool, String) {
        let schema = format!("captest_{}", uuid::Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn())
            .await
            .unwrap();
        admin
            .execute(format!("CREATE SCHEMA \"{schema}\"").as_str())
            .await
            .unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect({
                let schema = schema.clone();
                move |conn, _| {
                    let schema = schema.clone();
                    Box::pin(async move {
                        conn.execute(format!("SET search_path TO \"{schema}\"").as_str())
                            .await?;
                        Ok(())
                    })
                }
            })
            .connect(&dsn())
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        (pool, schema)
    }

    fn sample(decision: DecisionKind, inv: &str, node: &str) -> CaptureInput {
        CaptureInput {
            org_id: "local".into(),
            external_invocation_id: Some(inv.into()),
            session_id: Some("sess-1".into()),
            project_external_id: Some("123".into()),
            project_name: Some("jaffle_shop".into()),
            environment_name: Some("prod".into()),
            profile_name: Some("snowflake".into()),
            dialect: Some("snowflake".into()),
            database: Some("ANALYTICS_DB".into()),
            node_unique_id: Some(format!("model.jaffle_shop.{node}")),
            node_name: Some(node.into()),
            node_fqn: Some(format!("jaffle_shop.{node}")),
            resource_type: Some("model".into()),
            execution_type: 1,
            decision,
            is_stale: false,
            decision_description: Some("because".into()),
            request_id: Some("req-1".into()),
            execution_decision_id: Some("dec-1".into()),
            node_body_hash: Some("abc".into()),
            values_hash: None,
            table_namespace: Some("ns".into()),
            target_table: Some("\"DB\".\"S\".\"T\"".into()),
            default_schema: Some("s".into()),
            clone_source: None,
            clone_sqls: None,
            input_tables: vec![],
            execution_runtime_ms: Some(42),
        }
    }

    #[tokio::test]
    async fn capture_builds_domain_hierarchy_and_counts() {
        let (pool, _schema) = schema_pool().await;

        // Two decisions in one invocation: a build and a skip for different nodes.
        capture_decision(&pool, sample(DecisionKind::Build, "inv-1", "a")).await;
        capture_decision(&pool, sample(DecisionKind::Skip, "inv-1", "b")).await;

        let orgs: i64 = sqlx::query_scalar("SELECT count(*) FROM organizations")
            .fetch_one(&pool)
            .await
            .unwrap();
        let projects: i64 = sqlx::query_scalar("SELECT count(*) FROM projects")
            .fetch_one(&pool)
            .await
            .unwrap();
        let envs: i64 = sqlx::query_scalar("SELECT count(*) FROM environments")
            .fetch_one(&pool)
            .await
            .unwrap();
        let invs: i64 = sqlx::query_scalar("SELECT count(*) FROM invocations")
            .fetch_one(&pool)
            .await
            .unwrap();
        let decs: i64 = sqlx::query_scalar("SELECT count(*) FROM node_decisions")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(orgs, 1, "one org");
        assert_eq!(projects, 1, "one project");
        assert_eq!(envs, 1, "one environment");
        assert_eq!(invs, 1, "both decisions grouped into one invocation");
        assert_eq!(decs, 2, "two decision rows");

        // The invocation counters reflect 1 build + 1 reused.
        let (built, reused): (i32, i32) =
            sqlx::query_as("SELECT built_count, reused_count FROM invocations")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(built, 1);
        assert_eq!(reused, 1);
    }
}
