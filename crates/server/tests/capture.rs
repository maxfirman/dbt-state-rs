use dbt_state_server::capture::*;
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;
use sqlx::PgPool;

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
        query_dependencies: vec![],
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
