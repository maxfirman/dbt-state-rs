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
        decision_reason: match decision {
            DecisionKind::Build => dbt_state_server::decision::DecisionReason::FirstBuild,
            DecisionKind::Skip => dbt_state_server::decision::DecisionReason::Reused(
                dbt_state_server::decision::Reuse::Fresh,
            ),
            DecisionKind::Clone => dbt_state_server::decision::DecisionReason::ResultCopy,
        },
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
        matched_execution_id: None,
        freshness_tolerance_ms: None,
        build_age_ms: None,
        skip_rejection_reason: None,
        clone_rejection_reason: None,
        unmatched_execution_id: None,
        reads_relation: None,
        display_submitted_at_ms: None,
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

/// Ticket #30: the capture write records `matched_execution_id` on a SKIP/reuse
/// decision (the `executions.id` the decision matched) and leaves it NULL on a
/// build/first-build. Verified at the store seam by reading the persisted
/// `node_decisions.matched_execution_id` column back.
#[tokio::test]
async fn capture_records_matched_execution_id_on_skip_only() {
    let (pool, _schema) = schema_pool().await;

    // Seed a confirmed executions row to anchor the SKIP against (satisfies the
    // nullable FK). Its id is what a reuse decision records.
    let anchor_id: i64 = sqlx::query_scalar(
        r#"
        INSERT INTO executions (
            org_id, target_table, execution_type, dialect, input_tables,
            status, request_id, last_modified_epoch
        ) VALUES ('local', '"DB"."S"."T"', 1, 'snowflake', '[]'::jsonb,
                  'confirmed', 'seed-req', 100)
        RETURNING id
        "#,
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // A SKIP records the matched id; a build records NULL.
    let mut skip = sample(DecisionKind::Skip, "inv-1", "reused");
    skip.matched_execution_id = Some(anchor_id);
    capture_decision(&pool, skip).await;

    let mut build = sample(DecisionKind::Build, "inv-1", "fresh");
    build.matched_execution_id = None;
    capture_decision(&pool, build).await;

    let skip_matched: Option<i64> = sqlx::query_scalar(
        "SELECT matched_execution_id FROM node_decisions WHERE decision = 'skip'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        skip_matched,
        Some(anchor_id),
        "a SKIP records the matched executions.id"
    );

    let build_matched: Option<i64> = sqlx::query_scalar(
        "SELECT matched_execution_id FROM node_decisions WHERE decision = 'build'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        build_matched, None,
        "a build/first-build leaves matched_execution_id NULL"
    );
}

/// ADR 0006: every captured node decision records its decision reason as the
/// reason's stable lowercase string.
#[tokio::test]
async fn capture_records_the_decision_reason() {
    use dbt_state_server::decision::{DecisionReason, NoCandidate, Reuse};
    let (pool, _schema) = schema_pool().await;

    let mut first = sample(DecisionKind::Build, "inv-1", "first");
    first.decision_reason = DecisionReason::FirstBuild;
    capture_decision(&pool, first).await;
    let mut dropped = sample(DecisionKind::Build, "inv-1", "dropped");
    dropped.decision_reason =
        DecisionReason::NoReuseCandidate(NoCandidate::OwnTargetChangedOrMissing);
    capture_decision(&pool, dropped).await;
    let mut reused = sample(DecisionKind::Skip, "inv-1", "reused");
    reused.decision_reason = DecisionReason::Reused(Reuse::ToleratedStale);
    capture_decision(&pool, reused).await;

    let stored: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT node_name, decision_reason FROM node_decisions ORDER BY node_name")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        stored,
        [
            (
                "dropped".to_string(),
                Some("no_reuse_candidate.own_target_changed_or_missing".to_string())
            ),
            ("first".to_string(), Some("first_build".to_string())),
            (
                "reused".to_string(),
                Some("reused.tolerated_stale".to_string())
            ),
        ]
    );
}

/// Migration 0019 (#117): a view decision records whether its SQL reads a
/// relation, and Explain reads it back with the decision, org-scoped. Unknown
/// stays NULL.
#[tokio::test]
async fn capture_records_whether_a_view_reads_a_relation() {
    let (pool, _schema) = schema_pool().await;
    for (edid, reads) in [
        ("v-none", Some(false)),
        ("v-ref", Some(true)),
        ("v-?", None),
    ] {
        let mut view = sample(DecisionKind::Skip, "inv-1", edid);
        view.execution_type = 10;
        view.execution_decision_id = Some(edid.into());
        view.reads_relation = reads;
        capture_decision(&pool, view).await;
    }

    let store = dbt_state_server::store::Store::new(pool);
    let ids = ["v-none", "v-ref", "v-?"].map(String::from);
    let mut rows: Vec<(String, Option<bool>)> = store
        .fetch_explain_decisions("local", &ids)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.execution_decision_id.unwrap(), r.reads_relation))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        [
            ("v-?".to_string(), None),
            ("v-none".to_string(), Some(false)),
            ("v-ref".to_string(), Some(true)),
        ]
    );
    assert!(
        store
            .fetch_explain_decisions("other-org", &ids)
            .await
            .unwrap()
            .is_empty(),
        "another organization reads none of them"
    );
}
