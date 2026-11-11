//! Resource ownership is part of the harness interface, including panic cleanup.
mod support;

use dbt_state_proto::query_cache as qc;
use std::panic::{catch_unwind, AssertUnwindSafe};

async fn schema_exists(schema: &str) -> bool {
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&support::dsn())
        .await
        .unwrap();
    let exists = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1)")
        .bind(schema)
        .fetch_one(&admin)
        .await
        .unwrap();
    admin.close().await;
    exists
}

#[tokio::test]
async fn server_owner_stops_requests_and_removes_its_schema() {
    let server = support::start_server().await;
    let addr = server.addr;
    let schema = server.schema.clone();
    let mut sql = qc::sql_client::SqlClient::new(support::channel(addr).await);
    let response = sql
        .submit_enriched_sql(qc::SubmitEnrichedSqlRequest {
            target_table: Some("DB.S.T".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(support::response_variant(&response), "ready_to_execute");
    assert!(schema_exists(&schema).await);
    // Even an idle client must not keep a server or its schema alive.
    drop(server);
    assert!(!schema_exists(&schema).await);
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
}

#[tokio::test]
async fn panic_cleans_database_but_retains_project_evidence() {
    let server = support::start_server().await;
    let schema = server.schema.clone();
    let project = server.python_project("harness-failed-project");
    let path = project.path().to_owned();
    std::fs::write(path.join("evidence.txt"), "inspection evidence").unwrap();
    let result = catch_unwind(AssertUnwindSafe(move || {
        let _project = project;
        panic!("simulated assertion failure");
    }));
    assert!(result.is_err());
    assert_eq!(
        std::fs::read_to_string(path.join("evidence.txt")).unwrap(),
        "inspection evidence"
    );
    let result = catch_unwind(AssertUnwindSafe(move || {
        let _server = server;
        panic!("simulated assertion failure");
    }));
    assert!(result.is_err());
    assert!(!schema_exists(&schema).await);
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn successful_project_is_removed_when_its_owner_leaves_scope() {
    let server = support::start_server().await;
    let path = {
        let project = server.python_project("harness-successful-project");
        std::fs::write(project.path().join("result.txt"), "completed").unwrap();
        project.path().to_owned()
    };
    assert!(!path.exists());
}
