//! Shared integration-test support: spin up the dbt State server in-process
//! against a freshly-created, uniquely-named Postgres schema so each test is
//! isolated and repeatable.
//!
//! Included by both `differential.rs` and `robustness.rs` via `#[path]` so the
//! isolated-schema setup lives in exactly one place. Kept as a `tests/` module
//! (rather than the harness library) so the `dbt-state-server`/`sqlx`/`tokio`
//! deps stay dev-only.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use dbt_state_proto::query_cache as qc;
use dbt_state_server::AppState;
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tonic::transport::Channel;

pub const DEFAULT_DSN: &str = "postgres://dbtstate:dbtstate@localhost:55441/dbtstate";

/// The DSN to use, overridable via DATABASE_URL.
pub fn dsn() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string())
}

/// Spin up our server in-process against a fresh isolated schema. Returns the
/// bound address and the schema name (for cleanup / row assertions).
pub async fn start_server() -> (SocketAddr, String) {
    let schema = format!("difftest_{}", uuid::Uuid::new_v4().simple());

    // Admin pool to create the schema.
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn())
        .await
        .expect("connect admin");
    sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
        .execute(&admin)
        .await
        .expect("create schema");

    // Server pool pinned to the schema via search_path.
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .after_connect({
            let schema = schema.clone();
            move |conn, _meta| {
                let schema = schema.clone();
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute(format!("SET search_path TO \"{schema}\"").as_str())
                        .await?;
                    Ok(())
                })
            }
        })
        .connect(&dsn())
        .await
        .expect("connect server pool");

    // Run migrations into the schema.
    sqlx::migrate!("../server/migrations")
        .run(&pool)
        .await
        .expect("migrate");

    let state = AppState::from_pool(pool);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let router = dbt_state_server::build_router(state);
    tokio::spawn(async move {
        router
            .serve_with_incoming(incoming)
            .await
            .expect("server serve");
    });

    // Give the server a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, schema)
}

/// Connect a tonic channel to the given address.
pub async fn channel(addr: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect client")
}

/// A pool pinned to `schema` via search_path, for asserting on stored rows.
pub async fn schema_pool(schema: &str) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .after_connect({
            let schema = schema.to_string();
            move |conn, _meta| {
                let schema = schema.clone();
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute(format!("SET search_path TO \"{schema}\"").as_str())
                        .await?;
                    Ok(())
                })
            }
        })
        .connect(&dsn())
        .await
        .expect("connect schema pool")
}

/// The decision variant name of a SubmitSQLResponse.
pub fn response_variant(resp: &qc::SubmitSqlResponse) -> &'static str {
    match &resp.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(_)) => "ready_to_execute",
        Some(qc::submit_sql_response::Response::SkipExecution(_)) => "skip_execution",
        Some(qc::submit_sql_response::Response::ReadyToClone(_)) => "ready_to_clone",
        None => "none",
    }
}

/// Build a tonic::Request wrapping `body` with the `x-organization-id` metadata
/// header set to `org`. Mirrors how the real dbt client scopes calls per org.
pub fn request_with_org<T>(body: T, org: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(body);
    req.metadata_mut().insert(
        "x-organization-id",
        org.parse().expect("valid metadata value"),
    );
    req
}
