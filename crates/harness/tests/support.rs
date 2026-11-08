//! Shared integration-test support: spin up the dbt State server in-process
//! against a freshly-created, uniquely-named Postgres schema so each test is
//! isolated and repeatable.
//!
//! Owned resources live in `support/resources.rs` and `support/python.rs`.
//! Kept as a `tests/` module
//! (rather than the harness library) so the `dbt-state-server`/`sqlx`/`tokio`
//! deps stay dev-only.
#![allow(dead_code)]

use std::net::SocketAddr;

use dbt_state_proto::query_cache as qc;
use tonic::transport::Channel;

#[path = "support/python.rs"]
mod python;
#[path = "support/resources.rs"]
mod resources;
// Each test binary uses a different subset of these shared helpers.
#[allow(unused_imports)]
pub use resources::{start_server, start_server_require_org, TestDatabase, TestServer};

pub const DEFAULT_DSN: &str = "postgres://dbtstate:dbtstate@localhost:55441/dbtstate";

/// The DSN to use, overridable via DATABASE_URL.
pub fn dsn() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| DEFAULT_DSN.to_string())
}

/// Connect a tonic channel to the given address.
pub async fn channel(addr: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("connect client")
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

pub fn captured_org(entry: &dbt_state_harness::diff::GoldenEntry) -> &str {
    entry
        .metadata
        .iter()
        .find(|(key, _)| key == "x-organization-id")
        .map(|(_, value)| value.as_str())
        .unwrap_or("local")
}

pub fn captured_request<T>(
    body: T,
    entry: &dbt_state_harness::diff::GoldenEntry,
) -> tonic::Request<T> {
    request_with_org(body, captured_org(entry))
}

/// Preserve the captured outcome, correlating by the hosted request ID. Only
/// the opaque ID changes when the request is replayed locally.
pub async fn confirm_captured(
    exec: &mut qc::execution_client::ExecutionClient<Channel>,
    entries: &[dbt_state_harness::diff::GoldenEntry],
    submit: &dbt_state_harness::diff::GoldenEntry,
    local_id: String,
) {
    let hosted_id = submit.response["response"]["ready_to_execute"]["request_id"]
        .as_str()
        .expect("captured execute ID");
    let entry = entries
        .iter()
        .find(|e| {
            e.method == "ConfirmExecution" && e.request["request_id"].as_str() == Some(hosted_id)
        })
        .expect("captured confirmation for this submit");
    let mut req: qc::ConfirmExecutionRequest =
        serde_json::from_value(entry.request.clone()).unwrap();
    req.request_id = local_id;
    assert!(
        exec.confirm_execution(req)
            .await
            .unwrap()
            .into_inner()
            .success
    );
}
