//! Shared integration-test support: spin up the dbt State server in-process
//! against a freshly-created, uniquely-named Postgres schema so each test is
//! isolated and repeatable.
//!
//! Owned resources live in `support/resources.rs` and `support/python.rs`.
//! The scenario module (`support/scenario.rs`) builds requests and drives the
//! protocol; `support/rows.rs` reads rows back; `support/legacy.rs` and
//! `support/fault.rs` are the only raw-SQL State writers.
//! Kept as a `tests/` module
//! (rather than the harness library) so the `dbt-state-server`/`sqlx`/`tokio`
//! deps stay dev-only.
#![allow(dead_code)]

use std::net::SocketAddr;

use dbt_state_proto::query_cache as qc;
use tonic::transport::Channel;

/// Request shapes from the live clone captures in the demo database.
#[path = "support/demo.rs"]
pub mod demo;
/// Raw-SQL fault injection (the only DDL tests run against State tables).
#[path = "support/fault.rs"]
pub mod fault;
/// Raw-SQL legacy, frozen, corrupt and captured State (the only raw writers).
#[path = "support/legacy.rs"]
pub mod legacy;
#[path = "support/python.rs"]
mod python;
#[path = "support/resources.rs"]
mod resources;
/// Read-only row assertions for executions and node states.
#[path = "support/rows.rs"]
pub mod rows;
/// Request builders and the submit/confirm/expect protocol client.
#[path = "support/scenario.rs"]
pub mod scenario;
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

/// The answer class of a RegisterClone response: `ready_to_clone`,
/// `unable_to_clone` or `none` (its top-level fields; the deprecated
/// `response` oneof mirrors `ready_to_clone`).
pub fn clone_response_variant(resp: &qc::CloneResponse) -> &'static str {
    match (&resp.ready_to_clone, &resp.unable_to_clone) {
        (Some(_), _) => "ready_to_clone",
        (None, Some(_)) => "unable_to_clone",
        (None, None) => "none",
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

/// The captured request, scoped to its org and decided at its recorded time
/// (test servers use `Clock::Replay`).
pub fn captured_request<T>(
    body: T,
    entry: &dbt_state_harness::diff::GoldenEntry,
) -> tonic::Request<T> {
    let mut req = request_with_org(body, captured_org(entry));
    if let Some(at) = entry.recorded_at_ms() {
        req.metadata_mut().insert(
            dbt_state_server::REPLAY_NOW_HEADER,
            at.to_string().parse().expect("valid metadata value"),
        );
    }
    req
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
