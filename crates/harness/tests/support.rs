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

/// A dbt client's Token broker requests.
#[path = "support/broker_client.rs"]
pub mod broker_client;
/// A browser following a dbt client's sign-in redirects.
#[path = "support/browser.rs"]
pub mod browser;
/// Request shapes from the live clone captures in the demo database.
#[path = "support/demo.rs"]
pub mod demo;
/// The fake OIDC Upstream IdP.
#[path = "support/fake_idp.rs"]
pub mod fake_idp;
/// Raw-SQL fault injection (the only DDL tests run against State tables).
#[path = "support/fault.rs"]
pub mod fault;
/// HTTP test servers on their own thread.
#[path = "support/http.rs"]
pub mod http;
/// Keycloak in Docker as the Upstream IdP (opt-in suite).
#[path = "support/keycloak.rs"]
pub mod keycloak;
/// Raw-SQL legacy, frozen, corrupt and captured State (the only raw writers).
#[path = "support/legacy.rs"]
pub mod legacy;
/// Exact real clock evidence retained for supplemental native replays.
#[path = "support/native_clock.rs"]
pub mod native_clock;
/// `oidc` mode: test auth config, test signer and log capture.
#[path = "support/oidc.rs"]
pub mod oidc;
#[path = "support/python.rs"]
mod python;
/// The pinned dbt-state Python client's driver, run through `uv`.
#[path = "support/python_client.rs"]
pub mod python_client;
#[path = "support/resources.rs"]
mod resources;
/// Read-only row assertions for executions and node states.
#[path = "support/rows.rs"]
pub mod rows;
/// Every RPC of the dbt State protocol.
#[path = "support/rpcs.rs"]
pub mod rpcs;
/// Request builders and the submit/confirm/expect protocol client.
#[path = "support/scenario.rs"]
pub mod scenario;
/// The test-only TLS certificate both listeners serve, and its CA.
#[path = "support/tls.rs"]
pub mod tls;
/// The UI as the Token broker's confidential OIDC client.
#[path = "support/ui_client.rs"]
pub mod ui_client;
// Each test binary uses a different subset of these shared helpers.
#[allow(unused_imports)]
pub use resources::{
    start_broker, start_broker_beside, start_server, start_server_native, start_server_oidc,
    start_server_on_database, start_server_require_org, start_server_tls, start_server_with_broker,
    TestBroker, TestClock, TestDatabase, TestServer,
};

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

/// Extract the hosted decision ID from any captured submit variant.
pub fn hosted_submit_edid(resp: &serde_json::Value) -> Option<String> {
    let variants = resp.get("response")?.as_object()?;
    for payload in variants.values() {
        if let Some(id) = payload
            .get("execution_decision_id")
            .and_then(|v| v.as_str())
        {
            return Some(id.to_string());
        }
    }
    None
}

/// New Databricks captures expose the actual hosted decision instant in a
/// UUIDv7 decision ID. Request/response transit can straddle a second; using
/// response recorded_at would change otherwise exact relative-age Explain text.
/// Decoding is solely a replay clock source, never a production ID contract.
pub fn databricks_decision_time(
    entry: &dbt_state_harness::diff::GoldenEntry,
) -> Result<i64, String> {
    let raw = hosted_submit_edid(&entry.response).ok_or("missing decision ID")?;
    let id = uuid::Uuid::parse_str(&raw).map_err(|_| "invalid decision UUID")?;
    if id.get_version_num() != 7 || id.get_variant() != uuid::Variant::RFC4122 {
        return Err("unsupported decision UUID version/variant".into());
    }
    let at = (id.as_u128() >> 80) as i64;
    let submitted = entry
        .metadata
        .iter()
        .find(|(name, _)| name == "x-submitted-at-epoch")
        .and_then(|(_, value)| value.parse::<i64>().ok())
        .ok_or("missing captured submit instant")?;
    let recorded = entry
        .recorded_at_ms()
        .ok_or("missing captured response instant")?;
    if at < submitted || at > recorded {
        return Err(format!(
            "decision instant {at} outside captured interval {submitted}..={recorded}"
        ));
    }
    Ok(at)
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

/// A captured request at a separately evidenced decision instant.
pub fn captured_request_at<T>(
    body: T,
    entry: &dbt_state_harness::diff::GoldenEntry,
    at: i64,
) -> tonic::Request<T> {
    let mut request = request_with_org(body, captured_org(entry));
    request.metadata_mut().insert(
        dbt_state_server::REPLAY_NOW_HEADER,
        at.to_string().parse().expect("valid metadata value"),
    );
    request
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
