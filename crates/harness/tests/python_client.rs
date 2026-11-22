//! The pinned dbt-state Python client (2.62.6), unpatched, against a
//! deployment serving native TLS on both listeners with the fake Upstream
//! IdP.
//!
//! The driver (`crates/harness/python-client/driver.py`) runs the client's
//! own token acquisition and gRPC code, configured only through the
//! client's supported settings: API, auth and token URLs, a client id and
//! secret, and the CA it trusts. A CI machine logs in with
//! `client_credentials` and makes authenticated RPCs; a machine no rule
//! maps is refused, and the driver shows the client's error.
//!
//! Needs `uv` (the driver's Python environment is locked in
//! `python-client/uv.lock`); skipped without it unless
//! `DBT_STATE_REQUIRE_PYTHON_CLIENT` is set, as CI sets it.

#[path = "support.rs"]
mod support;

use serde_json::json;
use support::fake_idp::{FakeIdp, UpstreamMachine};
use support::oidc::{capture_logs, captured_logs, TestAuth, TestAuthBuilder};
use support::python_client::PythonClient;
use support::{rows, start_server_tls};

/// The CI machine's app registration at the Upstream IdP.
const CI: &str = "ci-pipeline";
const CI_SECRET: &str = "ci-secret-Zx81qv";

/// CI machines with the `dbt-state.ci` app role are developers in
/// `analytics`.
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(&[("analytics", false), ("research", false)])
        .upstream(idp)
        .rule("machine", "roles", "dbt-state.ci", "analytics", "developer")
}

/// The client logs in once with `client_credentials` through the Token
/// broker, finds its one Organization in the token, and makes RPCs over
/// TLS with the token; the decision is recorded in that Organization under
/// the machine's Principal.
#[tokio::test]
async fn a_machine_logs_in_with_client_credentials_and_makes_an_rpc_over_tls() {
    let idp = FakeIdp::start().await;
    idp.machine(UpstreamMachine::new(CI, CI_SECRET).claim("roles", json!(["dbt-state.ci"])));
    let server = start_server_tls(deployment(&idp)).await;
    let Some(client) = PythonClient::available(&server) else {
        return;
    };

    let run = client.machine(CI, CI_SECRET).run("machine").await;

    assert!(run.status.success(), "{run}");
    assert_eq!(run.result["outcome"], "ok", "{run}");
    assert_eq!(run.result["client_version_supported"], true, "{run}");
    assert_eq!(run.result["decision"], "ReadyToExecuteResponse", "{run}");
    let decisions = rows::decisions(&server, 1).await;
    assert_eq!(decisions.len(), 1, "{decisions:?}");
    assert_eq!(decisions[0].org_id, "analytics");
    assert_eq!(decisions[0].decision, "build");
    let principals = rows::principals(&server).await;
    assert_eq!(principals.len(), 1, "{principals:?}");
    assert_eq!(
        (&*principals[0].issuer, &*principals[0].subject),
        (&*idp.issuer(), "sp-ci-pipeline")
    );
    assert_eq!(principals[0].kind, "machine");
    assert_eq!(
        decisions[0].principal_id.as_deref(),
        Some(&*principals[0].id)
    );
    // The client reuses its fresh token for every RPC: one login.
    let logins = idp
        .token_requests()
        .iter()
        .filter(|request| request.grant_type() == Some("client_credentials"))
        .count();
    assert_eq!(logins, 1);
}

/// A machine the IdP authenticates but no rule maps gets no token: the
/// client raises its credentials error (the broker's `unauthorized_client`
/// is its cause), the driver shows it, and nothing reaches State history.
#[tokio::test]
async fn a_machine_no_rule_maps_is_refused_and_the_driver_shows_why() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.machine(UpstreamMachine::new("ci-unmapped", "unmapped-secret"));
    let server = start_server_tls(deployment(&idp)).await;
    let Some(client) = PythonClient::available(&server) else {
        return;
    };

    let run = client
        .machine("ci-unmapped", "unmapped-secret")
        .run("machine")
        .await;

    assert_eq!(run.status.code(), Some(1), "{run}");
    assert_eq!(run.result["outcome"], "error", "{run}");
    assert_eq!(run.result["error"], "AuthenticationError", "{run}");
    let message = run.result["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with("Error logging in with client credentials"),
        "{run}"
    );
    let cause = run.result["cause"].as_str().unwrap_or_default();
    assert!(cause.contains("unauthorized_client"), "{run}");
    assert!(
        captured_logs()
            .lines()
            .any(|line| line.contains("token request refused")
                && line.contains("no_matching_rule")
                && line.contains("client_id=\"ci-unmapped\"")),
        "{}",
        captured_logs()
    );
    assert!(rows::principals(&server).await.is_empty());
    assert!(rows::decisions(&server, 0).await.is_empty());
}
