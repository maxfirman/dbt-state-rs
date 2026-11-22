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
//! A person signs in through the client's browser flow (the harness
//! browser follows the redirects through the Token broker and the fake
//! Upstream IdP to the client's loopback handler), and keeps working
//! without the browser: the next invocation refreshes from the token the
//! client cached on disk, and a long-running process refreshes an expired
//! token in memory. Person tests take the loopback port lock
//! (`PythonClient::person`), so they run one at a time.
//!
//! Needs `uv` (the driver's Python environment is locked in
//! `python-client/uv.lock`); skipped without it unless
//! `DBT_STATE_REQUIRE_PYTHON_CLIENT` is set, as CI sets it.

#[path = "support.rs"]
mod support;

use std::time::Duration;

use serde_json::json;
use support::fake_idp::{FakeIdp, UpstreamMachine, UpstreamPerson};
use support::oidc::{capture_logs, captured_logs, TestAuth, TestAuthBuilder};
use support::python_client::PythonClient;
use support::{rows, start_server_tls};

/// The CI machine's app registration at the Upstream IdP.
const CI: &str = "ci-pipeline";
const CI_SECRET: &str = "ci-secret-Zx81qv";

/// CI machines with the `dbt-state.ci` app role, and people in the
/// `data-eng` group, are developers in `analytics`.
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(&[("analytics", false), ("research", false)])
        .upstream(idp)
        .rule("machine", "roles", "dbt-state.ci", "analytics", "developer")
        .rule("person", "groups", "data-eng", "analytics", "developer")
}

/// Alice, in the `data-eng` group.
fn alice() -> UpstreamPerson {
    UpstreamPerson::new("alice-sub").claim("groups", json!(["data-eng"]))
}

/// The client's loopback handler: its redirect URI, without query.
const LOOPBACK_HANDLER: &str = "http://127.0.0.1:29525/handler";

/// Where the client caches its token, in its home.
const TOKEN_FILE: &str = "state_auth.json";

/// How often the Token broker refreshed a person at the IdP.
fn upstream_refreshes(idp: &FakeIdp) -> usize {
    idp.token_requests()
        .iter()
        .filter(|request| request.grant_type() == Some("refresh_token"))
        .count()
}

/// Every recorded decision is in `analytics`, under Alice's Principal.
async fn decided_for_alice(server: &support::TestServer, idp: &FakeIdp, count: usize) {
    let principals = rows::principals(server).await;
    assert_eq!(principals.len(), 1, "{principals:?}");
    assert_eq!(
        (
            &*principals[0].issuer,
            &*principals[0].subject,
            &*principals[0].kind
        ),
        (&*idp.issuer(), "alice-sub", "person")
    );
    let decisions = rows::decisions(server, count).await;
    assert_eq!(decisions.len(), count, "{decisions:?}");
    for decision in &decisions {
        assert_eq!(decision.org_id, "analytics");
        assert_eq!(decision.principal_id.as_deref(), Some(&*principals[0].id));
    }
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

    let run = client.machine(CI, CI_SECRET).run(&["machine"]).await;

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
        .run(&["machine"])
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

/// A person signs in through the client's browser flow, over TLS: the
/// client opens the Token broker's sign-in URL, the browser follows the
/// redirects through the Upstream IdP and back through the broker to the
/// client's loopback handler, and the client exchanges the code and makes
/// RPCs with the person token. It caches the token on disk.
#[tokio::test]
async fn a_person_signs_in_through_the_browser_over_tls() {
    let idp = FakeIdp::start().await;
    let server = start_server_tls(deployment(&idp)).await;
    idp.follow(&server.broker_clock).sign_in(alice());
    let Some(client) = PythonClient::available(&server) else {
        return;
    };
    let client = client.person().await;

    let run = client.run(&["person", "rpc"]).await;

    assert!(run.status.success(), "{run}");
    assert_eq!(run.result["outcome"], "ok", "{run}");
    assert_eq!(
        run.result["decisions"],
        json!(["ReadyToExecuteResponse"]),
        "{run}"
    );
    let visits = client.browser_visits();
    assert_eq!(visits.len(), 1, "{visits:?}");
    let broker = server.broker_url();
    // The client opens its auth URL (`RUN_CACHE_AUTH_URL`, the broker's
    // sign-in endpoint) as it is, with the authorization request's query.
    let opened = visits[0]["opened"].as_str().unwrap_or_default();
    assert!(opened.starts_with(&format!("{broker}?")), "{visits:?}");
    assert!(opened.contains("code_challenge_method=S256"), "{visits:?}");
    assert!(broker.starts_with("https://"));
    assert_eq!(
        visits[0]["redirects"],
        json!([
            format!("{}/authorize", idp.issuer()),
            format!("{broker}/callback"),
            LOOPBACK_HANDLER,
        ]),
        "{visits:?}"
    );
    assert_eq!(visits[0]["landed"], LOOPBACK_HANDLER, "{visits:?}");
    assert_eq!(visits[0]["status"], 200, "{visits:?}");
    assert_eq!(idp.authorize_requests().len(), 1);
    decided_for_alice(&server, &idp, 1).await;
    assert!(client.home().join(TOKEN_FILE).is_file());
}

/// The next dbt invocation (a new process) finds the token the client
/// cached on disk and refreshes it through the Token broker (which renews
/// at the IdP) without opening the browser, then makes an authenticated
/// RPC with the renewed token.
#[tokio::test]
async fn the_next_invocation_refreshes_from_the_token_on_disk_without_a_browser() {
    let idp = FakeIdp::start().await;
    let server = start_server_tls(deployment(&idp)).await;
    idp.follow(&server.broker_clock).sign_in(alice());
    let Some(client) = PythonClient::available(&server) else {
        return;
    };
    let client = client.person().await;
    let signed_in = client.run(&["person", "rpc"]).await;
    assert!(signed_in.status.success(), "{signed_in}");
    assert_eq!(client.browser_visits().len(), 1);
    assert_eq!(upstream_refreshes(&idp), 0);

    let run = client.run(&["person", "rpc"]).await;

    assert!(run.status.success(), "{run}");
    assert_eq!(run.result["outcome"], "ok", "{run}");
    assert_eq!(
        run.result["decisions"],
        json!(["ReadyToExecuteResponse"]),
        "{run}"
    );
    assert_eq!(client.browser_visits().len(), 1, "no second browser visit");
    assert_eq!(idp.authorize_requests().len(), 1, "no second sign-in");
    assert_eq!(upstream_refreshes(&idp), 1);
    decided_for_alice(&server, &idp, 2).await;
}

/// In one long-running process, the person token expires: the process's
/// wall clock moves past it, and the Token broker's clock (which the fake
/// IdP follows) as far. The next RPC's credential lookup refreshes it from
/// the client's memory (the token cached on disk is gone) through the
/// broker, without opening the browser, and the RPC is authenticated.
#[tokio::test]
async fn a_long_running_process_refreshes_an_expired_token_in_memory() {
    let idp = FakeIdp::start().await;
    let server = start_server_tls(deployment(&idp)).await;
    idp.follow(&server.broker_clock).sign_in(alice());
    let Some(client) = PythonClient::available(&server) else {
        return;
    };
    let client = client.person().await;
    let mut driver = client.start(&["person", "rpc", "wait", "rpc"]);
    assert_eq!(driver.paused().await["rpcs"], 1);
    assert_eq!(client.browser_visits().len(), 1);
    std::fs::remove_file(client.home().join(TOKEN_FILE)).expect("token on disk");

    // One minute past the token's 60-minute lifetime.
    let elapsed = Duration::from_secs(61 * 60);
    server.broker_clock.advance(elapsed);
    driver.resume(elapsed).await;
    let run = driver.finish().await;

    assert!(run.status.success(), "{run}");
    assert_eq!(run.result["outcome"], "ok", "{run}");
    assert_eq!(
        run.result["decisions"],
        json!(["ReadyToExecuteResponse", "ReadyToExecuteResponse"]),
        "{run}"
    );
    assert_eq!(client.browser_visits().len(), 1, "no second browser visit");
    assert_eq!(idp.authorize_requests().len(), 1, "no second sign-in");
    assert_eq!(upstream_refreshes(&idp), 1);
    decided_for_alice(&server, &idp, 2).await;
}

/// At the minimum person token lifetime (15 minutes), a token stays fresh
/// for the client (it refreshes within 5 minutes of expiry) long enough
/// that consecutive credential lookups reuse it: two lookups (RPCs) after
/// signing in make no IdP call, and after 11 minutes, when the token is
/// due for refresh, the first lookup renews it once and the second makes
/// no second IdP call. The browser opens only to sign in.
#[tokio::test]
async fn at_the_minimum_lifetime_consecutive_lookups_make_no_second_idp_call() {
    let idp = FakeIdp::start().await;
    let server = start_server_tls(deployment(&idp).person_lifetime_minutes(15)).await;
    idp.follow(&server.broker_clock).sign_in(alice());
    let Some(client) = PythonClient::available(&server) else {
        return;
    };
    let client = client.person().await;
    let mut driver = client.start(&["person", "rpc", "rpc", "wait", "rpc", "rpc"]);
    assert_eq!(driver.paused().await["rpcs"], 2);
    // Signing in exchanged one code at the IdP; the lookups called nothing.
    assert_eq!(idp.token_requests().len(), 1);
    assert_eq!(client.browser_visits().len(), 1);

    // Under 5 minutes of the token's lifetime left.
    let elapsed = Duration::from_secs(11 * 60);
    server.broker_clock.advance(elapsed);
    driver.resume(elapsed).await;
    let run = driver.finish().await;

    assert!(run.status.success(), "{run}");
    assert_eq!(run.result["outcome"], "ok", "{run}");
    assert_eq!(
        run.result["decisions"].as_array().map(Vec::len),
        Some(4),
        "{run}"
    );
    assert_eq!(upstream_refreshes(&idp), 1, "one renewal for two lookups");
    assert_eq!(idp.token_requests().len(), 2);
    assert_eq!(client.browser_visits().len(), 1, "no second browser visit");
    assert_eq!(idp.authorize_requests().len(), 1, "no second sign-in");
    decided_for_alice(&server, &idp, 4).await;
}
