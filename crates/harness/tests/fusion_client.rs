//! Pinned dbt Fusion's own dbt State client code (dbt-fusion@9977b6c
//! `dbt-run-cache`), unpatched, against a deployment serving native TLS on
//! both listeners with the fake Upstream IdP. Opt-in: every test skips
//! unless `DBT_STATE_FUSION_DRIVER` names the built driver
//! (`crates/harness/fusion-driver`, outside the workspace; dbt-fusion is
//! ELv2, so it is compiled against, never vendored or shipped).
//!
//! The driver runs Fusion's own config loader, token source and gRPC
//! client, configured only through Fusion's supported settings. A CI
//! machine logs in with `client_credentials`; a person signs in through
//! Fusion's browser flow (Fusion's URL opener runs the harness browser,
//! which follows the redirects through the Token broker and the fake
//! Upstream IdP to Fusion's loopback handler), and the next invocation
//! refreshes from the token Fusion keeps on disk without the browser.
//!
//! Fusion's documented token lifecycle, which sets the person token
//! lifetime (ADR 0012): a process reads the token on disk at most once;
//! after that it uses its in-memory token while that has more than 300 s
//! left, then acquires a new one (the browser, for a person) rather than
//! refreshing. Time passes in the driver through its wall clock, which the
//! harness moves on with the Token broker's test clock (the fake IdP
//! follows it). Person tests take the loopback port lock, shared with the
//! Python driver's person tests, so they run one at a time.

#[path = "support.rs"]
mod support;

use std::time::Duration;

use serde_json::{json, Value};
use support::fake_idp::{FakeIdp, UpstreamMachine, UpstreamPerson};
use support::fusion_client::FusionClient;
use support::oidc::{capture_logs, captured_logs, TestAuth, TestAuthBuilder};
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

/// Fusion's loopback handler: its redirect URI, without query.
const LOOPBACK_HANDLER: &str = "http://127.0.0.1:29525/handler";

/// The default person token lifetime, and how long before expiry Fusion
/// stops using a token.
const PERSON_LIFETIME: Duration = Duration::from_secs(60 * 60);
const REFRESH_WINDOW: Duration = Duration::from_secs(5 * 60);

/// How often the Token broker refreshed a person at the IdP.
fn upstream_refreshes(idp: &FakeIdp) -> usize {
    idp.token_requests()
        .iter()
        .filter(|request| request.grant_type() == Some("refresh_token"))
        .count()
}

/// How often a person signed in at the IdP.
fn sign_ins(idp: &FakeIdp) -> usize {
    idp.authorize_requests().len()
}

/// The token Fusion keeps on disk.
fn token_on_disk(client: &FusionClient) -> Value {
    let text = std::fs::read_to_string(client.token_file()).expect("token on disk");
    serde_json::from_str(&text).expect("token JSON")
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

/// Fusion logs in once with `client_credentials` through the Token broker,
/// finds its one Organization in the token, and makes RPCs over TLS with
/// it; the decision is recorded in that Organization under the machine's
/// Principal. At the minimum machine token lifetime, the second
/// credential lookup reuses the token: one IdP call.
#[tokio::test]
async fn a_machine_logs_in_with_client_credentials_and_makes_an_rpc_over_tls() {
    let idp = FakeIdp::start().await;
    idp.machine(UpstreamMachine::new(CI, CI_SECRET).claim("roles", json!(["dbt-state.ci"])));
    let server = start_server_tls(deployment(&idp).machine_lifetime_minutes(15)).await;
    let Some(client) = FusionClient::available(&server) else {
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
        (
            &*principals[0].issuer,
            &*principals[0].subject,
            &*principals[0].kind
        ),
        (&*idp.issuer(), "sp-ci-pipeline", "machine")
    );
    assert_eq!(
        decisions[0].principal_id.as_deref(),
        Some(&*principals[0].id)
    );
    // Two credential lookups (two RPCs), one login.
    let logins: Vec<_> = idp
        .token_requests()
        .iter()
        .map(|request| request.grant_type().map(str::to_owned))
        .collect();
    assert_eq!(logins, [Some("client_credentials".to_owned())]);
}

/// A machine the IdP authenticates but no rule maps gets no token: Fusion
/// fails the token request with the broker's HTTP status (it discards the
/// OAuth error body), the driver shows it, and nothing reaches State
/// history.
#[tokio::test]
async fn a_machine_no_rule_maps_is_refused_and_the_driver_shows_why() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.machine(UpstreamMachine::new("ci-unmapped", "unmapped-secret"));
    let server = start_server_tls(deployment(&idp)).await;
    let Some(client) = FusionClient::available(&server) else {
        return;
    };

    let run = client
        .machine("ci-unmapped", "unmapped-secret")
        .run(&["machine"])
        .await;

    assert_eq!(run.status.code(), Some(1), "{run}");
    assert_eq!(run.result["outcome"], "error", "{run}");
    assert_eq!(run.result["error"], "AuthRequest", "{run}");
    assert_eq!(run.result["status"], 400, "{run}");
    let message = run.result["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with("dbt State service authentication request failed"),
        "{run}"
    );
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

/// A person signs in through Fusion's browser flow, over TLS: Fusion opens
/// the Token broker's sign-in URL, the browser follows the redirects
/// through the Upstream IdP and back through the broker to Fusion's
/// loopback handler, and Fusion exchanges the code and makes RPCs with the
/// person token. It keeps the token, with its refresh token, on disk.
#[tokio::test]
async fn a_person_signs_in_through_the_browser_over_tls() {
    let idp = FakeIdp::start().await;
    let server = start_server_tls(deployment(&idp)).await;
    idp.follow(&server.broker_clock).sign_in(alice());
    let Some(client) = FusionClient::available(&server) else {
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
    assert!(broker.starts_with("https://"));
    // Fusion opens its auth URL (`RUN_CACHE_AUTH_URL`, the broker's sign-in
    // endpoint) with the authorization request's query.
    let opened = visits[0]["opened"].as_str().unwrap_or_default();
    assert!(opened.starts_with(&format!("{broker}/?")), "{visits:?}");
    assert!(opened.contains("code_challenge_method=S256"), "{visits:?}");
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
    assert_eq!(sign_ins(&idp), 1);
    decided_for_alice(&server, &idp, 1).await;
    let stored = token_on_disk(&client);
    assert!(stored["refresh_token"].is_string(), "{stored}");
    assert!(stored["id_token"].is_string(), "{stored}");
}

/// The next dbt invocation (a new process), started once the token on disk
/// has under 5 minutes left, reads that token, refreshes it through the
/// Token broker (which renews at the IdP) without opening the browser,
/// keeps the rotated refresh token on disk, and makes an authenticated
/// RPC with the renewed token.
#[tokio::test]
async fn the_next_invocation_refreshes_from_the_token_on_disk_without_a_browser() {
    let idp = FakeIdp::start().await;
    let server = start_server_tls(deployment(&idp)).await;
    idp.follow(&server.broker_clock).sign_in(alice());
    let Some(client) = FusionClient::available(&server) else {
        return;
    };
    let client = client.person().await;
    let signed_in = client.run(&["person", "rpc"]).await;
    assert!(signed_in.status.success(), "{signed_in}");
    let stored = token_on_disk(&client);
    assert_eq!(client.browser_visits().len(), 1);

    let mut next = client.start(&["person", "wait", "rpc"]);
    next.paused().await;
    let elapsed = PERSON_LIFETIME - REFRESH_WINDOW + Duration::from_secs(60);
    server.broker_clock.advance(elapsed);
    next.resume(elapsed).await;
    let run = next.finish().await;

    assert!(run.status.success(), "{run}");
    assert_eq!(run.result["outcome"], "ok", "{run}");
    assert_eq!(
        run.result["decisions"],
        json!(["ReadyToExecuteResponse"]),
        "{run}"
    );
    assert_eq!(client.browser_visits().len(), 1, "no second browser visit");
    assert_eq!(sign_ins(&idp), 1, "no second sign-in");
    assert_eq!(upstream_refreshes(&idp), 1);
    let refreshed = token_on_disk(&client);
    assert_ne!(refreshed["refresh_token"], stored["refresh_token"]);
    assert_ne!(refreshed["id_token"], stored["id_token"]);
    decided_for_alice(&server, &idp, 2).await;
}

/// Fusion reads the token on disk once per process. A long-running process
/// signs in; later another invocation refreshes the token on disk; when
/// the first process's in-memory token comes within 300 s of expiry, it
/// neither reads the fresh token on disk again nor refreshes with its own
/// refresh token: it signs in through the browser again, and keeps that
/// token on disk.
#[tokio::test]
async fn a_process_reads_the_token_on_disk_once_and_then_signs_in_again() {
    let idp = FakeIdp::start().await;
    let server = start_server_tls(deployment(&idp)).await;
    idp.follow(&server.broker_clock).sign_in(alice());
    let Some(client) = FusionClient::available(&server) else {
        return;
    };
    let client = client.person().await;
    let mut long_running = client.start(&["person", "rpc", "wait", "rpc"]);
    assert_eq!(long_running.paused().await["rpcs"], 1);
    assert_eq!(client.browser_visits().len(), 1);

    let elapsed = PERSON_LIFETIME - REFRESH_WINDOW + Duration::from_secs(60);
    server.broker_clock.advance(elapsed);
    let mut next = client.start(&["person", "wait", "rpc"]);
    next.paused().await;
    next.resume(elapsed).await;
    let refreshed_by_next = next.finish().await;
    assert!(refreshed_by_next.status.success(), "{refreshed_by_next}");
    assert_eq!(upstream_refreshes(&idp), 1);
    let fresh_on_disk = token_on_disk(&client);

    long_running.resume(elapsed).await;
    let run = long_running.finish().await;

    assert!(run.status.success(), "{run}");
    assert_eq!(
        run.result["decisions"],
        json!(["ReadyToExecuteResponse", "ReadyToExecuteResponse"]),
        "{run}"
    );
    assert_eq!(upstream_refreshes(&idp), 1, "the process refreshed nothing");
    let visits = client.browser_visits();
    assert_eq!(visits.len(), 2, "{visits:?}");
    assert_eq!(visits[1]["landed"], LOOPBACK_HANDLER, "{visits:?}");
    assert_eq!(sign_ins(&idp), 2);
    assert_ne!(
        token_on_disk(&client)["id_token"],
        fresh_on_disk["id_token"]
    );
    decided_for_alice(&server, &idp, 3).await;
}

/// A run shorter than the person token lifetime minus 5 minutes needs no
/// new token: 54 minutes into the default 60-minute lifetime, Fusion's
/// credential lookup still uses its in-memory token (no token request, no
/// browser). Two minutes later, within 300 s of expiry, the next lookup
/// signs in through the browser again, once.
#[tokio::test]
async fn a_run_shorter_than_the_lifetime_minus_five_minutes_needs_no_new_token() {
    let idp = FakeIdp::start().await;
    let server = start_server_tls(deployment(&idp)).await;
    idp.follow(&server.broker_clock).sign_in(alice());
    let Some(client) = FusionClient::available(&server) else {
        return;
    };
    let client = client.person().await;
    let mut driver = client.start(&["person", "rpc", "wait", "rpc", "wait", "rpc"]);
    assert_eq!(driver.paused().await["rpcs"], 1);
    // Signing in exchanged one code at the IdP.
    assert_eq!(idp.token_requests().len(), 1);

    let short_run = PERSON_LIFETIME - REFRESH_WINDOW - Duration::from_secs(60);
    server.broker_clock.advance(short_run);
    driver.resume(short_run).await;
    assert_eq!(driver.paused().await["rpcs"], 2);
    assert_eq!(idp.token_requests().len(), 1, "no IdP call");
    assert_eq!(client.browser_visits().len(), 1, "no browser");

    let past_freshness = Duration::from_secs(2 * 60);
    server.broker_clock.advance(past_freshness);
    driver.resume(past_freshness).await;
    let run = driver.finish().await;

    assert!(run.status.success(), "{run}");
    assert_eq!(
        run.result["decisions"].as_array().map(Vec::len),
        Some(3),
        "{run}"
    );
    assert_eq!(client.browser_visits().len(), 2, "one new sign-in");
    assert_eq!(sign_ins(&idp), 2);
    assert_eq!(upstream_refreshes(&idp), 0);
    assert_eq!(idp.token_requests().len(), 2);
    decided_for_alice(&server, &idp, 3).await;
}

/// At the minimum person token lifetime (15 minutes), a token stays fresh
/// for Fusion (more than 300 s left) long enough that consecutive
/// credential lookups reuse it: two lookups (RPCs) after signing in make
/// no IdP call and open no browser. After 11 minutes, when the token is
/// due, the first lookup signs in again through the browser and the second
/// makes no second IdP call.
#[tokio::test]
async fn at_the_minimum_lifetime_consecutive_lookups_make_no_second_idp_call() {
    let idp = FakeIdp::start().await;
    let server = start_server_tls(deployment(&idp).person_lifetime_minutes(15)).await;
    idp.follow(&server.broker_clock).sign_in(alice());
    let Some(client) = FusionClient::available(&server) else {
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
    assert_eq!(
        run.result["decisions"].as_array().map(Vec::len),
        Some(4),
        "{run}"
    );
    assert_eq!(
        idp.token_requests().len(),
        2,
        "one new token for two lookups"
    );
    assert_eq!(client.browser_visits().len(), 2);
    assert_eq!(sign_ins(&idp), 2);
    assert_eq!(upstream_refreshes(&idp), 0);
    decided_for_alice(&server, &idp, 4).await;
}
