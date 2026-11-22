//! The Token broker's `client_credentials` pass-through: a CI machine posts
//! its Upstream IdP client id and secret with HTTP Basic, exactly as the dbt
//! client does, and gets back a dbt-shaped `id_token` it can use for gRPC.
//!
//! The broker forwards the credentials to the fake Upstream IdP
//! (`support/fake_idp.rs`) with the configured upstream scope, accepts only
//! a signed JWT access token from the IdP for the upstream API audience,
//! applies the machine mapping rules, and issues the token. It never stores
//! the secret, refuses token exchange, and logs every token it issues and
//! every refusal with the Principal, client id and reason.

#[path = "support.rs"]
mod support;

use std::collections::BTreeSet;

use dbt_state_auth::Role;
use serde_json::{json, Value};
use support::broker_client::{claims, client_credentials, json, token_exchange, ORGS_SCOPE};
use support::fake_idp::{FakeIdp, UpstreamMachine, UPSTREAM_SCOPE};
use support::oidc::{
    capture_logs, captured_logs, SignerOptions, TestAuth, TestAuthBuilder, TestKey, AUDIENCE,
    ISSUER,
};
use support::scenario::{model, Scenario};
use support::{rows, start_broker, start_server_oidc, start_server_with_broker};
use tonic::Code;

const TARGET: &str = "\"DB\".\"S\".\"T\"";

/// Two active Organizations and a disabled one.
const ORGANIZATIONS: &[(&str, bool)] =
    &[("analytics", false), ("research", false), ("finance", true)];

/// The CI machine's app registration at the Upstream IdP.
const CI: &str = "ci-pipeline";
const CI_SECRET: &str = "ci-secret-Zx81qv";

/// The deployment's mapping rules: CI machines with the `dbt-state.ci` app
/// role are developers in `analytics`; the `ci-deployer` client is an admin
/// there; people with that same role value are admins in `research`.
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(ORGANIZATIONS)
        .upstream(idp)
        .rule("machine", "roles", "dbt-state.ci", "analytics", "developer")
        .rule("machine", "azp", "ci-deployer", "analytics", "admin")
        .rule("person", "roles", "dbt-state.ci", "research", "admin")
}

/// The CI machine, registered under `client_id`.
fn ci_as(client_id: &str) -> UpstreamMachine {
    UpstreamMachine::new(client_id, CI_SECRET).claim("roles", json!(["dbt-state.ci", "other.role"]))
}

fn ci_machine() -> UpstreamMachine {
    ci_as(CI)
}

/// A CI machine authenticates through the fake IdP and makes an RPC with
/// the `id_token` it gets back.
#[tokio::test]
async fn a_machine_gets_a_token_through_client_credentials_and_makes_an_rpc() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.machine(ci_machine());
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;

    let response = client_credentials(&server.broker_url(), CI, CI_SECRET).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body = json(response).await;
    let fields: BTreeSet<&str> = body.keys().map(String::as_str).collect();
    assert_eq!(
        fields,
        BTreeSet::from(["expires_in", "id_token", "scope", "token_type"]),
        "{body:?}"
    );
    let scope = "runcache:scope:app:analytics:developer runcache:scope:org:analytics:developer";
    assert_eq!(body["scope"], scope);
    assert_eq!(body["token_type"], "Bearer");
    assert_eq!(body["expires_in"], 15 * 60);

    let id_token = body["id_token"].as_str().expect("an id_token");
    let token = claims(id_token);
    assert_eq!(token["iss"], ISSUER);
    assert_eq!(token["aud"], AUDIENCE);
    assert_eq!(token["azp"], CI);
    assert_eq!(token["scope"], scope);
    assert_eq!(
        token["exp"].as_u64().unwrap() - token["iat"].as_u64().unwrap(),
        15 * 60
    );
    assert!(!token.contains_key("sid"), "a machine token has no session");
    let principal = token["sub"].as_str().unwrap().to_owned();
    assert_ne!(
        principal,
        ci_machine().subject(),
        "the Principal id is opaque"
    );

    // The broker forwarded the client's own credentials with the
    // operator's upstream scope, not the scope the client asked for.
    let forwarded = idp.token_requests();
    assert_eq!(forwarded.len(), 1);
    assert_eq!(forwarded[0].grant_type(), Some("client_credentials"));
    assert_eq!(forwarded[0].scope(), Some(UPSTREAM_SCOPE));
    assert_eq!(
        forwarded[0].basic,
        Some((CI.to_owned(), CI_SECRET.to_owned()))
    );

    let sc = Scenario::connect(&server)
        .await
        .bearer(id_token)
        .org("analytics");
    sc.try_submit(model(TARGET))
        .await
        .expect("an authorized RPC");
    // A person rule with the machine's role value grants it nothing.
    let refused = sc
        .org("research")
        .try_submit(model(TARGET))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied);

    // The same machine is the same Principal on its next login.
    let again = json(client_credentials(&server.broker_url(), CI, CI_SECRET).await).await;
    assert_eq!(
        claims(again["id_token"].as_str().unwrap())["sub"],
        principal
    );

    let logs = captured_logs();
    let issued = logs
        .lines()
        .find(|line| line.contains("token issued") && line.contains(&principal))
        .unwrap_or_else(|| panic!("no issue log for {principal} in\n{logs}"));
    for needle in [
        "grant=\"client_credentials\"",
        "kind=\"machine\"",
        &format!("client_id=\"{CI}\""),
        &format!("subject=\"{}\"", ci_machine().subject()),
    ] {
        assert!(issued.contains(needle), "no {needle} in {issued}");
    }
    assert!(!logs.contains(id_token), "a token value was logged");
}

/// The highest Role among the matching machine rules wins.
#[tokio::test]
async fn the_highest_role_among_machine_rules_wins() {
    let idp = FakeIdp::start().await;
    idp.machine(
        UpstreamMachine::new("ci-deployer", "deployer-secret")
            .claim("roles", json!("dbt-state.ci")),
    );
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let body =
        json(client_credentials(&server.broker_url(), "ci-deployer", "deployer-secret").await)
            .await;
    assert_eq!(
        body["scope"],
        "runcache:scope:app:analytics:admin runcache:scope:org:analytics:admin"
    );
}

/// Each refusal is an OAuth error with a fixed description, and is logged
/// with the client id and its reason.
async fn assert_refused(
    server: &support::TestServer,
    client_id: &str,
    secret: &str,
    status: u16,
    error: &str,
    reason: &str,
) {
    let response = client_credentials(&server.broker_url(), client_id, secret).await;
    assert_eq!(response.status(), status, "{reason}");
    let body = json(response).await;
    assert_eq!(body["error"], error, "{reason}: {body:?}");
    assert!(!body.contains_key("id_token"), "{reason}");
    let logs = captured_logs();
    assert!(
        logs.lines()
            .any(|line| line.contains("token request refused")
                && line.contains(&format!("client_id=\"{client_id}\""))
                && line.contains(&format!("reason=\"{reason}\""))),
        "no refusal log for {client_id} ({reason}) in\n{logs}"
    );
}

/// A machine no machine rule maps is refused, and the refusal names the
/// claims the rules read that its token lacked.
#[tokio::test]
async fn an_unmapped_machine_is_refused() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.machine(UpstreamMachine::new("ci-unmapped", "unmapped-secret"));
    idp.machine(
        UpstreamMachine::new("ci-other-role", "other-secret").claim("roles", json!(["other.role"])),
    );
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    for client in ["ci-unmapped", "ci-other-role"] {
        let secret = if client == "ci-unmapped" {
            "unmapped-secret"
        } else {
            "other-secret"
        };
        assert_refused(
            &server,
            client,
            secret,
            400,
            "unauthorized_client",
            "no_matching_rule",
        )
        .await;
    }
    let logs = captured_logs();
    assert!(
        logs.lines()
            .any(|line| line.contains("client_id=\"ci-unmapped\"")
                && line.contains("missing_claims=\"roles\"")),
        "{logs}"
    );
}

/// Only a signed JWT from the IdP's issuer, for the upstream API audience,
/// and current, is accepted. Opaque tokens and tokens for another
/// application are refused.
#[tokio::test]
async fn upstream_tokens_the_broker_cannot_verify_are_refused() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let cases = [
        (
            ci_as("ci-opaque").opaque(),
            "upstream_token_not_a_signed_jwt",
        ),
        (
            ci_as("ci-graph").audience(json!("https://graph.microsoft.com")),
            "upstream_token_wrong_audience",
        ),
        (
            ci_as("ci-impostor").signed_by_impostor(),
            "upstream_token_bad_signature",
        ),
        (
            ci_as("ci-elsewhere").issuer("https://elsewhere.example.test"),
            "upstream_token_wrong_issuer",
        ),
        (ci_as("ci-expired").expired(), "upstream_token_expired"),
    ];
    for (machine, _) in &cases {
        idp.machine(machine.clone());
    }
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    for (machine, reason) in &cases {
        let client = machine.client_id().to_owned();
        assert_refused(
            &server,
            &client,
            CI_SECRET,
            400,
            "unauthorized_client",
            reason,
        )
        .await;
    }
    // An audience list holding the upstream API audience is accepted.
    idp.machine(
        ci_as("ci-two-audiences")
            .audience(json!(["https://graph.microsoft.com", "api://dbt-state"])),
    );
    let response = client_credentials(&server.broker_url(), "ci-two-audiences", CI_SECRET).await;
    assert_eq!(response.status(), 200);
}

/// Discovered endpoints must use TLS (plain http only on loopback, like
/// `[upstream] issuer`): the broker never sends a client's credentials to,
/// or takes keys from, a plaintext endpoint elsewhere.
#[tokio::test]
async fn plaintext_discovered_endpoints_are_never_used() {
    capture_logs();
    for endpoint in ["token_endpoint", "jwks_uri"] {
        let idp = FakeIdp::start().await;
        idp.machine(ci_machine());
        idp.advertise(endpoint, &format!("http://idp.example.test/{endpoint}"));
        let server = start_server_with_broker(&deployment(&idp).build()).await;
        assert_refused(
            &server,
            CI,
            CI_SECRET,
            503,
            "temporarily_unavailable",
            "upstream_unavailable",
        )
        .await;
        assert!(idp.token_requests().is_empty(), "{endpoint}");
        assert_eq!(idp.jwks_fetches(), 0, "{endpoint}");
    }
}

/// Credentials the IdP rejects, or none at all, are `invalid_client`.
#[tokio::test]
async fn bad_client_credentials_are_invalid_client() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.machine(ci_machine());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    assert_refused(
        &server,
        CI,
        "not-the-secret",
        401,
        "invalid_client",
        "upstream_rejected_client",
    )
    .await;

    let response = reqwest::Client::new()
        .post(format!("{}/token", server.broker_url()))
        .form(&[("grant_type", "client_credentials"), ("scope", ORGS_SCOPE)])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    assert!(response.headers().contains_key("www-authenticate"));
    assert_eq!(json(response).await["error"], "invalid_client");
    // Nothing was forwarded without credentials.
    assert_eq!(idp.token_requests().len(), 1);
}

/// dbt platform token exchange is refused with a 4xx, and the platform
/// token never reaches the IdP or the logs.
#[tokio::test]
async fn token_exchange_is_refused() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let platform_token = "dbtc_platform-token-Qm93kd";
    let response = token_exchange(&server.broker_url(), "exchange-client", platform_token).await;
    assert_eq!(response.status(), 400);
    let body = json(response).await;
    assert_eq!(body["error"], "unsupported_grant_type");
    assert!(idp.token_requests().is_empty());
    let logs = captured_logs();
    assert!(
        logs.lines()
            .any(|line| line.contains("token request refused")
                && line.contains("client_id=\"exchange-client\"")
                && line.contains("reason=\"token_exchange_unsupported\"")),
        "{logs}"
    );
    assert!(!logs.contains(platform_token));
}

/// The client secret is never written to Postgres or to logs, on success or
/// refusal.
#[tokio::test]
async fn the_client_secret_is_never_stored_or_logged() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.machine(ci_machine());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let body = json(client_credentials(&server.broker_url(), CI, CI_SECRET).await).await;
    let sc = Scenario::connect(&server)
        .await
        .bearer(body["id_token"].as_str().unwrap())
        .org("analytics")
        .invocation("inv-secret", "sess-secret");
    sc.try_submit(model(TARGET)).await.expect("an RPC");
    let wrong = "wrong-secret-Pp20ke";
    client_credentials(&server.broker_url(), CI, wrong).await;

    let stored = rows::every_row_as_text(&server).await;
    assert!(stored.contains("inv-secret"), "the RPC was recorded");
    let logs = captured_logs();
    for secret in [CI_SECRET, wrong] {
        assert!(!stored.contains(secret), "{secret} stored");
        assert!(!logs.contains(secret), "{secret} logged");
    }
}

/// The machine token lifetime is the configured one.
#[tokio::test]
async fn the_machine_token_lifetime_is_configurable() {
    let idp = FakeIdp::start().await;
    idp.machine(ci_machine());
    let auth = deployment(&idp).machine_lifetime_minutes(60).build();
    let server = start_server_with_broker(&auth).await;
    let body = json(client_credentials(&server.broker_url(), CI, CI_SECRET).await).await;
    assert_eq!(body["expires_in"], 3600);
    let token = claims(body["id_token"].as_str().unwrap());
    assert_eq!(
        token["exp"].as_u64().unwrap() - token["iat"].as_u64().unwrap(),
        3600
    );
}

/// The broker publishes OIDC discovery and its signing keys.
#[tokio::test]
async fn the_broker_serves_discovery_and_jwks() {
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let http = reqwest::Client::new();
    let discovery: Value = http
        .get(format!(
            "{}/.well-known/openid-configuration",
            server.broker_url()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(discovery["issuer"], ISSUER);
    // The dbt client's auth URL is the authorization endpoint itself; like
    // hosted dbt State's, it is the issuer's root.
    assert_eq!(discovery["authorization_endpoint"], format!("{ISSUER}/"));
    assert_eq!(discovery["token_endpoint"], format!("{ISSUER}/token"));
    assert_eq!(discovery["jwks_uri"], format!("{ISSUER}/jwks"));
    assert_eq!(discovery["response_types_supported"], json!(["code"]));
    assert_eq!(
        discovery["grant_types_supported"],
        json!(["authorization_code", "client_credentials", "refresh_token"])
    );
    assert_eq!(
        discovery["code_challenge_methods_supported"],
        json!(["S256"])
    );
    // `none` for the dbt client, HTTP Basic for machines, and
    // `private_key_jwt` (RS256) for the UI.
    assert_eq!(
        discovery["token_endpoint_auth_methods_supported"],
        json!(["none", "client_secret_basic", "private_key_jwt"])
    );
    assert_eq!(
        discovery["token_endpoint_auth_signing_alg_values_supported"],
        json!(["RS256"])
    );
    assert_eq!(
        discovery["id_token_signing_alg_values_supported"],
        json!(["RS256"])
    );
    assert_eq!(discovery["scopes_supported"], json!(["openid"]));
    let claims = discovery["claims_supported"].as_array().unwrap();
    for claim in ["iss", "sub", "aud", "exp", "iat", "sid", "nonce", "scope"] {
        assert!(claims.contains(&json!(claim)), "{claim}: {claims:?}");
    }

    let jwks: Value = http
        .get(format!("{}/jwks", server.broker_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let kids: BTreeSet<&str> = jwks["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|key| {
            assert_eq!(key["kty"], "RSA");
            assert_eq!(key["alg"], "RS256");
            assert_eq!(key["use"], "sig");
            assert!(key.get("d").is_none(), "a private key member was published");
            key["kid"].as_str().unwrap()
        })
        .collect();
    assert_eq!(kids, BTreeSet::from(["test-key-1", "test-key-2"]));
}

/// A gRPC server without signing keys fetches a standalone broker's keys
/// through its discovery, and keeps them: once the broker is gone, tokens it
/// issued still verify, and tokens no published key signed never do.
#[tokio::test]
async fn a_standalone_brokers_keys_are_fetched_and_cached_by_the_grpc_server() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.machine(ci_machine());
    let broker = start_broker(deployment(&idp)).await;
    let grpc_auth = TestAuth::builder(ORGANIZATIONS)
        .issuer(&broker.url())
        .without_signing()
        .build();
    let server = start_server_oidc(&grpc_auth).await;

    let body = json(client_credentials(&broker.url(), CI, CI_SECRET).await).await;
    let id_token = body["id_token"].as_str().unwrap().to_owned();
    let sc = Scenario::connect(&server).await.org("analytics");
    for _ in 0..2 {
        sc.bearer(&id_token)
            .try_submit(model(TARGET))
            .await
            .expect("verified with the broker's published keys");
    }
    let jwks = format!("jwks=\"{}/jwks\"", broker.url());
    let fetches = captured_logs()
        .lines()
        .filter(|line| line.contains("signing keys loaded") && line.contains(&jwks))
        .count();
    assert_eq!(fetches, 1, "the keys are fetched once and cached");

    let unpublished = broker
        .auth
        .signer_with(SignerOptions {
            issuer: &broker.url(),
            key: TestKey::Unknown,
            ..SignerOptions::default()
        })
        .machine("principal-ci")
        .grant("analytics", Role::Developer)
        .token();
    drop(broker);

    sc.bearer(&id_token)
        .try_submit(model(TARGET))
        .await
        .expect("cached keys outlive the broker");
    let refused = sc
        .bearer(&unpublished)
        .try_submit(model(TARGET))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated);
}

/// A machine Principal is the IdP issuer plus the configured subject claim,
/// as a person is.
#[tokio::test]
async fn the_machine_principal_follows_the_configured_subject_claim() {
    capture_logs();
    let idp = FakeIdp::start().await;
    for client_id in ["ci-blue", "ci-green"] {
        idp.machine(ci_as(client_id).claim("oid", json!("sp-object-1")));
    }
    idp.machine(ci_as("ci-no-oid"));
    let principal = |server: &support::TestServer, client_id: &'static str| {
        let url = server.broker_url();
        async move {
            let body = json(client_credentials(&url, client_id, CI_SECRET).await).await;
            claims(body["id_token"].as_str().expect("an id_token"))["sub"].clone()
        }
    };
    let by_sub = start_server_with_broker(&deployment(&idp).build()).await;
    assert_ne!(
        principal(&by_sub, "ci-blue").await,
        principal(&by_sub, "ci-green").await
    );
    let by_oid = start_server_with_broker(&deployment(&idp).subject_claim("oid").build()).await;
    assert_eq!(
        principal(&by_oid, "ci-blue").await,
        principal(&by_oid, "ci-green").await
    );
    assert_refused(
        &by_oid,
        "ci-no-oid",
        CI_SECRET,
        400,
        "unauthorized_client",
        "upstream_token_missing_subject",
    )
    .await;
    assert!(captured_logs()
        .lines()
        .any(|line| line.contains("client_id=\"ci-no-oid\"")
            && line.contains("missing_claims=\"oid\"")));
}
