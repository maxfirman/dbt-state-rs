//! Browser sign-in through the Token broker: a person's unmodified dbt
//! client opens the browser at the broker, the broker sends them to the
//! Upstream IdP (the fake one in `support/fake_idp.rs`, which approves the
//! signed-in person at once), and returns them to the client's loopback
//! handler with a code. The client exchanges the code for a dbt-shaped
//! person token and a refresh token, exactly as the pinned clients do
//! (`support/broker_client.rs`).
//!
//! The browser (`support/browser.rs`) follows the redirects; the broker's
//! public URL is routed to the test listener.

#[path = "support.rs"]
mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use serde_json::json;
use support::broker_client::{
    authorization_code, authorization_code_python, claims, json, SignIn, DBT_CLIENT_ID,
    LOOPBACK_REDIRECT_URI,
};
use support::browser::{Browser, Landing};
use support::fake_idp::{FakeIdp, UpstreamPerson, BROKER_CLIENT_ID, BROKER_CLIENT_SECRET};
use support::oidc::{
    capture_logs, captured_logs, BrokerCredential, TestAuth, TestAuthBuilder, AUDIENCE, ISSUER,
};
use support::scenario::{model, Scenario};
use support::{rows, start_server_with_broker, TestServer};
use tonic::Code;

const TARGET: &str = "\"DB\".\"S\".\"T\"";

/// Two active Organizations and a disabled one.
const ORGANIZATIONS: &[(&str, bool)] =
    &[("analytics", false), ("research", false), ("finance", true)];

/// The deployment's mapping rules: people in the `data-eng` group are
/// developers in `analytics`; CI machines with the `ci-pipeline` client id
/// (Entra ID v1 `appid`) are admins in `research`.
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(ORGANIZATIONS)
        .upstream(idp)
        .rule("person", "groups", "data-eng", "analytics", "developer")
        .rule("machine", "appid", "ci-pipeline", "research", "admin")
}

/// Alice, in the `data-eng` group.
fn alice() -> UpstreamPerson {
    UpstreamPerson::new("alice-sub").claim("groups", json!(["data-eng", "everyone"]))
}

/// A browser that reaches the broker at its public URL.
fn browser(server: &TestServer) -> Browser {
    Browser::new().route(ISSUER, &server.broker_url())
}

/// Start `sign_in` in the browser and follow it to the loopback handler.
async fn sign_in_through(server: &TestServer, sign_in: &SignIn) -> Landing {
    browser(server)
        .visit(&sign_in.authorize_url(ISSUER), LOOPBACK_REDIRECT_URI)
        .await
}

/// A complete browser sign-in: the loopback handler's code.
async fn code_for(server: &TestServer, sign_in: &SignIn) -> String {
    let query = sign_in_through(server, sign_in).await.loopback();
    assert_eq!(query.get("state"), Some(&sign_in.state), "{query:?}");
    query
        .get("code")
        .cloned()
        .unwrap_or_else(|| panic!("no code in {query:?}"))
}

/// A person signs in through the browser and makes an RPC with the
/// `id_token` the code exchange returns.
#[tokio::test]
async fn a_person_signs_in_through_the_browser_and_makes_an_rpc() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;

    let sign_in = SignIn::new();
    let code = code_for(&server, &sign_in).await;

    // The broker signed in at the IdP as a standard OIDC relying party.
    let authorize = idp.authorize_requests();
    assert_eq!(authorize.len(), 1);
    let upstream = &authorize[0];
    assert_eq!(upstream["client_id"], BROKER_CLIENT_ID);
    assert_eq!(upstream["redirect_uri"], format!("{ISSUER}/callback"));
    assert_eq!(upstream["response_type"], "code");
    let scopes: BTreeSet<&str> = upstream["scope"].split(' ').collect();
    assert!(scopes.contains("openid") && scopes.contains("offline_access"));
    assert_eq!(upstream["code_challenge_method"], "S256");
    for fresh in ["state", "nonce", "code_challenge"] {
        assert!(upstream[fresh].len() >= 43, "{fresh}: {upstream:?}");
    }
    assert_ne!(upstream["state"], sign_in.state, "the broker's own state");

    let response = authorization_code(&server.broker_url(), &sign_in, &code).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body = json(response).await;
    let fields: BTreeSet<&str> = body.keys().map(String::as_str).collect();
    assert_eq!(
        fields,
        BTreeSet::from([
            "expires_in",
            "id_token",
            "refresh_token",
            "scope",
            "token_type"
        ]),
        "{body:?}"
    );
    let scope = "runcache:scope:app:analytics:developer runcache:scope:org:analytics:developer";
    assert_eq!(body["scope"], scope);
    assert_eq!(body["token_type"], "Bearer");
    assert_eq!(body["expires_in"], 60 * 60);
    let refresh_token = body["refresh_token"].as_str().unwrap();
    assert!(refresh_token.len() >= 43);

    let id_token = body["id_token"].as_str().unwrap();
    let token = claims(id_token);
    assert_eq!(token["iss"], ISSUER);
    assert_eq!(token["aud"], AUDIENCE);
    assert_eq!(token["azp"], DBT_CLIENT_ID);
    assert_eq!(token["scope"], scope);
    assert_eq!(token["email"], "alice-sub@example.test");
    assert_eq!(token["name"], "Person alice-sub");
    assert_eq!(
        token["exp"].as_u64().unwrap() - token["iat"].as_u64().unwrap(),
        60 * 60
    );
    let session = token["sid"]
        .as_str()
        .expect("a person token names its session");
    let principal = token["sub"].as_str().unwrap().to_owned();
    assert_ne!(principal, "alice-sub", "the Principal id is opaque");

    let sc = Scenario::connect(&server)
        .await
        .bearer(id_token)
        .org("analytics");
    sc.try_submit(model(TARGET))
        .await
        .expect("an authorized RPC");
    let refused = sc
        .org("research")
        .try_submit(model(TARGET))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::PermissionDenied);

    // The same person signing in again is the same Principal, in a new
    // session; the Python client's exchange form works too.
    let again = SignIn::new();
    let code = code_for(&server, &again).await;
    let body = json(authorization_code_python(&server.broker_url(), &again, &code).await).await;
    let token = claims(body["id_token"].as_str().expect("an id_token"));
    assert_eq!(token["sub"], principal);
    assert_ne!(token["sid"], session);

    let logs = captured_logs();
    let issued = logs
        .lines()
        .find(|line| {
            line.contains("token issued") && line.contains(session) && line.contains(&principal)
        })
        .unwrap_or_else(|| panic!("no issue log for session {session} in\n{logs}"));
    for needle in [
        "grant=\"authorization_code\"",
        "kind=\"person\"",
        &format!("client_id=\"{DBT_CLIENT_ID}\""),
        "subject=\"alice-sub\"",
    ] {
        assert!(issued.contains(needle), "no {needle} in {issued}");
    }

    // No bearer value is logged, and none is stored in plain text: the
    // broker keeps hashes, and the IdP's refresh token encrypted.
    let stored = rows::every_row_as_text(&server).await;
    let upstream_refresh_tokens = idp.issued_refresh_tokens();
    assert_eq!(upstream_refresh_tokens.len(), 2);
    for secret in upstream_refresh_tokens.iter().map(String::as_str).chain([
        id_token,
        refresh_token,
        code.as_str(),
    ]) {
        assert!(!logs.contains(secret), "a secret was logged");
        assert!(!stored.contains(secret), "a secret was stored");
    }
}

/// Whether a `warn` refusal line names `reason` (and `extra`).
fn refusal_logged(message: &str, reason: &str, extra: &[&str]) -> bool {
    captured_logs().lines().any(|line| {
        line.contains(message)
            && line.contains(&format!("reason=\"{reason}\""))
            && extra.iter().all(|needle| line.contains(needle))
    })
}

/// The token endpoint refuses `response` as an OAuth error, logged with
/// `reason`.
async fn assert_token_refused(response: reqwest::Response, status: u16, error: &str, reason: &str) {
    assert_eq!(response.status(), status, "{reason}");
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body = json(response).await;
    assert_eq!(body["error"], error, "{reason}: {body:?}");
    assert!(!body.contains_key("id_token"), "{reason}");
    assert!(!body.contains_key("refresh_token"), "{reason}");
    assert!(
        refusal_logged(
            "token request refused",
            reason,
            &["grant=\"authorization_code\""]
        ),
        "no refusal log for {reason} in\n{}",
        captured_logs()
    );
}

/// An authorization code is used up by its first exchange: a replay is
/// refused.
#[tokio::test]
async fn an_authorization_code_is_single_use() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let sign_in = SignIn::new();
    let code = code_for(&server, &sign_in).await;
    let first = authorization_code(&server.broker_url(), &sign_in, &code).await;
    assert_eq!(first.status(), 200);
    let replay = authorization_code(&server.broker_url(), &sign_in, &code).await;
    assert_token_refused(
        replay,
        400,
        "invalid_grant",
        "code_replayed_session_revoked",
    )
    .await;
    let unknown = authorization_code(&server.broker_url(), &sign_in, "never-issued").await;
    assert_token_refused(unknown, 400, "invalid_grant", "unknown_code").await;
}

/// An authorization code is valid for 60 seconds.
#[tokio::test]
async fn an_authorization_code_expires_after_60_seconds() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;

    let in_time = SignIn::new();
    let code = code_for(&server, &in_time).await;
    server.broker_clock.advance(Duration::from_secs(59));
    let response = authorization_code(&server.broker_url(), &in_time, &code).await;
    assert_eq!(response.status(), 200, "{:?}", json(response).await);

    let late = SignIn::new();
    let code = code_for(&server, &late).await;
    server.broker_clock.advance(Duration::from_secs(61));
    let response = authorization_code(&server.broker_url(), &late, &code).await;
    assert_token_refused(response, 400, "invalid_grant", "code_expired").await;
}

/// The code is bound to the PKCE challenge of the authorize request, and is
/// used up by an exchange with the wrong verifier.
#[tokio::test]
async fn an_exchange_with_the_wrong_pkce_verifier_is_refused() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let sign_in = SignIn::new();
    let code = code_for(&server, &sign_in).await;
    let mut wrong = sign_in.clone();
    wrong.verifier = SignIn::new().verifier;
    let response = authorization_code(&server.broker_url(), &wrong, &code).await;
    assert_token_refused(response, 400, "invalid_grant", "pkce_mismatch").await;
    let response = authorization_code(&server.broker_url(), &sign_in, &code).await;
    assert_eq!(response.status(), 400, "the code was used up");
}

/// Browser sign-in requires PKCE S256: without it the browser goes back to
/// the dbt client with an error, before the IdP is involved.
#[tokio::test]
async fn sign_in_without_pkce_s256_is_refused() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    for sign_in in [SignIn::new().without_pkce(), SignIn::new().plain_pkce()] {
        let query = sign_in_through(&server, &sign_in).await.loopback();
        assert_eq!(query["error"], "invalid_request", "{query:?}");
        assert_eq!(query["state"], sign_in.state);
        assert!(query["error_description"].contains("S256"));
        assert!(!query.contains_key("code"));
    }
    assert!(idp.authorize_requests().is_empty());
    assert!(refusal_logged("sign-in refused", "pkce_s256_required", &[]));
}

/// Only the exact loopback redirect URI is registered: another one gets an
/// error page, never a redirect, and an exchange naming another one than
/// the authorize request is refused.
#[tokio::test]
async fn redirect_uris_must_match_exactly() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    for other in [
        "http://127.0.0.1:29526/handler",
        "http://localhost:29525/handler",
        "http://127.0.0.1:29525/handler/",
        "https://attacker.example/handler",
    ] {
        let sign_in = SignIn::new().redirect_uri(other);
        let landing = browser(&server)
            .visit(&sign_in.authorize_url(ISSUER), other)
            .await;
        let Landing::Page { status, body } = landing else {
            panic!("{other}: redirected: {landing:?}");
        };
        assert_eq!(status, 400, "{other}");
        assert!(body.contains("redirect URI"), "{body}");
    }
    assert!(idp.authorize_requests().is_empty());
    assert!(refusal_logged(
        "sign-in refused",
        "redirect_uri_mismatch",
        &[]
    ));

    let sign_in = SignIn::new();
    let code = code_for(&server, &sign_in).await;
    let mismatched = sign_in.clone().redirect_uri("http://127.0.0.1:29525/other");
    let response = authorization_code(&server.broker_url(), &mismatched, &code).await;
    assert_token_refused(response, 400, "invalid_grant", "redirect_uri_mismatch").await;
}

/// The dbt client registration accepts the client's built-in client id by
/// default, or the configured one instead; any other is unknown, at the
/// authorize request (an error page) and at the token endpoint.
#[tokio::test]
async fn only_the_registered_dbt_client_id_is_accepted() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let landing = sign_in_through(&server, &SignIn::new().client_id("someone-else")).await;
    let Landing::Page { status, body } = landing else {
        panic!("redirected: {landing:?}");
    };
    assert_eq!(status, 400);
    assert!(body.contains("not registered"), "{body}");
    assert!(refusal_logged(
        "sign-in refused",
        "unknown_client",
        &["client_id=\"someone-else\""]
    ));
    let sign_in = SignIn::new();
    let code = code_for(&server, &sign_in).await;
    let response = authorization_code(
        &server.broker_url(),
        &sign_in.clone().client_id("someone-else"),
        &code,
    )
    .await;
    assert_token_refused(response, 401, "invalid_client", "unknown_client").await;

    let configured =
        start_server_with_broker(&deployment(&idp).dbt_client_id("our-dbt-client").build()).await;
    let ours = SignIn::new().client_id("our-dbt-client");
    let code = code_for(&configured, &ours).await;
    let body = json(authorization_code(&configured.broker_url(), &ours, &code).await).await;
    assert_eq!(
        claims(body["id_token"].as_str().unwrap())["azp"],
        "our-dbt-client"
    );
    let landing = sign_in_through(&configured, &SignIn::new()).await;
    assert!(
        matches!(landing, Landing::Page { status: 400, .. }),
        "the built-in id is no longer registered: {landing:?}"
    );
}

/// Pending sign-ins are capped per client IP; a completed one frees its
/// slot. Behind a proxy, the configured header names the client.
#[tokio::test]
async fn pending_sign_ins_are_capped_per_client_ip() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server =
        start_server_with_broker(&deployment(&idp).max_pending_logins_per_ip(2).build()).await;
    // A browser that stops at the IdP leaves its sign-in pending.
    let stop_at_idp = |sign_in: SignIn| {
        let browser = browser(&server);
        let idp = idp.issuer();
        async move { browser.visit(&sign_in.authorize_url(ISSUER), &idp).await }
    };
    for _ in 0..2 {
        assert!(matches!(
            stop_at_idp(SignIn::new()).await,
            Landing::Loopback(_)
        ));
    }
    let refused = SignIn::new();
    let query = sign_in_through(&server, &refused).await.loopback();
    assert_eq!(query["error"], "temporarily_unavailable", "{query:?}");
    assert_eq!(query["state"], refused.state);
    assert!(refusal_logged(
        "sign-in refused",
        "too_many_pending_logins",
        &[]
    ));

    // Pending sign-ins expire after ten minutes.
    server.broker_clock.advance(Duration::from_secs(10 * 60));
    code_for(&server, &SignIn::new()).await;
    assert!(matches!(
        stop_at_idp(SignIn::new()).await,
        Landing::Loopback(_)
    ));
    let query = sign_in_through(&server, &SignIn::new()).await.loopback();
    assert!(
        query.contains_key("code"),
        "a completed sign-in is not pending: {query:?}"
    );
    let query = sign_in_through(&server, &SignIn::new()).await.loopback();
    assert!(query.contains_key("code"), "{query:?}");

    // Behind a proxy, each client is counted by the header's last entry.
    let proxied = start_server_with_broker(
        &deployment(&idp)
            .max_pending_logins_per_ip(1)
            .client_ip_header("x-forwarded-for")
            .build(),
    )
    .await;
    let from = |client: &'static str| {
        let url = SignIn::new().authorize_url(&proxied.broker_url());
        async move {
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap()
                .get(url)
                .header("x-forwarded-for", format!("198.51.100.9, {client}"))
                .send()
                .await
                .unwrap()
                .headers()["location"]
                .to_str()
                .unwrap()
                .to_owned()
        }
    };
    let idp_url = idp.issuer();
    assert!(from("203.0.113.1").await.starts_with(&idp_url));
    assert!(from("203.0.113.2").await.starts_with(&idp_url));
    assert!(
        from("203.0.113.1").await.starts_with(LOOPBACK_REDIRECT_URI),
        "the second sign-in from one client is refused"
    );
}

/// The callback only continues a pending sign-in, once: an unknown or
/// reused `state` gets an error page.
#[tokio::test]
async fn the_callback_requires_the_state_of_a_pending_sign_in() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let sign_in = SignIn::new();
    code_for(&server, &sign_in).await;
    let upstream_state = &idp.authorize_requests()[0]["state"];
    for state in [upstream_state.as_str(), "forged-state"] {
        let landing = browser(&server)
            .visit(
                &format!("{ISSUER}/callback?code=idp-code-x&state={state}"),
                LOOPBACK_REDIRECT_URI,
            )
            .await;
        let Landing::Page { status, body } = landing else {
            panic!("{state}: {landing:?}");
        };
        assert_eq!(status, 400);
        assert!(body.contains("expired or was already completed"), "{body}");
    }
    let landing = browser(&server)
        .visit(&format!("{ISSUER}/callback?code=x"), LOOPBACK_REDIRECT_URI)
        .await;
    assert!(
        matches!(landing, Landing::Page { status: 400, .. }),
        "{landing:?}"
    );
    assert!(refusal_logged(
        "sign-in refused",
        "unknown_login_state",
        &[]
    ));
    assert!(refusal_logged("sign-in refused", "missing_state", &[]));
}

/// The upstream ID token must be signed by the IdP, for the broker's client
/// id, from the IdP's issuer, and carry the nonce the broker sent;
/// otherwise the person is not signed in.
#[tokio::test]
async fn an_untrusted_upstream_id_token_signs_nobody_in() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let cases = [
        (alice().wrong_nonce(), "upstream_token_wrong_nonce"),
        (alice().signed_by_impostor(), "upstream_token_bad_signature"),
        (
            alice().audience(json!("another-app")),
            "upstream_token_wrong_audience",
        ),
        (
            alice().audience(json!([BROKER_CLIENT_ID, "another-app"])),
            "upstream_token_wrong_authorized_party",
        ),
        (
            alice().issuer("https://impostor.example"),
            "upstream_token_wrong_issuer",
        ),
        (alice().without_id_token(), "upstream_missing_id_token"),
    ];
    for (person, reason) in cases {
        idp.sign_in(person);
        let sign_in = SignIn::new();
        let query = sign_in_through(&server, &sign_in).await.loopback();
        assert_eq!(query["error"], "access_denied", "{reason}: {query:?}");
        assert_eq!(query["state"], sign_in.state);
        assert!(!query.contains_key("code"), "{reason}");
        assert!(
            refusal_logged("sign-in refused", reason, &["step=\"callback\""]),
            "no {reason} in\n{}",
            captured_logs()
        );
    }
    // Nobody signed in at the IdP.
    let fresh = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&fresh).build()).await;
    let query = sign_in_through(&server, &SignIn::new()).await.loopback();
    assert_eq!(query["error"], "access_denied");
    assert!(refusal_logged(
        "sign-in refused",
        "upstream_error",
        &["upstream=\"login_required\""]
    ));
}

/// Without an upstream refresh token a session could never be renewed, so
/// none is created.
#[tokio::test]
async fn a_sign_in_without_an_upstream_refresh_token_is_refused() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice().without_refresh_token());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let query = sign_in_through(&server, &SignIn::new()).await.loopback();
    assert_eq!(query["error"], "server_error", "{query:?}");
    assert!(refusal_logged(
        "sign-in refused",
        "upstream_missing_refresh_token",
        &[]
    ));
}

/// Only person rules apply to a person: a value matching a machine rule's
/// client id grants nothing, and a person no person rule maps is denied,
/// with a log naming the claims the rules read that the token lacked (such
/// as `groups` under Entra group overage).
#[tokio::test]
async fn only_person_rules_apply_and_unmapped_people_are_denied() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;

    idp.sign_in(alice().claim("appid", json!("ci-pipeline")));
    let sign_in = SignIn::new();
    let code = code_for(&server, &sign_in).await;
    let body = json(authorization_code(&server.broker_url(), &sign_in, &code).await).await;
    assert_eq!(
        body["scope"],
        "runcache:scope:app:analytics:developer runcache:scope:org:analytics:developer",
        "no machine grant in research"
    );

    idp.sign_in(UpstreamPerson::new("pipeline-lookalike").claim("appid", json!("ci-pipeline")));
    let denied = SignIn::new();
    let query = sign_in_through(&server, &denied).await.loopback();
    assert_eq!(query["error"], "access_denied", "{query:?}");
    assert_eq!(query["state"], denied.state);
    assert!(query["error_description"].contains("not in any group mapped to dbt State"));
    assert!(refusal_logged(
        "sign-in refused",
        "no_matching_rule",
        &[
            "subject=\"pipeline-lookalike\"",
            "missing_claims=\"groups\""
        ]
    ));

    idp.sign_in(UpstreamPerson::new("bob").claim("groups", json!(["other"])));
    let query = sign_in_through(&server, &SignIn::new()).await.loopback();
    assert_eq!(query["error"], "access_denied");
    assert!(
        captured_logs()
            .lines()
            .any(|line| line.contains("no_matching_rule")
                && line.contains("subject=\"bob\"")
                && !line.contains("missing_claims")),
        "bob has the claim the rules read"
    );
}

/// A Principal is the IdP issuer plus the configured subject claim; email
/// is for display only.
#[tokio::test]
async fn the_principal_is_the_issuer_and_the_configured_subject_claim() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let principal_of = |server: &TestServer, person: UpstreamPerson| {
        idp.sign_in(person);
        let url = server.broker_url();
        let browser = browser(server);
        async move {
            let sign_in = SignIn::new();
            let query = browser
                .visit(&sign_in.authorize_url(ISSUER), LOOPBACK_REDIRECT_URI)
                .await
                .loopback();
            let body = json(authorization_code(&url, &sign_in, &query["code"]).await).await;
            claims(body["id_token"].as_str().expect("an id_token"))["sub"]
                .as_str()
                .unwrap()
                .to_owned()
        }
    };

    // By default `sub`: a changed email is the same Principal.
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let first = principal_of(&server, alice()).await;
    let renamed = principal_of(&server, alice().claim("email", json!("a.new@example.test"))).await;
    assert_eq!(first, renamed);
    let other = principal_of(
        &server,
        UpstreamPerson::new("someone-else").claim("groups", json!(["data-eng"])),
    )
    .await;
    assert_ne!(first, other);

    // With `subject_claim = "oid"`, the same `oid` under another `sub`
    // (for example another app registration) is the same Principal.
    let server = start_server_with_broker(&deployment(&idp).subject_claim("oid").build()).await;
    let with_oid = |sub: &str| alice_as(sub).claim("oid", json!("0000-oid-alice"));
    let by_oid = principal_of(&server, with_oid("pairwise-1")).await;
    assert_eq!(by_oid, principal_of(&server, with_oid("pairwise-2")).await);
    assert_ne!(by_oid, first);
    idp.sign_in(alice());
    let query = sign_in_through(&server, &SignIn::new()).await.loopback();
    assert_eq!(query["error"], "access_denied", "no oid: {query:?}");
    assert!(refusal_logged(
        "sign-in refused",
        "upstream_token_missing_subject",
        &["missing_claims=\"oid\""]
    ));
}

fn alice_as(subject: &str) -> UpstreamPerson {
    UpstreamPerson::new(subject).claim("groups", json!(["data-eng"]))
}

/// Person tokens live the configured person lifetime.
#[tokio::test]
async fn the_person_token_lifetime_is_configurable() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server =
        start_server_with_broker(&deployment(&idp).person_lifetime_minutes(15).build()).await;
    let sign_in = SignIn::new();
    let code = code_for(&server, &sign_in).await;
    let body = json(authorization_code(&server.broker_url(), &sign_in, &code).await).await;
    assert_eq!(body["expires_in"], 15 * 60);
    let token = claims(body["id_token"].as_str().unwrap());
    assert_eq!(
        token["exp"].as_u64().unwrap() - token["iat"].as_u64().unwrap(),
        15 * 60
    );
    assert!(token["sid"].is_string());
}

/// With a private key configured, the broker authenticates at the IdP with
/// `private_key_jwt` and sends no secret; otherwise with its client secret
/// over HTTP Basic.
#[tokio::test]
async fn the_broker_authenticates_at_the_idp_with_its_configured_credential() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(
        &deployment(&idp)
            .broker_credential(BrokerCredential::PrivateKeyJwt)
            .build(),
    )
    .await;
    code_for(&server, &SignIn::new()).await;
    let exchange = idp.token_requests().pop().expect("a code exchange");
    assert_eq!(exchange.grant_type(), Some("authorization_code"));
    assert_eq!(exchange.basic, None);
    assert_eq!(
        exchange.form["client_assertion_type"],
        "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
    );
    assert_eq!(exchange.form["client_id"], BROKER_CLIENT_ID);
    assert!(!exchange.form.contains_key("client_secret"));
    assert_eq!(exchange.form["redirect_uri"], format!("{ISSUER}/callback"));
    assert!(exchange.form["code_verifier"].len() >= 43);

    let server = start_server_with_broker(&deployment(&idp).build()).await;
    code_for(&server, &SignIn::new()).await;
    let exchange = idp.token_requests().pop().expect("a code exchange");
    assert_eq!(
        exchange.basic,
        Some((BROKER_CLIENT_ID.to_owned(), BROKER_CLIENT_SECRET.to_owned()))
    );
    assert!(!exchange.form.contains_key("client_assertion"));
}

/// A discovered authorization endpoint must be https (plain http only on
/// loopback): the browser is never sent to a plaintext one.
#[tokio::test]
async fn a_plaintext_upstream_authorization_endpoint_is_never_used() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    idp.advertise(
        "authorization_endpoint",
        "http://idp.example.test/authorize",
    );
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let query = sign_in_through(&server, &SignIn::new()).await.loopback();
    assert_eq!(query["error"], "temporarily_unavailable", "{query:?}");
    assert!(idp.authorize_requests().is_empty());
    assert!(refusal_logged(
        "sign-in refused",
        "upstream_unavailable",
        &["authorization_endpoint"]
    ));
}
