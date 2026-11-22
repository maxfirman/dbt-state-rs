//! Shared logins and retries across Token broker replicas (#224): two dbt
//! processes that share one login, or a client retrying after a lost
//! response, renew with the same refresh token, possibly at different
//! replicas. A refresh token rotated less than 60 s ago still renews,
//! without asking the Upstream IdP again when the session's evidence is
//! that recent, so the IdP sees each of its refresh tokens at most once.
//! Presented after the window, it is a replay and revokes the session.
//!
//! Each test runs two standalone brokers on one Postgres schema and clock
//! (`start_broker_beside`), renewing as the pinned clients post it
//! (`support/broker_client.rs`) against the fake IdP (`support/fake_idp.rs`).

#[path = "support.rs"]
mod support;

use std::time::Duration;

use serde_json::{json, Map, Value};
use support::broker_client::{
    authorization_code, claims, json, refresh_token, SignIn, DBT_CLIENT_ID, LOOPBACK_REDIRECT_URI,
};
use support::browser::Browser;
use support::fake_idp::{FakeIdp, UpstreamPerson};
use support::oidc::{capture_logs, captured_logs, TestAuth, TestAuthBuilder};
use support::{start_broker, start_broker_beside, TestBroker};

/// People in `data-eng` are developers in `analytics`, in `data-admins`
/// admins there.
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(&[("analytics", false)])
        .upstream(idp)
        .rule("person", "groups", "data-eng", "analytics", "developer")
        .rule("person", "groups", "data-admins", "analytics", "admin")
}

const ANALYTICS_DEVELOPER: &str =
    "runcache:scope:app:analytics:developer runcache:scope:org:analytics:developer";
const ANALYTICS_ADMIN: &str =
    "runcache:scope:app:analytics:admin runcache:scope:org:analytics:admin";

/// Alice, in the `data-eng` group.
fn alice() -> UpstreamPerson {
    UpstreamPerson::new("alice-sub").claim("groups", json!(["data-eng"]))
}

/// Two replicas of one deployment, on one schema and clock, and the IdP
/// issuing at that clock.
async fn replicas(idp: &FakeIdp) -> (TestBroker, TestBroker) {
    let first = start_broker(deployment(idp)).await;
    let second = start_broker_beside(&first, deployment(idp));
    idp.follow(&first.clock);
    (first, second)
}

/// A browser sign-in of Alice at `broker` and its code exchange: the token
/// response, and the authorization code it used.
async fn signed_in(broker: &TestBroker, idp: &FakeIdp) -> (Map<String, Value>, SignIn, String) {
    idp.sign_in(alice());
    let sign_in = SignIn::new();
    let query = Browser::new()
        .visit(&sign_in.authorize_url(&broker.url()), LOOPBACK_REDIRECT_URI)
        .await
        .loopback();
    let code = query
        .get("code")
        .unwrap_or_else(|| panic!("no code in {query:?}"))
        .clone();
    let response = authorization_code(&broker.url(), &sign_in, &code).await;
    assert_eq!(response.status(), 200);
    (json(response).await, sign_in, code)
}

fn field<'a>(body: &'a Map<String, Value>, name: &str) -> &'a str {
    body[name]
        .as_str()
        .unwrap_or_else(|| panic!("no {name} in {body:?}"))
}

fn session_of(body: &Map<String, Value>) -> String {
    claims(field(body, "id_token"))["sid"]
        .as_str()
        .expect("a person token names its session")
        .to_owned()
}

/// Renew with `token` at `broker` as the dbt client does.
async fn renew(broker: &TestBroker, token: &str) -> reqwest::Response {
    refresh_token(&broker.url(), DBT_CLIENT_ID, token).await
}

/// Renew with `token` at `broker`, which must succeed: the token response.
async fn renewed(broker: &TestBroker, token: &str) -> Map<String, Value> {
    let response = renew(broker, token).await;
    assert_eq!(response.status(), 200, "renewal");
    assert_eq!(response.headers()["cache-control"], "no-store");
    json(response).await
}

/// The refresh tokens the brokers presented to the IdP, in order.
fn upstream_refreshes(idp: &FakeIdp) -> Vec<String> {
    idp.token_requests()
        .into_iter()
        .filter(|request| request.grant_type() == Some("refresh_token"))
        .map(|request| request.form["refresh_token"].clone())
        .collect()
}

/// A renewal through the grace window is a whole token response: a person
/// token in `session` with `scope`, and a refresh token of its own.
fn assert_renewal_in(body: &Map<String, Value>, session: &str, scope: &str) {
    assert_eq!(body["token_type"], "Bearer");
    assert_eq!(body["expires_in"], 60 * 60);
    assert_eq!(body["scope"], scope);
    let token = claims(field(body, "id_token"));
    assert_eq!(token["sid"], session);
    assert_eq!(token["scope"], scope);
    assert_eq!(token["azp"], DBT_CLIENT_ID);
    assert!(!field(body, "refresh_token").is_empty());
}

/// Whether a `token request refused` line for the `refresh_token` grant
/// names `reason` and each of `extra`.
fn refusal_logged(reason: &str, extra: &[&str]) -> bool {
    captured_logs().lines().any(|line| {
        line.contains("token request refused")
            && line.contains("grant=\"refresh_token\"")
            && line.contains(&format!("reason=\"{reason}\""))
            && extra.iter().all(|needle| line.contains(needle))
    })
}

/// The token endpoint refuses `response` with `invalid_grant`, logged with
/// `reason`.
async fn assert_denied(response: reqwest::Response, reason: &str) {
    assert_eq!(response.status(), 400, "{reason}");
    let body = json(response).await;
    assert_eq!(body["error"], "invalid_grant", "{reason}: {body:?}");
    assert!(!body.contains_key("refresh_token"), "{reason}");
    assert!(
        refusal_logged(reason, &[]),
        "no refusal log for {reason} in\n{}",
        captured_logs()
    );
}

/// Two dbt processes sharing one login present its refresh token at the
/// same moment, to different replicas. The session's row lock makes one
/// wait while the other renews at a slow IdP; the waiting one then finds
/// the token rotated within the window and the session's evidence just
/// renewed, so it renews too without asking the IdP. Each process ends up
/// with its own refresh token in the family, and both keep renewing, each
/// time with the IdP's newest refresh token.
#[tokio::test]
async fn two_processes_sharing_a_login_renew_at_once_through_two_replicas() {
    let idp = FakeIdp::start().await;
    let (first, second) = replicas(&idp).await;
    let (login, _, _) = signed_in(&first, &idp).await;
    let session = session_of(&login);
    let shared = field(&login, "refresh_token");

    idp.delay_refresh(Duration::from_millis(500));
    let (one, other) = tokio::join!(renew(&first, shared), renew(&second, shared));
    assert_eq!(one.status(), 200, "the first process");
    assert_eq!(other.status(), 200, "the second process");
    let (one, other) = (json(one).await, json(other).await);
    assert_renewal_in(&one, &session, ANALYTICS_DEVELOPER);
    assert_renewal_in(&other, &session, ANALYTICS_DEVELOPER);
    assert_ne!(one["refresh_token"], other["refresh_token"]);
    for body in [&one, &other] {
        assert_ne!(field(body, "refresh_token"), shared);
    }
    assert_eq!(
        upstream_refreshes(&idp),
        idp.issued_refresh_tokens()[..1],
        "the IdP was asked once, with the refresh token it issued at sign-in"
    );

    idp.delay_refresh(Duration::ZERO);
    let one = renewed(&first, field(&one, "refresh_token")).await;
    let other = renewed(&second, field(&other, "refresh_token")).await;
    assert_renewal_in(&one, &session, ANALYTICS_DEVELOPER);
    assert_renewal_in(&other, &session, ANALYTICS_DEVELOPER);
    let issued = idp.issued_refresh_tokens();
    assert_eq!(issued.len(), 4);
    assert_eq!(
        upstream_refreshes(&idp),
        issued[..3],
        "each upstream refresh token was presented once"
    );
}

/// A client whose renewal response was lost retries with the same refresh
/// token within the window, at another replica: it gets a token response
/// carrying the grant the lost renewal recorded, and the IdP is not asked
/// again. The retried token keeps renewing normally.
#[tokio::test]
async fn a_retry_after_a_lost_response_within_the_window_succeeds() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let (first, second) = replicas(&idp).await;
    let (login, _, _) = signed_in(&first, &idp).await;
    let session = session_of(&login);
    let token = field(&login, "refresh_token");

    // Alice became an admin since she signed in: the renewal whose response
    // is lost records it.
    idp.update(UpstreamPerson::new("alice-sub").claim("groups", json!(["data-admins"])));
    let lost = renew(&first, token).await;
    assert_eq!(lost.status(), 200);
    drop(lost);

    first.clock.advance(Duration::from_secs(59));
    let retried = renewed(&second, token).await;
    assert_renewal_in(&retried, &session, ANALYTICS_ADMIN);
    assert_eq!(
        upstream_refreshes(&idp),
        idp.issued_refresh_tokens()[..1],
        "the retry did not ask the IdP"
    );
    let logs = captured_logs();
    assert!(
        logs.lines().any(|line| line.contains("token issued")
            && line.contains("grant=\"refresh_token\"")
            && line.contains(&session)
            && line.contains("evidence=\"recent_renewal\"")),
        "no grace-window renewal log for {session} in\n{logs}"
    );

    let next = renewed(&first, field(&retried, "refresh_token")).await;
    assert_renewal_in(&next, &session, ANALYTICS_ADMIN);
    assert_eq!(
        upstream_refreshes(&idp),
        idp.issued_refresh_tokens()[..2],
        "the next renewal used the IdP's rotated refresh token"
    );
}

/// The window runs from the token's first rotation; renewing through it
/// does not extend it. Once 60 s have passed, presenting the rotated
/// token is a replay: the whole session is revoked, so every refresh token
/// issued in it, including the ones the window issued, stops working.
#[tokio::test]
async fn a_rotated_refresh_token_presented_after_the_window_revokes_the_session() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let (first, second) = replicas(&idp).await;
    let (login, _, _) = signed_in(&first, &idp).await;
    let session = session_of(&login);
    let token = field(&login, "refresh_token");

    let latest = renewed(&first, token).await;
    first.clock.advance(Duration::from_secs(59));
    let retried = renewed(&second, token).await;
    assert_renewal_in(&retried, &session, ANALYTICS_DEVELOPER);

    first.clock.advance(Duration::from_secs(1));
    assert_denied(
        renew(&first, token).await,
        "refresh_token_replayed_session_revoked",
    )
    .await;
    assert!(refusal_logged(
        "refresh_token_replayed_session_revoked",
        &[&session, "session_ended=true"]
    ));
    for issued in [&latest, &retried] {
        assert_denied(
            renew(&second, field(issued, "refresh_token")).await,
            "session_revoked",
        )
        .await;
    }
    assert_eq!(upstream_refreshes(&idp).len(), 1, "the IdP was asked once");
}

/// A revoked session never renews through the window: here its
/// authorization code was replayed just after a renewal.
#[tokio::test]
async fn a_revoked_session_does_not_renew_through_the_window() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let (first, second) = replicas(&idp).await;
    let (login, sign_in, code) = signed_in(&first, &idp).await;
    let token = field(&login, "refresh_token");

    let _renewed = renewed(&first, token).await;
    let replay = authorization_code(&second.url(), &sign_in, &code).await;
    assert_eq!(replay.status(), 400, "a code replay revokes the session");

    assert_denied(renew(&second, token).await, "session_revoked").await;
    assert_eq!(upstream_refreshes(&idp).len(), 1, "the IdP was asked once");
}
