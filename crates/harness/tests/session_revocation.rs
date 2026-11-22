//! Person sessions on gRPC: every RPC with a person token checks that the
//! token's broker session (`sid`) is still active, so an operator who
//! revokes a Principal's sessions with the admin command cuts their access
//! within 30 s, without waiting for their tokens to expire.
//!
//! A session is active until it is revoked or reaches its 12-hour cap. Each
//! replica caches a session's status for at most 30 s (on the clock the
//! test moves, shared with the in-process Token broker). Machine tokens
//! carry no `sid` and are not checked. When the session status cannot be
//! read, the RPC is refused (`UNAVAILABLE`), never let through.

#[path = "support.rs"]
mod support;

use std::time::{Duration, SystemTime};

use dbt_state_auth::Role;
use serde_json::json;
use support::broker_client::{
    authorization_code, claims, json, refresh_token, SignIn, LOOPBACK_REDIRECT_URI,
};
use support::browser::Browser;
use support::fake_idp::{FakeIdp, UpstreamPerson};
use support::oidc::{capture_logs, captured_logs, TestAuth, TestAuthBuilder, ISSUER};
use support::scenario::{model, Scenario};
use support::{fault, start_server_with_broker, TestServer};
use tonic::Code;

const TARGET: &str = "\"DB\".\"S\".\"T\"";
const ORGANIZATIONS: &[(&str, bool)] = &[("analytics", false)];
const INACTIVE: &str = "bearer token session is not active";

/// People in the `data-eng` group are developers in `analytics`.
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(ORGANIZATIONS).upstream(idp).rule(
        "person",
        "groups",
        "data-eng",
        "analytics",
        "developer",
    )
}

fn person(subject: &str) -> UpstreamPerson {
    UpstreamPerson::new(subject).claim("groups", json!(["data-eng"]))
}

/// `subject` signs in through the browser, up to the code the dbt client's
/// loopback handler receives.
async fn sign_in_code(server: &TestServer, idp: &FakeIdp, subject: &str) -> (SignIn, String) {
    idp.sign_in(person(subject));
    let sign_in = SignIn::new();
    let query = Browser::new()
        .route(ISSUER, &server.broker_url())
        .visit(&sign_in.authorize_url(ISSUER), LOOPBACK_REDIRECT_URI)
        .await
        .loopback();
    let code = query.get("code").expect("a code").clone();
    (sign_in, code)
}

/// `subject` signs in through the browser: a new session's person token.
async fn sign_in(server: &TestServer, idp: &FakeIdp, subject: &str) -> String {
    let (sign_in, code) = sign_in_code(server, idp, subject).await;
    let response = authorization_code(&server.broker_url(), &sign_in, &code).await;
    assert_eq!(response.status(), 200);
    let body = json(response).await;
    body["id_token"].as_str().expect("an id_token").to_owned()
}

/// The admin command's revocation of `subject`'s sessions.
async fn revoke(server: &TestServer, idp: &FakeIdp, subject: &str) -> dbt_state_broker::Revocation {
    dbt_state_broker::revoke_sessions(
        server.state.store.pool(),
        &idp.issuer(),
        subject,
        SystemTime::now(),
    )
    .await
    .expect("revoke")
}

/// A submit in `analytics` with `token`.
async fn submit(server: &TestServer, token: &str) -> Result<(), tonic::Status> {
    Scenario::connect(server)
        .await
        .bearer(token)
        .org("analytics")
        .try_submit(model(TARGET))
        .await
        .map(drop)
}

fn assert_inactive(result: Result<(), tonic::Status>, case: &str) {
    let status = result.expect_err(case);
    assert_eq!(status.code(), Code::Unauthenticated, "{case}: {status:?}");
    assert_eq!(status.message(), INACTIVE, "{case}");
}

/// Whether a `warn` gRPC refusal names `reason` and `needle`.
fn refusal_logged(reason: &str, needle: &str) -> bool {
    captured_logs().lines().any(|line| {
        line.contains("WARN")
            && line.contains("gRPC request refused")
            && line.contains(&format!("reason=\"{reason}\""))
            && line.contains(needle)
    })
}

/// Revoking a Principal's sessions refuses every person token of theirs on
/// gRPC within the 30 s cache bound, at once where this replica has not
/// cached the session. Another person, and machine tokens, keep working.
#[tokio::test]
async fn revoking_a_principal_s_sessions_refuses_their_rpcs_within_30_seconds() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    let laptop = sign_in(&server, &idp, "alice-sub").await;
    let desktop = sign_in(&server, &idp, "alice-sub").await;
    let bob = sign_in(&server, &idp, "bob-sub").await;
    let machine = auth
        .signer()
        .machine("principal-ci")
        .grant("analytics", Role::Developer)
        .token();
    submit(&server, &laptop).await.expect("an active session");
    submit(&server, &bob).await.expect("an active session");

    let revoked = revoke(&server, &idp, "alice-sub").await;
    assert_eq!(revoked.sessions, 2);
    let alice = claims(&laptop)["sub"].as_str().unwrap().to_owned();
    assert_eq!(revoked.principal.as_deref(), Some(alice.as_str()));

    // This replica never checked the desktop session: refused at once.
    assert_inactive(submit(&server, &desktop).await, "an uncached session");
    server.broker_clock.advance(Duration::from_secs(30));
    assert_inactive(submit(&server, &laptop).await, "after the cache bound");

    submit(&server, &bob)
        .await
        .expect("another Principal's session");
    submit(&server, &machine).await.expect("a machine token");

    let session = claims(&laptop)["sid"].as_str().unwrap().to_owned();
    assert!(
        refusal_logged("session_revoked", &format!("session=\"{session}\"")),
        "no refusal log in\n{}",
        captured_logs()
    );
    assert!(refusal_logged(
        "session_revoked",
        &format!("principal=\"{alice}\"")
    ));
    for secret in [&laptop, &desktop] {
        assert!(
            !captured_logs().contains(secret.as_str()),
            "a token was logged"
        );
    }
}

/// A revoked session issues no more tokens: its refresh token no longer
/// renews, and a sign-in still in progress when its Principal is revoked
/// gets no token from its code.
#[tokio::test]
async fn a_revoked_session_issues_no_more_tokens() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let (signed_in, code) = sign_in_code(&server, &idp, "alice-sub").await;
    let login = json(authorization_code(&server.broker_url(), &signed_in, &code).await).await;
    let refresh = login["refresh_token"].as_str().expect("a refresh token");
    let (in_progress, pending_code) = sign_in_code(&server, &idp, "alice-sub").await;
    assert_eq!(revoke(&server, &idp, "alice-sub").await.sessions, 2);

    for (case, response) in [
        (
            "refresh",
            refresh_token(&server.broker_url(), &signed_in.client_id, refresh).await,
        ),
        (
            "code exchange",
            authorization_code(&server.broker_url(), &in_progress, &pending_code).await,
        ),
    ] {
        assert_eq!(response.status(), 400, "{case}");
        let body = json(response).await;
        assert_eq!(body["error"], "invalid_grant", "{case}: {body:?}");
        assert!(!body.contains_key("id_token"), "{case}: {body:?}");
    }
    assert!(
        idp.token_requests()
            .iter()
            .all(|request| request.grant_type() != Some("refresh_token")),
        "a revoked session is never renewed at the IdP"
    );
}

/// A session ends at its 12-hour cap: its person tokens are refused even
/// while they have not expired.
#[tokio::test]
async fn a_person_token_is_refused_once_its_session_reaches_the_12_hour_cap() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let token = sign_in(&server, &idp, "alice-sub").await;
    submit(&server, &token).await.expect("an active session");

    server
        .broker_clock
        .advance(Duration::from_secs(12 * 60 * 60));
    assert_inactive(submit(&server, &token).await, "at the cap");
    let session = claims(&token)["sid"].as_str().unwrap().to_owned();
    assert!(refusal_logged(
        "session_expired",
        &format!("session=\"{session}\"")
    ));
}

/// A person token whose `sid` names no session the Token broker recorded
/// is refused; a machine token names none and needs none.
#[tokio::test]
async fn a_person_token_naming_no_recorded_session_is_refused() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    let signer = auth.signer();
    for session in ["session-1", "0b8a3c47-7a51-4b0f-9d3e-1f2f6c1d2e3a"] {
        let token = signer
            .person("principal-ada", session)
            .grant("analytics", Role::Developer)
            .token();
        assert_inactive(submit(&server, &token).await, session);
        assert!(refusal_logged(
            "session_unknown",
            &format!("session=\"{session}\"")
        ));
    }
    let machine = signer
        .machine("principal-ci")
        .grant("analytics", Role::Developer)
        .token();
    submit(&server, &machine).await.expect("a machine token");
}

/// When the session status cannot be read, a person's RPC is refused
/// (`UNAVAILABLE`, so the client may retry) rather than let through;
/// machine tokens are unaffected.
#[tokio::test]
async fn an_unreadable_session_status_refuses_the_rpc() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let auth = deployment(&idp).build();
    let server = start_server_with_broker(&auth).await;
    let token = sign_in(&server, &idp, "alice-sub").await;
    fault::fail_session_status(&server).await;

    let status = submit(&server, &token).await.expect_err("fails closed");
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert_eq!(status.message(), "session status unavailable");
    assert!(refusal_logged("session_status_unavailable", "principal="));

    let machine = auth
        .signer()
        .machine("principal-ci")
        .grant("analytics", Role::Developer)
        .token();
    submit(&server, &machine).await.expect("a machine token");
}
