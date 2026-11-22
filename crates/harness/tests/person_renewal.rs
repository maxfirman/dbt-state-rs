//! Person session renewal through the Token broker: a signed-in person's
//! dbt client renews its token with the `refresh_token` grant, exactly as
//! the pinned clients post it (`support/broker_client.rs`), and the broker
//! renews only on fresh proof from the Upstream IdP (the fake one in
//! `support/fake_idp.rs`) that the same Principal still maps to an
//! Organization.

#[path = "support.rs"]
mod support;

use std::time::Duration;

use serde_json::{json, Map, Value};
use support::broker_client::{
    authorization_code, claims, json, refresh_token, SignIn, DBT_CLIENT_ID, LOOPBACK_REDIRECT_URI,
};
use support::browser::Browser;
use support::fake_idp::{FakeIdp, UpstreamPerson, BROKER_CLIENT_ID, BROKER_CLIENT_SECRET};
use support::oidc::{capture_logs, captured_logs, TestAuth, TestAuthBuilder, ISSUER};
use support::scenario::{model, Scenario};
use support::{rows, start_broker, start_broker_beside, start_server_with_broker, TestServer};
use tonic::Code;

const TARGET: &str = "\"DB\".\"S\".\"T\"";

const ORGANIZATIONS: &[(&str, bool)] =
    &[("analytics", false), ("research", false), ("finance", true)];

/// People in `data-eng` are developers in `analytics`, in `data-admins`
/// admins there, and in `researchers` developers in `research`.
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(ORGANIZATIONS)
        .upstream(idp)
        .rule("person", "groups", "data-eng", "analytics", "developer")
        .rule("person", "groups", "data-admins", "analytics", "admin")
        .rule("person", "groups", "researchers", "research", "developer")
}

const ANALYTICS_DEVELOPER: &str =
    "runcache:scope:app:analytics:developer runcache:scope:org:analytics:developer";

/// Alice, in the `data-eng` group.
fn alice() -> UpstreamPerson {
    UpstreamPerson::new("alice-sub").claim("groups", json!(["data-eng"]))
}

/// Sign `person` in through the browser and exchange the code: the token
/// response.
async fn signed_in(
    server: &TestServer,
    idp: &FakeIdp,
    person: UpstreamPerson,
) -> Map<String, Value> {
    idp.sign_in(person);
    let sign_in = SignIn::new();
    let query = Browser::new()
        .route(ISSUER, &server.broker_url())
        .visit(&sign_in.authorize_url(ISSUER), LOOPBACK_REDIRECT_URI)
        .await
        .loopback();
    let code = query
        .get("code")
        .unwrap_or_else(|| panic!("no code in {query:?}"));
    let response = authorization_code(&server.broker_url(), &sign_in, code).await;
    assert_eq!(response.status(), 200);
    json(response).await
}

fn field<'a>(body: &'a Map<String, Value>, name: &str) -> &'a str {
    body[name]
        .as_str()
        .unwrap_or_else(|| panic!("no {name} in {body:?}"))
}

/// Renew with `token` as the dbt client does.
async fn renew(server: &TestServer, token: &str) -> reqwest::Response {
    refresh_token(&server.broker_url(), DBT_CLIENT_ID, token).await
}

/// The refresh tokens the broker presented to the IdP, in order.
fn upstream_refreshes(idp: &FakeIdp) -> Vec<String> {
    idp.token_requests()
        .into_iter()
        .filter(|request| request.grant_type() == Some("refresh_token"))
        .map(|request| request.form["refresh_token"].clone())
        .collect()
}

/// A person renews without a browser: the broker refreshes at the IdP with
/// its own credential, checks the IdP's fresh ID token, and returns a new
/// person token in the same session and a rotated refresh token. It uses
/// the IdP's rotated refresh token next time, and keeps it encrypted.
#[tokio::test]
async fn a_person_renews_without_a_browser_and_both_refresh_tokens_rotate() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let login = signed_in(&server, &idp, alice()).await;
    let first = claims(field(&login, "id_token"));

    let response = renew(&server, field(&login, "refresh_token")).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let renewed = json(response).await;
    let fields: Vec<&str> = renewed.keys().map(String::as_str).collect();
    assert_eq!(
        fields,
        [
            "expires_in",
            "id_token",
            "refresh_token",
            "scope",
            "token_type"
        ],
        "{renewed:?}"
    );
    assert_eq!(renewed["scope"], ANALYTICS_DEVELOPER);
    assert_eq!(renewed["token_type"], "Bearer");
    assert_eq!(renewed["expires_in"], 60 * 60);
    assert_ne!(renewed["refresh_token"], login["refresh_token"]);
    let token = claims(field(&renewed, "id_token"));
    for same in ["sub", "sid", "azp", "aud", "iss", "email", "name"] {
        assert_eq!(token[same], first[same], "{same}");
    }
    assert_eq!(token["scope"], ANALYTICS_DEVELOPER);

    Scenario::connect(&server)
        .await
        .bearer(field(&renewed, "id_token"))
        .org("analytics")
        .try_submit(model(TARGET))
        .await
        .expect("an RPC with the renewed token");

    // The broker refreshed at the IdP with the refresh token the IdP issued
    // at sign-in, authenticated as itself; then with the one the IdP
    // rotated to.
    let issued = idp.issued_refresh_tokens();
    assert_eq!(upstream_refreshes(&idp), [issued[0].clone()]);
    let request = idp.token_requests().pop().unwrap();
    assert_eq!(
        request.basic,
        Some((BROKER_CLIENT_ID.to_owned(), BROKER_CLIENT_SECRET.to_owned()))
    );
    let again = json(renew(&server, field(&renewed, "refresh_token")).await).await;
    assert_eq!(claims(field(&again, "id_token"))["sid"], first["sid"]);
    assert_eq!(
        upstream_refreshes(&idp),
        [issued[0].clone(), idp.issued_refresh_tokens()[1].clone()]
    );

    let logs = captured_logs();
    let session = first["sid"].as_str().unwrap();
    assert!(
        logs.lines().any(|line| line.contains("token issued")
            && line.contains("grant=\"refresh_token\"")
            && line.contains(session)
            && line.contains("evidence=\"id_token\"")),
        "no renewal log for {session} in\n{logs}"
    );
    let stored = rows::every_row_as_text(&server).await;
    for secret in idp
        .issued_refresh_tokens()
        .iter()
        .map(String::as_str)
        .chain([
            field(&renewed, "refresh_token"),
            field(&renewed, "id_token"),
            field(&again, "refresh_token"),
        ])
    {
        assert!(!logs.contains(secret), "a secret was logged");
        assert!(!stored.contains(secret), "a secret was stored");
    }
}

/// Whether a `token request refused` line for the `refresh_token` grant
/// names `reason` (and `extra`).
fn refusal_logged(reason: &str, extra: &[&str]) -> bool {
    captured_logs().lines().any(|line| {
        line.contains("token request refused")
            && line.contains("grant=\"refresh_token\"")
            && line.contains(&format!("reason=\"{reason}\""))
            && extra.iter().all(|needle| line.contains(needle))
    })
}

/// The token endpoint refuses `response` as an OAuth error, logged with
/// `reason`.
async fn assert_refused(response: reqwest::Response, status: u16, error: &str, reason: &str) {
    assert_eq!(response.status(), status, "{reason}");
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body = json(response).await;
    assert_eq!(body["error"], error, "{reason}: {body:?}");
    assert!(!body.contains_key("id_token"), "{reason}");
    assert!(!body.contains_key("refresh_token"), "{reason}");
    assert!(
        refusal_logged(reason, &[]),
        "no refusal log for {reason} in\n{}",
        captured_logs()
    );
}

/// A refresh token is used up by the renewal it starts. Presenting it again
/// is a replay: it revokes the whole session, so the token that renewal
/// returned stops working too.
#[tokio::test]
async fn a_rotated_refresh_token_presented_again_revokes_the_session() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let login = signed_in(&server, &idp, alice()).await;
    let session = claims(field(&login, "id_token"))["sid"]
        .as_str()
        .unwrap()
        .to_owned();
    let renewed = json(renew(&server, field(&login, "refresh_token")).await).await;

    let replay = renew(&server, field(&login, "refresh_token")).await;
    assert_refused(
        replay,
        400,
        "invalid_grant",
        "refresh_token_replayed_session_revoked",
    )
    .await;
    assert!(refusal_logged(
        "refresh_token_replayed_session_revoked",
        &[&session, "session_ended=true", "subject=\"alice-sub\""]
    ));
    let latest = renew(&server, field(&renewed, "refresh_token")).await;
    assert_refused(latest, 400, "invalid_grant", "session_revoked").await;
    assert_eq!(
        upstream_refreshes(&idp).len(),
        1,
        "a replay asks the IdP nothing"
    );

    let unknown = renew(&server, "never-issued").await;
    assert_refused(unknown, 400, "invalid_grant", "unknown_refresh_token").await;

    // Another session of the same person is unaffected.
    let other = signed_in(&server, &idp, alice()).await;
    assert_eq!(
        renew(&server, field(&other, "refresh_token"))
            .await
            .status(),
        200
    );
}

/// When the IdP's refresh response has no ID token, the evidence is its
/// userinfo for the new access token the refresh returned.
#[tokio::test]
async fn without_a_refreshed_id_token_the_evidence_is_userinfo_for_the_new_access_token() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let login = signed_in(&server, &idp, alice().without_refresh_id_token()).await;
    assert!(
        idp.userinfo_requests().is_empty(),
        "sign-in needs no userinfo"
    );

    let response = renew(&server, field(&login, "refresh_token")).await;
    assert_eq!(response.status(), 200);
    let renewed = json(response).await;
    assert_eq!(renewed["scope"], ANALYTICS_DEVELOPER);
    let token = claims(field(&renewed, "id_token"));
    assert_eq!(token["email"], "alice-sub@example.test");
    // The sign-in's access token, then the refresh's.
    let access_tokens = idp.issued_access_tokens();
    assert_eq!(access_tokens.len(), 2);
    assert_eq!(idp.userinfo_requests(), [Some(access_tokens[1].clone())]);
    assert!(captured_logs()
        .lines()
        .any(|line| line.contains("token issued")
            && line.contains("grant=\"refresh_token\"")
            && line.contains("evidence=\"userinfo\"")));
}

/// The person mapping is re-applied to the fresh evidence on every renewal,
/// so Role and Organization changes at the IdP take effect then, whether
/// the evidence is a refreshed ID token or userinfo.
#[tokio::test]
async fn role_and_organization_changes_at_the_idp_take_effect_on_renewal() {
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    for person in [alice(), alice().without_refresh_id_token()] {
        let login = signed_in(&server, &idp, person.clone()).await;
        assert_eq!(login["scope"], ANALYTICS_DEVELOPER);
        let token = field(&login, "id_token");
        let refused = Scenario::connect(&server)
            .await
            .bearer(token)
            .org("research")
            .try_submit(model(TARGET))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::PermissionDenied);

        // Promoted to admin in analytics, and added to research.
        idp.update(
            person
                .clone()
                .claim("groups", json!(["data-admins", "researchers"])),
        );
        let promoted = json(renew(&server, field(&login, "refresh_token")).await).await;
        let scope = "runcache:scope:app:analytics:admin runcache:scope:org:analytics:admin \
                     runcache:scope:app:research:developer runcache:scope:org:research:developer";
        assert_eq!(promoted["scope"], scope);
        assert_eq!(claims(field(&promoted, "id_token"))["scope"], scope);
        Scenario::connect(&server)
            .await
            .bearer(field(&promoted, "id_token"))
            .org("research")
            .try_submit(model(TARGET))
            .await
            .expect("an RPC in the Organization granted at renewal");

        // Moved out of analytics.
        idp.update(person.claim("groups", json!(["researchers"])));
        let moved = json(renew(&server, field(&promoted, "refresh_token")).await).await;
        assert_eq!(
            moved["scope"],
            "runcache:scope:app:research:developer runcache:scope:org:research:developer"
        );
        let refused = Scenario::connect(&server)
            .await
            .bearer(field(&moved, "id_token"))
            .org("analytics")
            .try_submit(model(TARGET))
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::PermissionDenied);
    }
}

/// Renewal is denied, and the session ends, when the IdP refuses the
/// refresh (the person is disabled, or the refresh fails) or its fresh
/// evidence is missing, untrusted, names another subject, lacks the claims
/// the rules read, or maps to no Organization. Once the IdP is back to
/// normal, the same refresh token still does not renew: the session is
/// over, and the person must sign in again.
#[tokio::test]
async fn renewal_is_denied_and_the_session_ends_without_fresh_proof_of_the_same_mapped_person() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    type Spoil = Box<dyn Fn(&FakeIdp)>;
    let update = |person: UpstreamPerson| -> Spoil {
        Box::new(move |idp| {
            idp.update(person.clone());
        })
    };
    let cases: Vec<(&str, Spoil, &str, &[&str])> = vec![
        (
            "the person is disabled at the IdP",
            Box::new(|idp| {
                idp.disable("alice-sub");
            }),
            "upstream_refused_grant",
            &["upstream=\"invalid_grant\""],
        ),
        (
            "the upstream refresh fails",
            Box::new(|idp| {
                idp.fail_refresh(400, "invalid_scope");
            }),
            "upstream_refused_grant",
            &["upstream=\"invalid_scope\""],
        ),
        (
            "the refreshed ID token names another subject",
            update(alice().renewed_as("mallory-sub")),
            "upstream_subject_mismatch",
            &[],
        ),
        (
            "userinfo names another subject",
            update(alice().without_refresh_id_token().renewed_as("mallory-sub")),
            "upstream_subject_mismatch",
            &[],
        ),
        (
            "the refreshed ID token is not signed by the IdP",
            update(alice().signed_by_impostor()),
            "upstream_token_bad_signature",
            &[],
        ),
        (
            "the refresh returns neither an ID token nor an access token",
            update(
                alice()
                    .without_refresh_id_token()
                    .without_refresh_access_token(),
            ),
            "upstream_missing_evidence",
            &[],
        ),
        (
            "userinfo lacks the claim the rules read",
            update(
                alice()
                    .without_refresh_id_token()
                    .userinfo_without("groups"),
            ),
            "no_matching_rule",
            &["missing_claims=\"groups\""],
        ),
        (
            "the refreshed ID token lacks the claim the rules read",
            update(alice().claim("groups", Value::Null)),
            "no_matching_rule",
            &["missing_claims=\"groups\""],
        ),
        (
            "the person maps to no Organization any more",
            update(alice().claim("groups", json!(["everyone"]))),
            "no_matching_rule",
            &[],
        ),
    ];
    for (case, spoil, reason, extra) in cases {
        let login = signed_in(&server, &idp, alice()).await;
        let session = claims(field(&login, "id_token"))["sid"]
            .as_str()
            .unwrap()
            .to_owned();
        spoil(&idp);
        let refreshes = upstream_refreshes(&idp).len();
        let denied = renew(&server, field(&login, "refresh_token")).await;
        assert_eq!(
            upstream_refreshes(&idp).len(),
            refreshes + 1,
            "{case}: asked the IdP"
        );
        assert_refused(denied, 400, "invalid_grant", reason).await;
        let mut needles = vec![session.as_str(), "session_ended=true"];
        needles.extend_from_slice(extra);
        assert!(
            refusal_logged(reason, &needles),
            "{case}: no {reason} {needles:?} in\n{}",
            captured_logs()
        );

        idp.enable("alice-sub").restore_refresh().update(alice());
        let again = renew(&server, field(&login, "refresh_token")).await;
        assert_eq!(again.status(), 400, "{case}: the session ended");
        assert!(refusal_logged("session_revoked", &[&session]), "{case}");
    }
}

/// When the broker cannot ask the IdP (it is unreachable or failing, or
/// refuses the broker's own credential), renewal is denied for now and the
/// session stays, so the same refresh token renews once the IdP is back.
#[tokio::test]
async fn renewal_waits_out_an_unavailable_idp_without_ending_the_session() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let login = signed_in(&server, &idp, alice()).await;
    for (status, error, reason) in [
        (503, "temporarily_unavailable", "upstream_unavailable"),
        (500, "server_error", "upstream_unavailable"),
        (401, "invalid_client", "upstream_rejected_client"),
    ] {
        idp.fail_refresh(status, error);
        let response = renew(&server, field(&login, "refresh_token")).await;
        assert_refused(response, 503, "temporarily_unavailable", reason).await;
        assert!(refusal_logged(reason, &["session_ended=false"]));
    }
    idp.restore_refresh();
    let renewed = renew(&server, field(&login, "refresh_token")).await;
    assert_eq!(renewed.status(), 200, "{:?}", json(renewed).await);
}

/// A refreshed ID token signed under a key id the broker has not loaded,
/// while the IdP's key set cannot be refetched (here: fetched less than
/// 30 s ago), is no proof either way: renewal waits, the session stays,
/// and the IdP's rotated refresh token is kept for the next attempt.
#[tokio::test]
async fn an_id_token_under_a_key_not_yet_loaded_waits_instead_of_ending_the_session() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let login = signed_in(&server, &idp, alice()).await;

    idp.update(alice().signed_with_unpublished_key());
    let response = renew(&server, field(&login, "refresh_token")).await;
    assert_refused(
        response,
        503,
        "temporarily_unavailable",
        "upstream_unavailable",
    )
    .await;
    assert!(refusal_logged(
        "upstream_unavailable",
        &["session_ended=false"]
    ));

    idp.update(alice());
    let renewed = renew(&server, field(&login, "refresh_token")).await;
    assert_eq!(renewed.status(), 200, "{:?}", json(renewed).await);
    let issued = idp.issued_refresh_tokens();
    assert_eq!(
        upstream_refreshes(&idp),
        [issued[0].clone(), issued[1].clone()],
        "the second attempt used the refresh token the IdP rotated to in the first"
    );
}

/// Renewal is denied once the session reaches its 12-hour cap, without
/// asking the IdP; until then it renews.
#[tokio::test]
async fn renewal_is_denied_once_the_session_reaches_the_12_hour_cap() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    idp.follow(&server.broker_clock);
    let login = signed_in(&server, &idp, alice()).await;

    server
        .broker_clock
        .advance(Duration::from_secs(12 * 60 * 60 - 60));
    let response = renew(&server, field(&login, "refresh_token")).await;
    assert_eq!(response.status(), 200, "just before the cap");
    let renewed = json(response).await;

    server.broker_clock.advance(Duration::from_secs(60));
    let refreshes = upstream_refreshes(&idp).len();
    let capped = renew(&server, field(&renewed, "refresh_token")).await;
    assert_refused(capped, 400, "invalid_grant", "session_expired").await;
    assert_eq!(
        upstream_refreshes(&idp).len(),
        refreshes,
        "the IdP was not asked"
    );
}

/// A revoked session does not renew. (Replaying its authorization code
/// revokes it; so will the admin command, #225.)
#[tokio::test]
async fn a_revoked_session_does_not_renew() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let sign_in = SignIn::new();
    let query = Browser::new()
        .route(ISSUER, &server.broker_url())
        .visit(&sign_in.authorize_url(ISSUER), LOOPBACK_REDIRECT_URI)
        .await
        .loopback();
    let login =
        json(authorization_code(&server.broker_url(), &sign_in, &query["code"]).await).await;
    let replay = authorization_code(&server.broker_url(), &sign_in, &query["code"]).await;
    assert_eq!(replay.status(), 400);

    let response = renew(&server, field(&login, "refresh_token")).await;
    assert_refused(response, 400, "invalid_grant", "session_revoked").await;
    assert!(upstream_refreshes(&idp).is_empty(), "the IdP was not asked");
}

/// A refresh token renews only for the dbt client it was issued to, and the
/// request needs one.
#[tokio::test]
async fn renewal_requires_the_dbt_client_and_a_refresh_token() {
    capture_logs();
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let login = signed_in(&server, &idp, alice()).await;
    let token = field(&login, "refresh_token");

    let other = refresh_token(&server.broker_url(), "someone-else", token).await;
    assert_refused(other, 401, "invalid_client", "unknown_client").await;
    let anonymous = reqwest::Client::new()
        .post(format!("{}/token", server.broker_url()))
        .form(&[("grant_type", "refresh_token"), ("refresh_token", token)])
        .send()
        .await
        .unwrap();
    assert_refused(anonymous, 401, "invalid_client", "missing_client_id").await;
    let tokenless = reqwest::Client::new()
        .post(format!("{}/token", server.broker_url()))
        .form(&[
            ("grant_type", "refresh_token"),
            ("client_id", DBT_CLIENT_ID),
        ])
        .send()
        .await
        .unwrap();
    assert_refused(tokenless, 400, "invalid_request", "missing_parameter").await;
    assert!(upstream_refreshes(&idp).is_empty());

    assert_eq!(renew(&server, token).await.status(), 200, "still renews");
}

/// An IdP that does not rotate its refresh tokens is asked with the same
/// one at every renewal; the dbt client's refresh token rotates anyway.
#[tokio::test]
async fn an_idp_that_keeps_its_refresh_token_is_asked_with_the_same_one() {
    let idp = FakeIdp::start().await;
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let login = signed_in(&server, &idp, alice().keeps_refresh_token()).await;
    let renewed = json(renew(&server, field(&login, "refresh_token")).await).await;
    let again = json(renew(&server, field(&renewed, "refresh_token")).await).await;
    assert_ne!(again["refresh_token"], renewed["refresh_token"]);
    let issued = idp.issued_refresh_tokens();
    assert_eq!(issued.len(), 1);
    assert_eq!(
        upstream_refreshes(&idp),
        [issued[0].clone(), issued[0].clone()]
    );
}

/// A deployment restarted without browser sign-in (no `[upstream]
/// client_id`) cannot renew the person sessions it started before: it
/// refuses their refresh tokens.
#[tokio::test]
async fn renewal_is_refused_once_browser_sign_in_is_no_longer_configured() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let before = start_broker(deployment(&idp)).await;
    let sign_in = SignIn::new();
    let query = Browser::new()
        .visit(&sign_in.authorize_url(&before.url()), LOOPBACK_REDIRECT_URI)
        .await
        .loopback();
    let login = json(authorization_code(&before.url(), &sign_in, &query["code"]).await).await;

    let after = start_broker_beside(&before, deployment(&idp).without_browser_sign_in());
    let response = refresh_token(&after.url(), DBT_CLIENT_ID, field(&login, "refresh_token")).await;
    assert_refused(response, 400, "invalid_grant", "sign_in_not_configured").await;
    assert!(upstream_refreshes(&idp).is_empty(), "the IdP was not asked");
}
