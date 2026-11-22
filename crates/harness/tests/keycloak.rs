//! The Token broker against a real Upstream IdP: Keycloak in Docker
//! (`support/keycloak.rs`), with the realm in
//! `crates/harness/keycloak/dbt-state-realm.json`. The dbt client's
//! requests are exactly the pinned clients' (`support/broker_client.rs`);
//! the browser fills in Keycloak's own login form.
//!
//! Opt-in: every test skips unless `DBT_STATE_KEYCLOAK=1`. Start Keycloak
//! first with `scripts/keycloak.sh start` (CI's `keycloak` job does).

#[path = "support.rs"]
mod support;

use std::collections::BTreeMap;

use serde_json::{Map, Value};
use support::broker_client::{
    authorization_code, claims, client_credentials, json, refresh_token, SignIn, DBT_CLIENT_ID,
    LOOPBACK_REDIRECT_URI,
};
use support::browser::Browser;
use support::keycloak::{
    alice, Keycloak, CI_CLIENT_ID, CI_CLIENT_SECRET, OTHER_CLIENT_ID, OTHER_CLIENT_SECRET,
};
use support::oidc::{
    capture_logs, captured_logs, BrokerCredential, TestAuth, TestAuthBuilder, ISSUER,
};
use support::scenario::{model, Scenario};
use support::{rows, start_server_with_broker, TestServer};
use tonic::Code;

const TARGET: &str = "\"DB\".\"S\".\"T\"";

const ANALYTICS_DEVELOPER: &str =
    "runcache:scope:app:analytics:developer runcache:scope:org:analytics:developer";

/// People in Keycloak's `data-eng` group are developers in `analytics`.
fn deployment(keycloak: &Keycloak) -> TestAuthBuilder {
    TestAuth::builder(&[("analytics", false), ("research", false)])
        .upstream_registration(keycloak.registration())
        .rule("person", "groups", "data-eng", "analytics", "developer")
}

/// Keycloak's CI client is a developer in `analytics`; so would be the
/// client for another API, were its tokens for dbt State. Keycloak names
/// the client in `azp`.
fn machines(keycloak: &Keycloak) -> TestAuthBuilder {
    deployment(keycloak)
        .rule("machine", "azp", CI_CLIENT_ID, "analytics", "developer")
        .rule("machine", "azp", OTHER_CLIENT_ID, "analytics", "developer")
}

/// Start `sign_in` and sign in at Keycloak's login form as `credentials`
/// (username, password): the dbt client's loopback handler's query.
async fn sign_in_through(
    keycloak: &Keycloak,
    server: &TestServer,
    sign_in: &SignIn,
    credentials: (&str, &str),
) -> BTreeMap<String, String> {
    let browser = Browser::new().route(ISSUER, &server.broker_url());
    let query = keycloak
        .sign_in(
            &browser,
            &sign_in.authorize_url(ISSUER),
            credentials,
            LOOPBACK_REDIRECT_URI,
        )
        .await
        .loopback();
    assert_eq!(query.get("state"), Some(&sign_in.state), "{query:?}");
    query
}

/// Sign in at Keycloak through the broker as `credentials` and exchange
/// the code: the token response.
async fn signed_in(
    keycloak: &Keycloak,
    server: &TestServer,
    credentials: (&str, &str),
) -> Map<String, Value> {
    let sign_in = SignIn::new();
    let query = sign_in_through(keycloak, server, &sign_in, credentials).await;
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

/// A person signs in through Keycloak's login form and makes an RPC with
/// the `id_token` the code exchange returns; the broker maps Keycloak's
/// `groups` claim and records the person as a Principal of the realm.
#[tokio::test]
async fn a_person_signs_in_through_the_keycloak_login_form_and_makes_an_rpc() {
    let Some(keycloak) = Keycloak::from_env().await else {
        return;
    };
    let server = start_server_with_broker(&deployment(&keycloak).build()).await;

    let body = signed_in(&keycloak, &server, alice()).await;
    assert_eq!(body["scope"], ANALYTICS_DEVELOPER);
    assert!(body["refresh_token"].as_str().is_some(), "{body:?}");
    let id_token = body["id_token"].as_str().expect("an id_token");
    let token = claims(id_token);
    assert_eq!(token["scope"], ANALYTICS_DEVELOPER);
    assert_eq!(token["email"], "alice@example.test");
    assert_eq!(token["name"], "Alice Example");

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

    let principals = rows::principals(&server).await;
    assert_eq!(principals.len(), 1, "{principals:?}");
    let principal = &principals[0];
    assert_eq!(principal.issuer, keycloak.issuer());
    assert_eq!(principal.subject, keycloak.user_id(alice().0).await);
    assert_eq!(principal.kind, "person");
    assert_eq!(principal.id, token["sub"]);
}

/// Whether a sign-in refusal for Keycloak user `subject` names `reason`
/// and, when given, the claims the rules read that Keycloak left out.
fn sign_in_refused(subject: &str, reason: &str, missing_claims: Option<&str>) -> bool {
    captured_logs().lines().any(|line| {
        line.contains("sign-in refused")
            && line.contains(&format!("subject=\"{subject}\""))
            && line.contains(&format!("reason=\"{reason}\""))
            && match missing_claims {
                Some(claims) => line.contains(&format!("missing_claims=\"{claims}\"")),
                None => !line.contains("missing_claims"),
            }
    })
}

/// A person in no mapped group is refused at sign-in. Keycloak leaves the
/// `groups` claim out for a person in no group at all, and the refusal log
/// says so; a person only in other groups is simply unmapped.
#[tokio::test]
async fn a_person_in_no_mapped_group_is_refused_sign_in() {
    let Some(keycloak) = Keycloak::from_env().await else {
        return;
    };
    capture_logs();
    let server = start_server_with_broker(&deployment(&keycloak).build()).await;

    for (groups, missing_claims) in [(&[][..], Some("groups")), (&["marketing"][..], None)] {
        let person = keycloak.create_person(groups).await;
        let query = sign_in_through(&keycloak, &server, &SignIn::new(), person.credentials()).await;
        assert_eq!(query["error"], "access_denied", "{groups:?}: {query:?}");
        assert!(
            sign_in_refused(&person.id, "no_matching_rule", missing_claims),
            "{groups:?}: {}",
            captured_logs()
        );
    }
    assert!(rows::principals(&server).await.is_empty());
}

/// Renew with `token` as the dbt client does.
async fn renew(server: &TestServer, token: &str) -> reqwest::Response {
    refresh_token(&server.broker_url(), DBT_CLIENT_ID, token).await
}

/// Whether a renewal refusal for `principal` names `reason`.
fn renewal_refused(principal: &str, reason: &str) -> bool {
    captured_logs().lines().any(|line| {
        line.contains("token request refused")
            && line.contains("grant=\"refresh_token\"")
            && line.contains(principal)
            && line.contains(&format!("reason=\"{reason}\""))
    })
}

/// A signed-in person's renewal refreshes at Keycloak and returns a fresh
/// token in a rotated family; once an administrator disables them in
/// Keycloak, their next renewal is denied and their session ends.
#[tokio::test]
async fn renewal_rechecks_the_person_at_keycloak_and_a_disabled_person_is_denied() {
    let Some(keycloak) = Keycloak::from_env().await else {
        return;
    };
    capture_logs();
    let server = start_server_with_broker(&deployment(&keycloak).build()).await;
    let person = keycloak.create_person(&["data-eng"]).await;
    let body = signed_in(&keycloak, &server, person.credentials()).await;
    let principal = claims(field(&body, "id_token"))["sub"]
        .as_str()
        .unwrap()
        .to_owned();

    let response = renew(&server, field(&body, "refresh_token")).await;
    assert_eq!(response.status(), 200);
    let renewed = json(response).await;
    assert_eq!(renewed["scope"], ANALYTICS_DEVELOPER);
    assert_ne!(renewed["refresh_token"], body["refresh_token"]);
    let token = claims(field(&renewed, "id_token"));
    assert_eq!(token["sub"], principal);
    assert_eq!(token["email"], person.email.as_str());
    Scenario::connect(&server)
        .await
        .bearer(field(&renewed, "id_token"))
        .org("analytics")
        .try_submit(model(TARGET))
        .await
        .expect("the renewed token is accepted");

    keycloak.disable(&person).await;
    let response = renew(&server, field(&renewed, "refresh_token")).await;
    assert_eq!(response.status(), 400);
    assert_eq!(json(response).await["error"], "invalid_grant");
    assert!(
        renewal_refused(&principal, "upstream_refused_grant"),
        "{}",
        captured_logs()
    );
    // The session has ended: the family's newest token renews nothing.
    let response = renew(&server, field(&renewed, "refresh_token")).await;
    assert_eq!(response.status(), 400);
}

/// A person removed from the mapped group in Keycloak is denied their
/// next renewal: Keycloak's renewed evidence maps to no Organization.
#[tokio::test]
async fn a_person_removed_from_the_mapped_group_is_denied_renewal() {
    let Some(keycloak) = Keycloak::from_env().await else {
        return;
    };
    capture_logs();
    let server = start_server_with_broker(&deployment(&keycloak).build()).await;
    let person = keycloak.create_person(&["data-eng", "marketing"]).await;
    let body = signed_in(&keycloak, &server, person.credentials()).await;
    let principal = claims(field(&body, "id_token"))["sub"]
        .as_str()
        .unwrap()
        .to_owned();

    keycloak.leave_group(&person, "data-eng").await;
    let response = renew(&server, field(&body, "refresh_token")).await;
    assert_eq!(response.status(), 400);
    assert_eq!(json(response).await["error"], "invalid_grant");
    assert!(
        renewal_refused(&principal, "no_matching_rule"),
        "{}",
        captured_logs()
    );
}

/// A CI pipeline's Keycloak client signs in with `client_credentials`: the
/// broker passes its id and secret through to Keycloak, checks the access
/// token Keycloak returns and issues a machine token. Its service account
/// is the Principal.
#[tokio::test]
async fn a_ci_client_signs_in_with_client_credentials_passed_through_to_keycloak() {
    let Some(keycloak) = Keycloak::from_env().await else {
        return;
    };
    let server = start_server_with_broker(&machines(&keycloak).build()).await;

    let response = client_credentials(&server.broker_url(), CI_CLIENT_ID, CI_CLIENT_SECRET).await;
    assert_eq!(response.status(), 200);
    let body = json(response).await;
    assert_eq!(body["scope"], ANALYTICS_DEVELOPER);
    assert!(body.get("refresh_token").is_none(), "{body:?}");
    let id_token = field(&body, "id_token");
    let token = claims(id_token);
    assert_eq!(token["azp"], CI_CLIENT_ID);
    assert!(token.get("sid").is_none(), "a machine token: {token:?}");

    Scenario::connect(&server)
        .await
        .bearer(id_token)
        .org("analytics")
        .try_submit(model(TARGET))
        .await
        .expect("an authorized RPC");

    let principals = rows::principals(&server).await;
    assert_eq!(principals.len(), 1, "{principals:?}");
    let principal = &principals[0];
    assert_eq!(principal.issuer, keycloak.issuer());
    assert_eq!(
        principal.subject,
        keycloak
            .user_id(&format!("service-account-{CI_CLIENT_ID}"))
            .await
    );
    assert_eq!(principal.kind, "machine");
    assert_eq!(principal.id, token["sub"]);

    // Keycloak refuses a wrong secret, and so does the broker.
    let response = client_credentials(&server.broker_url(), CI_CLIENT_ID, "wrong-secret").await;
    assert_eq!(response.status(), 401);
    assert_eq!(json(response).await["error"], "invalid_client");
}

/// A Keycloak client whose access tokens are for another API is refused,
/// though a machine rule names it: Keycloak issues its token, but not for
/// the dbt State API audience.
#[tokio::test]
async fn a_keycloak_token_for_another_audience_is_refused() {
    let Some(keycloak) = Keycloak::from_env().await else {
        return;
    };
    capture_logs();
    let server = start_server_with_broker(&machines(&keycloak).build()).await;

    let response =
        client_credentials(&server.broker_url(), OTHER_CLIENT_ID, OTHER_CLIENT_SECRET).await;
    assert_eq!(response.status(), 400);
    assert_eq!(json(response).await["error"], "unauthorized_client");
    assert!(
        captured_logs().lines().any(|line| {
            line.contains("token request refused")
                && line.contains(&format!("client_id=\"{OTHER_CLIENT_ID}\""))
                && line.contains("reason=\"upstream_token_wrong_audience\"")
        }),
        "{}",
        captured_logs()
    );
    assert!(rows::principals(&server).await.is_empty());
}

/// The broker can sign in at Keycloak and renew there with
/// `private_key_jwt` instead of a client secret.
#[tokio::test]
async fn the_broker_authenticates_at_keycloak_with_private_key_jwt() {
    let Some(keycloak) = Keycloak::from_env().await else {
        return;
    };
    let auth = TestAuth::builder(&[("analytics", false)])
        .upstream_registration(keycloak.private_key_jwt_registration())
        .broker_credential(BrokerCredential::PrivateKeyJwt)
        .rule("person", "groups", "data-eng", "analytics", "developer")
        .build();
    let server = start_server_with_broker(&auth).await;

    let body = signed_in(&keycloak, &server, alice()).await;
    assert_eq!(body["scope"], ANALYTICS_DEVELOPER);
    let response = renew(&server, field(&body, "refresh_token")).await;
    assert_eq!(response.status(), 200);
    assert_eq!(json(response).await["scope"], ANALYTICS_DEVELOPER);
}
