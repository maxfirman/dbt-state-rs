//! The UI's own client registration at the Token broker (`[clients.ui]`):
//! a confidential OIDC client with its own client id and exactly one
//! callback, which authenticates at the token endpoint with
//! `private_key_jwt` and signs people in with the standard code flow
//! (`openid`, a nonce echoed in the `id_token`, PKCE S256, an
//! `access_token`). A UI login is a person session like a dbt client's, but
//! its tokens carry `aud` = the UI client id, so no gRPC RPC accepts them.
//!
//! The UI is driven over HTTP exactly as an OIDC relying party drives the
//! broker (`support/ui_client.rs`); the browser (`support/browser.rs`)
//! follows the redirects through the fake Upstream IdP to the UI's callback.

#[path = "support.rs"]
mod support;

use std::collections::BTreeSet;

use serde_json::{json, Map, Value};
use support::broker_client::{
    authorization_code, refresh_token, SignIn, DBT_CLIENT_ID, LOOPBACK_REDIRECT_URI,
};
use support::broker_client::{claims, json};
use support::browser::{Browser, Landing};
use support::fake_idp::{FakeIdp, UpstreamPerson};
use support::oidc::{capture_logs, captured_logs, TestAuth, TestAuthBuilder, ISSUER};
use support::rpcs::call_every_rpc;
use support::scenario::Scenario;
use support::ui_client::{
    self, assertion, token_request, Assertion, UiSignIn, UI_CLIENT_ID, UI_REDIRECT_URI,
};
use support::{start_server_with_broker, TestServer};
use tonic::Code;

/// Two active Organizations and a disabled one.
const ORGANIZATIONS: &[(&str, bool)] =
    &[("analytics", false), ("research", false), ("finance", true)];

/// The broker's token endpoint, as the UI addresses its assertions.
fn token_endpoint() -> String {
    format!("{ISSUER}/token")
}

/// The deployment: people in `data-eng` are developers in `analytics`, and
/// the UI is registered.
fn deployment(idp: &FakeIdp) -> TestAuthBuilder {
    TestAuth::builder(ORGANIZATIONS)
        .upstream(idp)
        .ui_client()
        .rule("person", "groups", "data-eng", "analytics", "developer")
}

fn alice() -> UpstreamPerson {
    UpstreamPerson::new("alice-sub").claim("groups", json!(["data-eng"]))
}

/// Start `sign_in` in a browser and follow it to the UI's callback.
async fn sign_in_through(server: &TestServer, sign_in: &UiSignIn) -> Landing {
    Browser::new()
        .route(ISSUER, &server.broker_url())
        .visit(&sign_in.authorize_url(ISSUER), UI_REDIRECT_URI)
        .await
}

/// A complete UI sign-in: the code its callback receives.
async fn code_for(server: &TestServer, sign_in: &UiSignIn) -> String {
    let query = sign_in_through(server, sign_in).await.loopback();
    assert_eq!(query.get("state"), Some(&sign_in.state), "{query:?}");
    query
        .get("code")
        .cloned()
        .unwrap_or_else(|| panic!("no code in {query:?}"))
}

/// Whether a `warn` refusal line `message` names `reason` (and `extra`).
fn refusal_logged(message: &str, reason: &str, extra: &[&str]) -> bool {
    captured_logs().lines().any(|line| {
        line.contains("WARN")
            && line.contains(message)
            && line.contains(&format!("reason=\"{reason}\""))
            && extra.iter().all(|needle| line.contains(needle))
    })
}

/// The token endpoint refuses `response` as the OAuth error `error`, with
/// no token, logged with `reason`.
async fn assert_token_refused(response: reqwest::Response, status: u16, error: &str, reason: &str) {
    assert_eq!(response.status(), status, "{reason}");
    assert_eq!(response.headers()["cache-control"], "no-store", "{reason}");
    let body = json(response).await;
    assert_eq!(body["error"], error, "{reason}: {body:?}");
    for token in ["id_token", "access_token", "refresh_token"] {
        assert!(!body.contains_key(token), "{reason}: {body:?}");
    }
    assert!(
        refusal_logged("token request refused", reason, &[]),
        "no refusal log for {reason} in\n{}",
        captured_logs()
    );
}

/// A complete UI sign-in and code exchange: the token response.
async fn ui_login(server: &TestServer, sign_in: &UiSignIn) -> Map<String, Value> {
    let code = code_for(server, sign_in).await;
    let response = ui_client::exchange(
        &server.broker_url(),
        sign_in,
        &code,
        Some(&assertion(&token_endpoint())),
    )
    .await;
    assert_eq!(response.status(), 200);
    json(response).await
}

/// A person signs in to the UI through the broker: the UI's callback gets a
/// code, which the UI exchanges with a `private_key_jwt` assertion for an
/// `id_token` (echoing its nonce), an `access_token` and a refresh token,
/// all for the UI's own audience.
#[tokio::test]
async fn a_person_signs_in_to_the_ui_as_its_own_client() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;

    let sign_in = UiSignIn::new();
    let code = code_for(&server, &sign_in).await;
    let response = ui_client::exchange(
        &server.broker_url(),
        &sign_in,
        &code,
        Some(&assertion(&token_endpoint())),
    )
    .await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body = json(response).await;
    let fields: BTreeSet<&str> = body.keys().map(String::as_str).collect();
    assert_eq!(
        fields,
        BTreeSet::from([
            "access_token",
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

    let id_token = claims(body["id_token"].as_str().unwrap());
    assert_eq!(id_token["iss"], ISSUER);
    assert_eq!(id_token["aud"], UI_CLIENT_ID);
    assert_eq!(id_token["azp"], UI_CLIENT_ID);
    assert_eq!(id_token["nonce"], sign_in.nonce.as_deref().unwrap());
    assert_eq!(id_token["scope"], scope);
    assert_eq!(id_token["email"], "alice-sub@example.test");
    assert_eq!(id_token["name"], "Person alice-sub");
    assert_eq!(
        id_token["exp"].as_u64().unwrap() - id_token["iat"].as_u64().unwrap(),
        60 * 60
    );
    let session = id_token["sid"].as_str().expect("a person session");
    let access_token = claims(body["access_token"].as_str().unwrap());
    assert_eq!(access_token["iss"], ISSUER);
    assert_eq!(access_token["aud"], UI_CLIENT_ID);
    assert_eq!(access_token["sub"], id_token["sub"]);
    assert_eq!(access_token["sid"], session);
    assert_eq!(access_token["scope"], scope);
    assert!(access_token.get("nonce").is_none(), "{access_token:?}");

    let logs = captured_logs();
    let issued = logs
        .lines()
        .find(|line| line.contains("token issued") && line.contains(session))
        .unwrap_or_else(|| panic!("no issue log for session {session} in\n{logs}"));
    for needle in [
        "grant=\"authorization_code\"",
        "kind=\"person\"",
        &format!("client_id=\"{UI_CLIENT_ID}\""),
    ] {
        assert!(issued.contains(needle), "no {needle} in {issued}");
    }
    for token in ["id_token", "access_token", "refresh_token"] {
        assert!(
            !logs.contains(body[token].as_str().unwrap()),
            "{token} logged"
        );
    }
}

/// Neither token of a UI login calls any gRPC RPC, even for an
/// Organization the person belongs to: the gRPC server accepts only the
/// dbt State API audience, and UI tokens carry the UI's client id.
#[tokio::test]
async fn ui_tokens_are_refused_by_every_rpc() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let body = ui_login(&server, &UiSignIn::new()).await;
    let sc = Scenario::connect(&server).await.org("analytics");

    for token in ["id_token", "access_token"] {
        let bearer = body[token].as_str().unwrap();
        for (rpc, result) in call_every_rpc(&sc.bearer(bearer)).await {
            let status = result.expect_err(rpc);
            assert_eq!(
                status.code(),
                Code::Unauthenticated,
                "{token} {rpc}: {status:?}"
            );
            assert_eq!(
                status.message(),
                "bearer token has the wrong audience",
                "{token} {rpc}"
            );
        }
    }
}

/// The UI's authorize request must name its one registered callback (any
/// other gets an error page, never a redirect), and be a standard OIDC
/// request: `openid` in its scope, a nonce and PKCE S256. Those refusals go
/// back to the UI's callback, before the Upstream IdP is involved.
#[tokio::test]
async fn ui_sign_in_needs_its_callback_openid_a_nonce_and_pkce() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;

    for (client_id, other) in [
        (
            UI_CLIENT_ID,
            "https://console.dbt-state.test/auth/callback/",
        ),
        (UI_CLIENT_ID, "https://console.dbt-state.test/auth/other"),
        (UI_CLIENT_ID, "https://attacker.example/auth/callback"),
        (UI_CLIENT_ID, LOOPBACK_REDIRECT_URI),
        // Each client has only its own callback.
        (DBT_CLIENT_ID, UI_REDIRECT_URI),
    ] {
        let sign_in = UiSignIn::new().client_id(client_id).redirect_uri(other);
        let landing = Browser::new()
            .route(ISSUER, &server.broker_url())
            .visit(&sign_in.authorize_url(ISSUER), other)
            .await;
        let Landing::Page { status, body } = landing else {
            panic!("{client_id} {other}: redirected: {landing:?}");
        };
        assert_eq!(status, 400, "{other}");
        assert!(body.contains("redirect URI is not registered"), "{body}");
    }
    assert!(refusal_logged(
        "sign-in refused",
        "redirect_uri_mismatch",
        &[&format!("client_id=\"{UI_CLIENT_ID}\"")]
    ));

    for (sign_in, error, reason) in [
        (
            UiSignIn::new().scope("email profile"),
            "invalid_scope",
            "openid_scope_required",
        ),
        (
            UiSignIn::new().scope("openidx"),
            "invalid_scope",
            "openid_scope_required",
        ),
        (
            UiSignIn::new().without_nonce(),
            "invalid_request",
            "nonce_required",
        ),
        (
            UiSignIn::new().without_pkce(),
            "invalid_request",
            "pkce_s256_required",
        ),
    ] {
        let query = sign_in_through(&server, &sign_in).await.loopback();
        assert_eq!(query["error"], error, "{reason}: {query:?}");
        assert_eq!(query["state"], sign_in.state, "{reason}");
        assert!(!query.contains_key("code"), "{reason}");
        assert!(refusal_logged("sign-in refused", reason, &[]), "{reason}");
    }
    assert!(idp.authorize_requests().is_empty());
}

/// The UI authenticates at the token endpoint only with a `private_key_jwt`
/// assertion that its registered key signed, naming the UI as `iss` and
/// `sub` and the broker (issuer or token endpoint) as `aud`, current, valid
/// for at most five minutes, and never presented before. Anything else is
/// `invalid_client`, checked before the code is: the code still works
/// afterwards.
#[tokio::test]
async fn failed_ui_client_authentication_is_invalid_client() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let broker = server.broker_url();
    let sign_in = UiSignIn::new();
    let code = code_for(&server, &sign_in).await;
    let exchange_form = [
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", UI_REDIRECT_URI),
        ("code_verifier", sign_in.verifier.as_str()),
    ];
    let aud = token_endpoint();

    // Without an assertion, or with another kind of one.
    let with = |extra: &[(&'static str, &'static str)]| {
        let mut form = exchange_form.to_vec();
        form.extend_from_slice(extra);
        form
    };
    for (form, reason) in [
        (
            with(&[("client_id", UI_CLIENT_ID)]),
            "client_authentication_required",
        ),
        (
            with(&[
                ("client_id", UI_CLIENT_ID),
                ("client_assertion_type", "urn:example:other"),
                ("client_assertion", "x.y.z"),
            ]),
            "unsupported_client_assertion_type",
        ),
        (
            with(&[(
                "client_assertion_type",
                "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            )]),
            "missing_client_assertion",
        ),
    ] {
        let response = token_request(&broker, &form, None).await;
        assert_token_refused(response, 401, "invalid_client", reason).await;
    }
    let response = token_request(
        &broker,
        &with(&[("client_id", DBT_CLIENT_ID)]),
        Some(&assertion(&aud)),
    )
    .await;
    assert_token_refused(response, 401, "invalid_client", "client_id_mismatch").await;

    // Assertions that fail a check.
    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    };
    for (assertion, reason) in [
        ("not-a-jwt".to_owned(), "client_assertion_malformed"),
        (
            Assertion::new(&aud).alg("HS256").sign(),
            "client_assertion_malformed",
        ),
        (
            Assertion::new(&aud).without("exp").sign(),
            "client_assertion_malformed",
        ),
        (
            Assertion::new(&aud).signed_by_another_key().sign(),
            "client_assertion_bad_signature",
        ),
        (
            Assertion::new(&aud)
                .claim("iss", json!(DBT_CLIENT_ID))
                .sign(),
            "client_assertion_wrong_issuer",
        ),
        (
            Assertion::new(&aud)
                .claim("sub", json!("someone-else"))
                .sign(),
            "client_assertion_wrong_issuer",
        ),
        (
            Assertion::new("https://other-broker.example/token").sign(),
            "client_assertion_wrong_audience",
        ),
        (
            Assertion::new(&format!("{ISSUER}/")).sign(),
            "client_assertion_wrong_audience",
        ),
        // Expired more than the 60 s clock skew ago.
        (
            Assertion::new(&aud).issued(400, 300).sign(),
            "client_assertion_expired",
        ),
        // Valid for more than five minutes.
        (
            Assertion::new(&aud).issued(0, 3600).sign(),
            "client_assertion_too_long_lived",
        ),
        (
            Assertion::new(&aud)
                .without("iat")
                .claim("exp", json!(now() + 3600))
                .sign(),
            "client_assertion_too_long_lived",
        ),
        (
            Assertion::new(&aud).claim("nbf", json!(now() + 200)).sign(),
            "client_assertion_not_yet_valid",
        ),
        (
            Assertion::new(&aud).without("jti").sign(),
            "client_assertion_missing_jti",
        ),
    ] {
        let response = token_request(&broker, &exchange_form, Some(&assertion)).await;
        assert_token_refused(response, 401, "invalid_client", reason).await;
    }

    // A good assertion (here for the issuer as `aud`, which is also
    // accepted) redeems the code all those requests left alone, once.
    let good = assertion(ISSUER);
    let response = token_request(&broker, &exchange_form, Some(&good)).await;
    assert_eq!(response.status(), 200);
    let body = json(response).await;
    let response = ui_client::refresh(
        &broker,
        body["refresh_token"].as_str().unwrap(),
        Some(&good),
    )
    .await;
    assert_token_refused(response, 401, "invalid_client", "client_assertion_replayed").await;
    assert!(refusal_logged(
        "token request refused",
        "client_assertion_replayed",
        &[
            "grant=\"refresh_token\"",
            &format!("client_id=\"{UI_CLIENT_ID}\"")
        ]
    ));
}

/// The UI renews its person session at the broker as the dbt client does
/// (fresh upstream evidence, a rotated refresh token), authenticated with
/// `private_key_jwt`, and gets UI tokens again: same session and Principal,
/// no nonce (OIDC Core §12.2).
#[tokio::test]
async fn the_ui_renews_its_session_with_private_key_jwt() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let broker = server.broker_url();
    let login = ui_login(&server, &UiSignIn::new()).await;
    let first = claims(login["id_token"].as_str().unwrap());
    let refresh = login["refresh_token"].as_str().unwrap();

    let response = ui_client::refresh(&broker, refresh, Some(&assertion(&token_endpoint()))).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let body = json(response).await;
    let fields: BTreeSet<&str> = body.keys().map(String::as_str).collect();
    assert_eq!(
        fields,
        BTreeSet::from([
            "access_token",
            "expires_in",
            "id_token",
            "refresh_token",
            "scope",
            "token_type"
        ]),
        "{body:?}"
    );
    assert_ne!(body["refresh_token"], refresh, "rotated");
    let renewed = claims(body["id_token"].as_str().unwrap());
    for claim in ["iss", "sub", "aud", "azp", "sid", "scope"] {
        assert_eq!(renewed[claim], first[claim], "{claim}");
    }
    assert_eq!(renewed["aud"], UI_CLIENT_ID);
    assert!(renewed.get("nonce").is_none(), "{renewed:?}");
    assert_eq!(
        claims(body["access_token"].as_str().unwrap())["aud"],
        UI_CLIENT_ID
    );
    let session = renewed["sid"].as_str().unwrap();
    assert!(
        captured_logs()
            .lines()
            .any(|line| line.contains("token issued")
                && line.contains("grant=\"refresh_token\"")
                && line.contains(&format!("client_id=\"{UI_CLIENT_ID}\""))
                && line.contains("evidence=\"id_token\"")
                && line.contains(session)),
        "{}",
        captured_logs()
    );

    // Without an assertion the UI's client id does not authenticate.
    let next = body["refresh_token"].as_str().unwrap();
    let response = token_request(
        &broker,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", next),
            ("client_id", UI_CLIENT_ID),
        ],
        None,
    )
    .await;
    assert_token_refused(
        response,
        401,
        "invalid_client",
        "client_authentication_required",
    )
    .await;
    let response = ui_client::refresh(&broker, next, Some(&assertion(ISSUER))).await;
    assert_eq!(response.status(), 200, "the refresh token was left alone");
}

/// A UI login never yields a dbt-shaped token, and a dbt client login never
/// yields UI tokens: each code and refresh token is bound to the client it
/// was issued to. Presenting one as the other client is `invalid_grant`.
#[tokio::test]
async fn codes_and_refresh_tokens_are_bound_to_their_client() {
    capture_logs();
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let broker = server.broker_url();
    let ui_assertion = || assertion(&token_endpoint());

    // A UI code exchanged as the (public) dbt client.
    let ui_sign_in = UiSignIn::new();
    let code = code_for(&server, &ui_sign_in).await;
    let mut as_dbt = SignIn::new().redirect_uri(UI_REDIRECT_URI);
    as_dbt.verifier = ui_sign_in.verifier.clone();
    let response = authorization_code(&broker, &as_dbt, &code).await;
    assert_token_refused(response, 400, "invalid_grant", "client_mismatch").await;

    // A dbt client code exchanged by the UI.
    let dbt_sign_in = SignIn::new();
    let query = Browser::new()
        .route(ISSUER, &server.broker_url())
        .visit(&dbt_sign_in.authorize_url(ISSUER), LOOPBACK_REDIRECT_URI)
        .await
        .loopback();
    let response = token_request(
        &broker,
        &[
            ("grant_type", "authorization_code"),
            ("code", &query["code"]),
            ("redirect_uri", LOOPBACK_REDIRECT_URI),
            ("code_verifier", &dbt_sign_in.verifier),
        ],
        Some(&ui_assertion()),
    )
    .await;
    assert_token_refused(response, 400, "invalid_grant", "client_mismatch").await;

    // Refresh tokens, either way round; neither session is ended.
    let dbt_query = Browser::new()
        .route(ISSUER, &server.broker_url())
        .visit(&dbt_sign_in.authorize_url(ISSUER), LOOPBACK_REDIRECT_URI)
        .await
        .loopback();
    let dbt = json(authorization_code(&broker, &dbt_sign_in, &dbt_query["code"]).await).await;
    let dbt_refresh = dbt["refresh_token"].as_str().unwrap();
    let ui = ui_login(&server, &UiSignIn::new()).await;
    let ui_refresh = ui["refresh_token"].as_str().unwrap();

    let response = refresh_token(&broker, DBT_CLIENT_ID, ui_refresh).await;
    assert_eq!(response.status(), 400);
    assert_eq!(json(response).await["error"], "invalid_grant");
    let response = ui_client::refresh(&broker, dbt_refresh, Some(&ui_assertion())).await;
    assert_eq!(response.status(), 400);
    assert_eq!(json(response).await["error"], "invalid_grant");
    assert!(refusal_logged(
        "token request refused",
        "client_mismatch",
        &["grant=\"refresh_token\""]
    ));

    let response = ui_client::refresh(&broker, ui_refresh, Some(&ui_assertion())).await;
    assert_eq!(response.status(), 200);
    let response = refresh_token(&broker, DBT_CLIENT_ID, dbt_refresh).await;
    assert_eq!(response.status(), 200);
}

/// Each `id_token` echoes the nonce of the authorize request its code came
/// from. A code from another sign-in (as an attacker would inject one)
/// yields that sign-in's nonce, which the UI's own does not match.
#[tokio::test]
async fn the_id_token_carries_the_nonce_of_its_own_sign_in() {
    let idp = FakeIdp::start().await;
    idp.sign_in(alice());
    let server = start_server_with_broker(&deployment(&idp).build()).await;
    let mine = UiSignIn::new();
    let other = UiSignIn::new();
    let _my_code = code_for(&server, &mine).await;
    let other_login = ui_login(&server, &other).await;

    let nonce = &claims(other_login["id_token"].as_str().unwrap())["nonce"];
    assert_eq!(nonce, other.nonce.as_deref().unwrap());
    assert_ne!(
        nonce,
        mine.nonce.as_deref().unwrap(),
        "a nonce mismatch the UI refuses"
    );
}
