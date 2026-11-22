//! Browser sign-in: a registered client's authorize request (the dbt
//! client's or the UI's, `clients`), and the Upstream IdP's callback.
//!
//! Both pinned clients open the browser at their auth URL (the broker's
//! issuer, which hosted dbt State also uses as its authorization endpoint)
//! with `response_type=code`, `client_id`, `redirect_uri`
//! `http://127.0.0.1:29525/handler`, `scope=runcache:scope:orgs`, a random
//! `state`, and a PKCE S256 `code_challenge`; they then check that the
//! loopback redirect echoes their `state`, and show its `error` and
//! `error_description` when it carries one (`dbt-fusion@9977b6c`
//! `crates/dbt-run-cache/src/auth/browser_flow.rs` `build_authorize_url`,
//! `handle_redirect`; `dbt-state==2.62.6` `dbt_state/auth/sso.py`
//! `REDIRECT_URI`, `_login`, authlib's `create_authorization_url`, and
//! `dbt_state/auth/sso_server.py` `do_GET`).
//!
//! The UI is a standard OIDC relying party: its authorize request also has
//! `openid` in its `scope` and a `nonce` (OIDC Core §3.1.2.1), which its
//! code is bound to and its `id_token` echoes.
//!
//! 1. `GET /` checks the client id and the exact redirect URI (a mismatch
//!    gets an error page, never a redirect: RFC 6749 §4.1.2.1), requires
//!    PKCE S256 (and, for the UI, `openid` and a nonce), records a pending
//!    login (capped per client IP), and sends the browser to the Upstream
//!    IdP as an OIDC relying party with its own `state`, `nonce` and PKCE
//!    challenge, requesting `openid` and offline access.
//! 2. `GET /callback` takes the pending login named by the IdP's `state`
//!    (once), exchanges the IdP's code with the broker's own credential,
//!    checks the ID token (signature, issuer, audience, expiry, nonce),
//!    identifies the Principal by the IdP issuer and the configured subject
//!    claim, applies the person mapping rules only, and creates a person
//!    session holding the IdP's refresh token, encrypted. It then sends the
//!    browser to the client's redirect URI with a single-use authorization
//!    code valid for 60 s.
//!
//! A refusal after the redirect URI is known goes back to the client as an
//! OAuth error with a fixed description, and is logged at `warn` with
//! the reason; nothing secret is logged.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use axum::extract::{ConnectInfo, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use dbt_state_auth::{PrincipalId, PrincipalKind};
use uuid::Uuid;

use crate::clients::Client;
use crate::secret;
use crate::store::{NewCode, NewSession, PendingLogin, Principal};
use crate::upstream::{AuthorizeRequest, UpstreamError};
use crate::Broker;

/// The dbt client's only redirect URI: its loopback handler on port 29525
/// (Fusion `browser_flow.rs` `LOOPBACK_PORT`; Python `sso_server.py`
/// `LOCAL_OAUTH_PORT`).
pub(crate) const DBT_CLIENT_REDIRECT_URI: &str = "http://127.0.0.1:29525/handler";
/// How long a browser sign-in may take (both clients wait 300 s).
const PENDING_LOGIN_LIFETIME: Duration = Duration::from_secs(10 * 60);
/// How long an authorization code is valid.
pub(crate) const CODE_LIFETIME: Duration = Duration::from_secs(60);
/// The cap on every person session.
pub(crate) const SESSION_CAP: Duration = Duration::from_secs(12 * 60 * 60);

/// What a sign-in refusal log line says.
#[derive(Default)]
struct Refusal<'a> {
    step: &'a str,
    reason: &'a str,
    client_id: Option<&'a str>,
    principal: Option<&'a PrincipalId>,
    subject: Option<&'a str>,
    upstream: Option<&'a str>,
    missing_claims: Option<&'a str>,
}

impl Refusal<'_> {
    fn log(&self) {
        tracing::warn!(
            step = self.step,
            reason = self.reason,
            principal = self.principal.map(PrincipalId::as_str),
            subject = self.subject,
            client_id = self.client_id,
            upstream = self.upstream,
            missing_claims = self.missing_claims,
            "sign-in refused"
        );
    }
}

/// An error page: a fixed message, shown when the browser cannot be sent
/// back to the dbt client.
fn page(status: StatusCode, message: &'static str) -> Response {
    let body = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>dbt State sign-in</title>\
         </head><body><h1>Sign-in failed</h1><p>{message}</p></body></html>"
    );
    let mut response = (status, body).into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'"),
    );
    browser_headers(response)
}

/// A redirect of the browser to `location`.
fn redirect(location: &str) -> Response {
    match HeaderValue::from_str(location) {
        Ok(location) => {
            browser_headers((StatusCode::FOUND, [(header::LOCATION, location)]).into_response())
        }
        Err(_) => page(StatusCode::BAD_GATEWAY, "The sign-in could not continue."),
    }
}

/// Headers on every browser response: never cached, never framed, and no
/// URL (which can carry a code) leaked as a referrer.
fn browser_headers(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// The longest `nonce` accepted from the UI.
const MAX_NONCE_LENGTH: usize = 512;

/// The client's redirect URI (the dbt client's loopback handler, or the
/// UI's callback), where a sign-in ends.
struct ClientRedirect<'a> {
    redirect_uri: &'a str,
    state: Option<&'a str>,
}

/// An OAuth error sent back to the dbt client (RFC 6749 §4.1.2.1).
#[derive(Debug, Clone, Copy)]
enum SignInError {
    /// The request lacks PKCE S256, or is malformed.
    InvalidRequest,
    /// The UI's request lacks a `nonce`.
    MissingNonce,
    /// The UI's request lacks the `openid` scope.
    InvalidScope,
    UnsupportedResponseType,
    /// The broker has no Upstream IdP registration for browser sign-in.
    UnauthorizedClient,
    /// The Upstream IdP did not sign the person in, or its answer cannot be
    /// trusted.
    UpstreamDenied,
    /// No person mapping rule grants the person an Organization.
    NotMapped,
    /// Too many sign-ins in progress from the client's address.
    TooManyPending,
    /// The Upstream IdP or the database is unavailable.
    TemporarilyUnavailable,
    /// The broker's own setup at the Upstream IdP is wrong.
    ServerError,
}

impl SignInError {
    fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest | Self::MissingNonce => "invalid_request",
            Self::InvalidScope => "invalid_scope",
            Self::UnsupportedResponseType => "unsupported_response_type",
            Self::UnauthorizedClient => "unauthorized_client",
            Self::UpstreamDenied | Self::NotMapped => "access_denied",
            Self::TooManyPending | Self::TemporarilyUnavailable => "temporarily_unavailable",
            Self::ServerError => "server_error",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::InvalidRequest => {
                "the sign-in request is malformed; PKCE with code_challenge_method S256 is required"
            }
            Self::MissingNonce => "a nonce is required",
            Self::InvalidScope => "the openid scope is required",
            Self::UnsupportedResponseType => "only response_type=code is supported",
            Self::UnauthorizedClient => "browser sign-in is not configured on this dbt State",
            Self::UpstreamDenied => "the identity provider did not sign you in",
            Self::NotMapped => {
                "you are not in any group mapped to dbt State; ask your administrator for access"
            }
            Self::TooManyPending => {
                "too many sign-ins are in progress from your address; try later"
            }
            Self::TemporarilyUnavailable => "sign-in is unavailable; try again later",
            Self::ServerError => "dbt State's sign-in is misconfigured; tell your administrator",
        }
    }
}

impl ClientRedirect<'_> {
    fn to(&self, params: &[(&str, &str)]) -> Response {
        let mut query = form_urlencoded::Serializer::new(String::new());
        for (key, value) in params {
            query.append_pair(key, value);
        }
        if let Some(state) = self.state {
            query.append_pair("state", state);
        }
        redirect(&format!("{}?{}", self.redirect_uri, query.finish()))
    }

    fn error(&self, refusal: Refusal, error: SignInError) -> Response {
        refusal.log();
        self.to(&[
            ("error", error.code()),
            ("error_description", error.description()),
        ])
    }
}

/// The query of `uri`; `None` when a parameter is repeated (RFC 6749
/// §3.1).
fn parse_query(uri: &Uri) -> Option<BTreeMap<String, String>> {
    let mut query = BTreeMap::new();
    for (key, value) in form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()) {
        if query.insert(key.into_owned(), value.into_owned()).is_some() {
            return None;
        }
    }
    Some(query)
}

/// Whether `challenge` is a PKCE code challenge: 43 to 128 unreserved
/// characters (RFC 7636 §4.2; an S256 challenge is 43).
fn is_code_challenge(challenge: &str) -> bool {
    (43..=128).contains(&challenge.len())
        && challenge
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
}

fn at(time: std::time::SystemTime) -> DateTime<Utc> {
    DateTime::<Utc>::from(time)
}

/// The client's address: the last entry of the configured proxy header,
/// else the peer.
fn client_ip(broker: &Broker, headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
    broker
        .0
        .client_ip_header
        .as_deref()
        .and_then(|name| headers.get(name))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.rsplit(',').next())
        .and_then(|last| last.trim().parse().ok())
        .unwrap_or(peer.ip())
}

/// Whether `nonce` is an acceptable OIDC nonce: 1 to [`MAX_NONCE_LENGTH`]
/// visible ASCII characters or spaces.
fn is_nonce(nonce: &str) -> bool {
    (1..=MAX_NONCE_LENGTH).contains(&nonce.len())
        && nonce.bytes().all(|b| (0x20..=0x7e).contains(&b))
}

/// `GET /`: a registered client's authorize request.
pub(crate) async fn authorize(
    State(broker): State<Broker>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let step = "authorize";
    let Some(query) = parse_query(&uri) else {
        Refusal {
            step,
            reason: "repeated_parameter",
            ..Refusal::default()
        }
        .log();
        return page(StatusCode::BAD_REQUEST, "The sign-in request is malformed.");
    };
    let get = |key: &str| query.get(key).map(String::as_str);
    let client_id = get("client_id");
    let Some(registration) = client_id.and_then(|id| broker.registration(id)) else {
        Refusal {
            step,
            reason: "unknown_client",
            client_id,
            ..Refusal::default()
        }
        .log();
        return page(
            StatusCode::BAD_REQUEST,
            "This client is not registered with dbt State.",
        );
    };
    if get("redirect_uri") != Some(registration.redirect_uri) {
        Refusal {
            step,
            reason: "redirect_uri_mismatch",
            client_id,
            ..Refusal::default()
        }
        .log();
        return page(
            StatusCode::BAD_REQUEST,
            "The redirect URI is not registered for this client.",
        );
    }
    let client = ClientRedirect {
        redirect_uri: registration.redirect_uri,
        state: get("state"),
    };
    let refusal = |reason| Refusal {
        step,
        reason,
        client_id,
        ..Refusal::default()
    };
    if broker.0.upstream.login().is_none() {
        return client.error(
            refusal("sign_in_not_configured"),
            SignInError::UnauthorizedClient,
        );
    }
    if get("response_type") != Some("code") {
        return client.error(
            refusal("unsupported_response_type"),
            SignInError::UnsupportedResponseType,
        );
    }
    let Some(code_challenge) = get("code_challenge").filter(|challenge| {
        get("code_challenge_method") == Some("S256") && is_code_challenge(challenge)
    }) else {
        return client.error(refusal("pkce_s256_required"), SignInError::InvalidRequest);
    };
    let client_nonce = match registration.client {
        Client::Dbt => None,
        Client::Ui => {
            if !get("scope").is_some_and(|scope| scope.split(' ').any(|s| s == "openid")) {
                return client.error(refusal("openid_scope_required"), SignInError::InvalidScope);
            }
            let Some(nonce) = get("nonce").filter(|nonce| is_nonce(nonce)) else {
                return client.error(refusal("nonce_required"), SignInError::MissingNonce);
            };
            Some(nonce.to_owned())
        }
    };

    let state = secret::random();
    let nonce = secret::random();
    let verifier = secret::random();
    let upstream_url = match broker
        .0
        .upstream
        .authorization_url(&AuthorizeRequest {
            redirect_uri: &broker.callback_url(),
            state: &state,
            nonce: &nonce,
            code_challenge: &secret::s256_challenge(&verifier),
        })
        .await
    {
        Ok(url) => url,
        Err(error) => {
            let detail = upstream_detail(&error);
            return client.error(
                Refusal {
                    upstream: Some(&detail),
                    ..refusal("upstream_unavailable")
                },
                SignInError::TemporarilyUnavailable,
            );
        }
    };
    let now = broker.0.clock.now();
    let login = PendingLogin {
        state_hash: secret::hash(&state),
        client_id: registration.client_id.to_owned(),
        redirect_uri: registration.redirect_uri.to_owned(),
        client_state: client.state.map(str::to_owned),
        code_challenge: code_challenge.to_owned(),
        client_nonce,
        upstream_nonce: nonce,
        upstream_code_verifier: verifier,
        client_ip: client_ip(&broker, &headers, peer).to_string(),
        created_at: at(now),
        expires_at: at(now + PENDING_LOGIN_LIFETIME),
    };
    match broker
        .0
        .store
        .begin_login(&login, broker.0.pending_logins_per_ip)
        .await
    {
        Ok(true) => redirect(&upstream_url),
        Ok(false) => client.error(
            refusal("too_many_pending_logins"),
            SignInError::TooManyPending,
        ),
        Err(error) => {
            tracing::error!(%error, "cannot record a pending login");
            client.error(
                refusal("database_unavailable"),
                SignInError::TemporarilyUnavailable,
            )
        }
    }
}

fn upstream_detail(error: &UpstreamError) -> String {
    match error {
        UpstreamError::RejectedClient { error } => {
            format!(
                "rejected client: {}",
                error.as_deref().unwrap_or("HTTP 401")
            )
        }
        UpstreamError::Refused { status, error } => {
            error.clone().unwrap_or_else(|| format!("HTTP {status}"))
        }
        UpstreamError::Unavailable(detail) => detail.clone(),
    }
}

/// `GET /callback`: the Upstream IdP sends the browser back here.
pub(crate) async fn callback(State(broker): State<Broker>, uri: Uri) -> Response {
    let step = "callback";
    let expired_page = || {
        page(
            StatusCode::BAD_REQUEST,
            "This sign-in has expired or was already completed. Start it again from dbt.",
        )
    };
    let Some(query) = parse_query(&uri) else {
        Refusal {
            step,
            reason: "repeated_parameter",
            ..Refusal::default()
        }
        .log();
        return page(
            StatusCode::BAD_REQUEST,
            "The sign-in response is malformed.",
        );
    };
    let get = |key: &str| query.get(key).map(String::as_str);
    let Some(state) = get("state") else {
        Refusal {
            step,
            reason: "missing_state",
            ..Refusal::default()
        }
        .log();
        return expired_page();
    };
    let login = match broker.0.store.take_login(&secret::hash(state)).await {
        Ok(Some(login)) => login,
        Ok(None) => {
            Refusal {
                step,
                reason: "unknown_login_state",
                ..Refusal::default()
            }
            .log();
            return expired_page();
        }
        Err(error) => {
            tracing::error!(%error, "cannot read a pending login");
            return page(
                StatusCode::SERVICE_UNAVAILABLE,
                "Sign-in is unavailable. Try again later.",
            );
        }
    };
    let now = broker.0.clock.now();
    let client_id = Some(login.client_id.as_str());
    if login.expires_at <= at(now) {
        Refusal {
            step,
            reason: "login_expired",
            client_id,
            ..Refusal::default()
        }
        .log();
        return expired_page();
    }
    let client = ClientRedirect {
        redirect_uri: &login.redirect_uri,
        state: login.client_state.as_deref(),
    };
    let refusal = |reason| Refusal {
        step,
        reason,
        client_id,
        ..Refusal::default()
    };

    if let Some(error) = get("error") {
        return client.error(
            Refusal {
                upstream: Some(error),
                ..refusal("upstream_error")
            },
            SignInError::UpstreamDenied,
        );
    }
    let Some(code) = get("code") else {
        return client.error(
            refusal("upstream_missing_code"),
            SignInError::UpstreamDenied,
        );
    };
    let upstream = &broker.0.upstream;
    let tokens = match upstream
        .exchange_code(
            code,
            &broker.callback_url(),
            &login.upstream_code_verifier,
            now,
        )
        .await
    {
        Ok(tokens) => tokens,
        Err(error) => {
            let detail = upstream_detail(&error);
            let (reason, sign_in_error) = match error {
                UpstreamError::RejectedClient { .. } => {
                    ("upstream_rejected_client", SignInError::ServerError)
                }
                UpstreamError::Refused { .. } => {
                    ("upstream_refused_grant", SignInError::UpstreamDenied)
                }
                UpstreamError::Unavailable(_) => {
                    ("upstream_unavailable", SignInError::TemporarilyUnavailable)
                }
            };
            return client.error(
                Refusal {
                    upstream: Some(&detail),
                    ..refusal(reason)
                },
                sign_in_error,
            );
        }
    };
    let Some(id_token) = tokens.id_token else {
        return client.error(
            refusal("upstream_missing_id_token"),
            SignInError::UpstreamDenied,
        );
    };
    let claims = match upstream
        .verify_id_token(&id_token, &login.upstream_nonce, now)
        .await
    {
        Ok(Ok(claims)) => claims,
        Ok(Err(error)) => {
            return client.error(refusal(error.reason()), SignInError::UpstreamDenied)
        }
        Err(error) => {
            let detail = upstream_detail(&error);
            return client.error(
                Refusal {
                    upstream: Some(&detail),
                    ..refusal("upstream_unavailable")
                },
                SignInError::TemporarilyUnavailable,
            );
        }
    };
    let Some(subject) = claims
        .string(upstream.subject_claim())
        .filter(|subject| !subject.is_empty())
    else {
        return client.error(
            Refusal {
                missing_claims: Some(upstream.subject_claim()),
                ..refusal("upstream_token_missing_subject")
            },
            SignInError::UpstreamDenied,
        );
    };
    let principal = PrincipalId::upstream(upstream.issuer(), subject);
    let rules = &broker.0.rules;
    let scope = rules.scope(PrincipalKind::Person, &claims, &broker.0.organizations);
    if scope.is_empty() {
        let missing: Vec<&str> = rules
            .claims_read(PrincipalKind::Person)
            .into_iter()
            .filter(|claim| !claims.contains(claim))
            .collect();
        let missing = missing.join(",");
        return client.error(
            Refusal {
                principal: Some(&principal),
                subject: Some(subject),
                missing_claims: (!missing.is_empty()).then_some(missing.as_str()),
                ..refusal("no_matching_rule")
            },
            SignInError::NotMapped,
        );
    }
    // Every renewal goes back to the Upstream IdP (#221), so a session
    // needs its refresh token.
    let Some(upstream_refresh_token) = tokens.refresh_token.filter(|token| !token.is_empty())
    else {
        return client.error(
            Refusal {
                principal: Some(&principal),
                subject: Some(subject),
                ..refusal("upstream_missing_refresh_token")
            },
            SignInError::ServerError,
        );
    };

    let session = Uuid::new_v4();
    let key = upstream
        .login()
        .expect("browser sign-in is configured")
        .refresh_token_key();
    let code = secret::random();
    let scope_text = scope.to_string();
    let created = at(now);
    let started = broker
        .0
        .store
        .start_session(
            &Principal {
                id: &principal,
                issuer: upstream.issuer(),
                subject,
                kind: PrincipalKind::Person,
                email: claims.string("email"),
            },
            &NewSession {
                id: session,
                principal_id: principal.as_str().to_owned(),
                upstream_issuer: upstream.issuer().to_owned(),
                upstream_subject: subject.to_owned(),
                client_id: login.client_id.clone(),
                scope: scope_text.clone(),
                email: claims.string("email").map(str::to_owned),
                name: claims.string("name").map(str::to_owned),
                sealed_upstream_refresh_token: key
                    .seal(upstream_refresh_token.as_bytes(), session.as_bytes()),
                created_at: created,
                expires_at: at(now + SESSION_CAP),
            },
            &NewCode {
                code_hash: secret::hash(&code),
                client_id: login.client_id.clone(),
                redirect_uri: login.redirect_uri.clone(),
                code_challenge: login.code_challenge.clone(),
                nonce: login.client_nonce.clone(),
                expires_at: at(now + CODE_LIFETIME),
            },
        )
        .await;
    if let Err(error) = started {
        tracing::error!(%error, "cannot record a person session");
        return client.error(
            refusal("database_unavailable"),
            SignInError::TemporarilyUnavailable,
        );
    }
    tracing::info!(
        principal = principal.as_str(),
        kind = PrincipalKind::Person.as_str(),
        subject,
        client_id = login.client_id.as_str(),
        session = %session,
        scope = scope_text.as_str(),
        "sign-in completed"
    );
    client.to(&[("code", &code)])
}
