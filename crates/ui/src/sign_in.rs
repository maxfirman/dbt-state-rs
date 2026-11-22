//! `oidc` mode: people sign in to the console through the Token broker, as
//! its registered confidential client (`[clients.ui]`), and see only their
//! Organizations.
//!
//! **Sign-in.** A request without a session is sent to the broker's
//! authorize endpoint with a fresh `state`, `nonce` and PKCE S256
//! challenge. Those wait in the pending-sign-in cookie for at most
//! [`PENDING_LIFETIME`]. The broker returns the browser to the callback,
//! which checks `state`, exchanges the code at the token endpoint with a
//! fresh `private_key_jwt` assertion, and verifies the `id_token`: the
//! broker's signature, its issuer, `aud` the UI's client id, expiry, and
//! the nonce. The verified claims and the refresh token become the session.
//!
//! **Cookies.** Both cookies are encrypted and authenticated with the
//! deployment's cookie key (AES-256-GCM, the cookie crate's private jar),
//! and set `__Host-`, `Secure`, `HttpOnly`, `SameSite=Lax`, `Path=/`.
//! Page scripts cannot read them, and no other host or path receives
//! them. `SameSite=Lax` (not `Strict`) is what lets the pending-sign-in
//! cookie come back on the broker's cross-site redirect to the callback,
//! and the session cookie on the redirect that follows it and on links
//! from other sites. Lax never sends them on cross-site POSTs. The only
//! state-changing request, the Organization switch, is a POST that also
//! needs the session's CSRF token, behind Topcoat's default origin check.
//! The session lives in its cookie, so the console stays read-only on
//! Postgres and every replica that shares the cookie key serves it.
//!
//! **Each request.** Before a page renders, the session's broker session
//! must be active ([`SessionCheck`], each status cached for at most
//! `SESSION_STATUS_MAX_AGE`, 30 s): a revoked or capped session ends the
//! console's. When less than [`RENEW_BEFORE`] of the person token's
//! lifetime is left, the console renews at the broker (the refresh grant,
//! with a fresh assertion). The broker re-checks the person at the
//! Upstream IdP, so offboarding there ends the console's session within one
//! person token lifetime. A failed renewal ends it too. A session that
//! ends sends the browser to sign in again. The Organizations shown are the
//! token's active ones that this console's config also has active, so an
//! Organization disabled in config is refused once the console restarts
//! with that config.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use dbt_state_auth::{
    AuthConfig, CLIENT_ASSERTION_TYPE, ClientAssertionKey, OrganizationId, Organizations, Role,
    Scope, TokenError, TokenVerifier, VerifiedToken,
};
use dbt_state_broker::{
    Clock, KeyLocation, RemoteKeys, SESSION_STATUS_MAX_AGE, SessionCheck, SessionCheckError,
};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use topcoat::context::Cx;
use topcoat::cookie::{Cookie, Cookies, Key, SameSite, cookies, time};
use topcoat::router::error::{SeeOther, forbidden, see_other, service_unavailable};
use topcoat::router::request::{method, uri};

use crate::settings::OidcSettings;

/// How long a sign-in may take from the authorize redirect to the callback.
pub(crate) const PENDING_LIFETIME: Duration = Duration::from_secs(10 * 60);
/// Renew when less than this much of the person token's lifetime is left.
pub(crate) const RENEW_BEFORE: Duration = Duration::from_secs(5 * 60);
/// The session cookie's `Max-Age`: the Token broker's cap on a person
/// session, which its session check also enforces.
const SESSION_COOKIE_MAX_AGE: Duration = Duration::from_secs(12 * 60 * 60);

const SESSION_COOKIE: &str = "dbt-state-ui-session";
const PENDING_COOKIE: &str = "dbt-state-ui-sign-in";
/// The scopes the console asks the broker for.
const SCOPE: &str = "openid email profile";
/// The longest local path the console returns to after sign-in.
const MAX_RETURN_PATH: usize = 2048;

/// A sign-in between the authorize redirect and the callback.
#[derive(Serialize, Deserialize)]
struct PendingSignIn {
    state: String,
    nonce: String,
    verifier: String,
    /// The local path to return to.
    next: String,
    /// Unix seconds.
    expires_at: u64,
}

/// A signed-in person, as the session cookie holds them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Session {
    /// The Principal (token `sub`).
    principal: String,
    /// The broker session (`sid`).
    sid: String,
    email: Option<String>,
    name: Option<String>,
    /// The person token's `scope` claim.
    scope: String,
    /// The person token's `exp`, Unix seconds.
    expires_at: u64,
    refresh_token: String,
    /// The selected Organization.
    organization: Option<String>,
    /// The token the console's forms carry.
    csrf: String,
}

/// The person a request is for: the Organization it shows and the ones the
/// person may switch to.
pub(crate) struct Viewer {
    pub(crate) organization: String,
    /// The Organizations shown, with the person's Role in each, by id.
    pub(crate) organizations: Vec<(String, Role)>,
    pub(crate) csrf: String,
    pub(crate) email: Option<String>,
    pub(crate) name: Option<String>,
    session: Session,
}

/// Why a sign-in or a renewal at the broker failed: the reason logged.
#[derive(Debug, Clone, Copy)]
enum BrokerFailure {
    Unreachable,
    Refused,
    Malformed,
    Token(TokenError),
    WrongClient,
    WrongNonce,
    NotAPerson,
    NoRefreshToken,
    OtherPerson,
}

impl BrokerFailure {
    fn reason(self) -> &'static str {
        match self {
            Self::Unreachable => "broker_unreachable",
            Self::Refused => "broker_refused",
            Self::Malformed => "broker_response_malformed",
            Self::Token(error) => error.reason(),
            Self::WrongClient => "wrong_client",
            Self::WrongNonce => "wrong_nonce",
            Self::NotAPerson => "not_a_person_token",
            Self::NoRefreshToken => "no_refresh_token",
            Self::OtherPerson => "other_person",
        }
    }
}

/// A token endpoint response the console uses.
#[derive(Deserialize)]
struct TokenResponse {
    id_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
}

/// The broker's OAuth error.
#[derive(Deserialize)]
struct OAuthError {
    #[serde(default)]
    error: String,
}

/// What a callback request ended with, when not a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallbackError {
    /// No pending sign-in matches (expired, already completed, or another
    /// browser's).
    Expired,
    /// The broker denied the sign-in (`access_denied`).
    Denied,
    /// The broker is unavailable.
    Unavailable,
    /// The person has no Organization this console shows.
    NoOrganization,
    /// Anything else.
    Failed,
}

/// See the module docs.
pub(crate) struct Oidc {
    /// The broker's issuer, without a trailing `/`: its root is the
    /// authorize endpoint.
    issuer: String,
    client_id: String,
    redirect_uri: String,
    token_endpoint: String,
    /// The `aud` of the console's client assertions: the broker's token
    /// endpoint at its issuer.
    assertion_audience: String,
    assertion_key: ClientAssertionKey,
    cookie_key: Key,
    organizations: Organizations,
    verifier: TokenVerifier,
    /// The broker's keys, when fetched rather than in the config.
    remote: Option<RemoteKeys>,
    sessions: SessionCheck,
    http: reqwest::Client,
    clock: Clock,
}

impl Oidc {
    pub(crate) fn new(
        settings: OidcSettings,
        pool: sqlx::PgPool,
        clock: Clock,
    ) -> anyhow::Result<Self> {
        let OidcSettings {
            config,
            assertion_key,
            cookie_key,
            broker_url,
        } = settings;
        let http = dbt_state_broker::http_client()?;
        let (keys, remote) = match config.verification_keys() {
            Some(keys) => (keys, None),
            None => {
                let location = KeyLocation::Jwks(
                    config
                        .broker_jwks_url()
                        .map_or_else(|| format!("{broker_url}/jwks"), str::to_owned),
                );
                let remote = RemoteKeys::new(location, http.clone());
                (remote.keys(), Some(remote))
            }
        };
        let client = ui_client(&config)?;
        let issuer = config.broker_issuer().trim_end_matches('/').to_owned();
        Ok(Self {
            client_id: client.0,
            redirect_uri: client.1,
            token_endpoint: format!("{broker_url}/token"),
            assertion_audience: format!("{issuer}/token"),
            issuer,
            assertion_key,
            cookie_key,
            organizations: config.organizations().clone(),
            verifier: config
                .ui_token_verifier(keys)
                .expect("settings checked [clients.ui]"),
            remote,
            sessions: SessionCheck::new(pool, clock.clone()),
            http,
            clock,
        })
    }

    /// The console's cookie jar: encrypted, `__Host-`, `HttpOnly`,
    /// `SameSite=Lax`.
    fn jar<'a>(&'a self, cx: &'a Cx) -> impl Cookies + 'a {
        cookies(cx)
            .private(&self.cookie_key)
            .override_prefix_host()
            .override_http_only(true)
            .override_same_site(SameSite::Lax)
    }

    fn now(&self) -> SystemTime {
        self.clock.now()
    }

    /// Send the browser to sign in at the broker, returning to `next` (a
    /// local path) afterwards.
    pub(crate) fn sign_in(&self, cx: &Cx, next: &str) -> SeeOther {
        let pending = PendingSignIn {
            state: random(16),
            nonce: random(16),
            verifier: random(32),
            next: local_path(next).unwrap_or("/").to_owned(),
            expires_at: unix(self.now() + PENDING_LIFETIME),
        };
        let challenge = URL_SAFE_NO_PAD.encode(ring::digest::digest(
            &ring::digest::SHA256,
            pending.verifier.as_bytes(),
        ));
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.client_id)
            .append_pair("redirect_uri", &self.redirect_uri)
            .append_pair("scope", SCOPE)
            .append_pair("state", &pending.state)
            .append_pair("nonce", &pending.nonce)
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256")
            .finish();
        self.jar(cx).add(
            Cookie::build((
                PENDING_COOKIE,
                serde_json::to_string(&pending).expect("pending sign-in JSON"),
            ))
            .max_age(seconds(PENDING_LIFETIME))
            .build(),
        );
        see_other(format!("{}/?{query}", self.issuer))
    }

    /// Sign in again, returning to the current page (the console's home
    /// for anything but a GET).
    fn sign_in_again(&self, cx: &Cx) -> topcoat::Error {
        let next = if method(cx) == topcoat::router::Method::GET {
            uri(cx)
                .path_and_query()
                .map_or("/", |path| path.as_str())
                .to_owned()
        } else {
            "/".to_owned()
        };
        self.sign_in(cx, &next).into()
    }

    /// Complete a sign-in at the callback. On success the session cookie is
    /// set and the local path to return to is returned.
    pub(crate) async fn complete_sign_in(
        &self,
        cx: &Cx,
        code: Option<&str>,
        state: Option<&str>,
        error: Option<&str>,
    ) -> Result<String, CallbackError> {
        let jar = self.jar(cx);
        let pending = jar
            .get(PENDING_COOKIE)
            .and_then(|cookie| serde_json::from_str::<PendingSignIn>(cookie.value()).ok());
        if pending.is_some() {
            jar.remove(Cookie::build(PENDING_COOKIE).build());
        }
        let refuse = |reason: &str, outcome: CallbackError| {
            tracing::warn!(reason, "console sign-in refused");
            Err(outcome)
        };
        let Some(pending) = pending else {
            return refuse("no_pending_sign_in", CallbackError::Expired);
        };
        if pending.expires_at <= unix(self.now()) {
            return refuse("sign_in_expired", CallbackError::Expired);
        }
        if !state.is_some_and(|state| same(state, &pending.state)) {
            return refuse("state_mismatch", CallbackError::Expired);
        }
        if let Some(error) = error {
            let outcome = match error {
                "access_denied" => CallbackError::Denied,
                "temporarily_unavailable" => CallbackError::Unavailable,
                _ => CallbackError::Failed,
            };
            tracing::warn!(
                reason = "broker_error",
                error = oauth_code(error),
                "console sign-in refused"
            );
            return Err(outcome);
        }
        let Some(code) = code else {
            return refuse("missing_code", CallbackError::Failed);
        };
        let response = self
            .token_request(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", &self.redirect_uri),
                ("code_verifier", &pending.verifier),
            ])
            .await;
        let mut session = match self
            .session_from(response, Some(&pending.nonce), None)
            .await
        {
            Ok(session) => session,
            Err(failure) => {
                let outcome = match failure {
                    BrokerFailure::Unreachable => CallbackError::Unavailable,
                    _ => CallbackError::Failed,
                };
                return refuse(failure.reason(), outcome);
            }
        };
        let Some((first, _)) = self.shown(&session).into_iter().next() else {
            tracing::warn!(
                reason = "no_active_organization",
                principal = session.principal.as_str(),
                "console sign-in refused"
            );
            return Err(CallbackError::NoOrganization);
        };
        session.organization = Some(first);
        tracing::info!(
            principal = session.principal.as_str(),
            client_id = self.client_id.as_str(),
            "console sign-in completed"
        );
        self.write_session(cx, &session);
        Ok(pending.next)
    }

    /// The session a token response starts: for a sign-in, its `id_token`
    /// must echo `nonce`; for a renewal, it must be `previous`'s person and
    /// broker session, and keeps `previous`'s refresh token when the broker
    /// sends none.
    async fn session_from(
        &self,
        response: Result<TokenResponse, BrokerFailure>,
        nonce: Option<&str>,
        previous: Option<&Session>,
    ) -> Result<Session, BrokerFailure> {
        let response = response?;
        let token = self.verify(&response.id_token).await?;
        if token.client_id != self.client_id {
            return Err(BrokerFailure::WrongClient);
        }
        if let Some(nonce) = nonce
            && !token.nonce.as_deref().is_some_and(|got| same(got, nonce))
        {
            return Err(BrokerFailure::WrongNonce);
        }
        let sid = token.session.ok_or(BrokerFailure::NotAPerson)?;
        if let Some(previous) = previous
            && (token.principal.as_str() != previous.principal || sid != previous.sid)
        {
            return Err(BrokerFailure::OtherPerson);
        }
        let refresh_token = response
            .refresh_token
            .filter(|token| !token.is_empty())
            .or_else(|| previous.map(|session| session.refresh_token.clone()))
            .ok_or(BrokerFailure::NoRefreshToken)?;
        Ok(Session {
            principal: token.principal.as_str().to_owned(),
            sid,
            email: token.email,
            name: token.name,
            scope: token.scope.to_string(),
            expires_at: unix(token.expires_at),
            refresh_token,
            organization: previous.and_then(|session| session.organization.clone()),
            csrf: previous.map_or_else(|| random(16), |session| session.csrf.clone()),
        })
    }

    /// Check `token` with the broker's keys, refetching them once when it
    /// names a key they lack.
    async fn verify(&self, token: &str) -> Result<VerifiedToken, BrokerFailure> {
        if let Some(remote) = &self.remote {
            remote.refresh_if_stale().await;
        }
        let result = self.verifier.verify(token, self.now());
        let result = match (result, &self.remote) {
            (Err(TokenError::BadSignature), Some(remote)) => {
                remote.refresh_after_miss().await;
                self.verifier.verify(token, self.now())
            }
            (result, _) => result,
        };
        result.map_err(BrokerFailure::Token)
    }

    /// POST `form` to the broker's token endpoint, authenticated with a
    /// fresh client assertion.
    async fn token_request(&self, form: &[(&str, &str)]) -> Result<TokenResponse, BrokerFailure> {
        let assertion =
            self.assertion_key
                .assertion(&self.client_id, &self.assertion_audience, self.now());
        let mut form = form.to_vec();
        form.extend([
            ("client_id", self.client_id.as_str()),
            ("client_assertion_type", CLIENT_ASSERTION_TYPE),
            ("client_assertion", assertion.as_str()),
        ]);
        let body = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form)
            .finish();
        let response = self
            .http
            .post(&self.token_endpoint)
            .header("content-type", "application/x-www-form-urlencoded")
            .header("accept", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| {
                tracing::warn!(%error, "cannot reach the Token broker's token endpoint");
                BrokerFailure::Unreachable
            })?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|_| BrokerFailure::Unreachable)?;
        if status.is_success() {
            return serde_json::from_slice(&bytes).map_err(|_| BrokerFailure::Malformed);
        }
        let error = serde_json::from_slice::<OAuthError>(&bytes)
            .map(|body| body.error)
            .unwrap_or_default();
        tracing::warn!(
            status = status.as_u16(),
            error = oauth_code(&error),
            "the Token broker refused the console's token request"
        );
        if status.is_server_error() {
            Err(BrokerFailure::Unreachable)
        } else {
            Err(BrokerFailure::Refused)
        }
    }

    /// The person this request is for, after the session checks in the
    /// module docs; otherwise the error that answers it (sign in again,
    /// 403 without an Organization, 503 when session status is unreadable).
    pub(crate) async fn authenticate(&self, cx: &Cx) -> Result<Viewer, topcoat::Error> {
        let jar = self.jar(cx);
        let Some(mut session) = jar
            .get(SESSION_COOKIE)
            .and_then(|cookie| serde_json::from_str::<Session>(cookie.value()).ok())
        else {
            return Err(self.sign_in_again(cx));
        };
        let end = |reason: &str, session: &Session| {
            tracing::info!(
                reason,
                principal = session.principal.as_str(),
                "console session ended"
            );
            jar.remove(Cookie::build(SESSION_COOKIE).build());
            self.sign_in_again(cx)
        };
        match self.sessions.check(&session.sid).await {
            Ok(()) => {}
            Err(SessionCheckError::Inactive(refusal)) => {
                return Err(end(refusal.reason(), &session));
            }
            Err(SessionCheckError::Unavailable(error)) => {
                tracing::error!(%error, "cannot read the broker session status");
                return Err(service_unavailable(SESSION_STATUS_MAX_AGE.as_secs()).into());
            }
        }
        let mut changed = false;
        if unix(self.now() + RENEW_BEFORE) >= session.expires_at {
            let response = self
                .token_request(&[
                    ("grant_type", "refresh_token"),
                    ("refresh_token", &session.refresh_token),
                ])
                .await;
            match self.session_from(response, None, Some(&session)).await {
                Ok(renewed) => {
                    tracing::info!(
                        principal = renewed.principal.as_str(),
                        "console session renewed"
                    );
                    session = renewed;
                    changed = true;
                }
                Err(failure) => return Err(end(failure.reason(), &session)),
            }
        }
        let organizations = self.shown(&session);
        let Some(first) = organizations.first() else {
            tracing::warn!(
                reason = "no_active_organization",
                principal = session.principal.as_str(),
                "console request refused"
            );
            return Err(forbidden().into());
        };
        let organization = match &session.organization {
            Some(selected) if organizations.iter().any(|(id, _)| id == selected) => {
                selected.clone()
            }
            _ => {
                session.organization = Some(first.0.clone());
                changed = true;
                first.0.clone()
            }
        };
        if changed {
            self.write_session(cx, &session);
        }
        Ok(Viewer {
            organization,
            organizations,
            csrf: session.csrf.clone(),
            email: session.email.clone(),
            name: session.name.clone(),
            session,
        })
    }

    /// Select `organization` for `viewer`'s session, if it is one shown
    /// and `csrf` is the session's token.
    pub(crate) fn switch(
        &self,
        cx: &Cx,
        viewer: &Viewer,
        organization: &str,
        csrf: &str,
    ) -> Result<(), topcoat::Error> {
        let refuse = |reason: &str| {
            tracing::warn!(
                reason,
                principal = viewer.session.principal.as_str(),
                "console Organization switch refused"
            );
            Err(forbidden().into())
        };
        if !same(csrf, &viewer.csrf) {
            return refuse("csrf_mismatch");
        }
        if !viewer
            .organizations
            .iter()
            .any(|(id, _)| id == organization)
        {
            return refuse("organization_not_shown");
        }
        let mut session = viewer.session.clone();
        session.organization = Some(organization.to_owned());
        self.write_session(cx, &session);
        Ok(())
    }

    /// The Organizations `session` shows: active in its token and in this
    /// console's config, by id.
    fn shown(&self, session: &Session) -> Vec<(String, Role)> {
        let Ok(scope) = Scope::parse(&session.scope) else {
            return Vec::new();
        };
        scope
            .active()
            .filter(|(id, _)| self.organizations.is_active(id))
            .map(|(id, role): (&OrganizationId, Role)| (id.as_str().to_owned(), role))
            .collect()
    }

    fn write_session(&self, cx: &Cx, session: &Session) {
        self.jar(cx).add(
            Cookie::build((
                SESSION_COOKIE,
                serde_json::to_string(session).expect("session JSON"),
            ))
            .max_age(seconds(SESSION_COOKIE_MAX_AGE))
            .build(),
        );
    }
}

/// The UI's client id and callback from `config`.
fn ui_client(config: &AuthConfig) -> anyhow::Result<(String, String)> {
    let client = config
        .ui_client()
        .ok_or_else(|| anyhow::anyhow!("the auth config registers no UI client"))?;
    Ok((
        client.client_id().to_owned(),
        client.redirect_uri().to_owned(),
    ))
}

/// `path` if it is a local path to return to: absolute, on this host
/// (not `//` or `/\`), printable, and not a sign-in route.
pub(crate) fn local_path(path: &str) -> Option<&str> {
    let local = path.starts_with('/')
        && !path.starts_with("//")
        && !path.starts_with("/\\")
        && !path.starts_with("/auth/")
        && path.len() <= MAX_RETURN_PATH
        && path.chars().all(|c| c.is_ascii_graphic());
    local.then_some(path)
}

/// An OAuth error code as logged: at most 64 of its `[A-Za-z0-9_]`
/// characters, since the callback's `error` comes from the browser.
fn oauth_code(error: &str) -> String {
    error
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .take(64)
        .collect()
}

/// Unpadded base64url of `bytes` random bytes.
fn random(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    SystemRandom::new()
        .fill(&mut buf)
        .expect("the system random source");
    URL_SAFE_NO_PAD.encode(buf)
}

/// Whether `a` and `b` are equal, in time independent of where they differ.
fn same(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |diff, (x, y)| diff | (x ^ y))
            == 0
}

fn unix(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn seconds(duration: Duration) -> time::Duration {
    time::Duration::seconds(duration.as_secs() as i64)
}
