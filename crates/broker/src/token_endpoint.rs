//! `POST /token`: the dbt client's token requests.
//!
//! The pinned clients send (`dbt-fusion@9977b6c`
//! `crates/dbt-run-cache/src/auth/oauth.rs`; `dbt-state==2.62.6`,
//! `dbt-state@b0f3030` `clients/dbt_state/src/dbt_state/auth/sso.py`):
//! - with a client secret configured, `client_credentials`: HTTP Basic
//!   `client_id:secret` (reqwest `basic_auth` in Fusion, authlib's default
//!   `client_secret_basic` in Python) and the form
//!   `grant_type=client_credentials&scope=runcache:scope:orgs`;
//! - with a dbt platform credential, token exchange
//!   (`urn:ietf:params:oauth:grant-type:token-exchange`), refused here.
//!
//! Both read `id_token` (whose `scope` claim names the Organizations),
//! `token_type` (Python requires it), and `expires_in`; `refresh_token` only
//! when present. `client_credentials` therefore answers `id_token`, `scope`,
//! `token_type` and `expires_in`, and no refresh token.
//!
//! With a database, a token's Principal is recorded before the token is
//! issued (`principals`); a failed record refuses the request as
//! `temporarily_unavailable`, so every Principal on a decision this broker
//! enabled can be named.
//!
//! Every token issued is logged at `info`, and every refusal at `warn`, with
//! the grant, the Principal when known, the client id and the reason. Token
//! values and secrets are never logged.

use std::collections::BTreeMap;
use std::time::SystemTime;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use dbt_state_auth::{PrincipalId, PrincipalKind, TokenRequest};
use serde_json::json;

use crate::oauth::{no_store, OAuthError};
use crate::principals::{self, Principal};
use crate::upstream::UpstreamError;
use crate::Broker;

const CLIENT_CREDENTIALS: &str = "client_credentials";
const TOKEN_EXCHANGE: &str = "urn:ietf:params:oauth:grant-type:token-exchange";
/// The claim naming a machine Principal at the Upstream IdP.
const SUBJECT_CLAIM: &str = "sub";

/// What a refusal log line says.
#[derive(Default)]
struct Refusal<'a> {
    grant: Option<&'a str>,
    reason: &'a str,
    client_id: Option<&'a str>,
    principal: Option<&'a PrincipalId>,
    subject: Option<&'a str>,
    /// The Upstream IdP's error code, or why it was unavailable.
    upstream: Option<&'a str>,
    missing_claims: Option<&'a str>,
}

impl Refusal<'_> {
    fn respond(self, error: OAuthError) -> Response {
        tracing::warn!(
            grant = self.grant,
            reason = self.reason,
            principal = self.principal.map(PrincipalId::as_str),
            subject = self.subject,
            client_id = self.client_id,
            upstream = self.upstream,
            missing_claims = self.missing_claims,
            "token request refused"
        );
        error.into_response()
    }
}

pub(crate) async fn token(
    State(broker): State<Broker>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(form) = parse_form(&body) else {
        return Refusal {
            reason: "malformed_form",
            ..Refusal::default()
        }
        .respond(OAuthError::InvalidRequest);
    };
    let client_id = form.get("client_id").map(String::as_str);
    match form.get("grant_type").map(String::as_str) {
        Some(CLIENT_CREDENTIALS) => client_credentials(&broker, &headers).await,
        Some(TOKEN_EXCHANGE) => Refusal {
            grant: Some(TOKEN_EXCHANGE),
            reason: "token_exchange_unsupported",
            client_id,
            ..Refusal::default()
        }
        .respond(OAuthError::TokenExchange),
        Some(_) => Refusal {
            reason: "unsupported_grant_type",
            client_id,
            ..Refusal::default()
        }
        .respond(OAuthError::UnsupportedGrantType),
        None => Refusal {
            reason: "missing_grant_type",
            client_id,
            ..Refusal::default()
        }
        .respond(OAuthError::InvalidRequest),
    }
}

/// An `application/x-www-form-urlencoded` body; `None` when a parameter is
/// repeated (RFC 6749 §3.2) or the body is not UTF-8.
fn parse_form(body: &[u8]) -> Option<BTreeMap<String, String>> {
    std::str::from_utf8(body).ok()?;
    let mut form = BTreeMap::new();
    for (key, value) in form_urlencoded::parse(body) {
        if form.insert(key.into_owned(), value.into_owned()).is_some() {
            return None;
        }
    }
    Some(form)
}

/// The client id of an HTTP Basic `authorization` value: the part before
/// the first `:`, form-urlencoding decoded (RFC 6749 §2.3.1; Fusion sends it
/// raw, which decodes to itself for ordinary ids). The secret after it is
/// never decoded here.
fn basic_client_id(authorization: &HeaderValue) -> Option<String> {
    let (scheme, credentials) = authorization.to_str().ok()?.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = STANDARD.decode(credentials.trim()).ok()?;
    let colon = decoded.iter().position(|&b| b == b':')?;
    let id = form_decode(&decoded[..colon])?;
    (!id.is_empty()).then_some(id)
}

/// `bytes` with `application/x-www-form-urlencoded` escapes (`+`, `%XX`)
/// decoded, when the result is UTF-8. An invalid escape stays as it is.
fn form_decode(bytes: &[u8]) -> Option<String> {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |b: u8| (b as char).to_digit(16);
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => match (
                bytes.get(i + 1).and_then(|&b| hex(b)),
                bytes.get(i + 2).and_then(|&b| hex(b)),
            ) {
                (Some(high), Some(low)) => {
                    out.push((high * 16 + low) as u8);
                    i += 2;
                }
                _ => out.push(b'%'),
            },
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out).ok()
}

/// `client_credentials` pass-through: forward the client's credentials to
/// the Upstream IdP, check the access token it returns, apply the machine
/// mapping rules, and issue a machine token.
async fn client_credentials(broker: &Broker, headers: &HeaderMap) -> Response {
    let grant = Some(CLIENT_CREDENTIALS);
    let authorization = headers.get(header::AUTHORIZATION);
    let Some((authorization, client_id)) =
        authorization.and_then(|value| Some((value, basic_client_id(value)?)))
    else {
        return Refusal {
            grant,
            reason: "missing_client_credentials",
            ..Refusal::default()
        }
        .respond(OAuthError::InvalidClient);
    };
    let client = client_id;
    let client_id = Some(client.as_str());
    let upstream_failure = |error: UpstreamError| match error {
        UpstreamError::RejectedClient { error } => Refusal {
            grant,
            reason: "upstream_rejected_client",
            client_id,
            upstream: error.as_deref(),
            ..Refusal::default()
        }
        .respond(OAuthError::InvalidClient),
        UpstreamError::Refused { status, error } => Refusal {
            grant,
            reason: "upstream_refused_grant",
            client_id,
            upstream: Some(error.as_deref().unwrap_or(&format!("HTTP {status}"))),
            ..Refusal::default()
        }
        .respond(OAuthError::UnauthorizedClient),
        UpstreamError::Unavailable(detail) => Refusal {
            grant,
            reason: "upstream_unavailable",
            client_id,
            upstream: Some(&detail),
            ..Refusal::default()
        }
        .respond(OAuthError::TemporarilyUnavailable),
    };

    let access_token = match broker.0.upstream.client_credentials(authorization).await {
        Ok(token) => token,
        Err(error) => return upstream_failure(error),
    };
    let claims = match broker.0.upstream.verify(&access_token).await {
        Ok(Ok(claims)) => claims,
        Ok(Err(error)) => {
            return Refusal {
                grant,
                reason: error.reason(),
                client_id,
                ..Refusal::default()
            }
            .respond(OAuthError::UnauthorizedClient)
        }
        Err(error) => return upstream_failure(error),
    };
    let Some(subject) = claims.string(SUBJECT_CLAIM).filter(|s| !s.is_empty()) else {
        return Refusal {
            grant,
            reason: "upstream_token_missing_subject",
            client_id,
            ..Refusal::default()
        }
        .respond(OAuthError::UnauthorizedClient);
    };
    let principal = PrincipalId::upstream(broker.0.upstream.issuer(), subject);
    let scope = broker
        .0
        .rules
        .scope(PrincipalKind::Machine, &claims, &broker.0.organizations);
    if scope.is_empty() {
        let missing: Vec<&str> = broker
            .0
            .rules
            .claims_read(PrincipalKind::Machine)
            .into_iter()
            .filter(|claim| !claims.contains(claim))
            .collect();
        let missing = missing.join(",");
        return Refusal {
            grant,
            reason: "no_matching_rule",
            client_id,
            principal: Some(&principal),
            subject: Some(subject),
            missing_claims: (!missing.is_empty()).then_some(missing.as_str()),
            ..Refusal::default()
        }
        .respond(OAuthError::UnauthorizedClient);
    }

    if let Some(database) = &broker.0.database {
        let record = Principal {
            id: &principal,
            issuer: broker.0.upstream.issuer(),
            subject,
            kind: PrincipalKind::Machine,
            email: None,
        };
        if let Err(error) = principals::record(database, &record).await {
            tracing::error!(%error, "cannot record the Principal");
            return Refusal {
                grant,
                reason: "principal_not_recorded",
                client_id,
                principal: Some(&principal),
                subject: Some(subject),
                ..Refusal::default()
            }
            .respond(OAuthError::TemporarilyUnavailable);
        }
    }

    let client_id = client.as_str();
    let request = TokenRequest::machine(principal.clone(), client_id, scope.clone());
    let issued = broker.0.tokens.issue(&request, SystemTime::now());
    let scope = scope.to_string();
    tracing::info!(
        grant = CLIENT_CREDENTIALS,
        principal = principal.as_str(),
        kind = PrincipalKind::Machine.as_str(),
        subject,
        client_id,
        scope = scope.as_str(),
        expires_in = issued.expires_in.as_secs(),
        "token issued"
    );
    no_store(
        Json(json!({
            "id_token": issued.token,
            "scope": scope,
            "token_type": "Bearer",
            "expires_in": issued.expires_in.as_secs(),
        }))
        .into_response(),
    )
}
