//! Token issuing and checking: RS256 JWTs in the dbt client's shape.
//!
//! A token carries `iss` (the Token broker), `aud` (the dbt State API
//! audience), `azp` (the client id), `sub` (the Principal), `scope` (the
//! runcache grammar, [`Scope`]), `iat` and `exp`; person tokens add `sid`
//! (their broker session), and `email` and `name` appear when the Upstream
//! IdP provides them. The JOSE header names the signing key id (`kid`).
//!
//! Tokens for another registered client of the Token broker (the UI) are
//! standard OIDC tokens in the same shape, but for that client's audience
//! ([`TokenIssuer::issue_oidc`]): `aud` is its client id, never the dbt
//! State API audience, so the gRPC server refuses them.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::config::TokenLifetimes;
use crate::keys::{SigningKey, VerificationKeys};
use crate::Scope;

/// How far past `exp` a token is still accepted, for clock differences.
pub const CLOCK_SKEW: Duration = Duration::from_secs(60);

const ALGORITHM: &str = "RS256";

/// The opaque, stable id of a Principal: a token's `sub`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PrincipalId(String);

impl PrincipalId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The Principal the Upstream IdP `issuer` knows as `subject`: an opaque
    /// id (unpadded base64url SHA-256 of the length-prefixed pair) that is
    /// stable for that pair and never reveals it.
    pub fn upstream(issuer: &str, subject: &str) -> Self {
        let mut context = ring::digest::Context::new(&ring::digest::SHA256);
        context.update(&(issuer.len() as u64).to_be_bytes());
        context.update(issuer.as_bytes());
        context.update(subject.as_bytes());
        Self(URL_SAFE_NO_PAD.encode(context.finish()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Whether a Principal is a person or a machine. A person token names its
/// broker session (`sid`); a machine token has none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrincipalKind {
    Person,
    Machine,
}

impl PrincipalKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Person => "person",
            Self::Machine => "machine",
        }
    }
}

/// Who a token is for and what it grants.
#[derive(Debug, Clone)]
pub struct TokenRequest {
    pub principal: PrincipalId,
    /// A person's broker session (`sid`); `None` for a machine.
    pub session: Option<String>,
    /// The client the token is issued to (`azp`).
    pub client_id: String,
    pub scope: Scope,
    pub email: Option<String>,
    pub name: Option<String>,
}

impl TokenRequest {
    /// A person token for `principal` in broker session `session`.
    pub fn person(
        principal: PrincipalId,
        session: impl Into<String>,
        client_id: impl Into<String>,
        scope: Scope,
    ) -> Self {
        Self {
            principal,
            session: Some(session.into()),
            client_id: client_id.into(),
            scope,
            email: None,
            name: None,
        }
    }

    /// A machine token for `principal`.
    pub fn machine(principal: PrincipalId, client_id: impl Into<String>, scope: Scope) -> Self {
        Self {
            principal,
            session: None,
            client_id: client_id.into(),
            scope,
            email: None,
            name: None,
        }
    }

    /// The Principal kind: a person when the token names a session.
    pub fn kind(&self) -> PrincipalKind {
        match self.session {
            Some(_) => PrincipalKind::Person,
            None => PrincipalKind::Machine,
        }
    }
}

/// A signed token and how long it lives. `Debug` never shows the token.
#[derive(Clone)]
pub struct IssuedToken {
    pub token: String,
    pub expires_in: Duration,
}

impl fmt::Debug for IssuedToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IssuedToken")
            .field("token", &"<redacted>")
            .field("expires_in", &self.expires_in)
            .finish()
    }
}

/// The tokens an OIDC client (the UI) gets for a person: an ID token and a
/// JWT access token, both with `aud` the client id. `Debug` never shows
/// them.
#[derive(Clone)]
pub struct OidcTokens {
    pub id_token: String,
    pub access_token: String,
    pub expires_in: Duration,
}

impl fmt::Debug for OidcTokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OidcTokens")
            .field("id_token", &"<redacted>")
            .field("access_token", &"<redacted>")
            .field("expires_in", &self.expires_in)
            .finish()
    }
}

#[derive(Serialize)]
struct Header<'a> {
    alg: &'a str,
    typ: &'a str,
    kid: &'a str,
}

#[derive(Deserialize)]
struct ReceivedHeader {
    alg: String,
    kid: String,
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    aud: &'a str,
    azp: &'a str,
    sub: &'a str,
    scope: String,
    iat: u64,
    exp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    sid: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nonce: Option<&'a str>,
}

#[derive(Deserialize)]
struct ReceivedClaims {
    iss: serde_json::Value,
    aud: serde_json::Value,
    azp: String,
    sub: String,
    scope: String,
    /// Required in the issued shape; nothing depends on its value.
    #[allow(dead_code)]
    iat: u64,
    exp: u64,
    #[serde(default)]
    sid: Option<String>,
}

/// Signs tokens with the active signing key.
#[derive(Debug, Clone)]
pub struct TokenIssuer {
    issuer: String,
    audience: String,
    key: SigningKey,
    lifetimes: TokenLifetimes,
}

impl TokenIssuer {
    pub(crate) fn new(
        issuer: String,
        audience: String,
        key: SigningKey,
        lifetimes: TokenLifetimes,
    ) -> Self {
        Self {
            issuer,
            audience,
            key,
            lifetimes,
        }
    }

    /// A token for `request`, issued at `now`, living for its Principal
    /// kind's configured lifetime.
    pub fn issue(&self, request: &TokenRequest, now: SystemTime) -> IssuedToken {
        let (token, expires_in) = self.sign(request, &self.audience, None, now);
        IssuedToken { token, expires_in }
    }

    /// The OIDC tokens for `request`, a person signed in to another client
    /// of the Token broker (the UI), issued at `now`: `aud` is the request's
    /// client id. The ID token carries `nonce` when given (a code exchange
    /// echoes the authorize request's; a refresh has none, OIDC Core §12.2);
    /// the access token never does.
    pub fn issue_oidc(
        &self,
        request: &TokenRequest,
        nonce: Option<&str>,
        now: SystemTime,
    ) -> OidcTokens {
        let audience = &request.client_id;
        let (id_token, expires_in) = self.sign(request, audience, nonce, now);
        let (access_token, _) = self.sign(request, audience, None, now);
        OidcTokens {
            id_token,
            access_token,
            expires_in,
        }
    }

    fn sign(
        &self,
        request: &TokenRequest,
        audience: &str,
        nonce: Option<&str>,
        now: SystemTime,
    ) -> (String, Duration) {
        let lifetime = match request.kind() {
            PrincipalKind::Person => self.lifetimes.person,
            PrincipalKind::Machine => self.lifetimes.machine,
        };
        let iat = unix_seconds(now);
        let claims = Claims {
            iss: &self.issuer,
            aud: audience,
            azp: &request.client_id,
            sub: request.principal.as_str(),
            scope: request.scope.to_string(),
            iat,
            exp: iat.saturating_add(lifetime.as_secs()),
            sid: request.session.as_deref(),
            email: request.email.as_deref(),
            name: request.name.as_deref(),
            nonce,
        };
        let header = Header {
            alg: ALGORITHM,
            typ: "JWT",
            kid: &self.key.id,
        };
        let signing_input = format!("{}.{}", encode_json(&header), encode_json(&claims));
        let signature = URL_SAFE_NO_PAD.encode(self.key.sign(signing_input.as_bytes()));
        (format!("{signing_input}.{signature}"), lifetime)
    }
}

/// Why a token was refused. Each maps to one fixed `UNAUTHENTICATED`
/// message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenError {
    /// No `authorization` bearer token.
    Missing,
    /// Not a well-formed token in the issued shape.
    Malformed,
    /// Not signed by a loaded key.
    BadSignature,
    WrongIssuer,
    WrongAudience,
    /// Past `exp` by more than [`CLOCK_SKEW`].
    Expired,
}

impl TokenError {
    /// The fixed message returned to the client.
    pub fn message(self) -> &'static str {
        match self {
            Self::Missing => "missing bearer token",
            Self::Malformed => "malformed bearer token",
            Self::BadSignature => "invalid bearer token signature",
            Self::WrongIssuer => "bearer token has the wrong issuer",
            Self::WrongAudience => "bearer token has the wrong audience",
            Self::Expired => "bearer token has expired",
        }
    }

    /// The reason recorded in server logs.
    pub fn reason(self) -> &'static str {
        match self {
            Self::Missing => "missing_token",
            Self::Malformed => "malformed_token",
            Self::BadSignature => "bad_signature",
            Self::WrongIssuer => "wrong_issuer",
            Self::WrongAudience => "wrong_audience",
            Self::Expired => "expired",
        }
    }
}

/// A token that passed every check.
#[derive(Debug, Clone)]
pub struct VerifiedToken {
    pub principal: PrincipalId,
    pub kind: PrincipalKind,
    /// A person token's broker session.
    pub session: Option<String>,
    pub client_id: String,
    pub scope: Scope,
}

/// Checks tokens: signature by a loaded key, issuer, the dbt State API
/// audience, and expiry with [`CLOCK_SKEW`].
#[derive(Debug, Clone)]
pub struct TokenVerifier {
    issuer: String,
    audience: String,
    keys: VerificationKeys,
}

impl TokenVerifier {
    pub(crate) fn new(issuer: String, audience: String, keys: VerificationKeys) -> Self {
        Self {
            issuer,
            audience,
            keys,
        }
    }

    /// Check `token` at `now`.
    pub fn verify(&self, token: &str, now: SystemTime) -> Result<VerifiedToken, TokenError> {
        let payload = verify_signature(token, &self.keys).map_err(|error| match error {
            SignatureError::Malformed => TokenError::Malformed,
            SignatureError::BadSignature | SignatureError::UnknownKey => TokenError::BadSignature,
        })?;

        let claims: ReceivedClaims = decode_json(payload)?;
        if claims.iss.as_str() != Some(self.issuer.as_str()) {
            return Err(TokenError::WrongIssuer);
        }
        if claims.aud.as_str() != Some(self.audience.as_str()) {
            return Err(TokenError::WrongAudience);
        }
        if unix_seconds(now) > claims.exp.saturating_add(CLOCK_SKEW.as_secs()) {
            return Err(TokenError::Expired);
        }
        if claims.sub.is_empty() || claims.azp.is_empty() {
            return Err(TokenError::Malformed);
        }
        let scope = Scope::parse(&claims.scope).map_err(|_| TokenError::Malformed)?;
        let kind = match &claims.sid {
            Some(sid) if sid.is_empty() => return Err(TokenError::Malformed),
            Some(_) => PrincipalKind::Person,
            None => PrincipalKind::Machine,
        };
        Ok(VerifiedToken {
            principal: PrincipalId(claims.sub),
            kind,
            session: claims.sid,
            client_id: claims.azp,
            scope,
        })
    }
}

/// Why [`verify_signature`] refused a token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignatureError {
    /// Not a compact JWS, or not RS256 with a `kid`.
    Malformed,
    /// Its `kid` names a key of the set, which did not sign it.
    BadSignature,
    /// Its `kid` names no key of the set.
    UnknownKey,
}

/// The encoded payload of the compact RS256 JWS `token`, if one of `keys`
/// signed it.
pub(crate) fn verify_signature<'t>(
    token: &'t str,
    keys: &VerificationKeys,
) -> Result<&'t str, SignatureError> {
    let mut parts = token.split('.');
    let (Some(encoded_header), Some(payload), Some(signature), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(SignatureError::Malformed);
    };
    let header: ReceivedHeader =
        decode_json(encoded_header).map_err(|_| SignatureError::Malformed)?;
    if header.alg != ALGORITHM {
        return Err(SignatureError::Malformed);
    }
    let signature = URL_SAFE_NO_PAD
        .decode(signature)
        .map_err(|_| SignatureError::Malformed)?;
    let signing_input = &token[..encoded_header.len() + 1 + payload.len()];
    if !keys.verify(&header.kid, signing_input.as_bytes(), &signature) {
        return Err(if keys.contains(&header.kid) {
            SignatureError::BadSignature
        } else {
            SignatureError::UnknownKey
        });
    }
    Ok(payload)
}

fn encode_json(value: &impl Serialize) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).expect("token JSON"))
}

pub(crate) fn decode_json<T: for<'de> Deserialize<'de>>(part: &str) -> Result<T, TokenError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(part)
        .map_err(|_| TokenError::Malformed)?;
    serde_json::from_slice(&bytes).map_err(|_| TokenError::Malformed)
}

pub(crate) fn unix_seconds(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}
