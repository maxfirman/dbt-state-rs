//! Checking a token the Upstream IdP issued: an RS256 JWT signed by one of
//! the IdP's published keys, from the IdP's issuer, for the expected
//! audience, and current. Access tokens (`client_credentials`) are for the
//! configured upstream API audience; opaque ones are refused, since the
//! Token broker never introspects. ID tokens (browser sign-in) are for the
//! Token broker's own client id and carry the nonce it sent (OIDC Core
//! §3.1.3.7).

use std::time::SystemTime;

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::keys::VerificationKeys;
use crate::token::{decode_json, unix_seconds, verify_signature, SignatureError};
use crate::{UpstreamClaims, CLOCK_SKEW};

/// Why an upstream access token was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamTokenError {
    /// Not a compact RS256 JWS with a `kid` (for example an opaque token).
    NotSignedJwt,
    /// Not signed by a key the IdP publishes.
    BadSignature,
    WrongIssuer,
    /// `aud` does not hold the configured upstream API audience.
    WrongAudience,
    /// Past `exp` by more than [`CLOCK_SKEW`], or without `exp`.
    Expired,
    /// Before `nbf` by more than [`CLOCK_SKEW`].
    NotYetValid,
    /// An ID token without the nonce the Token broker sent.
    WrongNonce,
    /// An ID token whose `azp` is another client, or that has several
    /// audiences and no `azp`.
    WrongAuthorizedParty,
}

impl UpstreamTokenError {
    /// The reason recorded in Token broker logs.
    pub fn reason(self) -> &'static str {
        match self {
            Self::NotSignedJwt => "upstream_token_not_a_signed_jwt",
            Self::BadSignature => "upstream_token_bad_signature",
            Self::WrongIssuer => "upstream_token_wrong_issuer",
            Self::WrongAudience => "upstream_token_wrong_audience",
            Self::Expired => "upstream_token_expired",
            Self::NotYetValid => "upstream_token_not_yet_valid",
            Self::WrongNonce => "upstream_token_wrong_nonce",
            Self::WrongAuthorizedParty => "upstream_token_wrong_authorized_party",
        }
    }
}

#[derive(Deserialize)]
struct Registered {
    #[serde(default)]
    iss: Value,
    #[serde(default)]
    aud: Value,
    #[serde(default)]
    exp: Option<u64>,
    #[serde(default)]
    nbf: Option<u64>,
    #[serde(default)]
    azp: Option<Value>,
    #[serde(default)]
    nonce: Option<Value>,
}

/// Checks the Upstream IdP's access tokens.
#[derive(Debug, Clone)]
pub struct UpstreamTokenVerifier {
    issuer: String,
    audience: String,
    keys: VerificationKeys,
}

impl UpstreamTokenVerifier {
    /// Tokens from `issuer` for `audience`, signed by one of `keys` (the
    /// IdP's JWKS).
    pub fn new(issuer: String, audience: String, keys: VerificationKeys) -> Self {
        Self {
            issuer,
            audience,
            keys,
        }
    }

    /// Check `token` at `now`; its claims if it passes.
    pub fn verify(
        &self,
        token: &str,
        now: SystemTime,
    ) -> Result<UpstreamClaims, UpstreamTokenError> {
        self.check(token, now).map(|(claims, _)| claims)
    }

    /// Check the ID token `token` at `now`, for a verifier whose audience is
    /// the Token broker's client id: as [`Self::verify`], and also its
    /// `azp`, when present, is that client id (required with several
    /// audiences), and its `nonce` is `nonce` when one was sent. A refresh
    /// response's ID token normally carries none (OIDC Core §12.2).
    pub fn verify_id_token(
        &self,
        token: &str,
        nonce: Option<&str>,
        now: SystemTime,
    ) -> Result<UpstreamClaims, UpstreamTokenError> {
        let (claims, registered) = self.check(token, now)?;
        let several_audiences = registered.aud.as_array().is_some_and(|auds| auds.len() > 1);
        match &registered.azp {
            Some(azp) if azp.as_str() != Some(self.audience.as_str()) => {
                return Err(UpstreamTokenError::WrongAuthorizedParty)
            }
            None if several_audiences => return Err(UpstreamTokenError::WrongAuthorizedParty),
            _ => {}
        }
        if let Some(nonce) = nonce {
            if registered.nonce.as_ref().and_then(Value::as_str) != Some(nonce) {
                return Err(UpstreamTokenError::WrongNonce);
            }
        }
        Ok(claims)
    }

    fn check(
        &self,
        token: &str,
        now: SystemTime,
    ) -> Result<(UpstreamClaims, Registered), UpstreamTokenError> {
        let payload = verify_signature(token, &self.keys).map_err(|error| match error {
            SignatureError::Malformed => UpstreamTokenError::NotSignedJwt,
            SignatureError::BadSignature => UpstreamTokenError::BadSignature,
        })?;
        let claims: Map<String, Value> =
            decode_json(payload).map_err(|_| UpstreamTokenError::NotSignedJwt)?;
        let registered: Registered = serde_json::from_value(Value::Object(claims.clone()))
            .map_err(|_| UpstreamTokenError::NotSignedJwt)?;
        if registered.iss.as_str() != Some(self.issuer.as_str()) {
            return Err(UpstreamTokenError::WrongIssuer);
        }
        let for_us = match &registered.aud {
            Value::String(aud) => *aud == self.audience,
            Value::Array(auds) => auds.iter().any(|aud| aud.as_str() == Some(&self.audience)),
            _ => false,
        };
        if !for_us {
            return Err(UpstreamTokenError::WrongAudience);
        }
        let now = unix_seconds(now);
        let skew = CLOCK_SKEW.as_secs();
        match registered.exp {
            Some(exp) if now <= exp.saturating_add(skew) => {}
            _ => return Err(UpstreamTokenError::Expired),
        }
        if registered
            .nbf
            .is_some_and(|nbf| now.saturating_add(skew) < nbf)
        {
            return Err(UpstreamTokenError::NotYetValid);
        }
        Ok((UpstreamClaims::new(claims), registered))
    }
}
