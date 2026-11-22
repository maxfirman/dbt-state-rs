//! Checking an access token the Upstream IdP issued: an RS256 JWT signed by
//! one of the IdP's published keys, from the IdP's issuer, for the
//! configured upstream API audience, and current. Opaque tokens are refused:
//! the Token broker never introspects.

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
        Ok(UpstreamClaims::new(claims))
    }
}
