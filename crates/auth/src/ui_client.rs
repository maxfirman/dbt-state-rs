//! The UI's registration at the Token broker (`[clients.ui]`): a
//! confidential OIDC client with its own client id, exactly one callback,
//! and `private_key_jwt` client authentication at the token endpoint (RFC
//! 7523 §2.2 and §3, OIDC Core §9). The broker holds only the UI's public
//! key; the private key is mounted into the UI, and no shared secret exists.
//!
//! A client assertion is a compact RS256 JWS whose claims are `iss` and `sub`
//! the UI's client id, `aud` the broker's issuer or its token endpoint URL
//! (a single string), `exp`, and a `jti` the broker accepts once; `iat` and
//! `nbf` are optional. An assertion valid for more than
//! [`MAX_ASSERTION_LIFETIME`] is refused, so the broker need remember each
//! `jti` only briefly ([`VerifiedAssertion::remember_until`]).

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::signature::{RsaPublicKeyComponents, RSA_PKCS1_2048_8192_SHA256};
use rustls_pki_types::pem::{self, PemObject};
use rustls_pki_types::SubjectPublicKeyInfoDer;
use serde::Deserialize;

use crate::token::unix_seconds;
use crate::CLOCK_SKEW;

/// The longest a client assertion may be valid: its `exp` at most this far
/// past its `iat` and past the moment it is presented (plus [`CLOCK_SKEW`]).
pub const MAX_ASSERTION_LIFETIME: Duration = Duration::from_secs(300);

/// The longest `jti` accepted.
const MAX_JTI_LENGTH: usize = 256;

/// See the module docs.
#[derive(Debug, Clone)]
pub struct UiClient {
    client_id: String,
    redirect_uri: String,
    key: ClientPublicKey,
}

impl UiClient {
    pub(crate) fn new(client_id: String, redirect_uri: String, key: ClientPublicKey) -> Self {
        Self {
            client_id,
            redirect_uri,
            key,
        }
    }

    /// The UI's client id: the `aud` of the tokens issued to it.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// The UI's only callback: its authorize requests must name exactly
    /// this `redirect_uri`.
    pub fn redirect_uri(&self) -> &str {
        &self.redirect_uri
    }

    /// Check the client assertion `assertion` at `now`: signed by the UI's
    /// key, `iss` and `sub` its client id, `aud` one of `audiences` (the
    /// broker's issuer and token endpoint URL), current, and valid for at
    /// most [`MAX_ASSERTION_LIFETIME`]. Whether its `jti` was seen before is
    /// the caller's to check.
    pub fn verify_assertion(
        &self,
        assertion: &str,
        audiences: &[&str],
        now: SystemTime,
    ) -> Result<VerifiedAssertion, ClientAssertionError> {
        use ClientAssertionError as E;
        let mut parts = assertion.split('.');
        let (Some(header), Some(payload), Some(signature), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(E::Malformed);
        };
        let signing_input = &assertion[..header.len() + 1 + payload.len()];
        let header: AssertionHeader = decode(header)?;
        if header.alg != "RS256" {
            return Err(E::Malformed);
        }
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| E::Malformed)?;
        if !self.key.verify(signing_input.as_bytes(), &signature) {
            return Err(E::BadSignature);
        }
        let claims: AssertionClaims = decode(payload)?;
        if claims.iss != self.client_id || claims.sub != self.client_id {
            return Err(E::WrongIssuer);
        }
        if !audiences.contains(&claims.aud.as_str()) {
            return Err(E::WrongAudience);
        }
        let now = unix_seconds(now);
        let skew = CLOCK_SKEW.as_secs();
        let max = MAX_ASSERTION_LIFETIME.as_secs();
        if now > claims.exp.saturating_add(skew) {
            return Err(E::Expired);
        }
        if claims.exp > now.saturating_add(max + skew)
            || claims
                .iat
                .is_some_and(|iat| claims.exp > iat.saturating_add(max))
        {
            return Err(E::TooLongLived);
        }
        if [claims.iat, claims.nbf]
            .into_iter()
            .flatten()
            .any(|at| at > now.saturating_add(skew))
        {
            return Err(E::NotYetValid);
        }
        let jti = claims
            .jti
            .filter(|jti| !jti.is_empty())
            .ok_or(E::MissingJti)?;
        if jti.len() > MAX_JTI_LENGTH {
            return Err(E::Malformed);
        }
        Ok(VerifiedAssertion {
            jti,
            remember_until: UNIX_EPOCH + Duration::from_secs(claims.exp.saturating_add(skew)),
        })
    }
}

fn decode<T: for<'de> Deserialize<'de>>(part: &str) -> Result<T, ClientAssertionError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(part)
        .map_err(|_| ClientAssertionError::Malformed)?;
    serde_json::from_slice(&bytes).map_err(|_| ClientAssertionError::Malformed)
}

#[derive(Deserialize)]
struct AssertionHeader {
    alg: String,
}

#[derive(Deserialize)]
struct AssertionClaims {
    iss: String,
    sub: String,
    aud: String,
    exp: u64,
    #[serde(default)]
    iat: Option<u64>,
    #[serde(default)]
    nbf: Option<u64>,
    #[serde(default)]
    jti: Option<String>,
}

/// A client assertion that passed every check but the `jti` replay check.
#[derive(Debug, Clone)]
pub struct VerifiedAssertion {
    /// Its `jti`: a second assertion with it must be refused until
    /// [`Self::remember_until`].
    pub jti: String,
    /// When it can no longer be accepted (`exp` plus [`CLOCK_SKEW`]).
    pub remember_until: SystemTime,
}

/// Why a client assertion was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAssertionError {
    /// Not a compact RS256 JWS with the required claims.
    Malformed,
    /// Not signed by the UI's key.
    BadSignature,
    /// `iss` or `sub` is not the UI's client id.
    WrongIssuer,
    /// `aud` is neither the broker's issuer nor its token endpoint.
    WrongAudience,
    /// Past `exp` by more than [`CLOCK_SKEW`].
    Expired,
    /// Valid for longer than [`MAX_ASSERTION_LIFETIME`].
    TooLongLived,
    /// `iat` or `nbf` is in the future by more than [`CLOCK_SKEW`].
    NotYetValid,
    /// No `jti`.
    MissingJti,
}

impl ClientAssertionError {
    /// The reason recorded in the broker's logs.
    pub fn reason(self) -> &'static str {
        match self {
            Self::Malformed => "client_assertion_malformed",
            Self::BadSignature => "client_assertion_bad_signature",
            Self::WrongIssuer => "client_assertion_wrong_issuer",
            Self::WrongAudience => "client_assertion_wrong_audience",
            Self::Expired => "client_assertion_expired",
            Self::TooLongLived => "client_assertion_too_long_lived",
            Self::NotYetValid => "client_assertion_not_yet_valid",
            Self::MissingJti => "client_assertion_missing_jti",
        }
    }
}

/// Why a client's public key file was refused. Never carries its contents.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublicKeyFileError {
    #[error("cannot read: {0}")]
    Unreadable(String),
    #[error("no public key PEM section (BEGIN PUBLIC KEY)")]
    NoPublicKey,
    #[error("invalid PEM")]
    InvalidPem,
    #[error("not an RSA public key")]
    NotRsa,
    #[error("unusable RSA public key; RS256 needs 2048 to 4096 bits")]
    Rejected,
}

/// An RS256 public key from a PEM `SubjectPublicKeyInfo` (`BEGIN PUBLIC
/// KEY`, as `openssl pkey -pubout` writes it).
#[derive(Clone)]
pub(crate) struct ClientPublicKey(RsaPublicKeyComponents<Vec<u8>>);

impl std::fmt::Debug for ClientPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClientPublicKey")
    }
}

impl ClientPublicKey {
    pub(crate) fn load(path: &Path) -> Result<Self, PublicKeyFileError> {
        let der = SubjectPublicKeyInfoDer::from_pem_file(path).map_err(|error| match error {
            pem::Error::Io(error) => PublicKeyFileError::Unreadable(error.kind().to_string()),
            pem::Error::NoItemsFound => PublicKeyFileError::NoPublicKey,
            _ => PublicKeyFileError::InvalidPem,
        })?;
        rsa_public_key(der.as_ref()).map(Self)
    }

    fn verify(&self, message: &[u8], signature: &[u8]) -> bool {
        self.0
            .verify(&RSA_PKCS1_2048_8192_SHA256, message, signature)
            .is_ok()
    }
}

/// The content of the DER `SEQUENCE { OID rsaEncryption, NULL }` an RSA
/// `SubjectPublicKeyInfo` names its algorithm with (RFC 3279 §2.3.1).
const RSA_ENCRYPTION: &[u8] = &[
    0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00,
];

/// The modulus and exponent of the DER RSA `SubjectPublicKeyInfo` `spki`.
fn rsa_public_key(spki: &[u8]) -> Result<RsaPublicKeyComponents<Vec<u8>>, PublicKeyFileError> {
    use PublicKeyFileError::{InvalidPem, NotRsa, Rejected};
    const SEQUENCE: u8 = 0x30;
    const BIT_STRING: u8 = 0x03;
    const INTEGER: u8 = 0x02;
    let (info, rest) = der(spki, SEQUENCE).ok_or(InvalidPem)?;
    if !rest.is_empty() {
        return Err(InvalidPem);
    }
    let (algorithm, info) = der(info, SEQUENCE).ok_or(InvalidPem)?;
    if algorithm != RSA_ENCRYPTION {
        return Err(NotRsa);
    }
    let (bits, rest) = der(info, BIT_STRING).ok_or(InvalidPem)?;
    let ([0, key @ ..], []) = (bits, rest) else {
        return Err(InvalidPem);
    };
    let (key, rest) = der(key, SEQUENCE).ok_or(Rejected)?;
    let (n, key) = der(key, INTEGER).ok_or(Rejected)?;
    let (e, key) = der(key, INTEGER).ok_or(Rejected)?;
    if !key.is_empty() || !rest.is_empty() {
        return Err(Rejected);
    }
    let strip = |int: &[u8]| -> Vec<u8> {
        let start = int.iter().position(|&b| b != 0).unwrap_or(int.len());
        int[start..].to_vec()
    };
    let (n, e) = (strip(n), strip(e));
    let modulus_bits = n
        .first()
        .map_or(0, |&top| n.len() * 8 - top.leading_zeros() as usize);
    let odd_exponent = e.last().is_some_and(|&low| low & 1 == 1);
    if !(2048..=4096).contains(&modulus_bits) || !odd_exponent || e.len() > 4 {
        return Err(Rejected);
    }
    Ok(RsaPublicKeyComponents { n, e })
}

/// The content of the DER element with `tag` at the start of `input`, and
/// what follows it.
fn der(input: &[u8], tag: u8) -> Option<(&[u8], &[u8])> {
    let (&found, input) = input.split_first()?;
    if found != tag {
        return None;
    }
    let (&first, mut input) = input.split_first()?;
    let length = if first < 0x80 {
        usize::from(first)
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 2 || input.len() < count {
            return None;
        }
        let (bytes, rest) = input.split_at(count);
        input = rest;
        bytes
            .iter()
            .fold(0usize, |length, &b| (length << 8) | usize::from(b))
    };
    (input.len() >= length).then(|| input.split_at(length))
}
