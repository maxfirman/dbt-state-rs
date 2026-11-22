//! The Token broker's own credential at the Upstream IdP, for browser
//! sign-in: `private_key_jwt` (RFC 7523 §2.2, OIDC Core §9) when the IdP
//! supports it, otherwise a client secret sent as HTTP Basic (RFC 6749
//! §2.3.1). Both come from mounted files named in deployment config and are
//! never stored in the database or shown by `Debug`.

use std::fmt;
use std::time::{Duration, SystemTime};

use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use ring::rand::{SecureRandom, SystemRandom};
use serde::Serialize;

use crate::keys::SigningKey;
use crate::token::unix_seconds;

/// The `client_assertion_type` of a `private_key_jwt` assertion.
pub const CLIENT_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// How long a client assertion is valid.
const ASSERTION_LIFETIME: Duration = Duration::from_secs(300);

/// See the module docs.
#[derive(Debug, Clone)]
pub enum UpstreamCredential {
    ClientSecret(ClientSecret),
    PrivateKeyJwt(ClientAssertionKey),
}

impl UpstreamCredential {
    /// The token endpoint authentication method (OIDC Core §9).
    pub fn method(&self) -> &'static str {
        match self {
            Self::ClientSecret(_) => "client_secret_basic",
            Self::PrivateKeyJwt(_) => "private_key_jwt",
        }
    }
}

/// A client secret. `Debug` never shows it.
#[derive(Clone)]
pub struct ClientSecret(String);

impl fmt::Debug for ClientSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClientSecret(<redacted>)")
    }
}

impl ClientSecret {
    pub(crate) fn new(secret: String) -> Self {
        Self(secret)
    }

    /// The HTTP Basic `authorization` value for `client_id` and this
    /// secret, each form-urlencoded first (RFC 6749 §2.3.1).
    pub fn basic_authorization(&self, client_id: &str) -> String {
        let encode =
            |value: &str| form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>();
        let pair = format!("{}:{}", encode(client_id), encode(&self.0));
        format!("Basic {}", STANDARD.encode(pair))
    }
}

/// The RSA key the Token broker signs `private_key_jwt` assertions with.
#[derive(Debug, Clone)]
pub struct ClientAssertionKey {
    key: SigningKey,
    /// The `kid` the IdP knows the key by, if it needs one.
    kid: Option<String>,
}

#[derive(Serialize)]
struct Header<'a> {
    alg: &'static str,
    typ: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    kid: Option<&'a str>,
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    sub: &'a str,
    aud: &'a str,
    jti: String,
    iat: u64,
    exp: u64,
}

impl ClientAssertionKey {
    pub(crate) fn new(key: SigningKey, kid: Option<String>) -> Self {
        Self { key, kid }
    }

    /// A signed assertion that `client_id` is authenticating to the token
    /// endpoint `audience` at `now`: `iss` and `sub` the client id, a
    /// random `jti`, valid for five minutes.
    pub fn assertion(&self, client_id: &str, audience: &str, now: SystemTime) -> String {
        let mut jti = [0u8; 16];
        SystemRandom::new()
            .fill(&mut jti)
            .expect("the system random source");
        let iat = unix_seconds(now);
        let header = Header {
            alg: "RS256",
            typ: "JWT",
            kid: self.kid.as_deref(),
        };
        let claims = Claims {
            iss: client_id,
            sub: client_id,
            aud: audience,
            jti: URL_SAFE_NO_PAD.encode(jti),
            iat,
            exp: iat + ASSERTION_LIFETIME.as_secs(),
        };
        let input = format!("{}.{}", encode_json(&header), encode_json(&claims));
        let signature = URL_SAFE_NO_PAD.encode(self.key.sign(input.as_bytes()));
        format!("{input}.{signature}")
    }
}

fn encode_json(value: &impl Serialize) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).expect("assertion JSON"))
}
