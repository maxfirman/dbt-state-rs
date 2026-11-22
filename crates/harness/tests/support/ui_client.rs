//! The UI as a client of the Token broker: a confidential OIDC client
//! (`[clients.ui]`) that signs people in with the standard code flow
//! (`openid`, a nonce, PKCE S256) and authenticates at the token endpoint
//! with `private_key_jwt` (RFC 7523 §2.2, OIDC Core §9), never a secret.
//!
//! [`UiSignIn`] is one sign-in's authorize request; [`Assertion`] builds a
//! client assertion, signed with the UI's test key (`ui-client-key.pem`)
//! unless told otherwise. A well-formed assertion comes from the auth
//! crate's `ClientAssertionKey`, which the UI signs its own with.

use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use dbt_state_auth::{ClientAssertionKey, CLIENT_ASSERTION_TYPE};
use ring::rand::{SecureRandom, SystemRandom};
use ring::signature::RsaKeyPair;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::PrivatePkcs8KeyDer;
use serde_json::{json, Value};
use sha2::Digest;

/// The test config's `[clients.ui] client_id`.
pub const UI_CLIENT_ID: &str = "dbt-state-ui";
/// The test config's `[clients.ui] redirect_uri`: the UI's only callback.
pub const UI_REDIRECT_URI: &str = "https://console.dbt-state.test/auth/callback";

fn testdata(file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../auth/testdata")
        .join(file)
}

/// The `[clients.ui] public_key_file` of the test config.
pub fn ui_public_key_file() -> PathBuf {
    testdata("ui-client-public.pem")
}

fn random(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    SystemRandom::new().fill(&mut buf).expect("random");
    URL_SAFE_NO_PAD.encode(buf)
}

/// A UI sign-in's authorize request: `scope=openid email profile`, a
/// random `state` and `nonce`, and a PKCE S256 challenge.
#[derive(Debug, Clone)]
pub struct UiSignIn {
    pub client_id: String,
    pub redirect_uri: String,
    pub scope: String,
    pub state: String,
    pub nonce: Option<String>,
    pub verifier: String,
    pkce: bool,
}

impl Default for UiSignIn {
    fn default() -> Self {
        Self::new()
    }
}

impl UiSignIn {
    pub fn new() -> Self {
        Self {
            client_id: UI_CLIENT_ID.into(),
            redirect_uri: UI_REDIRECT_URI.into(),
            scope: "openid email profile".into(),
            state: random(16),
            nonce: Some(random(16)),
            verifier: random(32),
            pkce: true,
        }
    }

    pub fn client_id(mut self, client_id: &str) -> Self {
        self.client_id = client_id.into();
        self
    }

    pub fn redirect_uri(mut self, redirect_uri: &str) -> Self {
        self.redirect_uri = redirect_uri.into();
        self
    }

    pub fn scope(mut self, scope: &str) -> Self {
        self.scope = scope.into();
        self
    }

    pub fn without_nonce(mut self) -> Self {
        self.nonce = None;
        self
    }

    pub fn without_pkce(mut self) -> Self {
        self.pkce = false;
        self
    }

    /// The authorize URL at the broker's `issuer`.
    pub fn authorize_url(&self, issuer: &str) -> String {
        let mut query = form_urlencoded::Serializer::new(String::new());
        query
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.client_id)
            .append_pair("redirect_uri", &self.redirect_uri)
            .append_pair("scope", &self.scope)
            .append_pair("state", &self.state);
        if let Some(nonce) = &self.nonce {
            query.append_pair("nonce", nonce);
        }
        if self.pkce {
            let challenge = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(self.verifier.as_bytes()));
            query
                .append_pair("code_challenge", &challenge)
                .append_pair("code_challenge_method", "S256");
        }
        format!("{}/?{}", issuer.trim_end_matches('/'), query.finish())
    }
}

/// A well-formed client assertion for the audience `aud` (the broker's
/// issuer or token endpoint), signed as the UI signs it.
pub fn assertion(aud: &str) -> String {
    ClientAssertionKey::load(&testdata("ui-client-key.pem"), None)
        .expect("UI test key")
        .assertion(UI_CLIENT_ID, aud, SystemTime::now())
}

/// A client assertion with chosen claims and key, for refusals.
#[derive(Debug, Clone)]
pub struct Assertion {
    claims: serde_json::Map<String, Value>,
    alg: &'static str,
    key_file: &'static str,
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

impl Assertion {
    /// `iss` = `sub` = the UI client id, `aud`, `iat` now, `exp` in 5
    /// minutes, a random `jti`.
    pub fn new(aud: &str) -> Self {
        let now = unix_now();
        let Value::Object(claims) = json!({
            "iss": UI_CLIENT_ID,
            "sub": UI_CLIENT_ID,
            "aud": aud,
            "iat": now,
            "exp": now + 300,
            "jti": random(16),
        }) else {
            unreachable!()
        };
        Self {
            claims,
            alg: "RS256",
            key_file: "ui-client-key.pem",
        }
    }

    /// Set claim `name` to `value`.
    pub fn claim(mut self, name: &str, value: Value) -> Self {
        self.claims.insert(name.into(), value);
        self
    }

    /// Leave claim `name` out.
    pub fn without(mut self, name: &str) -> Self {
        self.claims.remove(name);
        self
    }

    /// `iat` `ago` seconds before now, and `exp` `lifetime` seconds after it.
    pub fn issued(self, ago: i64, lifetime: i64) -> Self {
        let iat = unix_now() - ago;
        self.claim("iat", json!(iat))
            .claim("exp", json!(iat + lifetime))
    }

    /// Signed with another key (one the broker never registered).
    pub fn signed_by_another_key(mut self) -> Self {
        self.key_file = "untrusted-key.pem";
        self
    }

    /// A JOSE header naming another algorithm (signed as RS256 anyway).
    pub fn alg(mut self, alg: &'static str) -> Self {
        self.alg = alg;
        self
    }

    pub fn sign(&self) -> String {
        let der = PrivatePkcs8KeyDer::from_pem_file(testdata(self.key_file)).expect("PKCS#8 key");
        let key = RsaKeyPair::from_pkcs8(der.secret_pkcs8_der()).expect("RSA key");
        let encode = |value: &Value| URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).unwrap());
        let input = format!(
            "{}.{}",
            encode(&json!({"alg": self.alg, "typ": "JWT"})),
            encode(&Value::Object(self.claims.clone()))
        );
        let mut signature = vec![0; key.public().modulus_len()];
        key.sign(
            &ring::signature::RSA_PKCS1_SHA256,
            &SystemRandom::new(),
            input.as_bytes(),
            &mut signature,
        )
        .expect("sign");
        format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature))
    }
}

/// POST a token request: `form` plus, when `assertion` is set, the
/// `private_key_jwt` parameters.
pub async fn token_request(
    broker: &str,
    form: &[(&str, &str)],
    assertion: Option<&str>,
) -> reqwest::Response {
    let mut form: Vec<(&str, &str)> = form.to_vec();
    if let Some(assertion) = assertion {
        form.push(("client_assertion_type", CLIENT_ASSERTION_TYPE));
        form.push(("client_assertion", assertion));
    }
    reqwest::Client::new()
        .post(format!("{broker}/token"))
        .form(&form)
        .send()
        .await
        .expect("token request")
}

/// POST the UI's `authorization_code` exchange of `code`, authenticated by
/// `assertion`.
pub async fn exchange(
    broker: &str,
    sign_in: &UiSignIn,
    code: &str,
    assertion: Option<&str>,
) -> reqwest::Response {
    token_request(
        broker,
        &[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", &sign_in.redirect_uri),
            ("code_verifier", &sign_in.verifier),
        ],
        assertion,
    )
    .await
}

/// POST the UI's `refresh_token` grant, authenticated by `assertion`.
pub async fn refresh(
    broker: &str,
    refresh_token: &str,
    assertion: Option<&str>,
) -> reqwest::Response {
    token_request(
        broker,
        &[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ],
        assertion,
    )
    .await
}
