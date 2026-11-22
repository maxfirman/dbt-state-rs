//! Token broker requests exactly as the pinned dbt clients send them.
//!
//! - `client_credentials`: HTTP Basic `client_id:secret` and the form
//!   `grant_type=client_credentials&scope=runcache:scope:orgs`
//!   (`dbt-fusion@9977b6c` `crates/dbt-run-cache/src/auth/oauth.rs`
//!   `fetch_client_credentials`; `dbt-state@b0f3030`
//!   `clients/dbt_state/src/dbt_state/auth/sso.py`
//!   `_login_with_client_credentials`, authlib's default
//!   `client_secret_basic` with the session scope `runcache:scope:orgs`).
//! - token exchange: the form Fusion's `fetch_platform_token_exchange` posts
//!   when a dbt platform credential is found.
//! - browser sign-in ([`SignIn`]): the authorize URL Fusion's
//!   `build_authorize_url` makes (`crates/dbt-run-cache/src/auth/
//!   browser_flow.rs`), and the code exchange each client posts: Fusion's
//!   `BrowserFlow::run` form ([`authorization_code`]), and the Python
//!   client's authlib `fetch_token(..., include_client_id=True)` body with
//!   no client secret ([`authorization_code_python`]; `dbt_state/auth/
//!   sso.py` `_login`).
//! - renewal ([`refresh_token`]): both clients post
//!   `grant_type=refresh_token&refresh_token=<token>&client_id=<id>` with no
//!   client authentication and no scope (Fusion `oauth.rs` `fetch_refresh`;
//!   the Python client's authlib `OAuth2Session.refresh_token(token_url,
//!   refresh_token=..., scope=None)` in `sso.py` `_refresh_token`, whose
//!   `none` client auth appends `client_id`).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use ring::rand::{SecureRandom, SystemRandom};
use serde_json::{Map, Value};
use sha2::Digest;

/// The scope both clients request.
pub const ORGS_SCOPE: &str = "runcache:scope:orgs";
/// Both clients' built-in OAuth client id (`dbt-state==2.62.6`
/// `dbt_state/config.py` `CLIENT_ID_DEFAULT`; `dbt-fusion@9977b6c`
/// `crates/dbt-run-cache/src/service_config.rs` `DEFAULT_OAUTH_CLIENT_ID`).
pub const DBT_CLIENT_ID: &str = "2fd87cd5-69a6-4c5f-9097-747a58f0edf6";
/// Both clients' loopback redirect URI (Fusion `browser_flow.rs`
/// `LOOPBACK_PORT`; Python `sso.py` `REDIRECT_URI`).
pub const LOOPBACK_REDIRECT_URI: &str = "http://127.0.0.1:29525/handler";

fn random(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    SystemRandom::new().fill(&mut buf).expect("random");
    URL_SAFE_NO_PAD.encode(buf)
}

/// A browser sign-in as the dbt client starts it: a 32-byte PKCE verifier
/// and its S256 challenge, a 16-byte `state` (Fusion `generate_pkce`,
/// `generate_state`).
#[derive(Debug, Clone)]
pub struct SignIn {
    pub client_id: String,
    pub redirect_uri: String,
    pub state: String,
    pub verifier: String,
    challenge: Option<(String, String)>,
}

impl Default for SignIn {
    fn default() -> Self {
        Self::new()
    }
}

impl SignIn {
    pub fn new() -> Self {
        let verifier = random(32);
        let challenge = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes()));
        Self {
            client_id: DBT_CLIENT_ID.into(),
            redirect_uri: LOOPBACK_REDIRECT_URI.into(),
            state: random(16),
            verifier,
            challenge: Some((challenge, "S256".into())),
        }
    }

    /// Another client id.
    pub fn client_id(mut self, client_id: &str) -> Self {
        self.client_id = client_id.into();
        self
    }

    /// Another redirect URI.
    pub fn redirect_uri(mut self, redirect_uri: &str) -> Self {
        self.redirect_uri = redirect_uri.into();
        self
    }

    /// No PKCE parameters.
    pub fn without_pkce(mut self) -> Self {
        self.challenge = None;
        self
    }

    /// PKCE `plain`: the verifier itself as the challenge.
    pub fn plain_pkce(mut self) -> Self {
        self.challenge = Some((self.verifier.clone(), "plain".into()));
        self
    }

    /// The URL the client opens in the browser: its auth URL with the
    /// parameters Fusion's `build_authorize_url` appends, in its order.
    pub fn authorize_url(&self, auth_url: &str) -> String {
        let mut query = form_urlencoded::Serializer::new(String::new());
        query
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.client_id)
            .append_pair("redirect_uri", &self.redirect_uri)
            .append_pair("scope", ORGS_SCOPE)
            .append_pair("state", &self.state);
        if let Some((challenge, method)) = &self.challenge {
            query
                .append_pair("code_challenge", challenge)
                .append_pair("code_challenge_method", method);
        }
        format!("{}/?{}", auth_url.trim_end_matches('/'), query.finish())
    }
}

/// POST Fusion's `authorization_code` exchange of `code`.
pub async fn authorization_code(broker: &str, sign_in: &SignIn, code: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{broker}/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", sign_in.redirect_uri.as_str()),
            ("client_id", sign_in.client_id.as_str()),
            ("code_verifier", sign_in.verifier.as_str()),
        ])
        .send()
        .await
        .expect("token request")
}

/// POST the Python client's `authorization_code` exchange of `code`:
/// authlib's body, which also carries `include_client_id=True`.
pub async fn authorization_code_python(
    broker: &str,
    sign_in: &SignIn,
    code: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{broker}/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("redirect_uri", sign_in.redirect_uri.as_str()),
            ("code_verifier", sign_in.verifier.as_str()),
            ("include_client_id", "True"),
            ("code", code),
            ("client_id", sign_in.client_id.as_str()),
        ])
        .send()
        .await
        .expect("token request")
}

/// POST the `refresh_token` grant both clients send.
pub async fn refresh_token(
    broker: &str,
    client_id: &str,
    refresh_token: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{broker}/token"))
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", client_id),
        ])
        .send()
        .await
        .expect("token request")
}

/// POST `client_credentials` to the broker's token endpoint at `broker`.
pub async fn client_credentials(broker: &str, client_id: &str, secret: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{broker}/token"))
        .basic_auth(client_id, Some(secret))
        .form(&[("grant_type", "client_credentials"), ("scope", ORGS_SCOPE)])
        .send()
        .await
        .expect("token request")
}

/// POST Fusion's dbt platform token exchange.
pub async fn token_exchange(
    broker: &str,
    client_id: &str,
    platform_token: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{broker}/token"))
        .form(&[
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            ("subject_token_type", "dbt"),
            ("subject_token", platform_token),
            ("dbt_hostname", "ab123.us1.dbt.com"),
            ("client_id", client_id),
        ])
        .send()
        .await
        .expect("token request")
}

/// The JSON object a response carries.
pub async fn json(response: reqwest::Response) -> Map<String, Value> {
    match response.json::<Value>().await.expect("JSON body") {
        Value::Object(body) => body,
        other => panic!("not a JSON object: {other}"),
    }
}

/// The claims of a JWT, read without verifying it (as both clients read
/// their `id_token`).
pub fn claims(token: &str) -> Map<String, Value> {
    let payload = token.split('.').nth(1).expect("a JWT");
    serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).expect("base64url payload"))
        .expect("JSON claims")
}
