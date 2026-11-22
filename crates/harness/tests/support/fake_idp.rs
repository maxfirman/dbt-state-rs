//! A fake OIDC Upstream IdP for Token broker tests: no Docker, controlled
//! by the test.
//!
//! It serves, on a loopback port:
//! - `GET /.well-known/openid-configuration`: OIDC discovery naming the
//!   endpoints below, with [`FakeIdp::issuer`] as `issuer`;
//! - `GET /jwks`: its one RS256 signing key (`upstream-idp-key.pem`, kid
//!   [`IDP_KEY_ID`]);
//! - `POST /token`, grant `client_credentials`: authenticates a registered
//!   [`UpstreamMachine`] by HTTP Basic, as a real IdP would, and returns the
//!   access token the test asked for.
//!
//! Tests register machines with the claims they carry and how their token
//! goes wrong ([`UpstreamMachine::audience`], [`UpstreamMachine::opaque`],
//! ...), then read back what the broker sent ([`FakeIdp::token_requests`])
//! and how often it fetched the keys ([`FakeIdp::jwks_fetches`]).
//!
//! Later flows extend the same state: people (authorize and callback),
//! refresh, userinfo and user disabling add routes to [`router`] and
//! entries to [`IdpState`], without changing the machine API.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use ring::rand::SystemRandom;
use ring::signature::{RsaKeyPair, RsaPublicKeyComponents};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::PrivatePkcs8KeyDer;
use serde_json::{json, Map, Value};

use super::http::HttpServer;

/// The `aud` the deployment's `[upstream] api_audience` names, and machine
/// tokens carry by default.
pub const UPSTREAM_AUDIENCE: &str = "api://dbt-state";
/// The `[upstream] client_credentials_scope` test deployments configure.
pub const UPSTREAM_SCOPE: &str = "api://dbt-state/.default";
/// The key id the fake IdP signs with and publishes.
pub const IDP_KEY_ID: &str = "idp-key-1";

/// A machine (app registration) the fake IdP knows.
#[derive(Debug, Clone)]
pub struct UpstreamMachine {
    client_id: String,
    secret: String,
    subject: String,
    claims: Map<String, Value>,
    audience: Value,
    issuer: Option<String>,
    shape: TokenShape,
    lifetime: Duration,
    issued_ago: Duration,
}

/// How the fake IdP's access token for a machine is made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenShape {
    /// An RS256 JWT signed with the published key.
    Signed,
    /// An opaque reference token.
    Opaque,
    /// An RS256 JWT signed with a key the IdP never published, under the
    /// published key id.
    Impostor,
}

impl UpstreamMachine {
    /// A machine authenticating with `client_id` and `secret`. Its token
    /// carries `sub` `sp-<client_id>`, `azp` `client_id`, and the
    /// [`UPSTREAM_AUDIENCE`].
    pub fn new(client_id: &str, secret: &str) -> Self {
        Self {
            client_id: client_id.into(),
            secret: secret.into(),
            subject: format!("sp-{client_id}"),
            claims: Map::new(),
            audience: json!(UPSTREAM_AUDIENCE),
            issuer: None,
            shape: TokenShape::Signed,
            lifetime: Duration::from_secs(3600),
            issued_ago: Duration::ZERO,
        }
    }

    /// The token also carries `claim` = `value` (a string, or an array such
    /// as `roles`).
    pub fn claim(mut self, claim: &str, value: Value) -> Self {
        self.claims.insert(claim.into(), value);
        self
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// The token's `sub`.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// The token's `aud` (a string or an array).
    pub fn audience(mut self, audience: Value) -> Self {
        self.audience = audience;
        self
    }

    /// The token's `iss`, instead of the IdP's own.
    pub fn issuer(mut self, issuer: &str) -> Self {
        self.issuer = Some(issuer.into());
        self
    }

    /// The IdP returns an opaque access token.
    pub fn opaque(mut self) -> Self {
        self.shape = TokenShape::Opaque;
        self
    }

    /// The token is signed by a key the IdP never published.
    pub fn signed_by_impostor(mut self) -> Self {
        self.shape = TokenShape::Impostor;
        self
    }

    /// The token expired before the IdP returned it.
    pub fn expired(mut self) -> Self {
        self.issued_ago = self.lifetime + Duration::from_secs(300);
        self
    }
}

/// A request the fake IdP's token endpoint received.
#[derive(Debug, Clone)]
pub struct UpstreamTokenRequest {
    /// The form body, by parameter.
    pub form: BTreeMap<String, String>,
    /// The HTTP Basic client id and secret, percent-decoded as RFC 6749
    /// §2.3.1 says, if the request carried them.
    pub basic: Option<(String, String)>,
}

impl UpstreamTokenRequest {
    pub fn grant_type(&self) -> Option<&str> {
        self.form.get("grant_type").map(String::as_str)
    }

    pub fn scope(&self) -> Option<&str> {
        self.form.get("scope").map(String::as_str)
    }
}

/// What the fake IdP knows and has seen.
#[derive(Default)]
struct IdpState {
    issuer: String,
    /// Discovery members that replace the IdP's own.
    advertised: Map<String, Value>,
    machines: BTreeMap<String, UpstreamMachine>,
    token_requests: Vec<UpstreamTokenRequest>,
    jwks_fetches: usize,
}

/// See the module docs. Dropping it stops the IdP.
pub struct FakeIdp {
    state: Arc<Mutex<IdpState>>,
    server: HttpServer,
}

fn testdata(file: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../auth/testdata")
        .join(file)
}

fn key_pair(file: &str) -> RsaKeyPair {
    let der = PrivatePkcs8KeyDer::from_pem_file(testdata(file)).expect("PKCS#8 test key");
    RsaKeyPair::from_pkcs8(der.secret_pkcs8_der()).expect("RSA test key")
}

struct Keys {
    published: RsaKeyPair,
    impostor: RsaKeyPair,
}

impl FakeIdp {
    pub async fn start() -> Self {
        let state = Arc::new(Mutex::new(IdpState::default()));
        let listener = HttpServer::bind();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        state.lock().unwrap().issuer = issuer;
        let shared = Arc::clone(&state);
        let server = HttpServer::start(listener, move || router(shared));
        Self { state, server }
    }

    /// The IdP's issuer (its base URL): `[upstream] issuer`.
    pub fn issuer(&self) -> String {
        self.server.url()
    }

    /// Register `machine`, replacing any with its client id.
    pub fn machine(&self, machine: UpstreamMachine) -> &Self {
        self.state
            .lock()
            .unwrap()
            .machines
            .insert(machine.client_id.clone(), machine);
        self
    }

    /// Discovery names `url` as its `member` (for example
    /// `token_endpoint`) instead of the IdP's own endpoint.
    pub fn advertise(&self, member: &str, url: &str) -> &Self {
        self.state
            .lock()
            .unwrap()
            .advertised
            .insert(member.into(), json!(url));
        self
    }

    /// Every request its token endpoint received, in order.
    pub fn token_requests(&self) -> Vec<UpstreamTokenRequest> {
        self.state.lock().unwrap().token_requests.clone()
    }

    /// How often its JWKS was fetched.
    pub fn jwks_fetches(&self) -> usize {
        self.state.lock().unwrap().jwks_fetches
    }
}

type Shared = Arc<Mutex<IdpState>>;

fn router(state: Shared) -> Router {
    let keys = Arc::new(Keys {
        published: key_pair("upstream-idp-key.pem"),
        impostor: key_pair("untrusted-key.pem"),
    });
    Router::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/jwks", get(jwks))
        .route("/token", post(token))
        .with_state((state, keys))
}

type AppState = (Shared, Arc<Keys>);

async fn discovery(State((state, _)): State<AppState>) -> Json<Value> {
    let state = state.lock().unwrap();
    let issuer = state.issuer.clone();
    let mut discovery = json!({
        "issuer": issuer,
        "token_endpoint": format!("{issuer}/token"),
        "jwks_uri": format!("{issuer}/jwks"),
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "grant_types_supported": ["client_credentials"],
        "token_endpoint_auth_methods_supported": ["client_secret_basic"],
    });
    for (member, value) in &state.advertised {
        discovery[member] = value.clone();
    }
    Json(discovery)
}

async fn jwks(State((state, keys)): State<AppState>) -> Json<Value> {
    state.lock().unwrap().jwks_fetches += 1;
    let public = RsaPublicKeyComponents::<Vec<u8>>::from(keys.published.public());
    Json(json!({"keys": [{
        "kty": "RSA",
        "use": "sig",
        "alg": "RS256",
        "kid": IDP_KEY_ID,
        "n": URL_SAFE_NO_PAD.encode(&public.n),
        "e": URL_SAFE_NO_PAD.encode(&public.e),
    }]}))
}

fn oauth_error(status: StatusCode, error: &str) -> Response {
    (status, Json(json!({ "error": error }))).into_response()
}

fn percent_decode(value: &str) -> String {
    form_urlencoded::parse(format!("v={}", value.replace('+', "%2B")).as_bytes())
        .next()
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default()
}

fn basic_credentials(headers: &HeaderMap) -> Option<(String, String)> {
    let value = headers.get("authorization")?.to_str().ok()?;
    let (scheme, encoded) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }
    let decoded = String::from_utf8(STANDARD.decode(encoded.trim()).ok()?).ok()?;
    let (id, secret) = decoded.split_once(':')?;
    Some((percent_decode(id), percent_decode(secret)))
}

async fn token(
    State((state, keys)): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let form: BTreeMap<String, String> = form_urlencoded::parse(&body).into_owned().collect();
    let basic = basic_credentials(&headers);
    let mut state = state.lock().unwrap();
    state.token_requests.push(UpstreamTokenRequest {
        form: form.clone(),
        basic: basic.clone(),
    });
    if form.get("grant_type").map(String::as_str) != Some("client_credentials") {
        return oauth_error(StatusCode::BAD_REQUEST, "unsupported_grant_type");
    }
    let Some((client_id, secret)) = basic else {
        return oauth_error(StatusCode::UNAUTHORIZED, "invalid_client");
    };
    let Some(machine) = state
        .machines
        .get(&client_id)
        .filter(|machine| machine.secret == secret)
    else {
        return oauth_error(StatusCode::UNAUTHORIZED, "invalid_client");
    };
    let access_token = match machine.shape {
        TokenShape::Opaque => format!("opaque-{}", uuid::Uuid::new_v4().simple()),
        shape => {
            let key = match shape {
                TokenShape::Impostor => &keys.impostor,
                _ => &keys.published,
            };
            let iat = SystemTime::now() - machine.issued_ago;
            let iat = iat.duration_since(UNIX_EPOCH).unwrap().as_secs();
            let mut claims = machine.claims.clone();
            claims.insert(
                "iss".into(),
                json!(machine
                    .issuer
                    .clone()
                    .unwrap_or_else(|| state.issuer.clone())),
            );
            claims.insert("aud".into(), machine.audience.clone());
            claims.insert("sub".into(), json!(machine.subject));
            claims
                .entry("azp")
                .or_insert_with(|| json!(machine.client_id));
            claims.insert("iat".into(), json!(iat));
            claims.insert("nbf".into(), json!(iat));
            claims.insert("exp".into(), json!(iat + machine.lifetime.as_secs()));
            sign(key, &Value::Object(claims))
        }
    };
    Json(json!({
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": machine.lifetime.as_secs(),
    }))
    .into_response()
}

/// A compact RS256 JWS of `claims`, under [`IDP_KEY_ID`].
fn sign(key: &RsaKeyPair, claims: &Value) -> String {
    let header = json!({"alg": "RS256", "typ": "JWT", "kid": IDP_KEY_ID});
    let encode = |value: &Value| URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).unwrap());
    let input = format!("{}.{}", encode(&header), encode(claims));
    let mut signature = vec![0; key.public().modulus_len()];
    key.sign(
        &ring::signature::RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        input.as_bytes(),
        &mut signature,
    )
    .expect("RS256 signature");
    format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature))
}
