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
//!   access token the test asked for;
//! - `GET /authorize`: an auto-approving OIDC authorization endpoint. The
//!   browser is already signed in as the person [`FakeIdp::sign_in`] named,
//!   so it redirects straight back to the relying party with a code (or
//!   `error=login_required` when nobody is signed in);
//! - `POST /token`, grant `authorization_code`: authenticates the Token
//!   broker (registered as [`BROKER_CLIENT_ID`]) by HTTP Basic
//!   [`BROKER_CLIENT_SECRET`] or a `private_key_jwt` assertion signed by
//!   `upstream-rp-key.pem`, checks the code's redirect URI and PKCE
//!   verifier, and returns an ID token (with the nonce of the authorize
//!   request), an access token and a refresh token.
//!
//! Tests register machines with the claims they carry and how their token
//! goes wrong ([`UpstreamMachine::audience`], [`UpstreamMachine::opaque`],
//! ...), and people likewise ([`UpstreamPerson::claim`],
//! [`UpstreamPerson::wrong_nonce`], ...), then read back what the broker
//! sent ([`FakeIdp::token_requests`], [`FakeIdp::authorize_requests`]) and
//! how often it fetched the keys ([`FakeIdp::jwks_fetches`]).
//!
//! Renewal (#221) extends the same state: the `refresh_token` grant can
//! look up [`IdpState::refresh_tokens`] and the current [`IdpState::people`]
//! (whose claims a test changes by signing the person in again, or
//! disables), and a userinfo route joins [`router`].

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
use sha2::Digest;

use super::http::HttpServer;

/// The `aud` the deployment's `[upstream] api_audience` names, and machine
/// tokens carry by default.
pub const UPSTREAM_AUDIENCE: &str = "api://dbt-state";
/// The `[upstream] client_credentials_scope` test deployments configure.
pub const UPSTREAM_SCOPE: &str = "api://dbt-state/.default";
/// The key id the fake IdP signs with and publishes.
pub const IDP_KEY_ID: &str = "idp-key-1";
/// The Token broker's client id at the fake IdP: `[upstream] client_id`.
pub const BROKER_CLIENT_ID: &str = "dbt-state-broker";
/// The Token broker's client secret at the fake IdP.
pub const BROKER_CLIENT_SECRET: &str = "broker-idp-secret-7Hq2";
/// The `client_assertion_type` of `private_key_jwt`.
const JWT_BEARER: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// A person the fake IdP can sign in.
#[derive(Debug, Clone)]
pub struct UpstreamPerson {
    subject: String,
    claims: Map<String, Value>,
    id_token: IdTokenShape,
    refresh_token: bool,
}

/// How the fake IdP's ID token for a person goes wrong, if at all.
#[derive(Debug, Clone, Default)]
struct IdTokenShape {
    missing: bool,
    wrong_nonce: bool,
    impostor: bool,
    audience: Option<Value>,
    issuer: Option<String>,
}

impl UpstreamPerson {
    /// A person with `sub` `subject`, `email` `<subject>@example.test` and
    /// a `name`.
    pub fn new(subject: &str) -> Self {
        let mut claims = Map::new();
        claims.insert("email".into(), json!(format!("{subject}@example.test")));
        claims.insert("name".into(), json!(format!("Person {subject}")));
        Self {
            subject: subject.into(),
            claims,
            id_token: IdTokenShape::default(),
            refresh_token: true,
        }
    }

    /// The ID token also carries `claim` = `value` (replacing `email` or
    /// `name`); `Value::Null` removes the claim.
    pub fn claim(mut self, claim: &str, value: Value) -> Self {
        if value.is_null() {
            self.claims.remove(claim);
        } else {
            self.claims.insert(claim.into(), value);
        }
        self
    }

    /// The ID token's `sub`.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// The token response has no ID token.
    pub fn without_id_token(mut self) -> Self {
        self.id_token.missing = true;
        self
    }

    /// The ID token carries a nonce other than the one requested.
    pub fn wrong_nonce(mut self) -> Self {
        self.id_token.wrong_nonce = true;
        self
    }

    /// The ID token is signed by a key the IdP never published.
    pub fn signed_by_impostor(mut self) -> Self {
        self.id_token.impostor = true;
        self
    }

    /// The ID token's `aud`, instead of the client id.
    pub fn audience(mut self, audience: Value) -> Self {
        self.id_token.audience = Some(audience);
        self
    }

    /// The ID token's `iss`, instead of the IdP's own.
    pub fn issuer(mut self, issuer: &str) -> Self {
        self.id_token.issuer = Some(issuer.into());
        self
    }

    /// The token response has no refresh token.
    pub fn without_refresh_token(mut self) -> Self {
        self.refresh_token = false;
        self
    }
}

/// An authorization code the fake IdP issued.
#[derive(Debug, Clone)]
struct IssuedCode {
    subject: String,
    client_id: String,
    redirect_uri: String,
    nonce: Option<String>,
    code_challenge: String,
}

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
pub struct IdpState {
    issuer: String,
    /// Discovery members that replace the IdP's own.
    advertised: Map<String, Value>,
    machines: BTreeMap<String, UpstreamMachine>,
    token_requests: Vec<UpstreamTokenRequest>,
    jwks_fetches: usize,
    /// Every person signed in so far, by subject, as last signed in.
    pub people: BTreeMap<String, UpstreamPerson>,
    /// The subject of the person the browser is signed in as.
    signed_in: Option<String>,
    authorize_requests: Vec<BTreeMap<String, String>>,
    codes: BTreeMap<String, IssuedCode>,
    /// Every refresh token issued, with its person's subject.
    pub refresh_tokens: BTreeMap<String, String>,
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
    /// The public half of the Token broker's `private_key_jwt` key.
    broker: RsaPublicKeyComponents<Vec<u8>>,
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

    /// The browser is signed in at the IdP as `person` (replacing any
    /// person with its subject): the next authorize request is approved for
    /// them.
    pub fn sign_in(&self, person: UpstreamPerson) -> &Self {
        let mut state = self.state.lock().unwrap();
        state.signed_in = Some(person.subject.clone());
        state.people.insert(person.subject.clone(), person);
        self
    }

    /// The query of every request its authorization endpoint received.
    pub fn authorize_requests(&self) -> Vec<BTreeMap<String, String>> {
        self.state.lock().unwrap().authorize_requests.clone()
    }

    /// Every refresh token it issued.
    pub fn issued_refresh_tokens(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .refresh_tokens
            .keys()
            .cloned()
            .collect()
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
        broker: RsaPublicKeyComponents::<Vec<u8>>::from(key_pair("upstream-rp-key.pem").public()),
    });
    Router::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/jwks", get(jwks))
        .route("/authorize", get(authorize))
        .route("/token", post(token))
        .with_state((state, keys))
}

type AppState = (Shared, Arc<Keys>);

async fn discovery(State((state, _)): State<AppState>) -> Json<Value> {
    let state = state.lock().unwrap();
    let issuer = state.issuer.clone();
    let mut discovery = json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/authorize"),
        "token_endpoint": format!("{issuer}/token"),
        "jwks_uri": format!("{issuer}/jwks"),
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "grant_types_supported": ["authorization_code", "client_credentials", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["client_secret_basic", "private_key_jwt"],
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
    match form.get("grant_type").map(String::as_str) {
        Some("client_credentials") => {}
        Some("authorization_code") => return code_grant(&mut state, &keys, &form, basic),
        _ => return oauth_error(StatusCode::BAD_REQUEST, "unsupported_grant_type"),
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

/// `GET /authorize`: approve the signed-in person at once.
async fn authorize(State((state, _)): State<AppState>, uri: axum::http::Uri) -> Response {
    let query: BTreeMap<String, String> =
        form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .into_owned()
            .collect();
    let mut state = state.lock().unwrap();
    state.authorize_requests.push(query.clone());
    let get = |key: &str| query.get(key).map(String::as_str);
    let (Some(BROKER_CLIENT_ID), Some(redirect_uri)) = (get("client_id"), get("redirect_uri"))
    else {
        return (StatusCode::BAD_REQUEST, "unknown client").into_response();
    };
    let back = |params: &[(&str, &str)]| {
        let mut query = form_urlencoded::Serializer::new(String::new());
        for (key, value) in params {
            query.append_pair(key, value);
        }
        if let Some(state) = get("state") {
            query.append_pair("state", state);
        }
        (
            StatusCode::FOUND,
            [("location", format!("{redirect_uri}?{}", query.finish()))],
        )
            .into_response()
    };
    let openid = get("scope").is_some_and(|scope| scope.split(' ').any(|s| s == "openid"));
    let pkce = get("code_challenge_method") == Some("S256");
    if get("response_type") != Some("code") || !openid || !pkce {
        return back(&[("error", "invalid_request")]);
    }
    let Some(subject) = state.signed_in.clone() else {
        return back(&[("error", "login_required")]);
    };
    let code = format!("idp-code-{}", uuid::Uuid::new_v4().simple());
    state.codes.insert(
        code.clone(),
        IssuedCode {
            subject,
            client_id: BROKER_CLIENT_ID.into(),
            redirect_uri: redirect_uri.into(),
            nonce: get("nonce").map(Into::into),
            code_challenge: get("code_challenge").unwrap_or_default().into(),
        },
    );
    back(&[("code", &code)])
}

/// Whether `assertion` is a `private_key_jwt` assertion by the Token broker
/// for this IdP's token endpoint, current, and signed by its key.
fn broker_assertion(issuer: &str, keys: &Keys, assertion: &str) -> bool {
    let mut parts = assertion.split('.');
    let (Some(header), Some(payload), Some(signature)) = (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let Ok(signature) = URL_SAFE_NO_PAD.decode(signature) else {
        return false;
    };
    let input = format!("{header}.{payload}");
    if keys
        .broker
        .verify(
            &ring::signature::RSA_PKCS1_2048_8192_SHA256,
            input.as_bytes(),
            &signature,
        )
        .is_err()
    {
        return false;
    }
    let Some(claims) = URL_SAFE_NO_PAD
        .decode(payload)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
    else {
        return false;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    claims["iss"] == BROKER_CLIENT_ID
        && claims["sub"] == BROKER_CLIENT_ID
        && claims["aud"] == format!("{issuer}/token")
        && claims["jti"].is_string()
        && claims["exp"].as_u64().is_some_and(|exp| exp > now)
}

/// `POST /token`, grant `authorization_code`, for the Token broker.
fn code_grant(
    state: &mut IdpState,
    keys: &Keys,
    form: &BTreeMap<String, String>,
    basic: Option<(String, String)>,
) -> Response {
    let get = |key: &str| form.get(key).map(String::as_str);
    let authenticated = match get("client_assertion_type") {
        Some(JWT_BEARER) => {
            get("client_id").is_none_or(|id| id == BROKER_CLIENT_ID)
                && get("client_assertion")
                    .is_some_and(|assertion| broker_assertion(&state.issuer, keys, assertion))
        }
        _ => basic == Some((BROKER_CLIENT_ID.into(), BROKER_CLIENT_SECRET.into())),
    };
    if !authenticated {
        return oauth_error(StatusCode::UNAUTHORIZED, "invalid_client");
    }
    let Some(code) = get("code").and_then(|code| state.codes.remove(code)) else {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
    };
    let verifier = get("code_verifier").unwrap_or_default();
    let challenge = URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes()));
    if get("redirect_uri") != Some(code.redirect_uri.as_str()) || challenge != code.code_challenge {
        return oauth_error(StatusCode::BAD_REQUEST, "invalid_grant");
    }
    let person = state.people[&code.subject].clone();
    let shape = &person.id_token;
    let iat = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut claims = person.claims.clone();
    claims.insert(
        "iss".into(),
        json!(shape.issuer.clone().unwrap_or_else(|| state.issuer.clone())),
    );
    claims.insert("sub".into(), json!(person.subject));
    claims.insert(
        "aud".into(),
        shape.audience.clone().unwrap_or(json!(code.client_id)),
    );
    claims.insert("iat".into(), json!(iat));
    claims.insert("exp".into(), json!(iat + 3600));
    if let Some(nonce) = &code.nonce {
        let nonce = if shape.wrong_nonce {
            format!("not-{nonce}")
        } else {
            nonce.clone()
        };
        claims.insert("nonce".into(), json!(nonce));
    }
    let key = if shape.impostor {
        &keys.impostor
    } else {
        &keys.published
    };
    let mut body = json!({
        "access_token": format!("idp-at-{}", uuid::Uuid::new_v4().simple()),
        "token_type": "Bearer",
        "expires_in": 3600,
    });
    if !shape.missing {
        body["id_token"] = json!(sign(key, &Value::Object(claims)));
    }
    if person.refresh_token {
        let refresh_token = format!("idp-rt-{}", uuid::Uuid::new_v4().simple());
        state
            .refresh_tokens
            .insert(refresh_token.clone(), person.subject.clone());
        body["refresh_token"] = json!(refresh_token);
    }
    Json(body).into_response()
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
