//! `oidc` mode test support: an auth config over the test-only signing keys
//! (`crates/auth/testdata/`), a test signer that issues tokens in the Token
//! broker's shape, and a capture of the server's logs.
//!
//! [`TestAuth`] writes a real TOML config and loads it with
//! `AuthConfig::load`, so tests exercise the operator's path. The server
//! started by `start_server_oidc` checks tokens against that config. A
//! [`TestSigner`] signs with the config's active key by default; one built
//! with [`SignerOptions`] can sign as another issuer, for another audience,
//! with another key, or from another view of the Organizations (for
//! example, one from before an Organization was disabled).

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use dbt_state_auth::{
    AuthConfig, OrganizationId, PrincipalId, Role, Scope, TokenIssuer, TokenRequest,
};

use super::fake_idp::{
    FakeIdp, BROKER_CLIENT_ID, BROKER_CLIENT_SECRET, UPSTREAM_AUDIENCE, UPSTREAM_SCOPE,
};

/// The test config's `broker.issuer`.
pub const ISSUER: &str = "https://broker.dbt-state.test";
/// The test config's `api.audience`.
pub const AUDIENCE: &str = "dbt-state-api";
/// The dbt client id test tokens are issued to (`azp`).
pub const CLIENT_ID: &str = "test-dbt-client";

/// A signing key a [`TestSigner`] can use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestKey {
    /// `test-key-1`, the server config's active key.
    Active,
    /// `test-key-2`, also loaded by the server config.
    Second,
    /// A key the server never loaded, presented as `test-key-1`.
    Impostor,
    /// A key the server never loaded, under a key id it does not know.
    Unknown,
}

impl TestKey {
    fn id_and_file(self) -> (&'static str, &'static str) {
        match self {
            Self::Active => ("test-key-1", "signing-key-1.pem"),
            Self::Second => ("test-key-2", "signing-key-2.pem"),
            Self::Impostor => ("test-key-1", "untrusted-key.pem"),
            Self::Unknown => ("test-key-unknown", "untrusted-key.pem"),
        }
    }
}

fn testdata(file: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../auth/testdata")
        .join(file)
}

fn toml_string(value: &str) -> String {
    format!("{value:?}")
}

/// Organizations as `(id, disabled)`.
pub type Declared<'a> = &'a [(&'a str, bool)];

/// The test refresh-token encryption key (`openssl rand -base64 32`).
const REFRESH_TOKEN_KEY: &str = "3m2pQm0x1i4lL6m6c8iCqv0wN3m8z5k2Jg7Yt1yH4aE=";
/// Files a test config with an Upstream IdP names, written next to it.
const SECRET_FILES: &[(&str, &str)] = &[
    ("upstream-client-secret", BROKER_CLIENT_SECRET),
    ("refresh-token.key", REFRESH_TOKEN_KEY),
];

/// The TOML text of a config. `keys` empty leaves out `[signing]`;
/// `broker` is added to `[broker]`, and `extra` appended (for example
/// `[upstream]` and `[[rules]]`).
fn config_text(
    issuer: &str,
    audience: &str,
    keys: &[TestKey],
    organizations: Declared,
    tokens: &str,
    broker: &str,
    extra: &str,
) -> String {
    let mut text = format!(
        "[broker]\nissuer = {}\n{broker}\n[api]\naudience = {}\n\n{tokens}\n",
        toml_string(issuer),
        toml_string(audience),
    );
    if let Some(active) = keys.first() {
        text.push_str(&format!(
            "\n[signing]\nactive_key_id = {}\n",
            toml_string(active.id_and_file().0)
        ));
    }
    for key in keys {
        let (id, file) = key.id_and_file();
        text.push_str(&format!(
            "\n[[signing.keys]]\nid = {}\nprivate_key_file = {}\n",
            toml_string(id),
            toml_string(testdata(file).to_str().expect("UTF-8 path")),
        ));
    }
    for (id, disabled) in organizations {
        text.push_str(&format!(
            "\n[[organizations]]\nid = {}\ndisabled = {disabled}\n",
            toml_string(id)
        ));
    }
    text.push_str(extra);
    text
}

/// Load `text` through a config file, as the server binary does, with the
/// secret files it may name beside it.
fn load(text: &str) -> AuthConfig {
    let dir = tempfile::tempdir().expect("config dir");
    for (file, contents) in SECRET_FILES {
        std::fs::write(dir.path().join(file), contents).expect("write secret file");
    }
    let path = dir.path().join("auth.toml");
    std::fs::write(&path, text).expect("write config");
    AuthConfig::load(&path).expect("valid test auth config")
}

/// How the Token broker authenticates at the fake Upstream IdP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerCredential {
    /// [`BROKER_CLIENT_SECRET`], from a mounted file.
    ClientSecret,
    /// `private_key_jwt` with `upstream-rp-key.pem`.
    PrivateKeyJwt,
}

/// The `[upstream]` table of a test config.
#[derive(Clone)]
struct Upstream {
    issuer: String,
    credential: BrokerCredential,
    subject_claim: Option<String>,
}

impl Upstream {
    fn text(&self) -> String {
        let credential = match self.credential {
            BrokerCredential::ClientSecret => {
                "client_secret_file = \"upstream-client-secret\"\n".into()
            }
            BrokerCredential::PrivateKeyJwt => format!(
                "private_key_file = {}\n",
                toml_string(
                    testdata("upstream-rp-key.pem")
                        .to_str()
                        .expect("UTF-8 path")
                )
            ),
        };
        let subject_claim = self
            .subject_claim
            .as_deref()
            .map(|claim| format!("subject_claim = {}\n", toml_string(claim)))
            .unwrap_or_default();
        format!(
            "\n[upstream]\nissuer = {}\napi_audience = {}\nclient_credentials_scope = {}\n\
             client_id = {}\n{credential}refresh_token_key_file = \"refresh-token.key\"\n\
             {subject_claim}",
            toml_string(&self.issuer),
            toml_string(UPSTREAM_AUDIENCE),
            toml_string(UPSTREAM_SCOPE),
            toml_string(BROKER_CLIENT_ID),
        )
    }
}

/// A deployment's auth config: [`ISSUER`], [`AUDIENCE`], `test-key-1`
/// (active) and `test-key-2`, default lifetimes, and the given
/// Organizations; for a Token broker, also an Upstream IdP and mapping
/// rules.
#[derive(Clone)]
pub struct TestAuth {
    config: AuthConfig,
    issuer: String,
    organizations: Vec<(String, bool)>,
}

/// Builds a [`TestAuth`]. See [`TestAuth::builder`].
#[derive(Clone)]
pub struct TestAuthBuilder {
    issuer: String,
    organizations: Vec<(String, bool)>,
    signing: bool,
    person_lifetime_minutes: Option<u32>,
    machine_lifetime_minutes: Option<u32>,
    broker: String,
    upstream: Option<Upstream>,
    extra: String,
}

impl TestAuthBuilder {
    /// The Token broker's issuer, instead of [`ISSUER`].
    pub fn issuer(mut self, issuer: &str) -> Self {
        self.issuer = issuer.into();
        self
    }

    /// The fake Upstream IdP `idp`, with [`UPSTREAM_AUDIENCE`] and
    /// [`UPSTREAM_SCOPE`], and the Token broker registered there for
    /// browser sign-in as [`BROKER_CLIENT_ID`] with
    /// [`BROKER_CLIENT_SECRET`] (or [`Self::broker_credential`]).
    pub fn upstream(mut self, idp: &FakeIdp) -> Self {
        self.upstream = Some(Upstream {
            issuer: idp.issuer(),
            credential: BrokerCredential::ClientSecret,
            subject_claim: None,
        });
        self
    }

    /// How the Token broker authenticates at the Upstream IdP.
    pub fn broker_credential(mut self, credential: BrokerCredential) -> Self {
        self.upstream.as_mut().expect("upstream() first").credential = credential;
        self
    }

    /// `[upstream] subject_claim`.
    pub fn subject_claim(mut self, claim: &str) -> Self {
        self.upstream
            .as_mut()
            .expect("upstream() first")
            .subject_claim = Some(claim.into());
        self
    }

    /// `[clients.dbt] client_id`, instead of the dbt client's built-in id.
    pub fn dbt_client_id(mut self, client_id: &str) -> Self {
        self.extra.push_str(&format!(
            "\n[clients.dbt]\nclient_id = {}\n",
            toml_string(client_id)
        ));
        self
    }

    /// `[broker] max_pending_logins_per_ip`.
    pub fn max_pending_logins_per_ip(mut self, cap: u32) -> Self {
        self.broker
            .push_str(&format!("max_pending_logins_per_ip = {cap}\n"));
        self
    }

    /// `[broker] client_ip_header`.
    pub fn client_ip_header(mut self, header: &str) -> Self {
        self.broker
            .push_str(&format!("client_ip_header = {}\n", toml_string(header)));
        self
    }

    /// A mapping rule: Principals of `kind` (`person` or `machine`) whose
    /// `claim` holds `value` get `role` in `organization`.
    pub fn rule(
        mut self,
        kind: &str,
        claim: &str,
        value: &str,
        organization: &str,
        role: &str,
    ) -> Self {
        self.extra.push_str(&format!(
            "\n[[rules]]\nkind = {}\nclaim = {}\nvalue = {}\norganization = {}\nrole = {}\n",
            toml_string(kind),
            toml_string(claim),
            toml_string(value),
            toml_string(organization),
            toml_string(role),
        ));
        self
    }

    /// Machine tokens live `minutes`.
    pub fn machine_lifetime_minutes(mut self, minutes: u32) -> Self {
        self.machine_lifetime_minutes = Some(minutes);
        self
    }

    /// Person tokens live `minutes`.
    pub fn person_lifetime_minutes(mut self, minutes: u32) -> Self {
        self.person_lifetime_minutes = Some(minutes);
        self
    }

    /// No `[signing]`: a gRPC server that fetches the Token broker's keys.
    pub fn without_signing(mut self) -> Self {
        self.signing = false;
        self
    }

    pub fn build(self) -> TestAuth {
        let organizations: Vec<(&str, bool)> = self
            .organizations
            .iter()
            .map(|(id, disabled)| (id.as_str(), *disabled))
            .collect();
        let keys: &[TestKey] = if self.signing {
            &[TestKey::Active, TestKey::Second]
        } else {
            &[]
        };
        let mut tokens = String::new();
        if let Some(minutes) = self.person_lifetime_minutes {
            tokens.push_str(&format!("person_lifetime_minutes = {minutes}\n"));
        }
        if let Some(minutes) = self.machine_lifetime_minutes {
            tokens.push_str(&format!("machine_lifetime_minutes = {minutes}\n"));
        }
        if !tokens.is_empty() {
            tokens = format!("[tokens]\n{tokens}");
        }
        let upstream = self
            .upstream
            .as_ref()
            .map(Upstream::text)
            .unwrap_or_default();
        TestAuth {
            config: load(&config_text(
                &self.issuer,
                AUDIENCE,
                keys,
                &organizations,
                &tokens,
                &self.broker,
                &format!("{upstream}{}", self.extra),
            )),
            issuer: self.issuer,
            organizations: self.organizations,
        }
    }
}

/// How a [`TestSigner`] differs from the server's config.
#[derive(Debug, Clone)]
pub struct SignerOptions<'a> {
    pub issuer: &'a str,
    pub audience: &'a str,
    pub key: TestKey,
    /// The Organizations the signer believes in; `None` for the server's.
    pub organizations: Option<Declared<'a>>,
}

impl Default for SignerOptions<'_> {
    fn default() -> Self {
        Self {
            issuer: ISSUER,
            audience: AUDIENCE,
            key: TestKey::Active,
            organizations: None,
        }
    }
}

impl TestAuth {
    pub fn new(organizations: Declared) -> Self {
        Self::builder(organizations).build()
    }

    /// A config like [`TestAuth::new`]'s, to extend.
    pub fn builder(organizations: Declared) -> TestAuthBuilder {
        TestAuthBuilder {
            issuer: ISSUER.into(),
            organizations: organizations
                .iter()
                .map(|(id, disabled)| ((*id).to_owned(), *disabled))
                .collect(),
            signing: true,
            person_lifetime_minutes: None,
            machine_lifetime_minutes: None,
            broker: String::new(),
            upstream: None,
            extra: String::new(),
        }
    }

    pub fn config(&self) -> &AuthConfig {
        &self.config
    }

    /// The Token broker's issuer.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Signs as the server's Token broker would.
    pub fn signer(&self) -> TestSigner {
        self.signer_with(SignerOptions {
            issuer: &self.issuer,
            ..SignerOptions::default()
        })
    }

    /// Signs with `options`.
    pub fn signer_with(&self, options: SignerOptions) -> TestSigner {
        let server: Vec<(&str, bool)> = self
            .organizations
            .iter()
            .map(|(id, disabled)| (id.as_str(), *disabled))
            .collect();
        let organizations = options.organizations.unwrap_or(&server);
        let config = load(&config_text(
            options.issuer,
            options.audience,
            &[options.key],
            organizations,
            "",
            "",
            "",
        ));
        TestSigner {
            lifetimes: config.token_lifetimes(),
            issuer: config.token_issuer().expect("signing keys"),
            organizations: config.organizations().clone(),
        }
    }
}

/// Issues test tokens. See [`TestAuth::signer`].
pub struct TestSigner {
    issuer: TokenIssuer,
    organizations: dbt_state_auth::Organizations,
    lifetimes: dbt_state_auth::TokenLifetimes,
}

impl TestSigner {
    /// A person token for `principal` in broker session `session`.
    pub fn person(&self, principal: &str, session: &str) -> TestToken<'_> {
        TestToken::new(self, principal, Some(session))
    }

    /// A machine token for `principal`.
    pub fn machine(&self, principal: &str) -> TestToken<'_> {
        TestToken::new(self, principal, None)
    }

    pub fn lifetimes(&self) -> dbt_state_auth::TokenLifetimes {
        self.lifetimes
    }
}

/// A token being built by a [`TestSigner`].
pub struct TestToken<'a> {
    signer: &'a TestSigner,
    principal: String,
    session: Option<String>,
    grants: Vec<(OrganizationId, Role)>,
    issued_at: SystemTime,
}

impl<'a> TestToken<'a> {
    fn new(signer: &'a TestSigner, principal: &str, session: Option<&str>) -> Self {
        Self {
            signer,
            principal: principal.into(),
            session: session.map(Into::into),
            grants: Vec::new(),
            issued_at: SystemTime::now(),
        }
    }

    /// Grant `role` in `organization` (issued as the signer's config says:
    /// `org` and `app` entries when active, `app` only when disabled).
    pub fn grant(mut self, organization: &str, role: Role) -> Self {
        self.grants.push((
            OrganizationId::new(organization).expect("valid Organization id"),
            role,
        ));
        self
    }

    /// Issued `ago` before now.
    pub fn issued_ago(mut self, ago: Duration) -> Self {
        self.issued_at = SystemTime::now() - ago;
        self
    }

    /// The signed token.
    pub fn token(self) -> String {
        let scope = Scope::issue(self.grants, &self.signer.organizations);
        let principal = PrincipalId::new(self.principal);
        let request = match self.session {
            Some(session) => TokenRequest::person(principal, session, CLIENT_ID, scope),
            None => TokenRequest::machine(principal, CLIENT_ID, scope),
        };
        self.signer.issuer.issue(&request, self.issued_at).token
    }
}

/// Start capturing every log line of this test process (idempotent).
pub fn capture_logs() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(|| LogWriter)
            .init();
    });
}

/// Every log line captured since [`capture_logs`].
pub fn captured_logs() -> String {
    String::from_utf8_lossy(&LOGS.lock().unwrap()).into_owned()
}

static LOGS: Mutex<Vec<u8>> = Mutex::new(Vec::new());

struct LogWriter;

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        LOGS.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
