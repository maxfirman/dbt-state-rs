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

/// The TOML text of a config.
fn config_text(
    issuer: &str,
    audience: &str,
    keys: &[TestKey],
    organizations: Declared,
    tokens: &str,
) -> String {
    let mut text = format!(
        "[broker]\nissuer = {}\n\n[api]\naudience = {}\n\n{tokens}\n\n[signing]\nactive_key_id = {}\n",
        toml_string(issuer),
        toml_string(audience),
        toml_string(keys[0].id_and_file().0),
    );
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
    text
}

/// Load `text` through a config file, as the server binary does.
fn load(text: &str) -> AuthConfig {
    let dir = tempfile::tempdir().expect("config dir");
    let path = dir.path().join("auth.toml");
    std::fs::write(&path, text).expect("write config");
    AuthConfig::load(&path).expect("valid test auth config")
}

/// The server's auth config: [`ISSUER`], [`AUDIENCE`], `test-key-1`
/// (active) and `test-key-2`, default lifetimes, and the given
/// Organizations.
#[derive(Clone)]
pub struct TestAuth {
    config: AuthConfig,
    organizations: Vec<(String, bool)>,
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
        Self {
            config: load(&config_text(
                ISSUER,
                AUDIENCE,
                &[TestKey::Active, TestKey::Second],
                organizations,
                "",
            )),
            organizations: organizations
                .iter()
                .map(|(id, disabled)| ((*id).to_owned(), *disabled))
                .collect(),
        }
    }

    pub fn config(&self) -> &AuthConfig {
        &self.config
    }

    /// Signs as the server's Token broker would.
    pub fn signer(&self) -> TestSigner {
        self.signer_with(SignerOptions::default())
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
        ));
        TestSigner {
            lifetimes: config.token_lifetimes(),
            issuer: config.token_issuer(),
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
