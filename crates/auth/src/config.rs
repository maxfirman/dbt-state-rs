//! The TOML auth config, read once at startup.
//!
//! ```toml
//! [broker]
//! issuer = "https://auth.state.example.com"   # `iss` of every token
//! # jwks_url = "http://broker.internal/jwks"  # optional; see below
//! # max_pending_logins_per_ip = 20            # browser sign-ins in progress
//! # client_ip_header = "x-forwarded-for"      # behind a proxy; last entry
//!
//! [api]
//! audience = "https://state.example.com"      # the only `aud` gRPC accepts
//!
//! [tokens]                                    # optional
//! person_lifetime_minutes = 60                # 15 to 240
//! machine_lifetime_minutes = 15               # 15 to 60
//!
//! [signing]                                   # optional; see below
//! active_key_id = "2026-11"
//! [[signing.keys]]
//! id = "2026-11"
//! private_key_file = "/run/secrets/dbt-state/signing-2026-11.pem"
//!
//! [[organizations]]
//! id = "analytics"
//! [[organizations]]
//! id = "finance"
//! disabled = true
//!
//! [upstream]                                  # the Token broker's Upstream IdP
//! issuer = "https://login.example.com/tenant/v2.0"   # OIDC discovery base
//! api_audience = "api://dbt-state"            # required `aud` of its access tokens
//! client_credentials_scope = "api://dbt-state/.default"   # optional
//! # subject_claim = "oid"                     # identifies a Principal; default "sub"
//! # Browser sign-in: the Token broker's own registration at the IdP.
//! client_id = "00000000-0000-0000-0000-000000000000"
//! private_key_file = "/run/secrets/dbt-state/idp-client.pem"   # private_key_jwt
//! # private_key_id = "..."                    # its `kid`, if the IdP needs one
//! # client_secret_file = "/run/secrets/dbt-state/idp-client-secret"   # or a secret
//! refresh_token_key_file = "/run/secrets/dbt-state/refresh-token.key"
//! # login_scope = "openid offline_access email profile"
//!
//! [clients.dbt]                               # optional
//! client_id = "2fd87cd5-69a6-4c5f-9097-747a58f0edf6"   # the dbt client's built-in id
//!
//! [clients.ui]                                # optional: the console
//! client_id = "dbt-state-ui"                  # the `aud` of its tokens
//! redirect_uri = "https://console.example.com/auth/callback"   # its only callback
//! public_key_file = "/run/secrets/dbt-state/ui-client.pub.pem" # private_key_jwt
//!
//! [[rules]]                                   # mapping rules
//! kind = "machine"                            # "person" or "machine"
//! claim = "roles"
//! value = "dbt-state.ci"
//! organization = "analytics"
//! role = "developer"
//! ```
//!
//! A Token broker needs `[signing]` and `[upstream]`; browser sign-in also
//! needs `upstream.client_id`, exactly one credential file, and
//! `upstream.refresh_token_key_file`. A gRPC server without
//! `[signing]` checks tokens against the Token broker's published keys,
//! fetched from `broker.jwks_url`, or found by OIDC discovery at
//! `broker.issuer` when that is unset.
//!
//! Unknown keys are refused, so a misspelt key (for example `disable`) can
//! never be silently ignored. Relative key paths resolve from the config
//! file's directory.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::credential::{ClientAssertionKey, ClientSecret, UpstreamCredential};
use crate::keys::{KeyFileError, SigningKey, SigningKeys, VerificationKeys};
use crate::sealing::RefreshTokenKey;
use crate::ui_client::{ClientPublicKey, UiClient};
use crate::{
    InvalidOrganizationId, MappingRule, MappingRules, Organization, OrganizationId, Organizations,
    PrincipalKind, RequestAuthenticator, Role, TokenIssuer, TokenVerifier,
};

/// Person token lifetime: default and bounds, in minutes.
const PERSON_LIFETIME_MINUTES: (u32, std::ops::RangeInclusive<u32>) = (60, 15..=240);
/// Machine token lifetime: default and bounds, in minutes.
const MACHINE_LIFETIME_MINUTES: (u32, std::ops::RangeInclusive<u32>) = (15, 15..=60);

/// The dbt client's built-in OAuth client id: the default id of the dbt
/// client registration (`dbt-state==2.62.6` `dbt_state/config.py`
/// `CLIENT_ID_DEFAULT`; `dbt-fusion@9977b6c`
/// `crates/dbt-run-cache/src/service_config.rs` `DEFAULT_OAUTH_CLIENT_ID`).
pub const DEFAULT_DBT_CLIENT_ID: &str = "2fd87cd5-69a6-4c5f-9097-747a58f0edf6";
/// The default cap on browser sign-ins in progress from one client IP.
const DEFAULT_PENDING_LOGINS_PER_IP: u32 = 20;
/// The scope the Token broker requests at the Upstream IdP by default.
const DEFAULT_LOGIN_SCOPE: &str = "openid offline_access email profile";

/// A loaded, validated auth config.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    issuer: String,
    jwks_url: Option<String>,
    audience: String,
    lifetimes: TokenLifetimes,
    keys: Option<SigningKeys>,
    organizations: Organizations,
    upstream: Option<UpstreamConfig>,
    rules: MappingRules,
    dbt_client_id: String,
    ui_client: Option<UiClient>,
    pending_logins_per_ip: u32,
    client_ip_header: Option<String>,
}

/// The Token broker's Upstream IdP (`[upstream]`).
#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    issuer: String,
    api_audience: String,
    client_credentials_scope: Option<String>,
    subject_claim: String,
    login: Option<UpstreamLogin>,
}

/// The Token broker's own registration at the Upstream IdP, as an OIDC
/// relying party for browser sign-in.
#[derive(Debug, Clone)]
pub struct UpstreamLogin {
    client_id: String,
    credential: UpstreamCredential,
    scope: String,
    refresh_token_key: RefreshTokenKey,
}

impl UpstreamLogin {
    /// The Token broker's client id at the IdP.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// How the Token broker authenticates at the IdP's token endpoint.
    pub fn credential(&self) -> &UpstreamCredential {
        &self.credential
    }

    /// The `scope` requested on the IdP's authorization endpoint: always
    /// `openid`, normally `offline_access` for a refresh token.
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// The key stored upstream refresh tokens are encrypted with.
    pub fn refresh_token_key(&self) -> &RefreshTokenKey {
        &self.refresh_token_key
    }
}

impl UpstreamConfig {
    /// The IdP's issuer: its OIDC discovery document is at
    /// `<issuer>/.well-known/openid-configuration`, and its tokens' `iss`
    /// must equal it.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// The `aud` an upstream access token must hold for the Token broker to
    /// accept it.
    pub fn api_audience(&self) -> &str {
        &self.api_audience
    }

    /// The `scope` the Token broker requests on the IdP's
    /// `client_credentials` grant, if any.
    pub fn client_credentials_scope(&self) -> Option<&str> {
        self.client_credentials_scope.as_deref()
    }

    /// The claim whose value, with the IdP's issuer, identifies a Principal
    /// (people and machines alike): `sub` unless configured (for example
    /// `oid` on Entra ID). Email is never an identity.
    pub fn subject_claim(&self) -> &str {
        &self.subject_claim
    }

    /// The Token broker's own registration for browser sign-in, when
    /// configured.
    pub fn login(&self) -> Option<&UpstreamLogin> {
        self.login.as_ref()
    }
}

/// How long issued tokens live, per Principal kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenLifetimes {
    pub person: Duration,
    pub machine: Duration,
}

/// Why an auth config was refused.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read auth config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid auth config: {0}")]
    Parse(String),
    #[error("invalid auth config: {0}")]
    Organization(#[from] InvalidOrganizationId),
    #[error("invalid auth config: {0}")]
    DuplicateOrganization(#[from] crate::organization::DuplicateOrganization),
    #[error("invalid auth config: {0}")]
    Invalid(String),
    #[error("invalid auth config: signing key {id:?} ({path}): {error}")]
    KeyFile {
        id: String,
        path: PathBuf,
        error: KeyFileError,
    },
    /// A secret or key file named by `key`. Never carries its contents.
    #[error("invalid auth config: {key} ({path}): {error}")]
    File {
        key: &'static str,
        path: PathBuf,
        error: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    broker: RawBroker,
    api: RawApi,
    #[serde(default)]
    tokens: RawTokens,
    signing: Option<RawSigning>,
    #[serde(default)]
    organizations: Vec<RawOrganization>,
    upstream: Option<RawUpstream>,
    #[serde(default)]
    rules: Vec<RawRule>,
    #[serde(default)]
    clients: RawClients,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBroker {
    issuer: String,
    jwks_url: Option<String>,
    max_pending_logins_per_ip: Option<u32>,
    client_ip_header: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpstream {
    issuer: String,
    api_audience: String,
    client_credentials_scope: Option<String>,
    subject_claim: Option<String>,
    client_id: Option<String>,
    client_secret_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
    private_key_id: Option<String>,
    login_scope: Option<String>,
    refresh_token_key_file: Option<PathBuf>,
}

/// The broker's client registrations (`[clients.dbt]`, `[clients.ui]`).
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawClients {
    dbt: Option<RawDbtClient>,
    ui: Option<RawUiClient>,
}

/// The UI registration: no secret, only a public key.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUiClient {
    client_id: String,
    redirect_uri: String,
    public_key_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDbtClient {
    client_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRule {
    kind: RawKind,
    claim: String,
    value: String,
    organization: String,
    role: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawKind {
    Person,
    Machine,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawApi {
    audience: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTokens {
    #[serde(default = "default_person_lifetime")]
    person_lifetime_minutes: u32,
    #[serde(default = "default_machine_lifetime")]
    machine_lifetime_minutes: u32,
}

impl Default for RawTokens {
    fn default() -> Self {
        Self {
            person_lifetime_minutes: default_person_lifetime(),
            machine_lifetime_minutes: default_machine_lifetime(),
        }
    }
}

fn default_person_lifetime() -> u32 {
    PERSON_LIFETIME_MINUTES.0
}

fn default_machine_lifetime() -> u32 {
    MACHINE_LIFETIME_MINUTES.0
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSigning {
    active_key_id: String,
    keys: Vec<RawKey>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawKey {
    id: String,
    private_key_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOrganization {
    id: String,
    #[serde(default)]
    disabled: bool,
}

impl AuthConfig {
    /// Read and validate the config file at `path`, loading its key files.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let base = path.parent().unwrap_or(Path::new("."));
        Self::from_toml(&text, base)
    }

    /// Validate config `text`, resolving relative key paths from `base`.
    pub fn from_toml(text: &str, base: &Path) -> Result<Self, ConfigError> {
        let raw: RawConfig =
            toml::from_str(text).map_err(|error| ConfigError::Parse(error.message().to_owned()))?;

        let issuer = raw.broker.issuer;
        if !(issuer.starts_with("https://") || issuer.starts_with("http://"))
            || issuer.chars().any(char::is_whitespace)
        {
            return Err(ConfigError::Invalid(
                "broker.issuer must be the Token broker's http(s) URL".into(),
            ));
        }
        let audience = raw.api.audience;
        if audience.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "api.audience must not be empty".into(),
            ));
        }

        let lifetimes = TokenLifetimes {
            person: lifetime(
                "tokens.person_lifetime_minutes",
                raw.tokens.person_lifetime_minutes,
                PERSON_LIFETIME_MINUTES.1,
            )?,
            machine: lifetime(
                "tokens.machine_lifetime_minutes",
                raw.tokens.machine_lifetime_minutes,
                MACHINE_LIFETIME_MINUTES.1,
            )?,
        };

        let keys = raw
            .signing
            .map(|signing| load_keys(signing, base))
            .transpose()?;
        let jwks_url = raw
            .broker
            .jwks_url
            .map(|url| secure_http_url("broker.jwks_url", url))
            .transpose()?;
        if keys.is_none() && jwks_url.is_none() && !secure_url(&issuer) {
            return Err(ConfigError::Invalid(
                "broker.issuer must be an https URL (plain http only on a loopback address) \
                 without [signing], since the Token broker's keys are fetched from it; or set \
                 broker.jwks_url"
                    .into(),
            ));
        }

        let organizations = raw
            .organizations
            .into_iter()
            .map(|organization| {
                Ok(Organization {
                    id: OrganizationId::new(&organization.id)?,
                    disabled: organization.disabled,
                })
            })
            .collect::<Result<Vec<_>, InvalidOrganizationId>>()?;
        let organizations = Organizations::new(organizations)?;

        let upstream = raw.upstream.map(|raw| upstream(raw, base)).transpose()?;
        let rules = raw
            .rules
            .into_iter()
            .map(|rule| self::rule(rule, &organizations))
            .collect::<Result<Vec<_>, _>>()?;

        let dbt_client_id = match raw.clients.dbt {
            Some(client) if client.client_id.trim().is_empty() => {
                return Err(ConfigError::Invalid(
                    "clients.dbt.client_id must not be empty".into(),
                ))
            }
            Some(client) => client.client_id,
            None => DEFAULT_DBT_CLIENT_ID.to_owned(),
        };
        let ui_client = raw
            .clients
            .ui
            .map(|raw| ui_client(raw, &dbt_client_id, &audience, base))
            .transpose()?;
        let pending_logins_per_ip = raw
            .broker
            .max_pending_logins_per_ip
            .unwrap_or(DEFAULT_PENDING_LOGINS_PER_IP);
        if !(1..=10_000).contains(&pending_logins_per_ip) {
            return Err(ConfigError::Invalid(
                "broker.max_pending_logins_per_ip must be 1 to 10000".into(),
            ));
        }
        let client_ip_header = raw
            .broker
            .client_ip_header
            .map(|name| {
                let valid = !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || "!#$%&'*+-.^_`|~".contains(c));
                if valid {
                    Ok(name.to_ascii_lowercase())
                } else {
                    Err(ConfigError::Invalid(format!(
                        "broker.client_ip_header {name:?} is not an HTTP header name"
                    )))
                }
            })
            .transpose()?;

        Ok(Self {
            issuer,
            jwks_url,
            audience,
            lifetimes,
            keys,
            organizations,
            upstream,
            rules: MappingRules::new(rules),
            dbt_client_id,
            ui_client,
            pending_logins_per_ip,
            client_ip_header,
        })
    }

    /// The UI registration (`[clients.ui]`), when configured.
    pub fn ui_client(&self) -> Option<&UiClient> {
        self.ui_client.as_ref()
    }

    /// The dbt client registration's client id: the dbt client's built-in
    /// id ([`DEFAULT_DBT_CLIENT_ID`]) unless `[clients.dbt]` names another.
    pub fn dbt_client_id(&self) -> &str {
        &self.dbt_client_id
    }

    /// At most this many browser sign-ins may be in progress from one
    /// client IP.
    pub fn pending_logins_per_ip(&self) -> u32 {
        self.pending_logins_per_ip
    }

    /// The request header (lowercase) whose last comma-separated entry is
    /// the client IP, when the Token broker runs behind a proxy that sets
    /// it; otherwise the peer address is.
    pub fn client_ip_header(&self) -> Option<&str> {
        self.client_ip_header.as_deref()
    }

    /// The Token broker's public URL: the `iss` of every token.
    pub fn broker_issuer(&self) -> &str {
        &self.issuer
    }

    /// Where a gRPC server without `[signing]` fetches the Token broker's
    /// keys, when set; otherwise OIDC discovery at [`Self::broker_issuer`]
    /// says.
    pub fn broker_jwks_url(&self) -> Option<&str> {
        self.jwks_url.as_deref()
    }

    /// The dbt State API audience: the `aud` of every token.
    pub fn api_audience(&self) -> &str {
        &self.audience
    }

    /// The Token broker's Upstream IdP, when configured.
    pub fn upstream(&self) -> Option<&UpstreamConfig> {
        self.upstream.as_ref()
    }

    /// The mapping rules.
    pub fn mapping_rules(&self) -> &MappingRules {
        &self.rules
    }

    /// The declared Organizations.
    pub fn organizations(&self) -> &Organizations {
        &self.organizations
    }

    /// Issued token lifetimes.
    pub fn token_lifetimes(&self) -> TokenLifetimes {
        self.lifetimes
    }

    /// The public halves of the `[signing]` keys, if any: the key set a
    /// Token broker publishes, and an in-process gRPC server shares.
    pub fn verification_keys(&self) -> Option<VerificationKeys> {
        self.keys.as_ref().map(|keys| keys.public().clone())
    }

    /// Issues tokens with this config's issuer, audience, active key and
    /// lifetimes; `None` without `[signing]`.
    pub fn token_issuer(&self) -> Option<TokenIssuer> {
        self.keys.as_ref().map(|keys| {
            TokenIssuer::new(
                self.issuer.clone(),
                self.audience.clone(),
                keys.active().clone(),
                self.lifetimes,
            )
        })
    }

    /// Checks tokens against this config's issuer and audience and `keys`
    /// ([`Self::verification_keys`], or the Token broker's fetched JWKS).
    pub fn token_verifier(&self, keys: VerificationKeys) -> TokenVerifier {
        TokenVerifier::new(self.issuer.clone(), self.audience.clone(), keys)
    }

    /// Authenticates gRPC requests: [`AuthConfig::token_verifier`] plus the
    /// declared Organizations.
    pub fn request_authenticator(&self, keys: VerificationKeys) -> RequestAuthenticator {
        RequestAuthenticator::new(self.token_verifier(keys), self.organizations.clone())
    }
}

fn load_keys(signing: RawSigning, base: &Path) -> Result<SigningKeys, ConfigError> {
    let mut keys: Vec<SigningKey> = Vec::new();
    for key in signing.keys {
        if key.id.is_empty() {
            return Err(ConfigError::Invalid(
                "signing.keys id must not be empty".into(),
            ));
        }
        if keys.iter().any(|k| k.id == key.id) {
            return Err(ConfigError::Invalid(format!(
                "signing key id {:?} is declared more than once",
                key.id
            )));
        }
        let path = base.join(&key.private_key_file);
        keys.push(SigningKey::load(key.id.clone(), &path).map_err(|error| {
            ConfigError::KeyFile {
                id: key.id,
                path,
                error,
            }
        })?);
    }
    SigningKeys::new(keys, &signing.active_key_id).ok_or_else(|| {
        ConfigError::Invalid(format!(
            "signing.active_key_id {:?} names no key in signing.keys",
            signing.active_key_id
        ))
    })
}

/// Whether credentials may be sent to, or keys fetched from, `url`: an
/// https URL, or plain http to a loopback host (local development).
pub fn secure_url(url: &str) -> bool {
    if url.chars().any(char::is_whitespace) {
        return false;
    }
    if let Some(rest) = url.strip_prefix("https://") {
        return !rest.is_empty();
    }
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => host,
        _ => authority,
    };
    matches!(host, "localhost" | "127.0.0.1" | "[::1]")
}

/// `url` as the value of `key`, if [`secure_url`] accepts it.
fn secure_http_url(key: &str, url: String) -> Result<String, ConfigError> {
    if secure_url(&url) {
        Ok(url)
    } else {
        Err(ConfigError::Invalid(format!(
            "{key} must be an https URL (plain http only on a loopback address)"
        )))
    }
}

fn upstream(raw: RawUpstream, base: &Path) -> Result<UpstreamConfig, ConfigError> {
    let issuer = secure_http_url("upstream.issuer", raw.issuer)?;
    if raw.api_audience.trim().is_empty() {
        return Err(ConfigError::Invalid(
            "upstream.api_audience must not be empty".into(),
        ));
    }
    let subject_claim = raw.subject_claim.unwrap_or_else(|| "sub".to_owned());
    if subject_claim.is_empty() {
        return Err(ConfigError::Invalid(
            "upstream.subject_claim must not be empty".into(),
        ));
    }
    let login = match raw.client_id {
        Some(client_id) => Some(upstream_login(
            client_id,
            raw.client_secret_file,
            raw.private_key_file,
            raw.private_key_id,
            raw.login_scope,
            raw.refresh_token_key_file,
            base,
        )?),
        None => {
            let stray = [
                (
                    "upstream.client_secret_file",
                    raw.client_secret_file.is_some(),
                ),
                ("upstream.private_key_file", raw.private_key_file.is_some()),
                ("upstream.private_key_id", raw.private_key_id.is_some()),
                ("upstream.login_scope", raw.login_scope.is_some()),
                (
                    "upstream.refresh_token_key_file",
                    raw.refresh_token_key_file.is_some(),
                ),
            ];
            if let Some((key, _)) = stray.into_iter().find(|(_, set)| *set) {
                return Err(ConfigError::Invalid(format!(
                    "{key} is for browser sign-in, which needs upstream.client_id"
                )));
            }
            None
        }
    };
    Ok(UpstreamConfig {
        issuer,
        api_audience: raw.api_audience,
        client_credentials_scope: raw.client_credentials_scope.filter(|s| !s.is_empty()),
        subject_claim,
        login,
    })
}

fn upstream_login(
    client_id: String,
    client_secret_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
    private_key_id: Option<String>,
    scope: Option<String>,
    refresh_token_key_file: Option<PathBuf>,
    base: &Path,
) -> Result<UpstreamLogin, ConfigError> {
    if client_id.trim().is_empty() {
        return Err(ConfigError::Invalid(
            "upstream.client_id must not be empty".into(),
        ));
    }
    let credential = match (private_key_file, client_secret_file) {
        (Some(_), Some(_)) => {
            return Err(ConfigError::Invalid(
                "set only one of upstream.private_key_file (private_key_jwt) and \
                 upstream.client_secret_file"
                    .into(),
            ))
        }
        (None, None) => {
            return Err(ConfigError::Invalid(
                "upstream.client_id needs a credential: upstream.private_key_file \
                 (private_key_jwt, preferred) or upstream.client_secret_file"
                    .into(),
            ))
        }
        (Some(file), None) => {
            let path = base.join(file);
            let key =
                SigningKey::load(String::new(), &path).map_err(|error| ConfigError::File {
                    key: "upstream.private_key_file",
                    path,
                    error: error.to_string(),
                })?;
            UpstreamCredential::PrivateKeyJwt(ClientAssertionKey::new(
                key,
                private_key_id.filter(|kid| !kid.is_empty()),
            ))
        }
        (None, Some(file)) => {
            if private_key_id.is_some() {
                return Err(ConfigError::Invalid(
                    "upstream.private_key_id names the key of upstream.private_key_file".into(),
                ));
            }
            let path = base.join(file);
            let file_error = |error: String| ConfigError::File {
                key: "upstream.client_secret_file",
                path: path.clone(),
                error,
            };
            let secret = std::fs::read_to_string(&path)
                .map_err(|error| file_error(format!("cannot read: {}", error.kind())))?;
            let secret = secret.trim();
            if secret.is_empty() {
                return Err(file_error("empty".into()));
            }
            UpstreamCredential::ClientSecret(ClientSecret::new(secret.to_owned()))
        }
    };
    let scope = scope.unwrap_or_else(|| DEFAULT_LOGIN_SCOPE.to_owned());
    if !scope.split(' ').any(|s| s == "openid") {
        return Err(ConfigError::Invalid(
            "upstream.login_scope must include openid".into(),
        ));
    }
    let Some(key_file) = refresh_token_key_file else {
        return Err(ConfigError::Invalid(
            "browser sign-in (upstream.client_id) needs upstream.refresh_token_key_file, the \
             key stored upstream refresh tokens are encrypted with"
                .into(),
        ));
    };
    let path = base.join(key_file);
    let refresh_token_key = RefreshTokenKey::load(&path).map_err(|error| ConfigError::File {
        key: "upstream.refresh_token_key_file",
        path,
        error: error.to_string(),
    })?;
    Ok(UpstreamLogin {
        client_id,
        credential,
        scope,
        refresh_token_key,
    })
}

fn ui_client(
    raw: RawUiClient,
    dbt_client_id: &str,
    api_audience: &str,
    base: &Path,
) -> Result<UiClient, ConfigError> {
    // UI tokens carry the UI's client id as `aud`: equal to the API
    // audience, gRPC would accept them.
    if raw.client_id.trim().is_empty()
        || raw.client_id == dbt_client_id
        || raw.client_id == api_audience
    {
        return Err(ConfigError::Invalid(
            "clients.ui.client_id must be set, and differ from the dbt client's id and from \
             api.audience"
                .into(),
        ));
    }
    // RFC 6749 §3.1.2: an absolute URI without a fragment.
    if !secure_url(&raw.redirect_uri) || raw.redirect_uri.contains('#') {
        return Err(ConfigError::Invalid(
            "clients.ui.redirect_uri must be an https URL (plain http only on a loopback \
             address) without a fragment"
                .into(),
        ));
    }
    let path = base.join(&raw.public_key_file);
    let key = ClientPublicKey::load(&path).map_err(|error| ConfigError::File {
        key: "clients.ui.public_key_file",
        path,
        error: error.to_string(),
    })?;
    Ok(UiClient::new(raw.client_id, raw.redirect_uri, key))
}

fn rule(raw: RawRule, organizations: &Organizations) -> Result<MappingRule, ConfigError> {
    if raw.claim.is_empty() {
        return Err(ConfigError::Invalid(
            "rules: claim must not be empty".into(),
        ));
    }
    if raw.value.is_empty() {
        return Err(ConfigError::Invalid(
            "rules: value must not be empty".into(),
        ));
    }
    let organization = OrganizationId::new(&raw.organization)?;
    if !organizations.is_active(&organization) && !organizations.is_disabled(&organization) {
        return Err(ConfigError::Invalid(format!(
            "rules: Organization {:?} is not declared in [[organizations]]",
            raw.organization
        )));
    }
    let role: Role = raw
        .role
        .parse()
        .map_err(|error| ConfigError::Invalid(format!("rules: {error}")))?;
    Ok(MappingRule {
        kind: match raw.kind {
            RawKind::Person => PrincipalKind::Person,
            RawKind::Machine => PrincipalKind::Machine,
        },
        claim: raw.claim,
        value: raw.value,
        organization,
        role,
    })
}

fn lifetime(
    key: &str,
    minutes: u32,
    bounds: std::ops::RangeInclusive<u32>,
) -> Result<Duration, ConfigError> {
    if bounds.contains(&minutes) {
        Ok(Duration::from_secs(u64::from(minutes) * 60))
    } else {
        Err(ConfigError::Invalid(format!(
            "{key} = {minutes}: a token lifetime must be {} to {} minutes",
            bounds.start(),
            bounds.end()
        )))
    }
}
