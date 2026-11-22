//! The TOML auth config, read once at startup.
//!
//! ```toml
//! [broker]
//! issuer = "https://auth.state.example.com"   # `iss` of every token
//! # jwks_url = "http://broker.internal/jwks"  # optional; see below
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
//!
//! [[rules]]                                   # mapping rules
//! kind = "machine"                            # "person" or "machine"
//! claim = "roles"
//! value = "dbt-state.ci"
//! organization = "analytics"
//! role = "developer"
//! ```
//!
//! A Token broker needs `[signing]` and `[upstream]`. A gRPC server without
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

use crate::keys::{KeyFileError, SigningKey, SigningKeys, VerificationKeys};
use crate::{
    InvalidOrganizationId, MappingRule, MappingRules, Organization, OrganizationId, Organizations,
    PrincipalKind, RequestAuthenticator, Role, TokenIssuer, TokenVerifier,
};

/// Person token lifetime: default and bounds, in minutes.
const PERSON_LIFETIME_MINUTES: (u32, std::ops::RangeInclusive<u32>) = (60, 15..=240);
/// Machine token lifetime: default and bounds, in minutes.
const MACHINE_LIFETIME_MINUTES: (u32, std::ops::RangeInclusive<u32>) = (15, 15..=60);

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
}

/// The Token broker's Upstream IdP (`[upstream]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamConfig {
    issuer: String,
    api_audience: String,
    client_credentials_scope: Option<String>,
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
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBroker {
    issuer: String,
    jwks_url: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpstream {
    issuer: String,
    api_audience: String,
    client_credentials_scope: Option<String>,
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
            .map(|url| http_url("broker.jwks_url", url, false))
            .transpose()?;

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

        let upstream = raw.upstream.map(upstream).transpose()?;
        let rules = raw
            .rules
            .into_iter()
            .map(|rule| self::rule(rule, &organizations))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            issuer,
            jwks_url,
            audience,
            lifetimes,
            keys,
            organizations,
            upstream,
            rules: MappingRules::new(rules),
        })
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

/// `url` as the value of `key`: an http(s) URL; with
/// `https_unless_loopback`, plain http only for a loopback host.
fn http_url(key: &str, url: String, https_unless_loopback: bool) -> Result<String, ConfigError> {
    let rest = url
        .strip_prefix("https://")
        .map(|rest| (rest, true))
        .or_else(|| url.strip_prefix("http://").map(|rest| (rest, false)));
    let valid = match rest {
        _ if url.chars().any(char::is_whitespace) => false,
        None => false,
        Some(("", _)) => false,
        Some((_, true)) => true,
        Some((_, false)) if !https_unless_loopback => true,
        Some((rest, false)) => {
            let host = rest.split(['/', '?', '#']).next().unwrap_or_default();
            let host = match host.rsplit_once(':') {
                Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => host,
                _ => host,
            };
            matches!(host, "localhost" | "127.0.0.1" | "[::1]")
        }
    };
    if valid {
        Ok(url)
    } else if https_unless_loopback {
        Err(ConfigError::Invalid(format!(
            "{key} must be an https URL (plain http only on a loopback address)"
        )))
    } else {
        Err(ConfigError::Invalid(format!(
            "{key} must be an http(s) URL"
        )))
    }
}

fn upstream(raw: RawUpstream) -> Result<UpstreamConfig, ConfigError> {
    let issuer = http_url("upstream.issuer", raw.issuer, true)?;
    if raw.api_audience.trim().is_empty() {
        return Err(ConfigError::Invalid(
            "upstream.api_audience must not be empty".into(),
        ));
    }
    Ok(UpstreamConfig {
        issuer,
        api_audience: raw.api_audience,
        client_credentials_scope: raw.client_credentials_scope.filter(|s| !s.is_empty()),
    })
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
