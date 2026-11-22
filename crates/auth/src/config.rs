//! The TOML auth config, read once at startup.
//!
//! ```toml
//! [broker]
//! issuer = "https://auth.state.example.com"   # `iss` of every token
//!
//! [api]
//! audience = "https://state.example.com"      # the only `aud` gRPC accepts
//!
//! [tokens]                                    # optional
//! person_lifetime_minutes = 60                # 15 to 240
//! machine_lifetime_minutes = 15               # 15 to 60
//!
//! [signing]
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
//! ```
//!
//! Unknown keys are refused, so a misspelt key (for example `disable`) can
//! never be silently ignored. Relative key paths resolve from the config
//! file's directory.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::keys::{KeyFileError, SigningKey, SigningKeys};
use crate::{
    InvalidOrganizationId, Organization, OrganizationId, Organizations, RequestAuthenticator,
    TokenIssuer, TokenVerifier,
};

/// Person token lifetime: default and bounds, in minutes.
const PERSON_LIFETIME_MINUTES: (u32, std::ops::RangeInclusive<u32>) = (60, 15..=240);
/// Machine token lifetime: default and bounds, in minutes.
const MACHINE_LIFETIME_MINUTES: (u32, std::ops::RangeInclusive<u32>) = (15, 15..=60);

/// A loaded, validated auth config.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    issuer: String,
    audience: String,
    lifetimes: TokenLifetimes,
    keys: SigningKeys,
    organizations: Organizations,
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
    signing: RawSigning,
    #[serde(default)]
    organizations: Vec<RawOrganization>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBroker {
    issuer: String,
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

        let mut keys: Vec<SigningKey> = Vec::new();
        for key in raw.signing.keys {
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
        let keys = SigningKeys::new(keys, &raw.signing.active_key_id).ok_or_else(|| {
            ConfigError::Invalid(format!(
                "signing.active_key_id {:?} names no key in signing.keys",
                raw.signing.active_key_id
            ))
        })?;

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

        Ok(Self {
            issuer,
            audience,
            lifetimes,
            keys,
            organizations,
        })
    }

    /// The declared Organizations.
    pub fn organizations(&self) -> &Organizations {
        &self.organizations
    }

    /// Issued token lifetimes.
    pub fn token_lifetimes(&self) -> TokenLifetimes {
        self.lifetimes
    }

    /// Issues tokens with this config's issuer, audience, active key and
    /// lifetimes.
    pub fn token_issuer(&self) -> TokenIssuer {
        TokenIssuer::new(
            self.issuer.clone(),
            self.audience.clone(),
            self.keys.active().clone(),
            self.lifetimes,
        )
    }

    /// Checks tokens against this config's issuer, audience and every loaded
    /// key.
    pub fn token_verifier(&self) -> TokenVerifier {
        TokenVerifier::new(
            self.issuer.clone(),
            self.audience.clone(),
            self.keys.public(),
        )
    }

    /// Authenticates gRPC requests: [`AuthConfig::token_verifier`] plus the
    /// declared Organizations.
    pub fn request_authenticator(&self) -> RequestAuthenticator {
        RequestAuthenticator::new(self.token_verifier(), self.organizations.clone())
    }
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
