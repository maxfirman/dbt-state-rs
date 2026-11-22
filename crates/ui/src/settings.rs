//! Console configuration from environment.
//!
//! | Variable | Meaning |
//! |---|---|
//! | `HOST`, `PORT` | listen address (default `127.0.0.1:3000`; read by Topcoat) |
//! | `DATABASE_URL` | the Postgres the dbt State server writes to |
//! | `DBT_STATE_AUTH` | `none` (default) or `oidc`, as for the server |
//! | `DBT_STATE_UI_ORG_ID` | `none`: the one Organization shown (default `local`); refused in `oidc` |
//! | `DBT_STATE_AUTH_CONFIG` | `oidc`: the TOML auth config, which must register the UI (`[clients.ui]`) |
//! | `DBT_STATE_UI_PRIVATE_KEY_FILE` | `oidc`: the PEM private key of `[clients.ui] public_key_file`, for `private_key_jwt` |
//! | `DBT_STATE_UI_PRIVATE_KEY_ID` | `oidc`, optional: the `kid` its client assertions name |
//! | `DBT_STATE_UI_COOKIE_KEY_FILE` | `oidc`: base64 of 32 random bytes that encrypt the console's cookies; the same on every replica |
//! | `DBT_STATE_UI_BROKER_URL` | `oidc`, optional: where the console reaches the Token broker's token endpoint (and JWKS without `[signing]`); default `broker.issuer` |
//!
//! The `oidc`-only variables are refused in `none` mode, so a console meant
//! to require sign-in never starts without it.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use dbt_state_auth::{AuthConfig, ClientAssertionKey, secure_url};
use topcoat::cookie::Key;

/// The path of the console's sign-in callback: `[clients.ui]
/// redirect_uri` must name it.
pub(crate) const CALLBACK_PATH: &str = "/auth/callback";

/// The variables only `oidc` mode reads.
const OIDC_ONLY: &[&str] = &[
    "DBT_STATE_UI_PRIVATE_KEY_FILE",
    "DBT_STATE_UI_PRIVATE_KEY_ID",
    "DBT_STATE_UI_COOKIE_KEY_FILE",
    "DBT_STATE_UI_BROKER_URL",
];

/// How the console is configured. See the module docs.
pub struct Settings {
    pub(crate) mode: Mode,
}

pub(crate) enum Mode {
    /// No sign-in: one fixed Organization.
    None { organization: String },
    /// People sign in through the Token broker.
    Oidc(Box<OidcSettings>),
}

/// `oidc` mode's deployment settings.
pub(crate) struct OidcSettings {
    pub(crate) config: AuthConfig,
    pub(crate) assertion_key: ClientAssertionKey,
    pub(crate) cookie_key: Key,
    /// The Token broker's base URL for the console's own requests.
    pub(crate) broker_url: String,
}

impl Settings {
    /// Read the process environment. See [`Self::from_vars`].
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// Read the variables `var` looks up, and in `oidc` mode load the auth
    /// config and key files. Refuses `DBT_STATE_UI_ORG_ID` in `oidc` mode
    /// (the signed-in person's Organizations replace it), the `oidc`-only
    /// variables in `none` mode, an auth config without `[clients.ui]` or
    /// whose `redirect_uri` is not the console's callback path, a private
    /// key that does not match `[clients.ui] public_key_file`, a cookie
    /// key that is not 32 bytes, and a broker URL that is not https (or
    /// plain http to a loopback host).
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let set = |name: &str| var(name).filter(|value| !value.is_empty());
        let mode = var("DBT_STATE_AUTH").unwrap_or_default();
        let mode = match mode.trim().to_ascii_lowercase().as_str() {
            "" | "none" => {
                if let Some(name) = OIDC_ONLY.iter().find(|name| set(name).is_some()) {
                    bail!(
                        "{name} is for DBT_STATE_AUTH=oidc, where people sign in to the \
                         console; set DBT_STATE_AUTH=oidc, or unset it"
                    );
                }
                Mode::None {
                    organization: var("DBT_STATE_UI_ORG_ID").unwrap_or_else(|| "local".into()),
                }
            }
            "oidc" => Mode::Oidc(Box::new(oidc(&set)?)),
            _ => bail!("DBT_STATE_AUTH={mode:?} is not a mode: use none or oidc"),
        };
        Ok(Self { mode })
    }
}

fn oidc(set: &impl Fn(&str) -> Option<String>) -> anyhow::Result<OidcSettings> {
    if set("DBT_STATE_UI_ORG_ID").is_some() {
        bail!(
            "DBT_STATE_UI_ORG_ID has no effect in DBT_STATE_AUTH=oidc mode, where the console \
             shows the Organizations of the person signed in; unset it"
        );
    }
    let path: PathBuf = set("DBT_STATE_AUTH_CONFIG")
        .context("DBT_STATE_AUTH=oidc needs DBT_STATE_AUTH_CONFIG, the auth config path")?
        .into();
    let config = AuthConfig::load(&path)?;
    let client = config.ui_client().context(
        "the auth config registers no UI client: add [clients.ui], which the console signs in as",
    )?;
    let redirect = reqwest::Url::parse(client.redirect_uri())
        .context("[clients.ui] redirect_uri is not a URL")?;
    if redirect.path() != CALLBACK_PATH {
        bail!(
            "[clients.ui] redirect_uri {:?} must be the console's callback, \
             <console URL>{CALLBACK_PATH}",
            client.redirect_uri()
        );
    }

    let key_file: PathBuf = set("DBT_STATE_UI_PRIVATE_KEY_FILE")
        .context(
            "DBT_STATE_AUTH=oidc needs DBT_STATE_UI_PRIVATE_KEY_FILE, the private key of \
             [clients.ui] public_key_file",
        )?
        .into();
    let assertion_key = ClientAssertionKey::load(&key_file, set("DBT_STATE_UI_PRIVATE_KEY_ID"))
        .with_context(|| format!("DBT_STATE_UI_PRIVATE_KEY_FILE ({})", key_file.display()))?;
    let audience = config.broker_issuer();
    let probe = assertion_key.assertion(client.client_id(), audience, SystemTime::now());
    if client
        .verify_assertion(&probe, &[audience], SystemTime::now())
        .is_err()
    {
        bail!(
            "DBT_STATE_UI_PRIVATE_KEY_FILE ({}) is not the private key of [clients.ui] \
             public_key_file",
            key_file.display()
        );
    }

    let cookie_file: PathBuf = set("DBT_STATE_UI_COOKIE_KEY_FILE")
        .context(
            "DBT_STATE_AUTH=oidc needs DBT_STATE_UI_COOKIE_KEY_FILE, base64 of 32 random bytes \
             (openssl rand -base64 32)",
        )?
        .into();
    let cookie_key = cookie_key(&cookie_file)?;

    let broker_url = set("DBT_STATE_UI_BROKER_URL")
        .unwrap_or_else(|| config.broker_issuer().to_owned())
        .trim_end_matches('/')
        .to_owned();
    if !secure_url(&broker_url) {
        bail!(
            "the console reaches the Token broker at {broker_url:?}, which must be an https URL \
             (plain http only on a loopback address); set DBT_STATE_UI_BROKER_URL"
        );
    }
    Ok(OidcSettings {
        config,
        assertion_key,
        cookie_key,
        broker_url,
    })
}

/// The cookie encryption key from `path`: base64 of 32 bytes, expanded into
/// the cookie crate's encryption and signing keys. Errors never carry the
/// file's contents.
fn cookie_key(path: &Path) -> anyhow::Result<Key> {
    let refused = |problem: &str| {
        anyhow::anyhow!(
            "DBT_STATE_UI_COOKIE_KEY_FILE ({}): {problem}; it must hold base64 of 32 random \
             bytes (openssl rand -base64 32)",
            path.display()
        )
    };
    let text = std::fs::read_to_string(path)
        .map_err(|error| refused(&format!("cannot read: {}", error.kind())))?;
    let bytes = STANDARD
        .decode(text.trim())
        .map_err(|_| refused("not base64"))?;
    if bytes.len() != 32 {
        return Err(refused(&format!("{} bytes", bytes.len())));
    }
    Ok(Key::derive_from(&bytes))
}
