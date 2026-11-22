//! Server configuration from environment.
//!
//! | Variable | Meaning |
//! |---|---|
//! | `DBT_STATE_LISTEN` | gRPC listen address (default `127.0.0.1:50051`) |
//! | `DATABASE_URL` | Postgres |
//! | `DBT_STATE_AUTH` | `none` (default) or `oidc` |
//! | `DBT_STATE_AUTH_CONFIG` | `oidc`: path of the TOML auth config |
//! | `DBT_STATE_ALLOW_INSECURE` | `none`: allow a non-loopback listen address |
//! | `DBT_STATE_REQUIRE_ORG_HEADER` | `none`: reject requests without `x-organization-id`; refused in `oidc` |
//! | `DBT_STATE_BROKER_LISTEN` | `oidc`: also run the Token broker in this process, on this HTTP address; refused in `none` |

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Context};
use dbt_state_auth::AuthConfig;

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub database_url: String,
    pub auth: AuthMode,
    /// `oidc`: the in-process Token broker's listen address, when it runs
    /// one (the gRPC server's own listener stays HTTP/2 gRPC only).
    pub broker_listen: Option<SocketAddr>,
}

/// How gRPC calls are authenticated (`DBT_STATE_AUTH`).
#[derive(Debug, Clone)]
pub enum AuthMode {
    /// No tokens. The Organization is `x-organization-id`, `local` when
    /// absent.
    None {
        /// When true, every request MUST carry a non-empty `x-organization-id`
        /// metadata header; requests without it are rejected rather than
        /// silently bucketed into the shared default tenant. Off by default
        /// to preserve the insecure local-dev flow (the dbt client omits the
        /// header when pointed at an insecure endpoint). This enforces org
        /// SCOPING presence, not AUTHENTICATION: that is `oidc` mode.
        require_org_header: bool,
    },
    /// Every call but health needs a Token-broker-signed token for the
    /// Organization it names, checked against the auth config read from
    /// `path`.
    Oidc {
        path: PathBuf,
        config: Box<AuthConfig>,
    },
}

impl Config {
    /// Read and check the environment, and in `oidc` mode load the auth
    /// config. Refuses `none` mode on a non-loopback address without
    /// `DBT_STATE_ALLOW_INSECURE=true`, `DBT_STATE_REQUIRE_ORG_HEADER` in
    /// `oidc` mode, `DBT_STATE_BROKER_LISTEN` in `none` mode or with an auth
    /// config that cannot run a Token broker, and an invalid auth config.
    pub fn from_env() -> anyhow::Result<Self> {
        let var = |key: &str| std::env::var(key).ok();
        let listen = var("DBT_STATE_LISTEN").unwrap_or_else(|| "127.0.0.1:50051".to_string());
        let listen: SocketAddr = listen
            .parse()
            .with_context(|| format!("DBT_STATE_LISTEN={listen:?} is not a socket address"))?;
        let broker_listen = var("DBT_STATE_BROKER_LISTEN")
            .filter(|listen| !listen.is_empty())
            .map(|listen| {
                listen.parse::<SocketAddr>().with_context(|| {
                    format!("DBT_STATE_BROKER_LISTEN={listen:?} is not a socket address")
                })
            })
            .transpose()?;
        let database_url = var("DATABASE_URL")
            .unwrap_or_else(|| "postgres://dbtstate:dbtstate@localhost:55441/dbtstate".to_string());

        let mode = var("DBT_STATE_AUTH").unwrap_or_default();
        let auth = match mode.trim().to_ascii_lowercase().as_str() {
            "" | "none" => {
                if !listen.ip().is_loopback() && !truthy(var("DBT_STATE_ALLOW_INSECURE")) {
                    bail!(
                        "DBT_STATE_AUTH=none serves without authentication; refusing the \
                         non-loopback address {listen}. Use DBT_STATE_AUTH=oidc, or set \
                         DBT_STATE_ALLOW_INSECURE=true to accept the risk"
                    );
                }
                if broker_listen.is_some() {
                    bail!(
                        "DBT_STATE_BROKER_LISTEN runs the Token broker, which needs \
                         DBT_STATE_AUTH=oidc; unset it in DBT_STATE_AUTH=none mode"
                    );
                }
                AuthMode::None {
                    require_org_header: truthy(var("DBT_STATE_REQUIRE_ORG_HEADER")),
                }
            }
            "oidc" => {
                if var("DBT_STATE_REQUIRE_ORG_HEADER").is_some() {
                    bail!(
                        "DBT_STATE_REQUIRE_ORG_HEADER has no effect in DBT_STATE_AUTH=oidc \
                         mode, where x-organization-id is always required and checked \
                         against the token; unset it"
                    );
                }
                let path: PathBuf = var("DBT_STATE_AUTH_CONFIG")
                    .filter(|path| !path.is_empty())
                    .context(
                        "DBT_STATE_AUTH=oidc needs DBT_STATE_AUTH_CONFIG, the auth config path",
                    )?
                    .into();
                let config = AuthConfig::load(&path)?;
                if broker_listen.is_some() {
                    dbt_state_broker::Broker::check(&config)
                        .context("DBT_STATE_BROKER_LISTEN is set")?;
                }
                AuthMode::Oidc {
                    path,
                    config: Box::new(config),
                }
            }
            _ => bail!("DBT_STATE_AUTH={mode:?} is not a mode: use none or oidc"),
        };

        Ok(Self {
            listen,
            database_url,
            auth,
            broker_listen,
        })
    }
}

/// The common truthy spellings; everything else (and unset) is off.
fn truthy(value: Option<String>) -> bool {
    value.is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

#[cfg(test)]
mod tests {
    use super::truthy;

    /// The env flags parse the common truthy spellings; everything else is off.
    #[test]
    fn flag_truthy_parsing() {
        for v in ["1", "true", "TRUE", " yes ", "Yes"] {
            assert!(truthy(Some(v.into())), "{v:?} should be truthy");
        }
        for v in ["0", "false", "", "off", "no", "maybe"] {
            assert!(!truthy(Some(v.into())), "{v:?} should be falsy");
        }
        assert!(!truthy(None));
    }
}
