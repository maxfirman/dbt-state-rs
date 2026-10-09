//! dbt State authentication for the recording proxy.
//!
//! Replicates the Fusion client's platform token-exchange: read the active dbt
//! Cloud credential from `~/.dbt/dbt_cloud.yml`, exchange it at
//! `https://auth.state.dbt.com/token` for a short-lived `id_token`, and derive
//! the organization id from the returned scope. The proxy attaches
//! `authorization: Bearer <id_token>` and `x-organization-id: <org_id>` to each
//! upstream call, exactly like the real client.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde::Deserialize;

const DEFAULT_TOKEN_URL: &str = "https://auth.state.dbt.com/token";
const DEFAULT_CLIENT_ID: &str = "2fd87cd5-69a6-4c5f-9097-747a58f0edf6";
const ORG_SCOPE_PREFIX: &str = "runcache:scope:org:";
// Refresh a little before the token actually expires.
const EXPIRY_SLACK: Duration = Duration::from_secs(60);

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("could not resolve home directory")]
    NoHome,
    #[error("failed to read dbt_cloud.yml: {0}")]
    ReadConfig(#[from] std::io::Error),
    #[error("dbt_cloud.yml missing field: {0}")]
    MissingField(&'static str),
    #[error("token exchange HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("token exchange failed (status {status}): {body}")]
    Exchange { status: u16, body: String },
    #[error("token response missing org scope: {0}")]
    NoOrgScope(String),
}

/// Active dbt Cloud credential parsed from `~/.dbt/dbt_cloud.yml`.
#[derive(Debug, Clone)]
pub struct DbtCloudCredential {
    pub host: String,
    pub token: String,
    pub project_id: String,
}

impl DbtCloudCredential {
    /// Parse the active project's credential from `~/.dbt/dbt_cloud.yml`.
    ///
    /// A deliberately dependency-light parser (no YAML crate): the file shape is
    /// stable and simple. Finds `active-host`, `active-project`, then the matching
    /// project block's `token-value`.
    pub fn from_default_config() -> Result<Self, AuthError> {
        let home = dirs_home().ok_or(AuthError::NoHome)?;
        let path = home.join(".dbt").join("dbt_cloud.yml");
        let text = std::fs::read_to_string(path)?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Self, AuthError> {
        let host = capture(text, "active-host:").ok_or(AuthError::MissingField("active-host"))?;
        let project_id =
            capture(text, "active-project:").ok_or(AuthError::MissingField("active-project"))?;

        // Split into project blocks and find the one whose project-id matches.
        let mut token = None;
        for block in text.split("- project-name:") {
            if block.contains(&format!("project-id: \"{project_id}\"")) {
                token = capture(block, "token-value:");
                if token.is_some() {
                    break;
                }
            }
        }
        let token = token.ok_or(AuthError::MissingField("token-value"))?;

        Ok(Self {
            host,
            token,
            project_id,
        })
    }
}

/// Extract the first double-quoted value following `key` on any line.
fn capture(text: &str, key: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(idx) = line.find(key) {
            let rest = &line[idx + key.len()..];
            if let Some(start) = rest.find('"') {
                let after = &rest[start + 1..];
                if let Some(end) = after.find('"') {
                    return Some(after[..end].to_string());
                }
            }
        }
    }
    None
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

#[derive(Debug, Deserialize)]
struct TokenExchangeResponse {
    id_token: String,
    scope: String,
    #[serde(default)]
    expires_in: Option<f64>,
}

/// A minted dbt State token plus the resolved org id.
#[derive(Debug, Clone)]
pub struct StateToken {
    pub id_token: String,
    pub org_id: String,
    expires_at: Option<Instant>,
}

impl StateToken {
    fn is_fresh(&self) -> bool {
        match self.expires_at {
            Some(at) => Instant::now() + EXPIRY_SLACK < at,
            None => true,
        }
    }
}

/// Mints and caches dbt State tokens via platform token-exchange.
pub struct TokenMinter {
    http: reqwest::Client,
    token_url: String,
    client_id: String,
    credential: DbtCloudCredential,
    cached: tokio::sync::Mutex<Option<StateToken>>,
}

impl TokenMinter {
    pub fn new(credential: DbtCloudCredential) -> Self {
        Self {
            http: reqwest::Client::new(),
            token_url: DEFAULT_TOKEN_URL.to_string(),
            client_id: DEFAULT_CLIENT_ID.to_string(),
            credential,
            cached: tokio::sync::Mutex::new(None),
        }
    }

    /// Return a fresh token, exchanging a new one if the cache is empty or stale.
    pub async fn token(&self) -> Result<StateToken, AuthError> {
        let mut guard = self.cached.lock().await;
        if let Some(tok) = guard.as_ref() {
            if tok.is_fresh() {
                return Ok(tok.clone());
            }
        }
        let fresh = self.exchange().await?;
        *guard = Some(fresh.clone());
        Ok(fresh)
    }

    async fn exchange(&self) -> Result<StateToken, AuthError> {
        let resp = self
            .http
            .post(&self.token_url)
            .form(&[
                (
                    "grant_type",
                    "urn:ietf:params:oauth:grant-type:token-exchange",
                ),
                ("subject_token_type", "dbt"),
                ("subject_token", self.credential.token.as_str()),
                ("dbt_hostname", self.credential.host.as_str()),
                ("client_id", self.client_id.as_str()),
            ])
            .send()
            .await?;

        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(AuthError::Exchange {
                status: status.as_u16(),
                body,
            });
        }

        let parsed: TokenExchangeResponse =
            serde_json::from_str(&body).map_err(|e| AuthError::Exchange {
                status: status.as_u16(),
                body: format!("invalid JSON: {e}: {body}"),
            })?;

        let org_id = org_id_from_scope(&parsed.scope)
            .ok_or_else(|| AuthError::NoOrgScope(parsed.scope.clone()))?;

        let expires_at = parsed
            .expires_in
            .filter(|s| *s > 0.0)
            .map(|s| Instant::now() + Duration::from_secs_f64(s));

        Ok(StateToken {
            id_token: parsed.id_token,
            org_id,
            expires_at,
        })
    }
}

/// Extract the org id from a scope string such as
/// `runcache:scope:org:<ORG_ID>:developer`.
fn org_id_from_scope(scope: &str) -> Option<String> {
    for part in scope.split_whitespace() {
        if let Some(rest) = part.strip_prefix(ORG_SCOPE_PREFIX) {
            // rest = "<ORG_ID>:<role>"; org id is up to the next ':'.
            let org = rest.split(':').next().unwrap_or("");
            if !org.is_empty() {
                return Some(org.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_active_project_token() {
        let yaml = r#"
version: "1"
context:
  active-host: "example-account.us1.dbt.com"
  active-project: "22222222222222"
projects:
  - project-name: "Other"
    project-id: "111"
    token-value: "wrong-token"
  - project-name: "Jaffle Shop"
    project-id: "22222222222222"
    token-value: "right-token"
"#;
        let c = DbtCloudCredential::parse(yaml).unwrap();
        assert_eq!(c.host, "example-account.us1.dbt.com");
        assert_eq!(c.project_id, "22222222222222");
        assert_eq!(c.token, "right-token");
    }

    #[test]
    fn extracts_org_id_from_scope() {
        let scope = "runcache:scope:app:act_ABC:developer runcache:scope:org:act_ABC:developer";
        assert_eq!(org_id_from_scope(scope).as_deref(), Some("act_ABC"));
    }

    #[test]
    fn missing_org_scope_returns_none() {
        assert_eq!(org_id_from_scope("runcache:scope:app:x:developer"), None);
    }
}
