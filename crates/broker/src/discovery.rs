//! OIDC discovery (OpenID Connect Discovery 1.0 §4): an issuer's metadata at
//! `<issuer>/.well-known/openid-configuration`.

use serde::Deserialize;

/// The discovery metadata the Token broker uses.
#[derive(Debug, Clone, Deserialize)]
pub struct Discovery {
    pub issuer: String,
    #[serde(default)]
    pub token_endpoint: Option<String>,
    pub jwks_uri: String,
}

/// Why discovery failed.
#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("cannot fetch OIDC discovery {url}: {error}")]
    Http { url: String, error: reqwest::Error },
    /// §4.3: the metadata's `issuer` must be the issuer it was fetched for.
    #[error("OIDC discovery {url} names issuer {found:?}, not {expected:?}")]
    WrongIssuer {
        url: String,
        expected: String,
        found: String,
    },
}

/// The discovery document URL of `issuer`.
pub fn url(issuer: &str) -> String {
    format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    )
}

impl Discovery {
    /// Fetch and check `issuer`'s metadata.
    pub async fn fetch(http: &reqwest::Client, issuer: &str) -> Result<Self, DiscoveryError> {
        let url = url(issuer);
        let discovery: Self = async {
            http.get(&url)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await
        }
        .await
        .map_err(|error| DiscoveryError::Http {
            url: url.clone(),
            error,
        })?;
        if discovery.issuer != issuer {
            return Err(DiscoveryError::WrongIssuer {
                url,
                expected: issuer.to_owned(),
                found: discovery.issuer,
            });
        }
        Ok(discovery)
    }
}
