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
    /// An endpoint credentials would be sent to, or keys fetched from, is
    /// neither https nor on loopback.
    #[error("OIDC discovery {url} names a plaintext {member} {endpoint:?}; it must be https")]
    InsecureEndpoint {
        url: String,
        member: &'static str,
        endpoint: String,
    },
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
    /// Fetch and check `issuer`'s metadata: its `issuer` must be `issuer`,
    /// and its token endpoint and `jwks_uri` must pass
    /// [`dbt_state_auth::secure_url`].
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
        let endpoints = [
            ("token_endpoint", discovery.token_endpoint.as_deref()),
            ("jwks_uri", Some(discovery.jwks_uri.as_str())),
        ];
        for (member, endpoint) in endpoints {
            if let Some(endpoint) = endpoint.filter(|e| !dbt_state_auth::secure_url(e)) {
                return Err(DiscoveryError::InsecureEndpoint {
                    url,
                    member,
                    endpoint: endpoint.to_owned(),
                });
            }
        }
        Ok(discovery)
    }
}
