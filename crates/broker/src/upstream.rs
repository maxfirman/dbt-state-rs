//! The Token broker's Upstream IdP: endpoints found by OIDC discovery, the
//! `client_credentials` pass-through, and checking the access tokens it
//! returns against its published keys.

use std::time::SystemTime;

use axum::http::HeaderValue;
use dbt_state_auth::{UpstreamClaims, UpstreamConfig, UpstreamTokenError, UpstreamTokenVerifier};
use serde::Deserialize;
use tokio::sync::OnceCell;

use crate::discovery::Discovery;
use crate::remote_keys::{KeyLocation, RemoteKeys};

/// The Upstream IdP's endpoints and keys, once discovered.
struct Endpoints {
    token_endpoint: String,
    keys: RemoteKeys,
    verifier: UpstreamTokenVerifier,
}

/// See the module docs.
pub(crate) struct UpstreamIdp {
    config: UpstreamConfig,
    http: reqwest::Client,
    /// Discovered on first use; a failed discovery is retried on the next.
    endpoints: OnceCell<Endpoints>,
}

/// Why the Upstream IdP issued no access token.
#[derive(Debug)]
pub(crate) enum UpstreamError {
    /// The IdP refused the client's credentials (HTTP 401, or
    /// `invalid_client`).
    RejectedClient { error: Option<String> },
    /// The IdP refused the grant for another reason (another 4xx).
    Refused { status: u16, error: Option<String> },
    /// The IdP could not be reached, failed, or answered nonsense.
    Unavailable(String),
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct ErrorResponse {
    error: Option<String>,
}

impl UpstreamIdp {
    pub(crate) fn new(config: UpstreamConfig, http: reqwest::Client) -> Self {
        Self {
            config,
            http,
            endpoints: OnceCell::new(),
        }
    }

    pub(crate) fn issuer(&self) -> &str {
        self.config.issuer()
    }

    async fn endpoints(&self) -> Result<&Endpoints, UpstreamError> {
        self.endpoints
            .get_or_try_init(|| async {
                let discovery = Discovery::fetch(&self.http, self.config.issuer())
                    .await
                    .map_err(|error| UpstreamError::Unavailable(error.to_string()))?;
                let token_endpoint = discovery.token_endpoint.ok_or_else(|| {
                    UpstreamError::Unavailable(format!(
                        "OIDC discovery of {} names no token_endpoint",
                        self.config.issuer()
                    ))
                })?;
                let keys =
                    RemoteKeys::new(KeyLocation::Jwks(discovery.jwks_uri), self.http.clone());
                if !keys.refresh().await {
                    return Err(UpstreamError::Unavailable(
                        "the Upstream IdP's signing keys could not be loaded".into(),
                    ));
                }
                let verifier = UpstreamTokenVerifier::new(
                    self.config.issuer().to_owned(),
                    self.config.api_audience().to_owned(),
                    keys.keys(),
                );
                Ok(Endpoints {
                    token_endpoint,
                    keys,
                    verifier,
                })
            })
            .await
    }

    /// The access token the IdP's `client_credentials` grant returns for the
    /// client whose HTTP Basic `authorization` the dbt client sent. The
    /// header is forwarded as received, so the secret is never decoded,
    /// kept or logged here; the configured upstream scope is requested.
    pub(crate) async fn client_credentials(
        &self,
        authorization: &HeaderValue,
    ) -> Result<String, UpstreamError> {
        let endpoints = self.endpoints().await?;
        let mut form = vec![("grant_type", "client_credentials")];
        if let Some(scope) = self.config.client_credentials_scope() {
            form.push(("scope", scope));
        }
        let mut authorization = authorization.clone();
        authorization.set_sensitive(true);
        let response = self
            .http
            .post(&endpoints.token_endpoint)
            .header(reqwest::header::AUTHORIZATION, authorization)
            .form(&form)
            .send()
            .await
            .map_err(|error| UpstreamError::Unavailable(error.without_url().to_string()))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|error| UpstreamError::Unavailable(error.without_url().to_string()))?;
        if status.is_success() {
            return serde_json::from_slice::<TokenResponse>(&body)
                .map(|token| token.access_token)
                .map_err(|_| {
                    UpstreamError::Unavailable("the token response has no access_token".into())
                });
        }
        let error = serde_json::from_slice::<ErrorResponse>(&body)
            .ok()
            .and_then(|body| body.error);
        if status.as_u16() == 401 || error.as_deref() == Some("invalid_client") {
            Err(UpstreamError::RejectedClient { error })
        } else if status.is_client_error() {
            Err(UpstreamError::Refused {
                status: status.as_u16(),
                error,
            })
        } else {
            Err(UpstreamError::Unavailable(format!("HTTP {status}")))
        }
    }

    /// Check an access token the IdP issued. A token signed by a key not yet
    /// loaded refreshes the keys once (rate-limited) and is checked again.
    pub(crate) async fn verify(
        &self,
        token: &str,
    ) -> Result<Result<UpstreamClaims, UpstreamTokenError>, UpstreamError> {
        let endpoints = self.endpoints().await?;
        endpoints.keys.refresh_if_stale().await;
        let result = endpoints.verifier.verify(token, SystemTime::now());
        if matches!(result, Err(UpstreamTokenError::BadSignature)) {
            endpoints.keys.refresh_after_miss().await;
            return Ok(endpoints.verifier.verify(token, SystemTime::now()));
        }
        Ok(result)
    }
}
