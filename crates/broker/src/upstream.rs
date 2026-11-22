//! The Token broker's Upstream IdP: endpoints found by OIDC discovery, the
//! `client_credentials` pass-through, the broker's own sign-in as an OIDC
//! relying party (authorization code flow with PKCE, OIDC Core §3.1), and
//! checking the tokens the IdP returns against its published keys.

use std::fmt;
use std::time::SystemTime;

use axum::http::HeaderValue;
use dbt_state_auth::{
    UpstreamClaims, UpstreamConfig, UpstreamCredential, UpstreamLogin, UpstreamTokenError,
    UpstreamTokenVerifier, CLIENT_ASSERTION_TYPE,
};
use serde::Deserialize;
use tokio::sync::OnceCell;

use crate::discovery::Discovery;
use crate::remote_keys::{KeyLocation, RemoteKeys};

/// The Upstream IdP's endpoints and keys, once discovered.
struct Endpoints {
    token_endpoint: String,
    authorization_endpoint: Option<String>,
    keys: RemoteKeys,
    verifier: UpstreamTokenVerifier,
    /// ID tokens for the broker's own client id, when it signs people in.
    id_token_verifier: Option<UpstreamTokenVerifier>,
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

/// What the IdP's `authorization_code` grant returned. `Debug` shows no
/// token.
#[derive(Deserialize)]
pub(crate) struct CodeTokens {
    #[serde(default)]
    pub id_token: Option<String>,
    #[serde(default)]
    pub refresh_token: Option<String>,
}

impl fmt::Debug for CodeTokens {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodeTokens")
            .field("id_token", &self.id_token.is_some())
            .field("refresh_token", &self.refresh_token.is_some())
            .finish()
    }
}

/// The broker's authorization request to the IdP (OIDC Core §3.1.2.1).
pub(crate) struct AuthorizeRequest<'a> {
    pub redirect_uri: &'a str,
    pub state: &'a str,
    pub nonce: &'a str,
    pub code_challenge: &'a str,
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

    /// The claim naming a Principal, with [`Self::issuer`].
    pub(crate) fn subject_claim(&self) -> &str {
        self.config.subject_claim()
    }

    /// The broker's own registration for browser sign-in, if configured.
    pub(crate) fn login(&self) -> Option<&UpstreamLogin> {
        self.config.login()
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
                let authorization_endpoint = discovery.authorization_endpoint;
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
                let id_token_verifier = self.config.login().map(|login| {
                    UpstreamTokenVerifier::new(
                        self.config.issuer().to_owned(),
                        login.client_id().to_owned(),
                        keys.keys(),
                    )
                });
                Ok(Endpoints {
                    token_endpoint,
                    authorization_endpoint,
                    keys,
                    verifier,
                    id_token_verifier,
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
        let body = self
            .post_token(&endpoints.token_endpoint, Some(authorization), &form)
            .await?;
        serde_json::from_slice::<TokenResponse>(&body)
            .map(|token| token.access_token)
            .map_err(|_| {
                UpstreamError::Unavailable("the token response has no access_token".into())
            })
    }

    /// The IdP's authorization endpoint URL for `request`: the broker's
    /// client id, the configured login scope, `state`, `nonce` and a PKCE
    /// S256 challenge.
    pub(crate) async fn authorization_url(
        &self,
        request: &AuthorizeRequest<'_>,
    ) -> Result<String, UpstreamError> {
        let login = self.login().expect("browser sign-in is configured");
        let endpoints = self.endpoints().await?;
        let endpoint = endpoints.authorization_endpoint.as_deref().ok_or_else(|| {
            UpstreamError::Unavailable(format!(
                "OIDC discovery of {} names no authorization_endpoint",
                self.config.issuer()
            ))
        })?;
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("response_type", "code")
            .append_pair("client_id", login.client_id())
            .append_pair("redirect_uri", request.redirect_uri)
            .append_pair("scope", login.scope())
            .append_pair("state", request.state)
            .append_pair("nonce", request.nonce)
            .append_pair("code_challenge", request.code_challenge)
            .append_pair("code_challenge_method", "S256")
            .finish();
        let separator = if endpoint.contains('?') { '&' } else { '?' };
        Ok(format!("{endpoint}{separator}{query}"))
    }

    /// Exchange the IdP's authorization `code` (OIDC Core §3.1.3.1),
    /// authenticating with the broker's own credential.
    pub(crate) async fn exchange_code(
        &self,
        code: &str,
        redirect_uri: &str,
        code_verifier: &str,
        now: SystemTime,
    ) -> Result<CodeTokens, UpstreamError> {
        let login = self.login().expect("browser sign-in is configured");
        let endpoints = self.endpoints().await?;
        let mut form = vec![
            ("grant_type", "authorization_code".to_owned()),
            ("code", code.to_owned()),
            ("redirect_uri", redirect_uri.to_owned()),
            ("code_verifier", code_verifier.to_owned()),
        ];
        let mut authorization = None;
        match login.credential() {
            UpstreamCredential::ClientSecret(secret) => {
                let mut value =
                    HeaderValue::from_str(&secret.basic_authorization(login.client_id()))
                        .map_err(|_| UpstreamError::Unavailable("unusable client secret".into()))?;
                value.set_sensitive(true);
                authorization = Some(value);
            }
            UpstreamCredential::PrivateKeyJwt(key) => {
                form.push(("client_id", login.client_id().to_owned()));
                form.push(("client_assertion_type", CLIENT_ASSERTION_TYPE.to_owned()));
                form.push((
                    "client_assertion",
                    key.assertion(login.client_id(), &endpoints.token_endpoint, now),
                ));
            }
        }
        let body = self
            .post_token(&endpoints.token_endpoint, authorization, &form)
            .await?;
        serde_json::from_slice::<CodeTokens>(&body)
            .map_err(|_| UpstreamError::Unavailable("the token response is not JSON".into()))
    }

    /// POST `form` to the IdP's token endpoint; the body of a 2xx answer.
    async fn post_token<V: serde::Serialize + ?Sized>(
        &self,
        token_endpoint: &str,
        authorization: Option<HeaderValue>,
        form: &V,
    ) -> Result<Vec<u8>, UpstreamError> {
        let mut request = self.http.post(token_endpoint).form(form);
        if let Some(authorization) = authorization {
            request = request.header(reqwest::header::AUTHORIZATION, authorization);
        }
        let response = request
            .send()
            .await
            .map_err(|error| UpstreamError::Unavailable(error.without_url().to_string()))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|error| UpstreamError::Unavailable(error.without_url().to_string()))?;
        if status.is_success() {
            return Ok(body.to_vec());
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
        Ok(checked(endpoints, || {
            endpoints.verifier.verify(token, SystemTime::now())
        })
        .await)
    }

    /// Check an ID token the IdP issued to the broker at sign-in: for its
    /// client id, carrying `nonce`.
    pub(crate) async fn verify_id_token(
        &self,
        token: &str,
        nonce: &str,
        now: SystemTime,
    ) -> Result<Result<UpstreamClaims, UpstreamTokenError>, UpstreamError> {
        let endpoints = self.endpoints().await?;
        let verifier = endpoints
            .id_token_verifier
            .as_ref()
            .expect("browser sign-in is configured");
        Ok(checked(endpoints, || {
            verifier.verify_id_token(token, Some(nonce), now)
        })
        .await)
    }
}

/// `verify()`, after refetching the IdP's keys when stale; a token signed by
/// a key not yet loaded refreshes the keys once (rate-limited) and is
/// checked again.
async fn checked(
    endpoints: &Endpoints,
    verify: impl Fn() -> Result<UpstreamClaims, UpstreamTokenError>,
) -> Result<UpstreamClaims, UpstreamTokenError> {
    endpoints.keys.refresh_if_stale().await;
    let result = verify();
    if matches!(result, Err(UpstreamTokenError::BadSignature)) {
        endpoints.keys.refresh_after_miss().await;
        return verify();
    }
    result
}
