//! The Token broker: speaks the dbt client's token protocol and delegates
//! identity to the Upstream IdP (ADR 0012).
//!
//! It serves, over HTTP/1.1:
//! - `GET /.well-known/openid-configuration`: OIDC discovery;
//! - `GET /jwks`: the public halves of its `[signing]` keys;
//! - `POST /token`: `client_credentials` pass-through for machines
//!   (`token_endpoint`). Token exchange is refused.
//!
//! It runs in the gRPC server's process on its own listener
//! (`DBT_STATE_BROKER_LISTEN`), sharing the server's key set, or as the
//! standalone `dbt-state-broker` binary, whose keys a gRPC server without
//! `[signing]` fetches and caches ([`RemoteKeys`]).
//!
//! It holds no credentials of the Principals it serves: a machine's client
//! secret is forwarded to the Upstream IdP and never stored or logged.
//!
//! With a database ([`Broker::with_database`]), it records each Principal it
//! issues a token to (`principals`), keyed by Upstream IdP issuer and
//! subject, before issuing the token.

mod discovery;
mod oauth;
mod principals;
mod remote_keys;
mod token_endpoint;
mod upstream;

use std::sync::Arc;
use std::time::Duration;

use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use dbt_state_auth::{AuthConfig, MappingRules, Organizations, TokenIssuer, VerificationKeys};
use serde_json::json;

pub use discovery::{Discovery, DiscoveryError};
pub use remote_keys::{KeyLocation, RemoteKeys, MAX_AGE, MIN_INTERVAL};

use crate::upstream::UpstreamIdp;

/// Why an auth config cannot run a Token broker.
#[derive(Debug, thiserror::Error)]
pub enum BrokerConfigError {
    #[error("the Token broker signs tokens: the auth config needs [signing]")]
    NoSigningKeys,
    #[error("the Token broker needs an Upstream IdP: the auth config needs [upstream]")]
    NoUpstream,
    #[error("cannot build the Token broker's HTTP client: {0}")]
    Http(#[from] reqwest::Error),
}

/// A Token broker over one auth config. Cheap to clone.
#[derive(Clone)]
pub struct Broker(Arc<Inner>);

struct Inner {
    issuer: String,
    tokens: TokenIssuer,
    keys: VerificationKeys,
    organizations: Organizations,
    rules: MappingRules,
    upstream: UpstreamIdp,
    /// Where Principals are recorded; `None` records none.
    database: Option<sqlx::PgPool>,
}

impl Broker {
    /// The broker `config` describes; it needs `[signing]` and `[upstream]`.
    /// Makes no request: the Upstream IdP is discovered on first use. It
    /// records no Principals.
    pub fn new(config: &AuthConfig) -> Result<Self, BrokerConfigError> {
        Self::build(config, None)
    }

    /// [`Broker::new`], recording each Principal it issues a token to in
    /// `database`'s `principals` table (migrated by the server). A token is
    /// issued only once its Principal is recorded.
    pub fn with_database(
        config: &AuthConfig,
        database: sqlx::PgPool,
    ) -> Result<Self, BrokerConfigError> {
        Self::build(config, Some(database))
    }

    fn build(
        config: &AuthConfig,
        database: Option<sqlx::PgPool>,
    ) -> Result<Self, BrokerConfigError> {
        let tokens = config
            .token_issuer()
            .ok_or(BrokerConfigError::NoSigningKeys)?;
        let keys = config
            .verification_keys()
            .ok_or(BrokerConfigError::NoSigningKeys)?;
        let upstream = config
            .upstream()
            .ok_or(BrokerConfigError::NoUpstream)?
            .clone();
        Ok(Self(Arc::new(Inner {
            issuer: config.broker_issuer().to_owned(),
            tokens,
            keys,
            organizations: config.organizations().clone(),
            rules: config.mapping_rules().clone(),
            upstream: UpstreamIdp::new(upstream, http_client()?),
            database,
        })))
    }

    /// The broker's HTTP routes.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/.well-known/openid-configuration", get(discovery_document))
            .route("/jwks", get(jwks))
            .route("/token", post(token_endpoint::token))
            .with_state(self.clone())
    }
}

/// Serve `broker` on `listener` until the process stops.
pub async fn serve(listener: tokio::net::TcpListener, broker: Broker) -> std::io::Result<()> {
    axum::serve(listener, broker.router()).await
}

/// The HTTP client for the Upstream IdP and JWKS endpoints: bounded
/// timeouts, and no redirects, so credentials never follow one.
pub fn http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

async fn discovery_document(
    axum::extract::State(broker): axum::extract::State<Broker>,
) -> Json<serde_json::Value> {
    let issuer = &broker.0.issuer;
    let base = issuer.trim_end_matches('/');
    Json(json!({
        "issuer": issuer,
        "token_endpoint": format!("{base}/token"),
        "jwks_uri": format!("{base}/jwks"),
        "grant_types_supported": ["client_credentials"],
        "token_endpoint_auth_methods_supported": ["client_secret_basic"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "subject_types_supported": ["public"],
    }))
}

async fn jwks(axum::extract::State(broker): axum::extract::State<Broker>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/jwk-set+json")],
        broker.0.keys.to_jwks(),
    )
}
