//! The Token broker: speaks the dbt client's token protocol and delegates
//! identity to the Upstream IdP (ADR 0012).
//!
//! It serves, over HTTP/1.1:
//! - `GET /.well-known/openid-configuration`: OIDC discovery;
//! - `GET /jwks`: the public halves of its `[signing]` keys;
//! - `GET /`: the authorization endpoint, where the dbt client's browser
//!   sign-in starts, and `GET /callback`, where the Upstream IdP returns
//!   (`sign_in`);
//! - `POST /token`: `client_credentials` pass-through for machines
//!   (`token_endpoint`), and the browser sign-in's `authorization_code`
//!   exchange (`code_grant`). Token exchange is refused.
//!
//! Its state (pending logins, person sessions, authorization codes and
//! refresh tokens) lives in the server's Postgres schema, migrated from
//! `crates/server/migrations` ([`MIGRATOR`]), so an in-process broker and
//! standalone replicas share it.
//!
//! It runs in the gRPC server's process on its own listener
//! (`DBT_STATE_BROKER_LISTEN`), sharing the server's key set, or as the
//! standalone `dbt-state-broker` binary, whose keys a gRPC server without
//! `[signing]` fetches and caches ([`RemoteKeys`]).
//!
//! It holds no credentials of the Principals it serves: a machine's client
//! secret is forwarded to the Upstream IdP and never stored or logged, and
//! a person's upstream refresh token is stored only encrypted.
//!
//! It records each Principal it issues a token to (`principals`), keyed by
//! Upstream IdP issuer and subject, before issuing the token.

mod clock;
mod code_grant;
mod discovery;
mod oauth;
mod remote_keys;
mod secret;
mod sign_in;
mod store;
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
use sqlx::PgPool;

pub use clock::Clock;
pub use discovery::{Discovery, DiscoveryError};
pub use remote_keys::{KeyLocation, RemoteKeys, MAX_AGE, MIN_INTERVAL};

use crate::store::Store;
use crate::upstream::UpstreamIdp;

/// The schema the Token broker's state lives in: the server's migrations,
/// which a standalone broker runs at startup as the server does.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../server/migrations");

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
    store: Store,
    clock: Clock,
    /// The dbt client registration's client id.
    dbt_client_id: String,
    pending_logins_per_ip: u32,
    client_ip_header: Option<String>,
}

impl Broker {
    /// Check that `config` can run a Token broker: it needs `[signing]` and
    /// `[upstream]`.
    pub fn check(config: &AuthConfig) -> Result<(), BrokerConfigError> {
        config
            .token_issuer()
            .ok_or(BrokerConfigError::NoSigningKeys)?;
        config.upstream().ok_or(BrokerConfigError::NoUpstream)?;
        Ok(())
    }

    /// The broker `config` describes (see [`Self::check`]), keeping its
    /// state in `pool`'s migrated schema. Makes no request: the Upstream
    /// IdP is discovered on first use.
    pub fn new(config: &AuthConfig, pool: PgPool) -> Result<Self, BrokerConfigError> {
        Self::with_clock(config, pool, Clock::system())
    }

    /// [`Self::new`] on `clock` instead of the host's wall clock.
    pub fn with_clock(
        config: &AuthConfig,
        pool: PgPool,
        clock: Clock,
    ) -> Result<Self, BrokerConfigError> {
        Self::check(config)?;
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
            store: Store::new(pool),
            clock,
            dbt_client_id: config.dbt_client_id().to_owned(),
            pending_logins_per_ip: config.pending_logins_per_ip(),
            client_ip_header: config.client_ip_header().map(str::to_owned),
        })))
    }

    /// The broker's HTTP routes. Serve them with the peer address
    /// (`into_make_service_with_connect_info::<SocketAddr>`), as [`serve`]
    /// does: pending logins are capped per client IP.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/", get(sign_in::authorize))
            .route("/callback", get(sign_in::callback))
            .route("/.well-known/openid-configuration", get(discovery_document))
            .route("/jwks", get(jwks))
            .route("/token", post(token_endpoint::token))
            .with_state(self.clone())
    }

    /// The issuer without a trailing `/`: the base of every endpoint URL.
    fn base(&self) -> &str {
        self.0.issuer.trim_end_matches('/')
    }

    /// Where the Upstream IdP sends the browser back: register this
    /// redirect URI for the broker's client id at the IdP.
    fn callback_url(&self) -> String {
        format!("{}/callback", self.base())
    }
}

/// Serve `broker` on `listener` until the process stops.
pub async fn serve(listener: tokio::net::TcpListener, broker: Broker) -> std::io::Result<()> {
    axum::serve(
        listener,
        broker
            .router()
            .into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
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
    let base = broker.base();
    // Hosted dbt State's authorization endpoint is its issuer's root, which
    // is also the dbt client's default auth URL (`docs/reports/
    // hosted-auth-2026-11-21.md` §1).
    Json(json!({
        "issuer": broker.0.issuer,
        "authorization_endpoint": format!("{base}/"),
        "token_endpoint": format!("{base}/token"),
        "jwks_uri": format!("{base}/jwks"),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "client_credentials"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none", "client_secret_basic"],
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
