//! The gRPC authentication layer, in front of every service except
//! `grpc.health.v1`.
//!
//! In `oidc` mode it authenticates each call (`dbt_state_auth`): a valid
//! token, then an `x-organization-id` naming an active Organization the token
//! grants. An accepted call carries its [`AuthenticatedContext`] in the
//! request extensions, where `RequestScope::resolve` takes the Organization
//! from. A refused call never reaches a handler: it gets `UNAUTHENTICATED`
//! or `PERMISSION_DENIED` with a fixed message, and the refusal is logged at
//! `warn` with its reason, never with the token. In `none` mode the layer
//! passes every call through.
//!
//! Without `[signing]` the keys are a standalone Token broker's published
//! ones ([`RemoteKeys`]): refetched when older than
//! `dbt_state_broker::MAX_AGE`, and when a token's signature fails to
//! verify (a rotation), at most once per `dbt_state_broker::MIN_INTERVAL`.
//!
//! A person token (one with a `sid`) also needs an active broker session:
//! not revoked and not past its 12-hour cap ([`SessionCheck`], read from
//! the Postgres the Token broker shares, each status cached on this
//! replica for at most `dbt_state_broker::SESSION_STATUS_MAX_AGE`, 30 s).
//! An inactive session is `UNAUTHENTICATED`. When the status cannot be
//! read the call fails closed with `UNAVAILABLE`, which the client may
//! retry. Machine tokens carry no `sid` and are not checked.

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;

use dbt_state_auth::{AuthConfig, Rejection, RejectionCode, RequestAuthenticator, TokenError};
use dbt_state_broker::{Clock, KeyLocation, RemoteKeys, SessionCheck, SessionCheckError};
use sqlx::PgPool;
use tonic::body::Body;
use tonic::codegen::http;
use tonic::Status;

pub use dbt_state_auth::AuthenticatedContext;

/// Calls under this path prefix need no token.
const HEALTH_SERVICE: &str = "/grpc.health.v1.Health/";
const AUTHORIZATION: &str = "authorization";
const ORGANIZATION: &str = "x-organization-id";

/// How `oidc` mode authenticates calls.
pub(crate) struct Oidc {
    authenticator: RequestAuthenticator,
    /// The Token broker's keys, when they are fetched rather than loaded.
    remote: Option<RemoteKeys>,
    /// Person sessions, in `pool`'s schema.
    sessions: SessionCheck,
}

/// Why `oidc` mode refused a call.
enum Refusal {
    Rejected(Rejection),
    /// A valid person token whose session status could not be read.
    SessionUnavailable {
        context: Box<AuthenticatedContext>,
        error: sqlx::Error,
    },
}

impl Oidc {
    /// Checks calls against `config`, and person sessions in `pool` on
    /// `clock`.
    pub(crate) fn new(
        config: &AuthConfig,
        pool: PgPool,
        clock: Clock,
    ) -> Result<Self, reqwest::Error> {
        let sessions = SessionCheck::new(pool, clock);
        Ok(match config.verification_keys() {
            Some(keys) => Self {
                authenticator: config.request_authenticator(keys),
                remote: None,
                sessions,
            },
            None => {
                let location = match config.broker_jwks_url() {
                    Some(url) => KeyLocation::Jwks(url.to_owned()),
                    None => KeyLocation::Discovery(config.broker_issuer().to_owned()),
                };
                let remote = RemoteKeys::new(location, dbt_state_broker::http_client()?);
                Self {
                    authenticator: config.request_authenticator(remote.keys()),
                    remote: Some(remote),
                    sessions,
                }
            }
        })
    }

    /// The token and Organization, then a person token's session.
    async fn authenticate(
        &self,
        authorization: Option<&[u8]>,
        organization: Option<&[u8]>,
    ) -> Result<AuthenticatedContext, Refusal> {
        let context = self
            .authenticate_token(authorization, organization)
            .await
            .map_err(Refusal::Rejected)?;
        let Some(session) = &context.session else {
            return Ok(context);
        };
        match self.sessions.check(session).await {
            Ok(()) => Ok(context),
            Err(SessionCheckError::Inactive(refusal)) => {
                Err(Refusal::Rejected(Rejection::Session {
                    refusal,
                    session: session.clone(),
                    principal: context.principal,
                    client_id: context.client_id,
                }))
            }
            Err(SessionCheckError::Unavailable(error)) => Err(Refusal::SessionUnavailable {
                context: Box::new(context),
                error,
            }),
        }
    }

    async fn authenticate_token(
        &self,
        authorization: Option<&[u8]>,
        organization: Option<&[u8]>,
    ) -> Result<AuthenticatedContext, Rejection> {
        if let Some(remote) = &self.remote {
            remote.refresh_if_stale().await;
        }
        let result =
            self.authenticator
                .authenticate(authorization, organization, SystemTime::now());
        match (&result, &self.remote) {
            (Err(Rejection::Token(TokenError::BadSignature)), Some(remote)) => {
                remote.refresh_after_miss().await;
                self.authenticator
                    .authenticate(authorization, organization, SystemTime::now())
            }
            _ => result,
        }
    }
}

/// Installs [`GrpcAuth`] around the router's services.
#[derive(Clone)]
pub struct GrpcAuthLayer {
    oidc: Option<Arc<Oidc>>,
}

impl GrpcAuthLayer {
    /// `oidc` mode when `oidc` is set, else `none` mode.
    pub(crate) fn new(oidc: Option<Arc<Oidc>>) -> Self {
        Self { oidc }
    }
}

impl<S> tower::Layer<S> for GrpcAuthLayer {
    type Service = GrpcAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcAuth {
            inner,
            oidc: self.oidc.clone(),
        }
    }
}

/// See the module docs.
#[derive(Clone)]
pub struct GrpcAuth<S> {
    inner: S,
    oidc: Option<Arc<Oidc>>,
}

impl<S> tower::Service<http::Request<Body>> for GrpcAuth<S>
where
    S: tower::Service<http::Request<Body>, Response = http::Response<Body>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        // Call the service that was polled ready; leave a fresh clone behind.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let Some(oidc) = self.oidc.clone() else {
            return Box::pin(inner.call(request));
        };
        if request.uri().path().starts_with(HEALTH_SERVICE) {
            return Box::pin(inner.call(request));
        }
        Box::pin(async move {
            let headers = request.headers();
            let result = oidc
                .authenticate(
                    headers.get(AUTHORIZATION).map(|v| v.as_bytes()),
                    headers.get(ORGANIZATION).map(|v| v.as_bytes()),
                )
                .await;
            match result {
                Ok(context) => {
                    request.extensions_mut().insert(context);
                    inner.call(request).await
                }
                Err(Refusal::SessionUnavailable { context, error }) => {
                    tracing::warn!(
                        rpc = request.uri().path(),
                        reason = "session_status_unavailable",
                        principal = context.principal.as_str(),
                        client_id = context.client_id,
                        session = context.session,
                        organization = context.organization.as_str(),
                        %error,
                        "gRPC request refused"
                    );
                    Ok(Status::unavailable("session status unavailable").into_http())
                }
                Err(Refusal::Rejected(rejection)) => {
                    let organization = request
                        .headers()
                        .get(ORGANIZATION)
                        .and_then(|v| v.to_str().ok());
                    tracing::warn!(
                        rpc = request.uri().path(),
                        reason = rejection.reason(),
                        principal = rejection.principal().map(|p| p.as_str()),
                        client_id = rejection.client_id(),
                        session = rejection.session(),
                        organization,
                        "gRPC request refused"
                    );
                    let status = match rejection.code() {
                        RejectionCode::Unauthenticated => {
                            Status::unauthenticated(rejection.message())
                        }
                        RejectionCode::PermissionDenied => {
                            Status::permission_denied(rejection.message())
                        }
                    };
                    Ok(status.into_http())
                }
            }
        })
    }
}
