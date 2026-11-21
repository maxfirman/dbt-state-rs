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

use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;

use dbt_state_auth::{RejectionCode, RequestAuthenticator};
use tonic::body::Body;
use tonic::codegen::http;
use tonic::Status;

pub use dbt_state_auth::AuthenticatedContext;

/// Calls under this path prefix need no token.
const HEALTH_SERVICE: &str = "/grpc.health.v1.Health/";
const AUTHORIZATION: &str = "authorization";
const ORGANIZATION: &str = "x-organization-id";

/// Installs [`GrpcAuth`] around the router's services.
#[derive(Clone)]
pub struct GrpcAuthLayer {
    authenticator: Option<Arc<RequestAuthenticator>>,
}

impl GrpcAuthLayer {
    /// `oidc` mode when `authenticator` is set, else `none` mode.
    pub(crate) fn new(authenticator: Option<Arc<RequestAuthenticator>>) -> Self {
        Self { authenticator }
    }
}

impl<S> tower::Layer<S> for GrpcAuthLayer {
    type Service = GrpcAuth<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcAuth {
            inner,
            authenticator: self.authenticator.clone(),
        }
    }
}

/// See the module docs.
#[derive(Clone)]
pub struct GrpcAuth<S> {
    inner: S,
    authenticator: Option<Arc<RequestAuthenticator>>,
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
        let Some(authenticator) = self.authenticator.clone() else {
            return Box::pin(inner.call(request));
        };
        if request.uri().path().starts_with(HEALTH_SERVICE) {
            return Box::pin(inner.call(request));
        }
        Box::pin(async move {
            let headers = request.headers();
            let result = authenticator.authenticate(
                headers.get(AUTHORIZATION).map(|v| v.as_bytes()),
                headers.get(ORGANIZATION).map(|v| v.as_bytes()),
                SystemTime::now(),
            );
            match result {
                Ok(context) => {
                    request.extensions_mut().insert(context);
                    inner.call(request).await
                }
                Err(rejection) => {
                    let organization = request
                        .headers()
                        .get(ORGANIZATION)
                        .and_then(|v| v.to_str().ok());
                    tracing::warn!(
                        rpc = request.uri().path(),
                        reason = rejection.reason(),
                        principal = rejection.principal().map(|p| p.as_str()),
                        client_id = rejection.client_id(),
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
