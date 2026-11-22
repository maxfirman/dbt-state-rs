//! The request scope of one gRPC call: the Organization it reads and writes
//! State history in, the Principal who made it, and the dbt invocation and
//! client session it belongs to.
//!
//! Every handler that scopes by Organization resolves one [`RequestScope`]
//! before it touches the store, and uses only it: store queries are scoped by
//! its Organization, and the decisions and executions it records name its
//! Principal, invocation and session. No handler or capture reads these
//! metadata headers or the authenticated context itself. [`organization`]
//! is the only place the Organization is resolved: in `oidc` mode it is the
//! authenticated context's (`grpc_auth`), never the raw header. The
//! Principal is the authenticated context's too; `none` mode has none.

use dbt_state_auth::PrincipalId;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

use crate::grpc_auth::AuthenticatedContext;
use crate::AppState;

/// The Organization of a request that names none (the insecure local flow).
const DEFAULT_ORGANIZATION: &str = "local";
const ORGANIZATION_HEADER: &str = "x-organization-id";
const INVOCATION_HEADER: &str = "x-dbt-invocation-id";
const SESSION_HEADER: &str = "x-session-id";

/// Who and what one gRPC call is for. Built only by [`RequestScope::resolve`].
#[derive(Debug, Clone)]
pub(crate) struct RequestScope {
    organization: String,
    principal: Option<PrincipalId>,
    invocation_id: Option<String>,
    session_id: Option<String>,
}

impl RequestScope {
    /// Resolve the scope of `request`, or the status that rejects it.
    pub(crate) fn resolve<T>(state: &AppState, request: &Request<T>) -> Result<Self, Status> {
        let metadata = request.metadata();
        Ok(Self {
            organization: organization(state, request)?,
            principal: request
                .extensions()
                .get::<AuthenticatedContext>()
                .map(|context| context.principal.clone()),
            invocation_id: header(metadata, INVOCATION_HEADER),
            session_id: header(metadata, SESSION_HEADER),
        })
    }

    /// The Organization every store query of this request is scoped by.
    pub(crate) fn organization(&self) -> &str {
        &self.organization
    }

    /// The authenticated Principal (`oidc` mode); `None` in `none` mode.
    pub(crate) fn principal(&self) -> Option<&PrincipalId> {
        self.principal.as_ref()
    }

    /// The client's `x-dbt-invocation-id`, if sent.
    pub(crate) fn invocation_id(&self) -> Option<&str> {
        self.invocation_id.as_deref()
    }

    /// The client's `x-session-id`, if sent.
    pub(crate) fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }
}

/// The request's Organization.
///
/// In `oidc` mode, the Organization the authentication layer accepted for
/// this request; a request that reached a handler without one is
/// `UNAUTHENTICATED`.
///
/// In `none` mode, a non-empty `x-organization-id`, else `local`. When the
/// deployment requires the header (`AppState::require_org_header`), a
/// missing or empty one is `UNAUTHENTICATED`. This enforces that a request is
/// scoped, not that its caller belongs to the Organization it names.
fn organization<T>(state: &AppState, request: &Request<T>) -> Result<String, Status> {
    if state.oidc.is_some() {
        return request
            .extensions()
            .get::<AuthenticatedContext>()
            .map(|context| context.organization.as_str().to_owned())
            .ok_or_else(|| Status::unauthenticated("missing bearer token"));
    }
    match header(request.metadata(), ORGANIZATION_HEADER).filter(|org| !org.is_empty()) {
        Some(org) => Ok(org),
        None if state.require_org_header => Err(Status::unauthenticated(
            "missing required x-organization-id metadata header",
        )),
        None => Ok(DEFAULT_ORGANIZATION.to_owned()),
    }
}

/// A metadata header's value, if present and valid ASCII.
fn header(metadata: &MetadataMap, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}
