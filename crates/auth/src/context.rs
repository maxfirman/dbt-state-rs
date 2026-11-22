//! Authenticating one gRPC request: a valid token, and an
//! `x-organization-id` naming an active Organization the token grants.

use std::time::SystemTime;

use crate::{
    OrganizationId, Organizations, PrincipalId, PrincipalKind, Role, TokenError, TokenVerifier,
};

/// Who made an authenticated request, and in which Organization with which
/// Role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedContext {
    pub principal: PrincipalId,
    pub kind: PrincipalKind,
    pub organization: OrganizationId,
    pub role: Role,
    /// The client the token was issued to (`azp`).
    pub client_id: String,
    /// A person's broker session (`sid`).
    pub session: Option<String>,
}

/// Why the request's Organization was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrganizationRefusal {
    /// No (or an empty) `x-organization-id`.
    MissingHeader,
    /// Not declared in config.
    Unknown,
    /// Declared but disabled in config.
    Disabled,
    /// Active, but not among the token's active entries.
    NotGranted,
}

impl OrganizationRefusal {
    /// The reason recorded in server logs.
    pub fn reason(self) -> &'static str {
        match self {
            Self::MissingHeader => "missing_organization",
            Self::Unknown => "unknown_organization",
            Self::Disabled => "disabled_organization",
            Self::NotGranted => "organization_not_granted",
        }
    }
}

/// The gRPC status class of a [`Rejection`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionCode {
    Unauthenticated,
    PermissionDenied,
}

/// Why a request was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    /// The token failed a check: `UNAUTHENTICATED`.
    Token(TokenError),
    /// A valid token, but not for this Organization: `PERMISSION_DENIED`.
    Organization {
        refusal: OrganizationRefusal,
        principal: PrincipalId,
        client_id: String,
    },
}

impl Rejection {
    pub fn code(&self) -> RejectionCode {
        match self {
            Self::Token(_) => RejectionCode::Unauthenticated,
            Self::Organization { .. } => RejectionCode::PermissionDenied,
        }
    }

    /// The fixed message returned to the client. Organization refusals
    /// share one message, so a response never tells whether an Organization
    /// exists.
    pub fn message(&self) -> &'static str {
        match self {
            Self::Token(error) => error.message(),
            Self::Organization {
                refusal: OrganizationRefusal::MissingHeader,
                ..
            } => "missing x-organization-id metadata",
            Self::Organization { .. } => "access to the organization is denied",
        }
    }

    /// The reason recorded in server logs.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Token(error) => error.reason(),
            Self::Organization { refusal, .. } => refusal.reason(),
        }
    }

    /// The Principal of a valid token, when the Organization was refused.
    pub fn principal(&self) -> Option<&PrincipalId> {
        match self {
            Self::Token(_) => None,
            Self::Organization { principal, .. } => Some(principal),
        }
    }

    /// The client id of a valid token, when the Organization was refused.
    pub fn client_id(&self) -> Option<&str> {
        match self {
            Self::Token(_) => None,
            Self::Organization { client_id, .. } => Some(client_id),
        }
    }
}

/// Authenticates requests against a token verifier and the declared
/// Organizations.
#[derive(Debug, Clone)]
pub struct RequestAuthenticator {
    verifier: TokenVerifier,
    organizations: Organizations,
}

impl RequestAuthenticator {
    pub fn new(verifier: TokenVerifier, organizations: Organizations) -> Self {
        Self {
            verifier,
            organizations,
        }
    }

    /// Authenticate a request from its raw `authorization` and
    /// `x-organization-id` metadata values, at `now`. Token problems come
    /// before Organization problems.
    pub fn authenticate(
        &self,
        authorization: Option<&[u8]>,
        organization: Option<&[u8]>,
        now: SystemTime,
    ) -> Result<AuthenticatedContext, Rejection> {
        let token = bearer_token(authorization).map_err(Rejection::Token)?;
        let token = self.verifier.verify(token, now).map_err(Rejection::Token)?;
        let refuse = |refusal| Rejection::Organization {
            refusal,
            principal: token.principal.clone(),
            client_id: token.client_id.clone(),
        };

        let organization = match organization {
            None | Some(b"") => return Err(refuse(OrganizationRefusal::MissingHeader)),
            Some(value) => std::str::from_utf8(value)
                .ok()
                .and_then(|id| OrganizationId::new(id).ok())
                .ok_or_else(|| refuse(OrganizationRefusal::Unknown))?,
        };
        if self.organizations.is_disabled(&organization) {
            return Err(refuse(OrganizationRefusal::Disabled));
        }
        if !self.organizations.is_active(&organization) {
            return Err(refuse(OrganizationRefusal::Unknown));
        }
        let role = token
            .scope
            .active_role(&organization)
            .ok_or_else(|| refuse(OrganizationRefusal::NotGranted))?;

        Ok(AuthenticatedContext {
            principal: token.principal,
            kind: token.kind,
            organization,
            role,
            client_id: token.client_id,
            session: token.session,
        })
    }
}

/// The token of an `authorization: Bearer <token>` value.
fn bearer_token(authorization: Option<&[u8]>) -> Result<&str, TokenError> {
    let value = match authorization {
        None | Some(b"") => return Err(TokenError::Missing),
        Some(value) => std::str::from_utf8(value).map_err(|_| TokenError::Malformed)?,
    };
    let (scheme, token) = value.split_once(' ').ok_or(TokenError::Malformed)?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return Err(TokenError::Malformed);
    }
    Ok(token)
}
