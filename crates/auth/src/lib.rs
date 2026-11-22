//! Auth core shared by the dbt State gRPC server, the Token broker and the
//! UI: the `scope` format, the TOML auth config, token issuing and checking,
//! and the authenticated context of a request (Principal, Principal kind,
//! Organization and Role).

mod config;
mod context;
mod keys;
mod organization;
mod scope;
mod token;

pub use config::{AuthConfig, ConfigError, TokenLifetimes};
pub use context::{
    AuthenticatedContext, OrganizationRefusal, Rejection, RejectionCode, RequestAuthenticator,
};
pub use keys::KeyFileError;
pub use organization::{
    DuplicateOrganization, InvalidOrganizationId, Organization, OrganizationId, Organizations,
    Role, UnknownRole,
};
pub use scope::{Scope, ScopeError};
pub use token::{
    IssuedToken, PrincipalId, PrincipalKind, TokenError, TokenIssuer, TokenRequest, TokenVerifier,
    VerifiedToken, CLOCK_SKEW,
};
