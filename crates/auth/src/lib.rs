//! Auth core shared by the dbt State gRPC server, the Token broker and the
//! UI: the `scope` format, the TOML auth config, token issuing and checking,
//! and the authenticated context of a request (Principal, Principal kind,
//! Organization and Role).

mod config;
mod context;
mod credential;
mod keys;
mod mapping;
mod organization;
mod scope;
mod sealing;
mod token;
mod ui_client;
mod upstream;

pub use config::{
    secure_url, AuthConfig, ConfigError, TokenLifetimes, UpstreamConfig, UpstreamLogin,
    DEFAULT_DBT_CLIENT_ID,
};
pub use context::{
    AuthenticatedContext, OrganizationRefusal, Rejection, RejectionCode, RequestAuthenticator,
    SessionRefusal,
};
pub use credential::{ClientAssertionKey, ClientSecret, UpstreamCredential, CLIENT_ASSERTION_TYPE};
pub use keys::{JwksError, KeyFileError, VerificationKeys};
pub use mapping::{MappingRule, MappingRules, UpstreamClaims};
pub use organization::{
    DuplicateOrganization, InvalidOrganizationId, Organization, OrganizationId, Organizations,
    Role, UnknownRole,
};
pub use scope::{Scope, ScopeError};
pub use sealing::{RefreshTokenKey, SealingKeyError};
pub use token::{
    IssuedToken, OidcTokens, PrincipalId, PrincipalKind, TokenError, TokenIssuer, TokenRequest,
    TokenVerifier, VerifiedToken, CLOCK_SKEW,
};
pub use ui_client::{
    ClientAssertionError, PublicKeyFileError, UiClient, VerifiedAssertion, MAX_ASSERTION_LIFETIME,
};
pub use upstream::{UpstreamTokenError, UpstreamTokenVerifier};
