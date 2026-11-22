//! Token endpoint responses: the OAuth 2.0 error shape (RFC 6749 §5.2) with
//! fixed descriptions, and the no-store headers every token response
//! carries (§5.1).

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

/// An OAuth error the token endpoint returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OAuthError {
    /// The form is malformed or lacks a parameter.
    InvalidRequest,
    /// Client authentication failed: no HTTP Basic credentials, or the
    /// Upstream IdP refused them.
    InvalidClient,
    /// The client authenticated but gets no dbt State token: the Upstream
    /// IdP refused the grant, its token cannot be trusted, or no mapping
    /// rule grants an Organization.
    UnauthorizedClient,
    UnsupportedGrantType,
    /// dbt platform token exchange (`unsupported_grant_type`, with its own
    /// description).
    TokenExchange,
    /// The Upstream IdP is unreachable or failing.
    TemporarilyUnavailable,
}

impl OAuthError {
    fn status(self) -> StatusCode {
        match self {
            Self::InvalidClient => StatusCode::UNAUTHORIZED,
            Self::TemporarilyUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    fn code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidClient => "invalid_client",
            Self::UnauthorizedClient => "unauthorized_client",
            Self::UnsupportedGrantType | Self::TokenExchange => "unsupported_grant_type",
            Self::TemporarilyUnavailable => "temporarily_unavailable",
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::InvalidRequest => "the token request is malformed",
            Self::InvalidClient => "client authentication failed",
            Self::UnauthorizedClient => "the client is not authorized for dbt State",
            Self::UnsupportedGrantType => "the grant type is not supported",
            Self::TokenExchange => {
                "dbt platform token exchange is not supported; authenticate with a client id \
                 and secret, or sign in through the browser"
            }
            Self::TemporarilyUnavailable => "the identity provider is unavailable; try again later",
        }
    }
}

impl IntoResponse for OAuthError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status(),
            Json(json!({
                "error": self.code(),
                "error_description": self.description(),
            })),
        )
            .into_response();
        if self == Self::InvalidClient {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"dbt-state\""),
            );
        }
        no_store(response)
    }
}

/// `response` with `Cache-Control: no-store` and `Pragma: no-cache`.
pub(crate) fn no_store(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    response
}
