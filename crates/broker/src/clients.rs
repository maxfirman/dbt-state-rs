//! The Token broker's registered clients (spec #215 "Registered clients"):
//!
//! - the **dbt client** (`[clients.dbt]`), public: redirect URI exactly
//!   `http://127.0.0.1:29525/handler`, no client authentication at the
//!   token endpoint (`none`), and the dbt-shaped person token (`id_token`
//!   for the dbt State API audience) with a refresh token;
//! - the **UI** (`[clients.ui]`), confidential: its one registered
//!   callback, an authorize request with `openid` and a `nonce`,
//!   `private_key_jwt` at the token endpoint (RFC 7523 §2.2, OIDC Core §9),
//!   and standard OIDC tokens for its own audience: an `id_token` echoing
//!   the nonce (none on refresh, OIDC Core §12.2) and an `access_token`,
//!   both with `aud` the UI's client id, so no gRPC RPC accepts them, and a
//!   refresh token renewing the same person session.
//!
//! Both use PKCE S256 and the same sign-in, code and person session
//! machinery; a code or refresh token is bound to the client it was issued
//! to, so a UI login never yields a dbt-shaped token.

use std::collections::BTreeMap;

use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use dbt_state_auth::{TokenRequest, CLIENT_ASSERTION_TYPE};
use serde_json::json;

use crate::oauth::{no_store, OAuthError};
use crate::secret;
use crate::sign_in::DBT_CLIENT_REDIRECT_URI;
use crate::Broker;

/// A registered client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Client {
    Dbt,
    Ui,
}

/// A registered client.
#[derive(Clone, Copy)]
pub(crate) struct Registration<'a> {
    pub client: Client,
    pub client_id: &'a str,
    /// Its only redirect URI.
    pub redirect_uri: &'a str,
}

impl Broker {
    /// The registration of `client_id`, if one is configured.
    pub(crate) fn registration(&self, client_id: &str) -> Option<Registration<'_>> {
        if client_id == self.0.dbt_client_id {
            return Some(Registration {
                client: Client::Dbt,
                client_id: &self.0.dbt_client_id,
                redirect_uri: DBT_CLIENT_REDIRECT_URI,
            });
        }
        self.0
            .ui_client
            .as_ref()
            .filter(|ui| ui.client_id() == client_id)
            .map(|ui| Registration {
                client: Client::Ui,
                client_id: ui.client_id(),
                redirect_uri: ui.redirect_uri(),
            })
    }
}

/// Why a token request's client did not authenticate.
pub(crate) struct ClientRefusal<'a> {
    pub reason: &'static str,
    /// The client id the request claimed, if any.
    pub client_id: Option<&'a str>,
    pub error: OAuthError,
}

/// Authenticate the client of the token request `form`: a
/// `private_key_jwt` assertion authenticates the UI; otherwise the request
/// must name the dbt client, which is public. The UI's client id without
/// an assertion is refused. An accepted assertion's `jti` is recorded, so
/// it is never accepted twice, on any replica.
pub(crate) async fn authenticate<'a>(
    broker: &'a Broker,
    form: &'a BTreeMap<String, String>,
) -> Result<Registration<'a>, ClientRefusal<'a>> {
    let get = |key: &str| form.get(key).map(String::as_str);
    let client_id = get("client_id");
    let refuse = |reason, error| ClientRefusal {
        reason,
        client_id,
        error,
    };
    let invalid_client = |reason| refuse(reason, OAuthError::InvalidClient);
    let assertion_type = get("client_assertion_type");
    let assertion = get("client_assertion");
    if assertion_type.is_none() && assertion.is_none() {
        let registration = client_id.and_then(|id| broker.registration(id));
        return match (client_id, registration) {
            (None, _) => Err(invalid_client("missing_client_id")),
            (Some(_), None) => Err(invalid_client("unknown_client")),
            (Some(_), Some(registration)) if registration.client == Client::Dbt => Ok(registration),
            (Some(_), Some(_)) => Err(invalid_client("client_authentication_required")),
        };
    }
    if assertion_type != Some(CLIENT_ASSERTION_TYPE) {
        return Err(invalid_client("unsupported_client_assertion_type"));
    }
    let Some(assertion) = assertion else {
        return Err(invalid_client("missing_client_assertion"));
    };
    let Some(ui) = broker.0.ui_client.as_ref() else {
        return Err(invalid_client("unknown_client"));
    };
    if client_id.is_some_and(|id| id != ui.client_id()) {
        return Err(invalid_client("client_id_mismatch"));
    }
    // Only the UI authenticates with an assertion.
    let client_id = Some(ui.client_id());
    let refuse = |reason, error| ClientRefusal {
        reason,
        client_id,
        error,
    };
    let invalid_client = |reason| refuse(reason, OAuthError::InvalidClient);
    let now = broker.0.clock.now();
    let token_endpoint = broker.token_endpoint();
    let verified = ui
        .verify_assertion(assertion, &[&broker.0.issuer, &token_endpoint], now)
        .map_err(|error| invalid_client(error.reason()))?;
    let accepted = broker
        .0
        .store
        .accept_client_assertion(
            ui.client_id(),
            &secret::hash(&verified.jti),
            DateTime::<Utc>::from(verified.remember_until),
            DateTime::<Utc>::from(now),
        )
        .await;
    match accepted {
        Ok(true) => Ok(Registration {
            client: Client::Ui,
            client_id: ui.client_id(),
            redirect_uri: ui.redirect_uri(),
        }),
        Ok(false) => Err(invalid_client("client_assertion_replayed")),
        Err(error) => {
            tracing::error!(%error, "cannot record a client assertion");
            Err(refuse(
                "database_unavailable",
                OAuthError::TemporarilyUnavailable,
            ))
        }
    }
}

/// The `200` token response that gives `client` a person token for
/// `request` (a code exchange or a renewal) and the next `refresh_token`
/// of its session, issued at `now`, and how long the token lives. `nonce`
/// is the UI's authorize nonce on a code exchange.
pub(crate) fn person_token_response(
    broker: &Broker,
    client: Client,
    request: &TokenRequest,
    nonce: Option<&str>,
    refresh_token: &str,
    now: std::time::SystemTime,
) -> (Response, u64) {
    let scope = request.scope.to_string();
    let (body, expires_in) = match client {
        Client::Dbt => {
            let issued = broker.0.tokens.issue(request, now);
            let expires_in = issued.expires_in.as_secs();
            (
                json!({
                    "id_token": issued.token,
                    "refresh_token": refresh_token,
                    "scope": scope,
                    "token_type": "Bearer",
                    "expires_in": expires_in,
                }),
                expires_in,
            )
        }
        Client::Ui => {
            let issued = broker.0.tokens.issue_oidc(request, nonce, now);
            let expires_in = issued.expires_in.as_secs();
            (
                json!({
                    "access_token": issued.access_token,
                    "id_token": issued.id_token,
                    "refresh_token": refresh_token,
                    "scope": scope,
                    "token_type": "Bearer",
                    "expires_in": expires_in,
                }),
                expires_in,
            )
        }
    };
    (no_store(Json(body).into_response()), expires_in)
}
