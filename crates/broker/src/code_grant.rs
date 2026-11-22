//! `POST /token`, grant `authorization_code`: the dbt client exchanges the
//! code its loopback handler received.
//!
//! The pinned clients post `grant_type=authorization_code`, `code`,
//! `redirect_uri`, `client_id` and `code_verifier`, with no client
//! authentication: the dbt client is public (`dbt-fusion@9977b6c`
//! `crates/dbt-run-cache/src/auth/browser_flow.rs` `BrowserFlow::run`;
//! `dbt-state==2.62.6` `dbt_state/auth/sso.py` `_login`, authlib's
//! `fetch_token(..., include_client_id=True)` with auth method `none`,
//! which also sends `include_client_id=True`, ignored here). Both read
//! `id_token`, `token_type`, `expires_in` and `refresh_token`, and persist
//! the refresh token (Fusion `oauth.rs` `CachedToken`; Python
//! `_update_token_info`).
//!
//! The UI (`clients`) posts the same form with a `private_key_jwt`
//! assertion (`client_assertion_type`, `client_assertion`) instead of, or
//! as well as, `client_id`, and gets standard OIDC tokens: an `id_token`
//! echoing its authorize nonce and an `access_token`, for its own audience.
//!
//! The client is authenticated before the code is looked at, so a request
//! failing client authentication leaves the code as it was. Otherwise the
//! code is used up by its first presentation, whatever the outcome; a
//! second presentation revokes the session it started (RFC 6749 §4.1.2). A
//! code is bound to the client it was issued to.

use std::collections::BTreeMap;

use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use dbt_state_auth::{PrincipalId, PrincipalKind, Scope, TokenRequest};

use crate::clients::{self, person_token_response};
use crate::oauth::OAuthError;
use crate::secret;
use crate::store::Redeemed;
use crate::Broker;

pub(crate) const AUTHORIZATION_CODE: &str = "authorization_code";

/// What a refusal log line says.
#[derive(Default)]
struct Refusal<'a> {
    reason: &'a str,
    client_id: Option<&'a str>,
    principal: Option<&'a str>,
    session: Option<String>,
}

impl Refusal<'_> {
    fn respond(self, error: OAuthError) -> Response {
        tracing::warn!(
            grant = AUTHORIZATION_CODE,
            reason = self.reason,
            principal = self.principal,
            client_id = self.client_id,
            session = self.session,
            "token request refused"
        );
        error.into_response()
    }
}

/// See the module docs.
pub(crate) async fn exchange(broker: &Broker, form: &BTreeMap<String, String>) -> Response {
    let get = |key: &str| form.get(key).map(String::as_str);
    let registration = match clients::authenticate(broker, form).await {
        Ok(registration) => registration,
        Err(refused) => {
            return Refusal {
                reason: refused.reason,
                client_id: refused.client_id,
                ..Refusal::default()
            }
            .respond(refused.error)
        }
    };
    let client = registration.client;
    let client_id = Some(registration.client_id);
    let refusal = |reason| Refusal {
        reason,
        client_id,
        ..Refusal::default()
    };
    let (Some(code), Some(redirect_uri), Some(verifier)) =
        (get("code"), get("redirect_uri"), get("code_verifier"))
    else {
        return refusal("missing_parameter").respond(OAuthError::InvalidRequest);
    };

    let now = broker.0.clock.now();
    let at = DateTime::<Utc>::from(now);
    let grant = match broker.0.store.redeem_code(&secret::hash(code), at).await {
        Ok(Redeemed::First(grant)) => grant,
        Ok(Redeemed::Unknown) => return refusal("unknown_code").respond(OAuthError::InvalidGrant),
        Ok(Redeemed::Replayed { session }) => {
            return Refusal {
                session: Some(session.to_string()),
                ..refusal("code_replayed_session_revoked")
            }
            .respond(OAuthError::InvalidGrant)
        }
        Err(error) => {
            tracing::error!(%error, "cannot redeem an authorization code");
            return refusal("database_unavailable").respond(OAuthError::TemporarilyUnavailable);
        }
    };
    let session = &grant.session;
    let bound = |reason| Refusal {
        principal: Some(&session.principal_id),
        session: Some(session.id.to_string()),
        ..refusal(reason)
    };
    if Some(grant.client_id.as_str()) != client_id {
        return bound("client_mismatch").respond(OAuthError::InvalidGrant);
    }
    if grant.redirect_uri != redirect_uri {
        return bound("redirect_uri_mismatch").respond(OAuthError::InvalidGrant);
    }
    if grant.expires_at <= at {
        return bound("code_expired").respond(OAuthError::InvalidGrant);
    }
    if secret::s256_challenge(verifier) != grant.code_challenge {
        return bound("pkce_mismatch").respond(OAuthError::InvalidGrant);
    }
    if session.revoked_at.is_some() || session.expires_at <= at {
        return bound("session_inactive").respond(OAuthError::InvalidGrant);
    }
    let Ok(scope) = Scope::parse(&session.scope) else {
        tracing::error!(session = %session.id, "a stored session scope does not parse");
        return bound("stored_scope_invalid").respond(OAuthError::TemporarilyUnavailable);
    };

    let refresh_token = secret::random();
    if let Err(error) = broker
        .0
        .store
        .add_refresh_token(session.id, &secret::hash(&refresh_token), at)
        .await
    {
        tracing::error!(%error, "cannot record a refresh token");
        return bound("database_unavailable").respond(OAuthError::TemporarilyUnavailable);
    }
    let client_id = grant.client_id.as_str();
    let principal = PrincipalId::new(session.principal_id.clone());
    let mut request = TokenRequest::person(
        principal.clone(),
        session.id.to_string(),
        client_id,
        scope.clone(),
    );
    request.email = session.email.clone();
    request.name = session.name.clone();
    let (response, expires_in) = person_token_response(
        broker,
        client,
        &request,
        grant.nonce.as_deref(),
        &refresh_token,
        now,
    );
    let scope = scope.to_string();
    tracing::info!(
        grant = AUTHORIZATION_CODE,
        principal = principal.as_str(),
        kind = PrincipalKind::Person.as_str(),
        subject = session.upstream_subject.as_str(),
        client_id,
        session = %session.id,
        scope = scope.as_str(),
        expires_in,
        "token issued"
    );
    response
}
