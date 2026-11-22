//! `POST /token`, grant `refresh_token`: a signed-in person's dbt client
//! renews its token without a browser.
//!
//! Both pinned clients post `grant_type=refresh_token`, `refresh_token` and
//! `client_id`, with no client authentication and no scope (`dbt-fusion@9977b6c`
//! `crates/dbt-run-cache/src/auth/oauth.rs` `fetch_refresh`;
//! `dbt-state==2.62.6` `dbt_state/auth/sso.py` `_refresh_token`, authlib's
//! `OAuth2Session.refresh_token(..., scope=None)` with auth method `none`,
//! which appends `client_id`). Both read `id_token` (its `scope` claim),
//! `token_type` (Python requires it) and `expires_in`, and persist the
//! `refresh_token` of the response: Fusion stores exactly what it gets (a
//! response without one would leave it none), authlib keeps the presented
//! one when it is absent. The response is therefore the code exchange's:
//! `id_token`, a rotated `refresh_token`, `scope`, `token_type` and
//! `expires_in`. On any refusal both fall back to a browser sign-in once
//! their token has expired.
//!
//! A renewal runs in one transaction over the session's row (spec #215,
//! "Token endpoint, by grant"):
//!
//! 1. **Lock and check the session**: the row lock serializes renewals of
//!    one session across replicas. A revoked session, or one past its
//!    12-hour cap, does not renew, whatever token it is presented with.
//! 2. **Handle the rotation grace window**: a refresh token is rotated by
//!    the first renewal it starts, but is still accepted for
//!    [`GRACE_WINDOW`] (60 s) from that rotation, so two dbt processes
//!    sharing one login, or a client retrying after a lost response, both
//!    renew. Each such renewal issues another refresh token in the family
//!    (one more live member; those issued before stay valid) and leaves
//!    the presented token's rotation time alone, so renewing through the
//!    window never extends it. When the session's upstream evidence was
//!    renewed within the window too (as the renewal that rotated the token
//!    did), its recorded grant (scope, email, name) is reissued without
//!    asking the Upstream IdP, which therefore sees each of its refresh
//!    tokens at most once. Otherwise (only replica clocks that disagree
//!    get here) the renewal continues at step 4.
//! 3. **Detect replay**: a rotated refresh token presented after the window
//!    revokes the whole session.
//! 4. **Renew upstream and collect fresh evidence**: refresh at the IdP
//!    with the session's sealed upstream refresh token (keeping the IdP's
//!    rotated one, sealed, when it rotates). The evidence is the refresh
//!    response's ID token, checked as at sign-in but without a nonce, or,
//!    when it has none, the IdP's userinfo for the new access token. It
//!    must name the session's subject (the configured `subject_claim`);
//!    the issuer is the session's because both come from the configured
//!    Upstream IdP, which the session must still name.
//! 5. **Re-apply the person mapping or deny**: the person rules are applied
//!    to the evidence, so Role and Organization changes at the IdP take
//!    effect; no grant denies.
//!
//! Missing or untrusted evidence, another subject, an IdP refusal or an
//! empty mapping deny renewal and end the session. When the broker cannot
//! ask the IdP (it is unreachable or failing, or refuses the broker's own
//! credential), renewal is denied as `temporarily_unavailable` and the
//! session is left as it was, so the same refresh token can be retried.

use std::collections::BTreeMap;

use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, Utc};
use dbt_state_auth::{PrincipalId, PrincipalKind, Scope, TokenRequest, UpstreamClaims};
use serde_json::json;

use crate::oauth::{no_store, OAuthError};
use crate::secret;
use crate::store::{Principal, Renewal, Renewed};
use crate::upstream::{UpstreamError, Userinfo};
use crate::Broker;

pub(crate) const REFRESH_TOKEN: &str = "refresh_token";

/// How long a rotated refresh token is still accepted (step 2).
pub(crate) const GRACE_WINDOW: chrono::Duration = chrono::Duration::seconds(60);

/// What a refusal log line says.
#[derive(Default)]
struct Refusal<'a> {
    reason: &'a str,
    client_id: Option<&'a str>,
    principal: Option<&'a str>,
    subject: Option<&'a str>,
    session: Option<String>,
    upstream: Option<&'a str>,
    missing_claims: Option<&'a str>,
    session_ended: bool,
}

impl Refusal<'_> {
    fn respond(self, error: OAuthError) -> Response {
        tracing::warn!(
            grant = REFRESH_TOKEN,
            reason = self.reason,
            principal = self.principal,
            subject = self.subject,
            client_id = self.client_id,
            session = self.session,
            upstream = self.upstream,
            missing_claims = self.missing_claims,
            session_ended = self.session_ended,
            "token request refused"
        );
        error.into_response()
    }
}

/// Why a renewal is denied, and what becomes of the session.
struct Denial {
    reason: &'static str,
    error: OAuthError,
    /// End the session: nothing issued in it renews again.
    end_session: bool,
    /// The IdP's error, or why it was unavailable.
    upstream: Option<String>,
    missing_claims: Option<String>,
    /// The IdP's rotated refresh token, sealed, to keep although the
    /// renewal is denied (the session stays).
    keep_upstream_refresh_token: Option<Vec<u8>>,
}

impl Denial {
    /// Deny, leaving the session as it was.
    fn keep(reason: &'static str, error: OAuthError) -> Self {
        Self {
            reason,
            error,
            end_session: false,
            upstream: None,
            missing_claims: None,
            keep_upstream_refresh_token: None,
        }
    }

    /// Deny and end the session.
    fn end(reason: &'static str) -> Self {
        Self {
            end_session: true,
            ..Self::keep(reason, OAuthError::RenewalDenied)
        }
    }

    fn upstream(mut self, detail: impl Into<String>) -> Self {
        self.upstream = Some(detail.into());
        self
    }

    fn missing_claims(mut self, claims: impl Into<String>) -> Self {
        self.missing_claims = Some(claims.into()).filter(|claims: &String| !claims.is_empty());
        self
    }
}

/// The fresh evidence a renewal is granted on.
struct Evidence {
    /// `id_token` or `userinfo`.
    source: &'static str,
    claims: UpstreamClaims,
    /// The IdP's rotated refresh token, sealed; `None` when it kept the
    /// current one.
    sealed_upstream_refresh_token: Option<Vec<u8>>,
}

/// What a renewal comes to.
enum Outcome {
    /// Renewed on fresh upstream evidence.
    Renewed {
        evidence: Evidence,
        scope: Scope,
    },
    /// Renewed through the grace window on the session's evidence, renewed
    /// within the window: its recorded `scope`.
    Reissued {
        scope: Scope,
    },
    Denied(Denial),
}

/// See the module docs.
pub(crate) async fn renew(broker: &Broker, form: &BTreeMap<String, String>) -> Response {
    let get = |key: &str| form.get(key).map(String::as_str);
    let client_id = get("client_id");
    let refusal = |reason| Refusal {
        reason,
        client_id,
        ..Refusal::default()
    };
    match client_id {
        None => return refusal("missing_client_id").respond(OAuthError::InvalidClient),
        Some(id) if id != broker.0.dbt_client_id => {
            return refusal("unknown_client").respond(OAuthError::InvalidClient)
        }
        Some(_) => {}
    }
    let Some(presented) = get(REFRESH_TOKEN) else {
        return refusal("missing_parameter").respond(OAuthError::InvalidRequest);
    };

    let now = broker.0.clock.now();
    let at = DateTime::<Utc>::from(now);
    let renewal = match broker.0.store.begin_renewal(&secret::hash(presented)).await {
        Ok(Some(renewal)) => renewal,
        Ok(None) => return refusal("unknown_refresh_token").respond(OAuthError::RenewalDenied),
        Err(error) => {
            tracing::error!(%error, "cannot begin a session renewal");
            return refusal("database_unavailable").respond(OAuthError::TemporarilyUnavailable);
        }
    };
    let session = renewal.session.clone();
    let bound = |reason| Refusal {
        principal: Some(&session.principal_id),
        subject: Some(&session.upstream_subject),
        session: Some(session.id.to_string()),
        ..refusal(reason)
    };

    let grace_window = renewal.presented.rotated_at.is_some();
    let outcome = assess(broker, &renewal, client_id, now).await;
    let refresh_token = secret::random();
    let principal = PrincipalId::new(session.principal_id.clone());
    let (scope, email, name, evidence, recorded) = match outcome {
        Outcome::Renewed { evidence, scope } => {
            let email = evidence.claims.string("email").or(session.email.as_deref());
            let name = evidence.claims.string("name").or(session.name.as_deref());
            let recorded = renewal
                .renew(&Renewed {
                    principal: &Principal {
                        id: &principal,
                        issuer: broker.0.upstream.issuer(),
                        subject: &session.upstream_subject,
                        kind: PrincipalKind::Person,
                        email,
                    },
                    scope: &scope.to_string(),
                    email,
                    name,
                    sealed_upstream_refresh_token: evidence.sealed_upstream_refresh_token.clone(),
                    next_token_hash: &secret::hash(&refresh_token),
                    now: at,
                })
                .await;
            let (email, name) = (email.map(str::to_owned), name.map(str::to_owned));
            (scope, email, name, evidence.source, recorded)
        }
        Outcome::Reissued { scope } => {
            let recorded = renewal.reissue(&secret::hash(&refresh_token), at).await;
            let (email, name) = (session.email.clone(), session.name.clone());
            (scope, email, name, "recent_renewal", recorded)
        }
        Outcome::Denied(denial) => {
            let applied = if denial.end_session {
                renewal.end_session(at).await
            } else if let Some(sealed) = &denial.keep_upstream_refresh_token {
                renewal.keep_upstream_refresh_token(sealed).await
            } else {
                drop(renewal);
                Ok(())
            };
            if let Err(error) = &applied {
                tracing::error!(%error, "cannot record a denied session renewal");
            }
            return Refusal {
                upstream: denial.upstream.as_deref(),
                missing_claims: denial.missing_claims.as_deref(),
                session_ended: denial.end_session && applied.is_ok(),
                ..bound(denial.reason)
            }
            .respond(denial.error);
        }
    };
    if let Err(error) = recorded {
        tracing::error!(%error, "cannot record a session renewal");
        return bound("database_unavailable").respond(OAuthError::TemporarilyUnavailable);
    }
    let scope_text = scope.to_string();

    let client_id = session.client_id.as_str();
    let mut request =
        TokenRequest::person(principal.clone(), session.id.to_string(), client_id, scope);
    request.email = email;
    request.name = name;
    let issued = broker.0.tokens.issue(&request, now);
    tracing::info!(
        grant = REFRESH_TOKEN,
        principal = principal.as_str(),
        kind = PrincipalKind::Person.as_str(),
        subject = session.upstream_subject.as_str(),
        client_id,
        session = %session.id,
        scope = scope_text.as_str(),
        evidence,
        grace_window,
        expires_in = issued.expires_in.as_secs(),
        "token issued"
    );
    no_store(
        Json(json!({
            "id_token": issued.token,
            "refresh_token": refresh_token,
            "scope": scope_text,
            "token_type": "Bearer",
            "expires_in": issued.expires_in.as_secs(),
        }))
        .into_response(),
    )
}

/// Steps 1 to 5 of the module docs, over the locked session of `renewal`.
/// Changes nothing: the caller applies the outcome to the renewal.
async fn assess(
    broker: &Broker,
    renewal: &Renewal,
    client_id: Option<&str>,
    now: std::time::SystemTime,
) -> Outcome {
    let session = &renewal.session;
    let at = DateTime::<Utc>::from(now);

    // 1. Lock (held by `renewal`) and check the session.
    if Some(session.client_id.as_str()) != client_id {
        return Outcome::Denied(Denial::keep("client_mismatch", OAuthError::RenewalDenied));
    }
    if session.revoked_at.is_some() {
        return Outcome::Denied(Denial::keep("session_revoked", OAuthError::RenewalDenied));
    }
    if session.expires_at <= at {
        return Outcome::Denied(Denial::keep("session_expired", OAuthError::RenewalDenied));
    }

    // 2. Handle the rotation grace window, and 3. detect replay. Replica
    // clocks may disagree slightly, so a rotation "in the future" is
    // within the window.
    let within_window = |time: DateTime<Utc>| at - time < GRACE_WINDOW;
    let in_grace_window = match renewal.presented.rotated_at {
        None => false,
        Some(rotated_at) if within_window(rotated_at) => true,
        Some(_) => return Outcome::Denied(Denial::end("refresh_token_replayed_session_revoked")),
    };

    let upstream = &broker.0.upstream;
    // Sessions outlive a config: this deployment may have been restarted
    // without the broker's registration for browser sign-in.
    let Some(login) = upstream.login() else {
        return Outcome::Denied(Denial::keep(
            "sign_in_not_configured",
            OAuthError::RenewalDenied,
        ));
    };
    if session.upstream_issuer != upstream.issuer() {
        return Outcome::Denied(Denial::end("upstream_issuer_changed"));
    }
    if in_grace_window && within_window(session.upstream_renewed_at) {
        let Ok(scope) = Scope::parse(&session.scope) else {
            tracing::error!(session = %session.id, "a stored session scope does not parse");
            return Outcome::Denied(Denial::keep(
                "stored_scope_invalid",
                OAuthError::TemporarilyUnavailable,
            ));
        };
        return Outcome::Reissued { scope };
    }

    // 4. Renew upstream and collect fresh evidence.
    let key = login.refresh_token_key();
    let context = session.id.as_bytes();
    let Some(upstream_refresh_token) = key
        .open(&session.sealed_upstream_refresh_token, context)
        .and_then(|token| String::from_utf8(token).ok())
    else {
        tracing::error!(session = %session.id, "a stored upstream refresh token does not open");
        return Outcome::Denied(Denial::end("upstream_refresh_token_unreadable"));
    };
    let tokens = match upstream.refresh(&upstream_refresh_token, now).await {
        Ok(tokens) => tokens,
        Err(UpstreamError::Refused { status, error }) => {
            return Outcome::Denied(
                Denial::end("upstream_refused_grant")
                    .upstream(error.unwrap_or_else(|| format!("HTTP {status}"))),
            )
        }
        Err(error) => return Outcome::Denied(unavailable(error, None)),
    };
    let sealed = tokens
        .refresh_token
        .filter(|token| !token.is_empty())
        .map(|token| key.seal(token.as_bytes(), context));

    let (source, claims) = if let Some(id_token) = &tokens.id_token {
        match upstream.verify_refreshed_id_token(id_token, now).await {
            Ok(Ok(claims)) => ("id_token", claims),
            Ok(Err(error)) => return Outcome::Denied(Denial::end(error.reason())),
            Err(error) => return Outcome::Denied(unavailable(error, sealed)),
        }
    } else if let Some(access_token) = &tokens.access_token {
        match upstream.userinfo(access_token).await {
            Ok(Userinfo::Claims(claims)) => ("userinfo", claims),
            Ok(Userinfo::NotOffered) => {
                return Outcome::Denied(Denial::end("upstream_missing_evidence"))
            }
            Ok(Userinfo::Unusable) => {
                return Outcome::Denied(Denial::end("upstream_userinfo_unusable"))
            }
            Err(UpstreamError::Refused { status, error }) => {
                return Outcome::Denied(
                    Denial::end("upstream_userinfo_refused")
                        .upstream(error.unwrap_or_else(|| format!("HTTP {status}"))),
                )
            }
            Err(error) => return Outcome::Denied(unavailable(error, sealed)),
        }
    } else {
        return Outcome::Denied(Denial::end("upstream_missing_evidence"));
    };
    let subject_claim = upstream.subject_claim();
    match claims.string(subject_claim).filter(|s| !s.is_empty()) {
        None => {
            return Outcome::Denied(
                Denial::end("upstream_evidence_missing_subject").missing_claims(subject_claim),
            )
        }
        Some(subject) if subject != session.upstream_subject => {
            return Outcome::Denied(Denial::end("upstream_subject_mismatch"))
        }
        Some(_) => {}
    }

    // 5. Re-apply the person mapping or deny.
    let rules = &broker.0.rules;
    let scope = rules.scope(PrincipalKind::Person, &claims, &broker.0.organizations);
    if scope.is_empty() {
        let missing: Vec<&str> = rules
            .claims_read(PrincipalKind::Person)
            .into_iter()
            .filter(|claim| !claims.contains(claim))
            .collect();
        return Outcome::Denied(Denial::end("no_matching_rule").missing_claims(missing.join(",")));
    }
    Outcome::Renewed {
        evidence: Evidence {
            source,
            claims,
            sealed_upstream_refresh_token: sealed,
        },
        scope,
    }
}

/// The broker could not ask the IdP: deny for now and keep the session,
/// with the IdP's rotated refresh token when it already renewed.
fn unavailable(error: UpstreamError, rotated: Option<Vec<u8>>) -> Denial {
    let (reason, detail) = match error {
        UpstreamError::RejectedClient { error } => (
            "upstream_rejected_client",
            format!(
                "rejected client: {}",
                error.as_deref().unwrap_or("HTTP 401")
            ),
        ),
        UpstreamError::Refused { status, error } => (
            "upstream_unavailable",
            error.unwrap_or_else(|| format!("HTTP {status}")),
        ),
        UpstreamError::Unavailable(detail) => ("upstream_unavailable", detail),
    };
    Denial {
        keep_upstream_refresh_token: rotated,
        ..Denial::keep(reason, OAuthError::TemporarilyUnavailable).upstream(detail)
    }
}
