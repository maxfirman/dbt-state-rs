//! `oidc` mode (`DBT_STATE_AUTH=oidc`): every RPC except `grpc.health.v1`
//! needs a Token-broker-signed token for the dbt State API audience, and
//! `x-organization-id` must name an active Organization the token lists.
//!
//! Tokens come from the test signer (`support/oidc.rs`); an accepted person
//! token names a recorded broker session (`person_session`). Token problems are
//! `UNAUTHENTICATED` and Organization problems `PERMISSION_DENIED`, each with
//! a fixed message; the server logs every refusal at `warn` with its reason
//! and never the token.

#[path = "support.rs"]
mod support;

use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use dbt_state_auth::Role;
use dbt_state_proto::grpc_health;
use support::oidc::{
    capture_logs, captured_logs, person_session, SignerOptions, TestAuth, TestKey,
};
use support::rpcs::call_every_rpc;
use support::scenario::{into_execute, into_skip, model, outcome, Scenario};
use support::{rows, start_server_oidc};
use tonic::Code;

const TARGET: &str = "\"DB\".\"S\".\"T\"";

/// The deployment's Organizations: two active, one disabled.
const ORGANIZATIONS: &[(&str, bool)] =
    &[("analytics", false), ("research", false), ("finance", true)];

fn auth() -> TestAuth {
    TestAuth::new(ORGANIZATIONS)
}

/// A person token in `analytics` (developer) and `research` (admin) records
/// its decisions in the Organization the request names; the other
/// Organization's State history never sees them.
#[tokio::test]
async fn a_valid_token_decides_and_records_in_the_requested_organization() {
    let auth = auth();
    let server = start_server_oidc(&auth).await;
    let session = person_session(&server, "principal-ada").await;
    let token = auth
        .signer()
        .person("principal-ada", &session)
        .grant("analytics", Role::Developer)
        .grant("research", Role::Admin)
        .token();
    let sc = Scenario::connect(&server).await.bearer(&token);
    let analytics = sc.org("analytics").invocation("inv-1", "sess-1");

    let built = into_execute(analytics.submit(model(TARGET)).await);
    assert!(
        analytics
            .confirm(&built.request_id, outcome().epoch(100))
            .await
    );
    let skipped = into_skip(analytics.submit(model(TARGET)).await);
    // The same target in the other Organization has no State history.
    into_execute(sc.org("research").submit(model(TARGET)).await);

    let ids = [
        built.execution_decision_id.expect("explainable"),
        skipped.execution_decision_id.expect("explainable"),
    ];
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    assert_eq!(analytics.explain(&ids).await.messages.len(), 2);
    assert!(sc.org("research").explain(&ids).await.messages.is_empty());

    let invocations = rows::invocations(&server).await;
    let analytics_rows: Vec<_> = invocations
        .iter()
        .filter(|i| i.org_id == "analytics")
        .collect();
    assert_eq!(analytics_rows.len(), 1, "{invocations:?}");
    assert_eq!(
        analytics_rows[0].external_invocation_id.as_deref(),
        Some("inv-1")
    );
    assert_eq!(
        (
            analytics_rows[0].built_count,
            analytics_rows[0].reused_count
        ),
        (1, 1)
    );
}

/// Machine tokens, and tokens signed with any loaded key id, are accepted.
#[tokio::test]
async fn machine_tokens_and_every_loaded_signing_key_are_accepted() {
    let auth = auth();
    let server = start_server_oidc(&auth).await;
    let session = person_session(&server, "principal-ada").await;
    let sc = Scenario::connect(&server).await.org("analytics");
    for key in [TestKey::Active, TestKey::Second] {
        let signer = auth.signer_with(SignerOptions {
            key,
            ..SignerOptions::default()
        });
        let person = signer
            .person("principal-ada", &session)
            .grant("analytics", Role::Developer)
            .token();
        let machine = signer
            .machine("principal-ci")
            .grant("analytics", Role::Developer)
            .token();
        for token in [person, machine] {
            sc.bearer(&token)
                .try_submit(model(TARGET))
                .await
                .unwrap_or_else(|status| panic!("{key:?}: {status:?}"));
        }
    }
}

/// Each token problem is `UNAUTHENTICATED` with its own fixed message.
#[tokio::test]
async fn token_problems_are_unauthenticated_with_fixed_messages() {
    let auth = auth();
    let server = start_server_oidc(&auth).await;
    let sc = Scenario::connect(&server).await.org("analytics");
    let signed = |options: SignerOptions| {
        auth.signer_with(options)
            .machine("principal-ci")
            .grant("analytics", Role::Developer)
            .token()
    };
    let valid = signed(SignerOptions::default());
    let lifetime = auth.signer().lifetimes().machine;
    let expired = auth
        .signer()
        .machine("principal-ci")
        .grant("analytics", Role::Developer)
        .issued_ago(lifetime + Duration::from_secs(90))
        .token();

    let cases: Vec<(&str, Option<String>, &str)> = vec![
        ("no token", None, "missing bearer token"),
        ("empty", Some(String::new()), "missing bearer token"),
        (
            "not a bearer scheme",
            Some(format!("Basic {valid}")),
            "malformed bearer token",
        ),
        (
            "bearer without a token",
            Some("Bearer ".into()),
            "malformed bearer token",
        ),
        (
            "not a JWT",
            Some("Bearer not-a-jwt".into()),
            "malformed bearer token",
        ),
        (
            "unsigned (alg none)",
            Some(format!("Bearer {}", unsigned(&valid))),
            "malformed bearer token",
        ),
        (
            "signed by an impostor key under a loaded key id",
            Some(format!(
                "Bearer {}",
                signed(SignerOptions {
                    key: TestKey::Impostor,
                    ..SignerOptions::default()
                })
            )),
            "invalid bearer token signature",
        ),
        (
            "signed under an unknown key id",
            Some(format!(
                "Bearer {}",
                signed(SignerOptions {
                    key: TestKey::Unknown,
                    ..SignerOptions::default()
                })
            )),
            "invalid bearer token signature",
        ),
        (
            "claims altered after signing",
            Some(format!("Bearer {}", altered(&valid))),
            "invalid bearer token signature",
        ),
        (
            "wrong issuer",
            Some(format!(
                "Bearer {}",
                signed(SignerOptions {
                    issuer: "https://other-broker.test",
                    ..SignerOptions::default()
                })
            )),
            "bearer token has the wrong issuer",
        ),
        (
            "wrong audience (a UI client id)",
            Some(format!(
                "Bearer {}",
                signed(SignerOptions {
                    audience: "dbt-state-ui",
                    ..SignerOptions::default()
                })
            )),
            "bearer token has the wrong audience",
        ),
        (
            "expired beyond the 60 s skew",
            Some(format!("Bearer {expired}")),
            "bearer token has expired",
        ),
    ];
    for (case, authorization, message) in cases {
        let client = match &authorization {
            Some(value) => sc.authorization(value),
            None => sc.clone(),
        };
        let status = client.try_submit(model(TARGET)).await.unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated, "{case}: {status:?}");
        assert_eq!(status.message(), message, "{case}");
    }

    // Within the skew, an expired token is still accepted.
    let within_skew = auth
        .signer()
        .machine("principal-ci")
        .grant("analytics", Role::Developer)
        .issued_ago(lifetime + Duration::from_secs(30))
        .token();
    sc.bearer(&within_skew)
        .try_submit(model(TARGET))
        .await
        .expect("within the clock skew");
}

/// Each Organization problem is `PERMISSION_DENIED`. Only a missing header
/// has its own message, so a response never reveals whether an
/// Organization exists.
#[tokio::test]
async fn organization_problems_are_permission_denied() {
    let auth = auth();
    let server = start_server_oidc(&auth).await;
    let sc = Scenario::connect(&server).await;
    let denied = "access to the organization is denied";
    let analytics_only = auth
        .signer()
        .person("principal-ada", "session-1")
        .grant("analytics", Role::Admin)
        .grant("finance", Role::Developer)
        .token();
    // Issued before `finance` was disabled and before `retired` was removed
    // from config: both appear as active entries.
    let stale = auth
        .signer_with(SignerOptions {
            organizations: Some(&[("analytics", false), ("finance", false), ("retired", false)]),
            ..SignerOptions::default()
        })
        .person("principal-ada", "session-1")
        .grant("finance", Role::Developer)
        .grant("retired", Role::Developer)
        .token();
    // Issued while `research` was disabled: an `app` entry only.
    let member_only = auth
        .signer_with(SignerOptions {
            organizations: Some(&[("research", true)]),
            ..SignerOptions::default()
        })
        .person("principal-ada", "session-1")
        .grant("research", Role::Developer)
        .token();

    let cases: Vec<(&str, &str, Option<&str>, &str)> = vec![
        (
            "no x-organization-id",
            &analytics_only,
            None,
            "missing x-organization-id metadata",
        ),
        (
            "empty x-organization-id",
            &analytics_only,
            Some(""),
            "missing x-organization-id metadata",
        ),
        (
            "an active Organization the token does not list",
            &analytics_only,
            Some("research"),
            denied,
        ),
        (
            "an Organization listed only as an app entry",
            &member_only,
            Some("research"),
            denied,
        ),
        (
            "an Organization disabled in config (token lists it as an app entry)",
            &analytics_only,
            Some("finance"),
            denied,
        ),
        (
            "an Organization disabled in config (token lists it as active)",
            &stale,
            Some("finance"),
            denied,
        ),
        (
            "an Organization unknown in config",
            &stale,
            Some("retired"),
            denied,
        ),
        ("an invalid Organization id", &stale, Some("*"), denied),
    ];
    for (case, token, organization, message) in cases {
        let client = match organization {
            Some(org) => sc.org(org),
            None => sc.clone(),
        }
        .bearer(token);
        let status = client.try_submit(model(TARGET)).await.unwrap_err();
        assert_eq!(status.code(), Code::PermissionDenied, "{case}: {status:?}");
        assert_eq!(status.message(), message, "{case}");
    }

    // A token problem comes before an Organization problem.
    let status = sc.try_submit(model(TARGET)).await.unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
}

/// Every RPC except the health service needs a token and an Organization,
/// including those that never read State history.
#[tokio::test]
async fn every_rpc_except_health_needs_a_token_and_an_organization() {
    let auth = auth();
    let server = start_server_oidc(&auth).await;
    let sc = Scenario::connect(&server).await;
    let token = auth
        .signer()
        .machine("principal-ci")
        .grant("analytics", Role::Developer)
        .token();

    for (rpc, result) in call_every_rpc(&sc.org("analytics")).await {
        let status = result.expect_err(rpc);
        assert_eq!(status.code(), Code::Unauthenticated, "{rpc}: {status:?}");
        assert_eq!(status.message(), "missing bearer token", "{rpc}");
    }
    for (rpc, result) in call_every_rpc(&sc.bearer(&token)).await {
        let status = result.expect_err(rpc);
        assert_eq!(status.code(), Code::PermissionDenied, "{rpc}: {status:?}");
    }
    for (rpc, result) in call_every_rpc(&sc.bearer(&token).org("analytics")).await {
        if let Err(status) = result {
            assert!(
                !matches!(
                    status.code(),
                    Code::Unauthenticated | Code::PermissionDenied
                ),
                "{rpc}: {status:?}"
            );
        }
    }

    let health = grpc_health::health_client::HealthClient::new(sc.channel())
        .check(grpc_health::HealthCheckRequest::default())
        .await
        .expect("health needs no token")
        .into_inner();
    assert_eq!(
        health.status,
        grpc_health::health_check_response::ServingStatus::Serving as i32
    );
}

/// Refusals are logged at `warn` with their reason (and the Principal, once
/// the token is valid), never with the token.
#[tokio::test]
async fn refusals_are_logged_at_warn_without_the_token() {
    capture_logs();
    let auth = auth();
    let server = start_server_oidc(&auth).await;
    let sc = Scenario::connect(&server).await;
    let wrong_audience = auth
        .signer_with(SignerOptions {
            audience: "dbt-state-ui",
            ..SignerOptions::default()
        })
        .machine("principal-logged-audience")
        .grant("analytics", Role::Developer)
        .token();
    let valid = auth
        .signer()
        .machine("principal-logged-denied")
        .grant("analytics", Role::Developer)
        .token();

    sc.org("analytics")
        .bearer(&wrong_audience)
        .try_submit(model(TARGET))
        .await
        .unwrap_err();
    sc.org("research")
        .bearer(&valid)
        .try_submit(model(TARGET))
        .await
        .unwrap_err();
    sc.org("analytics")
        .bearer(&valid)
        .try_submit(model(TARGET))
        .await
        .expect("accepted");

    let logs = captured_logs();
    let line = |needle: &str| {
        logs.lines()
            .find(|line| line.contains(needle))
            .unwrap_or_else(|| panic!("no log line with {needle:?} in:\n{logs}"))
            .to_owned()
    };
    let audience = line("wrong_audience");
    assert!(audience.contains("WARN"), "{audience}");
    assert!(audience.contains("SubmitEnrichedSQL"), "{audience}");
    let denied = line("principal-logged-denied");
    assert!(denied.contains("WARN"), "{denied}");
    assert!(denied.contains("organization_not_granted"), "{denied}");
    assert!(denied.contains("research"), "{denied}");
    for token in [&wrong_audience, &valid] {
        let signature = token.rsplit('.').next().unwrap();
        assert!(!logs.contains(signature), "a token reached the logs");
        let claims = token.split('.').nth(1).unwrap();
        assert!(!logs.contains(claims), "a token reached the logs");
    }
}

/// `token` with its header replaced by `{"alg":"none"}` and no signature.
fn unsigned(token: &str) -> String {
    let claims = token.split('.').nth(1).unwrap();
    format!(
        "{}.{claims}.",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#)
    )
}

/// `token` with its claims re-encoded to name another Principal, keeping
/// the original signature.
fn altered(token: &str) -> String {
    let parts: Vec<&str> = token.split('.').collect();
    let mut claims: serde_json::Value =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
    claims["sub"] = "principal-mallory".into();
    let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
    format!("{}.{claims}.{}", parts[0], parts[2])
}
