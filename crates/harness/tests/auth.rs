//! AUTHENTICATION / MULTI-TENANCY hardening (review issue #1 P1: "Header-based
//! org selection is isolation, NOT authentication").
//!
//! The server scopes every store query by the `x-organization-id` metadata
//! header, defaulting to a shared `local` tenant when absent (the insecure
//! local-dev flow). A multi-tenant deployment can enable
//! `DBT_STATE_REQUIRE_ORG_HEADER` so requests without the header are REJECTED
//! rather than silently bucketed into `local`. This pins that scoping-presence
//! enforcement.
//!
//! NOTE: this enforces that an org is SCOPED, not that the caller is
//! AUTHENTICATED as that org — verifying the claimed org against a trusted
//! bearer token remains deployment work (documented in
//! docs/correctness-handoff.md "Deployment gaps" and docs/protocol.md).

#[path = "support.rs"]
mod support;

use support::scenario::{
    data_test, into_execute, into_skip, into_speculative_skip, model, outcome, seed, test_results,
    Scenario,
};
use support::{demo, rows, start_server, start_server_require_org};

/// Any decidable submission: these tests only exercise org-header scoping.
const TARGET: &str = "\"DB\".\"S\".\"T\"";

/// With the hardening enabled, a request WITHOUT an org header is rejected
/// (UNAUTHENTICATED), not silently scoped to the default tenant.
#[tokio::test]
async fn require_org_header_rejects_missing_header() {
    let server = start_server_require_org().await;
    let err = Scenario::connect(&server)
        .await
        .try_submit(model(TARGET)) // no x-organization-id metadata
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

/// With the hardening enabled, a request carrying a non-empty org header is
/// accepted and scoped to that org.
#[tokio::test]
async fn require_org_header_accepts_present_header() {
    let server = start_server_require_org().await;
    let resp = Scenario::connect(&server)
        .await
        .org("acme")
        .try_submit(model(TARGET))
        .await
        .expect("present org header is accepted");
    assert!(resp.response.is_some(), "a scoped request gets a decision");
}

/// DEFAULT (hardening OFF): a request without an org header still works and is
/// bucketed into the `local` tenant — preserving the insecure local-dev flow.
#[tokio::test]
async fn default_mode_allows_missing_header() {
    let server = start_server().await;
    let resp = Scenario::connect(&server)
        .await
        .try_submit(model(TARGET))
        .await
        .expect("default mode tolerates a missing org header");
    assert!(resp.response.is_some());
}

/// An EMPTY org header is treated as absent under the hardening (rejected): an
/// empty string must not slip through as a valid tenant.
#[tokio::test]
async fn require_org_header_rejects_empty_header() {
    let server = start_server_require_org().await;
    let err = Scenario::connect(&server)
        .await
        .org("")
        .try_submit(model(TARGET))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

/// Every decision is recorded under the request's Organization, dbt
/// invocation and client session: the same Organization the handler decided
/// in. `dbt state explain` in that Organization finds each decision (an
/// ordinary BUILD and SKIP, a seed BUILD, a RegisterClone copy and a
/// speculative SKIP); neither another Organization nor the default `local`
/// one finds any, and the decisions are counted under one invocation.
#[tokio::test]
async fn decisions_are_recorded_under_the_request_organization_and_invocation() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let scoped = sc.org("scope-org").invocation("inv-scope", "sess-scope");

    let built = into_execute(scoped.submit(model(TARGET)).await);
    assert!(
        scoped
            .confirm(&built.request_id, outcome().epoch(100))
            .await
    );
    let skipped = into_skip(scoped.submit(model(TARGET)).await);
    let seed = into_execute(
        scoped
            .submit_values(seed("\"DB\".\"S\".\"SEED\"").values("bytes"))
            .await,
    );
    let copy = scoped
        .register_clone(demo::copy(TARGET, "\"DB\".\"DEV\".\"T\"").source_epoch(100))
        .await
        .expect("RegisterClone")
        .ready_to_clone
        .expect("ready_to_clone");
    let test = data_test("test.p.scope_t")
        .namespace("scope-ns")
        .project("p");
    scoped
        .build_with(test.clone(), outcome().results(test_results(0)))
        .await;
    let speculative = into_speculative_skip(scoped.speculative(test).await);

    let ids: Vec<String> = [
        built.execution_decision_id,
        skipped.execution_decision_id,
        seed.execution_decision_id,
        copy.execution_decision_id,
        speculative.execution_decision_id,
    ]
    .into_iter()
    .map(|id| id.expect("an explainable decision"))
    .collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();

    let mut expected: Vec<String> = ids.iter().map(|id| (*id).to_owned()).collect();
    expected.sort();
    assert_eq!(explained(&scoped, &ids).await, expected);
    assert!(explained(&sc.org("other-org"), &ids).await.is_empty());
    assert!(explained(&sc, &ids).await.is_empty(), "nor the `local` org");

    // The data test's build is in the invocation too: three builds.
    assert_eq!(
        rows::invocations(&server).await,
        [rows::StoredInvocation {
            org_id: "scope-org".into(),
            external_invocation_id: Some("inv-scope".into()),
            session_id: Some("sess-scope".into()),
            built_count: 3,
            reused_count: 2,
            cloned_count: 1,
        }]
    );
}

/// The decision ids among `ids` that `dbt state explain` in `sc`'s
/// Organization finds, sorted.
async fn explained(sc: &Scenario, ids: &[&str]) -> Vec<String> {
    let mut found: Vec<String> = sc
        .explain(ids)
        .await
        .messages
        .into_iter()
        .map(|m| m.execution_decision_id)
        .collect();
    found.sort();
    found
}
