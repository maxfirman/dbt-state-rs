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
//! docs/correctness-handoff.md §D and docs/protocol.md).

#[path = "support.rs"]
mod support;

use dbt_state_proto::query_cache as qc;
use qc::sql_client::SqlClient;
use support::{channel, request_with_org, start_server, start_server_require_org};

fn minimal_submit() -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some("\"DB\".\"S\".\"T\"".into()),
        dialect: "snowflake".into(),
        execution_type: 1,
        sql: "select 1 as id".into(),
        tolerate_nondeterminism: true,
        ..Default::default()
    }
}

/// With the hardening enabled, a request WITHOUT an org header is rejected
/// (UNAUTHENTICATED), not silently scoped to the default tenant.
#[tokio::test]
async fn require_org_header_rejects_missing_header() {
    let (addr, _schema) = start_server_require_org().await;
    let ch = channel(addr).await;
    let err = SqlClient::new(ch)
        .submit_enriched_sql(minimal_submit()) // no x-organization-id metadata
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

/// With the hardening enabled, a request carrying a non-empty org header is
/// accepted and scoped to that org.
#[tokio::test]
async fn require_org_header_accepts_present_header() {
    let (addr, _schema) = start_server_require_org().await;
    let ch = channel(addr).await;
    let resp = SqlClient::new(ch)
        .submit_enriched_sql(request_with_org(minimal_submit(), "acme"))
        .await
        .expect("present org header is accepted")
        .into_inner();
    assert!(resp.response.is_some(), "a scoped request gets a decision");
}

/// DEFAULT (hardening OFF): a request without an org header still works and is
/// bucketed into the `local` tenant — preserving the insecure local-dev flow.
#[tokio::test]
async fn default_mode_allows_missing_header() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let resp = SqlClient::new(ch)
        .submit_enriched_sql(minimal_submit())
        .await
        .expect("default mode tolerates a missing org header")
        .into_inner();
    assert!(resp.response.is_some());
}

/// An EMPTY org header is treated as absent under the hardening (rejected): an
/// empty string must not slip through as a valid tenant.
#[tokio::test]
async fn require_org_header_rejects_empty_header() {
    let (addr, _schema) = start_server_require_org().await;
    let ch = channel(addr).await;
    let err = SqlClient::new(ch)
        .submit_enriched_sql(request_with_org(minimal_submit(), ""))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}
