//! Offline `state:*` selector tests that exercise the FULL server wiring at the
//! gRPC boundary: a `SubmitEnrichedSQL`/`SubmitValues`/`RecordExecutions` records
//! a node's comparison state (from `dbt_node_state`), and a subsequent
//! `GetStateSelection` reads it. Complements `selector_conformance.rs` (which
//! seeds the store directly from captured hosted state): these prove the record
//! path and the org/project/target scoping that the hosted capture cannot
//! isolate offline. Semantics are the live-verified ones encoded in
//! `dbt_state_server::selector`.

#[path = "support.rs"]
mod support;

use std::collections::BTreeSet;

use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::selector_service_client::SelectorServiceClient;
use qc::sql_client::SqlClient;
use support::{channel, request_with_org, start_server};

// SelectorCriteria wire ints.
const MODIFIED: i32 = 1;
const NEW: i32 = 2;
const UNMODIFIED: i32 = 3;
const OLD: i32 = 4;
const BODY: i32 = 5;
const CONFIGS: i32 = 6;
const RELATION: i32 = 7;
const CONTRACT: i32 = 10;

const PROJECT: &str = "proj-1";
const TARGET: &str = "prod";
const ORG: &str = "acme";

fn node_state(uid: &str) -> qc::DbtNodeState {
    qc::DbtNodeState {
        node_unique_id: uid.to_string(),
        target_name: TARGET.to_string(),
        project_name: "pkg".to_string(),
        resource_type: "model".to_string(),
        node_hash: "nh".to_string(),
        node_body_hash: Some("body0".to_string()),
        node_configs_hash: Some("cfg0".to_string()),
        node_persisted_descriptions_hash: None,
        node_macros_hash: None,
        node_contract_hash: Some("con0".to_string()),
        profile_name: "snowflake".to_string(),
        project_id: Some(PROJECT.to_string()),
        node_database_representation: None,
    }
}

/// A submit that records `node_state(uid)` under (ORG, PROJECT, TARGET). Uses a
/// trivial always-execute SQL submit; we only care that it records node state.
fn submit_req(uid: &str, target_table: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target_table.to_string()),
        dialect: "snowflake".into(),
        default_catalog: "DB".into(),
        default_schema: Some("S".into()),
        execution_type: 1,
        sql: format!("select 1 as id -- {uid}"),
        tolerate_nondeterminism: true,
        freshness_tolerance_seconds: 2700,
        dbt_node_state: Some(node_state(uid)),
        ..Default::default()
    }
}

/// A DbtNodeData the client would submit for `uid`, defaulting to the same
/// hashes as the recorded baseline (unchanged).
fn node_data(uid: &str) -> qc::DbtNodeData {
    qc::DbtNodeData {
        node_unique_id: uid.to_string(),
        node_hash: "nh".to_string(),
        node_body_hash: Some("body0".to_string()),
        node_configs_hash: Some("cfg0".to_string()),
        node_persisted_descriptions_hash: None,
        node_macros_hash: None,
        node_contract_hash: Some("con0".to_string()),
        // The selector always sends a real relation; builds record null.
        node_database_representation: Some("DB.S.T".to_string()),
    }
}

async fn select(
    ch: &tonic::transport::Channel,
    org: &str,
    target: &str,
    project: &str,
    criteria: i32,
    nodes: Vec<qc::DbtNodeData>,
) -> BTreeSet<String> {
    let resp = SelectorServiceClient::new(ch.clone())
        .get_state_selection(request_with_org(
            qc::SelectorRequest {
                target: target.to_string(),
                selector_criteria: criteria,
                project_id: project.to_string(),
                nodes,
            },
            org,
        ))
        .await
        .expect("get_state_selection")
        .into_inner();
    resp.node_unique_ids.into_iter().collect()
}

#[tokio::test]
async fn record_path_then_select_classifies_against_recorded_state() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;

    // Record a baseline for model.a (via a real SQL submit).
    SqlClient::new(ch.clone())
        .submit_enriched_sql(request_with_org(submit_req("model.a", "DB.S.A"), ORG))
        .await
        .expect("submit model.a");

    // model.a is now OLD/UNMODIFIED; model.b (never submitted) is NEW/MODIFIED.
    let nodes = vec![node_data("model.a"), node_data("model.b")];

    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, NEW, nodes.clone()).await,
        BTreeSet::from(["model.b".to_string()])
    );
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, OLD, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    // model.a unchanged -> unmodified; model.b new -> modified.
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, UNMODIFIED, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, MODIFIED, nodes.clone()).await,
        BTreeSet::from(["model.b".to_string()])
    );

    // A body change to model.a makes it modified + modified.body, still old.
    let mut changed = node_data("model.a");
    changed.node_body_hash = Some("body1".to_string());
    let nodes = vec![changed];
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, BODY, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, MODIFIED, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    assert!(
        select(&ch, ORG, TARGET, PROJECT, CONFIGS, nodes.clone())
            .await
            .is_empty(),
        "a body change must not fire modified.configs"
    );
}

#[tokio::test]
async fn relation_differs_from_recorded_null_but_is_excluded_from_modified() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    SqlClient::new(ch.clone())
        .submit_enriched_sql(request_with_org(submit_req("model.a", "DB.S.A"), ORG))
        .await
        .expect("submit");

    // Recorded node_database_representation is null; the selector submits a real
    // value -> modified.relation fires, but modified does NOT (relation excluded),
    // and the node stays unmodified.
    let nodes = vec![node_data("model.a")];
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, RELATION, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    assert!(
        select(&ch, ORG, TARGET, PROJECT, MODIFIED, nodes.clone())
            .await
            .is_empty(),
        "relation change must be excluded from modified"
    );
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, UNMODIFIED, nodes).await,
        BTreeSet::from(["model.a".to_string()])
    );
}

#[tokio::test]
async fn new_node_matches_every_modified_sub_selector() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    // No baseline recorded at all -> model.z is new.
    let nodes = vec![node_data("model.z")];
    for crit in [MODIFIED, NEW, BODY, CONFIGS, RELATION, CONTRACT] {
        assert_eq!(
            select(&ch, ORG, TARGET, PROJECT, crit, nodes.clone()).await,
            BTreeSet::from(["model.z".to_string()]),
            "criterion {crit} must select a brand-new node"
        );
    }
    for crit in [OLD, UNMODIFIED] {
        assert!(
            select(&ch, ORG, TARGET, PROJECT, crit, nodes.clone())
                .await
                .is_empty(),
            "criterion {crit} must not select a brand-new node"
        );
    }
}

#[tokio::test]
async fn scoping_isolates_by_org_project_and_target() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    // Record model.a under (ORG, PROJECT, TARGET=prod).
    SqlClient::new(ch.clone())
        .submit_enriched_sql(request_with_org(submit_req("model.a", "DB.S.A"), ORG))
        .await
        .expect("submit");

    let nodes = vec![node_data("model.a")];
    // Same scope -> old.
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, OLD, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    // Different org -> new (isolated).
    assert!(
        select(&ch, "other-org", TARGET, PROJECT, OLD, nodes.clone())
            .await
            .is_empty()
    );
    assert_eq!(
        select(&ch, "other-org", TARGET, PROJECT, NEW, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    // Different project -> new.
    assert!(select(&ch, ORG, TARGET, "other-proj", OLD, nodes.clone())
        .await
        .is_empty());
    // Different target env -> new.
    assert!(select(&ch, ORG, "staging", PROJECT, OLD, nodes.clone())
        .await
        .is_empty());
}

#[tokio::test]
async fn record_executions_also_populates_selector_state() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;

    // Hydrate via RecordExecutions (the bypass path) rather than Submit/Confirm.
    let rec = qc::ExecutionRecord {
        outcome: Some(qc::ExecutionOutcome {
            last_modified_epoch: Some(100),
            table_type: Some("TABLE".into()),
            execution_results: None,
            execution_runtime_ms: Some(5),
        }),
        input: Some(qc::execution_record::Input::EnrichedSql(qc::SqlExecution {
            target_table: Some("DB.S.A".into()),
            dialect: "snowflake".into(),
            default_catalog: "DB".into(),
            default_schema: Some("S".into()),
            execution_type: 1,
            sql: "select 1 as id".into(),
            dbt_node_state: Some(node_state("model.rec")),
            ..Default::default()
        })),
    };
    ExecutionClient::new(ch.clone())
        .record_executions(request_with_org(
            qc::RecordExecutionsRequest { records: vec![rec] },
            ORG,
        ))
        .await
        .expect("record_executions");

    let nodes = vec![node_data("model.rec")];
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, OLD, nodes.clone()).await,
        BTreeSet::from(["model.rec".to_string()])
    );
    assert_eq!(
        select(&ch, ORG, TARGET, PROJECT, UNMODIFIED, nodes).await,
        BTreeSet::from(["model.rec".to_string()])
    );
}

#[tokio::test]
async fn unknown_criteria_and_missing_project_are_rejected() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;

    // SC_UNSPECIFIED (0) and out-of-range are rejected, not guessed.
    for bad in [0, 11, 99] {
        let err = SelectorServiceClient::new(ch.clone())
            .get_state_selection(request_with_org(
                qc::SelectorRequest {
                    target: TARGET.into(),
                    selector_criteria: bad,
                    project_id: PROJECT.into(),
                    nodes: vec![node_data("model.a")],
                },
                ORG,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "criteria {bad}");
    }

    // Missing project_id is rejected.
    let err = SelectorServiceClient::new(ch.clone())
        .get_state_selection(request_with_org(
            qc::SelectorRequest {
                target: TARGET.into(),
                selector_criteria: MODIFIED,
                project_id: String::new(),
                nodes: vec![node_data("model.a")],
            },
            ORG,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
