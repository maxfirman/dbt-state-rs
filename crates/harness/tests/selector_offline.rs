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
use support::scenario::{model, outcome, variant, Scenario};
use support::{fault, start_server};

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

/// GetStateSelection under `org` for (`target`, `project`) and `criteria`.
async fn select(
    sc: &Scenario,
    org: &str,
    target: &str,
    project: &str,
    criteria: i32,
    nodes: Vec<qc::DbtNodeData>,
) -> BTreeSet<String> {
    sc.org(org)
        .select(qc::SelectorRequest {
            target: target.to_string(),
            selector_criteria: criteria,
            project_id: project.to_string(),
            nodes,
        })
        .await
}

#[tokio::test]
async fn record_path_then_select_classifies_against_recorded_state() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Record a baseline for model.a (via a real SQL submit).
    sc.org(ORG)
        .submit(model("DB.S.A").node_state(|ns| *ns = node_state("model.a")))
        .await;

    // model.a is now OLD/UNMODIFIED; model.b (never submitted) is NEW/MODIFIED.
    let nodes = vec![node_data("model.a"), node_data("model.b")];

    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, NEW, nodes.clone()).await,
        BTreeSet::from(["model.b".to_string()])
    );
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, OLD, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    // model.a unchanged -> unmodified; model.b new -> modified.
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, UNMODIFIED, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, MODIFIED, nodes.clone()).await,
        BTreeSet::from(["model.b".to_string()])
    );

    // A body change to model.a makes it modified + modified.body, still old.
    let mut changed = node_data("model.a");
    changed.node_body_hash = Some("body1".to_string());
    let nodes = vec![changed];
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, BODY, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, MODIFIED, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    assert!(
        select(&sc, ORG, TARGET, PROJECT, CONFIGS, nodes.clone())
            .await
            .is_empty(),
        "a body change must not fire modified.configs"
    );
}

#[tokio::test]
async fn relation_differs_from_recorded_null_but_is_excluded_from_modified() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.org(ORG)
        .submit(model("DB.S.A").node_state(|ns| *ns = node_state("model.a")))
        .await;

    // Recorded node_database_representation is null; the selector submits a real
    // value -> modified.relation fires, but modified does NOT (relation excluded),
    // and the node stays unmodified.
    let nodes = vec![node_data("model.a")];
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, RELATION, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    assert!(
        select(&sc, ORG, TARGET, PROJECT, MODIFIED, nodes.clone())
            .await
            .is_empty(),
        "relation change must be excluded from modified"
    );
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, UNMODIFIED, nodes).await,
        BTreeSet::from(["model.a".to_string()])
    );
}

#[tokio::test]
async fn new_node_matches_every_modified_sub_selector() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // No baseline recorded at all -> model.z is new.
    let nodes = vec![node_data("model.z")];
    for crit in [MODIFIED, NEW, BODY, CONFIGS, RELATION, CONTRACT] {
        assert_eq!(
            select(&sc, ORG, TARGET, PROJECT, crit, nodes.clone()).await,
            BTreeSet::from(["model.z".to_string()]),
            "criterion {crit} must select a brand-new node"
        );
    }
    for crit in [OLD, UNMODIFIED] {
        assert!(
            select(&sc, ORG, TARGET, PROJECT, crit, nodes.clone())
                .await
                .is_empty(),
            "criterion {crit} must not select a brand-new node"
        );
    }
}

#[tokio::test]
async fn scoping_isolates_by_org_project_and_target() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // Record model.a under (ORG, PROJECT, TARGET=prod).
    sc.org(ORG)
        .submit(model("DB.S.A").node_state(|ns| *ns = node_state("model.a")))
        .await;

    let nodes = vec![node_data("model.a")];
    // Same scope -> old.
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, OLD, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    // Different org -> new (isolated).
    assert!(
        select(&sc, "other-org", TARGET, PROJECT, OLD, nodes.clone())
            .await
            .is_empty()
    );
    assert_eq!(
        select(&sc, "other-org", TARGET, PROJECT, NEW, nodes.clone()).await,
        BTreeSet::from(["model.a".to_string()])
    );
    // Different project -> new.
    assert!(select(&sc, ORG, TARGET, "other-proj", OLD, nodes.clone())
        .await
        .is_empty());
    // Different target env -> new.
    assert!(select(&sc, ORG, "staging", PROJECT, OLD, nodes.clone())
        .await
        .is_empty());
}

#[tokio::test]
async fn record_executions_also_populates_selector_state() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Hydrate via RecordExecutions (the bypass path) rather than Submit/Confirm.
    let rec = model("DB.S.A")
        .catalog("DB")
        .schema("S")
        .sql("select 1 as id")
        .no_tables()
        .node_state(|ns| *ns = node_state("model.rec"))
        .record(outcome().epoch(100).table().runtime(5));
    sc.org(ORG).record([rec]).await.expect("record_executions");

    let nodes = vec![node_data("model.rec")];
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, OLD, nodes.clone()).await,
        BTreeSet::from(["model.rec".to_string()])
    );
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, UNMODIFIED, nodes).await,
        BTreeSet::from(["model.rec".to_string()])
    );
}

#[tokio::test]
async fn unknown_criteria_and_missing_project_are_rejected() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // SC_UNSPECIFIED (0) and out-of-range are rejected, not guessed.
    for bad in [0, 11, 99] {
        let err = sc
            .org(ORG)
            .try_select(qc::SelectorRequest {
                target: TARGET.into(),
                selector_criteria: bad,
                project_id: PROJECT.into(),
                nodes: vec![node_data("model.a")],
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "criteria {bad}");
    }

    // Missing project_id is rejected.
    let err = sc
        .org(ORG)
        .try_select(qc::SelectorRequest {
            target: TARGET.into(),
            selector_criteria: MODIFIED,
            project_id: String::new(),
            nodes: vec![node_data("model.a")],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

/// LIVE-VERIFIED (experiments/selector_last_write_wins.sh,
/// golden/fixtures/selector_last_write_wins.jsonl): the selector comparison
/// baseline is LAST-WRITE-WINS. A node rebuilt in the defer-to env records a new
/// baseline, and a subsequent selector query compares against that NEWEST record
/// — so the previously-identical submission is then reported `modified`.
///
/// This pins the semantics of `store::upsert_node_state`'s latest-wins
/// `ON CONFLICT DO UPDATE`. The hosted service's behavior under CONCURRENT
/// arrival (racing records for the same node) is NOT establishable from a
/// capture and is intentionally not asserted here; the deterministic sequential
/// contract is what we reproduce.
#[tokio::test]
async fn selector_baseline_is_last_write_wins() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.org(ORG);
    let v1_req = model("DB.S.A").node_state(|ns| *ns = node_state("model.a"));

    // Record V1 for model.a (body0).
    sc.submit(v1_req.clone()).await;

    // Submitting the identical V1 body => unmodified.
    let v1 = node_data("model.a"); // body0
    assert!(
        select(&sc, ORG, TARGET, PROJECT, MODIFIED, vec![v1.clone()])
            .await
            .is_empty(),
        "V1 submission matches the V1 baseline => not modified"
    );

    // Re-record the SAME node with a CHANGED body (V2 = body1). Latest wins.
    let v2_req = v1_req
        .node_state(|ns| ns.node_body_hash = Some("body1".to_string()))
        .sql("select 2 as id -- v2");
    sc.submit(v2_req).await;

    // The ORIGINAL V1 submission now DIFFERS from the newest (V2) baseline =>
    // modified. This is the hosted last-write-wins behavior.
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, MODIFIED, vec![v1.clone()]).await,
        BTreeSet::from(["model.a".to_string()]),
        "after re-recording V2, the V1 submission is modified vs the newest baseline"
    );
    assert_eq!(
        select(&sc, ORG, TARGET, PROJECT, BODY, vec![v1]).await,
        BTreeSet::from(["model.a".to_string()]),
        "the moved baseline is a body change"
    );
}

/// The best-effort selector-state write must NEVER fail the decision path, and a
/// write FAILURE must be observable (counted) rather than silently lost. We
/// force the failure by dropping the `node_states` table out from under the
/// server, then submit: the SQL decision still succeeds and the
/// `selector_write_failures` gauge increments.
#[tokio::test]
async fn selector_state_write_failures_are_observable() {
    let server = start_server().await;
    let state = server.state.clone();
    let sc = Scenario::connect(&server).await.org(ORG);

    assert_eq!(state.selector_write_failures(), 0, "clean start");

    // Drop the selector store table in the server's schema to force an upsert
    // error on the next record (the executions ledger is untouched).
    fault::drop_node_states(&server).await;

    // The decision path must still succeed (best-effort capture).
    let resp = sc
        .try_submit(model("DB.S.OBS").node_state(|ns| *ns = node_state("model.obs")))
        .await
        .expect("submit still succeeds despite selector-store outage");
    assert!(
        variant(&resp) == "ready_to_execute",
        "the SQL decision is unaffected by the selector-store failure"
    );

    // ...and the failure was counted, not silently swallowed.
    assert_eq!(
        state.selector_write_failures(),
        1,
        "a failed best-effort selector-state write must be observable"
    );
}
