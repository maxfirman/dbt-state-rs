//! DEFERRAL — `Execution.ResolveDeferredRelations`.
//!
//! GOLDEN SOURCE (live, api.state.dbt.com, Fusion client, Snowflake;
//! experiments/deferral_experiment.sh). When building from `dev_clone`
//! (`defer_to_target: prod_demo`) and selecting only a downstream node, the
//! client defers the UNSELECTED upstreams and calls
//! `ResolveDeferredRelations(profile, target=prod_demo, project, [uids])`. The
//! hosted service returns, for each node already built in that env, its physical
//! relation:
//!
//!   { "model.jaffle_shop.stg_customers":
//!       "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"STG_CUSTOMERS\"",
//!     "model.jaffle_shop.orders":
//!       "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ORDERS\"" }
//!
//! The FQN equals the submit `target_table` recorded for that (project, env,
//! node). Nodes NOT built in the defer-to env are omitted (the empty-map case).
//!
//! The server records target_table in node_states (migration 0010) on every
//! Submit/Record and resolves (org, project_id, target_name, uids) -> FQN.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use support::scenario::{model, Scenario};
use support::start_server;

const ORG: &str = "acme";
const PROJECT: &str = "70437463667433";
const DEFER_TO: &str = "prod_demo";

/// ResolveDeferredRelations for `uids` in (`org`, `project_id`, env `target`).
async fn resolve(
    sc: &Scenario,
    org: &str,
    target: &str,
    project_id: Option<&str>,
    uids: &[&str],
) -> std::collections::HashMap<String, String> {
    sc.org(org)
        .resolve_deferred(qc::ResolveDeferredRelationsRequest {
            profile_name: "snowflake".into(),
            target_name: target.into(),
            project_name: "jaffle_shop".into(),
            project_id: project_id.map(str::to_string),
            node_unique_ids: uids.iter().map(|s| s.to_string()).collect(),
        })
        .await
}

/// Core live-verified behavior: nodes built in the defer-to env resolve to their
/// physical FQNs; unbuilt nodes are omitted.
#[tokio::test]
async fn resolves_built_nodes_to_their_defer_to_env_fqn() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // A SQL submit that records node_state for `uid` built as `fqn` in the
    // defer-to env.
    let build = |uid: &str, fqn: &str| {
        model(fqn)
            .catalog("CDO_DEMO_DB")
            .schema(DEFER_TO)
            .sql(&format!("select 1 as id -- {uid}"))
            .tolerance_seconds(2700)
            .no_tables()
            .unique_id(uid)
            .target_name(DEFER_TO)
            .project(PROJECT)
            .node_state(|state| {
                state.project_name = "jaffle_shop".into();
                state.resource_type = "model".into();
                state.node_hash = "h".into();
                state.node_body_hash = Some("b".into());
                state.profile_name = "snowflake".into();
            })
    };

    let stg = "model.jaffle_shop.stg_customers";
    let stg_fqn = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"STG_CUSTOMERS\"";
    let orders = "model.jaffle_shop.orders";
    let orders_fqn = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ORDERS\"";

    // Build both upstreams in the defer-to env (records their FQNs).
    for (uid, fqn) in [(stg, stg_fqn), (orders, orders_fqn)] {
        sc.org(ORG).submit(build(uid, fqn)).await;
    }

    // Resolve both + one never-built node.
    let map = resolve(
        &sc,
        ORG,
        DEFER_TO,
        Some(PROJECT),
        &[stg, orders, "model.jaffle_shop.never_built"],
    )
    .await;

    assert_eq!(map.get(stg).map(String::as_str), Some(stg_fqn));
    assert_eq!(map.get(orders).map(String::as_str), Some(orders_fqn));
    assert!(
        !map.contains_key("model.jaffle_shop.never_built"),
        "a node not built in the defer-to env must be omitted"
    );
    assert_eq!(map.len(), 2);
}

/// Deferral resolution is scoped: a node built in a DIFFERENT env, project, or
/// org must not resolve for this (org, project, defer-to env).
#[tokio::test]
async fn resolution_is_scoped_by_org_project_and_env() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // A SQL submit that records node_state for `uid` built as `fqn` in the
    // defer-to env.
    let build = |uid: &str, fqn: &str| {
        model(fqn)
            .catalog("CDO_DEMO_DB")
            .schema(DEFER_TO)
            .sql(&format!("select 1 as id -- {uid}"))
            .tolerance_seconds(2700)
            .no_tables()
            .unique_id(uid)
            .target_name(DEFER_TO)
            .project(PROJECT)
            .node_state(|state| {
                state.project_name = "jaffle_shop".into();
                state.resource_type = "model".into();
                state.node_hash = "h".into();
                state.node_body_hash = Some("b".into());
                state.profile_name = "snowflake".into();
            })
    };

    let uid = "model.jaffle_shop.stg_customers";
    let fqn = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"STG_CUSTOMERS\"";
    sc.org(ORG).submit(build(uid, fqn)).await;

    // Same scope resolves.
    assert_eq!(
        resolve(&sc, ORG, DEFER_TO, Some(PROJECT), &[uid])
            .await
            .get(uid)
            .map(String::as_str),
        Some(fqn)
    );
    // Different env, project, or org => unresolved (empty).
    assert!(resolve(&sc, ORG, "dev_clone", Some(PROJECT), &[uid])
        .await
        .is_empty());
    assert!(resolve(&sc, ORG, DEFER_TO, Some("other-proj"), &[uid])
        .await
        .is_empty());
    assert!(resolve(&sc, "other-org", DEFER_TO, Some(PROJECT), &[uid])
        .await
        .is_empty());
}

/// Missing project_id / target / ids yield an empty map (never an error).
#[tokio::test]
async fn missing_scope_yields_empty_map_not_error() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let uid = "model.jaffle_shop.x";

    assert!(resolve(&sc, ORG, DEFER_TO, None, &[uid]).await.is_empty());
    assert!(resolve(&sc, ORG, DEFER_TO, Some(""), &[uid])
        .await
        .is_empty());
    assert!(resolve(&sc, ORG, "", Some(PROJECT), &[uid])
        .await
        .is_empty());
    assert!(resolve(&sc, ORG, DEFER_TO, Some(PROJECT), &[])
        .await
        .is_empty());
}

/// Pin the hosted response shape from the live capture: a non-empty resolution
/// mapping node_unique_id -> quoted 3-part FQN.
#[test]
fn hosted_resolution_shape_is_pinned() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/deferral_resolve.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load deferral fixture");
    let entry = entries
        .iter()
        .find(|e| e.method == "ResolveDeferredRelations")
        .expect("a ResolveDeferredRelations entry");
    let map = entry.response["fqn_by_unique_id"]
        .as_object()
        .expect("fqn_by_unique_id map");
    assert!(!map.is_empty(), "hosted returned a nonempty resolution");
    for (uid, fqn) in map {
        assert!(uid.starts_with("model."), "key is a node_unique_id");
        let f = fqn.as_str().unwrap();
        assert_eq!(f.matches('"').count(), 6, "quoted 3-part FQN: {f}");
        assert!(f.contains("PROD_STATE_DEMO"), "built in the defer-to env");
    }
}
