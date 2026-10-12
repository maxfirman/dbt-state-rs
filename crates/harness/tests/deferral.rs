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
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, request_with_org, start_server};

const ORG: &str = "acme";
const PROJECT: &str = "70437463667433";
const DEFER_TO: &str = "prod_demo";

/// A SQL submit that records node_state for `uid` built as `fqn` in `env`.
fn build_submit(uid: &str, fqn: &str, env: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(fqn.to_string()),
        dialect: "snowflake".into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: Some(env.to_string()),
        execution_type: 1,
        sql: format!("select 1 as id -- {uid}"),
        tolerate_nondeterminism: true,
        freshness_tolerance_seconds: 2700,
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: uid.to_string(),
            target_name: env.to_string(),
            project_name: "jaffle_shop".into(),
            resource_type: "model".into(),
            node_hash: "h".into(),
            node_body_hash: Some("b".into()),
            profile_name: "snowflake".into(),
            project_id: Some(PROJECT.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn resolve(
    exec: &mut ExecutionClient<tonic::transport::Channel>,
    org: &str,
    target: &str,
    project_id: Option<&str>,
    uids: &[&str],
) -> std::collections::HashMap<String, String> {
    exec.resolve_deferred_relations(request_with_org(
        qc::ResolveDeferredRelationsRequest {
            profile_name: "snowflake".into(),
            target_name: target.into(),
            project_name: "jaffle_shop".into(),
            project_id: project_id.map(str::to_string),
            node_unique_ids: uids.iter().map(|s| s.to_string()).collect(),
        },
        org,
    ))
    .await
    .expect("resolve_deferred_relations")
    .into_inner()
    .fqn_by_unique_id
}

/// Core live-verified behavior: nodes built in the defer-to env resolve to their
/// physical FQNs; unbuilt nodes are omitted.
#[tokio::test]
async fn resolves_built_nodes_to_their_defer_to_env_fqn() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let stg = "model.jaffle_shop.stg_customers";
    let stg_fqn = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"STG_CUSTOMERS\"";
    let orders = "model.jaffle_shop.orders";
    let orders_fqn = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ORDERS\"";

    // Build both upstreams in the defer-to env (records their FQNs).
    for (uid, fqn) in [(stg, stg_fqn), (orders, orders_fqn)] {
        sql.submit_enriched_sql(request_with_org(build_submit(uid, fqn, DEFER_TO), ORG))
            .await
            .expect("submit");
    }

    // Resolve both + one never-built node.
    let map = resolve(
        &mut exec,
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
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let uid = "model.jaffle_shop.stg_customers";
    let fqn = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"STG_CUSTOMERS\"";
    sql.submit_enriched_sql(request_with_org(build_submit(uid, fqn, DEFER_TO), ORG))
        .await
        .expect("submit");

    // Same scope resolves.
    assert_eq!(
        resolve(&mut exec, ORG, DEFER_TO, Some(PROJECT), &[uid])
            .await
            .get(uid)
            .map(String::as_str),
        Some(fqn)
    );
    // Different env, project, or org => unresolved (empty).
    assert!(resolve(&mut exec, ORG, "dev_clone", Some(PROJECT), &[uid])
        .await
        .is_empty());
    assert!(
        resolve(&mut exec, ORG, DEFER_TO, Some("other-proj"), &[uid])
            .await
            .is_empty()
    );
    assert!(
        resolve(&mut exec, "other-org", DEFER_TO, Some(PROJECT), &[uid])
            .await
            .is_empty()
    );
}

/// Missing project_id / target / ids yield an empty map (never an error).
#[tokio::test]
async fn missing_scope_yields_empty_map_not_error() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut exec = ExecutionClient::new(ch);
    let uid = "model.jaffle_shop.x";

    assert!(resolve(&mut exec, ORG, DEFER_TO, None, &[uid])
        .await
        .is_empty());
    assert!(resolve(&mut exec, ORG, DEFER_TO, Some(""), &[uid])
        .await
        .is_empty());
    assert!(resolve(&mut exec, ORG, "", Some(PROJECT), &[uid])
        .await
        .is_empty());
    assert!(resolve(&mut exec, ORG, DEFER_TO, Some(PROJECT), &[])
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
