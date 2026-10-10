//! Differential tests for the CLONE decision, replaying the real golden
//! RegisterClone request captured from api.state.dbt.com.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff::{self, GoldenEntry};
use dbt_state_proto::query_cache as qc;
use qc::clone_client::CloneClient;

const CLONE_GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/golden_20261009T100637.316Z.jsonl"
);

fn find_register_clone(entries: &[GoldenEntry]) -> &GoldenEntry {
    entries
        .iter()
        .find(|e| e.method == "RegisterClone")
        .expect("a RegisterClone golden entry")
}

#[tokio::test]
async fn register_clone_matches_real_shape() {
    let entries = diff::load_golden(CLONE_GOLDEN).expect("load clone golden");
    let entry = find_register_clone(&entries);
    let req: qc::CloneRequest =
        serde_json::from_value(entry.request.clone()).expect("decode CloneRequest");

    // The real recorded response, for field-level comparison.
    let real = &entry.response;
    let real_ready = real
        .get("ready_to_clone")
        .expect("real ready_to_clone present");
    let real_sql = real_ready["clone_sqls"][0].as_str().unwrap();
    let real_source = real_ready["clone_source"].as_str().unwrap();
    let real_target = real_ready["clone_target"].as_str().unwrap();
    let real_decision = real_ready["explained_decision"]["decision"]
        .as_i64()
        .unwrap();

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut client = CloneClient::new(ch);

    let resp = client
        .register_clone(req.clone())
        .await
        .expect("register_clone")
        .into_inner();

    let ready = resp.ready_to_clone.expect("ready_to_clone field populated");
    assert!(!ready.request_id.is_empty(), "request_id present");
    let ed = ready
        .explained_decision
        .as_ref()
        .expect("explained_decision");

    // Decision must be READY_TO_CLONE (3), matching the real service.
    assert_eq!(ed.decision as i64, real_decision);
    assert_eq!(ed.decision, 3);

    // Source/target echoed exactly as the client requested (and as real).
    assert_eq!(ready.clone_source, real_source);
    assert_eq!(ready.clone_target, real_target);
    assert_eq!(ready.clone_source, req.clone_source_table);
    assert_eq!(ready.clone_target, req.target_table);

    // Clone SQL must match the real service's generated DDL exactly.
    assert_eq!(ready.clone_sqls.len(), 1);
    assert_eq!(
        ready.clone_sqls[0], real_sql,
        "generated clone SQL must match real"
    );

    // The deprecated oneof mirror must also be populated (as the real one is).
    #[allow(deprecated)]
    match resp.response {
        Some(qc::clone_response::Response::ReadyToCloneV1(v1)) => {
            assert_eq!(v1.clone_sqls[0], real_sql);
        }
        _ => panic!("expected ready_to_clone_v1 oneof mirror"),
    }
}

/// Replay the real ResolveDeferredRelations request and assert our response
/// shape matches (the hosted service returned an empty fqn_by_unique_id map).
#[tokio::test]
async fn deferred_relations_are_explicitly_unsupported() {
    use qc::execution_client::ExecutionClient;
    let entries = diff::load_golden(CLONE_GOLDEN).expect("load clone golden");
    let entry = entries
        .iter()
        .find(|e| e.method == "ResolveDeferredRelations")
        .expect("a ResolveDeferredRelations golden entry");
    let req: qc::ResolveDeferredRelationsRequest =
        serde_json::from_value(entry.request.clone()).expect("decode request");

    let real_map = entry.response["fqn_by_unique_id"]
        .as_object()
        .expect("real fqn_by_unique_id");

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut client = ExecutionClient::new(ch);
    let resp = client.resolve_deferred_relations(req).await.unwrap_err();

    assert!(real_map.is_empty()); // This capture does not establish nonempty deferral behavior.
    assert_eq!(resp.code(), tonic::Code::Unimplemented);
}
