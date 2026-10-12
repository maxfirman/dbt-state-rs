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

/// Replay the real ResolveDeferredRelations request. The hosted service
/// returned an EMPTY `fqn_by_unique_id` map for this capture (the requested
/// node had not been built in the defer-to env in that session). Our server now
/// IMPLEMENTS deferral (see deferral.rs) and must likewise return an empty map
/// when it has no recorded physical state for the requested nodes — NOT an
/// error. The nonempty resolution is pinned by `deferral.rs`.
#[tokio::test]
async fn deferred_relations_empty_when_nothing_built() {
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
    assert!(real_map.is_empty(), "this capture has an empty resolution");

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut client = ExecutionClient::new(ch);
    let resp = client
        .resolve_deferred_relations(req)
        .await
        .expect("resolve_deferred_relations succeeds")
        .into_inner();

    assert!(
        resp.fqn_by_unique_id.is_empty(),
        "no recorded node state => empty resolution, matching the hosted capture"
    );
}

/// CLONE REJECTION — allow_clones=false. VERIFIED LIVE (experiments/
/// clone_experiment.sh --no-clones): a dev_clone target with profile
/// `allow_clones: false` makes the Fusion client send RegisterClone with
/// `clone_chain_depth_limit = 0`. The hosted service answers `unable_to_clone`
/// with decision=1 (READY_TO_EXECUTE), clone_rejection_reason=10
/// (CLONE_CHAIN_LIMIT_EXCEEDED), empty description, and leaves the deprecated
/// top-level oneof empty. The client then builds the node normally.
#[tokio::test]
async fn register_clone_rejected_when_allow_clones_false() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_rejected_allow_clones_false.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load clone rejection fixture");
    let entry = entries
        .iter()
        .find(|e| e.method == "RegisterClone")
        .expect("a RegisterClone entry");

    // Confirm the capture precondition and the hosted response shape.
    let req: qc::CloneRequest =
        serde_json::from_value(entry.request.clone()).expect("decode CloneRequest");
    assert_eq!(
        req.clone_chain_depth_limit,
        Some(0),
        "allow_clones=false is encoded as clone_chain_depth_limit=0"
    );
    let real = &entry.response["unable_to_clone"];
    assert!(real.is_object(), "hosted responded unable_to_clone");
    assert_eq!(real["explained_decision"]["decision"].as_i64(), Some(1));
    assert_eq!(
        real["explained_decision"]["clone_rejection_reason"].as_i64(),
        Some(10)
    );

    let (addr, schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut client = CloneClient::new(ch);
    let resp = client
        .register_clone(support::captured_request(req.clone(), entry))
        .await
        .expect("register_clone")
        .into_inner();

    // Our server must reproduce the unable_to_clone shape exactly.
    assert!(
        resp.ready_to_clone.is_none(),
        "no ready_to_clone on rejection"
    );
    let unable = resp.unable_to_clone.expect("unable_to_clone populated");
    assert_eq!(unable.clone_source, req.clone_source_table);
    assert_eq!(unable.clone_target, req.target_table);
    let ed = unable.explained_decision.expect("explained_decision");
    assert_eq!(ed.decision, 1, "decision READY_TO_EXECUTE");
    assert_eq!(
        ed.clone_rejection_reason,
        Some(10),
        "CLONE_CHAIN_LIMIT_EXCEEDED"
    );
    assert!(ed.skip_rejection_reason.is_none());
    assert!(ed.decision_description.is_empty());
    #[allow(deprecated)]
    {
        assert!(
            resp.response.is_none(),
            "deprecated oneof empty on rejection"
        );
    }

    // No pending clone history must be written on a rejection.
    let pool = support::schema_pool(&schema).await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM executions")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "a rejected clone must not persist a pending row");
}
