//! REGISTERCLONE / CLONE-CHAIN PROVENANCE (review #2 finding 4, P0) —
//! live-captured from api.state.dbt.com (Fusion client, Snowflake;
//! experiments/clone_chain_provenance.sh; golden/fixtures/
//! clone_chain_upstream_drift.jsonl).
//!
//! The follow-up review's concern is that a clone inherits the SOURCE's logic
//! identity while a later same-target reuse is anchored on the CLONE's build
//! epoch, so a clone whose data predates the current upstream could be reused
//! unsafely. The live clone chain settles the provenance question:
//!
//!   1. prod builds/holds the source S.
//!   2. dev build of an absent target T CLONES S into T (ready_to_clone),
//!      confirmed with the clone's build epoch (CC_CONFIRM lme).
//!   3. the dev upstream is bumped so it is NEWER than the clone's build epoch.
//!   4. dev re-submits T: the submit now carries T's OWN epoch and the newer
//!      upstream, and the hosted service answers ready_to_execute
//!      ("model was executed because ... its upstream data is out of date").
//!
//! So after a clone, hosted anchors the clone target's freshness on the CLONE's
//! build epoch and REBUILDS when an upstream advances past it — exactly the
//! own-build anchor the engine implements (decision.rs). The clone is NOT reused
//! once the upstream moves past it; a clone is not treated as perpetually fresh.
//! (The ORTHOGONAL fact that a clone may be created from a source whose data is
//! older than the current upstream is hosted-TOLERATED at clone time — see
//! clone_eligibility.rs / finding 2 — and is a separate axis from this post-clone
//! freshness anchor.)
//!
//! This fixture REPLAYS EXACTLY on the server (it is in the conformance corpus),
//! so the server reproduces the whole chain including the step-4 rebuild. These
//! assertions pin the hosted truth so a regression that made a post-clone target
//! perpetually skippable (ignoring upstream drift) would fail.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::clone_client::CloneClient;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, start_server};

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/clone_chain_upstream_drift.jsonl"
);
const SOURCE_AGE_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/clone_chain_source_age.jsonl"
);

/// Pin the hosted clone-chain: dev clones, then a later submit with an upstream
/// NEWER than the clone's build epoch REBUILDS (own-build freshness anchor).
#[test]
fn hosted_clone_rebuilds_when_upstream_advances_past_clone() {
    let entries = diff::load_golden(FIXTURE).expect("load clone-chain fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    let confirms: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "ConfirmExecution")
        .collect();
    // prod source submit, dev clone submit, dev re-submit after bump.
    assert_eq!(submits.len(), 3, "prod + dev-clone + dev-resubmit");
    assert!(confirms.len() >= 2, "clone confirm + rebuild confirm");

    // The dev clone (second submit) is a clone.
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "ready_to_clone",
        "the dev build clones the equivalent"
    );
    // The clone's build epoch is recorded by its confirm.
    let clone_build_epoch = confirms[1].request["last_modified_epoch"]
        .as_i64()
        .expect("clone confirm epoch");

    // The third submit (after the upstream bump) carries an upstream epoch NEWER
    // than the clone's build epoch, and hosted REBUILDS.
    let third = submits[2];
    let max_upstream = third.request["tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["last_modified_epoch"].as_i64())
        .max()
        .expect("some upstream epoch");
    assert!(
        max_upstream > clone_build_epoch,
        "the re-submit's upstream (@{max_upstream}) is newer than the clone build \
         (@{clone_build_epoch})"
    );
    assert_eq!(
        diff::decision_variant(&third.response).unwrap(),
        "ready_to_execute",
        "a post-clone target with an upstream advanced past the clone REBUILDS \
         (clone is not perpetually fresh)"
    );
    let ed = &third.response["response"]["ready_to_execute"]["explained_decision"];
    assert!(
        ed["decision_description"]
            .as_str()
            .unwrap_or("")
            .contains("out of date"),
        "hosted attributes the rebuild to stale upstream data"
    );
}

/// REGISTERCLONE SOURCE-AGE vs CLONE-AGE (review #2 2nd follow-up, P0 #2) —
/// live-captured (experiments/registerclone_source_age.sh;
/// golden/fixtures/clone_chain_source_age.jsonl). This capture isolates the
/// DISTINGUISHING interval the review asked for (the earlier upstream-drift
/// capture could not separate the two policies):
///
///   source built @T1 (confirm lme=1791831369299)
///   upstream bumped @T2 (dev RC_UP @1791831449030, T2 > T1; source NOT rebuilt)
///   RegisterClone source→dev @T3 (confirm lme=1791831502677, T3 > T2)
///   resubmit dev with upstream @T2  (T1 < T2 < T3)  ->  skip_execution
///
/// Hosted SKIPs with the upstream NEWER than the source build (T1) but OLDER
/// than the clone creation (T3). So hosted anchors the clone target's freshness
/// on the CLONE CREATION epoch (clone-age), NOT the source content age — a clone
/// whose data predates the current upstream is still reused as long as the clone
/// OBJECT was created after that upstream. The server's own-build anchor uses
/// the clone's confirmed epoch and reproduces this, so NO source-epoch
/// validation is added (hosted does not do one). This settles the original
/// `source@100 / upstream@200 / clone@300 -> resubmit` counterexample: the SKIP
/// is hosted-faithful, not an unsafe reuse.
#[test]
fn hosted_clone_age_not_source_age_shape_is_pinned() {
    let entries = diff::load_golden(SOURCE_AGE_FIXTURE).expect("load source-age fixture");
    // The chain must contain a RegisterClone (the clone creation event).
    assert!(
        entries.iter().any(|e| e.method == "RegisterClone"),
        "the fixture includes the RegisterClone call (the clone creation)"
    );
    let confirms: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "ConfirmExecution")
        .collect();
    let source_build = confirms[0].request["last_modified_epoch"].as_i64().unwrap();
    let clone_creation = confirms[1].request["last_modified_epoch"].as_i64().unwrap();
    let dev = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .find(|e| e.request["default_schema"].as_str() == Some("dev_clone_demo"))
        .expect("the dev resubmit");
    let dev_upstream = dev.request["tables"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|t| {
            !t["name"]
                .as_str()
                .unwrap_or("")
                .to_uppercase()
                .contains("RC_MODEL")
        })
        .filter_map(|t| t["last_modified_epoch"].as_i64())
        .max()
        .expect("dev upstream epoch");
    // The distinguishing interval: source build < upstream < clone creation.
    assert!(
        source_build < dev_upstream && dev_upstream < clone_creation,
        "upstream (@{dev_upstream}) must be NEWER than the source build \
         (@{source_build}) and OLDER than the clone creation (@{clone_creation})"
    );
    // Hosted SKIPs → clone-age anchor, not source-age.
    assert_eq!(
        diff::decision_variant(&dev.response).as_deref(),
        Some("skip_execution"),
        "hosted anchors on the clone creation epoch (clone-age), not source-content age"
    );
}

/// Server reproduction: the server reproduces the clone-age anchor. Build an
/// incremental source and confirm it (T1); RegisterClone it into dev and confirm
/// the clone (T3); resubmit the clone target with an upstream epoch BETWEEN T1
/// and T3 → SKIP (the clone's own-build anchor is T3, and T3 >= upstream).
#[tokio::test]
async fn server_clone_age_anchor_skips_upstream_between_source_and_clone() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    const NS: &str = "src-age-ns";
    let src = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"RCSRC\"";
    let tgt = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"RCSRC\"";
    let t1 = 1_000i64; // source build
    let t2 = 2_000i64; // upstream bump (source NOT rebuilt)
    let t3 = 3_000i64; // clone creation

    let submit = |target: &str, schema: &str, own: Option<i64>, up: i64| {
        let mut tables = vec![qc::TableModifiedInfo {
            name: format!("\"CDO_DEMO_DB\".\"{}\".\"RCUP\"", schema.to_uppercase()),
            last_modified_epoch: Some(up),
        }];
        if let Some(o) = own {
            tables.push(qc::TableModifiedInfo {
                name: target.into(),
                last_modified_epoch: Some(o),
            });
        }
        qc::SubmitEnrichedSqlRequest {
            target_table: Some(target.into()),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            default_schema: Some(schema.into()),
            execution_type: 3,
            sql: format!("select id, amount from CDO_DEMO_DB.{schema}.rcup"),
            tolerate_nondeterminism: true,
            freshness_tolerance_seconds: 2700,
            tables,
            query_dependencies: vec![qc::QueryDependency {
                name: format!("\"CDO_DEMO_DB\".\"{}\".\"RCUP\"", schema.to_uppercase()),
                query: "select 1 as id, 100 as amount".into(),
                default_catalog: "CDO_DEMO_DB".into(),
                default_schema: schema.into(),
            }],
            table_namespace: Some(NS.into()),
            dbt_node_state: Some(qc::DbtNodeState {
                node_unique_id: "model.jaffle.rcsrc".into(),
                node_body_hash: Some("body".into()),
                project_id: Some("p".into()),
                ..Default::default()
            }),
            labels: [
                ("dbt_node_name".to_string(), "rcsrc".to_string()),
                (
                    "dbt_node_unique_id".to_string(),
                    "model.jaffle.rcsrc".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        }
    };

    // Source build @T1 (upstream also @T1) + confirm.
    let r0 = sql
        .submit_enriched_sql(submit(src, "prod_state_demo", None, t1))
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("source must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: x.request_id,
        last_modified_epoch: Some(t1),
        table_type: Some("TRANSIENT TABLE".into()),
        execution_runtime_ms: Some(50),
        ..Default::default()
    })
    .await
    .unwrap();

    // RegisterClone source→dev; confirm the clone at T3 (the clone creation).
    let clone_resp = clone
        .register_clone(qc::CloneRequest {
            target_table: tgt.into(),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            execution_type: 3,
            clone_source_table: src.into(),
            clone_source_last_modified_epoch: Some(t1),
            clone_source_table_type: Some("TRANSIENT TABLE".into()),
            table_namespace: Some(NS.into()),
            labels: [(
                "dbt_node_unique_id".to_string(),
                "model.jaffle.rcsrc".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: ready.request_id,
        last_modified_epoch: Some(t3),
        table_type: Some("TRANSIENT TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();

    // Resubmit the clone target with the clone's own epoch T3 and an upstream at
    // T2 (T1 < T2 < T3). The clone-age anchor (T3) is >= the upstream (T2) → SKIP.
    let resub = sql
        .submit_enriched_sql(submit(tgt, "dev_clone_demo", Some(t3), t2))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        support::response_variant(&resub),
        "skip_execution",
        "the server anchors the clone target's freshness on the CLONE creation \
         epoch (clone-age), reproducing hosted — an upstream older than the clone \
         creation does not force a rebuild"
    );
}
