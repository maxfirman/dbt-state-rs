//! SQL-SUBMIT CROSS-NAME CLONE FALLBACK — `ready_to_clone` from the SQL path.
//!
//! VERIFIED LIVE (api.state.dbt.com, Fusion client, Snowflake;
//! experiments/clone_experiment.sh table materialization). When a plain
//! `SubmitEnrichedSQL` targets a relation that does NOT yet exist, but an
//! EQUIVALENT model (same `table_namespace` + schema-abstracted logic) was
//! already built under a DIFFERENT physical name, the hosted service answers
//! `ready_to_clone` cloning that equivalent into the submit target:
//!
//!   decision=3 (READY_TO_CLONE), skip_rejection_reason=1 (TARGET_TABLE_MISMATCH),
//!   clone_rejection_reason=null, description="an equivalent model exists under
//!   another name so we cloned that one", clone_source=<existing equivalent>,
//!   clone_target=<submit target>, clone_sqls=[CREATE OR REPLACE … CLONE … COPY
//!   GRANTS], clone_required_last_modified_epoch=<source build epoch>,
//!   execution_runtime_ms=<source runtime>.
//!
//! The server reproduces this using the namespace logic identity
//! (fingerprint::namespace_logic_hash) recorded on every execution: it finds a
//! confirmed equivalent under another name in the same namespace and emits the
//! clone DDL. See docs/correctness-handoff.md.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, start_server};

const NS: &str = "fc38a5ba0eceb701c6ab7c6b882e28b0";

fn submit(target: &str, schema: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: "snowflake".into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: Some(schema.into()),
        execution_type: 1,
        sql: format!("select id from CDO_DEMO_DB.{schema}.lp_up_a"),
        tolerate_nondeterminism: true,
        freshness_tolerance_seconds: 2700,
        tables: vec![],
        table_namespace: Some(NS.into()),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.jaffle_shop.clone_probe".into(),
            node_body_hash: Some("body".into()),
            project_id: Some("p".into()),
            ..Default::default()
        }),
        labels: [("dbt_node_name".to_string(), "clone_probe".to_string())]
            .into_iter()
            .collect(),
        ..Default::default()
    }
}

/// Pin the hosted response shape from the live capture (no server needed).
#[test]
fn hosted_submit_fallback_shape_is_pinned() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_submit_equivalent_fallback.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load fixture");
    let entry = entries
        .iter()
        .find(|e| {
            e.method == "SubmitEnrichedSQL"
                && diff::decision_variant(&e.response).as_deref() == Some("ready_to_clone")
        })
        .expect("a submit->ready_to_clone entry");
    let rc = &entry.response["response"]["ready_to_clone"];
    let ed = &rc["explained_decision"];
    assert_eq!(ed["decision"].as_i64(), Some(3));
    assert_eq!(ed["skip_rejection_reason"].as_i64(), Some(1));
    assert!(ed["clone_rejection_reason"].is_null());
    assert_eq!(
        ed["decision_description"].as_str(),
        Some("an equivalent model exists under another name so we cloned that one")
    );
    assert!(rc["clone_required_last_modified_epoch"].as_i64().is_some());
    assert!(rc["clone_sqls"][0].as_str().unwrap().contains("CLONE"));
    // The clone direction: source and target are DIFFERENT physical names in the
    // same namespace (cross-name).
    assert_ne!(rc["clone_source"], rc["clone_target"]);
}

/// Controlled server reproduction: build model under name A (namespace N), then
/// submit for name B (same namespace, same logic, B absent) → ready_to_clone
/// cloning A→B. A subsequent confirm + re-submit for B then SKIPS (adopted).
#[tokio::test]
async fn server_emits_ready_to_clone_for_equivalent_under_another_name() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let a_epoch = 1_791_747_221_679i64;

    // Build the equivalent under name A (prod) and confirm it (records logic +
    // table_type + build epoch).
    let r0 = sql
        .submit_enriched_sql(submit(a, "prod_state_demo"))
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(x) = r0.response.unwrap() else {
        panic!("first build of A must execute");
    };
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: x.request_id,
            last_modified_epoch: Some(a_epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            execution_runtime_ms: Some(15275),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );

    // Submit for name B (same namespace + logic, B absent) → ready_to_clone A→B.
    let rb = sql
        .submit_enriched_sql(submit(b, "dev_clone_demo"))
        .await
        .unwrap()
        .into_inner();
    let c = match rb.response {
        Some(qc::submit_sql_response::Response::ReadyToClone(c)) => c,
        other => panic!("expected ready_to_clone, got {other:?}"),
    };
    assert_eq!(c.clone_source, a, "clone FROM the existing equivalent (A)");
    assert_eq!(c.clone_target, b, "clone INTO the submit target (B)");
    let ed = c.explained_decision.unwrap();
    assert_eq!(ed.decision, 3, "READY_TO_CLONE");
    assert_eq!(ed.skip_rejection_reason, Some(1), "TARGET_TABLE_MISMATCH");
    assert!(ed.clone_rejection_reason.is_none());
    assert_eq!(
        ed.decision_description,
        "an equivalent model exists under another name so we cloned that one"
    );
    assert_eq!(
        c.clone_required_last_modified_epoch,
        Some(a_epoch),
        "clone_required_last_modified_epoch = the source build epoch"
    );
    assert_eq!(c.execution_runtime_ms, Some(15275), "source runtime echoed");
    assert_eq!(c.clone_sqls.len(), 1);
    assert!(c.clone_sqls[0].contains("CREATE OR REPLACE TRANSIENT TABLE"));
    assert!(c.clone_sqls[0].contains("CLONE"));

    // Confirming the clone persists B as a clone-origin row (clone_source_table
    // set, inheriting A's logic identity). Its subsequent same-target reuse is
    // covered by clone_lifecycle.rs (which supplies realistic own-target
    // freshness evidence); here we assert the confirm succeeds.
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: c.request_id,
            last_modified_epoch: Some(a_epoch + 1),
            table_type: Some("TRANSIENT TABLE".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );
}

/// SAFETY: with NO equivalent recorded, a submit for an absent target executes
/// normally (no spurious clone).
#[tokio::test]
async fn no_equivalent_executes_normally() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch);
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let rb = sql
        .submit_enriched_sql(submit(b, "dev_clone_demo"))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        support::response_variant(&rb),
        "ready_to_execute",
        "no equivalent under another name => normal execute"
    );
}

/// HOSTED TRUTH (live-captured, experiments/p0_fallback_over_existing.sh): the
/// SQL-submit cross-name clone fallback fires EVEN WHEN the submit target
/// already exists with its own confirmed build history holding DIFFERENT logic.
/// The captured DEV sequence for one `clone_probe` table model is, in order:
/// logic L1 with DEV absent -> ready_to_clone (srr=1, cross-name); logic L2
/// (added column) -> ready_to_execute (srr=6, no equivalent); logic L1 again
/// with DEV now holding L2 -> ready_to_clone (srr=1, cross-name). The fallback
/// discriminator is purely "does an equivalent for THIS logic exist under
/// another name", NOT target existence. This overturned an earlier inferred
/// gate (`!target_has_confirmed_history`) and is why review issue #1 P0#2's
/// resubmit is `ready_to_clone`, not `ready_to_execute`. The server reproduces
/// it: the stale clone-origin row is NOT adopted (no unsafe SKIP) and the
/// cross-name fallback then clones the current equivalent in.
#[test]
fn hosted_fallback_fires_over_existing_target() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_fallback_over_existing_target.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load fixture");
    let decisions: Vec<Option<String>> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .map(|e| diff::decision_variant(&e.response))
        .collect();
    assert_eq!(
        decisions,
        vec![
            Some("ready_to_clone".to_string()),
            Some("ready_to_execute".to_string()),
            Some("ready_to_clone".to_string()),
        ],
        "hosted clone→rebuild(diff logic)→clone sequence over the SAME target"
    );
    // The third decision (fallback over an existing target) carries the
    // cross-name signature: TARGET_TABLE_MISMATCH and the equivalent description.
    let third = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .nth(2)
        .expect("third submit");
    let ed = &third.response["response"]["ready_to_clone"]["explained_decision"];
    assert_eq!(ed["skip_rejection_reason"].as_i64(), Some(1));
    assert_eq!(
        ed["decision_description"].as_str(),
        Some("an equivalent model exists under another name so we cloned that one")
    );
}
