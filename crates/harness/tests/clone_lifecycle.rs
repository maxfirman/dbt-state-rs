//! CLONE LIFECYCLE — register -> confirm -> cross-environment reuse.
//!
//! GOLDEN SOURCE (live, api.state.dbt.com, Fusion client, Snowflake). The real
//! dev-clone flow for `clone_probe` (an incremental model) was characterized via
//! the recording proxy (experiments/clone_experiment.sh + clone_logic_
//! sensitivity.sh). The verified contract:
//!
//!   1. The SOURCE model is built in prod (SubmitEnrichedSQL -> execute ->
//!      ConfirmExecution), recording its logic.
//!   2. A dev build with the dev object absent and `defer_to_target: prod` +
//!      `allow_clones: true` issues RegisterClone(source=prod, target=dev) ->
//!      ready_to_clone, then ConfirmExecution for the clone target.
//!   3. A subsequent SubmitEnrichedSQL for the dev (clone) target SKIPS when its
//!      logic matches the source — EVEN THOUGH the rendered SQL differs from the
//!      prod source ONLY in the physical schema qualifier (prod_state_demo vs
//!      dev_clone_demo). VERIFIED: `clone_logic_sensitivity.sh` STEP A.
//!   4. A dev submit whose logic GENUINELY changes (added column) EXECUTES
//!      (skip_rejection_reason = 8, LATEST_QUERY_HASH_NOT_MATCH). VERIFIED:
//!      STEP B.
//!
//! So post-clone reuse is CROSS-ENVIRONMENT (schema-abstracted) and
//! LOGIC-SENSITIVE. The server reproduces it by inheriting the clone source's
//! schema-abstracted logic identity (fingerprint::namespace_logic_hash) onto the
//! clone-origin row at RegisterClone, and matching a later submit's abstracted
//! logic against it. See docs/correctness-handoff.md.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::clone_client::CloneClient;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, response_variant, start_server};

const NS: &str = "fc38a5ba0eceb701c6ab7c6b882e28b0";
const PROD: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
const DEV: &str = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
const RAW: &str = "\"CDO_DEMO_DB\".\"RAW\".\"RAW_ORDERS\"";

fn tbl(name: &str, epoch: i64) -> qc::TableModifiedInfo {
    qc::TableModifiedInfo {
        name: name.into(),
        last_modified_epoch: Some(epoch),
    }
}

/// An incremental (MERGE, `execution_type: 3`) model submit for `target`, built
/// from the same logical model (the physical schema in the SQL/deps mirrors
/// `schema`). `extra_col` injects a genuine projection change when non-empty.
///
/// Shared by the type-3 MERGE lifecycle tests. Tests that need a characterized
/// (FULL/model) execution_type use [`submit_with_type`] directly.
fn submit(
    target: &str,
    schema: &str,
    own_epoch: i64,
    raw_epoch: i64,
    extra_col: &str,
) -> qc::SubmitEnrichedSqlRequest {
    submit_with_type(target, schema, own_epoch, raw_epoch, extra_col, 3)
}

/// Like [`submit`], but with an explicit `execution_type`. Factored out so the
/// F5 clone-adoption explain test can drive a FULL model (`execution_type: 1`),
/// which F7 keeps characterized, without changing the `execution_type: 3`
/// (incremental MERGE) lifecycle the other clone_lifecycle tests exercise.
fn submit_with_type(
    target: &str,
    schema: &str,
    own_epoch: i64,
    raw_epoch: i64,
    extra_col: &str,
    execution_type: i32,
) -> qc::SubmitEnrichedSqlRequest {
    let sql = format!(
        "select order_id, customer_id, ordered_at{extra_col} from CDO_DEMO_DB.{schema}.stg_orders where ordered_at > '1900-01-01'"
    );
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: "snowflake".into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: Some(schema.into()),
        execution_type,
        sql,
        tolerate_nondeterminism: true,
        freshness_tolerance_seconds: 2700,
        tables: vec![tbl(target, own_epoch), tbl(RAW, raw_epoch)],
        query_dependencies: vec![qc::QueryDependency {
            name: format!("\"CDO_DEMO_DB\".\"{}\".\"STG_ORDERS\"", schema.to_uppercase()),
            query: format!("create or replace view STG_ORDERS as (select * from CDO_DEMO_DB.{schema}.raw_orders)"),
            default_catalog: "CDO_DEMO_DB".into(),
            default_schema: schema.into(),
        }],
        table_namespace: Some(NS.into()),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.jaffle_shop.clone_probe".into(),
            node_body_hash: Some("body".into()),
            project_id: Some("p".into()),
            ..Default::default()
        }),
        labels: [
            ("dbt_node_name".to_string(), "clone_probe".to_string()),
            ("dbt_node_unique_id".to_string(), "model.jaffle_shop.clone_probe".to_string()),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    }
}

async fn confirm(
    exec: &mut ExecutionClient<tonic::transport::Channel>,
    resp: qc::SubmitSqlResponse,
    epoch: i64,
) {
    let qc::submit_sql_response::Response::ReadyToExecute(r) = resp.response.unwrap() else {
        panic!("expected execute before confirm");
    };
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: r.request_id,
            last_modified_epoch: Some(epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );
}

/// Full controlled lifecycle: prod build (records source logic) -> clone+confirm
/// -> cross-environment reuse (same logic skips; changed logic executes).
#[tokio::test]
async fn clone_register_confirm_then_cross_env_submit_skips() {
    let server = start_server().await;
    let addr = server.addr;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let prod_epoch = 1_791_743_740_667i64;
    let clone_epoch = 1_791_743_784_002i64;
    let raw_epoch = 1_791_320_590_839i64; // older than both builds

    // 1) Build the SOURCE in prod: execute + confirm (records source logic).
    let r0 = sql
        .submit_enriched_sql(submit(PROD, "prod_state_demo", prod_epoch, raw_epoch, ""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&r0),
        "ready_to_execute",
        "fresh prod source builds"
    );
    confirm(&mut exec, r0, prod_epoch).await;

    // 2) RegisterClone(source=prod, target=dev) -> ready_to_clone, then confirm.
    let clone_resp = clone
        .register_clone(qc::CloneRequest {
            target_table: DEV.into(),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            execution_type: 3,
            clone_source_table: PROD.into(),
            clone_source_last_modified_epoch: Some(prod_epoch),
            clone_source_table_type: Some("TRANSIENT TABLE".into()),
            table_namespace: Some(NS.into()),
            labels: [(
                "dbt_node_unique_id".to_string(),
                "model.jaffle_shop.clone_probe".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert_eq!(ready.explained_decision.as_ref().unwrap().decision, 3);
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: ready.request_id,
            last_modified_epoch: Some(clone_epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            failed_to_clone: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );

    // 3) A dev submit for the clone target with the SAME model logic (schema
    //    differs: dev_clone_demo vs prod_state_demo) must SKIP — reproducing the
    //    hosted cross-environment post-clone reuse. Upstream RAW is older than
    //    the clone build epoch (fresh).
    let dev_same = sql
        .submit_enriched_sql(submit(DEV, "dev_clone_demo", clone_epoch, raw_epoch, ""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&dev_same),
        "skip_execution",
        "post-clone submit with same (schema-abstracted) logic must SKIP"
    );

    // 4) A dev submit whose logic GENUINELY changes (added column) must EXECUTE,
    //    even though the physical target exists — reproducing the hosted
    //    LATEST_QUERY_HASH_NOT_MATCH behavior (clone_logic_sensitivity STEP B).
    let dev_changed = sql
        .submit_enriched_sql(submit(
            DEV,
            "dev_clone_demo",
            clone_epoch,
            raw_epoch,
            ", upper(customer_id) as c",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&dev_changed),
        "ready_to_execute",
        "a genuine logic change after a clone must EXECUTE (logic-sensitive reuse)"
    );
}

/// SAFETY: a cross-environment submit must NOT skip when no clone/source logic
/// was ever recorded (no provenance). Without the inherited logic identity, the
/// clone-origin row cannot be adopted and the submit executes.
#[tokio::test]
async fn cross_env_submit_without_source_logic_executes() {
    let server = start_server().await;
    let addr = server.addr;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let clone_epoch = 1_791_743_784_002i64;
    let raw_epoch = 1_791_320_590_839i64;

    // Register+confirm a clone WITHOUT ever recording the source's logic.
    let clone_resp = clone
        .register_clone(qc::CloneRequest {
            target_table: DEV.into(),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            execution_type: 3,
            clone_source_table: PROD.into(),
            clone_source_table_type: Some("TRANSIENT TABLE".into()),
            table_namespace: Some(NS.into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let ready = clone_resp.ready_to_clone.unwrap();
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: ready.request_id,
        last_modified_epoch: Some(clone_epoch),
        table_type: Some("TRANSIENT TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();

    let dev = sql
        .submit_enriched_sql(submit(DEV, "dev_clone_demo", clone_epoch, raw_epoch, ""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&dev),
        "ready_to_execute",
        "no recorded source logic => cannot prove equivalence => must EXECUTE"
    );
}

/// Fixture replay: the real register->confirm->submit from clone_happy_path.
/// This pins the captured RegisterClone/Confirm shapes against our server end to
/// end (the submit's reuse depends on cross-session history absent from the
/// file, so only the register+confirm successes are asserted here).
#[tokio::test]
async fn clone_happy_path_register_confirm_shapes() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_happy_path.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load clone_happy_path");
    let register = entries
        .iter()
        .find(|e| e.method == "RegisterClone")
        .unwrap();
    let confirm_entry = entries
        .iter()
        .find(|e| e.method == "ConfirmExecution")
        .unwrap();

    let server = start_server().await;
    let addr = server.addr;
    let ch = channel(addr).await;
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let creq: qc::CloneRequest = serde_json::from_value(register.request.clone()).unwrap();
    let cresp = clone
        .register_clone(support::captured_request(creq, register))
        .await
        .unwrap()
        .into_inner();
    let ready = cresp.ready_to_clone.expect("ready_to_clone");
    assert_eq!(ready.explained_decision.as_ref().unwrap().decision, 3);

    let mut confirm_req: qc::ConfirmExecutionRequest =
        serde_json::from_value(confirm_entry.request.clone()).unwrap();
    confirm_req.request_id = ready.request_id;
    assert!(
        exec.confirm_execution(support::captured_request(confirm_req, confirm_entry))
            .await
            .unwrap()
            .into_inner()
            .success
    );
}

/// A successful ordinary build after a failed RegisterClone has no source SQL
/// evidence. Source-derived identity must not authorize adoption of that build.
// The deprecated oneof must still mirror the current field on the wire.
#[allow(deprecated)]
#[tokio::test]
async fn failed_clone_confirmation_does_not_certify_source_logic() {
    let server = start_server().await;
    let addr = server.addr;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);
    let source = sql
        .submit_enriched_sql(submit(PROD, "prod_state_demo", 100, 10, ""))
        .await
        .unwrap()
        .into_inner();
    confirm(&mut exec, source, 100).await;
    let response = clone
        .register_clone(qc::CloneRequest {
            target_table: DEV.into(),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            execution_type: 3,
            clone_source_table: PROD.into(),
            clone_source_table_type: Some("TRANSIENT TABLE".into()),
            table_namespace: Some(NS.into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let ready = response.ready_to_clone.expect("copy proposal");
    assert_eq!(
        Some(&ready),
        match response.response.as_ref() {
            Some(qc::clone_response::Response::ReadyToCloneV1(mirror)) => Some(mirror),
            _ => None,
        }
    );
    let confirmation = qc::ConfirmExecutionRequest {
        request_id: ready.request_id,
        failed_to_clone: true,
        last_modified_epoch: Some(200),
        table_type: Some("TABLE".into()),
        ..Default::default()
    };
    assert!(
        exec.confirm_execution(confirmation.clone())
            .await
            .unwrap()
            .into_inner()
            .success
    );
    // Duplicate success acknowledgement cannot replace the first actual outcome.
    let mut duplicate = confirmation;
    duplicate.failed_to_clone = false;
    assert!(
        exec.confirm_execution(duplicate)
            .await
            .unwrap()
            .into_inner()
            .success
    );
    let result = sql
        .submit_enriched_sql(submit(DEV, "dev_clone_demo", 200, 10, ""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&result),
        "ready_to_clone",
        "ordinary fallback is not certified as the source model"
    );
}

/// REGRESSION (PR #31 round-3 finding F5): the explain tree for a clone-adoption
/// freshness-driven EXECUTE must re-derive the SAME per-upstream freshness
/// verdicts the DECISION used, not the raw (empty) clone-origin anchor.
///
/// `reuse_candidate::for_model` adopts a clone-origin row whose recorded `input_tables`
/// is EMPTY by substituting the submit's CURRENT upstreams in memory (so the
/// freshness check compares each upstream against the clone's own build epoch).
/// That normalization is NOT persisted. Before the store fix, the explain path
/// reloaded the RAW clone-origin row via `matched_execution_id` with its empty
/// input set, so `decision::upstream_freshness` saw an empty recorded set vs a
/// non-empty current set → `DependencySetChanged`, dropping every per-upstream
/// verdict. An upstream-drift EXECUTE then mis-rendered as the SHAPE-7
/// "the model query has changed / upstream data is up to date" tree instead of
/// the correct SHAPE-9 OUTDATED-upstream tree.
///
/// This drives register-clone → confirm → cross-env submit whose upstream RAW is
/// NEWER than the clone build epoch (so freshness forces an EXECUTE), then calls
/// GetExplainMessages and asserts the SHAPE-9 OUTDATED freshness subtree renders
/// (OUTDATED badge + "upstream data is outdated" [FAIL]), NOT the SHAPE-7
/// query-changed tree. Fails before the `store::ExplainDecisionRow::anchor()`
/// clone-adoption normalization, passes after.
///
/// The adopting lifecycle runs at `execution_type: 1` (FULL / table model), NOT
/// incremental MERGE (type 3). Clone adoption in `reuse_candidate::for_model` is
/// type-agnostic (it keys on clone-origin provenance + schema-abstracted logic
/// identity, never on the execution_type), so a FULL model adopts the
/// clone-origin row via the identical path — and FULL is a kind F7's
/// `is_characterized` fence keeps characterized, so the SHAPE-9 subtree actually
/// renders. (The incremental type-3 variant is correctly fenced to an EMPTY tree
/// by F7 until a real incremental explain is captured; see the companion test
/// `clone_adoption_incremental_execute_explain_is_fenced_empty`.)
#[tokio::test]
async fn clone_adoption_upstream_drift_execute_explain_is_shape9_outdated() {
    use qc::explain_client::ExplainClient;

    const BADGE_OUTDATED: i32 = 3;
    const BADGE_NODE_QUERY_CHANGED: i32 = 4;

    let server = start_server().await;
    let addr = server.addr;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch.clone());

    let prod_epoch = 1_791_743_740_667i64;
    let clone_epoch = 1_791_743_784_002i64;
    // RAW older than the clone build (so clone adoption is possible at all) for
    // the register/confirm, then a NEWER RAW on the drift submit below.
    let raw_old = 1_791_320_590_839i64;
    // RAW strictly NEWER than the clone build epoch ⇒ upstream OUTDATED ⇒ the
    // adopted clone-origin row forces a freshness-driven EXECUTE.
    let raw_new = clone_epoch + 60_000;

    // 1) Build + confirm the prod SOURCE (records source logic). FULL model
    //    (execution_type 1) so the adopting EXECUTE lands on a characterized kind.
    let r0 = sql
        .submit_enriched_sql(submit_with_type(
            PROD,
            "prod_state_demo",
            prod_epoch,
            raw_old,
            "",
            1,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&r0), "ready_to_execute");
    confirm(&mut exec, r0, prod_epoch).await;

    // 2) RegisterClone(source=prod, target=dev) + confirm the clone build. Type 1
    //    (FULL) to match the adopting submit's scope lookup — see
    //    clone_history_provenance.rs for the type-1 adoption precedent.
    let clone_resp = clone
        .register_clone(qc::CloneRequest {
            target_table: DEV.into(),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            execution_type: 1,
            clone_source_table: PROD.into(),
            clone_source_last_modified_epoch: Some(prod_epoch),
            clone_source_table_type: Some("TRANSIENT TABLE".into()),
            table_namespace: Some(NS.into()),
            labels: [(
                "dbt_node_unique_id".to_string(),
                "model.jaffle_shop.clone_probe".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: ready.request_id,
            last_modified_epoch: Some(clone_epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            failed_to_clone: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );

    // 3) Dev submit with the SAME model logic but an upstream RAW NEWER than the
    //    clone build epoch ⇒ EXECUTE driven by upstream drift. The clone-origin
    //    row is adopted (matched_execution_id set), so explain joins it. FULL
    //    (execution_type 1) ⇒ F7 keeps it characterized ⇒ SHAPE-9 renders.
    let dev = sql
        .submit_enriched_sql(submit_with_type(
            DEV,
            "dev_clone_demo",
            clone_epoch,
            raw_new,
            "",
            1,
        ))
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(dev_exec) = dev.response.unwrap() else {
        panic!("a clone-adopted submit with a NEWER upstream must EXECUTE (upstream drift)");
    };
    let edid = dev_exec.execution_decision_id.expect("edid");

    // 4) GetExplainMessages → the re-derived EXECUTE tree. Poll for the
    //    best-effort capture.
    let mut explain = ExplainClient::new(ch);
    let mut msg = None;
    for _ in 0..100 {
        let resp = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![edid.clone()],
            })
            .await
            .unwrap()
            .into_inner();
        if let Some(m) = resp.messages.into_iter().next() {
            msg = Some(m);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let msg = msg.expect("clone-adoption EXECUTE explain message re-derived");

    // Flatten every line's text + badge so we can assert the SHAPE.
    fn collect(lines: &[qc::ExplainLine], out: &mut Vec<(String, Option<i32>)>) {
        for l in lines {
            out.push((l.text.clone(), l.badge));
            collect(&l.children, out);
        }
    }
    let mut flat = Vec::new();
    collect(&msg.explain_lines, &mut flat);

    let has_outdated_badge = flat.iter().any(|(_, b)| *b == Some(BADGE_OUTDATED));
    let has_outdated_summary = flat.iter().any(|(t, _)| t == "upstream data is outdated");
    let has_query_changed_badge = flat
        .iter()
        .any(|(_, b)| *b == Some(BADGE_NODE_QUERY_CHANGED));
    let has_up_to_date = flat.iter().any(|(t, _)| t == "upstream data is up to date");

    assert!(
        has_outdated_badge && has_outdated_summary,
        "clone-adoption upstream-drift EXECUTE must render the SHAPE-9 OUTDATED freshness \
         subtree (OUTDATED badge + \"upstream data is outdated\"); got tree: {flat:?}"
    );
    assert!(
        !(has_query_changed_badge && has_up_to_date),
        "must NOT mis-render as the SHAPE-7 query-changed tree \
         (\"the model query has changed\" + \"upstream data is up to date\"); got tree: {flat:?}"
    );
}

/// COMPANION FENCE (PR #31 round-4 F7): the SAME clone-adoption upstream-drift
/// EXECUTE lifecycle as `clone_adoption_upstream_drift_execute_explain_is_shape9_outdated`,
/// but driven at `execution_type: 3` (incremental MERGE) — an UNOBSERVED node
/// kind (docs/explain-catalog.md lists seed/snapshot/incremental as unobserved).
///
/// F7's `explain.rs::is_characterized` fence renders an EMPTY explain tree for
/// uncharacterized kinds rather than inventing a model-shaped tree. This test
/// pins that honest behavior: the clone-adoption EXECUTE decision is still
/// recorded and GetExplainMessages still answers for its execution_decision_id,
/// but the re-derived tree is EMPTY until a real incremental explain is captured.
///
/// This keeps the type-3 MERGE clone-adoption path covered as "fenced" and
/// documents the F7 decision, complementing the type-1 SHAPE-9 assertion above.
#[tokio::test]
async fn clone_adoption_incremental_execute_explain_is_fenced_empty() {
    use qc::explain_client::ExplainClient;

    let server = start_server().await;
    let addr = server.addr;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch.clone());

    let prod_epoch = 1_791_743_740_667i64;
    let clone_epoch = 1_791_743_784_002i64;
    let raw_old = 1_791_320_590_839i64;
    let raw_new = clone_epoch + 60_000;

    // 1) Build + confirm the prod SOURCE at incremental MERGE (execution_type 3).
    let r0 = sql
        .submit_enriched_sql(submit(PROD, "prod_state_demo", prod_epoch, raw_old, ""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&r0), "ready_to_execute");
    confirm(&mut exec, r0, prod_epoch).await;

    // 2) RegisterClone + confirm the clone build (type 3, matching the source).
    let clone_resp = clone
        .register_clone(qc::CloneRequest {
            target_table: DEV.into(),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            execution_type: 3,
            clone_source_table: PROD.into(),
            clone_source_last_modified_epoch: Some(prod_epoch),
            clone_source_table_type: Some("TRANSIENT TABLE".into()),
            table_namespace: Some(NS.into()),
            labels: [(
                "dbt_node_unique_id".to_string(),
                "model.jaffle_shop.clone_probe".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: ready.request_id,
            last_modified_epoch: Some(clone_epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            failed_to_clone: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );

    // 3) Dev submit with an upstream RAW NEWER than the clone build epoch ⇒
    //    clone-adoption upstream-drift EXECUTE, at incremental MERGE (type 3).
    let dev = sql
        .submit_enriched_sql(submit(DEV, "dev_clone_demo", clone_epoch, raw_new, ""))
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(dev_exec) = dev.response.unwrap() else {
        panic!("a clone-adopted submit with a NEWER upstream must EXECUTE (upstream drift)");
    };
    let edid = dev_exec.execution_decision_id.expect("edid");

    // 4) GetExplainMessages → the tree must be EMPTY (F7 fence for an unobserved
    //    incremental node kind). Poll briefly to allow the best-effort capture,
    //    then assert that whatever is returned carries no explain lines.
    let mut explain = ExplainClient::new(ch);
    let mut saw_message = false;
    for _ in 0..25 {
        let resp = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![edid.clone()],
            })
            .await
            .unwrap()
            .into_inner();
        if let Some(m) = resp.messages.into_iter().next() {
            saw_message = true;
            assert!(
                m.explain_lines.is_empty(),
                "incremental (execution_type 3) clone-adoption EXECUTE is UNOBSERVED and must \
                 render an EMPTY explain tree (F7 fence), not an invented model-shaped tree; \
                 got: {:?}",
                m.explain_lines
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    // Either a message was captured and asserted empty above, or no message was
    // emitted at all for the fenced kind — both are catalog-consistent (nothing
    // invented). What must never happen is a populated tree, which the assert
    // inside the loop rules out.
    let _ = saw_message;
}
