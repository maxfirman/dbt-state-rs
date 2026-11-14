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
use support::demo::{
    self, clone_probe, clone_probe_sql, transient_at, CLONE_PROBE, DEV_SCHEMA, PROD_SCHEMA,
    RAW_ORDERS as RAW,
};
use support::scenario::{into_execute, outcome, ready_id, variant, ExecutionType, Scenario};
use support::{captured_org, start_server};

const PROD: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
const DEV: &str = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
/// The type-3 MERGE lifecycle most tests here exercise. The F5 clone-adoption
/// explain test drives a FULL model (`execution_type: 1`) instead, which F7
/// keeps characterized.
const MERGE: ExecutionType = ExecutionType::Merge;
/// A genuine projection change of the probe's logic.
const ADDED_COLUMN: &str = ", upper(customer_id) as c";

/// Full controlled lifecycle: prod build (records source logic) -> clone+confirm
/// -> cross-environment reuse (same logic skips; changed logic executes).
#[tokio::test]
async fn clone_register_confirm_then_cross_env_submit_skips() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let prod_epoch = 1_791_743_740_667i64;
    let clone_epoch = 1_791_743_784_002i64;
    let raw_epoch = 1_791_320_590_839i64; // older than both builds

    // 1) Build the SOURCE in prod: execute + confirm (records source logic).
    let r0 = sc
        .submit(
            clone_probe(PROD, PROD_SCHEMA)
                .execution_type(MERGE)
                .own_epoch(Some(prod_epoch))
                .upstream(RAW, raw_epoch),
        )
        .await;
    assert_eq!(variant(&r0), "ready_to_execute", "fresh prod source builds");
    assert!(sc.confirm(&ready_id(&r0), transient_at(prod_epoch)).await);

    // 2) RegisterClone(source=prod, target=dev) -> ready_to_clone, then confirm.
    let clone_resp = sc
        .register_clone(
            demo::copy(PROD, DEV)
                .execution_type(MERGE)
                .source_epoch(prod_epoch)
                .label("dbt_node_unique_id", CLONE_PROBE),
        )
        .await
        .unwrap();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert_eq!(ready.explained_decision.as_ref().unwrap().decision, 3);
    assert!(
        sc.confirm(&ready.request_id, transient_at(clone_epoch))
            .await
    );

    // 3) A dev submit for the clone target with the SAME model logic (schema
    //    differs: dev_clone_demo vs prod_state_demo) must SKIP — reproducing the
    //    hosted cross-environment post-clone reuse. Upstream RAW is older than
    //    the clone build epoch (fresh).
    let dev = clone_probe(DEV, DEV_SCHEMA)
        .execution_type(MERGE)
        .own_epoch(Some(clone_epoch))
        .upstream(RAW, raw_epoch);
    assert_eq!(
        sc.verdict(dev.clone()).await,
        "skip_execution",
        "post-clone submit with same (schema-abstracted) logic must SKIP"
    );

    // 4) A dev submit whose logic GENUINELY changes (added column) must EXECUTE,
    //    even though the physical target exists — reproducing the hosted
    //    LATEST_QUERY_HASH_NOT_MATCH behavior (clone_logic_sensitivity STEP B).
    let dev_changed = dev.sql(&clone_probe_sql(DEV_SCHEMA, ADDED_COLUMN));
    assert_eq!(
        sc.verdict(dev_changed).await,
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
    let sc = Scenario::connect(&server).await;

    let clone_epoch = 1_791_743_784_002i64;
    let raw_epoch = 1_791_320_590_839i64;

    // Register+confirm a clone WITHOUT ever recording the source's logic.
    let clone_resp = sc
        .register_clone(demo::copy(PROD, DEV).execution_type(MERGE))
        .await
        .unwrap();
    let ready = clone_resp.ready_to_clone.unwrap();
    sc.confirm(&ready.request_id, transient_at(clone_epoch))
        .await;

    let dev = clone_probe(DEV, DEV_SCHEMA)
        .execution_type(MERGE)
        .own_epoch(Some(clone_epoch))
        .upstream(RAW, raw_epoch);
    assert_eq!(
        sc.verdict(dev).await,
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
    let sc = Scenario::connect(&server).await;

    let creq: qc::CloneRequest = serde_json::from_value(register.request.clone()).unwrap();
    let cresp = sc
        .org(captured_org(register))
        .register_clone(creq)
        .await
        .unwrap();
    let ready = cresp.ready_to_clone.expect("ready_to_clone");
    assert_eq!(ready.explained_decision.as_ref().unwrap().decision, 3);

    let mut confirm_req: qc::ConfirmExecutionRequest =
        serde_json::from_value(confirm_entry.request.clone()).unwrap();
    confirm_req.request_id = ready.request_id;
    assert!(
        sc.org(captured_org(confirm_entry))
            .try_confirm(confirm_req)
            .await
            .unwrap()
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
    let sc = Scenario::connect(&server).await;
    let source = sc
        .submit(
            clone_probe(PROD, PROD_SCHEMA)
                .execution_type(MERGE)
                .own_epoch(Some(100))
                .upstream(RAW, 10),
        )
        .await;
    assert!(sc.confirm(&ready_id(&source), transient_at(100)).await);
    let response = sc
        .register_clone(demo::copy(PROD, DEV).execution_type(MERGE))
        .await
        .unwrap();
    let ready = response.ready_to_clone.expect("copy proposal");
    assert_eq!(
        Some(&ready),
        match response.response.as_ref() {
            Some(qc::clone_response::Response::ReadyToCloneV1(mirror)) => Some(mirror),
            _ => None,
        }
    );
    let failed = outcome().failed_to_clone().epoch(200).table();
    assert!(sc.confirm(&ready.request_id, failed).await);
    // Duplicate success acknowledgement cannot replace the first actual outcome.
    let duplicate = outcome().epoch(200).table();
    let error = sc
        .try_confirm(duplicate.confirm(&ready.request_id))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Internal);
    let result = sc
        .submit(
            clone_probe(DEV, DEV_SCHEMA)
                .execution_type(MERGE)
                .own_epoch(Some(200))
                .upstream(RAW, 10),
        )
        .await;
    // Not a SKIP of the uncertified build. Nor a submit-time Result copy of
    // the prod MERGE build: submit copies are FULL only (#101), so it builds
    // and reports the same-type equivalent (1 / 6, as hosted 2.0.11 in
    // explain_incremental_copy_over.jsonl).
    assert_eq!(
        variant(&result),
        "ready_to_execute",
        "ordinary fallback is not certified as the source model"
    );
    let decision = into_execute(result)
        .explained_decision
        .expect("explained decision");
    assert_eq!(
        (
            decision.skip_rejection_reason,
            decision.clone_rejection_reason
        ),
        (Some(1), Some(6))
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
/// renders. The incremental type-3 variant renders the same SHAPE 9 tree; see
/// `clone_adoption_incremental_execute_explain_is_shape9_outdated`.
#[tokio::test]
async fn clone_adoption_upstream_drift_execute_explain_is_shape9_outdated() {
    use qc::explain_client::ExplainClient;

    const BADGE_OUTDATED: i32 = 3;
    const BADGE_NODE_QUERY_CHANGED: i32 = 4;

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

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
    let r0 = sc
        .submit(
            clone_probe(PROD, PROD_SCHEMA)
                .own_epoch(Some(prod_epoch))
                .upstream(RAW, raw_old),
        )
        .await;
    assert_eq!(variant(&r0), "ready_to_execute");
    assert!(sc.confirm(&ready_id(&r0), transient_at(prod_epoch)).await);

    // 2) RegisterClone(source=prod, target=dev) + confirm the clone build. Type 1
    //    (FULL) to match the adopting submit's scope lookup — see
    //    clone_history_provenance.rs for the type-1 adoption precedent.
    let clone_resp = sc
        .register_clone(
            demo::copy(PROD, DEV)
                .source_epoch(prod_epoch)
                .label("dbt_node_unique_id", CLONE_PROBE),
        )
        .await
        .unwrap();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert!(
        sc.confirm(&ready.request_id, transient_at(clone_epoch))
            .await
    );

    // 3) Dev submit with the SAME model logic but an upstream RAW NEWER than the
    //    clone build epoch ⇒ EXECUTE driven by upstream drift. The clone-origin
    //    row is adopted (matched_execution_id set), so explain joins it. FULL
    //    (execution_type 1) ⇒ F7 keeps it characterized ⇒ SHAPE-9 renders.
    let dev_exec = sc
        .expect_execute(
            clone_probe(DEV, DEV_SCHEMA)
                .own_epoch(Some(clone_epoch))
                .upstream(RAW, raw_new),
        )
        .await;
    let edid = dev_exec.execution_decision_id.expect("edid");

    // 4) GetExplainMessages → the re-derived EXECUTE tree. Poll for the
    //    best-effort capture.
    let mut explain = ExplainClient::new(sc.channel());
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

/// A client-requested copy (RegisterClone) records the Result copy decision
/// reason, so Explain keeps rendering its CLONE tree (ADR 0006): a NULL reason
/// would render nothing.
#[tokio::test]
async fn register_clone_decision_explains_as_a_clone() {
    use qc::explain_client::ExplainClient;

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let prod_epoch = 1_791_743_740_667i64;

    let ready = sc
        .register_clone(demo::copy(PROD, DEV).source_epoch(prod_epoch))
        .await
        .unwrap()
        .ready_to_clone
        .expect("ready_to_clone");
    let edid = ready.execution_decision_id.expect("edid");

    let mut explain = ExplainClient::new(sc.channel());
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
    let msg = msg.expect("the RegisterClone decision is explained");
    let texts: Vec<&str> = msg.explain_lines.iter().map(|l| l.text.as_str()).collect();
    assert_eq!(
        texts,
        [
            format!("table analysis ({DEV})").as_str(),
            "query analysis (vs clone)",
            "data freshness analysis (vs clone)",
        ]
    );
    assert_eq!(
        msg.explain_lines[0].children[1].text,
        format!("the model table could be cloned from {PROD}")
    );
}

/// The SAME clone-adoption upstream-drift EXECUTE lifecycle as
/// `clone_adoption_upstream_drift_execute_explain_is_shape9_outdated`, driven
/// at `execution_type: 3` (incremental MERGE). A merge incremental renders the
/// MODEL trees, including the SHAPE 9 upstream-OUTDATED rebuild (VERIFIED LIVE
/// 2026-11-10, explain_incremental_append_merge_history.jsonl `m73z`), so the
/// adopted copy's drift rebuild renders SHAPE 9 too. Until #73 opened the
/// MERGE cells this test pinned an empty (fenced) tree.
#[tokio::test]
async fn clone_adoption_incremental_execute_explain_is_shape9_outdated() {
    use qc::explain_client::ExplainClient;

    const BADGE_OUTDATED: i32 = 3;

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let prod_epoch = 1_791_743_740_667i64;
    let clone_epoch = 1_791_743_784_002i64;
    let raw_old = 1_791_320_590_839i64;
    let raw_new = clone_epoch + 60_000;

    // 1) Build + confirm the prod SOURCE at incremental MERGE (execution_type 3).
    let r0 = sc
        .submit(
            clone_probe(PROD, PROD_SCHEMA)
                .execution_type(MERGE)
                .own_epoch(Some(prod_epoch))
                .upstream(RAW, raw_old),
        )
        .await;
    assert_eq!(variant(&r0), "ready_to_execute");
    assert!(sc.confirm(&ready_id(&r0), transient_at(prod_epoch)).await);

    // 2) RegisterClone + confirm the clone build (type 3, matching the source).
    let clone_resp = sc
        .register_clone(
            demo::copy(PROD, DEV)
                .execution_type(MERGE)
                .source_epoch(prod_epoch)
                .label("dbt_node_unique_id", CLONE_PROBE),
        )
        .await
        .unwrap();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert!(
        sc.confirm(&ready.request_id, transient_at(clone_epoch))
            .await
    );

    // 3) Dev submit with an upstream RAW NEWER than the clone build epoch ⇒
    //    clone-adoption upstream-drift EXECUTE, at incremental MERGE (type 3).
    let dev_exec = sc
        .expect_execute(
            clone_probe(DEV, DEV_SCHEMA)
                .execution_type(MERGE)
                .own_epoch(Some(clone_epoch))
                .upstream(RAW, raw_new),
        )
        .await;
    let edid = dev_exec.execution_decision_id.expect("edid");

    // 4) GetExplainMessages → the re-derived SHAPE 9 tree (poll for the
    //    best-effort capture).
    let mut explain = ExplainClient::new(sc.channel());
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
    let msg = msg.expect("clone-adoption MERGE EXECUTE explain message re-derived");
    fn collect(lines: &[qc::ExplainLine], out: &mut Vec<(String, Option<i32>)>) {
        for l in lines {
            out.push((l.text.clone(), l.badge));
            collect(&l.children, out);
        }
    }
    let mut flat = Vec::new();
    collect(&msg.explain_lines, &mut flat);
    assert!(
        flat.iter().any(|(_, b)| *b == Some(BADGE_OUTDATED))
            && flat.iter().any(|(t, _)| t == "upstream data is outdated")
            && flat
                .iter()
                .any(|(t, _)| t == "the model query has not changed"),
        "a MERGE clone-adoption upstream-drift EXECUTE renders the SHAPE 9 tree; got: {flat:?}"
    );
}
