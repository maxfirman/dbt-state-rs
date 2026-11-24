//! The Explain freshness anchor is the complete execution row the decision
//! matched, read through the same execution read type as State history, against
//! a real per-test Postgres schema (no gRPC). Explain tree shapes stay covered by
//! the Explain conformance gate (`conformance.rs`) and `clone_lifecycle.rs`.

#[path = "support.rs"]
mod support;

use dbt_state_server::capture::{self, CaptureInput, DecisionKind};
use dbt_state_server::decision::{DecisionReason, Reuse};
use dbt_state_server::explain::{decision_description, explain_entry, explain_lines, ExplainNode};
use dbt_state_server::submit_decision::{self, SubmitOutcome};
use support::scenario::{
    built_at, confirm_in, environment_model, environment_target, outcome, Scenario,
    ENVIRONMENT_NAMESPACE,
};
use support::TestDatabase;

const ORG: &str = "org-explain-anchor";
const BUILT: i64 = 1_000;

/// The public renderer accepts the same struct literal as before copy evidence
/// was added to persisted decisions.
#[test]
fn library_caller_can_render_a_node_without_stored_copy_evidence() {
    let node = ExplainNode {
        display_submitted_at_ms: None,
        template_comparison: None,
        volatile_evaluation: Default::default(),
        execution_decision_id: "build".into(),
        decision: DecisionKind::Build,
        execution_type: 1,
        decision_description: Some("first build".into()),
        reason: Some(DecisionReason::FirstBuild),
        is_stale: false,
        target_table: Some(environment_target("dev")),
        display_target_table: None,
        clone_source: None,
        input_tables: vec![],
        query_dependencies: vec![],
        tolerance_ms: 0,
        build_age_ms: None,
        skip_rejection_reason: None,
        unmatched: None,
        reads_relation: None,
        anchor: None,
    };
    let entry = explain_entry(&node);
    assert_eq!(decision_description(&node), "first build");
    assert_eq!(entry.explain_lines, explain_lines(&node));
    assert!(!entry.explain_lines.is_empty());
}

fn now_ms() -> i64 {
    dbt_state_server::Clock::System.now_ms(&Default::default())
}

/// The `node_decisions` row a Skip capture writes, anchored on `matched`.
fn skip_capture(execution_decision_id: &str, matched: i64) -> CaptureInput {
    CaptureInput {
        display_submitted_at_ms: None,
        template_comparison: None,
        volatile_evaluation: Default::default(),
        org_id: ORG.into(),
        principal_id: None,
        external_invocation_id: None,
        session_id: None,
        project_external_id: Some("project".into()),
        project_name: None,
        environment_name: Some("prod".into()),
        profile_name: None,
        dialect: Some("snowflake".into()),
        database: Some("DB".into()),
        node_unique_id: Some("model.p.m".into()),
        node_name: None,
        node_fqn: None,
        resource_type: None,
        execution_type: 1,
        decision: DecisionKind::Skip,
        decision_reason: DecisionReason::Reused(Reuse::Fresh),
        is_stale: false,
        decision_description: None,
        request_id: None,
        execution_decision_id: Some(execution_decision_id.into()),
        node_body_hash: Some("body".into()),
        values_hash: None,
        table_namespace: Some(ENVIRONMENT_NAMESPACE.into()),
        target_table: Some(environment_target("prod")),
        default_schema: Some("prod".into()),
        clone_source: None,
        clone_sqls: None,
        copy_source_execution_id: None,
        input_tables: Vec::new(),
        query_dependencies: Vec::new(),
        execution_runtime_ms: None,
        matched_execution_id: Some(matched),
        freshness_tolerance_ms: None,
        build_age_ms: None,
        skip_rejection_reason: None,
        clone_rejection_reason: None,
        unmatched_execution_id: None,
        reads_relation: None,
    }
}

#[tokio::test]
async fn explain_anchor_is_the_complete_matched_execution() {
    let db = TestDatabase::new("explain_anchor").await;
    let state = db.state(false).await;
    let store = &state.store;

    // The model in `prod`, its own target absent and then at the build epoch.
    let model = environment_model("prod");
    let SubmitOutcome::Execute { ids, .. } =
        submit_decision::submit_sql(store, ORG, &model.clone().build(), now_ms())
            .await
            .expect("first submit")
    else {
        panic!("a first build executes");
    };
    let built = outcome().epoch(BUILT).table().runtime(42);
    assert!(confirm_in(store, ORG, built.confirm(&ids.request_id)).await);

    let SubmitOutcome::Skip {
        execution_decision_id,
        matched_execution,
        ..
    } = submit_decision::submit_sql(store, ORG, &model.own_epoch(Some(BUILT)).build(), now_ms())
        .await
        .expect("second submit")
    else {
        panic!("an unchanged rebuild skips");
    };
    capture::capture_decision(
        store.pool(),
        skip_capture(&execution_decision_id, matched_execution.id),
    )
    .await;

    let decisions = store
        .fetch_explain_decisions(ORG, std::slice::from_ref(&execution_decision_id))
        .await
        .expect("fetch explain decisions");
    let [decision] = decisions.as_slice() else {
        panic!("one captured decision, got {}", decisions.len());
    };
    let anchor = decision.anchor().expect("a skip has an anchor");

    assert_eq!(anchor.request_id, ids.request_id, "no defaulted fields");
    assert_eq!(anchor.node_unique_id.as_deref(), Some("model.p.m"));
    assert_eq!(
        anchor, matched_execution,
        "the anchor is the row the decision matched"
    );
}

#[tokio::test]
async fn explain_follows_the_selected_copy_when_its_source_is_rebuilt_during_lookup() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await.org(ORG);

    // Start a build of QA. Before it confirms, QA receives a copy of PROD.
    // The copy and the replacement build both read the same upstream epoch.
    let qa_target = r#""DB"."PROD"."QA_M""#;
    let dev_target = r#""DB"."PROD"."DEV_M""#;
    let qa = environment_model("prod").target(qa_target);
    let replacement = sc.execute_id(qa.clone()).await;
    sc.build(environment_model("prod"), 1_000).await;
    let selected = sc.expect_clone(qa).await;
    assert!(sc.confirm(&selected.request_id, built_at(2_000)).await);
    assert!(sc.confirm(&replacement, built_at(3_000)).await);

    let replacement = support::rows::execution_in(&server, ORG, &replacement)
        .await
        .expect("confirmed replacement");
    support::fault::replace_source_after_candidate_lookup(&server, replacement.id).await;
    let copy = sc
        .expect_clone(environment_model("prod").target(dev_target))
        .await;
    assert_eq!(copy.clone_source, qa_target);
    assert_eq!(copy.clone_required_last_modified_epoch, Some(2_000));

    let id = copy.execution_decision_id.unwrap();
    let explain = sc.explain(&[&id]).await;
    let upstream = &explain.messages[0].explain_lines[2].children[0].children[0];
    assert_eq!(upstream.badge, Some(1)); // FRESH
    assert_eq!(
        upstream.children[0].text,
        format!("no updates since {dev_target} last executed")
    );

    // QA's selected copy holds PROD's build. Recording QA's newer build
    // would give the same badge here but would cite the wrong evidence.
    let decisions = server
        .state
        .store
        .fetch_explain_decisions(ORG, &[id])
        .await
        .unwrap();
    let build = decisions[0]
        .copy_source_build
        .as_ref()
        .expect("selected copy's held build");
    assert_eq!(build["target_table"], environment_target("prod"));
}
