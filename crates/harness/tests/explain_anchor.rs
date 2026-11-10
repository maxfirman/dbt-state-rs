//! The Explain freshness anchor is the complete execution row the decision
//! matched, read through the same execution read type as State history, against
//! a real per-test Postgres schema (no gRPC). Explain tree shapes stay covered by
//! the Explain conformance gate (`conformance.rs`) and `clone_lifecycle.rs`.

#[path = "support.rs"]
mod support;

use dbt_state_server::capture::{self, CaptureInput, DecisionKind};
use dbt_state_server::decision::{DecisionReason, Reuse};
use dbt_state_server::submit_decision::{self, SubmitOutcome};
use support::scenario::{
    confirm_in, environment_model, environment_target, outcome, ENVIRONMENT_NAMESPACE,
};
use support::TestDatabase;

const ORG: &str = "org-explain-anchor";
const BUILT: i64 = 1_000;

fn now_ms() -> i64 {
    dbt_state_server::Clock::System.now_ms(&Default::default())
}

/// The `node_decisions` row a Skip capture writes, anchored on `matched`.
fn skip_capture(execution_decision_id: &str, matched: i64) -> CaptureInput {
    CaptureInput {
        org_id: ORG.into(),
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
        input_tables: Vec::new(),
        query_dependencies: Vec::new(),
        execution_runtime_ms: None,
        matched_execution_id: Some(matched),
        freshness_tolerance_ms: None,
        build_age_ms: None,
        skip_rejection_reason: None,
        clone_rejection_reason: None,
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
