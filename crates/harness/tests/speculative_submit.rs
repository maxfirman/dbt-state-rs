//! SubmitEnrichedSQLSpeculative (#103). A speculative request carries only
//! the upstream epochs the client had already fetched, so the server decides
//! it only where the decision needs no table evidence at all: a data test
//! with no Target relation and no reported tables that the ordinary submit
//! would SKIP. Hosted SKIPs that case on the speculative call
//! (`explain_rebuild_reason_gaps.jsonl` entry 39, replayed by
//! `conformance.rs`) and answers `undecided` to every other speculative call
//! in the corpus, after which the client sends the ordinary submit.

#[path = "support.rs"]
mod support;

use dbt_state_proto::query_cache as qc;
use support::scenario::{
    data_test, into_speculative_skip, model, outcome, speculative_variant, test_results, Scenario,
};
use support::start_server;

const PROJECT: &str = "70437463667433";
const NS: &str = "speculative-ns";
const UPSTREAM: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SPEC_UP\"";
const TARGET: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SPEC_MODEL\"";

#[tokio::test]
async fn unchanged_data_test_without_upstreams_skips_on_the_speculative_submit() {
    use qc::explain_client::ExplainClient;

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let test = data_test("test.jaffle_shop.spec_t0")
        .namespace(NS)
        .project(PROJECT);

    // A first run has nothing to reuse: undecided, and the client builds
    // through the ordinary submit.
    assert_eq!(
        speculative_variant(&sc.speculative(test.clone()).await),
        "undecided"
    );
    sc.build_with(
        test.clone(),
        outcome().results(test_results(0)).runtime(3058),
    )
    .await;

    let speculative = into_speculative_skip(sc.speculative(test.clone()).await);
    let ordinary = sc.expect_skip(test.clone()).await;
    let speculative_edid = speculative
        .execution_decision_id
        .clone()
        .expect("a speculative SKIP is an explainable decision");
    assert_ne!(
        Some(&speculative_edid),
        ordinary.execution_decision_id.as_ref(),
        "each decision mints its own id"
    );
    assert_eq!(
        qc::SkipExecutionResponse {
            execution_decision_id: None,
            ..speculative
        },
        qc::SkipExecutionResponse {
            execution_decision_id: None,
            ..ordinary.clone()
        },
        "the speculative SKIP is the ordinary submit's SKIP"
    );
    assert_eq!(
        ordinary
            .explained_decision
            .as_ref()
            .map(|d| d.decision_description.as_str()),
        Some("data test was a no-op because both its query and its upstream data are up to date")
    );
    assert_eq!(ordinary.execution_runtime_ms, Some(3058));

    // `dbt state explain` of the speculative decision.
    let mut explain = ExplainClient::new(sc.channel());
    let mut message = None;
    for _ in 0..100 {
        let resp = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![speculative_edid.clone()],
            })
            .await
            .unwrap()
            .into_inner();
        if let Some(m) = resp.messages.into_iter().next() {
            message = Some(m);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let message = message.expect("the speculative SKIP was recorded for Explain");
    assert_eq!(message.decision, 0, "SKIP");
    let sections: Vec<&str> = message
        .explain_lines
        .iter()
        .map(|l| l.text.as_str())
        .collect();
    assert_eq!(sections, ["query analysis", "data freshness analysis"]);
}

#[tokio::test]
async fn data_test_with_an_upstream_stays_undecided() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let test = data_test("test.jaffle_shop.spec_up")
        .namespace(NS)
        .project(PROJECT)
        .upstream(UPSTREAM, 100);
    sc.build_with(test.clone(), outcome().results(test_results(0)))
        .await;

    assert_eq!(
        speculative_variant(&sc.speculative(test.clone()).await),
        "undecided",
        "an upstream epoch on a speculative request is not evidence"
    );
    assert_eq!(sc.verdict(test).await, "skip_execution", "control");
}

#[tokio::test]
async fn incomplete_cached_results_stay_undecided() {
    use qc::value::Kind;

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let test = data_test("test.jaffle_shop.spec_partial")
        .namespace(NS)
        .project(PROJECT);
    let only_failures = qc::Struct {
        fields: [(
            "failures".to_string(),
            qc::Value {
                kind: Some(Kind::IntValue(0)),
            },
        )]
        .into(),
    };
    sc.build_with(test.clone(), outcome().results(only_failures))
        .await;

    assert_eq!(
        speculative_variant(&sc.speculative(test.clone()).await),
        "undecided"
    );
    assert_eq!(
        sc.verdict(test).await,
        "ready_to_execute",
        "control: incomplete cached results are rebuilt (AGENTS.md invariant 5)"
    );
}

#[tokio::test]
async fn model_without_upstreams_stays_undecided() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let built = model(TARGET).namespace(NS);
    sc.build(built.clone(), 1_000).await;

    // Client 2.0.11 sends a model's speculative request without its Target
    // relation's epoch; the ordinary submit's shape is not decided either.
    for req in [built.clone().no_tables(), built.clone()] {
        assert_eq!(speculative_variant(&sc.speculative(req).await), "undecided");
    }
    assert_eq!(sc.verdict(built).await, "skip_execution", "control");
}

#[tokio::test]
async fn another_orgs_history_never_yields_a_speculative_skip() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let (built_in, other) = (sc.org("org-spec-a"), sc.org("org-spec-b"));
    let test = data_test("test.jaffle_shop.spec_org")
        .namespace(NS)
        .project(PROJECT);
    built_in
        .build_with(test.clone(), outcome().results(test_results(0)))
        .await;

    assert_eq!(
        speculative_variant(&other.speculative(test.clone()).await),
        "undecided"
    );
    assert_eq!(
        speculative_variant(&sc.speculative(test.clone()).await),
        "undecided",
        "nor the default org's"
    );
    assert_eq!(
        speculative_variant(&built_in.speculative(test).await),
        "skip_execution",
        "control"
    );
}
