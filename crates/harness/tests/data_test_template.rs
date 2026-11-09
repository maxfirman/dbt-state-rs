//! DATA-TEST compare_unrendered_code DUAL IDENTITY (review #2 finding 6
//! FOLLOW-UP regression). The finding-6 fix threaded the unrendered-template
//! hash into the MODEL lookup (`find_confirmed`) but NOT the DATA-TEST lookup
//! (`find_confirmed_test`), which still compared only the rendered
//! `node_sql_hash`.
//!
//! Fusion reads `compare_unrendered_code` from `DataTestState` just like model
//! state (public Rust client run_cache_service.rs), so a data test with
//! `compare_unrendered_code = true`, a STABLE unrendered template and a changed
//! interpolated rendered literal must be REUSED (SKIP) — the documented
//! "rebuild only when BOTH the template and the rendered SQL change" contract.
//! Before the fix the data-test path EXECUTED on a rendered-only change.
//!
//! These tests exercise execution type 8 end-to-end against the server (TDD):
//! the template-only / rendered-only cells SKIP and the both-changed cell
//! EXECUTES, mirroring template_comparison.rs for models.

#[path = "support.rs"]
mod support;

use support::scenario::{data_test, outcome, test_results, Scenario};
use support::start_server;

const TEST_UID: &str = "test.jaffle_shop.cuc_test.deadbeef02";
const PROJECT: &str = "70437463667433";
const NS: &str = "dt-cuc-ns";
const UPSTREAM: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"DT_UP\"";

/// Build + confirm a cuc data test, then re-submit with the SAME template but a
/// changed interpolated rendered literal (upstream unchanged) -> must SKIP.
/// This is the finding-6 regression: the data-test lookup must honor the
/// template identity, not only the rendered hash.
#[tokio::test]
async fn data_test_rendered_only_change_skips_under_cuc() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // A `compare_unrendered_code=true` data-test (execution_type=8) submit with
    // a given rendered SQL and body (template) hash, over one fixed upstream.
    let cuc_test = data_test(TEST_UID)
        .catalog("CDO_DEMO_DB")
        .schema("PROD_STATE_DEMO")
        .compare_unrendered_code()
        .namespace(NS)
        .project(PROJECT)
        .target_name("prod_demo");
    let cuc_test_submit = |sql: &str, body_hash: &str, upstream_epoch: i64| {
        cuc_test
            .clone()
            .sql(sql)
            .body(body_hash)
            .upstream(UPSTREAM, upstream_epoch)
    };

    // Baseline: template "body-A", rendered literal '2020-01-01'.
    sc.build_with(
        cuc_test_submit(
            "select * from CDO_DEMO_DB.PROD_STATE_DEMO.DT_UP where ordered_at > '2020-01-01'",
            "body-A",
            100,
        ),
        outcome().results(test_results(0)),
    )
    .await;

    // Rendered literal changes, TEMPLATE (body hash) identical, upstream same ->
    // SKIP (reuse the cached pass; template unchanged means logically same test).
    assert_eq!(
        sc.verdict(cuc_test_submit(
            "select * from CDO_DEMO_DB.PROD_STATE_DEMO.DT_UP where ordered_at > '2021-06-30'",
            "body-A",
            100,
        ))
        .await,
        "skip_execution",
        "cuc data test: a rendered-only change with identical template must SKIP"
    );
}

/// Template SAME, rendered SAME, upstream SAME -> SKIP (the plain reuse cell).
#[tokio::test]
async fn data_test_unchanged_skips_under_cuc() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let cuc_test = data_test(TEST_UID)
        .catalog("CDO_DEMO_DB")
        .schema("PROD_STATE_DEMO")
        .compare_unrendered_code()
        .namespace(NS)
        .project(PROJECT)
        .target_name("prod_demo");
    let cuc_test_submit = |sql: &str, body_hash: &str, upstream_epoch: i64| {
        cuc_test
            .clone()
            .sql(sql)
            .body(body_hash)
            .upstream(UPSTREAM, upstream_epoch)
    };

    sc.build_with(
        cuc_test_submit("select * from DT_UP where x < 0", "body-A", 100),
        outcome().results(test_results(0)),
    )
    .await;

    assert_eq!(
        sc.verdict(cuc_test_submit(
            "select * from DT_UP where x < 0",
            "body-A",
            100
        ))
        .await,
        "skip_execution",
        "cuc data test: unchanged template+rendered+upstream must SKIP"
    );
}

/// Template CHANGED and rendered CHANGED -> EXECUTE (rebuild only when BOTH
/// change). Mirrors the model truth table for data tests.
#[tokio::test]
async fn data_test_both_changed_executes_under_cuc() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let cuc_test = data_test(TEST_UID)
        .catalog("CDO_DEMO_DB")
        .schema("PROD_STATE_DEMO")
        .compare_unrendered_code()
        .namespace(NS)
        .project(PROJECT)
        .target_name("prod_demo");
    let cuc_test_submit = |sql: &str, body_hash: &str, upstream_epoch: i64| {
        cuc_test
            .clone()
            .sql(sql)
            .body(body_hash)
            .upstream(UPSTREAM, upstream_epoch)
    };

    sc.build_with(
        cuc_test_submit("select * from DT_UP where x < 0", "body-A", 100),
        outcome().results(test_results(0)),
    )
    .await;

    assert_eq!(
        sc.verdict(cuc_test_submit(
            "select * from DT_UP where x < 5",
            "body-B",
            100
        ))
        .await,
        "ready_to_execute",
        "cuc data test: a change to BOTH template and rendered SQL must EXECUTE"
    );
}

/// Upstream freshness still governs under cuc: even with an unchanged template
/// AND unchanged rendered SQL, a NEWER upstream epoch must EXECUTE (the
/// finding-1 data-test freshness anchor is preserved alongside dual identity).
#[tokio::test]
async fn data_test_upstream_drift_executes_even_under_cuc() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let cuc_test = data_test(TEST_UID)
        .catalog("CDO_DEMO_DB")
        .schema("PROD_STATE_DEMO")
        .compare_unrendered_code()
        .namespace(NS)
        .project(PROJECT)
        .target_name("prod_demo");
    let cuc_test_submit = |sql: &str, body_hash: &str, upstream_epoch: i64| {
        cuc_test
            .clone()
            .sql(sql)
            .body(body_hash)
            .upstream(UPSTREAM, upstream_epoch)
    };

    sc.build_with(
        cuc_test_submit("select * from DT_UP where x < 0", "body-A", 100),
        outcome().results(test_results(0)),
    )
    .await;

    assert_eq!(
        sc.verdict(cuc_test_submit(
            "select * from DT_UP where x < 0",
            "body-A",
            200
        ))
        .await,
        "ready_to_execute",
        "cuc data test: a newer upstream epoch must still EXECUTE (freshness)"
    );
}

/// Pin the hosted truth directly from the live capture
/// (experiments/data_test_template.sh,
/// golden/fixtures/data_test_compare_unrendered.jsonl): a cuc=true DATA TEST
/// (execution_type=8) whose unrendered template changed (Jinja comment) but
/// rendered SQL is byte-identical is REUSED (skip_execution), with the hosted
/// description "data test was a no-op because both its query and its upstream
/// data are up to date". This is the finding-6 follow-up regression proof: the
/// data-test lookup must honor the template identity like the model lookup.
#[test]
fn hosted_data_test_cuc_template_change_skips_shape_is_pinned() {
    use dbt_state_harness::diff;
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/data_test_compare_unrendered.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load data-test cuc fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 2, "baseline + template-changed re-submit");
    for s in &submits {
        assert_eq!(
            s.request["execution_type"].as_i64(),
            Some(8),
            "both submits are data tests (execution_type 8)"
        );
        assert_eq!(
            s.request["compare_unrendered_code"].as_bool(),
            Some(true),
            "both submits set compare_unrendered_code"
        );
    }
    // Template (body hash) changed; rendered SQL identical.
    let body = |i: usize| {
        submits[i].request["dbt_node_state"]["node_body_hash"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let rendered = |i: usize| {
        submits[i].request["sql"]
            .as_str()
            .unwrap()
            .trim()
            .to_string()
    };
    assert_ne!(body(0), body(1), "the template (body hash) changed");
    assert_eq!(
        rendered(0),
        rendered(1),
        "the rendered SQL is byte-identical"
    );
    assert_eq!(
        diff::decision_variant(&submits[0].response).unwrap(),
        "ready_to_execute",
        "first build executes"
    );
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "skip_execution",
        "a template-only change on a data test SKIPS (hosted truth)"
    );
}
