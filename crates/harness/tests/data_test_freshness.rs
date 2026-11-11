//! DATA-TEST UPSTREAM FRESHNESS (issue #2, finding 1) — live-captured from
//! api.state.dbt.com (Fusion client, Snowflake;
//! experiments/data_test_freshness{,_control}.sh).
//!
//! A dbt data test (execution_type = 8) produces no physical object, so the
//! client confirms it with `ConfirmExecution.last_modified_epoch = None` (only
//! the test results struct). The hosted service nonetheless invalidates a cached
//! test result when the TESTED (upstream) data changed:
//!
//!   baseline : dt_test submit with DT_UP@735547 -> ready_to_execute
//!              confirm lme=None, results {failures:0, should_error/warn:false}
//!   treatment: dt_up recreated in place (identical content, epoch -> 752144);
//!              dt_test submit with DT_UP@752144  -> ready_to_execute
//!              ("data test was executed because ... its query changed")
//!   control  : dt_test submit with DT_UP@752144 (unchanged) -> skip_execution
//!              ("data test was a no-op because both its query and its upstream
//!               data are up to date")
//!
//! The ONLY difference between the treatment and control re-runs is the upstream
//! epoch, isolating the cause: a data test's freshness anchor is the RECORDED
//! UPSTREAM epoch (it has no own-build epoch), compared per input with strict
//! `>`. Golden fixture: golden/fixtures/data_test_upstream_freshness.jsonl.
//!
//! BUG this pins/guards: the decision engine previously returned
//! should_rebuild=false for a data test with no own-build epoch BEFORE comparing
//! upstream epochs, so it SKIPPED and reused a stale cached pass/fail after the
//! tested data changed. The fix anchors a data test's freshness on the recorded
//! upstream epoch.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use support::scenario::{data_test, outcome, test_results, Scenario};
use support::start_server;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/data_test_upstream_freshness.jsonl"
);

const TEST_UID: &str = "test.jaffle_shop.dt_test.deadbeef01";
const PROJECT: &str = "70437463667433";
const NS: &str = "dt-ns";

const UPSTREAM: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"DT_UP\"";

/// End-to-end: a confirmed-passing data test must EXECUTE after its upstream
/// data changed, and SKIP when the upstream is unchanged.
#[tokio::test]
async fn data_test_reruns_when_upstream_data_changed() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // A data-test (execution_type=8) submit over one upstream `DB.S.DT_UP@epoch`.
    // Data tests carry no physical target.
    let test = data_test(TEST_UID)
        .catalog("CDO_DEMO_DB")
        .schema("PROD_STATE_DEMO")
        .sql("select * from CDO_DEMO_DB.PROD_STATE_DEMO.DT_UP where amount < 0")
        .namespace(NS)
        .project(PROJECT)
        .target_name("prod_demo")
        .body("test-body");
    let test_submit = |upstream_epoch: i64| test.clone().upstream(UPSTREAM, upstream_epoch);

    // Baseline: first build of the test over DT_UP@100, then confirm it with no
    // object epoch, only results (just like the client).
    sc.build_with(test_submit(100), outcome().results(test_results(0)))
        .await;

    // CONTROL: same upstream epoch -> the hosted service SKIPs (reuses the pass).
    assert_eq!(
        sc.verdict(test_submit(100)).await,
        "skip_execution",
        "an unchanged data test (same upstream epoch) must skip"
    );

    // TREATMENT: the upstream data changed (newer epoch) -> must EXECUTE, NOT
    // reuse the stale cached pass. This is the finding-1 fix.
    assert_eq!(
        sc.verdict(test_submit(200)).await,
        "ready_to_execute",
        "a data test must re-run when its tested (upstream) data changed"
    );

    // An older-or-equal upstream (clock skew / restore) is still fresh -> skip.
    assert_eq!(
        sc.verdict(test_submit(50)).await,
        "skip_execution",
        "an older upstream epoch is not drift"
    );
}

/// Pin the hosted truth table directly from the live capture: the treatment
/// submit (upstream newer than recorded) EXECUTES; the control submit (upstream
/// equal to recorded) SKIPS; and the test's confirmation carried no object
/// epoch (last_modified_epoch = null).
#[test]
fn hosted_data_test_freshness_shape_is_pinned() {
    let entries = diff::load_golden(FIXTURE).expect("load data-test freshness fixture");

    // The test's ConfirmExecution carries NO object epoch, only results.
    let test_confirms: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| {
            e.method == "ConfirmExecution"
                && e.request["execution_results"].is_object()
                && e.request["execution_results"]["fields"]["failures"].is_object()
        })
        .collect();
    assert!(
        !test_confirms.is_empty(),
        "fixture has a data-test confirmation with results"
    );
    for c in &test_confirms {
        assert!(
            c.request["last_modified_epoch"].is_null(),
            "a data test is confirmed with no object epoch (last_modified_epoch = null)"
        );
    }

    // Among the data-test (execution_type=8) submits, there is at least one
    // EXECUTE whose upstream epoch is strictly greater than an earlier SKIP/
    // baseline upstream epoch, and the equal-epoch submits SKIP.
    let dt_submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| {
            e.method == "SubmitEnrichedSQL" && e.request["execution_type"].as_i64() == Some(8)
        })
        .collect();
    assert!(
        dt_submits.len() >= 3,
        "baseline + treatment + control submits"
    );

    let upstream_epoch = |e: &diff::GoldenEntry| -> i64 {
        e.request["tables"][0]["last_modified_epoch"]
            .as_i64()
            .expect("upstream epoch")
    };
    let variant = |e: &diff::GoldenEntry| diff::decision_variant(&e.response).unwrap();

    // Collect (epoch, variant) pairs.
    let pairs: Vec<(i64, String)> = dt_submits
        .iter()
        .map(|e| (upstream_epoch(e), variant(e)))
        .collect();

    // There is a strictly-newer-upstream EXECUTE and an equal-upstream SKIP.
    let max_epoch = pairs.iter().map(|(ep, _)| *ep).max().unwrap();
    let min_epoch = pairs.iter().map(|(ep, _)| *ep).min().unwrap();
    assert!(
        max_epoch > min_epoch,
        "the fixture advances the upstream epoch"
    );

    // The submit at the OLD epoch (first build) executed; a submit at the NEW
    // epoch BEFORE the re-confirm executed (invalidation); and a submit at the
    // NEW epoch AFTER the re-confirm skipped (control).
    assert!(
        pairs
            .iter()
            .any(|(ep, v)| *ep == max_epoch && v == "ready_to_execute"),
        "a newer-upstream submit EXECUTES (invalidation)"
    );
    assert!(
        pairs
            .iter()
            .any(|(ep, v)| *ep == max_epoch && v == "skip_execution"),
        "an unchanged (same-epoch, re-confirmed) submit SKIPS (control)"
    );
}

// ---------------------------------------------------------------------------
// store_failures audit relation (#88)
// ---------------------------------------------------------------------------

/// The `store_failures` test's tested source and its own audit relation, as
/// client 2.0.11 names them (explain_data_test_history.jsonl `t75s`): the
/// audit relation is the test's database, schema and name.
const SF_SOURCE: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SF_SRC\"";
const SF_AUDIT: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SF_TEST\"";

/// Hosted SKIPs an unchanged `store_failures` test although its audit
/// relation now appears in `tables`: it is the test's own write. The audit
/// relation is excluded, not every unrecorded table: a genuinely new upstream
/// next to it changes the dependency set and rebuilds.
#[tokio::test]
async fn store_failures_audit_relation_is_ignored_but_a_new_upstream_rebuilds() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // A `store_failures: true` singular test over SF_SOURCE. Its first
    // execution reports only the source (the audit relation does not exist).
    let test = data_test("test.jaffle_shop.sf_test")
        .catalog("CDO_DEMO_DB")
        .schema("prod_state_demo")
        .sql("select id from CDO_DEMO_DB.prod_state_demo.sf_src where id < 0")
        .namespace(NS)
        .project(PROJECT)
        .target_name("prod_demo")
        .label("dbt_node_name", "sf_test")
        .extra("store_failures", "true")
        .extra("store_failures_as", "\"table\"")
        .upstream(SF_SOURCE, 100);
    sc.build_with(test.clone(), outcome().results(test_results(0)))
        .await;
    let rerun = test.clone().upstream(SF_AUDIT, 150);
    assert_eq!(sc.verdict(rerun.clone()).await, "skip_execution");

    let decision = sc
        .expect_execute(rerun.upstream("\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SF_NEW\"", 100))
        .await
        .explained_decision
        .expect("explained decision");
    assert_eq!(
        (
            decision.skip_rejection_reason,
            decision.clone_rejection_reason
        ),
        (Some(6), Some(6)),
        "a dependency-set change rebuilds as a changed query (6/6)"
    );
}

/// A table that resembles the audit relation but is not the test's own
/// relation still counts as a new input: the same name in another schema or
/// database, a case-sensitive quoted schema, or the exact name for a test
/// that does not store its failures. (An unnamed table in the test's own
/// schema is the aliased-audit case, #123, below.)
#[tokio::test]
async fn tables_resembling_the_audit_relation_still_count() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // One test (own unique id, so its own State history) per case.
    let test = |case: usize, store_failures: &str| {
        data_test(&format!("test.jaffle_shop.sf_test.{case}"))
            .catalog("CDO_DEMO_DB")
            .schema("prod_state_demo")
            .sql("select id from CDO_DEMO_DB.prod_state_demo.sf_src where id < 0")
            .namespace(NS)
            .project(PROJECT)
            .target_name("prod_demo")
            .label("dbt_node_name", "sf_test")
            .extra("store_failures", store_failures)
            .upstream(SF_SOURCE, 100)
    };
    let cases = [
        ("\"CDO_DEMO_DB\".\"OTHER_SCHEMA\".\"SF_TEST\"", "true"),
        ("\"OTHER_DB\".\"PROD_STATE_DEMO\".\"SF_TEST\"", "true"),
        (
            "\"CDO_DEMO_DB\".\"prod_state_demo\".\"SF_TEST_AUD\"",
            "true",
        ),
        // A test that does not store failures reads a table of its own name.
        (SF_AUDIT, "false"),
        (
            "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SF_TEST_AUD\"",
            "false",
        ),
    ];
    for (case, (name, store_failures)) in cases.into_iter().enumerate() {
        sc.build_with(
            test(case, store_failures),
            outcome().results(test_results(0)),
        )
        .await;
        assert_eq!(
            sc.verdict(test(case, store_failures).upstream(name, 150))
                .await,
            "ready_to_execute",
            "{name} (store_failures {store_failures}) is a new input"
        );
    }
}

/// A `store_failures` test whose name matches a relation it reads (Codex
/// review on #109: test `orders` with alias `orders_audit` reading `orders`).
/// The request does not carry the alias, so the reported `orders` cannot be
/// told apart from an unaliased audit relation by name; because the test's SQL
/// reads it, it stays an input and its drift rebuilds the test.
#[tokio::test]
async fn a_same_named_relation_the_test_reads_stays_an_input() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let orders = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ORDERS\"";
    let test = |epoch: i64| {
        data_test("test.jaffle_shop.orders")
            .catalog("CDO_DEMO_DB")
            .schema("prod_state_demo")
            .sql("select id from CDO_DEMO_DB.prod_state_demo.orders where id < 0")
            .namespace(NS)
            .project(PROJECT)
            .target_name("prod_demo")
            .label("dbt_node_name", "orders")
            .extra("store_failures", "true")
            .extra("store_failures_as", "\"table\"")
            .upstream(orders, epoch)
    };
    sc.build_with(test(100), outcome().results(test_results(0)))
        .await;
    assert_eq!(
        sc.verdict(test(100)).await,
        "skip_execution",
        "unchanged (control)"
    );
    assert_eq!(
        sc.verdict(test(200)).await,
        "ready_to_execute",
        "the read relation changed, so the cached result is stale"
    );
}

// ---------------------------------------------------------------------------
// store_failures audit relation in a separate schema or under an alias (#106)
// ---------------------------------------------------------------------------

/// The test's schema and its audit relation when a schema macro puts the
/// audit relation outside the target schema
/// (explain_store_failures_audit_schema.jsonl).
const SF_AUDIT_SCHEMA: &str = "prod_state_demo_dbt_test__audit";
const SF_SEPARATE_AUDIT: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO_DBT_TEST__AUDIT\".\"SF_TEST\"";

/// Client 2.0.11 sends the test's own schema as `default_schema`, so an audit
/// relation in a separate audit schema is recognized: the unchanged rerun
/// SKIPs as hosted does. The test's name in the target schema is a lookalike
/// and rebuilds, and so does a relation of its name in the audit schema that
/// its SQL reads.
#[tokio::test]
async fn audit_relation_in_a_separate_schema_is_ignored_but_lookalikes_rebuild() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // A `store_failures: true` test over SF_SOURCE in the given schema.
    let sf_test_in = |uid: &str, schema: &str, sql: &str| {
        data_test(uid)
            .catalog("CDO_DEMO_DB")
            .schema(schema)
            .sql(sql)
            .namespace(NS)
            .project(PROJECT)
            .target_name("prod_demo")
            .label("dbt_node_name", "sf_test")
            .extra("store_failures", "true")
            .extra("store_failures_as", "\"table\"")
            .upstream(SF_SOURCE, 100)
    };
    let sql = "select id from CDO_DEMO_DB.prod_state_demo.sf_src where id < 0";

    let test = sf_test_in("test.jaffle_shop.sf_test", SF_AUDIT_SCHEMA, sql);
    sc.build_with(test.clone(), outcome().results(test_results(0)))
        .await;
    assert_eq!(
        sc.verdict(test.clone().upstream(SF_SEPARATE_AUDIT, 150))
            .await,
        "skip_execution",
        "the audit relation in the test's own schema is its own write"
    );

    let lookalike = sf_test_in("test.jaffle_shop.sf_test.1", SF_AUDIT_SCHEMA, sql);
    sc.build_with(lookalike.clone(), outcome().results(test_results(0)))
        .await;
    assert_eq!(
        sc.verdict(lookalike.upstream(SF_AUDIT, 150)).await,
        "ready_to_execute",
        "the test's name in the target schema is a new input"
    );

    let reads = sf_test_in(
        "test.jaffle_shop.sf_test.2",
        SF_AUDIT_SCHEMA,
        "select id from CDO_DEMO_DB.prod_state_demo_dbt_test__audit.sf_test where id < 0",
    );
    sc.build_with(
        reads.clone().upstream(SF_SEPARATE_AUDIT, 100),
        outcome().results(test_results(0)),
    )
    .await;
    assert_eq!(
        sc.verdict(reads.clone().upstream(SF_SEPARATE_AUDIT, 100))
            .await,
        "skip_execution",
        "unchanged (control)"
    );
    assert_eq!(
        sc.verdict(reads.upstream(SF_SEPARATE_AUDIT, 200)).await,
        "ready_to_execute",
        "a relation the test reads stays an input, so its drift rebuilds"
    );
}

/// A configured `alias` names the audit relation and no request field carries
/// it (explain_store_failures_audit_alias.jsonl). The one reported table in the
/// test's schema that its SQL never mentions is its audit relation (#123,
/// ADR 0008): the unchanged rerun SKIPs as hosted does. A genuinely new
/// upstream next to it still rebuilds: one the SQL reads is an input, and an
/// unnamed second table makes the request ambiguous, so both stay inputs.
#[tokio::test]
async fn an_aliased_audit_relation_is_ignored_but_a_new_upstream_rebuilds() {
    const SF_ALIASED: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SF_TEST_AUD\"";
    const SF_NEW: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"SF_NEW\"";
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // A `store_failures: true` test whose SQL reads SF_SOURCE and SF_NEW; its
    // first execution reports only SF_SOURCE.
    let test = |uid: &str| {
        data_test(uid)
            .catalog("CDO_DEMO_DB")
            .schema("prod_state_demo")
            .sql(
                "select id from CDO_DEMO_DB.prod_state_demo.sf_src where id not in \
                 (select id from CDO_DEMO_DB.prod_state_demo.sf_new)",
            )
            .namespace(NS)
            .project(PROJECT)
            .target_name("prod_demo")
            .label("dbt_node_name", "sf_test")
            .extra("store_failures", "true")
            .extra("store_failures_as", "\"table\"")
            .upstream(SF_SOURCE, 100)
    };
    let rebuild_reason = |r: dbt_state_proto::query_cache::ReadyToExecuteResponse| {
        let d = r.explained_decision.expect("explained decision");
        (d.skip_rejection_reason, d.clone_rejection_reason)
    };

    let reads = test("test.jaffle_shop.sf_test");
    sc.build_with(reads.clone(), outcome().results(test_results(0)))
        .await;
    let rerun = reads.upstream(SF_ALIASED, 150);
    assert_eq!(
        sc.verdict(rerun.clone()).await,
        "skip_execution",
        "the aliased audit relation is the test's own write"
    );
    let r = sc.expect_execute(rerun.upstream(SF_NEW, 100)).await;
    assert_eq!(
        rebuild_reason(r),
        (Some(6), Some(6)),
        "a new upstream the SQL reads is a dependency-set change (6/6)"
    );

    let unnamed = test("test.jaffle_shop.sf_test.1")
        .sql("select id from CDO_DEMO_DB.prod_state_demo.sf_src where id < 0");
    sc.build_with(unnamed.clone(), outcome().results(test_results(0)))
        .await;
    let r = sc
        .expect_execute(unnamed.upstream(SF_ALIASED, 150).upstream(SF_NEW, 100))
        .await;
    assert_eq!(
        rebuild_reason(r),
        (Some(6), Some(6)),
        "two unnamed tables in the test's schema are ambiguous, so both count"
    );
}
