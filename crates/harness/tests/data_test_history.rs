//! SNOWFLAKE DATA-TEST HISTORY BY KEY (#200, maintainer decision option A,
//! ADR 0007 "Data-test history key").
//!
//! Hosted keeps no data-test history per Target or per unique id: it reuses
//! the latest confirmed run of the same normalized SQL over the same input
//! relations, with the same result-setting configuration, in the same
//! `table_namespace`. A relation hosted copied stands for its source while
//! the copy is unchanged. Characterized live on 2026-11-21 with client
//! 2.0.11 (`golden/fixtures/data_test_history.jsonl`,
//! `data_test_history_probes.jsonl`,
//! `docs/reports/snowflake-data-test-history-2026-11-21.md`).
//!
//! These tests drive the server over the protocol only.

#[path = "support.rs"]
mod support;

use support::demo::{
    self, relation, singular_test, table_model, test_run, transient_at, CATALOG, DEV_SCHEMA,
    PROD_SCHEMA,
};
use support::scenario::{data_test, Scenario};
use support::start_server;

fn failures(skip: &dbt_state_proto::query_cache::SkipExecutionResponse) -> i64 {
    use dbt_state_proto::query_cache::value::Kind;
    match skip
        .execution_results
        .as_ref()
        .and_then(|r| r.fields.get("failures"))
        .and_then(|v| v.kind.as_ref())
    {
        Some(Kind::IntValue(n)) => *n,
        other => panic!("no failures in the cached results: {other:?}"),
    }
}

/// Q6 (F87, F95): a new unique id's first submit reuses another unique id's
/// confirmed run of the same SQL, echoing that run's runtime.
#[tokio::test]
async fn a_unique_id_reuses_another_unique_ids_run_of_the_same_key() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.build_with(
        singular_test("test.p.xa", PROD_SCHEMA, "sh", 100),
        test_run(0, 2251),
    )
    .await;

    let skip = sc
        .expect_skip(singular_test("test.p.xb", PROD_SCHEMA, "sh", 100))
        .await;
    assert_eq!(skip.execution_runtime_ms, Some(2251), "xa's run is served");
    assert_eq!(failures(&skip), 0);
}

/// The same-key order cells (P60–P75): two unique ids with one SQL, both
/// submitted before either confirms, confirmed second-first. Every later
/// SKIP serves the later-confirmed run, its runtime and its failures,
/// whatever the unique id and the submit order.
#[tokio::test]
async fn every_unique_id_in_a_key_is_served_the_later_confirmed_run() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let first = sc
        .execute_id(singular_test("test.p.t", PROD_SCHEMA, "s", 100))
        .await;
    let second = sc
        .execute_id(singular_test("test.p.t2", PROD_SCHEMA, "s", 100))
        .await;
    assert!(sc.confirm(&second, test_run(0, 43022)).await);
    assert!(sc.confirm(&first, test_run(1, 43011)).await);

    for uid in ["test.p.t", "test.p.t2"] {
        let skip = sc
            .expect_skip(singular_test(uid, PROD_SCHEMA, "s", 100))
            .await;
        assert_eq!(skip.execution_runtime_ms, Some(43011), "{uid}");
        assert_eq!(failures(&skip), 1, "{uid}: the failure is served");
    }
}

/// Severity separates two histories (P26–P31), and an older run under
/// another key stays reusable: a data test has no Target relation that a
/// later run overwrites.
#[tokio::test]
async fn severity_separates_keys_and_an_older_run_under_another_key_is_reused() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let base = || singular_test("test.p.sev", PROD_SCHEMA, "s", 100);
    let warn = || base().extra("severity", "\"WARN\"");
    sc.build_with(base(), test_run(0, 41611)).await;
    sc.build_with(warn(), test_run(0, 41622)).await;

    let skip = sc.expect_skip(base()).await;
    assert_eq!(skip.execution_runtime_ms, Some(41611), "the base run");
    let skip = sc.expect_skip(warn()).await;
    assert_eq!(skip.execution_runtime_ms, Some(41622), "the WARN run");
}

/// `table_namespace` separates two histories (P16–P21), as do the other
/// result-setting config entries (conservative, uncaptured).
#[tokio::test]
async fn namespace_and_result_setting_config_separate_keys() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.build_with(
        singular_test("test.p.ns", PROD_SCHEMA, "s", 100),
        test_run(0, 41411),
    )
    .await;
    let variants = [
        singular_test("test.p.ns", PROD_SCHEMA, "s", 100).namespace("another-connection"),
        singular_test("test.p.ns", PROD_SCHEMA, "s", 100).extra("warn_if", "\"> 10\""),
        singular_test("test.p.ns", PROD_SCHEMA, "s", 100).extra("error_if", "\"> 10\""),
        singular_test("test.p.ns", PROD_SCHEMA, "s", 100).extra("fail_calc", "\"sum(n)\""),
        singular_test("test.p.ns", PROD_SCHEMA, "s", 100).extra("limit", "1"),
        singular_test("test.p.ns", PROD_SCHEMA, "s", 100).extra("where", "\"id > 0\""),
        singular_test("test.p.ns", PROD_SCHEMA, "s", 100).extra("store_failures", "true"),
    ];
    for variant in variants {
        assert_eq!(
            sc.verdict(variant.clone()).await,
            "ready_to_execute",
            "{:?}",
            variant.build().semantic_extras
        );
    }
}

/// Organization isolation: another organization's run of the same key is
/// never reused.
#[tokio::test]
async fn another_organizations_run_is_never_reused() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.org("org-a")
        .build_with(
            singular_test("test.p.org", PROD_SCHEMA, "s", 100),
            test_run(0, 10),
        )
        .await;
    assert_eq!(
        sc.org("org-b")
            .verdict(singular_test("test.p.org", PROD_SCHEMA, "s", 100))
            .await,
        "ready_to_execute"
    );
    assert_eq!(
        sc.verdict(singular_test("test.p.org", PROD_SCHEMA, "s", 100))
            .await,
        "ready_to_execute",
        "nor the default organization's"
    );
    assert_eq!(
        sc.org("org-a")
            .verdict(singular_test("test.p.org", PROD_SCHEMA, "s", 100))
            .await,
        "skip_execution"
    );
}

/// `project_id`, `target_name` and, for fully qualified SQL,
/// `default_schema` do not separate histories (P0–P15). SQL that names a
/// relation without its schema resolves it in the default schema, so there
/// the default schema stays in the key (conservative, uncaptured).
#[tokio::test]
async fn project_target_and_default_schema_do_not_separate_qualified_sql() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.build_with(
        singular_test("test.p.q", PROD_SCHEMA, "s", 100),
        test_run(0, 41111),
    )
    .await;
    let elsewhere = singular_test("test.p.q", PROD_SCHEMA, "s", 100)
        .project("99999999999999")
        .target_name("dev_clone")
        .schema(DEV_SCHEMA);
    let skip = sc.expect_skip(elsewhere).await;
    assert_eq!(skip.execution_runtime_ms, Some(41111));

    let unqualified = |schema: &str| {
        singular_test("test.p.unq", PROD_SCHEMA, "s", 100)
            .sql("select id, v from s where id < 0")
            .schema(schema)
    };
    sc.build_with(unqualified(PROD_SCHEMA), test_run(0, 7))
        .await;
    assert_eq!(
        sc.verdict(unqualified(DEV_SCHEMA)).await,
        "ready_to_execute",
        "an unqualified relation resolves in the default schema"
    );
    assert_eq!(sc.verdict(unqualified(PROD_SCHEMA)).await, "skip_execution");
}

/// Build `name` on prod at `epoch`, then let the server copy it into dev
/// (a Result copy on submit), confirmed at `copy_epoch`.
async fn prod_build_copied_to_dev(sc: &Scenario, name: &str, epoch: i64, copy_epoch: i64) {
    let build = sc.execute_id(table_model(name, PROD_SCHEMA)).await;
    assert!(sc.confirm(&build, transient_at(epoch).runtime(3833)).await);
    let copy = sc.expect_clone(table_model(name, DEV_SCHEMA)).await;
    assert_eq!(copy.clone_source, relation(PROD_SCHEMA, name));
    assert!(sc.confirm(&copy.request_id, transient_at(copy_epoch)).await);
}

/// Q1 (F11, F34, F59, F76): prod, dev, prod, dev with unchanged inputs. The
/// dev upstream is the server's copy of the prod upstream, unchanged since
/// the copy, so the dev test reads it as the prod upstream: one execution
/// serves both Targets. Later reads report metadata epochs slightly before
/// the confirmations (F59, F76), which are still unchanged.
#[tokio::test]
async fn alternating_targets_share_one_run_through_a_recorded_copy() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    prod_build_copied_to_dev(&sc, "u", 1_000, 2_000).await;
    sc.build_with(
        singular_test("test.p.ta", PROD_SCHEMA, "u", 1_000),
        test_run(0, 969),
    )
    .await;

    for step in ["R2 dev", "R3 prod", "R4 dev"] {
        let submit = match step {
            "R2 dev" => singular_test("test.p.ta", DEV_SCHEMA, "u", 2_000),
            "R3 prod" => singular_test("test.p.ta", PROD_SCHEMA, "u", 990),
            _ => singular_test("test.p.ta", DEV_SCHEMA, "u", 1_990),
        };
        let skip = sc.expect_skip(submit).await;
        assert_eq!(skip.execution_runtime_ms, Some(969), "{step}: the prod run");
    }
}

/// Q3 tf (F10, F31, F44, F62, F79): an External modification of the dev copy
/// breaks it, so the dev upstream is itself from then on: the dev test
/// executes and fails, prod still reuses its own pass, and dev then reuses
/// its failing run. The older pass, read through the copy before the
/// change, is never resurrected for dev.
#[tokio::test]
async fn a_copy_changed_after_the_copy_stands_for_itself() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    prod_build_copied_to_dev(&sc, "uf", 1_000, 2_000).await;
    sc.build_with(
        singular_test("test.p.tf", PROD_SCHEMA, "uf", 1_000),
        test_run(0, 1014),
    )
    .await;
    assert_eq!(
        sc.expect_skip(singular_test("test.p.tf", DEV_SCHEMA, "uf", 2_000))
            .await
            .execution_runtime_ms,
        Some(1014),
        "R2 dev, through the unchanged copy"
    );

    let changed = sc
        .execute_id(singular_test("test.p.tf", DEV_SCHEMA, "uf", 2_500))
        .await;
    assert!(sc.confirm(&changed, test_run(1, 2220)).await);

    let prod = sc
        .expect_skip(singular_test("test.p.tf", PROD_SCHEMA, "uf", 1_000))
        .await;
    assert_eq!(
        (prod.execution_runtime_ms, failures(&prod)),
        (Some(1014), 0)
    );
    let dev = sc
        .expect_skip(singular_test("test.p.tf", DEV_SCHEMA, "uf", 2_500))
        .await;
    assert_eq!((dev.execution_runtime_ms, failures(&dev)), (Some(2220), 1));
}

/// Within one key only the latest confirmed run is reusable, failures
/// included: after upstream drift reruns a passing test and it fails, no
/// submit of the key is served the older pass, not even one reporting the
/// older epoch.
#[tokio::test]
async fn a_newer_failing_run_in_the_key_is_served_and_nothing_older() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.build_with(
        singular_test("test.p.nr", PROD_SCHEMA, "s", 100),
        test_run(0, 11),
    )
    .await;
    let rerun = sc
        .execute_id(singular_test("test.p.nr", PROD_SCHEMA, "s", 200))
        .await;
    assert!(sc.confirm(&rerun, test_run(3, 22)).await);

    for epoch in [200, 100] {
        let skip = sc
            .expect_skip(singular_test("test.p.nr", PROD_SCHEMA, "s", epoch))
            .await;
        assert_eq!(
            (skip.execution_runtime_ms, failures(&skip)),
            (Some(22), 3),
            "at {epoch}: the latest run"
        );
    }
    let other_uid = sc
        .expect_skip(singular_test("test.p.nr2", PROD_SCHEMA, "s", 200))
        .await;
    assert_eq!(other_uid.execution_runtime_ms, Some(22));
}

/// A copy stands for the build it was made from, at that build's epoch:
/// once the source is rebuilt and the test rerun there, the latest run in
/// the key read newer contents than the copy holds, and the dev test over
/// the older copy rebuilds.
#[tokio::test]
async fn a_copy_of_an_older_source_build_does_not_reuse_a_newer_run() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    prod_build_copied_to_dev(&sc, "u", 1_000, 2_000).await;
    sc.build_with(
        singular_test("test.p.old", PROD_SCHEMA, "u", 1_000),
        test_run(0, 1),
    )
    .await;
    // Modified after its build, so rebuilt with the same logic: the same
    // producer at a later epoch.
    let rebuild = sc
        .execute_id(table_model("u", PROD_SCHEMA).own_epoch(Some(1_500)))
        .await;
    assert!(sc.confirm(&rebuild, transient_at(3_000)).await);
    sc.build_with(
        singular_test("test.p.old", PROD_SCHEMA, "u", 3_000),
        test_run(0, 2),
    )
    .await;

    assert_eq!(
        sc.verdict(singular_test("test.p.old", DEV_SCHEMA, "u", 2_000))
            .await,
        "ready_to_execute"
    );
}

/// A client-requested copy (RegisterClone) of the prod build stands for it
/// too, while unchanged.
#[tokio::test]
async fn a_registered_copy_stands_for_its_source_build() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let build = sc.execute_id(table_model("r", PROD_SCHEMA)).await;
    assert!(sc.confirm(&build, transient_at(1_000).runtime(5)).await);
    let copy = sc
        .register_clone(
            demo::copy(&relation(PROD_SCHEMA, "r"), &relation(DEV_SCHEMA, "r"))
                .source_epoch(1_000)
                .label("dbt_node_unique_id", "model.jaffle_shop.r"),
        )
        .await
        .expect("RegisterClone")
        .ready_to_clone
        .expect("ready_to_clone");
    assert!(sc.confirm(&copy.request_id, transient_at(2_000)).await);
    sc.build_with(
        singular_test("test.p.rc", PROD_SCHEMA, "r", 1_000),
        test_run(0, 77),
    )
    .await;

    let skip = sc
        .expect_skip(singular_test("test.p.rc", DEV_SCHEMA, "r", 2_000))
        .await;
    assert_eq!(skip.execution_runtime_ms, Some(77));
}

/// An accepted risk inherited from model copy adoption (ADR 0004, ADR 0007
/// "Data-test history key"): the server cannot see an External modification
/// of a prod source between its build and a submit-time Result copy. The
/// dev submit reports no epoch of the source, and the copy's confirmation
/// reports the copy's own epoch, its clone time. So the copy stands for the
/// source build, and a dev data test over it reuses the prod run that read
/// the source before the change. A prod run that read the change is the
/// latest in the key, and then the dev test rebuilds. A RegisterClone names
/// the source epoch it copied, and a copy of another epoch than the recorded
/// build's stands for itself.
#[tokio::test]
async fn a_source_modified_before_a_submit_time_copy_is_not_visible() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // Built at 1000 and tested there; then externally modified at 1500,
    // which nothing reports, and copied into dev.
    prod_build_copied_to_dev(&sc, "xm", 1_000, 2_000).await;
    sc.build_with(
        singular_test("test.p.xm", PROD_SCHEMA, "xm", 1_000),
        test_run(0, 51),
    )
    .await;
    let skip = sc
        .expect_skip(singular_test("test.p.xm", DEV_SCHEMA, "xm", 2_000))
        .await;
    assert_eq!(
        skip.execution_runtime_ms,
        Some(51),
        "the prod run over the contents before the change is served"
    );

    let rerun = sc
        .execute_id(singular_test("test.p.xm", PROD_SCHEMA, "xm", 1_500))
        .await;
    assert!(sc.confirm(&rerun, test_run(0, 52)).await);
    assert_eq!(
        sc.verdict(singular_test("test.p.xm", DEV_SCHEMA, "xm", 2_000))
            .await,
        "ready_to_execute",
        "once a prod run read the change"
    );

    let build = sc.execute_id(table_model("xr", PROD_SCHEMA)).await;
    assert!(sc.confirm(&build, transient_at(1_000)).await);
    let copy = sc
        .register_clone(
            demo::copy(&relation(PROD_SCHEMA, "xr"), &relation(DEV_SCHEMA, "xr"))
                .source_epoch(1_500)
                .label("dbt_node_unique_id", "model.jaffle_shop.xr"),
        )
        .await
        .expect("RegisterClone")
        .ready_to_clone
        .expect("ready_to_clone");
    assert!(sc.confirm(&copy.request_id, transient_at(2_000)).await);
    sc.build_with(
        singular_test("test.p.xr", PROD_SCHEMA, "xr", 1_000),
        test_run(0, 53),
    )
    .await;
    assert_eq!(
        sc.verdict(singular_test("test.p.xr", DEV_SCHEMA, "xr", 2_000))
            .await,
        "ready_to_execute",
        "a RegisterClone of the modified source stands for itself"
    );
}

/// P48–P53: a dev relation this server never copied is another input, even
/// with the prod relation's name and epoch; both histories stay reusable.
#[tokio::test]
async fn a_relation_that_was_not_copied_is_another_input() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.build_with(
        singular_test("test.p.nc", PROD_SCHEMA, "nc", 1_000),
        test_run(0, 42011),
    )
    .await;
    let dev = sc
        .execute_id(singular_test("test.p.nc", DEV_SCHEMA, "nc", 1_000))
        .await;
    assert!(sc.confirm(&dev, test_run(0, 42022)).await);

    let prod = sc
        .expect_skip(singular_test("test.p.nc", PROD_SCHEMA, "nc", 1_000))
        .await;
    assert_eq!(prod.execution_runtime_ms, Some(42011));
    let dev = sc
        .expect_skip(singular_test("test.p.nc", DEV_SCHEMA, "nc", 1_000))
        .await;
    assert_eq!(dev.execution_runtime_ms, Some(42022));
}

/// Rows recorded before the key (ADR 0004): a data test without a key and a
/// copy without its source build cold-miss once, under
/// `compare_unrendered_code` too, and the rebuild is reusable.
#[tokio::test]
async fn rows_recorded_before_the_key_miss_safely() {
    use support::legacy;
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    prod_build_copied_to_dev(&sc, "u", 1_000, 2_000).await;
    let cuc = || {
        singular_test("test.p.cuc", PROD_SCHEMA, "u", 1_000)
            .sql(&format!(
                "select id, v from {CATALOG}.{PROD_SCHEMA}.u where id < -2"
            ))
            .compare_unrendered_code()
    };
    sc.build_with(
        singular_test("test.p.old", PROD_SCHEMA, "u", 1_000),
        test_run(0, 1),
    )
    .await;
    sc.build_with(cuc(), test_run(0, 2)).await;
    assert!(legacy::forget_data_test_history_keys(&server).await > 0);

    assert_eq!(
        sc.verdict(singular_test("test.p.old", DEV_SCHEMA, "u", 2_000))
            .await,
        "ready_to_execute",
        "a copy recorded before the key stands for itself"
    );
    assert_eq!(
        sc.verdict(cuc()).await,
        "ready_to_execute",
        "nor through the template identity"
    );
    let rebuild = sc
        .execute_id(singular_test("test.p.old", PROD_SCHEMA, "u", 1_000))
        .await;
    assert!(sc.confirm(&rebuild, test_run(0, 3)).await);
    let skip = sc
        .expect_skip(singular_test("test.p.old", PROD_SCHEMA, "u", 1_000))
        .await;
    assert_eq!(skip.execution_runtime_ms, Some(3));
}

/// `compare_unrendered_code` keeps the dual identity data tests had before
/// the key (AGENTS.md reuse invariant 5, ADR 0007 "Data-test history key"):
/// when the submit's key has no run, the latest keyed run of its unique id
/// in its project and physical scope is reused when its unrendered template
/// matches. That alternative is scoped by unique id and project, not by the
/// key, and is uncaptured under the key. A run recorded before the key never
/// matches through it.
#[tokio::test]
async fn a_template_match_under_another_key_reuses_the_unique_ids_keyed_run() {
    use support::legacy;
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let cuc = |uid: &str, bound: i64| {
        singular_test(uid, PROD_SCHEMA, "tpl", 100)
            .sql(&format!(
                "select id, v from {CATALOG}.{PROD_SCHEMA}.tpl where id < {bound}"
            ))
            .body("body-tpl")
            .compare_unrendered_code()
    };
    sc.build_with(cuc("test.p.tpl", 0), test_run(0, 31)).await;

    let skip = sc.expect_skip(cuc("test.p.tpl", -1)).await;
    assert_eq!(
        skip.execution_runtime_ms,
        Some(31),
        "another key with the same template reuses the unique id's run"
    );
    for (other, why) in [
        (cuc("test.p.tpl2", -1), "another unique id"),
        (
            cuc("test.p.tpl", -1).project("99999999999999"),
            "another project",
        ),
        (
            singular_test("test.p.tpl", PROD_SCHEMA, "tpl", 100)
                .sql(&format!(
                    "select id, v from {CATALOG}.{PROD_SCHEMA}.tpl where id < -1"
                ))
                .body("body-tpl"),
            "without compare_unrendered_code",
        ),
    ] {
        assert_eq!(sc.verdict(other).await, "ready_to_execute", "{why}");
    }

    assert!(legacy::forget_data_test_history_keys(&server).await > 0);
    assert_eq!(
        sc.verdict(cuc("test.p.tpl", -2)).await,
        "ready_to_execute",
        "a run recorded before the key never matches through its template"
    );
}

/// Databricks and DuckDB data tests keep the history of each unique id in
/// its project (uncaptured): the same SQL under another unique id or
/// project builds.
#[tokio::test]
async fn databricks_and_duckdb_data_tests_keep_unique_id_history() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    for dialect in ["databricks", "duckdb"] {
        let test = |uid: &str| {
            data_test(uid)
                .dialect(dialect)
                .catalog("main")
                .schema("prod")
                .sql("select id from main.prod.t where id < 0")
                .namespace("ns")
                .project("p")
                .upstream("main.prod.t", 10)
        };
        let uid = format!("test.p.{dialect}");
        sc.build_with(test(&uid), test_run(0, 5)).await;
        assert_eq!(sc.verdict(test(&uid)).await, "skip_execution", "{dialect}");
        assert_eq!(
            sc.verdict(test(&format!("{uid}_other"))).await,
            "ready_to_execute",
            "{dialect}: another unique id"
        );
        assert_eq!(
            sc.verdict(test(&uid).project("q")).await,
            "ready_to_execute",
            "{dialect}: another project"
        );
    }
}
