//! Scoped DuckDB cached outcomes at the production gRPC seam.
mod support;
use dbt_state_proto::query_cache as qc;
use support::scenario::{data_test, outcome, test_results, Scenario};

#[tokio::test]
async fn duckdb_cached_tests_require_complete_scoped_confirmed_evidence() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    let one = sc.org("one");
    let baseline = data_test("test.same_name.positive")
        .dialect("duckdb")
        .catalog("warehouse")
        .schema("dev")
        .sql("select count(*) as failures from warehouse.dev.numbers where amount < 0")
        .namespace("file-generation")
        .upstream("warehouse.dev.numbers", 10)
        .project("local-project-root-a");
    // Pending SQL errors / interrupted execution never become cached passes.
    for payload in [None, Some(qc::Struct::default()), Some(test_results(0))] {
        let build = one.execute_id(baseline.clone()).await;
        assert_eq!(one.verdict(baseline.clone()).await, "ready_to_execute");
        let confirmed = match payload.clone() {
            Some(results) => outcome().results(results),
            None => outcome(),
        };
        assert!(one.confirm(&build, confirmed).await);
        if payload == Some(test_results(0)) {
            // A complete pass reuses.
            let skip = one.expect_skip(baseline.clone()).await;
            assert_eq!(skip.execution_results, payload);
        } else {
            assert_eq!(one.verdict(baseline.clone()).await, "ready_to_execute");
        }
    }
    let variants = [
        baseline.clone().project("local-project-root-b"),
        baseline.clone().namespace("other-file-generation"),
        baseline.clone().unique_id("test.same_name.other"),
        baseline.clone().table("warehouse.dev.numbers", None),
        baseline.clone().upstream("warehouse.dev.numbers", 11),
        baseline.clone().with(|req| {
            req.tables.push(qc::TableModifiedInfo {
                name: "warehouse.dev.numbers".into(),
                last_modified_epoch: Some(11),
            })
        }),
        baseline.clone().no_tables(),
    ];
    for changed in variants {
        assert_eq!(one.verdict(changed).await, "ready_to_execute");
    }
    assert_eq!(
        sc.org("two").verdict(baseline.clone()).await,
        "ready_to_execute"
    );
    assert_eq!(one.verdict(baseline).await, "skip_execution");
}
