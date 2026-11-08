//! Real dbt seam: complete data-test outcomes retain dbt failure and warning behavior.
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_complete_cached_data_test_outcomes() {
    run_scenario(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_stored_failures_use_ordinary_execution() {
    run_scenario(Some("store-failures")).await;
}

async fn run_scenario(scenario: Option<&str>) {
    let server = support::start_server().await;
    let project = server.python_project("duckdb-data-tests");
    project
        .run("data_tests.py", &scenario.into_iter().collect::<Vec<_>>())
        .await;
}
