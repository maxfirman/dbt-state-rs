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
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-data-tests-{}", uuid::Uuid::new_v4()));
    let mut command = tokio::process::Command::new(python);
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../integrations/duckdb/tests/data_tests.py"
        ))
        .arg(&temp);
    if let Some(scenario) = scenario {
        command.arg(scenario);
    }
    let result = command
        .env("DBT_STATE_LOCAL", "1")
        .env("RUN_CACHE_API_URL", addr.to_string())
        .env("RUN_CACHE_API_SECURE", "false")
        .env("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        .env("RUN_CACHE_HOME", temp.join("auth"))
        .env("CI", "true")
        .output()
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(state.store.pool())
        .await
        .unwrap();
    std::fs::remove_dir_all(temp).unwrap();
}
