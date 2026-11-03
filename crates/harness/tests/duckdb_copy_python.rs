mod support;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_table_copy_and_adoption() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-copy-{}", uuid::Uuid::new_v4()));
    let result = tokio::process::Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../integrations/duckdb/tests/table_copy.py"
        ))
        .arg(&temp)
        .env("DBT_STATE_LOCAL", "1")
        .env("RUN_CACHE_API_URL", addr.to_string())
        .env("RUN_CACHE_API_SECURE", "false")
        .env("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
        .env("RUN_CACHE_HOME", temp.join("auth"))
        .env("CI", "true")
        .env(
            "PYTHONPATH",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../integrations/duckdb/src"),
        )
        .output()
        .await
        .expect("execute real dbt");
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
