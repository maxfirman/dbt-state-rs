//! The real Python/dbt/warehouse compatibility gate. Install integrations/duckdb first.
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment; run scripts/duckdb-smoke.sh"]
async fn real_dbt_durable_table_reuse_and_external_changes() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-smoke-{}", uuid::Uuid::new_v4()));
    for (name, mode) in [
        ("first", "normal"),
        ("second", "normal"),
        ("first", "skip"),
        ("first", "rewrite"),
        ("first", "input"),
        ("first", "skip"),
        ("first", "target"),
        ("first", "target-schema"),
        ("first", "drop"),
        ("first", "schema"),
        ("first", "sql"),
        ("first", "identical"),
        ("first", "skip"),
        ("first", "transport"),
        ("first", "rebuild"),
        ("first", "skip"),
        ("scalar", "normal"),
        ("scalar", "skip"),
        ("scalar", "target"),
        ("scalar", "input"),
        ("scalar", "skip"),
        ("hooks", "normal"),
        ("hooks", "skip"),
        ("first", "unknown"),
        ("first", "unsupported"),
    ] {
        let result = tokio::process::Command::new(&python)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../integrations/duckdb/tests/smoke.py"
            ))
            .arg(temp.join(name))
            .arg(mode)
            .env("DBT_STATE_LOCAL", "1")
            .env("RUN_CACHE_API_URL", addr.to_string())
            .env("RUN_CACHE_API_SECURE", "false")
            .env("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
            .env("RUN_CACHE_HOME", temp.join("auth"))
            .env("CI", "true")
            .output()
            .await
            .expect("execute real dbt");
        assert!(
            result.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let observations = |name: &str| -> Vec<serde_json::Value> {
        std::fs::read_to_string(temp.join(name).join("observations.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    };
    let first = observations("first");
    let second = observations("second");
    assert_ne!(
        first[0]["namespace"], second[0]["namespace"],
        "independent files isolate history"
    );
    for event in &first[..first.len() - 1] {
        assert_eq!(
            event["namespace"], first[0]["namespace"],
            "normal restarts preserve the generation"
        );
    }
    assert_ne!(
        first.last().unwrap()["namespace"],
        first[0]["namespace"],
        "incompatible metadata must not match valid history"
    );
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(state.store.pool())
        .await
        .unwrap();
    std::fs::remove_dir_all(temp).unwrap();
}
