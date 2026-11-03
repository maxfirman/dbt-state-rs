//! The real Python/dbt/warehouse compatibility gate. Install integrations/duckdb first.
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment; run scripts/duckdb-smoke.sh"]
async fn real_dbt_cold_build_and_file_isolation() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-smoke-{}", uuid::Uuid::new_v4()));
    for (name, mode) in [
        ("first", "normal"),
        ("second", "normal"),
        ("first", "normal"),
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
    // Pending executions prove the public service returned BUILD, and unknown
    // freshness evidence must not be promoted into successful confirmations.
    let rows: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT node_unique_id, status, table_namespace, dialect FROM executions WHERE node_unique_id = 'model.duckdb_smoke.totals' ORDER BY id"
    ).fetch_all(state.store.pool()).await.unwrap();
    assert_eq!(rows.len(), 4, "silently disabled State must fail the gate");
    assert!(rows.iter().all(|r| r.1 == "pending" && r.3 == "duckdb"));
    assert!(rows
        .iter()
        .all(|r| r.2.as_ref().is_some_and(|v| !v.is_empty())));
    assert_ne!(
        rows[0].2, rows[1].2,
        "distinct files need distinct namespaces"
    );
    assert_eq!(
        rows[0].2, rows[2].2,
        "normal restarts preserve file generation"
    );
    assert_ne!(
        rows[0].2, rows[3].2,
        "unknown metadata cannot match valid history"
    );
    let confirmed: i64 = sqlx::query_scalar("SELECT count(*) FROM executions WHERE status = 'confirmed' AND node_unique_id IN ('model.duckdb_smoke.totals', 'seed.duckdb_smoke.numbers')").fetch_one(state.store.pool()).await.unwrap();
    assert_eq!(
        confirmed, 0,
        "unknown epochs must never confirm model or seed outcomes"
    );
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(state.store.pool())
        .await
        .unwrap();
    std::fs::remove_dir_all(temp).unwrap();
}
