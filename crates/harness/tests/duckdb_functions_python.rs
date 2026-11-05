//! Real dbt seam: changed function bindings cannot certify stale results.
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_function_bindings_prevent_stale_reuse() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-functions-{}", uuid::Uuid::new_v4()));
    let result = tokio::process::Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../integrations/duckdb/tests/functions.py"
        ))
        .arg(&temp)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_hook_changes_and_pre_hook_bindings_rebuild() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-hooks-{}", uuid::Uuid::new_v4()));
    let result = tokio::process::Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../integrations/duckdb/tests/hooks.py"
        ))
        .arg(&temp)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_hidden_rowid_references_rebuild_after_reordering() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-rowid-{}", uuid::Uuid::new_v4()));
    let result = tokio::process::Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../integrations/duckdb/tests/rowid.py"
        ))
        .arg(&temp)
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
        "retained project: {}\n{}\n{}",
        temp.display(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(state.store.pool())
        .await
        .unwrap();
    std::fs::remove_dir_all(temp).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_named_type_redefinitions_rebuild() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-types-{}", uuid::Uuid::new_v4()));
    let result = tokio::process::Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../integrations/duckdb/tests/semantics.py"
        ))
        .arg(&temp)
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
        "retained project: {}\n{}\n{}",
        temp.display(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(state.store.pool())
        .await
        .unwrap();
    std::fs::remove_dir_all(temp).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_permissive_scalar_subqueries_rebuild_after_reordering() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-scalar-{}", uuid::Uuid::new_v4()));
    let result = tokio::process::Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../integrations/duckdb/tests/semantics.py"
        ))
        .arg(&temp)
        .arg("scalar")
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
        "retained project: {}\n{}\n{}",
        temp.display(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(state.store.pool())
        .await
        .unwrap();
    std::fs::remove_dir_all(temp).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment"]
async fn real_dbt_worker_collation_changes_rebuild() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let (addr, schema, state) = support::start_server_with_state().await;
    let temp = std::env::temp_dir().join(format!("duckdb-collation-{}", uuid::Uuid::new_v4()));
    let result = tokio::process::Command::new(python)
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../integrations/duckdb/tests/semantics.py"
        ))
        .arg(&temp)
        .arg("collation")
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
        "retained project: {}\n{}\n{}",
        temp.display(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    sqlx::query(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
        .execute(state.store.pool())
        .await
        .unwrap();
    std::fs::remove_dir_all(temp).unwrap();
}
