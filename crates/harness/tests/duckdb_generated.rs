//! Physical state generation over real DuckDB and the normal server binary.
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned Python environment and DBT_STATE_SERVER_BIN"]
async fn generated_physical_state_and_oracle_mutations() {
    let python = std::env::var("DBT_STATE_DUCKDB_PYTHON").expect("pinned Python executable");
    let binary = std::env::var("DBT_STATE_SERVER_BIN").expect("normal production server binary");
    let database = support::TestDatabase::new("generated").await;
    let dsn = database.scoped_dsn();
    let artifacts = std::env::var_os("DBT_STATE_GENERATED_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("duckdb-generated-{}", uuid::Uuid::new_v4()))
        });
    let mut command = tokio::process::Command::new(python);
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../integrations/duckdb/tests/generated/runner.py"
        ))
        .arg("--artifacts")
        .arg(&artifacts)
        .arg("--seeds")
        .arg(std::env::var("DBT_STATE_GENERATED_SEEDS").unwrap_or_else(|_| "0,1,2,3".into()))
        .arg("--steps")
        .arg(std::env::var("DBT_STATE_GENERATED_STEPS").unwrap_or_else(|_| "4".into()))
        .arg("--mutation-checks")
        .env("DATABASE_URL", dsn.as_str())
        .env("DBT_STATE_SERVER_BIN", binary)
        .env("RUST_LOG", "warn")
        .env(
            "PYTHONPATH",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../../integrations/duckdb/src"),
        );
    if let Some(replay) = std::env::var_os("DBT_STATE_GENERATED_REPLAY") {
        command.arg("--replay").arg(replay);
    }
    if std::env::var_os("DBT_STATE_GENERATED_SHRINK").is_some() {
        command.arg("--shrink");
    }
    let result = command.output().await;
    // Python stops its production binary before returning, including failures.
    let result = result.expect("execute generated runner");
    println!("{}", String::from_utf8_lossy(&result.stdout));
    assert!(
        result.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}
