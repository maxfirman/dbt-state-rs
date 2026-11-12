//! Databricks physical checks run offline; cloud acceptance is explicitly opt-in.
mod support;

#[test]
fn databricks_baseline_oracle_rejects_incorrect_physical_results() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_baseline_test.py"
        ))
        .output()
        .expect("run offline physical-oracle fault probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt 2.0.11 and Databricks credentials; writes run-owned demo schemas"]
async fn real_dbt_table_and_view_match_independent_reference() {
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("databricks-baseline-{}", uuid::Uuid::new_v4()))
        });
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML installed"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_baseline.py"
        ))
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt executable"))
        .arg("--proxy")
        .arg(env!("CARGO_BIN_EXE_record-proxy"))
        .arg("--probe")
        .arg(env!("CARGO_BIN_EXE_state-probe"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(&artifacts);
    if let Some(lease) = std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE") {
        command.arg("--schema-lease").arg(lease);
    }
    if let Some(identity) = std::env::var_os("DBT_STATE_DATABRICKS_QUERY_IDENTITY") {
        command.arg("--query-identity").arg(identity);
    }
    let output = command
        .output()
        .await
        .expect("run live Databricks baseline");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
