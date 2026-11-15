//! Credential-free preparation of the complete null-safe key replacement history result oracle.
//! Live acceptance requires an exact fresh-container allocation and clearance.
#[test]
fn delete_insert_history_oracle_rejects_replacement_and_deletion_mistakes() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_delete_insert_test.py"
        ))
        .output()
        .expect("run credential-free null-safe key replacement history fault probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[path = "support.rs"]
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires exact #149 live clearance, pinned dbt and fresh Databricks pair"]
async fn real_dbt_delete_insert_matches_independent_operation_history() {
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained null-safe key replacement artifact directory");
    let executable = std::env::current_exe().expect("running acceptance executable");
    std::fs::create_dir_all(&artifacts).expect("create receipt directory");
    std::fs::write(
        artifacts.join("running-server-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "pid": std::process::id(),
            "executable": executable,
            "address": server.addr.to_string(),
        }))
        .unwrap(),
    )
    .expect("persist server process identity before warehouse use");
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("persistent Python with PyYAML"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_delete_insert.py"
        ))
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROXY").expect("immutable proxy"))
        .arg("--probe")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROBE").expect("immutable probe"))
        .arg("--live-clearance")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_LIVE_CLEARANCE").expect("exact live clearance"))
        .arg("--operation-ledger")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_OPERATION_LEDGER")
                .expect("frozen independent operation ledger"),
        )
        .arg("--run-id")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID").expect("exact cleared Rust run ID"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(artifacts.join("campaign"));
    let output = command
        .output()
        .await
        .expect("run exact cleared null-safe key replacement phase");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    // Postflight evidence for future runs; the proxy capture stays unchanged.
    let captures: Vec<_> = std::fs::read_dir(artifacts.join("campaign/golden"))
        .expect("read drained native capture directory")
        .map(|entry| entry.expect("native capture entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .collect();
    assert_eq!(captures.len(), 1, "one complete native capture");
    support::native_clock::retain(
        &server,
        &captures[0],
        &artifacts.join("native-replay-clocks.json"),
    )
    .await
    .expect("retain exact native audit clocks before State schema cleanup");
    std::fs::write(
        artifacts.join("native-extra-replay-environment.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "DBT_STATE_EXTRA_REPLAY_CAPTURE": captures[0],
            "DBT_STATE_EXTRA_REPLAY_CLOCKS": artifacts.join("native-replay-clocks.json"),
        }))
        .unwrap(),
    )
    .expect("retain actual capture and clock sidecar for native extra replay");
    assert!(
        output.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
