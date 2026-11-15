//! Credential-free preparation of the complete timestamp-ignore snapshot history result oracle.
//! Live acceptance requires an exact fresh-container allocation and clearance.
#[test]
fn snapshot_oracle_rejects_inconsistent_scd_histories() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_snapshot_test.py"
        ))
        .output()
        .expect("run credential-free timestamp-ignore snapshot history fault probes");
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
#[ignore = "requires exact #151 live clearance, pinned dbt and fresh Databricks pair"]
async fn real_dbt_snapshots_match_independent_operation_history() {
    use sha2::{Digest, Sha256};
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained timestamp-ignore snapshot artifact directory");
    let executable = std::env::current_exe().expect("running acceptance executable");
    std::fs::create_dir_all(&artifacts).expect("create receipt directory");
    std::fs::write(
        artifacts.join("running-server-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "pid": std::process::id(),
            "executable": executable,
            "executable_sha256": format!("{:x}", Sha256::digest(std::fs::read(&executable).expect("read running executable"))),
            "argv": std::env::args().collect::<Vec<_>>(),
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
            "/../../experiments/databricks_snapshots.py"
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
                .expect("frozen independent snapshot ledger"),
        )
        .arg("--run-id")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID").expect("exact cleared Rust run ID"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(artifacts.join("campaign"));
    let child_argv = std::iter::once(
        command
            .as_std()
            .get_program()
            .to_string_lossy()
            .into_owned(),
    )
    .chain(
        command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned()),
    )
    .collect::<Vec<_>>();
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = command
        .spawn()
        .expect("spawn exact cleared timestamp-ignore snapshot phase");
    std::fs::write(
        artifacts.join("native-child-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"pid": child.id(), "argv": child_argv}))
            .unwrap(),
    )
    .expect("retain actual native child PID and argv");
    let output = child
        .wait_with_output()
        .await
        .expect("drain exact snapshot child");
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
