//! Credential-free preparation of the complete bounded event-time replacement history result oracle.
//! Live acceptance requires an exact fresh-container allocation and clearance.
#[test]
fn microbatch_history_oracle_rejects_replacement_and_deletion_mistakes() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_microbatch_test.py"
        ))
        .output()
        .expect("run credential-free bounded event-time replacement history fault probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn moving_window_oracle_detects_lost_retention_lookback_and_predicate_rows() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_moving_windows_test.py"
        ))
        .output()
        .expect("run independent moving-window faults");
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
#[ignore = "requires exact #150 live clearance, pinned dbt and fresh Databricks pair"]
async fn real_dbt_microbatch_matches_independent_operation_history() {
    native_microbatch(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt and exact fresh Databricks real-clock window names"]
async fn real_dbt_moving_windows_match_independent_operation_history() {
    native_microbatch(true).await;
}

async fn native_microbatch(moving: bool) {
    use sha2::{Digest, Sha256};
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained bounded event-time replacement artifact directory");
    let executable = std::env::current_exe().expect("running acceptance executable");
    std::fs::create_dir_all(&artifacts).expect("create receipt directory");
    std::fs::write(
        artifacts.join("running-server-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "pid": std::process::id(),
            "executable": executable,
            "executable_sha256": format!("{:x}", Sha256::digest(std::fs::read(&executable).unwrap())),
            "address": server.addr.to_string(),
        }))
        .unwrap(),
    )
    .expect("persist server process identity before warehouse use");
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("persistent Python with PyYAML"),
    );
    command
        .arg(if moving {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_moving_windows.py"
            )
        } else {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_microbatch.py"
            )
        })
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROXY").expect("immutable proxy"))
        .arg("--probe")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROBE").expect("immutable probe"))
        .arg("--run-id")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID").expect("exact cleared Rust run ID"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(artifacts.join("campaign"));
    if moving {
        for (flag, variable) in [
            ("--schema-lease", "DBT_STATE_DATABRICKS_SCHEMA_LEASE"),
            ("--schema", "DBT_STATE_DATABRICKS_SCHEMA"),
            ("--object-id", "DBT_STATE_DATABRICKS_OBJECT_ID"),
            ("--utc-day", "DBT_STATE_DATABRICKS_UTC_DAY"),
        ] {
            command
                .arg(flag)
                .arg(std::env::var_os(variable).expect(variable));
        }
        if let Some(prefix) = std::env::var_os("DBT_STATE_DATABRICKS_NATIVE_PREFIX") {
            command.arg("--native-prefix").arg(prefix);
        }
    } else {
        command
            .arg("--live-clearance")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_LIVE_CLEARANCE").expect("exact clearance"))
            .arg("--operation-ledger")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_OPERATION_LEDGER").expect("frozen ledger"));
    }
    let output = command
        .output()
        .await
        .expect("run exact cleared bounded event-time replacement phase");
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
