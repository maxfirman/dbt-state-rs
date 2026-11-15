//! Credential-free preparation of the complete partition replacement history result oracle.
//! Live acceptance requires an exact fresh-container allocation and clearance.
#[test]
fn insert_overwrite_history_oracle_rejects_replacement_and_deletion_mistakes() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_insert_overwrite_test.py"
        ))
        .output()
        .expect("run credential-free partition replacement history fault probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn composite_overwrite_rejects_wrong_partition_bags_and_schema() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_overwrite_recovery_test.py"
        ))
        .output()
        .expect("run independent composite partition faults");
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
#[ignore = "requires exact #147 live clearance, pinned dbt and fresh Databricks pair"]
async fn real_dbt_insert_overwrite_matches_independent_operation_history() {
    native_overwrite(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt and exact fresh Databricks composite partition names"]
async fn real_dbt_composite_overwrite_matches_operation_history() {
    native_overwrite(true).await;
}

async fn native_overwrite(recovery: bool) {
    use sha2::{Digest, Sha256};
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained partition replacement artifact directory");
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
        .arg(if recovery {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_overwrite_recovery.py"
            )
        } else {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_insert_overwrite.py"
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
    if recovery {
        command
            .arg("--schema-lease")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("exact lease"))
            .arg("--schema")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA").expect("owned schema"))
            .arg("--object-id")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_OBJECT_ID").expect("fresh UUID"));
    } else {
        command
            .arg("--live-clearance")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_LIVE_CLEARANCE").expect("exact clearance"));
    }
    let output = command
        .output()
        .await
        .expect("run exact cleared partition replacement phase");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
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
}
