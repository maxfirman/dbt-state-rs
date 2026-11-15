//! Bounded MERGE history and schema-recovery oracles plus opt-in native acceptance.
#[test]
fn merge_history_oracle_rejects_replacement_and_deletion_mistakes() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_merge_test.py"
        ))
        .output()
        .expect("run credential-free MERGE history fault probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn merge_recovery_oracle_preserves_failed_and_cumulative_results() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_merge_recovery_test.py"
        ))
        .output()
        .expect("run MERGE recovery fault probes");
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
#[ignore = "requires exact #145 live clearance, pinned dbt and fresh Databricks pair"]
async fn real_dbt_merge_matches_independent_operation_history() {
    native_merge(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt and exact fresh Databricks schema-recovery names"]
async fn real_dbt_merge_schema_recovery_matches_operation_history() {
    native_merge(true).await;
}

async fn native_merge(recovery: bool) {
    use sha2::{Digest, Sha256};

    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained MERGE artifact directory");
    let executable = std::env::current_exe().expect("running acceptance executable");
    let executable_sha256 = format!("{:x}", Sha256::digest(std::fs::read(&executable).unwrap()));
    std::fs::create_dir_all(&artifacts).expect("create receipt directory");
    std::fs::write(
        artifacts.join("running-server-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "pid": std::process::id(),
            "executable": executable,
            "executable_sha256": executable_sha256,
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
                "/../../experiments/databricks_merge_recovery.py"
            )
        } else {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_merge.py"
            )
        })
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROXY").expect("immutable proxy"))
        .arg("--probe")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROBE").expect("immutable probe"))
        .arg("--schema-lease")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("exact lease"))
        .arg("--run-id")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID").expect("exact cleared Rust run ID"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(artifacts.join("campaign"));
    if recovery {
        command
            .arg("--schema")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA").expect("owned schema"))
            .arg("--object-id")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_OBJECT_ID").expect("fresh UUID"));
    } else {
        command.arg("--live-clearance").arg(
            std::env::var_os("DBT_STATE_DATABRICKS_LIVE_CLEARANCE").expect("exact live clearance"),
        );
    }
    let output = command
        .output()
        .await
        .expect("run exact cleared MERGE phase");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
