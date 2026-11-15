//! Credential-free preparation of the complete APPEND history result oracle.
//! Live acceptance requires an exact fresh-container allocation and clearance.
#[test]
fn append_history_oracle_rejects_replacement_and_deletion_mistakes() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_append_test.py"
        ))
        .output()
        .expect("run credential-free APPEND history fault probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn append_transitions_reject_incompatible_results_and_extra_appends() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_append_transitions_test.py"
        ))
        .output()
        .expect("run independent transition faults");
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
#[ignore = "requires exact #146 live clearance, pinned dbt and fresh Databricks pair"]
async fn real_dbt_append_matches_independent_operation_history() {
    native_append(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt and exact fresh Databricks transition names"]
async fn real_dbt_append_transitions_match_operation_history() {
    native_append(true).await;
}

async fn native_append(transitions: bool) {
    use sha2::{Digest, Sha256};

    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained APPEND artifact directory");
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
        .arg(if transitions {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_append_transitions.py"
            )
        } else {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_append.py"
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
    if transitions {
        command
            .arg("--schema-lease")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("exact lease"))
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
        .expect("run exact cleared APPEND phase");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
