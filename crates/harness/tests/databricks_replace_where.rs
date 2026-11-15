//! Credential-free preparation of the complete predicate replacement history result oracle.
//! Live acceptance requires an exact fresh-container allocation and clearance.
#[test]
fn replace_where_history_oracle_rejects_replacement_and_deletion_mistakes() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_replace_where_test.py"
        ))
        .output()
        .expect("run credential-free predicate replacement history fault probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn predicate_recovery_rejects_wrong_retained_rows_and_schema() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_predicate_recovery_test.py"
        ))
        .output()
        .expect("run independent predicate recovery faults");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn replace_where_capture_keeps_custom_refresh_and_sql_identity_transition() {
    let rows: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_replace_where_history.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    assert_eq!(rows.len(), 90);
    assert!(rows.iter().all(|row| row["status"]["code"] == 0));
    let submits: Vec<_> = rows
        .iter()
        .filter(|row| row["method"] == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 12);
    assert!(submits
        .iter()
        .all(|row| row["request"]["execution_type"] == 11));
    assert_eq!(
        submits
            .iter()
            .filter(|row| row["response"]["response"]["skip_execution"].is_object())
            .count(),
        5
    );
    let records: Vec<_> = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row["method"] == "RecordExecutions")
        .collect();
    assert_eq!(records.len(), 1);
    let (position, record) = records[0];
    assert_eq!(record["response"]["records_stored"], 1);
    let input = &record["request"]["records"][0]["input"]["enriched_sql"];
    assert_eq!(input["execution_type"], 11);
    assert!(!input["sql"]
        .as_str()
        .unwrap()
        .to_lowercase()
        .contains(" where "));
    let next = rows[position + 1..]
        .iter()
        .find(|row| row["method"] == "SubmitEnrichedSQL")
        .unwrap();
    assert_eq!(next["request"]["execution_type"], 11);
    assert!(next["request"]["sql"]
        .as_str()
        .unwrap()
        .contains("where p = DATE '2026-11-01' OR p IS NULL"));
    assert!(next["response"]["response"]["ready_to_execute"].is_object());
}

#[path = "support.rs"]
mod support;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires exact #148 live clearance, pinned dbt and fresh Databricks pair"]
async fn real_dbt_replace_where_matches_independent_operation_history() {
    native_replace(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt and exact fresh Databricks predicate/schema names"]
async fn real_dbt_predicate_recovery_matches_operation_history() {
    native_replace(true).await;
}

async fn native_replace(recovery: bool) {
    use sha2::{Digest, Sha256};
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained predicate replacement artifact directory");
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
                "/../../experiments/databricks_predicate_recovery.py"
            )
        } else {
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_replace_where.py"
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
            .arg(
                std::env::var_os("DBT_STATE_DATABRICKS_LIVE_CLEARANCE")
                    .expect("exact live clearance"),
            )
            .arg("--operation-ledger")
            .arg(
                std::env::var_os("DBT_STATE_DATABRICKS_OPERATION_LEDGER")
                    .expect("frozen independent operation ledger"),
            );
    }
    let output = command
        .output()
        .await
        .expect("run exact cleared predicate replacement phase");
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
