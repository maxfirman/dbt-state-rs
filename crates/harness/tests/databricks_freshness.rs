//! #140 complete client freshness histories and independent physical acceptance.
mod support;

#[test]
fn freshness_oracle_rejects_stale_rows_duplicate_loss_null_and_schema_changes() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_freshness_test.py"
        ))
        .output()
        .expect("run credential-free freshness oracle checks");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn hosted_freshness_captures_retain_all_required_build_reuse_record_and_explain_cells() {
    for (bytes, builds, reuses, explains, records) in [
        (
            include_str!("../../../golden/fixtures/databricks_freshness_client.jsonl"),
            8,
            10,
            19,
            0,
        ),
        (
            include_str!("../../../golden/fixtures/databricks_freshness_recorded.jsonl"),
            3,
            4,
            5,
            1,
        ),
    ] {
        let rows: Vec<serde_json::Value> = bytes
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let ordinary = rows
            .iter()
            .filter(|r| r["method"] == "SubmitEnrichedSQL")
            .collect::<Vec<_>>();
        assert_eq!(
            ordinary
                .iter()
                .filter(|r| r["response"]["response"].get("ready_to_execute").is_some())
                .count(),
            builds
        );
        assert_eq!(
            ordinary
                .iter()
                .filter(|r| r["response"]["response"].get("skip_execution").is_some())
                .count(),
            reuses
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r["method"] == "ConfirmExecution")
                .count(),
            builds
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r["method"] == "GetExplainMessages")
                .count(),
            explains
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r["method"] == "RecordExecutions")
                .count(),
            records
        );
        assert!(ordinary
            .iter()
            .all(|r| r["request"]["freshness_tolerance_seconds"] == 0));
    }
}

#[test]
fn crafted_epoch_fixture_covers_equal_older_one_ms_and_delayed_known_evidence() {
    let rows: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_freshness_epoch_boundaries.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    assert_eq!(rows.len(), 18);
    assert_eq!(
        rows.iter()
            .filter(|r| r["method"] == "ConfirmExecution")
            .count(),
        4
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r["method"] == "GetExplainMessages")
            .count(),
        5
    );
    let submits = rows
        .iter()
        .filter(|r| r["method"] == "SubmitEnrichedSQL")
        .collect::<Vec<_>>();
    assert_eq!(submits.len(), 9);
    assert_eq!(
        submits
            .iter()
            .filter(|r| r["response"]["response"].get("skip_execution").is_some())
            .count(),
        3
    );
    assert!(submits
        .iter()
        .all(|r| r["request"]["dbt_node_state"]["project_name"]
            .as_str()
            .unwrap()
            .starts_with("boundary_")));
    let cells = submits
        .iter()
        .map(|r| r["request"]["labels"]["dbt_node_name"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        cells,
        std::collections::BTreeSet::from(["equal", "older", "plus_one_ms", "delayed_old_then_new"])
    );
}

async fn run_live(entrypoint: &str) {
    let server = support::start_server().await;
    let artifacts = std::path::PathBuf::from(
        std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh retained artifact path"),
    );
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../experiments")
                .join(entrypoint),
        )
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(env!("CARGO_BIN_EXE_record-proxy"))
        .arg("--probe")
        .arg(env!("CARGO_BIN_EXE_state-probe"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(&artifacts)
        .arg("--schema-lease")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("owned schema lease"));
    if let Some(identity) = std::env::var_os("DBT_STATE_DATABRICKS_QUERY_IDENTITY") {
        command.arg("--query-identity").arg(identity);
    }
    let output = command.output().await.expect("run live freshness campaign");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned client and Databricks credentials; writes leased run-owned demo relations"]
async fn real_dbt_source_changes_match_independent_reference() {
    run_live("databricks_freshness.py").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned client and Databricks credentials; writes leased run-owned demo relations"]
async fn real_dbt_same_logic_recorded_rebuild_matches_independent_reference() {
    run_live("databricks_recorded_freshness.py").await;
}
