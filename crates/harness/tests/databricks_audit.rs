//! Bounded audit-output/status oracle. Native error-aware counterparts require
//! exact receipts; whole Databricks audit support remains incomplete.
#[test]
fn audit_oracle_rejects_wrong_typed_rows_and_physical_identity() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_audit_test.py"
        ))
        .output()
        .expect("run credential-free audit history fault probes");
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
#[ignore = "requires exact #144 live clearance, pinned dbt and fresh audit names"]
async fn real_dbt_audit_relations_match_independent_recorded_and_fresh_results() {
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained audit artifact directory");
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
            "/../../experiments/databricks_audit.py"
        ))
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROXY").expect("immutable proxy"))
        .arg("--probe")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROBE").expect("immutable probe"))
        .arg("--schema-lease")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("exact lease"))
        .arg("--live-clearance")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_LIVE_CLEARANCE").expect("exact live clearance"))
        .arg("--run-id")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID").expect("exact cleared Rust run ID"))
        .arg("--contract")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_AUDIT_CONTRACT")
                .expect("observed hosted contract"),
        )
        .arg("--rust-replay-clearance")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_AUDIT_RUST_CLEARANCE")
                .expect("separate complete-green-replay clearance"),
        )
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(artifacts.join("campaign"));
    let output = command
        .output()
        .await
        .expect("run exact cleared audit phase");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "artifacts: {}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

use dbt_state_proto::query_cache as qc;

fn audit_histories() -> Vec<Vec<serde_json::Value>> {
    [
        include_str!("../../../golden/fixtures/databricks_audit_aliased_error_history.jsonl"),
        include_str!("../../../golden/fixtures/databricks_audit_default_error_history.jsonl"),
    ]
    .into_iter()
    .map(|history| {
        history
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    })
    .collect()
}

fn audit_rejected_request() -> qc::SubmitEnrichedSqlRequest {
    let histories = audit_histories();
    let row = histories[1]
        .iter()
        .find(|row| row["method"] == "SubmitEnrichedSQL" && row["status"]["code"] == 3)
        .unwrap();
    serde_json::from_value(row["request"].clone()).unwrap()
}

#[tokio::test]
async fn ordinary_source_less_audit_errors_match_both_complete_captured_histories() {
    let server = support::start_server().await;
    let channel = support::channel(server.addr).await;
    let mut sql = qc::sql_client::SqlClient::new(channel);
    let mut rejected = 0;
    for history in audit_histories() {
        for row in history
            .iter()
            .filter(|row| row["method"] == "SubmitEnrichedSQL" && row["status"]["code"] == 3)
        {
            let request: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(row["request"].clone()).unwrap();
            let error = sql.submit_enriched_sql(request).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert_eq!(error.message(), "Failed to process SQL");
            rejected += 1;
        }
    }
    assert_eq!(rejected, 5);
}

#[tokio::test]
async fn audit_error_boundary_preserves_genuine_inputs_and_uncharacterized_requests() {
    let original = audit_rejected_request();
    let mut cases = Vec::new();
    for (name, sql) in [
        ("direct", "SELECT id, v FROM cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114a144aa"),
        ("cte", "WITH actual_input AS (SELECT * FROM cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114a144aa) SELECT * FROM actual_input"),
        ("subquery", "SELECT * FROM (SELECT * FROM cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114a144aa) actual_input"),
        ("table-function", "SELECT * FROM range(3)"),
        ("data-table-function", "SELECT * FROM read_files('/Volumes/owned/data')"),
        ("source-less-cte", "WITH literal_row AS (SELECT 1 AS id) SELECT id FROM literal_row"),
        ("quoted-input-cte", "WITH `actual_input` AS (SELECT * FROM cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114a144aa) SELECT * FROM `actual_input`"),
        ("uncharacterized-select", "SELECT DISTINCT 1 AS id"),
        ("unknown-placeholder", "SELECT ? AS id"),
        ("uncharacterized-cast", "SELECT try_cast('1' AS INT) AS id"),
        ("uncharacterized-set", "SELECT 1 AS id INTERSECT SELECT 1 AS id"),
        ("scalar-function", "SELECT abs(-1) AS id, 2 AS v"),
        ("unknown-sql", "not a parsed query"),
        ("ambiguous-cte", "WITH first AS (SELECT * FROM later), later AS (SELECT 1 AS id) SELECT * FROM first"),
    ] {
        let mut request = original.clone();
        request.sql = sql.into();
        cases.push((name, request));
    }
    for dialect in ["snowflake", "duckdb", "bigquery", "postgres"] {
        let mut request = original.clone();
        request.dialect = dialect.into();
        cases.push((dialect, request));
    }
    for name in [
        "false-store",
        "missing-store",
        "false-compare",
        "missing-input",
        "multiple-inputs",
        "unknown-epoch",
        "zero-epoch",
        "negative-epoch",
        "outside-catalog",
        "outside-schema",
        "non-audit-schema",
        "missing-schema",
        "unknown-name",
        "quoted-dot",
        "query-dependency",
        "audit-view",
        "explicit-target",
    ] {
        let mut request = original.clone();
        match name {
            "false-store" => {
                request
                    .semantic_extras
                    .insert("store_failures".into(), "false".into());
            }
            "missing-store" => {
                request.semantic_extras.remove("store_failures");
            }
            "false-compare" => request.compare_unrendered_code = false,
            "missing-input" => request.tables.clear(),
            "multiple-inputs" => request.tables.push(request.tables[0].clone()),
            "unknown-epoch" => request.tables[0].last_modified_epoch = None,
            "zero-epoch" => request.tables[0].last_modified_epoch = Some(0),
            "negative-epoch" => request.tables[0].last_modified_epoch = Some(-1),
            "outside-catalog" => {
                request.tables[0].name =
                    "other.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114a144aa".into()
            }
            "outside-schema" => {
                request.tables[0].name =
                    "cdo_demo_db.other_dbt_test__audit.sf_20261114a144aa".into()
            }
            "non-audit-schema" => {
                request.default_schema = Some("ordinary".into());
                request.tables[0].name = "cdo_demo_db.ordinary.sf_20261114a144aa".into();
            }
            "missing-schema" => request.default_schema = None,
            "unknown-name" => request.tables[0].name = "sf_20261114a144aa".into(),
            "quoted-dot" => {
                request.tables[0].name =
                    "cdo_demo_db.`ds_20261112a134ef_ref_dbt_test__audit.other`.sf_20261114a144aa"
                        .into()
            }
            "query-dependency" => request.query_dependencies.push(qc::QueryDependency {
                name: "cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.actual_view".into(),
                query: "SELECT 1 AS id".into(),
                default_catalog: "cdo_demo_db".into(),
                default_schema: "ds_20261112a134ef_ref_dbt_test__audit".into(),
            }),
            "audit-view" => {
                request
                    .semantic_extras
                    .insert("store_failures_as".into(), "\"view\"".into());
            }
            "explicit-target" => request.target_table = Some(request.tables[0].name.clone()),
            _ => unreachable!(),
        }
        cases.push((name, request));
    }
    let server = support::start_server().await;
    let channel = support::channel(server.addr).await;
    let mut sql = qc::sql_client::SqlClient::new(channel);
    for (name, request) in cases {
        let response = sql.submit_enriched_sql(request).await;
        assert!(response.is_ok(), "{name}: {response:?}");
    }
}
