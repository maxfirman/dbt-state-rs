//! SQL equivalence acceptance at the public router and pinned-client seams.
mod support;

use dbt_state_proto::query_cache as qc;
use support::legacy::{self, Rows};
use support::scenario::{built_at, Scenario};

#[tokio::test]
async fn pre_sql_equivalence_confirmed_and_pending_history_cold_misses() {
    // Real immutable old binaries; never recompute old hashes with today's tag.
    // V2 is #159's candidate with exact dirty-source/build provenance. Its two
    // unrelated known diagnostic gaps are retained in the fixture metadata.
    for (bytes, expected_hash) in [
        (
            include_str!("../../../golden/upgrade/databricks_identifiers_v1_aa89b6a.json"),
            "9efd366c92c8df6ae3813369c74cedcee4f1eeb0052d8dde1312342c231bbb2f",
        ),
        (
            include_str!("../../../golden/upgrade/databricks_identifiers_v2_candidate159.json"),
            "0a5635a59ea486faf70f05cec50ba2c8e9c30388fb2c6cd47c1e2564ae1de2a8",
        ),
    ] {
        let frozen: serde_json::Value = serde_json::from_str(bytes).unwrap();
        let request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(frozen["request"].clone()).unwrap();
        let old_hash = frozen["rows"][0]["node_sql_hash"].as_str().unwrap();
        assert_eq!(old_hash, expected_hash);
        if let Some(pending) = frozen["pending_rows"].as_array() {
            assert_eq!(pending[0]["status"], "pending");
            assert_eq!(pending[0]["node_sql_hash"], old_hash);
        }
        for confirmed_before_upgrade in [true, false] {
            let server = support::start_server().await;
            let scenario = Scenario::connect(&server).await;
            let id = scenario.execute_id(request.clone()).await;
            if confirmed_before_upgrade {
                assert!(scenario.confirm(&id, built_at(159)).await);
                scenario.expect_skip(request.clone()).await;
            }
            legacy::rewrite_logic_hashes(&server, Rows::All, Some(old_hash), None).await;
            if !confirmed_before_upgrade {
                assert!(scenario.confirm(&id, built_at(159)).await);
            }
            scenario.expect_execute(request.clone()).await;
        }
    }
}

#[tokio::test]
async fn captured_direct_select_formatting_and_comments_reuse_confirmed_result() {
    // Exact pinned-client payloads from the accepted hosted campaign. This is
    // the compiled counterpart of the independently retained v2 public tracer.
    let rows: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_sql_equivalence.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let submits: Vec<_> = rows
        .iter()
        .filter(|row| row["method"] == "SubmitEnrichedSQL")
        .collect();
    let first = submits[0];
    let old_id = &first["response"]["response"]["ready_to_execute"]["request_id"];
    let epoch = rows
        .iter()
        .find(|row| row["method"] == "ConfirmExecution" && row["request"]["request_id"] == *old_id)
        .unwrap()["request"]["last_modified_epoch"]
        .as_i64()
        .unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let first_request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(first["request"].clone()).unwrap();
    let id = scenario.execute_id(first_request).await;
    assert!(scenario.confirm(&id, built_at(epoch)).await);
    // Unchanged, spacing/newlines, keyword case, leading/inline/trailing
    // comments are precisely the next six actual-client submissions.
    for row in &submits[1..7] {
        assert!(row["response"]["response"]["skip_execution"].is_object());
        let request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(row["request"].clone()).unwrap();
        scenario.expect_skip(request).await;
    }
}

#[test]
fn sql_campaign_requires_complete_literal_results_and_exact_ownership() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_sql_equivalence_test.py"
        ))
        .output()
        .expect("credential-free SQL campaign checks");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt 2.0.11 and live Databricks; writes exact leased demo names"]
async fn real_dbt_sql_equivalences_match_independent_results() {
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained SQL artifact directory");
    // Match the two independent accepted hosted histories: 26 main cells and
    // four fresh CTE controls. Combining them would change the first CTE reason.
    for (phase, identity_variable) in [
        ("main", "DBT_STATE_DATABRICKS_QUERY_IDENTITY"),
        ("cte", "DBT_STATE_DATABRICKS_CTE_QUERY_IDENTITY"),
    ] {
        let server = support::start_server().await;
        let destination = artifacts.join(phase);
        let mut command = tokio::process::Command::new(
            std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
        );
        command
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_sql_equivalence.py"
            ))
            .arg("--dbt")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
            .arg("--proxy")
            .arg(env!("CARGO_BIN_EXE_record-proxy"))
            .arg("--probe")
            .arg(env!("CARGO_BIN_EXE_state-probe"))
            .arg("--schema-lease")
            .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("explicit lease"))
            .arg("--contract")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../experiments/databricks_sql_equivalence_contract.json"
            ))
            .arg("--rust-url")
            .arg(format!("http://{}", server.addr))
            .arg("--work-dir")
            .arg(&destination);
        if phase == "cte" {
            command.arg("--only-cte");
        }
        if let Some(identity) = std::env::var_os(identity_variable) {
            command.arg("--query-identity").arg(identity);
        }
        let output = command.output().await.expect("run SQL acceptance");
        println!("{}", String::from_utf8_lossy(&output.stdout));
        assert!(
            output.status.success(),
            "artifacts: {}\n{}\n{}",
            destination.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

async fn replay_captured_required_decisions(bytes: &str, submit_limit: usize) {
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let mut pending = std::collections::HashMap::new();
    let mut submits = 0;
    for line in bytes.lines() {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        match row["method"].as_str().unwrap() {
            "SubmitEnrichedSQL" => {
                submits += 1;
                if submits > submit_limit {
                    break;
                }
                let request: qc::SubmitEnrichedSqlRequest =
                    serde_json::from_value(row["request"].clone()).unwrap();
                if let Some(build) = row["response"]["response"]["ready_to_execute"].as_object() {
                    let id = scenario.execute_id(request).await;
                    pending.insert(build["request_id"].as_str().unwrap().to_owned(), id);
                } else {
                    assert!(row["response"]["response"]["skip_execution"].is_object());
                    scenario.expect_skip(request).await;
                }
            }
            "ConfirmExecution" => {
                let original = row["request"]["request_id"].as_str().unwrap();
                if let Some(id) = pending.remove(original) {
                    let epoch = row["request"]["last_modified_epoch"].as_i64().unwrap();
                    assert!(scenario.confirm(&id, built_at(epoch)).await);
                }
            }
            _ => {}
        }
    }
    assert!(submits >= submit_limit);
    assert!(pending.is_empty());
}

#[tokio::test]
async fn captured_table_alias_rename_reuses_but_adding_alias_rebuilds() {
    replay_captured_required_decisions(
        include_str!("../../../golden/fixtures/databricks_sql_equivalence.jsonl"),
        9,
    )
    .await;
}

#[tokio::test]
async fn captured_literals_keep_values_spelling_and_output_schema_while_comments_reuse() {
    replay_captured_required_decisions(
        include_str!("../../../golden/fixtures/databricks_sql_equivalence.jsonl"),
        20,
    )
    .await;
}

#[tokio::test]
async fn captured_cte_comment_and_format_controls_reuse() {
    replay_captured_required_decisions(
        include_str!("../../../golden/fixtures/databricks_sql_equivalence_cte.jsonl"),
        4,
    )
    .await;
}

#[tokio::test]
async fn captured_cte_names_definitions_and_nested_bindings_stay_distinct() {
    replay_captured_required_decisions(
        include_str!("../../../golden/fixtures/databricks_sql_equivalence.jsonl"),
        26,
    )
    .await;
}

#[tokio::test]
async fn bounded_comment_alias_and_literal_sequences_preserve_binding_and_schema() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_identifiers_v2_candidate159.json"
    ))
    .unwrap();
    let mut request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["request"].clone()).unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let source = "cdo_demo_db.frozen_sql_137.input";
    request.sql = format!("select id, cast('literal--/* kept */' as string) tag from {source}");
    scenario.build(request.clone(), 159).await;
    for seed in 0..5 {
        let mut equivalent = request.clone();
        equivalent.sql = format!(
            "SELECT /* generated {seed} */ id,\n CAST('literal--/* kept */' AS STRING) tag FROM {source} -- end {seed}"
        );
        scenario.expect_skip(equivalent).await;
    }
    for sql in [
        format!("select id, cast('literal--/* changed */' as string) tag from {source}"),
        format!("select id, cast('literal--/* kept */' as string) renamed_tag from {source}"),
    ] {
        let mut changed = request.clone();
        changed.sql = sql;
        scenario.expect_execute(changed).await;
        scenario.expect_skip(request.clone()).await;
    }
    request.sql = format!("select x.id from {source} as x");
    scenario.build(request.clone(), 159).await;
    // Includes an alias equal to the physical source's last component, proving
    // alpha-renaming never rewrites that separate physical-name token.
    for alias in ["y", "input", "__dbt_state_table_alias", "binding_137"] {
        let mut equivalent = request.clone();
        equivalent.sql = format!("select {alias}.id from {source} {alias}");
        scenario.expect_skip(equivalent).await;
    }
    for sql in [
        format!("select x.changed_column from {source} as x"),
        format!("select other.id from {source} as x"),
        "select x.id from cdo_demo_db.frozen_sql_137.other_input as x".to_string(),
        format!("select id from {source}"),
    ] {
        let mut changed = request.clone();
        changed.sql = sql;
        scenario.expect_execute(changed).await;
        scenario.expect_skip(request.clone()).await;
    }
    request.sql = format!("with stable as (select id from {source}) select id from stable");
    scenario.build(request.clone(), 159).await;
    for seed in 0..3 {
        let mut equivalent = request.clone();
        equivalent.sql = format!(
            "with stable as (SELECT /* cte {seed} */ id FROM {source}) SELECT id FROM stable"
        );
        scenario.expect_skip(equivalent).await;
    }
    for sql in [
        request.sql.replace("stable", "renamed_cte"),
        request
            .sql
            .replace(source, "cdo_demo_db.frozen_sql_137.other_input"),
    ] {
        let mut changed = request.clone();
        changed.sql = sql;
        scenario.expect_execute(changed).await;
        scenario.expect_skip(request.clone()).await;
    }
}

#[tokio::test]
async fn uncharacterized_syntax_and_punctuation_retain_exact_identity() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_identifiers_v2_candidate159.json"
    ))
    .unwrap();
    for sql in [
        "select local_function(id) as id from cdo_demo_db.frozen_sql_137.input",
        "select id from cdo_demo_db.frozen_sql_137.input where id > 0",
        "select /*+ REPARTITION(1) */ id from cdo_demo_db.frozen_sql_137.input",
        "select id from cdo_demo_db.frozen_sql_137.input;",
    ] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let mut request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(frozen["request"].clone()).unwrap();
        request.sql = sql.into();
        scenario.build(request.clone(), 159).await;
        scenario.expect_skip(request.clone()).await;
        request.sql = if let Some(without_semicolon) = sql.strip_suffix(';') {
            without_semicolon.into()
        } else {
            format!("-- uncharacterized formatting\n{sql}")
        };
        scenario.expect_execute(request).await;
    }
}

#[tokio::test]
async fn escaped_literal_spellings_remain_distinct_while_ordinary_comments_reuse() {
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_identifiers_v2_candidate159.json"
    ))
    .unwrap();
    for (original, other) in [
        (r"'it\'s'", "'it''s'"),
        (r"'path\\file'", r"'path\file'"),
        (r"'slash\\and\'quote'", "'slash\\and''quote'"),
    ] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let mut request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(frozen["request"].clone()).unwrap();
        request.sql = format!(
            "select id, cast({original} as string) tag from cdo_demo_db.frozen_sql_137.input"
        );
        scenario.build(request.clone(), 159).await;
        scenario.expect_skip(request.clone()).await;
        let mut commented = request.clone();
        commented.sql = format!("-- ordinary comment\n{}", request.sql);
        scenario.expect_skip(commented).await;
        let mut changed = request.clone();
        changed.sql =
            format!("select id, cast({other} as string) tag from cdo_demo_db.frozen_sql_137.input");
        scenario.expect_execute(changed).await;
        scenario.expect_skip(request).await;
    }
}
