//! Captured Databricks dependency/template/configuration identity, public gRPC.
mod support;

use dbt_state_proto::query_cache as qc;
use support::scenario::{built_at, Scenario};

fn template_capture() -> Vec<serde_json::Value> {
    include_str!("../../../golden/fixtures/databricks_template_config.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn actual_template_comment_and_projection_reuse_latest_confirmed_body() {
    let rows = template_capture();
    let submits: Vec<_> = rows
        .iter()
        .filter(|r| r["method"] == "SubmitEnrichedSQL")
        .collect();
    let first_id = &submits[0]["response"]["response"]["ready_to_execute"]["request_id"];
    let confirm = rows
        .iter()
        .find(|r| r["method"] == "ConfirmExecution" && r["request"]["request_id"] == *first_id)
        .unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submits[0]["request"].clone()).unwrap();
    assert!(request.compare_unrendered_code);
    let id = scenario.execute_id(request).await;
    assert!(
        scenario
            .confirm(
                &id,
                built_at(confirm["request"]["last_modified_epoch"].as_i64().unwrap())
            )
            .await
    );
    // Actual unchanged, Jinja comment edit and --vars projection change. The
    // latter restores the body's latest confirmed bytes after the comment SKIP.
    for row in &submits[1..4] {
        assert!(row["response"]["response"]["skip_execution"].is_object());
        let request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(row["request"].clone()).unwrap();
        scenario.expect_skip(request).await;
    }
}

fn dependency_capture() -> Vec<serde_json::Value> {
    include_str!("../../../golden/fixtures/databricks_dependency_identity.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

async fn build_first(scenario: &Scenario, rows: &[serde_json::Value]) {
    let first = rows
        .iter()
        .find(|r| r["method"] == "SubmitEnrichedSQL")
        .unwrap();
    let hosted_id = &first["response"]["response"]["ready_to_execute"]["request_id"];
    let confirm = rows
        .iter()
        .find(|r| r["method"] == "ConfirmExecution" && r["request"]["request_id"] == *hosted_id)
        .unwrap();
    let request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(first["request"].clone()).unwrap();
    let id = scenario.execute_id(request).await;
    assert!(
        scenario
            .confirm(
                &id,
                built_at(confirm["request"]["last_modified_epoch"].as_i64().unwrap())
            )
            .await
    );
}

#[tokio::test]
async fn identical_dependency_duplicates_are_redundant_but_swaps_are_not() {
    let rows = dependency_capture();
    let submits: Vec<_> = rows
        .iter()
        .filter(|r| r["method"] == "SubmitEnrichedSQL")
        .collect();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    build_first(&scenario, &rows).await;
    // Actual unchanged and crafted order, swapped definitions, then duplicate.
    for (index, reuse) in [(1, true), (2, true), (3, false), (10, true)] {
        let row = submits[index];
        let req: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(row["request"].clone()).unwrap();
        if reuse {
            scenario.expect_skip(req).await;
        } else {
            scenario.expect_execute(req).await;
        }
    }
}

#[tokio::test]
async fn missing_template_state_uses_only_rendered_identity() {
    let rows = dependency_capture();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    build_first(&scenario, &rows).await;
    let missing = rows
        .iter()
        .find(|r| r["method"] == "SubmitEnrichedSQL" && r["request"]["dbt_node_state"].is_null())
        .unwrap();
    let req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(missing["request"].clone()).unwrap();
    scenario.expect_skip(req.clone()).await;
    let mut changed = req.clone();
    changed.sql.push_str(" UNION ALL SELECT 9, 9, 'other'");
    scenario.expect_execute(changed).await;
    scenario.expect_skip(req).await;
}

#[tokio::test]
async fn record_preserves_actual_template_but_false_submit_does_not_create_one() {
    let rows: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_template_transitions.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let record = rows
        .iter()
        .find(|r| r["method"] == "RecordExecutions")
        .unwrap();
    let request: qc::RecordExecutionsRequest =
        serde_json::from_value(record["request"].clone()).unwrap();
    let after: Vec<_> = rows
        .iter()
        .skip_while(|r| r["method"] != "RecordExecutions")
        .filter(|r| r["method"] == "SubmitEnrichedSQL")
        .take(2)
        .collect();
    assert_eq!(after.len(), 2);
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    assert_eq!(scenario.record(request.records).await.unwrap(), 1);
    for row in after {
        assert!(row["response"]["response"]["skip_execution"].is_object());
        let req: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(row["request"].clone()).unwrap();
        scenario.expect_skip(req).await;
    }
}

#[tokio::test]
async fn declared_config_and_runtime_extras_are_separate_exact_alternatives() {
    let rows = dependency_capture();
    let unchanged = rows
        .iter()
        .filter(|r| r["method"] == "SubmitEnrichedSQL")
        .nth(1)
        .unwrap();
    let original: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(unchanged["request"].clone()).unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    build_first(&scenario, &rows).await;
    // Hosted one-axis controls: neither change builds; together they build.
    for (compare, extras_change, config_change, reuse) in [
        (true, false, false, true),
        (true, true, false, true),
        (true, false, true, true),
        (true, true, true, false),
        (false, true, false, false),
        (false, false, true, true),
    ] {
        let mut req = original.clone();
        req.compare_unrendered_code = compare;
        if extras_change {
            req.semantic_extras
                .insert("on_schema_change".into(), "\"fail\"".into());
        }
        if config_change {
            req.dbt_node_state.as_mut().unwrap().node_configs_hash =
                Some("d523caa05d27c4bfae39b9823ab35526".into());
        }
        if reuse {
            scenario.expect_skip(req).await;
        } else {
            scenario.expect_execute(req).await;
        }
        scenario.expect_skip(original.clone()).await;
    }
    // Unknown config evidence cannot supply a weaker template alternative.
    let mut missing = original.clone();
    missing.dbt_node_state.as_mut().unwrap().node_configs_hash = None;
    missing.sql.push_str(" UNION ALL SELECT 9, 9, 'other'");
    scenario.expect_execute(missing).await;
    for field in ["body", "config", "macro"] {
        for signature in ["", "aa", "a", "gggggggggggggggggggggggggggggggg"] {
            let mut req = original.clone();
            let state = req.dbt_node_state.as_mut().unwrap();
            match field {
                "body" => state.node_body_hash = Some(signature.into()),
                "config" => state.node_configs_hash = Some(signature.into()),
                _ => state.node_macros_hash = Some(signature.into()),
            }
            let status = scenario.try_submit(req).await.unwrap_err();
            assert_eq!(status.code(), tonic::Code::Internal);
            assert_eq!(status.message(), "Database operation failed");
        }
    }
}

#[tokio::test]
async fn genuine_v3_pending_and_confirmed_identities_cold_miss_with_current_controls() {
    use support::legacy::{self, Rows};
    for (bytes, expected_template) in [
        (
            include_str!("../../../golden/upgrade/databricks_identity_v3_rendered.json"),
            None,
        ),
        (
            include_str!("../../../golden/upgrade/databricks_identity_v3_template.json"),
            Some("be4662efbaedda108caf615d95558fc1b293d00cca74d375fab56e428c9f722c"),
        ),
    ] {
        let frozen: serde_json::Value = serde_json::from_str(bytes).unwrap();
        let old_hash = frozen["rows"][0]["node_sql_hash"].as_str().unwrap();
        assert_eq!(
            old_hash,
            "cc0a750be73caa805e1d96b157d187154fbd09583106f956ea805354e4ffdf92"
        );
        assert_eq!(
            frozen["rows"][0]["node_template_hash"].as_str(),
            expected_template
        );
        assert_eq!(frozen["pending_rows"][0]["node_sql_hash"], old_hash);
        for confirmed in [false, true] {
            let server = support::start_server().await;
            let scenario = Scenario::connect(&server).await;
            let request: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(frozen["request"].clone()).unwrap();
            let repeat: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(frozen["unchanged_request"].clone()).unwrap();
            let epoch = frozen["outcome"]["last_modified_epoch"].as_i64().unwrap();
            let id = scenario.execute_id(request).await;
            if confirmed {
                assert!(scenario.confirm(&id, built_at(epoch)).await);
                scenario.expect_skip(repeat.clone()).await;
            }
            legacy::rewrite_logic_hashes(&server, Rows::All, Some(old_hash), expected_template)
                .await;
            if !confirmed {
                assert!(scenario.confirm(&id, built_at(epoch)).await);
            }
            scenario.expect_execute(repeat).await;
        }
    }
    let frozen: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_identity_v3_record.json"
    ))
    .unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let record: qc::RecordExecutionsRequest =
        serde_json::from_value(frozen["record"]["request"].clone()).unwrap();
    assert_eq!(scenario.record(record.records).await.unwrap(), 1);
    let repeat: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["unchanged_request"].clone()).unwrap();
    scenario.expect_skip(repeat.clone()).await;
    let projection: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(frozen["projection_request"].clone()).unwrap();
    scenario.expect_skip(projection).await;
    legacy::rewrite_logic_hashes(
        &server,
        Rows::All,
        Some("13e836d844cb5f00033b6512725799d5eaf12e78079e8e1409978a23b516ac2d"),
        None,
    )
    .await;
    scenario.expect_execute(repeat).await;
}

#[tokio::test]
async fn validated_body_config_digest_case_and_macro_presence_are_bound_separately() {
    let rows = dependency_capture();
    let original: qc::SubmitEnrichedSqlRequest = serde_json::from_value(
        rows.iter()
            .filter(|r| r["method"] == "SubmitEnrichedSQL")
            .nth(1)
            .unwrap()["request"]
            .clone(),
    )
    .unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    build_first(&scenario, &rows).await;
    for upper_body in [true, false] {
        let mut req = original.clone();
        req.sql = req.sql.replace("where id = 1", "where id = 2");
        let state = req.dbt_node_state.as_mut().unwrap();
        let digest = if upper_body {
            &mut state.node_body_hash
        } else {
            &mut state.node_configs_hash
        };
        *digest = digest.as_ref().map(|s| s.to_ascii_uppercase());
        scenario.expect_skip(req).await;
    }
    let mut macros = original.clone();
    macros.sql = macros.sql.replace("where id = 1", "where id = 2");
    macros.dbt_node_state.as_mut().unwrap().node_macros_hash =
        Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into());
    scenario.expect_execute(macros).await;
    scenario.expect_skip(original).await;
}

#[tokio::test]
async fn bounded_whole_binding_operation_traces_preserve_meaning() {
    let rows = dependency_capture();
    let original: qc::SubmitEnrichedSqlRequest = serde_json::from_value(
        rows.iter()
            .filter(|r| r["method"] == "SubmitEnrichedSQL")
            .nth(1)
            .unwrap()["request"]
            .clone(),
    )
    .unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    build_first(&scenario, &rows).await;
    // Seeds are complete symbolic traces: permutation and exact duplication
    // preserve whole bindings; swapping attached definitions or changing a
    // physical name changes meaning. No expected hash comes from production.
    for seed in 0..16_u8 {
        let mut req = original.clone();
        if seed & 1 != 0 {
            req.query_dependencies.reverse();
        }
        if seed & 4 != 0 {
            let query = req.query_dependencies[0].query.clone();
            req.query_dependencies[0].query = req.query_dependencies[1].query.clone();
            req.query_dependencies[1].query = query;
        }
        if seed & 8 != 0 {
            req.query_dependencies[0].name.push_str("_other");
        }
        if seed & 2 != 0 {
            req.query_dependencies
                .push(req.query_dependencies[0].clone());
        }
        if seed & 12 == 0 {
            scenario.expect_skip(req).await;
        } else {
            scenario.expect_execute(req).await;
        }
        scenario.expect_skip(original.clone()).await;
    }
    // An identical name with a different definition/context stays ambiguous;
    // exact duplicates must never turn a conflicting binding into evidence.
    for context in [false, true] {
        let mut req = original.clone();
        let mut conflicting = req.query_dependencies[0].clone();
        if context {
            conflicting.default_schema.push_str("_other");
        } else {
            conflicting.query.push_str(" WHERE 1 = 0");
        }
        req.query_dependencies.push(conflicting);
        scenario.expect_execute(req).await;
        scenario.expect_skip(original.clone()).await;
    }
}

#[test]
fn campaign_ownership_and_independent_physical_fault_probes() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_dependency_identity_test.py"
        ))
        .output()
        .expect("credential-free identity campaign checks");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn real_identity_command(
    server: &support::TestServer,
    only_tests: bool,
) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_dependency_identity.py"
        ))
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(env!("CARGO_BIN_EXE_record-proxy"))
        .arg("--probe")
        .arg(env!("CARGO_BIN_EXE_state-probe"))
        .arg("--schema-lease")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("explicit lease"))
        .arg("--query-identity")
        .arg("20261114c138cc")
        .arg("--dependency-query-identity")
        .arg("20261114e138ee")
        .arg("--supplement-query-identity")
        .arg("20261114a138ab")
        .arg("--contract")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_dependency_identity_contract.json"
        ))
        .arg(if only_tests {
            "--only-tests"
        } else {
            "--all-phases"
        })
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh retained artifacts"));
    command
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned client and Databricks; no warehouse DDL or relation writes"]
async fn real_dbt_cached_test_identity_matches_independent_results() {
    let server = support::start_server().await;
    let output = real_identity_command(&server, true)
        .output()
        .await
        .expect("current cached-test acceptance");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt 2.0.11 and live Databricks; exact leased demo allowlist"]
async fn real_dbt_identity_matches_independent_results() {
    let server = support::start_server().await;
    let mut command = real_identity_command(&server, false);
    let output = command
        .output()
        .await
        .expect("run current pinned identity acceptance");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn malformed_speculative_signatures_do_not_bypass_ordinary_validation() {
    let rows: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_template_tests.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let first = rows
        .iter()
        .find(|r| r["method"] == "SubmitEnrichedSQL")
        .unwrap();
    let request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(first["request"].clone()).unwrap();
    let id = scenario.execute_id(request.clone()).await;
    let captured = rows
        .iter()
        .find(|r| r["method"] == "ConfirmExecution")
        .unwrap();
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(captured["request"].clone()).unwrap();
    confirmation.request_id = id;
    let mut client =
        qc::execution_client::ExecutionClient::new(support::channel(server.addr).await);
    assert!(
        client
            .confirm_execution(confirmation)
            .await
            .unwrap()
            .into_inner()
            .success
    );
    scenario.expect_skip(request.clone()).await;
    let mut malformed = request;
    malformed.dbt_node_state.as_mut().unwrap().node_body_hash = Some(String::new());
    let mut sql = qc::sql_client::SqlClient::new(support::channel(server.addr).await);
    let status = sql
        .submit_enriched_sql_speculative(malformed.clone())
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(status.message(), "Database operation failed");
    malformed.dbt_node_state.as_mut().unwrap().node_body_hash = None;
    assert!(matches!(
        scenario.speculative(malformed.clone()).await.response,
        Some(qc::submit_sql_speculative_response::Response::SkipExecution(_))
    ));
    malformed.sql = malformed.sql.replace("where 0 = 1", "where 1 = 1");
    assert!(matches!(
        scenario.speculative(malformed).await.response,
        Some(qc::submit_sql_speculative_response::Response::Undecided(_))
    ));
}

#[tokio::test]
async fn required_full_uid_scope_keeps_cross_project_template_reuse_unresolved() {
    // Actual hosted first H history, then the fresh D project's speculative
    // reuse retained outside matching fixtures. Equal literal rows do not waive
    // the required full UID boundary. This is an intentional safety regression,
    // not a claim that hosted returns these fresh-D responses.
    let rows: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_template_tests.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    let first = rows
        .iter()
        .find(|r| r["method"] == "SubmitEnrichedSQL")
        .unwrap();
    let original: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(first["request"].clone()).unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let id = scenario.execute_id(original.clone()).await;
    let captured = rows
        .iter()
        .find(|r| r["method"] == "ConfirmExecution")
        .unwrap();
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(captured["request"].clone()).unwrap();
    confirm.request_id = id;
    let mut client =
        qc::execution_client::ExecutionClient::new(support::channel(server.addr).await);
    assert!(
        client
            .confirm_execution(confirm)
            .await
            .unwrap()
            .into_inner()
            .success
    );
    scenario.expect_skip(original.clone()).await;
    let mut fresh = original.clone();
    let old = "survey_20261114c138cd";
    let new = "survey_20261114d138df";
    for value in fresh.labels.values_mut() {
        *value = value.replace(old, new);
    }
    let state = fresh.dbt_node_state.as_mut().unwrap();
    state.node_unique_id = state.node_unique_id.replace(old, new);
    state.project_name = new.into();
    state.node_hash = "4bc307b21e8d28bd10158d53c64d8f05".into();
    assert!(matches!(
        scenario.speculative(fresh.clone()).await.response,
        Some(qc::submit_sql_speculative_response::Response::Undecided(_))
    ));
    scenario.expect_execute(fresh).await;
    scenario.expect_skip(original).await;
}

#[tokio::test]
async fn repeated_dependency_dag_keeps_display_work_bounded() {
    let rows = dependency_capture();
    let mut request: qc::SubmitEnrichedSqlRequest = serde_json::from_value(
        rows.iter()
            .find(|r| r["method"] == "SubmitEnrichedSQL")
            .unwrap()["request"]
            .clone(),
    )
    .unwrap();
    let schema = request.default_schema.clone().unwrap();
    let name = |index| format!("cdo_demo_db.{schema}.display_guard_v{index}");
    request.sql = format!("select id, amount, tag from {}", name(0));
    request.query_dependencies = (0..30)
        .map(|index| qc::QueryDependency {
            name: name(index),
            query: if index == 29 {
                "select id, amount, tag from cdo_demo_db.owned.literal_input".into()
            } else {
                format!(
                    "select id, amount, tag from {} union all select id, amount, tag from {}",
                    name(index + 1),
                    name(index + 1)
                )
            },
            default_catalog: "cdo_demo_db".into(),
            default_schema: schema.clone(),
        })
        .collect();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        scenario.expect_execute(request),
    )
    .await
    .expect("display traversal must not expand the exponential DAG");
}
