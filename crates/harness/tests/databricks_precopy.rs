//! Native #134 causal pre-copy requests, unchanged except correlated transport IDs.
mod support;

use dbt_state_harness::diff::GoldenEntry;
use dbt_state_proto::query_cache as qc;
use support::scenario::Scenario;

fn entries() -> Vec<GoldenEntry> {
    include_str!("fixtures/databricks_precopy_native.jsonl")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
fn precopy_native_evidence_guards_reject_faults() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_precopy_test.py"
        ))
        .output()
        .expect("run warehouse-free native evidence guards");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires exact #152 physical pre-copy clearance, pinned dbt and fresh objects"]
async fn real_dbt_precopy_physical_adoption() {
    use sha2::{Digest, Sha256};
    let server = support::start_server_databricks_direct_copy().await;
    let artifacts = std::path::PathBuf::from(
        std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh native artifact root"),
    );
    std::fs::create_dir_all(&artifacts).unwrap();
    let executable = std::env::current_exe().unwrap();
    std::fs::write(artifacts.join("running-server-process.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "pid":std::process::id(), "executable":executable,
        "executable_sha256":format!("{:x}",Sha256::digest(std::fs::read(&executable).unwrap())),
        "argv":std::env::args().collect::<Vec<_>>(), "address":server.addr.to_string(), "direct_copy_opt_in":true
    })).unwrap()).unwrap();
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command.arg(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../experiments/databricks_precopy.py"
    ));
    for (flag, variable) in [
        ("dbt", "DBT"),
        ("proxy", "PROXY"),
        ("probe", "PROBE"),
        ("live-clearance", "LIVE_CLEARANCE"),
        ("operation-ledger", "OPERATION_LEDGER"),
        ("schema-lease", "SCHEMA_LEASE"),
        ("run-id", "RUN_ID"),
    ] {
        command.arg(format!("--{flag}")).arg(
            std::env::var_os(format!("DBT_STATE_DATABRICKS_{variable}"))
                .expect("exact cleared native input"),
        );
    }
    command
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(artifacts.join("campaign"));
    let argv = std::iter::once(
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
            .map(|a| a.to_string_lossy().into_owned()),
    )
    .collect::<Vec<_>>();
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = command.spawn().expect("spawn cleared native child");
    std::fs::write(
        artifacts.join("native-child-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"pid":child.id(),"argv":argv})).unwrap(),
    )
    .unwrap();
    let output = child.wait_with_output().await.expect("drain native child");
    std::fs::write(artifacts.join("native-child.stdout"), &output.stdout).unwrap();
    std::fs::write(artifacts.join("native-child.stderr"), &output.stderr).unwrap();
    println!("{}", String::from_utf8_lossy(&output.stdout));
    let captures = std::fs::read_dir(artifacts.join("campaign/golden"))
        .expect("child capture; inspect retained stderr on early failure")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|s| s == "jsonl"))
        .collect::<Vec<_>>();
    assert_eq!(captures.len(), 1, "one complete native session");
    support::native_clock::retain(
        &server,
        &captures[0],
        &artifacts.join("native-replay-clocks.json"),
    )
    .await
    .expect("retain actual decision audit clocks before State cleanup");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn source(sc: &Scenario, rows: &[GoldenEntry]) {
    let req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[0].request.clone()).unwrap();
    let id = sc.execute_id(req).await;
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[1].request.clone()).unwrap();
    confirm.request_id = id;
    assert!(
        qc::execution_client::ExecutionClient::new(sc.channel())
            .confirm_execution(support::captured_request(confirm, &rows[1]))
            .await
            .unwrap()
            .into_inner()
            .success
    );
}

/// Preview policy refuses even a source that qualifies for the isolated copy path.
#[tokio::test]
async fn preview_defaults_to_no_databricks_copy_grants() {
    let rows = entries();
    let server = support::start_server().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    source(&sc, &rows).await;
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let response = sc.register_clone(req).await.unwrap();
    assert!(
        response.ready_to_clone.is_none(),
        "preview must not grant copy DDL"
    );
    assert!(
        response.response.is_none(),
        "deprecated copy grant must also be absent"
    );
    let refusal = response.unable_to_clone.expect("normal no-copy response");
    assert_eq!(
        refusal.explained_decision.unwrap().clone_rejection_reason,
        Some(10)
    );

    // The caller can continue via ordinary SubmitEnrichedSQL after refusal.
    // The absent target has no copied result and must be built normally.
    let mut submit: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    submit
        .tables
        .retain(|table| Some(&table.name) != submit.target_table.as_ref());
    assert_eq!(sc.verdict(submit).await, "ready_to_execute");
}

#[tokio::test]
async fn ordinary_databricks_full_submit_never_automatically_copies() {
    let rows = entries();
    // Even direct-copy opt-in cannot expand ordinary-submit copy policy.
    for direct_copy in [false, true] {
        let server = if direct_copy {
            support::start_server_databricks_direct_copy().await
        } else {
            support::start_server().await
        };
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        let mut submit: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[0].request.clone()).unwrap();
        submit.execution_type = qc::ModelExecutionType::Full as i32;
        let id = sc.execute_id(submit.clone()).await;
        let mut confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[1].request.clone()).unwrap();
        confirm.request_id = id;
        assert!(sc.try_confirm(confirm).await.unwrap().success);
        submit.target_table = Some(rows[2].request["target_table"].as_str().unwrap().into());
        assert_eq!(sc.verdict(submit).await, "ready_to_execute");
    }
}

#[tokio::test]
async fn native_default_format_precopy_returns_exact_hosted_ddl() {
    for rows in entries().chunks(8) {
        let server = support::start_server_databricks_direct_copy().await;
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        source(&sc, rows).await;
        let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        assert!(req.clone_source_table_type.is_none());
        let response = sc.register_clone(req).await.unwrap();
        let mut actual = serde_json::to_value(response).unwrap();
        // IDs are generated independently by the local service. Keep every
        // decision, DDL byte, null and deprecated mirror from the capture.
        for path in ["ready_to_clone", "response"] {
            let (actual, expected) = if path == "response" {
                (
                    &mut actual[path]["ready_to_clone_v1"],
                    &rows[2].response[path]["ready_to_clone_v1"],
                )
            } else {
                (&mut actual[path], &rows[2].response[path])
            };
            actual["request_id"] = expected["request_id"].clone();
        }
        assert_eq!(actual, rows[2].response);
    }
}

/// Safety controls are local requirements; they make no claim about unknown
/// hosted rejection codes. A pending or unproven source cannot authorize DDL.
#[tokio::test]
async fn direct_precopy_requires_current_source_in_exact_scope() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    let request: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    assert!(sc
        .register_clone(request.clone())
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
    let source_request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[0].request.clone()).unwrap();
    let pending = sc.execute_id(source_request.clone()).await;
    assert!(sc
        .register_clone(request.clone())
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[1].request.clone()).unwrap();
    confirmation.request_id = pending;
    assert!(sc.try_confirm(confirmation.clone()).await.unwrap().success);
    assert!(sc
        .register_clone(request.clone())
        .await
        .unwrap()
        .ready_to_clone
        .is_some());
    for case in [
        "missing_namespace",
        "empty_namespace",
        "wrong_namespace",
        "wrong_uid",
        "missing_uid",
        "wrong_epoch",
        "unknown_epoch",
        "wrong_kind",
        "foreign_family",
        "chain_limit",
    ] {
        let mut req = request.clone();
        match case {
            "missing_namespace" => req.table_namespace = None,
            "empty_namespace" => req.table_namespace = Some(String::new()),
            "wrong_namespace" => req.table_namespace = Some("another-connection".into()),
            "wrong_uid" => {
                req.labels.insert(
                    "dbt_node_unique_id".into(),
                    "model.other.copy_incremental".into(),
                );
            }
            "missing_uid" => {
                req.labels.remove("dbt_node_unique_id");
            }
            "wrong_epoch" => req.clone_source_last_modified_epoch = Some(1),
            "unknown_epoch" => req.clone_source_last_modified_epoch = None,
            "wrong_kind" => req.execution_type = 7,
            "foreign_family" => req.dialect = "duckdb".into(),
            "chain_limit" => req.clone_chain_depth_limit = Some(0),
            _ => unreachable!(),
        }
        assert!(
            sc.register_clone(req)
                .await
                .unwrap()
                .ready_to_clone
                .is_none(),
            "{case}"
        );
    }
    assert!(sc
        .org("other-org")
        .register_clone(request.clone())
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
    // A newer incompatible result hides the older valid source. It is not a
    // candidate search through history for whichever source happens to match.
    let mut replacement = source_request;
    replacement.sql.push_str(" where id > 0");
    replacement.dbt_node_state.as_mut().unwrap().node_unique_id =
        "model.other.copy_incremental".into();
    let id = sc.execute_id(replacement).await;
    confirmation.request_id = id;
    confirmation.last_modified_epoch = confirmation.last_modified_epoch.map(|epoch| epoch + 1000);
    assert!(sc.try_confirm(confirmation).await.unwrap().success);
    assert!(sc
        .register_clone(request)
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
}

#[tokio::test]
async fn native_confirmed_copy_adopts_exact_source_identity_and_inputs() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    source(&sc, &rows).await;
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let ready = sc
        .register_clone(req)
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let adopter: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    sc.expect_execute(adopter.clone()).await;
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirmation.request_id = ready.request_id;
    assert!(sc.try_confirm(confirmation).await.unwrap().success);
    let response = sc.submit(adopter).await;
    let mut actual = serde_json::to_value(response).unwrap();
    actual["response"]["skip_execution"]["execution_decision_id"] =
        rows[4].response["response"]["skip_execution"]["execution_decision_id"].clone();
    assert_eq!(actual, rows[4].response);
}

#[tokio::test]
async fn earlier_submitted_pending_source_blocks_registration() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    let mut earlier: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[0].request.clone()).unwrap();
    earlier.sql.push_str(" where id > 0");
    let _earlier_pending = sc.execute_id(earlier).await;
    source(&sc, &rows).await;
    // Submission order cannot prove completion order. The earlier pending
    // write may replace the current source while the warehouse copies it.
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    assert!(sc
        .register_clone(req)
        .await
        .unwrap()
        .ready_to_clone
        .is_none());
}

#[tokio::test]
async fn template_mode_without_signatures_cannot_establish_direct_adoption() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    let mut request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[0].request.clone()).unwrap();
    request.compare_unrendered_code = true;
    request.dbt_node_state.as_mut().unwrap().node_body_hash = None;
    request.dbt_node_state.as_mut().unwrap().node_configs_hash = None;
    let id = sc.execute_id(request).await;
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[1].request.clone()).unwrap();
    confirm.request_id = id;
    assert!(sc.try_confirm(confirm).await.unwrap().success);
    let copy: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let ready = sc
        .register_clone(copy)
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirm.request_id = ready.request_id;
    assert!(sc.try_confirm(confirm).await.unwrap().success);
    let mut adopter: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    adopter.compare_unrendered_code = true;
    adopter.dbt_node_state.as_mut().unwrap().node_body_hash = None;
    adopter.dbt_node_state.as_mut().unwrap().node_configs_hash = None;
    sc.expect_execute(adopter).await;
}

fn assert_native_response(actual: impl serde::Serialize, expected: &serde_json::Value) {
    fn remap(actual: &mut serde_json::Value, expected: &serde_json::Value) {
        if let (Some(actual), Some(expected)) = (actual.as_object_mut(), expected.as_object()) {
            for (key, value) in actual {
                if let Some(want) = expected.get(key) {
                    if ["request_id", "execution_decision_id"].contains(&key.as_str())
                        && value.as_str().is_some_and(|s| !s.is_empty())
                        && want.as_str().is_some_and(|s| !s.is_empty())
                    {
                        *value = want.clone();
                    } else {
                        remap(value, want);
                    }
                }
            }
        }
    }
    let mut actual = serde_json::to_value(actual).unwrap();
    remap(&mut actual, expected);
    assert_eq!(&actual, expected);
}

#[tokio::test]
async fn native_merge_and_snapshot_copy_adoption_repeat_and_changed_input() {
    for rows in entries().chunks(8) {
        let server = support::start_server_databricks_direct_copy().await;
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        source(&sc, rows).await;
        let request: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        let ready = sc
            .register_clone(request)
            .await
            .unwrap()
            .ready_to_clone
            .unwrap();
        let mut confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[3].request.clone()).unwrap();
        confirm.request_id = ready.request_id;
        assert!(sc.try_confirm(confirm).await.unwrap().success);
        for row in &rows[4..7] {
            let request: qc::SubmitEnrichedSqlRequest =
                serde_json::from_value(row.request.clone()).unwrap();
            let response = sc.submit(request.clone()).await;
            assert_native_response(&response, &row.response);
            if let Some(qc::submit_sql_response::Response::ReadyToExecute(ready)) =
                response.response
            {
                let mut confirmation: qc::ConfirmExecutionRequest =
                    serde_json::from_value(rows[7].request.clone()).unwrap();
                confirmation.request_id = ready.request_id;
                let epoch = confirmation.last_modified_epoch;
                assert!(sc.try_confirm(confirmation).await.unwrap().success);
                let mut repeated = request;
                for table in &mut repeated.tables {
                    if Some(&table.name) == repeated.target_table.as_ref() {
                        table.last_modified_epoch = epoch;
                    }
                }
                sc.expect_skip(repeated).await;
            }
        }
    }
}

#[tokio::test]
async fn adoption_uses_source_input_epoch_even_when_input_changed_before_copy() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    source(&sc, &rows).await;
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let ready = sc
        .register_clone(req)
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirmation.request_id = ready.request_id;
    assert!(sc.try_confirm(confirmation.clone()).await.unwrap().success);
    let adopter: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    sc.expect_skip(adopter.clone()).await;
    let mut changed = adopter.clone();
    changed
        .tables
        .iter_mut()
        .find(|t| Some(&t.name) != changed.target_table.as_ref())
        .unwrap()
        .last_modified_epoch = confirmation.last_modified_epoch.map(|epoch| epoch - 1000);
    // It changed AFTER the source read but BEFORE the copy. The copy epoch
    // must not silently replace the immutable source's input epoch baseline.
    sc.expect_execute(changed).await;
    for case in [
        "sql",
        "config",
        "uid",
        "binding",
        "own_epoch",
        "unknown_input",
        "template",
        "template_without_signatures",
    ] {
        let mut request = adopter.clone();
        match case {
            "sql" => request.sql.push_str(" where id > 0"),
            "config" => {
                request
                    .semantic_extras
                    .insert("unique_key".into(), "\"other\"".into());
            }
            "uid" => {
                request.dbt_node_state.as_mut().unwrap().node_unique_id =
                    "model.other.copy_incremental".into()
            }
            "binding" => {
                request.sql = request.sql.replace("`inputs`", "`other_inputs`");
                for table in &mut request.tables {
                    table.name = table.name.replace("`inputs`", "`other_inputs`");
                }
            }
            "own_epoch" => {
                request
                    .tables
                    .iter_mut()
                    .find(|t| Some(&t.name) == request.target_table.as_ref())
                    .unwrap()
                    .last_modified_epoch = confirmation.last_modified_epoch.map(|epoch| epoch + 1)
            }
            "unknown_input" => {
                request
                    .tables
                    .iter_mut()
                    .find(|t| Some(&t.name) != request.target_table.as_ref())
                    .unwrap()
                    .last_modified_epoch = None
            }
            "template" => request.compare_unrendered_code = true,
            "template_without_signatures" => {
                request.compare_unrendered_code = true;
                let state = request.dbt_node_state.as_mut().unwrap();
                state.node_body_hash = None;
                state.node_configs_hash = None;
            }
            _ => unreachable!(),
        }
        sc.expect_execute(request).await;
    }
}

#[tokio::test]
async fn source_replacement_between_registration_and_confirmation_cannot_be_adopted() {
    for publish_source in [false, true] {
        let rows = entries();
        let server = support::start_server_databricks_direct_copy().await;
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        source(&sc, &rows).await;
        let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        let ready = sc
            .register_clone(req)
            .await
            .unwrap()
            .ready_to_clone
            .unwrap();
        let mut replacement: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[0].request.clone()).unwrap();
        replacement.sql.push_str(" where id > 0");
        let source_id = sc.execute_id(replacement).await;
        let mut source_confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[1].request.clone()).unwrap();
        source_confirm.request_id = source_id;
        source_confirm.last_modified_epoch =
            source_confirm.last_modified_epoch.map(|epoch| epoch + 1);
        if publish_source {
            assert!(sc.try_confirm(source_confirm).await.unwrap().success);
        }
        let mut confirmation: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[3].request.clone()).unwrap();
        confirmation.request_id = ready.request_id;
        assert!(sc.try_confirm(confirmation).await.unwrap().success);
        let adopter: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[4].request.clone()).unwrap();
        sc.expect_execute(adopter).await;
    }
}

#[tokio::test]
async fn changed_producer_before_copy_cannot_be_hidden_by_copy_age_or_lag() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    source(&sc, &rows).await;
    let adopter: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    let input = adopter
        .tables
        .iter()
        .find(|t| Some(&t.name) != adopter.target_table.as_ref())
        .unwrap();
    let mut producer = adopter.clone();
    producer.target_table = Some(input.name.clone());
    producer.tables.clear();
    producer.sql = "select 1 as id".into();
    producer.execution_type = 1;
    producer.dbt_node_state.as_mut().unwrap().node_unique_id = "model.other.raw_producer".into();
    let producer_id = sc.execute_id(producer).await;
    let mut producer_confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[1].request.clone()).unwrap();
    producer_confirm.request_id = producer_id;
    producer_confirm.last_modified_epoch = input.last_modified_epoch;
    assert!(sc.try_confirm(producer_confirm).await.unwrap().success);
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let ready = sc
        .register_clone(req)
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirmation.request_id = ready.request_id;
    assert!(sc.try_confirm(confirmation).await.unwrap().success);
    let mut adopter = adopter;
    adopter.freshness_tolerance_seconds = 1_000_000_000;
    sc.expect_execute(adopter).await;
}

#[tokio::test]
async fn failed_clone_clears_adoption_and_unknown_source_inputs_do_not_establish_it() {
    for case in [
        "failed_clone",
        "unknown_source_epoch",
        "unqualified_sql",
        "incompatible_source_type",
    ] {
        let rows = entries();
        let server = support::start_server_databricks_direct_copy().await;
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        let mut source_request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[0].request.clone()).unwrap();
        if case == "unknown_source_epoch" {
            source_request.tables[0].last_modified_epoch = None;
        }
        if case == "unqualified_sql" {
            source_request.sql = "select * from inputs".into();
        }
        let id = sc.execute_id(source_request).await;
        let mut source_confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[1].request.clone()).unwrap();
        source_confirm.request_id = id;
        if case == "incompatible_source_type" {
            source_confirm.table_type = Some("VIEW".into());
        }
        assert!(sc.try_confirm(source_confirm).await.unwrap().success);
        let request: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        let response = sc.register_clone(request).await.unwrap();
        if case == "incompatible_source_type" {
            assert!(response.ready_to_clone.is_none());
            continue;
        }
        let mut confirmation: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[3].request.clone()).unwrap();
        confirmation.request_id = response.ready_to_clone.unwrap().request_id;
        confirmation.failed_to_clone = case == "failed_clone";
        assert!(sc.try_confirm(confirmation).await.unwrap().success);
        let mut adopter: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[4].request.clone()).unwrap();
        if case == "unqualified_sql" {
            adopter.sql = "select * from inputs".into();
        }
        sc.expect_execute(adopter).await;
    }
}

#[tokio::test]
#[allow(deprecated)] // Both on-wire response mirrors remain part of the contract.
async fn confirmed_direct_copy_is_not_a_chain_source_even_with_a_larger_limit() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let sc = Scenario::connect(&server)
        .await
        .org(support::captured_org(&rows[0]));
    source(&sc, &rows).await;
    let request: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let granted = sc
        .register_clone(request.clone())
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let mut confirmation: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirmation.request_id = granted.request_id;
    assert!(sc.try_confirm(confirmation.clone()).await.unwrap().success);
    for depth in [1, 2, 100] {
        let mut chained = request.clone();
        chained.clone_source_table = request.target_table.clone();
        chained.clone_source_last_modified_epoch = confirmation.last_modified_epoch;
        chained.target_table = "`cdo_demo_db`.`third`.`chain_target`".into();
        chained.clone_chain_depth_limit = Some(depth);
        let response = sc.register_clone(chained).await.unwrap();
        assert!(response.ready_to_clone.is_none(), "depth {depth}");
        assert!(!matches!(
            response.response,
            Some(qc::clone_response::Response::ReadyToCloneV1(_))
        ));
        if depth == 1 {
            assert_eq!(
                response
                    .unable_to_clone
                    .unwrap()
                    .explained_decision
                    .unwrap()
                    .clone_rejection_reason,
                Some(10),
                "captured native chain-limit refusal"
            );
        }
    }
    for case in ["unknown_epoch", "missing_uid", "empty_uid"] {
        let mut chained = request.clone();
        chained.clone_source_table = request.target_table.clone();
        chained.clone_source_last_modified_epoch = confirmation.last_modified_epoch;
        chained.target_table = "`cdo_demo_db`.`third`.`chain_target`".into();
        chained.clone_chain_depth_limit = Some(1);
        match case {
            "unknown_epoch" => chained.clone_source_last_modified_epoch = None,
            "missing_uid" => {
                chained.labels.remove("dbt_node_unique_id");
            }
            "empty_uid" => {
                chained
                    .labels
                    .insert("dbt_node_unique_id".into(), "".into());
            }
            _ => unreachable!(),
        }
        let response = sc.register_clone(chained).await.unwrap();
        assert!(response.ready_to_clone.is_none());
        assert_eq!(
            response
                .unable_to_clone
                .unwrap()
                .explained_decision
                .unwrap()
                .clone_rejection_reason,
            Some(5),
            "{case}"
        );
    }
}

/// Broader query shape at the existing public direct-copy seam. Expectations
/// here are protocol/model safety; physical cumulative/SCD acceptance is #186.
#[tokio::test]
async fn shared_analyzed_merge_and_check_snapshot_adopt_then_rebuild() {
    for rows in entries().chunks(8) {
        let server = support::start_server_databricks_direct_copy().await;
        let sc = Scenario::connect(&server)
            .await
            .org(support::captured_org(&rows[0]));
        let mut src: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[0].request.clone()).unwrap();
        // Same structured physical name with a bare catalog and backtick
        // schema/relation: the shared adapter handles each component.
        let input = src.tables[0].name.replacen('`', "", 1).replacen('`', "", 1);
        src.sql = if src.execution_type == 3 {
            let second = input.replace("`inputs`", "`stats`");
            src.tables.push(qc::TableModifiedInfo {
                name: second.clone(),
                last_modified_epoch: src.tables[0].last_modified_epoch,
            });
            format!("WITH a AS (SELECT system.builtin.count(*) AS id FROM {input}), b AS (SELECT system.builtin.count(*) AS v FROM {second}) SELECT a.id AS id, b.v AS v FROM a LEFT JOIN b ON a.id = b.v")
        } else {
            format!("WITH a AS (SELECT id, v, ts FROM {input} WHERE id IS NOT NULL) SELECT id, v, ts FROM a")
        };
        let id = sc.execute_id(src.clone()).await;
        let mut source_confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[1].request.clone()).unwrap();
        source_confirm.request_id = id;
        assert!(sc.try_confirm(source_confirm).await.unwrap().success);
        let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        let ready = sc
            .register_clone(req)
            .await
            .unwrap()
            .ready_to_clone
            .unwrap();
        let mut confirmation: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[3].request.clone()).unwrap();
        confirmation.request_id = ready.request_id;
        assert!(sc.try_confirm(confirmation.clone()).await.unwrap().success);
        let mut adopter: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[4].request.clone()).unwrap();
        adopter.sql = src.sql;
        let own = adopter
            .tables
            .iter()
            .find(|t| Some(&t.name) == adopter.target_table.as_ref())
            .unwrap()
            .clone();
        adopter.tables = src.tables;
        adopter.tables.push(own);
        let response = sc.expect_skip(adopter.clone()).await;
        let components: Vec<_> = input.split('.').collect();
        let collision = format!(
            "`{}.{}`.{}",
            components[0],
            components[1].trim_matches('`'),
            components[2]
        );
        let mut wrong_binding = adopter.clone();
        wrong_binding.sql = wrong_binding.sql.replace(&input, &collision);
        sc.expect_execute(wrong_binding).await;
        let original: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[1].request.clone()).unwrap();
        assert_eq!(response.execution_runtime_ms, original.execution_runtime_ms);
        let mut duplicate = confirmation.clone();
        duplicate.last_modified_epoch = Some(1);
        duplicate.execution_runtime_ms = Some(1);
        duplicate.failed_to_clone = true;
        assert_eq!(
            sc.try_confirm(duplicate).await.unwrap_err().code(),
            tonic::Code::Internal
        );
        assert_eq!(
            sc.expect_skip(adopter.clone()).await.execution_runtime_ms,
            original.execution_runtime_ms
        );
        adopter.tables[0].last_modified_epoch = confirmation.last_modified_epoch.map(|e| e - 1);
        let id = sc.execute_id(adopter.clone()).await;
        confirmation.request_id = id;
        confirmation.last_modified_epoch = confirmation.last_modified_epoch.map(|e| e + 1000);
        assert!(sc.try_confirm(confirmation.clone()).await.unwrap().success);
        adopter.tables.last_mut().unwrap().last_modified_epoch = confirmation.last_modified_epoch;
        sc.expect_skip(adopter).await;
    }
}

#[tokio::test]
async fn actual_v1_direct_history_cold_misses_after_upgrade() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_direct_copy_v1_75a2a34.json"
    ))
    .unwrap();
    for (pending, fault) in [
        (false, "v1"),
        (true, "v1"),
        (false, "null"),
        (true, "unknown"),
    ] {
        let server = support::start_server_databricks_direct_copy().await;
        let org = fixture["source"]["org_id"].as_str().unwrap();
        let sc = Scenario::connect(&server).await.org(org);
        let mut copy = fixture[if pending {
            "pending_copy"
        } else {
            "confirmed_copy"
        }]
        .clone();
        match fault {
            "null" => copy["direct_copy_provenance"] = serde_json::Value::Null,
            "unknown" => copy["direct_copy_provenance"]["version"] = serde_json::json!(255),
            _ => {}
        }
        support::legacy::import_complete_history(&server, &[fixture["source"].clone(), copy]).await;
        let mut src: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(fixture["source_request"].clone()).unwrap();
        src.tables.push(qc::TableModifiedInfo {
            name: src.target_table.clone().unwrap(),
            last_modified_epoch: fixture["source"]["last_modified_epoch"].as_i64(),
        });
        sc.expect_skip(src).await;
        let confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(fixture["confirm"].clone()).unwrap();
        if pending {
            assert!(sc.try_confirm(confirm).await.unwrap().success);
        }
        let adopter: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(fixture["target_request"].clone()).unwrap();
        sc.expect_execute(adopter).await;
        let req: qc::CloneRequest = serde_json::from_value(entries()[2].request.clone()).unwrap();
        // A legacy ordinary source still permits DDL, but has no v2 adoption proof.
        let ready = sc
            .register_clone(req)
            .await
            .unwrap()
            .ready_to_clone
            .unwrap();
        let mut confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(fixture["confirm"].clone()).unwrap();
        confirm.request_id = ready.request_id;
        assert!(sc.try_confirm(confirm).await.unwrap().success);
        let adopter: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(fixture["target_request"].clone()).unwrap();
        sc.expect_execute(adopter).await;
    }
}

#[tokio::test]
async fn direct_v2_retains_original_source_age_within_and_outside_lag() {
    for rows in entries().chunks(8) {
        let server = support::start_server_databricks_direct_copy().await;
        let org = support::captured_org(&rows[0]);
        let sc = Scenario::connect(&server).await.org(org);
        source(&sc, rows).await;
        let original: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[1].request.clone()).unwrap();
        let epoch = original.last_modified_epoch.unwrap();
        let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
        let ready = sc
            .register_clone(req)
            .await
            .unwrap()
            .ready_to_clone
            .unwrap();
        let mut confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(rows[3].request.clone()).unwrap();
        confirm.request_id = ready.request_id;
        confirm.last_modified_epoch = Some(epoch + 80_000);
        assert!(sc.try_confirm(confirm.clone()).await.unwrap().success);
        let mut adopter: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(rows[4].request.clone()).unwrap();
        for t in &mut adopter.tables {
            t.last_modified_epoch = Some(if Some(&t.name) == adopter.target_table.as_ref() {
                epoch + 80_000
            } else {
                epoch - 1
            });
        }
        for (tolerance, expected) in [(120, "skip_execution"), (30, "ready_to_execute")] {
            adopter.freshness_tolerance_seconds = tolerance;
            let mut request = support::request_with_org(adopter.clone(), org);
            request.metadata_mut().insert(
                dbt_state_server::REPLAY_NOW_HEADER,
                (epoch + 90_000).to_string().parse().unwrap(),
            );
            let response = qc::sql_client::SqlClient::new(sc.channel())
                .submit_enriched_sql(request)
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                support::scenario::variant(&response),
                expected,
                "source90s vs copy10s, tolerance{tolerance}"
            );
        }
    }
}

#[tokio::test]
async fn invalid_direct_v2_origin_cannot_bypass_proof_through_strict_or_other_origin() {
    let rows = entries();
    let server = support::start_server_databricks_direct_copy().await;
    let org = support::captured_org(&rows[0]);
    let sc = Scenario::connect(&server).await.org(org);
    source(&sc, &rows).await;
    let req: qc::CloneRequest = serde_json::from_value(rows[2].request.clone()).unwrap();
    let ready = sc
        .register_clone(req)
        .await
        .unwrap()
        .ready_to_clone
        .unwrap();
    let mut confirm: qc::ConfirmExecutionRequest =
        serde_json::from_value(rows[3].request.clone()).unwrap();
    confirm.request_id = ready.request_id.clone();
    assert!(sc.try_confirm(confirm).await.unwrap().success);
    let adopter: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(rows[4].request.clone()).unwrap();
    sc.expect_skip(adopter.clone()).await;
    let copy = support::rows::complete_execution(&server, &ready.request_id).await;
    assert_eq!(copy["direct_copy_provenance"]["version"], 2);
    assert_eq!(copy["analyzed_copy_proof"]["version"], 2);
    assert_eq!(copy["direct_copy_provenance"]["published"], true);
    let mut changed = adopter.clone();
    changed.tables[0].last_modified_epoch = changed.tables[0].last_modified_epoch.map(|e| e + 1000);
    let own_id = sc.execute_id(changed).await;
    let own = support::rows::complete_execution(&server, &own_id).await;
    for fault in [
        "null",
        "legacy",
        "unknown",
        "unpublished",
        "missing_core",
        "incompatible_core",
        "automatic_origin",
        "strict_bypass",
    ] {
        let target_server = support::start_server_databricks_direct_copy().await;
        let target_sc = Scenario::connect(&target_server).await.org(org);
        let mut bad = copy.clone();
        match fault {
            "null" => bad["direct_copy_provenance"] = serde_json::Value::Null,
            "legacy" => bad["direct_copy_provenance"]["version"] = serde_json::json!(1),
            "unknown" => bad["direct_copy_provenance"]["version"] = serde_json::json!(255),
            "unpublished" => bad["direct_copy_provenance"]["published"] = serde_json::json!(false),
            "missing_core" => bad["analyzed_copy_proof"] = serde_json::Value::Null,
            "incompatible_core" => bad["analyzed_copy_proof"]["version"] = serde_json::json!(255),
            "automatic_origin" => {
                bad["submit_copy_provenance"] = serde_json::json!({"version":2,"published":true,"source_execution_id":1,"source_epoch":1,"source_physical_key":"wrong"});
            }
            "strict_bypass" => {
                bad["node_sql_hash"] = own["node_sql_hash"].clone();
                bad["direct_copy_provenance"]["published"] = serde_json::json!(false);
            }
            _ => unreachable!(),
        }
        // These deliberate persisted-evidence faults retain the actual v2
        // hashes. They cannot become permission through a different entry path.
        support::legacy::import_complete_execution(&target_server, &bad).await;
        target_sc.expect_execute(adopter.clone()).await;
    }
}
