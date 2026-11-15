//! Bounded automatic FULL copy: publication, isolation and native acceptance.
mod support;

use dbt_state_proto::query_cache as qc;
use support::scenario::Scenario;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires exact #153 ordinary submit-copy clearance, pinned dbt and fresh objects"]
async fn real_dbt_submit_copy_physical_adoption() {
    use sha2::{Digest, Sha256};
    let server = support::start_server_databricks_submit_copy().await;
    let artifacts = std::path::PathBuf::from(
        std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh native artifact root"),
    );
    std::fs::create_dir_all(&artifacts).unwrap();
    let executable = std::env::current_exe().unwrap();
    std::fs::write(artifacts.join("running-server-process.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "pid":std::process::id(), "executable":executable,
        "executable_sha256":format!("{:x}",Sha256::digest(std::fs::read(&executable).unwrap())),
        "argv":std::env::args().collect::<Vec<_>>(), "address":server.addr.to_string(), "submit_copy_opt_in":true
    })).unwrap()).unwrap();
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command.arg(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../experiments/databricks_submit_copy.py"
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

fn shapes() -> (
    qc::SubmitEnrichedSqlRequest,
    qc::ConfirmExecutionRequest,
    qc::SubmitEnrichedSqlRequest,
    qc::ConfirmExecutionRequest,
) {
    let entries: Vec<serde_json::Value> =
        include_str!("../../../golden/fixtures/databricks_submit_copy.jsonl")
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
    let submits: Vec<_> = entries
        .iter()
        .filter(|e| e["method"] == "SubmitEnrichedSQL")
        .collect();
    let confirms: Vec<_> = entries
        .iter()
        .filter(|e| e["method"] == "ConfirmExecution")
        .collect();
    (
        serde_json::from_value(submits[0]["request"].clone()).unwrap(),
        serde_json::from_value(confirms[0]["request"].clone()).unwrap(),
        serde_json::from_value(submits[1]["request"].clone()).unwrap(),
        serde_json::from_value(confirms[1]["request"].clone()).unwrap(),
    )
}
async fn source(sc: &Scenario) {
    let (req, mut confirm, _, _) = shapes();
    confirm.request_id = sc.execute_id(req).await;
    assert!(sc.try_confirm(confirm).await.unwrap().success);
}
fn after_copy(
    mut req: qc::SubmitEnrichedSqlRequest,
    epoch: Option<i64>,
) -> qc::SubmitEnrichedSqlRequest {
    req.tables.push(qc::TableModifiedInfo {
        name: req.target_table.clone().unwrap(),
        last_modified_epoch: epoch,
    });
    req
}

#[test]
fn native_evidence_guards() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_submit_copy_test.py"
        ))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn automatic_copy_remains_separate_and_disabled_by_default() {
    for direct in [false, true] {
        let server = if direct {
            support::start_server_databricks_direct_copy().await
        } else {
            support::start_server().await
        };
        let sc = Scenario::connect(&server).await.org("scope");
        source(&sc).await;
        let (_, _, req, _) = shapes();
        assert_eq!(sc.verdict(req).await, "ready_to_execute");
    }
}

#[tokio::test]
async fn pending_and_replaced_sources_block_grants_and_invalidate_publication() {
    let server = support::start_server_databricks_submit_copy().await;
    for case in [
        "pending_before",
        "latest_before",
        "pending_after",
        "latest_after",
    ] {
        let sc = Scenario::connect(&server).await.org(case);
        source(&sc).await;
        let (mut src, mut src_confirm, target, mut copy_confirm) = shapes();
        let grant = if case.ends_with("after") {
            Some(sc.expect_clone(target.clone()).await)
        } else {
            None
        };
        if case.starts_with("latest") {
            src.sql.push_str(" where id = 1");
        }
        let pending = sc.execute_id(src).await;
        if case.starts_with("latest") {
            src_confirm.request_id = pending;
            src_confirm.last_modified_epoch = src_confirm.last_modified_epoch.map(|e| e + 1000);
            assert!(sc.try_confirm(src_confirm).await.unwrap().success);
        }
        if let Some(grant) = grant {
            copy_confirm.request_id = grant.request_id.clone();
            assert!(sc.try_confirm(copy_confirm.clone()).await.unwrap().success);
            let proof: serde_json::Value = sqlx::query_scalar(
                "SELECT submit_copy_provenance FROM executions WHERE org_id=$1 AND request_id=$2",
            )
            .bind(case)
            .bind(&grant.request_id)
            .fetch_one(server.state.store.pool())
            .await
            .unwrap();
            assert_eq!(proof["published"], false, "{case}");
            assert_eq!(
                sc.verdict(after_copy(target, copy_confirm.last_modified_epoch))
                    .await,
                "ready_to_execute",
                "{case}"
            );
        } else {
            assert_eq!(sc.verdict(target).await, "ready_to_execute", "{case}");
        }
    }
}

#[tokio::test]
async fn automatic_copy_preserves_org_namespace_uid_and_unknown_input_barriers() {
    let server = support::start_server_databricks_submit_copy().await;
    let sc = Scenario::connect(&server).await.org("owner");
    source(&sc).await;
    let (_, _, original, _) = shapes();
    assert_eq!(
        sc.org("foreign").verdict(original.clone()).await,
        "ready_to_execute"
    );
    for case in [
        "namespace",
        "blank_namespace",
        "uid",
        "unknown_epoch",
        "unknown_own",
        "family",
        "template",
        "config",
    ] {
        let mut req = original.clone();
        match case {
            "namespace" => req.table_namespace = Some("other".into()),
            "blank_namespace" => req.table_namespace = Some("".into()),
            "uid" => req
                .dbt_node_state
                .as_mut()
                .unwrap()
                .node_unique_id
                .push('x'),
            "unknown_epoch" => req.tables[0].last_modified_epoch = None,
            "unknown_own" => req.tables.push(qc::TableModifiedInfo {
                name: req.target_table.clone().unwrap(),
                last_modified_epoch: None,
            }),
            "family" => req.dialect = "snowflake".into(),
            "template" => req.compare_unrendered_code = true,
            "config" => {
                req.dbt_node_state.as_mut().unwrap().node_configs_hash = Some("c".repeat(32))
            }
            _ => unreachable!(),
        }
        assert_eq!(sc.verdict(req).await, "ready_to_execute", "{case}");
    }
}

async fn submit_at(
    server: &support::TestServer,
    req: qc::SubmitEnrichedSqlRequest,
    at: i64,
) -> qc::SubmitSqlResponse {
    let mut request = support::request_with_org(req, "age");
    request.metadata_mut().insert(
        dbt_state_server::REPLAY_NOW_HEADER,
        at.to_string().parse().unwrap(),
    );
    qc::sql_client::SqlClient::new(support::channel(server.addr).await)
        .submit_enriched_sql(request)
        .await
        .unwrap()
        .into_inner()
}
async fn confirm_at(server: &support::TestServer, req: qc::ConfirmExecutionRequest, at: i64) {
    let mut request = support::request_with_org(req, "age");
    request.metadata_mut().insert(
        dbt_state_server::REPLAY_NOW_HEADER,
        at.to_string().parse().unwrap(),
    );
    assert!(
        qc::execution_client::ExecutionClient::new(support::channel(server.addr).await)
            .confirm_execution(request)
            .await
            .unwrap()
            .into_inner()
            .success
    );
}
#[tokio::test]
async fn recent_copy_never_renews_old_source_lag_tolerance() {
    use qc::submit_sql_response::Response;
    let server = support::start_server_databricks_submit_copy().await;
    let (mut src, mut src_confirm, mut target, mut copy_confirm) = shapes();
    src.tables[0].last_modified_epoch = Some(10_000);
    target.tables[0].last_modified_epoch = Some(10_000);
    let Some(Response::ReadyToExecute(build)) = submit_at(&server, src, 20_000).await.response
    else {
        panic!("source BUILD")
    };
    src_confirm.request_id = build.request_id;
    src_confirm.last_modified_epoch = Some(20_000);
    confirm_at(&server, src_confirm, 20_000).await;
    let Some(Response::ReadyToClone(copy)) =
        submit_at(&server, target.clone(), 100_000).await.response
    else {
        panic!("COPY")
    };
    copy_confirm.request_id = copy.request_id;
    copy_confirm.last_modified_epoch = Some(100_000);
    confirm_at(&server, copy_confirm, 100_000).await;
    let mut repeat = after_copy(target, Some(100_000));
    assert!(matches!(
        submit_at(&server, repeat.clone(), 100_500).await.response,
        Some(Response::SkipExecution(_))
    ));
    repeat.tables[0].last_modified_epoch = Some(11_000);
    repeat.freshness_tolerance_seconds = 30;
    assert!(
        matches!(
            submit_at(&server, repeat, 110_000).await.response,
            Some(Response::ReadyToExecute(_))
        ),
        "source age90s exceeds30s; recent10s copy age must not permit stale SKIP"
    );
}

/// The separately authorized controls retain the complete hosted91 history.
/// This asserts the bounded local decision contract, not RAND rewrite/Explain
/// wire parity. The independently certified primary75 is replayed exactly by
/// conformance.rs, and the live runner checks full physical invariance.
#[tokio::test]
async fn crafted_controls_build_safely_without_claiming_volatile_rewrite_parity() {
    let entries: Vec<serde_json::Value> =
        include_str!("fixtures/databricks_submit_copy_controls.jsonl")
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
    assert_eq!(entries.len(), 91);
    let server = support::start_server_databricks_submit_copy().await;
    let sc = Scenario::connect(&server).await.org("controls");
    let mut pending = std::collections::HashMap::new();
    for entry in &entries[..75] {
        match entry["method"].as_str().unwrap() {
            "SubmitEnrichedSQL" => {
                let request: qc::SubmitEnrichedSqlRequest =
                    serde_json::from_value(entry["request"].clone()).unwrap();
                let response = sc.submit(request).await;
                let expected = entry["response"]["response"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .next()
                    .unwrap();
                assert_eq!(support::scenario::variant(&response), expected);
                let actual = serde_json::to_value(response).unwrap();
                if let Some(id) = entry["response"]["response"][expected]["request_id"].as_str() {
                    pending.insert(
                        id.to_owned(),
                        actual["response"][expected]["request_id"]
                            .as_str()
                            .unwrap()
                            .to_owned(),
                    );
                }
            }
            "ConfirmExecution" => {
                let mut req: qc::ConfirmExecutionRequest =
                    serde_json::from_value(entry["request"].clone()).unwrap();
                req.request_id = pending[&req.request_id].clone();
                assert!(sc.try_confirm(req).await.unwrap().success);
            }
            _ => {}
        }
    }
    let mut cases = 0;
    let mut baseline_runtime = None;
    for entry in &entries[75..] {
        if entry["method"] != "SubmitEnrichedSQL" {
            continue;
        }
        let req: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(entry["request"].clone()).unwrap();
        let response = sc.submit(req).await;
        let expected = if matches!(cases, 0 | 7) {
            "skip_execution"
        } else {
            "ready_to_execute"
        };
        assert_eq!(
            support::scenario::variant(&response),
            expected,
            "control{cases}"
        );
        let value = serde_json::to_value(response).unwrap();
        if matches!(cases, 0 | 7) {
            let runtime = value["response"][expected]["execution_runtime_ms"].clone();
            if cases == 0 {
                baseline_runtime = Some(runtime);
            } else {
                assert_eq!(Some(runtime), baseline_runtime);
            }
        } else {
            assert_eq!(
                value["response"][expected]["explained_decision"]["decision"],
                1
            );
            assert!(value["response"].get("ready_to_clone").is_none());
        }
        if cases == 5 {
            assert!(
                !entry["response"]["response"][expected]["transformed_nodes_by_query"]
                    .as_object()
                    .unwrap()
                    .is_empty(),
                "retain actual hosted RAND rewrite evidence"
            );
            assert!(
                value["response"][expected]["transformed_nodes_by_query"]
                    .as_object()
                    .unwrap()
                    .is_empty(),
                "bounded local policy does not evaluate volatile SQL"
            );
        }
        cases += 1;
    }
    assert_eq!(cases, 8);
}

#[tokio::test]
async fn auto_copy_identity_does_not_change_ordinary_physical_reuse() {
    for automatic in [false, true] {
        let server = if automatic {
            support::start_server_databricks_submit_copy().await
        } else {
            support::start_server().await
        };
        let sc = Scenario::connect(&server).await.org("ordinary");
        source(&sc).await;
        let (source, source_confirm, target, _) = shapes();
        for fault in ["uid", "declared_config"] {
            let mut ordinary = after_copy(source.clone(), source_confirm.last_modified_epoch);
            let mut other_target = target.clone();
            for req in [&mut ordinary, &mut other_target] {
                let state = req.dbt_node_state.as_mut().unwrap();
                if fault == "uid" {
                    state.node_unique_id.push('x');
                } else {
                    state.node_configs_hash = Some("c".repeat(32));
                }
            }
            assert_eq!(
                sc.verdict(ordinary).await,
                "skip_execution",
                "ordinary {fault},auto={automatic}"
            );
            assert_eq!(
                sc.verdict(other_target).await,
                "ready_to_execute",
                "copy still requires {fault},auto={automatic}"
            );
        }
    }
}
