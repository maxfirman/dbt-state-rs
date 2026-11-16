//! Bounded automatic FULL copy: publication, isolation and native acceptance.
mod support;

use dbt_state_proto::query_cache as qc;
use support::scenario::Scenario;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires exact #153 ordinary submit-copy clearance, pinned dbt and fresh objects"]
async fn real_dbt_submit_copy_physical_adoption() {
    let server = support::start_server_databricks_submit_copy().await;
    native_copy_campaign(server, "databricks_submit_copy.py", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires exact #154 recovery clearance, pinned dbt and fresh objects"]
async fn real_dbt_copy_recovery() {
    let server = support::start_server_databricks_copy_recovery().await;
    native_copy_campaign(server, "databricks_copy_recovery.py", true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires exact #186 shared-copy clearance, pinned dbt and fresh objects"]
async fn real_dbt_shared_copy_analysis_acceptance() {
    let server = support::start_server_databricks_copy_native().await;
    assert_eq!(server.state.clock, dbt_state_server::Clock::System);
    native_copy_campaign(server, "databricks_shared_copy.py", true).await;
}

async fn native_copy_campaign(server: support::TestServer, script: &str, direct_copy: bool) {
    use sha2::{Digest, Sha256};
    let artifacts = std::path::PathBuf::from(
        std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh native artifact root"),
    );
    std::fs::create_dir_all(&artifacts).unwrap();
    let executable = std::env::current_exe().unwrap();
    std::fs::write(
        artifacts.join("running-server-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "pid":std::process::id(), "executable":executable,
            "executable_sha256":format!("{:x}",Sha256::digest(std::fs::read(&executable).unwrap())),
            "argv":std::env::args().collect::<Vec<_>>(), "address":server.addr.to_string(),
            "submit_copy_opt_in":true, "direct_copy_opt_in":direct_copy,
            "clock":format!("{:?}", server.state.clock)
        }))
        .unwrap(),
    )
    .unwrap();
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command.arg(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments")
            .join(script),
    );
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
    for script in [
        "databricks_submit_copy_test.py",
        "databricks_copy_recovery_test.py",
        "databricks_shared_copy_test.py",
    ] {
        let output = std::process::Command::new("python3")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../experiments")
                    .join(script),
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
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

/// The client sets failed_to_clone only after a successful ordinary fallback.
/// Its rows then belong to that SQL build, never to the abandoned copy grant.
#[tokio::test]
async fn failed_copy_fallback_is_an_ordinary_build_and_retry_cannot_restore_origin() {
    let server = support::start_server_databricks_submit_copy().await;
    let sc = Scenario::connect(&server).await.org("fallback");
    source(&sc).await;
    let (_, _, target, mut confirmation) = shapes();
    let grant = sc.expect_clone(target.clone()).await;
    confirmation.request_id = grant.request_id.clone();
    confirmation.failed_to_clone = true;
    confirmation.execution_runtime_ms = Some(154);
    let epoch = confirmation.last_modified_epoch;
    // A missing physical outcome cannot publish either the grant or fallback.
    confirmation.last_modified_epoch = None;
    assert!(sc.try_confirm(confirmation.clone()).await.is_err());
    confirmation.last_modified_epoch = epoch;
    assert!(sc.try_confirm(confirmation.clone()).await.unwrap().success);
    let retained: (Option<String>, Option<serde_json::Value>, Option<serde_json::Value>, Option<i64>) =
        sqlx::query_as("SELECT clone_source_table, clone_source_inputs, submit_copy_provenance, execution_runtime_ms FROM executions WHERE org_id='fallback' AND request_id=$1")
            .bind(&grant.request_id).fetch_one(server.state.store.pool()).await.unwrap();
    assert_eq!(retained, (None, None, None, Some(154)));
    let skipped = sc.expect_skip(after_copy(target.clone(), epoch)).await;
    assert_eq!(skipped.execution_runtime_ms, Some(154));
    // A duplicate success delivery cannot relabel the fallback as a copy or
    // replace its measured runtime with the source's cached runtime.
    confirmation.failed_to_clone = false;
    confirmation.execution_runtime_ms = Some(9999);
    assert!(sc.try_confirm(confirmation).await.is_err());
    assert_eq!(
        sc.expect_skip(after_copy(target, epoch))
            .await
            .execution_runtime_ms,
        Some(154)
    );
}

#[tokio::test]
async fn a_confirmed_copy_cannot_supply_an_automatic_chain_when_original_is_pending() {
    let server = support::start_server_databricks_submit_copy().await;
    let sc = Scenario::connect(&server).await.org("chain");
    source(&sc).await;
    let (mut original, _, mut target, mut confirmation) = shapes();
    confirmation.request_id = sc.expect_clone(target.clone()).await.request_id;
    assert!(sc.try_confirm(confirmation).await.unwrap().success);
    // Keep the original out of the candidate set using an actual public
    // pending write; the only confirmed equivalent left is the copied result.
    original.sql.push_str(" where id = 1");
    sc.expect_execute(original).await;
    target.target_table = Some(
        target
            .target_table
            .unwrap()
            .replace("ds_20261112a134ef_ref_dbt_test__audit", "third"),
    );
    target.default_schema = Some("third".into());
    sc.expect_execute(target).await;
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
    repeat.freshness_tolerance_seconds = 120;
    assert!(
        matches!(
            submit_at(&server, repeat.clone(), 110_000).await.response,
            Some(Response::SkipExecution(_))
        ),
        "source age90s is within120s tolerance; unknown age must not hide the anchor"
    );
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

/// Contract example: null checks preserve symbolic input types; COUNT produces
/// checked integer outputs, including the nullable right side of LEFT JOIN.
#[tokio::test]
async fn analyzed_full_two_count_ctes_copy_confirm_and_reuse() {
    let server = support::start_server_databricks_submit_copy().await;
    let sc = Scenario::connect(&server).await.org("analyzed-two-counts");
    let (mut src, mut built, mut target, mut copied) = shapes();
    let sql = "WITH orders AS (SELECT system.builtin.COUNT(*) AS n FROM cdo.raw.orders WHERE id IS NOT NULL), payments AS (SELECT system.builtin.COUNT(*) AS n FROM cdo.raw.payments WHERE id IS NOT NULL) SELECT orders.n AS order_count, payments.n AS payment_count FROM orders LEFT JOIN payments ON orders.n = payments.n";
    for req in [&mut src, &mut target] {
        req.sql = sql.into();
        req.tables = vec![
            qc::TableModifiedInfo {
                name: "cdo.raw.orders".into(),
                last_modified_epoch: Some(100),
            },
            qc::TableModifiedInfo {
                name: "cdo.raw.payments".into(),
                last_modified_epoch: Some(101),
            },
        ];
    }
    built.request_id = sc.execute_id(src).await;
    assert!(sc.try_confirm(built).await.unwrap().success);
    let grant = sc.expect_clone(target.clone()).await;
    copied.request_id = grant.request_id;
    let epoch = copied.last_modified_epoch;
    assert!(sc.try_confirm(copied).await.unwrap().success);
    assert_eq!(
        sc.verdict(after_copy(target, epoch)).await,
        "skip_execution"
    );
}

#[tokio::test]
async fn analyzed_full_checked_forms_and_opaque_effects() {
    let server = support::start_server_databricks_submit_copy().await;
    // Expected decisions come from the contract's effect rules, not hashes.
    for (index, (sql, admits)) in [
        ("SELECT COUNT(*) AS n FROM cdo.raw.input",false),
        ("SELECT -CAST(-2147483648 AS INT) AS overflow FROM cdo.raw.input",false),
        ("SELECT CAST(1 AS DECIMAL(38,37)) * CAST(1 AS DECIMAL(38,37)) AS contextual FROM cdo.raw.input",false),
        ("SELECT CAST(1 AS DECIMAL(4,0)) AS exact_decimal FROM cdo.raw.input",true),
        ("SELECT 'hello' AS text FROM cdo.raw.input",false),
        ("SELECT id FROM cdo.raw.input GROUP BY 1",false),
        ("SELECT 2147483647 + 1 AS overflow FROM cdo.raw.input",false),
        ("SELECT custom.count(*) AS n FROM cdo.raw.input",false),
        ("SELECT current_user AS who FROM cdo.raw.input",false),
        ("SELECT id FROM cdo.raw.input WHERE id IS NOT NULL",true),
        ("SELECT i.id AS item, CASE WHEN i.id IS NULL THEN 0 ELSE 1 END AS present FROM cdo.raw.input AS i",true),
        ("SELECT CAST(2 AS BIGINT) + 3 AS five, system.builtin.COUNT(*) AS n FROM cdo.raw.input GROUP BY CAST(2 AS BIGINT) + 3 HAVING system.builtin.COUNT(*) > 0",true),
        ("SELECT system.builtin.MIN(CAST(1 AS BIGINT)) AS low, system.builtin.MAX(CAST(2 AS INT)) AS high, system.builtin.SUM(CAST(0 AS BIGINT)) AS zero FROM cdo.raw.input",true),
        ("SELECT system.builtin.COUNT(id) AS n FROM cdo.raw.input",true),
        ("SELECT system.builtin.SUM(id) AS total FROM cdo.raw.input",false),
        ("SELECT CAST(id AS BIGINT) AS numeric_id FROM cdo.raw.input",false),
        ("SELECT id + 1 AS next_id FROM cdo.raw.input",false),
        ("SELECT id FROM cdo.raw.input WHERE id = 1",false),
        ("SELECT system.builtin.COUNT(*) OVER () AS n FROM cdo.raw.input",false),
        ("SELECT mystery(id) AS n FROM cdo.raw.input",false),
        ("SELECT RAND() AS n FROM cdo.raw.input",false),
        ("SELECT CAST('2026-01-01' AS TIMESTAMP) AS n FROM cdo.raw.input",false),
        ("SELECT system.builtin.COUNT(DISTINCT id) AS n FROM cdo.raw.input",false),
        ("SELECT id + 1 FROM cdo.raw.input",false),
        ("SELECT id FROM cdo.raw.input UNION ALL SELECT id FROM cdo.raw.input",false),
        ("WITH RECURSIVE q AS (SELECT id FROM cdo.raw.input) SELECT id FROM q",false),
        ("SELECT -CAST(9223372036854775807 AS INT) AS bad FROM cdo.raw.input",false),
    ].into_iter().enumerate() {
        let org=format!("checked-{index}");
        let sc=Scenario::connect(&server).await.org(&org);
        let (mut src,mut built,mut target,mut copied)=shapes();
        src.sql=sql.into();target.sql=sql.into();
        src.tables=vec![qc::TableModifiedInfo{name:"cdo.raw.input".into(),last_modified_epoch:Some(100)}];
        target.tables=src.tables.clone();
        built.request_id=sc.execute_id(src).await;
        assert!(sc.try_confirm(built).await.unwrap().success);
        if admits {
            copied.request_id=sc.expect_clone(target.clone()).await.request_id;
            let epoch=copied.last_modified_epoch;
            assert!(sc.try_confirm(copied).await.unwrap().success,"{sql}");
            sc.expect_skip(after_copy(target,epoch)).await;
        } else {
            assert_eq!(sc.verdict(target).await,"ready_to_execute","{sql}");
        }
    }
}

#[tokio::test]
async fn analyzed_full_dependency_bindings_retain_context_and_definition() {
    let server = support::start_server_databricks_submit_copy().await;
    let sc = Scenario::connect(&server).await.org("bound-dependencies");
    let (mut src, mut built, mut target, mut copied) = shapes();
    for req in [&mut src, &mut target] {
        req.tables = vec![qc::TableModifiedInfo {
            name: "cdo.raw.input".into(),
            last_modified_epoch: Some(100),
        }];
        req.sql="SELECT a.n AS an, b.n AS bn FROM cdo.views.a AS a INNER JOIN cdo.views.b AS b ON a.n = b.n".into();
        req.query_dependencies = vec![
            qc::QueryDependency {
                name: "cdo.views.a".into(),
                query: "SELECT system.builtin.COUNT(*) AS n FROM input WHERE id IS NOT NULL".into(),
                default_catalog: "cdo".into(),
                default_schema: "raw".into(),
                ..Default::default()
            },
            qc::QueryDependency {
                name: "cdo.views.b".into(),
                query: "SELECT system.builtin.COUNT(id) AS n FROM input".into(),
                default_catalog: "cdo".into(),
                default_schema: "raw".into(),
                ..Default::default()
            },
        ];
    }
    built.request_id = sc.execute_id(src).await;
    assert!(sc.try_confirm(built).await.unwrap().success);
    for fault in [
        "swap",
        "context",
        "missing_leaf",
        "uid",
        "extra_input",
        "cycle",
    ] {
        let mut changed = target.clone();
        match fault {
            "swap" => {
                let a = changed.query_dependencies[0].query.clone();
                changed.query_dependencies[0].query = changed.query_dependencies[1].query.clone();
                changed.query_dependencies[1].query = a;
            }
            "context" => changed.query_dependencies[0].default_schema = "other".into(),
            "missing_leaf" => changed.tables.clear(),
            "uid" => changed
                .dbt_node_state
                .as_mut()
                .unwrap()
                .node_unique_id
                .push('x'),
            "extra_input" => changed.tables.push(qc::TableModifiedInfo {
                name: "cdo.raw.extra".into(),
                last_modified_epoch: Some(100),
            }),
            "cycle" => changed.query_dependencies[0].query = "SELECT n FROM cdo.views.a".into(),
            _ => unreachable!(),
        }
        assert_eq!(sc.verdict(changed).await, "ready_to_execute", "{fault}");
    }
    copied.request_id = sc.expect_clone(target.clone()).await.request_id;
    let epoch = copied.last_modified_epoch;
    assert!(sc.try_confirm(copied).await.unwrap().success);
    sc.expect_skip(after_copy(target, epoch)).await;
}

#[tokio::test]
async fn analyzed_full_multiple_inputs_keep_recorded_build_producers() {
    let server = support::start_server_databricks_submit_copy().await;
    let sc = Scenario::connect(&server).await.org("multiple-producers");
    let (mut producer, mut producer_confirm, _, _) = shapes();
    producer.target_table = Some("cdo.raw.orders".into());
    producer.sql = "SELECT 1 AS id".into();
    producer.tables.clear();
    producer.dbt_node_state.as_mut().unwrap().node_unique_id = "model.project.orders".into();
    producer_confirm.last_modified_epoch = Some(100);
    producer_confirm.request_id = sc.execute_id(producer.clone()).await;
    assert!(
        sc.try_confirm(producer_confirm.clone())
            .await
            .unwrap()
            .success
    );
    let (mut src, mut built, mut target, mut copied) = shapes();
    for req in [&mut src, &mut target] {
        req.sql="SELECT a.id AS order_id, b.id AS payment_id FROM cdo.raw.orders AS a LEFT JOIN cdo.raw.payments AS b ON TRUE".into();
        req.tables = vec![
            qc::TableModifiedInfo {
                name: "cdo.raw.orders".into(),
                last_modified_epoch: Some(100),
            },
            qc::TableModifiedInfo {
                name: "cdo.raw.payments".into(),
                last_modified_epoch: Some(101),
            },
        ];
    }
    built.request_id = sc.execute_id(src).await;
    assert!(sc.try_confirm(built).await.unwrap().success);
    // An existing target observation is separate from the two read inputs.
    let mut old = target.clone();
    old.sql = "SELECT 7 AS id".into();
    old.tables.clear();
    let mut old_confirm = copied.clone();
    old_confirm.request_id = sc.execute_id(old).await;
    assert!(sc.try_confirm(old_confirm).await.unwrap().success);
    target = after_copy(target, copied.last_modified_epoch);
    copied.request_id = sc.expect_clone(target.clone()).await.request_id;
    assert!(sc.try_confirm(copied).await.unwrap().success);
    sc.expect_skip(target.clone()).await;
    // Same observed epoch, different producer definition: never reuse its
    // consumer's old result, even with a newly empty output.
    producer.sql = "SELECT 2 AS id".into();
    producer_confirm.request_id = sc.execute_id(producer).await;
    assert!(sc.try_confirm(producer_confirm).await.unwrap().success);
    target
        .tables
        .retain(|t| Some(&t.name) != target.target_table.as_ref());
    let parts: Vec<_> = target.target_table.as_deref().unwrap().split('.').collect();
    target.target_table = Some(format!("{}.`third`.{}", parts[0], parts[2]));
    target.default_schema = Some("third".into());
    assert_eq!(sc.verdict(target).await, "ready_to_execute");
}

#[tokio::test]
async fn analyzed_full_legacy_ordinary_strict_reuse_survives_but_copy_v1_is_cold() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../golden/upgrade/databricks_auto_copy_v1_727f6a3.json"
    ))
    .unwrap();
    for pending in [false, true] {
        let server = support::start_server_databricks_submit_copy().await;
        let sc = Scenario::connect(&server).await.org("legacy-auto-v1");
        support::legacy::import_complete_history(
            &server,
            &[
                fixture["source"].clone(),
                fixture[if pending {
                    "pending_copy"
                } else {
                    "confirmed_copy"
                }]
                .clone(),
            ],
        )
        .await;
        let src: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(fixture["source_request"].clone()).unwrap();
        let source_epoch = fixture["source"]["last_modified_epoch"].as_i64();
        sc.expect_skip(after_copy(src, source_epoch)).await;
        let target: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(fixture["target_request"].clone()).unwrap();
        let confirm: qc::ConfirmExecutionRequest =
            serde_json::from_value(fixture["confirm"].clone()).unwrap();
        if pending {
            assert!(sc.try_confirm(confirm.clone()).await.unwrap().success);
        }
        // Real v1 hashes and origin proof, including a v1 pending row that
        // confirms after upgrade, cannot grant v2 adoption or a source chain.
        assert_eq!(
            sc.verdict(after_copy(target.clone(), confirm.last_modified_epoch))
                .await,
            "ready_to_execute"
        );
        let mut third = target;
        let parts: Vec<_> = third.target_table.as_deref().unwrap().split('.').collect();
        third.target_table = Some(format!("{}.`third`.{}", parts[0], parts[2]));
        third.default_schema = Some("third".into());
        assert_eq!(sc.verdict(third).await, "ready_to_execute");
    }
}
