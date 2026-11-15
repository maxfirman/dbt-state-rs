//! Credential-free preparation of the complete timestamp-ignore snapshot history result oracle.
//! Live acceptance requires an exact fresh-container allocation and clearance.
#[test]
fn snapshot_oracle_rejects_inconsistent_scd_histories() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_snapshot_test.py"
        ))
        .output()
        .expect("run credential-free timestamp-ignore snapshot history fault probes");
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
#[ignore = "requires exact #151 live clearance, pinned dbt and fresh Databricks pair"]
async fn real_dbt_snapshots_match_independent_operation_history() {
    run_snapshot_campaign(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires exact #151 source-clock modes clearance and fresh Databricks objects"]
async fn real_dbt_source_clock_snapshot_modes() {
    run_snapshot_campaign(true).await;
}

async fn run_snapshot_campaign(modes: bool) {
    use sha2::{Digest, Sha256};
    let server = support::start_server().await;
    let artifacts = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .expect("fresh retained snapshot artifact directory");
    let executable = std::env::current_exe().expect("running acceptance executable");
    std::fs::create_dir_all(&artifacts).expect("create receipt directory");
    std::fs::write(
        artifacts.join("running-server-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "pid": std::process::id(),
            "executable": executable,
            "executable_sha256": format!("{:x}", Sha256::digest(std::fs::read(&executable).expect("read running executable"))),
            "argv": std::env::args().collect::<Vec<_>>(),
            "address": server.addr.to_string(),
        }))
        .unwrap(),
    )
    .expect("persist server process identity before warehouse use");
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("persistent Python with PyYAML"),
    );
    command
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(if modes {
                "../../experiments/databricks_snapshot_modes.py"
            } else {
                "../../experiments/databricks_snapshots.py"
            }),
        )
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned dbt"))
        .arg("--proxy")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROXY").expect("immutable proxy"))
        .arg("--probe")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_PROBE").expect("immutable probe"))
        .arg("--live-clearance")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_LIVE_CLEARANCE").expect("exact live clearance"))
        .arg("--operation-ledger")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_OPERATION_LEDGER")
                .expect("frozen independent snapshot ledger"),
        )
        .arg(if modes {
            "--schema-lease"
        } else {
            "--result-ledger"
        })
        .arg(
            std::env::var_os(if modes {
                "DBT_STATE_DATABRICKS_SCHEMA_LEASE"
            } else {
                "DBT_STATE_DATABRICKS_RESULT_LEDGER"
            })
            .expect("frozen snapshot ledger/ownership lease"),
        )
        .arg("--run-id")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID").expect("exact cleared Rust run ID"))
        .arg("--rust-url")
        .arg(format!("http://{}", server.addr))
        .arg("--work-dir")
        .arg(artifacts.join("campaign"));
    let child_argv = std::iter::once(
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
            .map(|arg| arg.to_string_lossy().into_owned()),
    )
    .collect::<Vec<_>>();
    command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = command.spawn().expect("spawn exact cleared snapshot phase");
    std::fs::write(
        artifacts.join("native-child-process.json"),
        serde_json::to_vec_pretty(&serde_json::json!({"pid": child.id(), "argv": child_argv}))
            .unwrap(),
    )
    .expect("retain actual native child PID and argv");
    let output = child
        .wait_with_output()
        .await
        .expect("drain exact snapshot child");
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

#[test]
fn source_clock_snapshot_modes_preserve_complete_scd_history() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_snapshot_modes_test.py"
        ))
        .output()
        .expect("run source-clock snapshot fault probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Modeled protocol safety complements the real source-clock SCD campaigns.
/// Unknown metadata and another physical/node scope cannot establish reuse.
#[tokio::test]
async fn snapshots_require_current_scoped_source_and_successful_publication() {
    use dbt_state_proto::query_cache::ModelExecutionType;
    use support::scenario::{model, Scenario};
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let source = "cdo_demo_db.snapshots.inputs";
    let snapshot = model("cdo_demo_db.snapshots.result")
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("snapshots")
        .namespace("snapshot-connection")
        .unique_id("snapshot.project.result")
        .node_state(|state| state.resource_type = "snapshot".into())
        .execution_type(ModelExecutionType::Snapshot)
        .extra("unique_key", "\"id\"")
        .sql("select id,v,p,ts from cdo_demo_db.snapshots.inputs")
        .upstream(source, 100)
        .own_epoch(Some(200))
        .allow_clones(false);
    let full = snapshot
        .clone()
        .execution_type(ModelExecutionType::Full)
        .unique_id("model.project.result")
        .node_state(|state| state.resource_type = "model".into());
    scenario.build(full, 200).await;
    scenario.expect_execute(snapshot.clone()).await;
    // An unconfirmed snapshot cannot publish successful reusable history.
    scenario.expect_execute(snapshot.clone()).await;
    scenario.build(snapshot.clone(), 200).await;
    scenario.expect_skip(snapshot.clone()).await;
    for unsafe_evidence in [
        snapshot.clone().table(source, None),
        snapshot.clone().upstream(source, 201),
        snapshot.clone().own_epoch(Some(201)),
        snapshot.clone().namespace("another-connection"),
        snapshot.clone().target("cdo_demo_db.snapshots.other"),
    ] {
        scenario.expect_execute(unsafe_evidence).await;
    }
    scenario
        .org("another-organization")
        .expect_execute(snapshot.clone())
        .await;
    scenario.expect_skip(snapshot).await;
}

/// A confirmed same-query snapshot can explain a refused optimization, but
/// cannot provide this absent target's physical result or publish a SKIP.
#[tokio::test]
async fn snapshot_refusal_diagnostic_is_current_and_scoped_without_copying() {
    use dbt_state_proto::query_cache::ModelExecutionType;
    use support::scenario::{model, Scenario};
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let source = "cdo_demo_db.snapshot_diagnostic.inputs";
    for case in [
        "positive",
        "unknown",
        "drift",
        "namespace",
        "other_source",
        "sql",
        "extras",
        "kind",
        "template",
        "dependency",
        "lag",
        "policy",
        "chain",
        "disabled",
        "ignore_external",
        "dialect",
        "counterpart_type",
        "counterpart_pending",
        "latest_changed",
        "producer",
        "org",
    ] {
        let ns = format!("snapshot-refusal-{case}");
        let counterpart = model("cdo_demo_db.snapshot_diagnostic.counterpart")
            .dialect("databricks")
            .catalog("cdo_demo_db")
            .schema("snapshot_diagnostic")
            .namespace(&ns)
            .execution_type(ModelExecutionType::Snapshot)
            .unique_id("snapshot.project.counterpart")
            .node_state(|state| state.resource_type = "snapshot".into())
            .sql("select id,v,p,ts from cdo_demo_db.snapshot_diagnostic.inputs")
            .extra("unique_key", "\"id\"")
            .upstream(source, 100)
            .own_epoch(Some(200))
            .allow_clones(true)
            .clone_chain_depth_limit(0);
        match case {
            "counterpart_pending" => {
                scenario.expect_execute(counterpart.clone()).await;
            }
            "counterpart_type" => {
                scenario
                    .build(
                        counterpart.clone().execution_type(ModelExecutionType::Full),
                        200,
                    )
                    .await;
            }
            _ => {
                scenario.build(counterpart.clone(), 200).await;
            }
        }
        if case == "latest_changed" {
            scenario
                .build(
                    counterpart.clone().sql(
                        "select id,v,p,ts from cdo_demo_db.snapshot_diagnostic.inputs where id=1",
                    ),
                    201,
                )
                .await;
        }
        if case == "producer" {
            scenario
                .build(model(source).dialect("databricks").namespace(&ns), 100)
                .await;
        }
        let mut request = counterpart
            .target("cdo_demo_db.snapshot_diagnostic.result")
            .unique_id("snapshot.project.result")
            .own_epoch(None);
        request = match case {
            "unknown" => request.table(source, None),
            "drift" => request.upstream(source, 101),
            "namespace" => request.namespace("unseen-namespace"),
            "other_source" => request.with(|q| {
                q.tables[0].name = "cdo_demo_db.snapshot_diagnostic.other".into();
                q.sql = "select id,v,p,ts from cdo_demo_db.snapshot_diagnostic.other".into();
            }),
            "sql" => request.sql("select v,id,p,ts from cdo_demo_db.snapshot_diagnostic.inputs"),
            "extras" => request.extra("unique_key", "\"v\""),
            "kind" => request.execution_type(ModelExecutionType::Merge),
            "template" => request.with(|q| {
                q.compare_unrendered_code = true;
                let state = q.dbt_node_state.as_mut().unwrap();
                state.node_body_hash = Some("11111111111111111111111111111111".into());
                state.node_configs_hash = Some("22222222222222222222222222222222".into());
            }),
            "dependency" => request.dependency(source, "select 1"),
            "lag" => request.with(|q| q.freshness_tolerance_seconds = 1),
            "policy" => request.with(|q| q.stale_upstream_policy = 1),
            "chain" => request.clone_chain_depth_limit(1),
            "disabled" => request.allow_clones(false),
            "ignore_external" => request.with(|q| q.ignore_external_modifications = true),
            "dialect" => request.dialect("bigquery"),
            _ => request,
        };
        let decision = if case == "org" {
            scenario.org("foreign-org").expect_execute(request).await
        } else {
            scenario.expect_execute(request).await
        }
        .explained_decision
        .unwrap();
        assert_eq!(
            (
                decision.skip_rejection_reason,
                decision.clone_rejection_reason
            ),
            if case == "positive" {
                (Some(1), Some(3))
            } else {
                (Some(6), Some(6))
            },
            "{case}"
        );
    }
}
