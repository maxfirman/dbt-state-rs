//! Bounded independent Databricks producer/policy acceptance.
mod support;

#[test]
fn policy_reference_rejects_wrong_snapshot_and_lossy_rows() {
    let output = std::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_policy_test.py"
        ))
        .output()
        .expect("run independent policy snapshot fault probes");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned client, explicit clearance and four fresh leased demo relations"]
async fn real_dbt_policy_matches_saved_and_fresh_independent_references() {
    let server = support::start_server().await;
    let artifacts =
        std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh retained artifact path");
    let mut command = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    );
    command
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_policy_actual.py"
        ))
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
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("owned schema lease"))
        .arg("--live-clearance")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_LIVE_CLEARANCE")
                .expect("explicit exact phase clearance"),
        )
        .arg("--run-id")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID").expect("exact cleared run ID"))
        .arg("--query-identity")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_QUERY_IDENTITY")
                .expect("shared independent literal identity"),
        )
        .arg("--contract")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_POLICY_CONTRACT")
                .expect("prior captured hosted contract"),
        );
    let output = command.output().await.expect("run bounded policy campaign");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "artifacts: {:?}\n{}\n{}",
        artifacts,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

// Crafted local boundary controls. Exact T equality below pins the conservative
// local contract; it is not an assertion that a hosted decision occurred at T.
use dbt_state_proto::query_cache as qc;
use support::scenario::{built_at, into_execute, into_skip, model, Policy, Scenario};

async fn at_time(
    scenario: &Scenario,
    request: qc::SubmitEnrichedSqlRequest,
    at: i64,
) -> qc::SubmitSqlResponse {
    let mut request = tonic::Request::new(request);
    request.metadata_mut().insert(
        dbt_state_server::REPLAY_NOW_HEADER,
        at.to_string().parse().unwrap(),
    );
    qc::sql_client::SqlClient::new(scenario.channel())
        .submit_enriched_sql(request)
        .await
        .unwrap()
        .into_inner()
}

fn crafted_model(name: &str) -> support::scenario::Submit {
    model(name)
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("policy")
        .namespace("crafted-policy")
        .allow_clones(false)
}

#[tokio::test]
async fn crafted_local_clock_bounds_do_not_advance_build_anchors_on_skip() {
    const A: &str = "`cdo_demo_db`.`policy`.`a`";
    const B: &str = "`cdo_demo_db`.`policy`.`b`";
    const C: &str = "`cdo_demo_db`.`policy`.`consumer`";
    for policy in [Policy::Any, Policy::All] {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let a = crafted_model(A).sql("select 1 id");
        let b = crafted_model(B).sql("select 2 id");
        scenario.build(a.clone(), 1_000).await;
        scenario.build(b.clone(), 1_000).await;
        let consumer = crafted_model(C)
            .sql(
                "select id from cdo_demo_db.policy.a union all select id from cdo_demo_db.policy.b",
            )
            .upstream(A, 1_000)
            .upstream(B, 1_000)
            .tolerance_seconds(60)
            .policy(policy);
        scenario.build(consumer.clone(), 10_000).await;
        // Same-logic producer Record, through the public publication seam.
        scenario.record([a.record(built_at(20_000))]).await.unwrap();
        let partial = consumer.clone().own_epoch(Some(10_000)).upstream(A, 20_000);
        into_skip(at_time(&scenario, partial.clone().build(), 69_999).await);
        if policy == Policy::Any {
            into_execute(at_time(&scenario, partial.clone().build(), 70_000).await);
            into_execute(at_time(&scenario, partial.clone().build(), 70_001).await);
        } else {
            into_skip(at_time(&scenario, partial.clone().build(), 70_000).await);
            into_skip(at_time(&scenario, partial.clone().build(), 70_001).await);
        }
        let zero = partial.clone().tolerance_seconds(0);
        if policy == Policy::Any {
            into_execute(at_time(&scenario, zero.build(), 69_999).await);
        } else {
            into_skip(at_time(&scenario, zero.build(), 69_999).await);
        }
        // The earlier partial SKIPs did not become new read anchors.
        scenario.record([b.record(built_at(21_000))]).await.unwrap();
        let both = partial.clone().upstream(B, 21_000);
        into_skip(at_time(&scenario, both.clone().build(), 69_999).await);
        into_execute(at_time(&scenario, both.clone().build(), 70_000).await);
        into_execute(at_time(&scenario, both.clone().build(), 70_001).await);
        into_execute(at_time(&scenario, both.clone().tolerance_seconds(0).build(), 69_999).await);
        // Hold the consumer's SQL/dependencies fixed to isolate producer logic.
        // Actual-client changed dependency definitions are audited separately.
        let changed = a.sql("select 3 id");
        scenario
            .record([changed.record(built_at(25_000))])
            .await
            .unwrap();
        let logic = both.upstream(A, 25_000);
        into_execute(at_time(&scenario, logic.clone().build(), 69_999).await);
        into_execute(at_time(&scenario, logic.build(), 70_001).await);
    }
}

#[tokio::test]
async fn bounded_record_traces_preserve_same_logic_reuse_across_policy_switches() {
    // Eight worked public histories vary physical names, clocks and literals.
    // Expected results follow the declared operations, not fingerprint helpers.
    for seed in 0..8_i64 {
        let server = support::start_server().await;
        let scenario = Scenario::connect(&server).await;
        let a_name = format!("`cdo_demo_db`.`policy`.`a_{seed}`");
        let b_name = format!("`cdo_demo_db`.`policy`.`b_{seed}`");
        let c_name = format!("`cdo_demo_db`.`policy`.`c_{seed}`");
        let base = 100_000 + seed * 100_000;
        let a = crafted_model(&a_name).sql(&format!("select {seed} id"));
        let b = crafted_model(&b_name).sql("select 100 id");
        scenario.build(a.clone(), base).await;
        scenario.build(b.clone(), base).await;
        let policy = if seed % 2 == 0 {
            Policy::Any
        } else {
            Policy::All
        };
        let other = if policy == Policy::Any {
            Policy::All
        } else {
            Policy::Any
        };
        let consumer = crafted_model(&c_name)
            .sql(&format!(
                "select id from {a_name} union all select id from {b_name}"
            ))
            .upstream(&a_name, base)
            .upstream(&b_name, base)
            .own_epoch(Some(base + 1_000))
            .tolerance_seconds(60)
            .policy(policy);
        scenario.build(consumer.clone(), base + 1_000).await;
        into_skip(at_time(&scenario, consumer.clone().build(), base + 3_000).await);
        scenario
            .record([a.record(built_at(base + 2_000))])
            .await
            .unwrap();
        let partial = consumer.clone().upstream(&a_name, base + 2_000);
        into_skip(at_time(&scenario, partial.clone().build(), base + 3_000).await);
        let switched = partial.policy(other);
        into_skip(at_time(&scenario, switched.clone().build(), base + 3_001).await);
        let changed = a.sql(&format!("select {} id", seed + 1));
        scenario
            .record([changed.record(built_at(base + 2_500))])
            .await
            .unwrap();
        let logic = switched.upstream(&a_name, base + 2_500);
        into_execute(at_time(&scenario, logic.clone().build(), base + 3_002).await);
        // A proposed BUILD is not a confirmed new baseline.
        into_execute(at_time(&scenario, logic.build(), base + 3_003).await);
    }
}

#[test]
fn captured_actual_policy_matrix_has_complete_history_and_isolated_producer_changes() {
    use dbt_state_harness::diff;
    let entries = diff::load_golden(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/databricks_producer_policy.jsonl"
    ))
    .unwrap();
    assert_eq!(entries.len(), 261);
    assert_eq!(
        entries
            .iter()
            .filter(|e| e.method == "ConfirmExecution")
            .count(),
        11
    );
    assert_eq!(
        entries
            .iter()
            .filter(|e| e.method == "RecordExecutions")
            .count(),
        19
    );
    let contract: serde_json::Value = serde_json::from_str(include_str!(
        "../../../experiments/databricks_policy_contract.json"
    ))
    .unwrap();
    for cell in contract["cells"].as_array().unwrap() {
        let entry = &entries[cell["entry"].as_u64().unwrap() as usize];
        assert_eq!(entry.method, "SubmitEnrichedSQL");
        assert_eq!(entry.request["execution_type"], 1);
        assert_eq!(entry.request["compare_unrendered_code"], false);
        assert_eq!(entry.request["freshness_tolerance_seconds"], 180);
        assert_eq!(entry.request["stale_upstream_policy"], cell["policy"]);
        let variant = diff::decision_variant(&entry.response).unwrap();
        assert_eq!(variant, cell["decision"].as_str().unwrap());
        assert_eq!(
            entry.response["response"][&variant]["explained_decision"],
            cell["explained_decision"]
        );
        let label = cell["label"].as_str().unwrap();
        if label.contains("changed_logic") {
            assert_eq!(variant, "ready_to_execute");
            assert_eq!(cell["consumer_delta_fields"], serde_json::json!(["tables"]));
        }
    }
    let cells = contract["cells"].as_array().unwrap();
    let get = |label: &str| cells.iter().find(|c| c["label"] == label).unwrap();
    for policy in ["any", "all"] {
        for drift in ["partial", "both"] {
            assert_eq!(
                get(&format!("{policy}_{drift}_inside"))["decision"],
                "skip_execution"
            );
            assert_eq!(
                get(&format!("{policy}_{drift}_inside"))["explained_decision"]["is_stale"],
                true
            );
        }
        assert_eq!(
            get(&format!("{policy}_both_outside"))["decision"],
            "ready_to_execute"
        );
        assert_eq!(
            get(&format!("{policy}_logic_outside_unchanged"))["decision"],
            "skip_execution"
        );
    }
    assert_eq!(get("any_partial_outside")["decision"], "ready_to_execute");
    assert_eq!(get("all_partial_outside")["decision"], "skip_execution");
    assert_eq!(
        get("all_partial_outside")["explained_decision"]["is_stale"],
        true
    );
    assert_eq!(
        get("all_b_after_partial_skips")["decision"],
        "ready_to_execute"
    );
}

#[tokio::test]
async fn captured_producer_change_explain_rounds_the_recorded_input_gap() {
    use dbt_state_harness::diff;
    let entries = diff::load_golden(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/databricks_producer_policy.jsonl"
    ))
    .unwrap();
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    // Exact captured A/B publications, consumer BUILD snapshot and changed A.
    // Retain both consumed producer histories; no raw State writes or guessed
    // fingerprints. Full-corpus replay separately retains every original RPC.
    for index in [77, 81, 98, 108] {
        let request: qc::RecordExecutionsRequest =
            serde_json::from_value(entries[index].request.clone()).unwrap();
        assert_eq!(scenario.record(request.records).await.unwrap(), 1);
    }
    let submit = &entries[113];
    let hosted_id = submit.response["response"]["ready_to_execute"]["execution_decision_id"]
        .as_str()
        .unwrap();
    let id = uuid::Uuid::parse_str(hosted_id).unwrap();
    assert_eq!(id.get_version_num(), 7);
    let at = (id.as_u128() >> 80) as i64;
    let request: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(submit.request.clone()).unwrap();
    let ready = into_execute(at_time(&scenario, request, at).await);
    let mut explain = qc::explain_client::ExplainClient::new(scenario.channel());
    let mut message = None;
    for _ in 0..100 {
        let response = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![ready.execution_decision_id.clone().unwrap()],
            })
            .await
            .unwrap()
            .into_inner();
        if let Some(entry) = response.messages.into_iter().next() {
            message = Some(entry);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let mut actual = serde_json::to_value(message.expect("captured decision Explain")).unwrap();
    let mut expected = entries[116].response["messages"][0].clone();
    actual
        .as_object_mut()
        .unwrap()
        .remove("execution_decision_id");
    expected
        .as_object_mut()
        .unwrap()
        .remove("execution_decision_id");
    assert_eq!(
        actual, expected,
        "complete primary Explain after changed producer"
    );
}

#[tokio::test]
async fn captured_gap_display_boundaries_preserve_seconds_even_ties_and_hour_carry() {
    use dbt_state_harness::diff;
    let mut entries = diff::load_golden(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/databricks_gap_duration.jsonl"
    ))
    .unwrap();
    assert_eq!(entries.len(), 102);
    entries.extend(
        diff::load_golden(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../golden/fixtures/databricks_relative_hour_duration.jsonl"
        ))
        .unwrap(),
    );
    assert_eq!(entries.len(), 138);
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    for triple in entries.chunks_exact(3) {
        assert_eq!(triple[0].method, "RecordExecutions");
        let record: qc::RecordExecutionsRequest =
            serde_json::from_value(triple[0].request.clone()).unwrap();
        assert_eq!(scenario.record(record.records).await.unwrap(), 1);
        assert_eq!(triple[1].method, "SubmitEnrichedSQL");
        let variant = diff::decision_variant(&triple[1].response).unwrap();
        let hosted = &triple[1].response["response"][&variant];
        let id = uuid::Uuid::parse_str(hosted["execution_decision_id"].as_str().unwrap()).unwrap();
        assert_eq!(id.get_version_num(), 7);
        let at = (id.as_u128() >> 80) as i64;
        let request: qc::SubmitEnrichedSqlRequest =
            serde_json::from_value(triple[1].request.clone()).unwrap();
        let actual = serde_json::to_value(at_time(&scenario, request, at).await).unwrap();
        let local_id = actual["response"][&variant]["execution_decision_id"]
            .as_str()
            .unwrap();
        let mut explain = qc::explain_client::ExplainClient::new(scenario.channel());
        let mut message = None;
        for _ in 0..100 {
            let response = explain
                .get_explain_messages(qc::GetExplainMessagesRequest {
                    execution_decision_ids: vec![local_id.into()],
                })
                .await
                .unwrap()
                .into_inner();
            if let Some(entry) = response.messages.into_iter().next() {
                message = Some(entry);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let mut actual = serde_json::to_value(message.expect("captured boundary Explain")).unwrap();
        let mut expected = triple[2].response["messages"][0].clone();
        actual
            .as_object_mut()
            .unwrap()
            .remove("execution_decision_id");
        expected
            .as_object_mut()
            .unwrap()
            .remove("execution_decision_id");
        assert_eq!(actual, expected, "full crafted primary boundary tree");
    }
}
