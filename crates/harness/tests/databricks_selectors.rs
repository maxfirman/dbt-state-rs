//! Native dbt 2.0.11 Databricks selectors, including their complete baseline.
mod support;

use std::collections::{BTreeSet, HashMap};

use dbt_state_harness::diff::{load_golden, replay_rpc};

#[tokio::test]
async fn native_databricks_selector_history_matches_complete_hosted_responses() {
    let entries = load_golden(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/databricks_selectors156.jsonl"
    ))
    .expect("load complete native selector history");
    let server = support::start_server().await;
    let channel = support::channel(server.addr).await;
    let mut request_ids: HashMap<String, String> = HashMap::new();
    let mut criteria = BTreeSet::new();
    let mut selections = 0;
    let mut submits = 0;
    let mut confirms = 0;
    for (index, mut entry) in entries.into_iter().enumerate() {
        if entry.method == "ConfirmExecution" {
            let id = entry.request["request_id"].as_str().unwrap();
            let local = request_ids.get(id).expect("confirmation has causal submit");
            entry.request["request_id"] = serde_json::Value::String(local.clone());
            entry.response["request_id"] = serde_json::Value::String(local.clone());
            confirms += 1;
        }
        let actual = replay_rpc(channel.clone(), &entry).await.unwrap();
        if entry.method == "SubmitEnrichedSQL" {
            submits += 1;
            // Correlate only the server-minted IDs. All other response fields,
            // statuses and protocol metadata remain exact hosted expectations.
            let expected = entry.response["response"].as_object_mut().unwrap();
            assert_eq!(expected.len(), 1);
            for (variant, fields) in expected {
                let fields = fields.as_object_mut().unwrap();
                let received = actual.response["response"][variant].as_object().unwrap();
                for key in ["request_id", "execution_decision_id"] {
                    if let Some(old) = fields.get(key) {
                        let new = received.get(key).expect("same response ID presence");
                        if key == "request_id" {
                            request_ids.insert(
                                old.as_str().unwrap().to_string(),
                                new.as_str().unwrap().to_string(),
                            );
                        }
                        fields.insert(key.to_string(), new.clone());
                    }
                }
            }
        }
        if entry.method == "GetStateSelection" {
            selections += 1;
            criteria.insert(entry.request["selector_criteria"].as_i64().unwrap());
        }
        entry
            .compare_outcome(&actual)
            .unwrap_or_else(|error| panic!("entry {index} {}: {error}", entry.method));
    }
    assert_eq!(criteria, (1..=10).collect());
    assert_eq!(submits, 4, "first build and unchanged run, both nodes");
    assert_eq!(confirms, 2, "every initial build confirmed");
    assert_eq!(
        selections, 102,
        "all criteria at each change plus graph closure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned dbt and Databricks credentials; writes exactly two fresh demo tables"]
async fn real_dbt_selectors_use_rust_state() {
    let server = support::start_server().await;
    let work = std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("artifact directory");
    let output = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../experiments/databricks_selectors.py"
        ))
        .arg("--archive")
        .arg(
            std::env::var_os("DBT_STATE_DATABRICKS_ARCHIVE")
                .expect("retained native profile archive"),
        )
        .arg("--work-dir")
        .arg(work)
        .arg("--dbt")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_DBT").expect("pinned client"))
        .arg("--proxy")
        .arg(env!("CARGO_BIN_EXE_record-proxy"))
        .arg("--schema")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA").expect("owned demo schema"))
        .arg("--run-id")
        .arg(std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID").expect("fresh run UUID"))
        .arg("--local-upstream")
        .arg(format!("http://{}", server.addr))
        .output()
        .await
        .expect("run native selector campaign");
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
