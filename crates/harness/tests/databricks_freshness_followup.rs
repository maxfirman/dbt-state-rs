//! Public freshness safety controls supplement the complete hosted replay.
mod support;

use support::scenario::{model, Scenario};

const TARGET: &str = "`cdo_demo_db`.`freshness`.`consumer`";
const SOURCE: &str = "`cdo_demo_db`.`freshness`.`source`";

#[tokio::test]
async fn identical_known_current_upstream_reuses_a_unique_recorded_read() {
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let request = model(TARGET)
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("freshness")
        .namespace("freshness")
        .sql("select id from `cdo_demo_db`.`freshness`.`source`")
        .own_epoch(Some(200))
        .upstream(SOURCE, 100)
        .allow_clones(false);
    scenario.build(request.clone(), 200).await;
    scenario.expect_skip(request.clone()).await;
    scenario
        .expect_skip(request.with(|q| q.tables.push(q.tables[1].clone())))
        .await;
}

#[tokio::test]
async fn ambiguous_current_or_recorded_evidence_never_reuses() {
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let request = model(TARGET)
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("freshness")
        .namespace("freshness")
        .sql("select id from `cdo_demo_db`.`freshness`.`source`")
        .own_epoch(Some(200))
        .upstream(SOURCE, 100)
        .allow_clones(false);
    scenario.build(request.clone(), 200).await;
    for epochs in [
        [Some(100), Some(101)],
        [Some(101), Some(100)],
        [Some(100), None],
        [None, Some(100)],
        [None, None],
    ] {
        scenario
            .expect_execute(request.clone().with(|q| {
                q.tables[1].last_modified_epoch = epochs[0];
                let mut other = q.tables[1].clone();
                other.last_modified_epoch = epochs[1];
                q.tables.push(other);
            }))
            .await;
    }
    // Alias equivalence alone never makes two raw evidence entries identical.
    scenario
        .expect_execute(request.clone().with(|q| {
            let mut other = q.tables[1].clone();
            other.name = "CDO_DEMO_DB.FRESHNESS.SOURCE".into();
            q.tables.push(other);
        }))
        .await;
    scenario
        .expect_execute(request.clone().with(|q| q.tables.push(q.tables[0].clone())))
        .await;
    scenario
        .expect_execute(request.clone().with(|q| {
            q.tables.remove(1);
        }))
        .await;
    scenario.expect_skip(request.clone()).await;
    // A rebuild with redundant current evidence persists the uncollapsed wire
    // reads; neither this new history nor old recorded duplicates prove reuse.
    let changed = request.clone().upstream(SOURCE, 101);
    scenario
        .build(
            changed.clone().with(|q| q.tables.push(q.tables[1].clone())),
            201,
        )
        .await;
    scenario.expect_execute(changed).await;
}

/// Every response/status/metadata remains exact; only opaque correlation IDs
/// are mapped. UUIDv7 gives the captured submit instant within request/response
/// bounds, as in the strict generic Databricks replay gate.
async fn exact_history(fixture: &str) {
    use dbt_state_harness::diff;
    use std::collections::HashMap;
    let server = support::start_server().await;
    let channel = support::channel(server.addr).await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../golden/fixtures")
        .join(fixture);
    let entries = diff::load_golden(path.to_str().unwrap()).unwrap();
    let mut ids: HashMap<String, String> = HashMap::new();
    for (index, mut entry) in entries.into_iter().enumerate() {
        fn remap(value: &mut serde_json::Value, ids: &HashMap<String, String>) {
            match value {
                serde_json::Value::String(s) => {
                    if let Some(local) = ids.get(s) {
                        *s = local.clone();
                    }
                }
                serde_json::Value::Array(a) => {
                    for v in a {
                        remap(v, ids);
                    }
                }
                serde_json::Value::Object(o) => {
                    for v in o.values_mut() {
                        remap(v, ids);
                    }
                }
                _ => {}
            }
        }
        remap(&mut entry.request, &ids);
        if entry.method == "SubmitEnrichedSQL" {
            let hosted = entry.response["response"]
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap();
            let at = (uuid::Uuid::parse_str(hosted["execution_decision_id"].as_str().unwrap())
                .unwrap()
                .as_u128()
                >> 80) as i64;
            let submitted = entry
                .metadata
                .iter()
                .find(|(k, _)| k == "x-submitted-at-epoch")
                .unwrap()
                .1
                .parse::<i64>()
                .unwrap();
            assert!(at >= submitted && at <= entry.recorded_at_ms().unwrap());
            entry.recorded_at = Some(
                chrono::DateTime::from_timestamp_millis(at)
                    .unwrap()
                    .to_rfc3339(),
            );
        }
        let actual = diff::replay_rpc(channel.clone(), &entry).await.unwrap();
        if entry.method == "SubmitEnrichedSQL" {
            let hosted = entry.response["response"]
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap();
            let local = actual.response["response"]
                .as_object()
                .unwrap()
                .values()
                .next()
                .unwrap();
            for key in ["request_id", "execution_decision_id"] {
                if let Some(id) = hosted[key].as_str() {
                    ids.insert(
                        id.into(),
                        local[key].as_str().expect("present correlation ID").into(),
                    );
                }
            }
        }
        remap(&mut entry.response, &ids);
        entry
            .compare_outcome(&actual)
            .unwrap_or_else(|error| panic!("{fixture} entry {index}: {error}"));
    }
}

#[tokio::test]
async fn missing_current_epoch_explain_matches_complete_frozen_hosted_history() {
    exact_history("databricks_freshness_missing_current.jsonl").await;
}

#[tokio::test]
async fn both_missing_epochs_explain_matches_complete_frozen_hosted_history() {
    exact_history("databricks_freshness_both_missing.jsonl").await;
}

#[tokio::test]
async fn duplicate_normalization_preserves_physical_scope_and_missing_epoch_floors() {
    use support::scenario::Policy;
    let server = support::start_server().await;
    let scenario = Scenario::connect(&server).await;
    let request = model(TARGET)
        .dialect("databricks")
        .catalog("cdo_demo_db")
        .schema("freshness")
        .namespace("freshness")
        .sql("select id from `cdo_demo_db`.`freshness`.`source`")
        .own_epoch(Some(200))
        .upstream(SOURCE, 100)
        .allow_clones(false);
    scenario.build(request.clone(), 200).await;
    let duplicate = request.clone().with(|q| q.tables.push(q.tables[1].clone()));
    scenario.expect_skip(duplicate.clone()).await;
    scenario
        .org("another-org")
        .expect_execute(duplicate.clone())
        .await;
    for other in [
        duplicate.clone().namespace("another-connection"),
        duplicate.clone().dialect("snowflake"),
        duplicate.clone().target("`cdo_demo_db`.`other`.`consumer`"),
    ] {
        scenario.expect_execute(other).await;
    }
    scenario.expect_execute(duplicate.own_epoch(None)).await;
    // Display defaults may exist for the current submit, but cannot prove
    // freshness under ALL or while a build would otherwise be within tolerance.
    let tolerant = request.clone().with(|q| {
        q.freshness_tolerance_seconds = i64::MAX / 1000;
        q.stale_upstream_policy = Policy::All as i32;
        q.tables[1].last_modified_epoch = None;
    });
    scenario.expect_execute(tolerant).await;
    scenario
        .build(
            request
                .clone()
                .with(|q| q.tables[1].last_modified_epoch = None),
            201,
        )
        .await;
    scenario.expect_execute(request.clone()).await;
    scenario
        .expect_execute(request.with(|q| q.tables[1].last_modified_epoch = None))
        .await;
}

#[tokio::test]
async fn identity_changed_explain_uses_submit_clock_without_a_reuse_anchor() {
    exact_history("databricks_explain_identity_submit_clock.jsonl").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned client and Databricks credentials; writes fresh owned relations in leased demo schemas"]
async fn real_dbt_repeated_source_and_crafted_duplicate_match_independent_results() {
    let server = support::start_server().await;
    let artifacts = std::path::PathBuf::from(
        std::env::var_os("DBT_STATE_DATABRICKS_ARTIFACTS").expect("fresh evidence directory"),
    );
    let output = tokio::process::Command::new(
        std::env::var_os("DBT_STATE_DATABRICKS_PYTHON").expect("Python with PyYAML"),
    )
    .arg(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../experiments/databricks_freshness_followup.py"
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
    .arg("--run-id")
    .arg(std::env::var_os("DBT_STATE_DATABRICKS_RUN_ID").expect("unique owned run prefix"))
    .arg("--schema-lease")
    .arg(std::env::var_os("DBT_STATE_DATABRICKS_SCHEMA_LEASE").expect("owned schema lease"))
    .output()
    .await
    .expect("run opt-in live campaign");
    assert!(
        output.status.success(),
        "{}\n{}\n{}",
        artifacts.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
