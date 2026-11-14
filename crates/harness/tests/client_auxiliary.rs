//! Pinned-client auxiliary calls, compared at the production public RPC seam.
#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;

#[tokio::test]
async fn pinned_databricks_client_auxiliary_calls_match_hosted() {
    let server = support::start_server().await;
    let channel = support::channel(server.addr).await;
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../golden/fixtures/databricks_client_auxiliary.jsonl");
    let entries = diff::load_golden(fixture.to_str().unwrap()).unwrap();
    assert_eq!(entries.len(), 6);
    assert!(entries
        .iter()
        .any(|entry| entry.method == "ValidateClientVersion"));
    assert!(entries
        .iter()
        .any(|entry| entry.method == "SubmitTelemetryBatch"));
    let events: Vec<_> = entries
        .iter()
        .filter(|entry| entry.method == "SubmitTelemetryBatch")
        .flat_map(|entry| entry.request["events"].as_array().unwrap())
        .collect();
    let ends: Vec<_> = events
        .iter()
        .filter_map(|event| event["request"].get("session_end"))
        .collect();
    assert_eq!(ends.len(), 2);
    assert_eq!(ends[0]["result"], 0);
    assert_eq!(ends[1]["result"], 1);
    assert!(ends.iter().all(|end| end["session_duration"].is_object()));
    assert!(events
        .iter()
        .any(|event| event["request"].get("enriched_sql_prepared").is_some()));
    for (index, entry) in entries.iter().enumerate() {
        let outcome = diff::replay_rpc(channel.clone(), entry).await.unwrap();
        entry
            .compare_outcome(&outcome)
            .unwrap_or_else(|error| panic!("auxiliary entry {index} {}: {error}", entry.method));
        // Status, field presence and application metadata stay observable;
        // only evidenced hosted ingress headers are omitted by comparison.
        let mut wrong = entry.clone();
        wrong.response = serde_json::json!({});
        assert!(wrong.compare_outcome(&outcome).is_err());
        wrong = entry.clone();
        wrong.status.as_mut().unwrap().code = 13;
        assert!(wrong.compare_outcome(&outcome).is_err());
        wrong = entry.clone();
        wrong
            .response_metadata
            .as_mut()
            .unwrap()
            .ascii
            .push(("x-application-evidence".into(), "changed".into()));
        assert!(wrong.compare_outcome(&outcome).is_err());
    }
}
