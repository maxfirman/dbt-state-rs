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
    assert!(entries
        .iter()
        .any(|entry| entry.method == "ValidateClientVersion"));
    assert!(entries
        .iter()
        .any(|entry| entry.method == "SubmitTelemetryBatch"));
    for (index, entry) in entries.iter().enumerate() {
        let outcome = diff::replay_rpc(channel.clone(), entry).await.unwrap();
        entry
            .compare_outcome(&outcome)
            .unwrap_or_else(|error| panic!("auxiliary entry {index} {}: {error}", entry.method));
    }
}
