//! Capture outcomes replay through the production router, without hosted access.
#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;

#[tokio::test]
async fn replay_compares_success_and_rejection_without_erasing_status() {
    let server = support::start_server().await;
    let channel = support::channel(server.addr).await;
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../golden/fixtures/rpc_success_and_rejection.jsonl");
    let entries = diff::load_golden(fixture.to_str().unwrap()).unwrap();
    let success = &entries[0];
    let rejection = &entries[1];
    let outcome = diff::replay_rpc(channel.clone(), success).await.unwrap();
    success.compare_outcome(&outcome).unwrap();
    let outcome = diff::replay_rpc(channel, rejection).await.unwrap();
    rejection.compare_outcome(&outcome).unwrap();
    let mut wrong = rejection.clone();
    wrong.status.as_mut().unwrap().message = "another error".into();
    assert!(wrong.compare_outcome(&outcome).is_err());
    wrong = rejection.clone();
    wrong.status.as_mut().unwrap().details = vec![42];
    assert!(wrong.compare_outcome(&outcome).is_err());
    assert!(
        success.compare_outcome(&outcome).is_err(),
        "a failure cannot equal a successful response"
    );
}

#[test]
fn incomplete_campaigns_fail_offline_validation() {
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../experiments/capture_integrity_test.py");
    let output = std::process::Command::new("python3")
        .arg(script)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
