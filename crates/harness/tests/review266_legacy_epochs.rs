#[path = "support.rs"]
mod support;

use support::scenario::{environment_model, Scenario};
use support::{legacy, start_server};

// This fixture was exported after three confirmed writes via public RPCs
// on main (7270338): a build, RegisterClone, and a submit copy.
#[tokio::test]
async fn a_legacy_copy_with_conflicting_source_epochs_requires_a_build() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let history: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/review266_legacy_epochs.json")).unwrap();
    for row in history {
        let (before, after) = legacy::import_complete_execution(&server, &row).await;
        assert_eq!(before, after);
    }
    let dev = environment_model("dev")
        .no_tables()
        .sql("select v from DB.RAW.U")
        .upstream("\"DB\".\"RAW\".\"U\"", 100)
        .tolerance_seconds(0)
        .own_epoch(Some(700));
    sc.expect_execute(dev).await;
}
