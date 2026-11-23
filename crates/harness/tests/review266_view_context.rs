#[path = "support.rs"]
mod support;

use support::demo::{self, transient_at, NS};
use support::scenario::Scenario;
use support::start_server;

// CURRENT_SCHEMA() inside a Snowflake view returns the view's owning schema.
#[tokio::test]
async fn a_view_returning_its_schema_cannot_be_copied_across_schemas() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |schema: &str| {
        let view = demo::relation(schema, "V");
        demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
            .namespace(NS)
            .sql(&format!("select v from {view}"))
            .dependency_in(
                &view,
                "create view V as select current_schema() as v from CDO_DEMO_DB.RAW.U",
                demo::CATALOG,
                schema,
            )
            .upstream(&demo::relation("RAW", "U"), 100)
            .tolerance_seconds(0)
    };
    sc.build_with(submit("PROD"), transient_at(500)).await;
    sc.expect_execute(submit("DEV")).await;
}

// Inside SHARED.V the unqualified U resolves in SHARED, even when another
// schema's U is also present. The fully qualified root reads the same view.
#[tokio::test]
async fn two_names_inside_one_shared_view_can_still_prove_the_same_read() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |target: &str| {
        let view = demo::relation("SHARED", "V");
        demo::model(&demo::relation("ZZZ", target), "ZZZ", "model.p.m")
            .namespace(NS)
            .sql(&format!("select v from {view}"))
            .dependency_in(
                &view,
                "create view V as select local.v from U local cross join OTHER.U o",
                demo::CATALOG,
                "SHARED",
            )
            .upstream(&demo::relation("SHARED", "U"), 100)
            .upstream(&demo::relation("OTHER", "U"), 200)
            .tolerance_seconds(0)
    };
    sc.build_with(submit("PROD_M"), transient_at(500)).await;
    sc.expect_clone(submit("DEV_M")).await;
}

// The source was last modified at 90s. A build at 100s reads as of 40s,
// while a request at 160s reads as of 100s, across that modification.
#[tokio::test]
async fn relative_time_travel_reads_different_versions_at_different_times() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |schema: &str| {
        demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
            .namespace(NS)
            .sql("select v from CDO_DEMO_DB.RAW.U at (offset => -60)")
            .upstream(&demo::relation("RAW", "U"), 90000)
            .tolerance_seconds(0)
    };
    sc.at(100000)
        .build_with(submit("PROD"), transient_at(100000))
        .await;
    sc.at(160000).expect_execute(submit("DEV")).await;
}
