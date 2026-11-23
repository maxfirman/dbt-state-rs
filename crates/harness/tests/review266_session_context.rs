#[path = "support.rs"]
mod support;

use support::demo::{self, transient_at, NS};
use support::scenario::{Scenario, Submit};
use support::start_server;

fn shared_input(schema: &str, sql: &str) -> Submit {
    demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
        .sql(sql)
        .namespace(NS)
        .upstream(&demo::relation("RAW", "U"), 100)
        .tolerance_seconds(0)
        .tolerate_nondeterminism(false)
}

#[tokio::test]
async fn a_database_only_projection_can_copy_across_schemas_in_one_database() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let sql = "select current_database() as v from CDO_DEMO_DB.RAW.U";
    sc.build_with(shared_input("PROD", sql), transient_at(500))
        .await;
    sc.expect_clone(shared_input("DEV", sql)).await;
}

#[tokio::test]
async fn a_schema_projection_with_no_reported_session_schema_lacks_evidence() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |schema| {
        let mut req = shared_input(
            schema,
            "select current_schema() as v from CDO_DEMO_DB.RAW.U",
        )
        .build();
        req.default_schema = None;
        req
    };
    sc.build_with(submit("PROD"), transient_at(500)).await;
    sc.expect_execute(submit("DEV")).await;
}

#[tokio::test]
async fn the_sys_context_schema_property_observes_the_session_schema() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let sql = "select sys_context('SNOWFLAKE$SESSION', 'SCHEMA') as v from CDO_DEMO_DB.RAW.U";
    sc.build_with(shared_input("PROD", sql), transient_at(500))
        .await;
    sc.expect_execute(shared_input("DEV", sql)).await;
}
