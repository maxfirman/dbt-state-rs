#[path = "support.rs"]
mod support;

use support::demo::{self, transient_at, NS};
use support::scenario::{Scenario, Submit};
use support::start_server;

fn model(schema: &str, sql: &str) -> Submit {
    demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
        .sql(sql)
        .namespace(NS)
        .upstream(&demo::relation("RAW", "U"), 100)
        .tolerance_seconds(0)
        .tolerate_nondeterminism(false)
}

#[tokio::test]
async fn a_known_database_does_not_establish_the_warehouse_session_id() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let prod = sc.invocation("00000000-0000-4000-8000-000000000001", "prod-session");
    let dev = sc.invocation("00000000-0000-4000-8000-000000000002", "dev-session");
    let sql = "select current_database() as db, current_session() as sid from CDO_DEMO_DB.RAW.U";
    prod.build_with(model("PROD", sql), transient_at(500)).await;
    dev.expect_execute(model("DEV", sql)).await;
}

#[tokio::test]
async fn a_known_view_database_does_not_establish_its_owner_role() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |schema: &str| {
        let view = demo::relation(schema, "V");
        model(schema, &format!("select db, owner_role from {view}")).dependency_in(
            &view,
            "create view V as select current_database() as db, invoker_role() as owner_role from CDO_DEMO_DB.RAW.U",
            demo::CATALOG,
            schema,
        )
    };
    sc.build_with(submit("PROD"), transient_at(500)).await;
    sc.expect_execute(submit("DEV")).await;
}

#[tokio::test]
async fn a_dollar_quoted_database_property_is_still_a_constant() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let sql = "select sys_context('SNOWFLAKE$SESSION', $$DATABASE$$) as db from CDO_DEMO_DB.RAW.U";
    sc.build_with(model("PROD", sql), transient_at(500)).await;
    sc.expect_clone(model("DEV", sql)).await;
}

#[tokio::test]
async fn organization_group_membership_is_session_context_for_a_result_copy() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let sql =
        "select is_organization_user_group_in_session('ANALYST') as member from CDO_DEMO_DB.RAW.U";
    sc.build_with(model("PROD", sql), transient_at(500)).await;
    sc.expect_execute(model("DEV", sql)).await;
}

#[tokio::test]
async fn both_spellings_of_a_session_variable_require_a_build() {
    for expression in ["getvariable('MARKER')", "session_context('MARKER')"] {
        let server = start_server().await;
        let sc = Scenario::connect(&server).await;
        let sql = format!("select {expression} as marker from CDO_DEMO_DB.RAW.U");
        sc.build_with(model("PROD", &sql), transient_at(500)).await;
        assert_eq!(
            sc.verdict(model("DEV", &sql)).await,
            "ready_to_execute",
            "{expression}"
        );
    }
}
