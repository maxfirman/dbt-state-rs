#[path = "support.rs"]
mod support;

use support::demo::{self, transient_at, NS};
use support::scenario::Scenario;
use support::start_server;

#[tokio::test]
async fn identical_shared_read_keeps_its_copy_after_identifier_case_changes() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |target: &str, projection: &str| {
        demo::model(&demo::relation("ZZZ", target), "ZZZ", "model.p.m")
            .sql(&format!("select {projection} from CDO_DEMO_DB.AAA.FIXED"))
            .namespace(NS)
            .upstream(&demo::relation("AAA", "FIXED"), 100)
            .tolerance_seconds(0)
    };
    sc.build_with(submit("PROD_M", "v"), transient_at(500))
        .await;
    sc.expect_clone(submit("DEV_M", "V")).await;
}

#[tokio::test]
async fn failed_stale_copy_records_the_inputs_of_the_fallback_build() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.at(3_000);
    let submit = |schema: &str, epoch| {
        demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
            .namespace(NS)
            .sql("select v from CDO_DEMO_DB.AAA.FIXED")
            .upstream(&demo::relation("AAA", "FIXED"), epoch)
    };
    sc.build_with(
        submit("PROD", 1000).tolerance_seconds(0),
        transient_at(2000),
    )
    .await;
    let grant = sc
        .expect_clone(submit("DEV", 2100).tolerance_seconds(60))
        .await;
    assert!(
        sc.confirm(&grant.request_id, transient_at(2500).failed_to_clone())
            .await
    );
    sc.expect_skip(
        submit("DEV", 2100)
            .own_epoch(Some(2500))
            .tolerance_seconds(0),
    )
    .await;
}

#[tokio::test]
async fn physical_binding_preserves_the_declared_columns_of_a_view() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |catalog: &str| {
        let target = format!("{catalog}.S.M");
        demo::model(&target, "S", "model.p.m")
            .catalog(catalog)
            .namespace(NS)
            .sql("select local.x from V local cross join PROD_DB.S.V p cross join DEV_DB.S.V d")
            .dependency_in(
                "PROD_DB.S.V",
                "create view V (x,y) as select 1 as a, 2 as b",
                "PROD_DB",
                "S",
            )
            .dependency_in(
                "DEV_DB.S.V",
                "create view V (y,x) as select 1 as a, 2 as b",
                "DEV_DB",
                "S",
            )
            .tolerance_seconds(0)
    };
    sc.build_with(submit("PROD_DB"), transient_at(500)).await;
    sc.expect_execute(submit("DEV_DB")).await;
}

#[tokio::test]
async fn conflicting_definitions_of_one_snowflake_view_cannot_prove_a_copy() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let model = |schema: &str| {
        let unquoted = format!("CDO_DEMO_DB.{schema}.V");
        let quoted = demo::relation(schema, "V");
        demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
            .sql(&format!("select v from {unquoted}"))
            .namespace(NS)
            .dependency_in(&unquoted, "select 'dummy' as v", demo::CATALOG, schema)
            .dependency_in(
                &quoted,
                "select local.v from U local cross join P.U p cross join Q.U q",
                demo::CATALOG,
                schema,
            )
            .upstream(&demo::relation("P", "U"), 100)
            .upstream(&demo::relation("Q", "U"), 200)
            .tolerance_seconds(0)
    };
    sc.build_with(model("P"), transient_at(500)).await;
    sc.expect_execute(model("Q")).await;
}

#[tokio::test]
async fn ambiguous_unqualified_root_read_needs_search_path_evidence() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let model = |target: &str| {
        demo::model(&demo::relation("AAA", target), "AAA", "model.p.m")
            .sql("select local.v from U local cross join AAA.U a cross join ZZZ.U z")
            .namespace(NS)
            .upstream(&demo::relation("AAA", "U"), 100)
            .upstream(&demo::relation("ZZZ", "U"), 200)
            .tolerance_seconds(0)
    };
    // The two Snowflake sessions use the same current schema. The first
    // searches AAA first and the second ZZZ first. Both send all reads.
    sc.build_with(model("PROD_M"), transient_at(500)).await;
    sc.expect_execute(model("DEV_M")).await;
}
