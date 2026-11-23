#[path = "support.rs"]
mod support;

use support::demo::{self, transient_at, NS};
use support::scenario::Scenario;
use support::start_server;

/// Both requests report exactly the same two genuine inputs. The projection
/// reads the unqualified U, which changes with the session default schema.
#[tokio::test]
async fn default_schema_selects_a_different_join_input() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let sql = "select local.v from U local cross join AAA.U a cross join ZZZ.U z";
    let model = |schema| {
        demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
            .sql(sql)
            .namespace(NS)
            .tolerance_seconds(0)
            .upstream(&demo::relation("AAA", "U"), 100)
            .upstream(&demo::relation("ZZZ", "U"), 200)
    };
    for (schema, marker, epoch) in [("AAA", "prod", 100), ("ZZZ", "dev", 200)] {
        sc.build_with(
            demo::model(&demo::relation(schema, "U"), schema, "model.p.u")
                .namespace(NS)
                .sql(&format!("select '{marker}' as v")),
            transient_at(epoch),
        )
        .await;
    }
    sc.build_with(model("AAA"), transient_at(500)).await;
    sc.expect_execute(model("ZZZ")).await;
}

async fn omitted_table(sql: &str) {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let model = |schema| {
        demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
            .sql(sql)
            .namespace(NS)
            .tolerance_seconds(0)
            .upstream(&demo::relation("FIXED", "S"), 100)
    };
    sc.build_with(model("AAA_PROD"), transient_at(500)).await;
    sc.expect_execute(model("BBB_DEV")).await;
}

#[tokio::test]
async fn a_snowflake_table_literal_reads_a_physical_relation() {
    omitted_table("select u.v from table('U') u cross join CDO_DEMO_DB.FIXED.S s").await;
}

#[tokio::test]
async fn a_snowflake_identifier_literal_reads_a_physical_relation() {
    omitted_table("select u.v from identifier('U') u cross join CDO_DEMO_DB.FIXED.S s").await;
}

#[tokio::test]
async fn unqualified_reads_in_views_keep_their_bindings_through_copies() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let upstream = |schema: &str, value: &str| {
        demo::model(&demo::relation(schema, "U"), "ZZZ", "model.p.u")
            .sql(&format!("select '{value}' as v"))
            .namespace(NS)
            .tolerance_seconds(0)
    };
    sc.build_with(upstream("P", "p"), transient_at(100)).await;
    sc.build_with(upstream("Q", "q"), transient_at(200)).await;
    for (schema, value, source, epoch) in [("A", "q", "Q", 300), ("B", "p", "P", 400)] {
        let grant = sc.expect_clone(upstream(schema, value)).await;
        assert_eq!(grant.clone_source, demo::relation(source, "U"));
        assert!(sc.confirm(&grant.request_id, transient_at(epoch)).await);
    }
    let downstream = |target: &str, left: &str, left_epoch: i64, right: &str, right_epoch: i64| {
        demo::model(&demo::relation("ZZZ", target), "ZZZ", "model.p.m")
            .sql(&format!(
                "select l.v from CDO_DEMO_DB.{left}.V l cross join CDO_DEMO_DB.{right}.V r"
            ))
            .dependency_in(
                &demo::relation(left, "V"),
                "select v from U",
                demo::CATALOG,
                left,
            )
            .dependency_in(
                &demo::relation(right, "V"),
                "select v from U",
                demo::CATALOG,
                right,
            )
            .upstream(&demo::relation(left, "U"), left_epoch)
            .upstream(&demo::relation(right, "U"), right_epoch)
            .namespace(NS)
            .tolerance_seconds(0)
    };
    sc.build_with(downstream("PROD_M", "P", 100, "Q", 200), transient_at(500))
        .await;
    sc.expect_execute(downstream("DEV_M", "A", 300, "B", 400))
        .await;
}

#[tokio::test]
async fn the_held_build_must_report_the_input_its_sql_read() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let model = |schema| {
        demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
            .sql(&format!("select v from CDO_DEMO_DB.{schema}.U"))
            .namespace(NS)
            .tolerance_seconds(0)
            .upstream(&demo::relation("DEV", "U"), 100)
    };
    sc.build_with(model("PROD"), transient_at(500)).await;
    sc.expect_execute(model("DEV")).await;
}

#[tokio::test]
async fn only_the_source_underreports_a_genuine_input() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let model = |schema| {
        demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
            .sql("select local.v from U local cross join FIXED.U f")
            .namespace(NS)
            .tolerance_seconds(0)
            .upstream(&demo::relation("FIXED", "U"), 100)
    };
    // The build omitted PROD.U. The destination correctly reports FIXED.U,
    // which it reads twice. Their lists of reported names are identical.
    sc.build_with(model("PROD"), transient_at(500)).await;
    sc.expect_execute(model("FIXED")).await;
}

#[tokio::test]
async fn shared_input_in_a_recursive_cte_keeps_the_safe_copy() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let model = |target| {
        demo::model(&demo::relation("ZZZ", target), "ZZZ", "model.p.m")
            .sql("with r(n) as (select n from CDO_DEMO_DB.AAA.FIXED union all select n+1 from r where n < 3) select n from r")
            .namespace(NS)
            .tolerance_seconds(0)
            .upstream(&demo::relation("AAA", "FIXED"), 100)
    };
    sc.build_with(model("PROD_M"), transient_at(500)).await;
    sc.expect_clone(model("DEV_M")).await;
}
