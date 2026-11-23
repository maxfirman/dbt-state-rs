#[path = "support.rs"]
mod support;

use support::demo::{self, transient_at, DEV_SCHEMA, NS, PROD_SCHEMA};
use support::scenario::{
    built_at, clone_request, environment_model, environment_target, Scenario, ENVIRONMENT_NAMESPACE,
};
use support::start_server;
#[tokio::test]
async fn rereview266_copied_upstreams_in_a_view_must_keep_their_query_bindings() {
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
        let left = demo::relation(left, "U");
        let right = demo::relation(right, "U");
        demo::model(&demo::relation("ZZZ", target), "ZZZ", "model.p.m")
            .sql("select * from CDO_DEMO_DB.ZZZ.V")
            .dependency_in(
                &demo::relation("ZZZ", "V"),
                &format!(
                    "create or replace view V as select l.v from {left} l cross join {right} r"
                ),
                demo::CATALOG,
                "ZZZ",
            )
            .namespace(NS)
            .tolerance_seconds(0)
            .upstream(&left, left_epoch)
            .upstream(&right, right_epoch)
    };
    sc.build_with(downstream("PROD_M", "P", 100, "Q", 200), transient_at(500))
        .await;
    // The candidate projects P's 'p'. A holds Q's 'q', so projecting A's
    // value over the same two underlying relations requires a new execution.
    sc.expect_execute(downstream("DEV_M", "A", 300, "B", 400))
        .await;
}

#[tokio::test]
async fn rereview266_reported_shared_input_does_not_cover_a_second_unreported_schema_input() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let model = |s: &str| {
        demo::model(&demo::relation(s, "M"), s, "model.p.m")
            .sql(&format!(
                "select local.v from CDO_DEMO_DB.{s}.U local cross join CDO_DEMO_DB.AAA.FIXED f"
            ))
            .namespace(NS)
            .tolerance_seconds(0)
            .upstream(&demo::relation("AAA", "FIXED"), 100)
    };
    sc.build_with(model("PROD"), transient_at(1000)).await;
    sc.expect_execute(model("DEV")).await;
}

#[tokio::test]
async fn rereview266_candidate_copy_of_incremented_value_is_a_real_dependency() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let ci = environment_target("ci");
    let down = |schema| {
        environment_model(schema)
            .no_tables()
            .schema("FIXED")
            .sql("select id + 1 as id from DB.CI.M")
            .upstream(&ci, 100)
            .tolerance_seconds(0)
    };
    sc.build_with(
        environment_model("ci")
            .no_tables()
            .schema("FIXED")
            .sql("select 0 as id"),
        built_at(100),
    )
    .await;
    sc.build_with(down("prod"), built_at(200)).await;
    let copy = sc
        .register_clone(
            clone_request(&environment_target("prod"), &ci)
                .catalog("DB")
                .namespace(ENVIRONMENT_NAMESPACE)
                .source_epoch(200)
                .label("dbt_node_unique_id", "model.p.m"),
        )
        .await
        .expect("register")
        .ready_to_clone
        .expect("ready to clone");
    assert!(sc.confirm(&copy.request_id, built_at(300)).await);
    sc.expect_execute(down("dev").upstream(&ci, 300)).await;
}

#[tokio::test]
async fn rereview266_stricter_resubmit_checks_epoch_of_build_that_was_copied() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await.at(3_000);
    let input = demo::relation("AAA", "FIXED");
    let model = |s: &str, epoch| {
        demo::model(&demo::relation(s, "M"), s, "model.p.m")
            .namespace(NS)
            .sql("select v from CDO_DEMO_DB.AAA.FIXED")
            .upstream(&input, epoch)
    };
    sc.build_with(
        model("PROD", 1_000).tolerance_seconds(0),
        transient_at(2_000),
    )
    .await;
    let grant = sc
        .expect_clone(model("DEV", 2_100).tolerance_seconds(60))
        .await;
    assert_eq!(grant.clone_source, demo::relation("PROD", "M"));
    assert!(sc.confirm(&grant.request_id, transient_at(2_500)).await);
    sc.expect_execute(
        model("DEV", 2_100)
            .own_epoch(Some(2_500))
            .tolerance_seconds(0),
    )
    .await;
}
#[tokio::test]
async fn rereview266_copy_from_stale_source_expires_with_lag() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let probe = |schema, epoch| {
        demo::model(&demo::relation(schema, "M"), "fixed", "model.p.m")
            .namespace(NS)
            .sql("select v from CDO_DEMO_DB.RAW.U")
            .upstream(&demo::relation("RAW", "U"), epoch)
            .tolerance_seconds(30)
    };
    sc.at(2000)
        .build_with(probe(PROD_SCHEMA, 100), transient_at(2000))
        .await;
    let grant = sc.at(3000).expect_clone(probe(DEV_SCHEMA, 2500)).await;
    assert!(
        sc.at(3000)
            .confirm(&grant.request_id, transient_at(3000))
            .await
    );
    sc.at(90_000)
        .expect_execute(probe(DEV_SCHEMA, 2500).own_epoch(Some(3000)))
        .await;
}

#[tokio::test]
async fn rereview266_a_cte_over_a_constant_dependency_is_still_source_free() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |target: &str| {
        demo::model(&demo::relation("ZZZ", target), "ZZZ", "model.p.down")
            .namespace(NS)
            .sql("with c as (select v from CDO_DEMO_DB.ZZZ.V) select v from c")
            .dependency_in(
                &demo::relation("ZZZ", "V"),
                "select 1 as v",
                demo::CATALOG,
                "ZZZ",
            )
    };
    sc.build_with(submit("PROD_M"), transient_at(500)).await;
    sc.expect_clone(submit("DEV_M")).await;
}

#[tokio::test]
async fn rereview266_a_view_declaration_is_not_an_upstream_of_its_own_body() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let submit = |target: &str| {
        demo::model(&demo::relation("ZZZ", target), "ZZZ", "model.p.down")
            .namespace(NS)
            .sql("select v from CDO_DEMO_DB.ZZZ.V")
            .dependency_in(
                &demo::relation("ZZZ", "V"),
                "create or replace view V as (select 1 as v)",
                demo::CATALOG,
                "ZZZ",
            )
    };
    sc.build_with(submit("PROD_M"), transient_at(500)).await;
    sc.expect_clone(submit("DEV_M")).await;
}

#[tokio::test]
async fn rereview266_cross_environment_self_read_with_a_shared_input() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let initial = |schema, value| {
        environment_model(schema)
            .no_tables()
            .sql(&format!("select {value} as id"))
    };
    let self_read = |schema, own| {
        environment_model(schema)
            .no_tables()
            .sql(&format!(
                "select m.id + s.v as id from DB.{schema}.m m cross join DB.ZZZ.S s"
            ))
            .upstream("\"DB\".\"ZZZ\".\"S\"", 50)
            .own_epoch(Some(own))
    };
    sc.build_with(initial("prod", 10), built_at(100)).await;
    sc.build_with(initial("dev", 20), built_at(200)).await;
    sc.build_with(self_read("prod", 100), built_at(300)).await;
    sc.expect_execute(self_read("dev", 200)).await;
}
