//! Offline gRPC regressions for physical scope and conservative SQL identity.

#[path = "support.rs"]
mod support;

use support::scenario::{model, Scenario};
use support::start_server;

#[tokio::test]
async fn same_relation_text_is_independent_across_warehouse_connections() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let base = model("DB.S.T")
        .catalog("db")
        .schema("schema")
        .own_epoch(Some(100))
        .without_node_state();
    let a = base.clone().namespace("account-a").sql("select 1");
    let b = base.clone().namespace("account-b").sql("select 2");
    let c = base
        .dialect("redshift")
        .namespace("account-a")
        .sql("select 3");

    sc.build(a.clone(), 200).await;
    sc.build(b.clone(), 200).await;
    sc.build(c.clone(), 200).await;
    for request in [a, b, c] {
        assert_eq!(sc.verdict(request).await, "skip_execution");
    }
}

#[tokio::test]
async fn bigquery_reuses_exact_sql_but_does_not_clone_unverified_equivalence() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let source = "`project.dataset.source`";
    let target = "`project.dataset.target`";
    let bigquery = model(source)
        .dialect("bigquery")
        .namespace("project")
        .catalog("db")
        .schema("schema")
        .own_epoch(Some(100))
        .without_node_state();
    let base = bigquery.clone().sql("select 1 as id");
    sc.build(base.clone(), 200).await;
    assert_eq!(sc.verdict(base.clone()).await, "skip_execution");

    let changed_whitespace = base.sql("SELECT 1 AS id");
    assert_eq!(sc.verdict(changed_whitespace).await, "ready_to_execute");

    let fresh_target = bigquery.target(target).sql("select 1 as id");
    assert_eq!(sc.verdict(fresh_target).await, "ready_to_execute");
}
