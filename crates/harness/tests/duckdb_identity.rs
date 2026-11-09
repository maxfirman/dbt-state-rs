//! DuckDB identity through the production router, including Record/Submit parity.
mod support;
use support::legacy::{self, Rows};
use support::scenario::{model, outcome, Scenario};
use support::start_server;

const TARGET: &str = "db.dev.target";

#[tokio::test]
async fn duckdb_formatting_preserves_identifiers_context_configuration_and_bindings() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let base = model(TARGET)
        .without_node_state()
        .dialect("duckdb")
        .namespace("one-file-generation")
        .catalog("db")
        .schema("dev")
        .own_epoch(Some(100))
        .sql("select id, amount + 1 as \"Output.Name\" from db.dev.input")
        .dependency_in("db.dev.\"a.b\"", "select 1 as id", "db", "dev")
        .dependency_in("db.\"dev.a\".b", "select 2 as id", "db", "dev.a");
    sc.build_with(base.clone(), outcome().epoch(100).table())
        .await;
    let formatted = base
        .clone()
        .sql("SELECT /* safe */ id,\namount + 1 AS \"Output.Name\" FROM db.dev.input")
        .with(|req| req.query_dependencies[0].query = "SELECT /* dep */ 1 AS id".into());
    assert_eq!(sc.verdict(formatted).await, "skip_execution");
    let changed = [
        base.clone()
            .with(|req| req.sql = req.sql.replace("+ 1", "+ 2")),
        base.clone()
            .with(|req| req.sql = req.sql.replace("Output.Name", "output.name")),
        base.clone().schema("prod"),
        base.clone().catalog("another"),
        base.clone().extra("contract", "changed"),
        base.clone()
            .with(|req| req.query_dependencies[0].default_schema = "other".into()),
        base.clone().with(|req| {
            let q = req.query_dependencies[0].query.clone();
            req.query_dependencies[0].query = req.query_dependencies[1].query.clone();
            req.query_dependencies[1].query = q;
        }),
        base.clone()
            .with(|req| req.query_dependencies[0].name = "db.dev.a.b".into()),
        base.clone().target("db.prod.target"),
    ];
    for r in changed {
        assert_eq!(sc.verdict(r).await, "ready_to_execute");
    }
}

#[tokio::test]
async fn duckdb_dotted_reference_bindings_do_not_collapse_or_swap() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let base = model(TARGET)
        .without_node_state()
        .dialect("duckdb")
        .namespace("one-file-generation")
        .catalog("db")
        .schema("dev")
        .own_epoch(Some(100))
        .sql("select a.id as id from db.dev.\"a.b\" a cross join db.\"dev.a\".b b")
        .dependency_in("db.dev.\"a.b\"", "select 1 as id", "db", "dev")
        .dependency_in("db.\"dev.a\".b", "select 2 as id", "db", "dev.a");
    sc.build_with(base.clone(), outcome().epoch(100).table())
        .await;
    assert_eq!(sc.verdict(base.clone()).await, "skip_execution");
    let swapped = base.clone().with(|req| {
        req.query_dependencies[0].query = "select 2 as id".into();
        req.query_dependencies[1].query = "select 1 as id".into();
    });
    assert_eq!(sc.verdict(swapped).await, "ready_to_execute");
    let split = base.with(|req| {
        req.sql = req.sql.replace("db.dev.\"a.b\"", "db.\"dev.a\".b");
        req.query_dependencies.remove(0);
    });
    assert_eq!(sc.verdict(split).await, "ready_to_execute");
}

#[tokio::test]
async fn unsupported_duckdb_syntax_and_implicit_column_names_keep_exact_identity() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let request = model(TARGET)
        .without_node_state()
        .dialect("duckdb")
        .namespace("one-file-generation")
        .catalog("db")
        .schema("dev")
        .own_epoch(Some(100));
    for text in [
        "select 1 + 2",
        "select sum(amount) as total from input",
        "from input select id",
        "select * exclude (id) from input",
        "select [1, 2] as array",
        "select 1 as id; select 2 as id",
    ] {
        let base = request.clone().sql(text);
        sc.build_with(base.clone(), outcome().epoch(100).table())
            .await;
        assert_eq!(sc.verdict(base.clone()).await, "skip_execution");
        let formatted = base.with(|req| req.sql.push_str(" /* unknown semantics */"));
        assert_eq!(sc.verdict(formatted).await, "ready_to_execute");
    }
}

#[tokio::test]
async fn recorded_duckdb_formatting_uses_the_same_identity_as_submission() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let base = model(TARGET)
        .without_node_state()
        .dialect("duckdb")
        .namespace("one-file-generation")
        .catalog("db")
        .schema("dev")
        .own_epoch(Some(100))
        .sql("select id, amount + 1 as amount from db.dev.input");
    // The record carries the submission's identity inputs but no tables.
    sc.record([base
        .clone()
        .no_tables()
        .record(outcome().epoch(100).table())])
        .await
        .unwrap();
    let formatted = base.sql("SELECT id, /* trivia */ amount + 1 AS amount FROM db.dev.input");
    assert_eq!(sc.verdict(formatted).await, "skip_execution");
}

#[tokio::test]
async fn old_exact_duckdb_history_cold_misses_after_identity_upgrade() {
    let server = support::start_server().await;
    let sc = Scenario::connect(&server).await;
    // Already-canonical SQL isolates version invalidation from formatting.
    let base = model(TARGET)
        .without_node_state()
        .dialect("duckdb")
        .namespace("one-file-generation")
        .catalog("db")
        .schema("dev")
        .own_epoch(Some(100))
        .sql("SELECT id, amount + 1 AS amount FROM db.dev.input");
    sc.build_with(base.clone(), outcome().epoch(100).table())
        .await;
    assert_eq!(sc.verdict(base.clone()).await, "skip_execution");
    // Frozen b51cca4 exact-byte digest: length-framed v4, duckdb/db/dev,
    // rendered, the SQL above, empty extras and dependencies. This literal
    // does not call today's hashing code with a pretend historical version.
    legacy::rewrite_sql_hash(
        &server,
        Rows::Confirmed,
        "5c1424fbf75b4ea08a9b41339bc91c32d063026bdc7919ae85a7470df86b7b4a",
    )
    .await;
    assert_eq!(sc.verdict(base).await, "ready_to_execute");
}
