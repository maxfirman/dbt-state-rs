//! Conservative table result copying through the production service.
mod support;
use dbt_state_proto::query_cache as qc;
use support::scenario::{duckdb_ctas_at, duckdb_model, into_clone, into_execute, Scenario};
use support::start_server;

const PROD: &str = "\"db\".\"prod\".\"result\"";
const DEV: &str = "\"db\".\"dev\".\"result\"";

#[tokio::test]
async fn eligible_table_copy_confirms_then_skips_preserving_physical_inputs() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.build_with(duckdb_model("prod"), duckdb_ctas_at(20))
        .await;
    let dev = duckdb_model("dev");
    for case in 0..10 {
        // An unrelated physical base input must never become equivalent.
        let unsupported = match case {
            0 => dev
                .clone()
                .sql("select id, amount + 1 as amount from db.other.input"),
            1 => dev.clone().schema("unapproved"),
            2 => dev.clone().with(|req| {
                req.semantic_extras.remove("duckdb_copy_contract");
            }),
            3 => dev.clone().sql("select id from input"),
            4 => dev.clone().allow_clones(false),
            5 => dev.clone().with(|req| {
                req.clone_table_properties = Some(qc::TableProperties {
                    hours_to_expiration: Some(1),
                    partition_expiration_days: None,
                })
            }),
            6 => dev.clone().no_tables(),
            7 => dev.clone().table("db.raw.input", None),
            8 => dev
                .clone()
                .with(|req| req.tables.push(req.tables[0].clone())),
            _ => dev.clone().table("db._dbt_state.unresolved", None),
        };
        assert_eq!(sc.verdict(unsupported).await, "ready_to_execute");
    }
    let clone = sc.expect_clone(dev.clone()).await;
    assert_eq!(clone.clone_required_last_modified_epoch, Some(20));
    assert_eq!(clone.clone_source, PROD);
    assert_eq!(clone.clone_target, DEV);
    assert_eq!(clone.clone_sqls, ["CREATE OR REPLACE TABLE \"db\".\"dev\".\"result\" AS SELECT * FROM \"db\".\"prod\".\"result\""]);
    sc.confirm(&clone.request_id, duckdb_ctas_at(30)).await;
    let later = dev.table(DEV, Some(30));
    assert_eq!(sc.verdict(later).await, "skip_execution");
}

#[tokio::test]
async fn failed_sql_copy_records_actual_build_and_becomes_an_ordinary_source() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.build_with(duckdb_model("prod"), duckdb_ctas_at(20))
        .await;
    let copy = sc.expect_clone(duckdb_model("dev")).await;
    sc.confirm(&copy.request_id, duckdb_ctas_at(30).failed_to_clone())
        .await;
    let next = duckdb_model("prod")
        .target("\"db\".\"prod\".\"next\"")
        .clone_chain_depth_limit(1);
    // The ordinary fallback must be a valid source.
    let copy = sc.expect_clone(next).await;
    // With a one-hop client limit, selecting this fallback proves it became an
    // ordinary source rather than retaining false copy provenance.
    assert_eq!(copy.clone_source, DEV);
    assert_eq!(copy.clone_required_last_modified_epoch, Some(30));
}

#[tokio::test]
async fn view_graph_copy_binds_definitions_context_and_exact_leaves() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let graph = |schema: &str| {
        duckdb_model(schema)
            .sql(&format!(
                "select id, amount + 1 as amount from db.{schema}.upper_view"
            ))
            .dependency_in(
                &format!("db.{schema}.upper_view"),
                &format!(
                    "CREATE VIEW {schema}.upper_view AS SELECT id, amount FROM {schema}.lower_view"
                ),
                "db",
                schema,
            )
            .dependency_in(
                &format!("db.{schema}.lower_view"),
                "CREATE VIEW lower_view AS SELECT id, amount FROM raw.input",
                "db",
                schema,
            )
    };
    sc.build_with(graph("prod"), duckdb_ctas_at(20)).await;
    let dev = graph("dev");
    assert_eq!(sc.verdict(dev.clone()).await, "ready_to_clone");
    // An unqualified reference resolves through the explicit query context.
    let unqualified = dev
        .clone()
        .sql("select id, amount + 1 as amount from upper_view");
    assert_eq!(sc.verdict(unqualified).await, "ready_to_clone");
    // Dependency list order changes no reference-to-definition bindings.
    let reordered = dev.clone().with(|req| req.query_dependencies.reverse());
    assert_eq!(sc.verdict(reordered).await, "ready_to_clone");
    for case in 0..8 {
        let req = dev.clone().with(|req| {
            match case {
                0 => {
                    req.query_dependencies[1].query =
                        "select id, amount + 3 as amount from raw.input".into()
                }
                1 => req.query_dependencies[1].query = "select id, amount from other.input".into(),
                2 => req.query_dependencies[1].default_schema = "other".into(),
                3 => req
                    .query_dependencies
                    .push(req.query_dependencies[0].clone()),
                4 => req.query_dependencies.swap(0, 1), // restore order below, but swap bound definitions
                5 => req.query_dependencies[1].query = "select id, amount from lower_view".into(),
                6 => {
                    req.sql = req.sql.replace("dev.upper_view", "environment.upper_view");
                    req.query_dependencies[0].name = "db.environment.upper_view".into();
                }
                _ => {
                    let mut duplicate = req.query_dependencies[0].clone();
                    duplicate.name = "db.dev.UPPER_VIEW".into();
                    req.query_dependencies.push(duplicate);
                }
            }
            if case == 4 {
                let query = req.query_dependencies[0].query.clone();
                req.query_dependencies[0].query = req.query_dependencies[1].query.clone();
                req.query_dependencies[1].query = query;
            }
        });
        assert_eq!(sc.verdict(req).await, "ready_to_execute", "negative {case}");
    }
}

#[tokio::test]
async fn graph_copy_chain_supports_two_hops_and_rejects_a_third() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let graph = |schema: &str| {
        duckdb_model(schema)
            .sql(&format!(
                "select id, amount + 1 as amount from db.{schema}.upper_view"
            ))
            .dependency_in(
                &format!("db.{schema}.upper_view"),
                &format!(
                    "CREATE VIEW {schema}.upper_view AS SELECT id, amount FROM {schema}.lower_view"
                ),
                "db",
                schema,
            )
            .dependency_in(
                &format!("db.{schema}.lower_view"),
                "CREATE VIEW lower_view AS SELECT id, amount FROM raw.input",
                "db",
                schema,
            )
    };
    let mut previous = String::new();
    for (step, schema) in ["prod", "dev", "prod", "dev"].iter().enumerate() {
        if step == 3 {
            // A later ordinary rebuild of an ancestor must not shorten the
            // already confirmed third relation's two-hop provenance.
            let rebuild = graph("dev")
                .target("\"db\".\"dev\".\"result1\"")
                .allow_clones(false);
            sc.build_with(rebuild, duckdb_ctas_at(35)).await;
        }
        let target = format!("\"db\".\"{schema}\".\"result{step}\"");
        let response = sc.submit(graph(schema).target(&target)).await;
        let id = if step == 0 || step == 3 {
            into_execute(response).request_id
        } else {
            let copy = into_clone(response);
            assert_eq!(copy.clone_source, previous);
            assert_eq!(copy.clone_target, target);
            copy.request_id
        };
        previous = target;
        sc.confirm(&id, duckdb_ctas_at(20 + step as i64 * 10)).await;
    }
}
