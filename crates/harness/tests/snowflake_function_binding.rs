//! Cross-target Result copy over function calls (#272). Each pair of
//! targets reads one physical input ([`demo::SHARED_UP`]), so only the
//! function a call resolves to can differ.
#[path = "support.rs"]
mod support;

use support::demo::{self, transient_at, NS};
use support::scenario::{Scenario, Submit};
use support::start_server;

/// A model building `M` in `schema` projecting `expr` (`{schema}` replaced
/// by the target's schema) over the shared input.
fn model(schema: &str, expr: &str) -> Submit {
    demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
        .sql(&format!(
            "select {} as id from CDO_DEMO_DB.raw.lp_up_a",
            expr.replace("{schema}", schema)
        ))
        .namespace(NS)
        .tolerance_seconds(0)
        .upstream(demo::SHARED_UP, 100)
}

/// A scenario with the model projecting `expr` built in `PROD`.
async fn built_in_prod(server: &support::TestServer, expr: &str) -> Scenario {
    let sc = Scenario::connect(server).await;
    sc.build_with(model("PROD", expr), transient_at(500)).await;
    sc
}

async fn dev_builds(expr: &str) {
    let server = start_server().await;
    let sc = built_in_prod(&server, expr).await;
    sc.expect_execute(model("DEV", expr)).await;
}

async fn dev_copies_prod(expr: &str) {
    let server = start_server().await;
    let sc = built_in_prod(&server, expr).await;
    let grant = sc.expect_clone(model("DEV", expr)).await;
    assert_eq!(grant.clone_source, demo::relation("PROD", "M"));
}

/// An unqualified call resolves through each session's search path: the
/// two schemas can hold different functions of one name.
#[tokio::test]
async fn an_unqualified_user_function_builds() {
    dev_builds("my_udf(id)").await;
}

#[tokio::test]
async fn built_in_functions_copy() {
    dev_copies_prod("coalesce(upper(id::varchar), 'none')").await;
}

/// One function, named in full: both targets call it.
#[tokio::test]
async fn a_shared_fully_qualified_function_copies() {
    dev_copies_prod("CDO_DEMO_DB.UTIL.MY_UDF(id)").await;
}

/// dbt's `{{ function() }}` renders each target's own function in full.
/// Nothing in the request shows the two definitions are equivalent, so the
/// model builds even when they are (#272).
#[tokio::test]
async fn a_per_target_function_builds() {
    dev_builds("CDO_DEMO_DB.{schema}.MY_UDF(id)").await;
}
