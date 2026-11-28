//! Cross-target Result copy over function calls (#272). Each pair of
//! targets reads one physical input ([`demo::SHARED_UP`]), so only the
//! function a call resolves to can differ.
#[path = "support.rs"]
mod support;

use dbt_state_proto::query_cache::submit_sql_response::Response;
use support::demo::{self, transient_at, NS};
use support::scenario::{Scenario, Submit};
use support::start_server;

/// A model building `M` in `schema` with `select` (`{schema}` replaced by
/// the target's schema) over the shared input.
fn model(schema: &str, select: &str) -> Submit {
    demo::model(&demo::relation(schema, "M"), schema, "model.p.m")
        .sql(&format!(
            "select {} as id from CDO_DEMO_DB.raw.lp_up_a",
            select.replace("{schema}", schema)
        ))
        .namespace(NS)
        .tolerance_seconds(0)
        .upstream(demo::SHARED_UP, 100)
}

/// Builds the model in `PROD`; returns whether the same model in `DEV` is
/// copied from it.
async fn copied(select: &str) -> bool {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    sc.build_with(model("PROD", select), transient_at(500))
        .await;
    match sc.submit(model("DEV", select)).await.response {
        Some(Response::ReadyToClone(grant)) => {
            assert_eq!(grant.clone_source, demo::relation("PROD", "M"));
            true
        }
        Some(Response::ReadyToExecute(_)) => false,
        other => panic!("unexpected decision {other:?}"),
    }
}

/// An unqualified call resolves through each session's search path: the
/// two schemas can hold different functions of one name.
#[tokio::test]
async fn an_unqualified_user_function_builds() {
    assert!(!copied("my_udf(id)").await);
}

#[tokio::test]
async fn built_in_functions_copy() {
    assert!(copied("coalesce(upper(id::varchar), 'none')").await);
}

/// One function, named in full: both targets call it.
#[tokio::test]
async fn a_shared_fully_qualified_function_copies() {
    assert!(copied("CDO_DEMO_DB.UTIL.MY_UDF(id)").await);
}

/// dbt's `{{ function() }}` renders each target's own function in full.
/// Nothing in the request shows the two definitions are equivalent, so the
/// model builds even when they are (#272).
#[tokio::test]
async fn a_per_target_function_builds() {
    assert!(!copied("CDO_DEMO_DB.{schema}.MY_UDF(id)").await);
}
