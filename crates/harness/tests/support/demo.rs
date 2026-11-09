//! Request shapes from the live clone captures (experiments/clone_*.sh and the
//! review #2 probes): Snowflake models built in the demo database
//! `CDO_DEMO_DB`, in a prod schema (`prod_state_demo`) and a dev schema
//! (`dev_clone_demo`) that share one connection namespace. Built on the
//! `scenario` builders, so every setter there still applies.

use super::scenario::{
    clone_request, model as scenario_model, outcome, CopyRequest, Outcome, Submit,
};

/// The demo database every capture builds in.
pub const CATALOG: &str = "CDO_DEMO_DB";
/// The prod environment schema (the clone source side).
pub const PROD_SCHEMA: &str = "prod_state_demo";
/// The dev environment schema (the clone target side).
pub const DEV_SCHEMA: &str = "dev_clone_demo";
/// The captured Snowflake connection namespace (`table_namespace`).
pub const NS: &str = "fc38a5ba0eceb701c6ab7c6b882e28b0";
/// The captured incremental probe model.
pub const CLONE_PROBE: &str = "model.jaffle_shop.clone_probe";
/// The raw upstream table [`clone_probe`] reads through its `STG_ORDERS` view.
pub const RAW_ORDERS: &str = "\"CDO_DEMO_DB\".\"RAW\".\"RAW_ORDERS\"";

/// A Snowflake table model (`FULL`) as the captures submit it: `target` with
/// default catalog [`CATALOG`] and default schema `schema`, a 2700 s freshness
/// tolerance, nondeterminism tolerated, no tables, no namespace and no query
/// dependencies. Its node state carries `unique_id`, body hash `body` (no
/// `node_hash`) and project `p`; its `dbt_node_name` label is the last segment
/// of `unique_id`. The SQL is the scenario default; set it with `sql(..)`.
pub fn model(target: &str, schema: &str, unique_id: &str) -> Submit {
    let name = unique_id.rsplit('.').next().unwrap_or(unique_id);
    scenario_model(target)
        .catalog(CATALOG)
        .schema(schema)
        .tolerance_seconds(2700)
        .no_tables()
        .unique_id(unique_id)
        .project("p")
        .node_state(|state| state.node_hash.clear())
        .label("dbt_node_name", name)
}

/// The SQL of [`clone_probe`] in `schema`; `extra_col` (e.g.
/// `", upper(customer_id) as c"`) is a genuine projection change.
pub fn clone_probe_sql(schema: &str, extra_col: &str) -> String {
    format!(
        "select order_id, customer_id, ordered_at{extra_col} from CDO_DEMO_DB.{schema}.stg_orders where ordered_at > '1900-01-01'"
    )
}

/// The captured [`CLONE_PROBE`] model building `target` in `schema`: a
/// `FULL` model reading the `STG_ORDERS` view of `schema` (a query dependency
/// over `raw_orders`), in namespace [`NS`], labelled with its unique id. It
/// reports no tables; add its own target and [`RAW_ORDERS`] with `own_epoch`
/// and `upstream`.
pub fn clone_probe(target: &str, schema: &str) -> Submit {
    model(target, schema, CLONE_PROBE)
        .sql(&clone_probe_sql(schema, ""))
        .dependency_in(
            &format!("\"CDO_DEMO_DB\".\"{}\".\"STG_ORDERS\"", schema.to_uppercase()),
            &format!(
                "create or replace view STG_ORDERS as (select * from CDO_DEMO_DB.{schema}.raw_orders)"
            ),
            CATALOG,
            schema,
        )
        .namespace(NS)
        .label("dbt_node_unique_id", CLONE_PROBE)
}

/// A RegisterClone of `source` into `target` in [`CATALOG`] and namespace
/// [`NS`], with a `TRANSIENT TABLE` source (Snowflake `FULL`).
pub fn copy(source: &str, target: &str) -> CopyRequest {
    clone_request(source, target)
        .catalog(CATALOG)
        .namespace(NS)
        .source_table_type("TRANSIENT TABLE")
}

/// A `TRANSIENT TABLE` outcome built at `epoch` (the captured Snowflake
/// table type), without a runtime.
pub fn transient_at(epoch: i64) -> Outcome {
    outcome().epoch(epoch).table_type("TRANSIENT TABLE")
}
