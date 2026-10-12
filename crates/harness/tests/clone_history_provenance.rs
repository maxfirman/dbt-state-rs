//! CLONE HISTORY / PHYSICAL-STATE PROVENANCE regressions (review issue #1, P0).
//!
//! A historical execution must never be treated as evidence of the CURRENT
//! contents of a physical object. Two clone-state lookups violated this:
//!
//!   P0#1 `store::find_clone_equivalent` (SQL-submit cross-name fallback):
//!        it filtered confirmed rows by (namespace, schema-abstracted logic,
//!        execution_type) BEFORE picking the newest per source target, so an
//!        OBSOLETE row for a source that has since been rebuilt with different
//!        logic was still eligible as a clone source. A→B→A: build PROD with
//!        logic A, rebuild PROD with logic B, then a new target submits logic A
//!        — the stale A row made PROD a clone source even though PROD now holds
//!        B. Correct: the source's CURRENT (latest confirmed) logic must match.
//!
//!   P0#2 `store::find_latest_confirmed_target` (clone adoption on submit):
//!        it applied `clone_source_table IS NOT NULL` BEFORE selecting the
//!        latest confirmed row, so a target that was cloned (clone-origin row A)
//!        and later rebuilt normally (ordinary row B) still resolved to the old
//!        clone-origin row A. clone→rebuild→old-logic: the physical target holds
//!        B, but a resubmit of logic A adopted the stale clone-origin row and
//!        could SKIP. Correct: the latest confirmed row for the target wins; it
//!        is adopted only if it is ITSELF clone-origin and logic-compatible.
//!
//! Both scenarios assert the SAFE (physical-state-faithful) outcome: EXECUTE,
//! never reuse of a historical object description.

#[path = "support.rs"]
mod support;

use dbt_state_proto::query_cache as qc;
use qc::clone_client::CloneClient;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, response_variant, start_server};

const NS: &str = "fc38a5ba0eceb701c6ab7c6b882e28b0";
const RAW: &str = "\"CDO_DEMO_DB\".\"RAW\".\"RAW_ORDERS\"";

fn tbl(name: &str, epoch: i64) -> qc::TableModifiedInfo {
    qc::TableModifiedInfo {
        name: name.into(),
        last_modified_epoch: Some(epoch),
    }
}

/// A table-materialization submit (execution_type=1) for `target`, built from
/// the same logical model in `schema`. `extra_col` injects a genuine projection
/// change (different schema-abstracted logic) when non-empty. Mirrors the shape
/// used by `clone_lifecycle.rs`/`clone_submit_fallback.rs`.
fn submit(
    target: &str,
    schema: &str,
    own_epoch: i64,
    raw_epoch: i64,
    extra_col: &str,
) -> qc::SubmitEnrichedSqlRequest {
    let sql = format!(
        "select order_id, customer_id, ordered_at{extra_col} from CDO_DEMO_DB.{schema}.stg_orders where ordered_at > '1900-01-01'"
    );
    qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: "snowflake".into(),
        default_catalog: "CDO_DEMO_DB".into(),
        default_schema: Some(schema.into()),
        execution_type: 1,
        sql,
        tolerate_nondeterminism: true,
        freshness_tolerance_seconds: 2700,
        tables: vec![tbl(target, own_epoch), tbl(RAW, raw_epoch)],
        query_dependencies: vec![qc::QueryDependency {
            name: format!("\"CDO_DEMO_DB\".\"{}\".\"STG_ORDERS\"", schema.to_uppercase()),
            query: format!(
                "create or replace view STG_ORDERS as (select * from CDO_DEMO_DB.{schema}.raw_orders)"
            ),
            default_catalog: "CDO_DEMO_DB".into(),
            default_schema: schema.into(),
        }],
        table_namespace: Some(NS.into()),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.jaffle_shop.clone_probe".into(),
            node_body_hash: Some("body".into()),
            project_id: Some("p".into()),
            ..Default::default()
        }),
        labels: [
            ("dbt_node_name".to_string(), "clone_probe".to_string()),
            (
                "dbt_node_unique_id".to_string(),
                "model.jaffle_shop.clone_probe".to_string(),
            ),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    }
}

async fn execute_and_confirm(
    sql: &mut SqlClient<tonic::transport::Channel>,
    exec: &mut ExecutionClient<tonic::transport::Channel>,
    req: qc::SubmitEnrichedSqlRequest,
    confirm_epoch: i64,
) {
    let resp = sql.submit_enriched_sql(req).await.unwrap().into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(r) = resp.response.unwrap() else {
        panic!("expected ready_to_execute to build");
    };
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: r.request_id,
            last_modified_epoch: Some(confirm_epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            execution_runtime_ms: Some(1000),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );
}

/// P0#1 — A→B→A cross-name clone selection.
///
/// `PROD` built with logic A, then rebuilt with logic B (physical PROD now
/// holds B). A new target `DEV` (absent) submits logic A. PROD's CURRENT logic
/// is B, so there is NO equivalent for logic A under another name — the submit
/// must EXECUTE, not clone the stale A description of PROD.
#[tokio::test]
async fn cross_name_clone_source_must_reflect_current_logic() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let prod = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"ORDERS\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"ORDERS\"";
    let raw_epoch = 1_791_320_590_839i64;

    // 1) Build PROD with logic A, confirm.
    execute_and_confirm(
        &mut sql,
        &mut exec,
        submit(prod, "prod_state_demo", 1_791_743_740_000, raw_epoch, ""),
        1_791_743_740_000,
    )
    .await;

    // 2) Rebuild PROD with logic B (added column) — physical PROD now holds B.
    execute_and_confirm(
        &mut sql,
        &mut exec,
        submit(
            prod,
            "prod_state_demo",
            1_791_743_750_000,
            raw_epoch,
            ", upper(customer_id) as c",
        ),
        1_791_743_750_000,
    )
    .await;

    // 3) New target DEV (absent) submits logic A. PROD's current logic is B, so
    //    no equivalent for A exists under another name: MUST EXECUTE.
    let dev_a = sql
        .submit_enriched_sql(submit(dev, "dev_clone_demo", 1, raw_epoch, ""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&dev_a),
        "ready_to_execute",
        "clone source's CURRENT logic is B, not A: a stale A record must not make \
         PROD a clone source for logic A"
    );
}

/// P0#2 — clone→rebuild→old-logic adoption must not SKIP on stale clone state.
///
/// `DEV` is cloned from `PROD` (clone-origin row, logic A), then rebuilt
/// normally with logic B (physical DEV now holds B). A resubmit of logic A must
/// NOT SKIP by resurrecting the stale clone-origin A row: DEV's CURRENT state is
/// the ordinary B row (not clone-origin), so clone adoption cannot fire on it.
///
/// LIVE-VERIFIED (experiments/p0_fallback_over_existing.sh): the hosted service
/// answers `ready_to_clone` here — an equivalent for logic A still exists under
/// another name (PROD), so it re-clones that equivalent into DEV, EVEN THOUGH
/// DEV already exists holding logic B. The unsafe outcome the P0#2 bug produced
/// was a `skip_execution` (reusing A's description for a target physically
/// holding B); that must never happen. We assert the decision is NOT
/// `skip_execution` and matches the hosted `ready_to_clone`.
#[tokio::test]
async fn clone_adoption_must_not_resurrect_rebuilt_target() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let prod = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
    let dev = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
    let raw_epoch = 1_791_320_590_839i64;
    let prod_epoch = 1_791_743_740_000i64;
    let clone_epoch = 1_791_743_784_000i64;

    // 1) Build the SOURCE in prod with logic A (records source logic for the
    //    clone-origin row to inherit), confirm.
    execute_and_confirm(
        &mut sql,
        &mut exec,
        submit(prod, "prod_state_demo", prod_epoch, raw_epoch, ""),
        prod_epoch,
    )
    .await;

    // 2) RegisterClone(source=prod, target=dev) + confirm → clone-origin row A
    //    (clone_source_table set, inherits logic A).
    let clone_resp = clone
        .register_clone(qc::CloneRequest {
            target_table: dev.into(),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            execution_type: 1,
            clone_source_table: prod.into(),
            clone_source_last_modified_epoch: Some(prod_epoch),
            clone_source_table_type: Some("TRANSIENT TABLE".into()),
            table_namespace: Some(NS.into()),
            labels: [(
                "dbt_node_unique_id".to_string(),
                "model.jaffle_shop.clone_probe".to_string(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let ready = clone_resp.ready_to_clone.expect("ready_to_clone");
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: ready.request_id,
            last_modified_epoch: Some(clone_epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );

    // 3) Rebuild DEV normally with logic B (added column). Physical DEV now
    //    holds B; this ordinary row is the latest confirmed state for DEV.
    execute_and_confirm(
        &mut sql,
        &mut exec,
        submit(
            dev,
            "dev_clone_demo",
            1_791_743_790_000,
            raw_epoch,
            ", upper(customer_id) as c",
        ),
        1_791_743_790_000,
    )
    .await;

    // 4) Resubmit logic A for DEV. DEV's latest confirmed state is the ordinary
    //    B row (not clone-origin), so the stale clone-origin A row must NOT be
    //    adopted → NO SKIP. Because an equivalent for logic A still exists under
    //    another name (PROD), the hosted-faithful outcome is `ready_to_clone`.
    let dev_a = sql
        .submit_enriched_sql(submit(
            dev,
            "dev_clone_demo",
            1_791_743_790_000,
            raw_epoch,
            "",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(
        response_variant(&dev_a),
        "skip_execution",
        "a stale clone-origin row (logic A) must NEVER be resurrected to SKIP a \
         target that physically holds logic B"
    );
    assert_eq!(
        response_variant(&dev_a),
        "ready_to_clone",
        "LIVE-VERIFIED: an equivalent for logic A exists under another name (PROD), \
         so the hosted service re-clones it into DEV even though DEV exists holding B"
    );
}
