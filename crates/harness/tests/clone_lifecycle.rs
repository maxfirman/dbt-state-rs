//! CLONE LIFECYCLE — register -> confirm -> cross-environment reuse.
//!
//! GOLDEN SOURCE (live, api.state.dbt.com, Fusion client, Snowflake). The real
//! dev-clone flow for `clone_probe` (an incremental model) was characterized via
//! the recording proxy (experiments/clone_experiment.sh + clone_logic_
//! sensitivity.sh). The verified contract:
//!
//!   1. The SOURCE model is built in prod (SubmitEnrichedSQL -> execute ->
//!      ConfirmExecution), recording its logic.
//!   2. A dev build with the dev object absent and `defer_to_target: prod` +
//!      `allow_clones: true` issues RegisterClone(source=prod, target=dev) ->
//!      ready_to_clone, then ConfirmExecution for the clone target.
//!   3. A subsequent SubmitEnrichedSQL for the dev (clone) target SKIPS when its
//!      logic matches the source — EVEN THOUGH the rendered SQL differs from the
//!      prod source ONLY in the physical schema qualifier (prod_state_demo vs
//!      dev_clone_demo). VERIFIED: `clone_logic_sensitivity.sh` STEP A.
//!   4. A dev submit whose logic GENUINELY changes (added column) EXECUTES
//!      (skip_rejection_reason = 8, LATEST_QUERY_HASH_NOT_MATCH). VERIFIED:
//!      STEP B.
//!
//! So post-clone reuse is CROSS-ENVIRONMENT (schema-abstracted) and
//! LOGIC-SENSITIVE. The server reproduces it by inheriting the clone source's
//! schema-abstracted logic identity (fingerprint::namespace_logic_hash) onto the
//! clone-origin row at RegisterClone, and matching a later submit's abstracted
//! logic against it. See docs/correctness-handoff.md.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::clone_client::CloneClient;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, response_variant, start_server};

const NS: &str = "fc38a5ba0eceb701c6ab7c6b882e28b0";
const PROD: &str = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"CLONE_PROBE\"";
const DEV: &str = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CLONE_PROBE\"";
const RAW: &str = "\"CDO_DEMO_DB\".\"RAW\".\"RAW_ORDERS\"";

fn tbl(name: &str, epoch: i64) -> qc::TableModifiedInfo {
    qc::TableModifiedInfo {
        name: name.into(),
        last_modified_epoch: Some(epoch),
    }
}

/// An incremental model submit for `target`, built from the same logical model
/// (the physical schema in the SQL/deps mirrors `schema`). `extra_col` injects a
/// genuine projection change when non-empty.
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
        execution_type: 3,
        sql,
        tolerate_nondeterminism: true,
        freshness_tolerance_seconds: 2700,
        tables: vec![tbl(target, own_epoch), tbl(RAW, raw_epoch)],
        query_dependencies: vec![qc::QueryDependency {
            name: format!("\"CDO_DEMO_DB\".\"{}\".\"STG_ORDERS\"", schema.to_uppercase()),
            query: format!("create or replace view STG_ORDERS as (select * from CDO_DEMO_DB.{schema}.raw_orders)"),
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
            ("dbt_node_unique_id".to_string(), "model.jaffle_shop.clone_probe".to_string()),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    }
}

async fn confirm(
    exec: &mut ExecutionClient<tonic::transport::Channel>,
    resp: qc::SubmitSqlResponse,
    epoch: i64,
) {
    let qc::submit_sql_response::Response::ReadyToExecute(r) = resp.response.unwrap() else {
        panic!("expected execute before confirm");
    };
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: r.request_id,
            last_modified_epoch: Some(epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );
}

/// Full controlled lifecycle: prod build (records source logic) -> clone+confirm
/// -> cross-environment reuse (same logic skips; changed logic executes).
#[tokio::test]
async fn clone_register_confirm_then_cross_env_submit_skips() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let prod_epoch = 1_791_743_740_667i64;
    let clone_epoch = 1_791_743_784_002i64;
    let raw_epoch = 1_791_320_590_839i64; // older than both builds

    // 1) Build the SOURCE in prod: execute + confirm (records source logic).
    let r0 = sql
        .submit_enriched_sql(submit(PROD, "prod_state_demo", prod_epoch, raw_epoch, ""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&r0),
        "ready_to_execute",
        "fresh prod source builds"
    );
    confirm(&mut exec, r0, prod_epoch).await;

    // 2) RegisterClone(source=prod, target=dev) -> ready_to_clone, then confirm.
    let clone_resp = clone
        .register_clone(qc::CloneRequest {
            target_table: DEV.into(),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            execution_type: 3,
            clone_source_table: PROD.into(),
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
    assert_eq!(ready.explained_decision.as_ref().unwrap().decision, 3);
    assert!(
        exec.confirm_execution(qc::ConfirmExecutionRequest {
            request_id: ready.request_id,
            last_modified_epoch: Some(clone_epoch),
            table_type: Some("TRANSIENT TABLE".into()),
            failed_to_clone: false,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .success
    );

    // 3) A dev submit for the clone target with the SAME model logic (schema
    //    differs: dev_clone_demo vs prod_state_demo) must SKIP — reproducing the
    //    hosted cross-environment post-clone reuse. Upstream RAW is older than
    //    the clone build epoch (fresh).
    let dev_same = sql
        .submit_enriched_sql(submit(DEV, "dev_clone_demo", clone_epoch, raw_epoch, ""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&dev_same),
        "skip_execution",
        "post-clone submit with same (schema-abstracted) logic must SKIP"
    );

    // 4) A dev submit whose logic GENUINELY changes (added column) must EXECUTE,
    //    even though the physical target exists — reproducing the hosted
    //    LATEST_QUERY_HASH_NOT_MATCH behavior (clone_logic_sensitivity STEP B).
    let dev_changed = sql
        .submit_enriched_sql(submit(
            DEV,
            "dev_clone_demo",
            clone_epoch,
            raw_epoch,
            ", upper(customer_id) as c",
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&dev_changed),
        "ready_to_execute",
        "a genuine logic change after a clone must EXECUTE (logic-sensitive reuse)"
    );
}

/// SAFETY: a cross-environment submit must NOT skip when no clone/source logic
/// was ever recorded (no provenance). Without the inherited logic identity, the
/// clone-origin row cannot be adopted and the submit executes.
#[tokio::test]
async fn cross_env_submit_without_source_logic_executes() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let clone_epoch = 1_791_743_784_002i64;
    let raw_epoch = 1_791_320_590_839i64;

    // Register+confirm a clone WITHOUT ever recording the source's logic.
    let clone_resp = clone
        .register_clone(qc::CloneRequest {
            target_table: DEV.into(),
            dialect: "snowflake".into(),
            default_catalog: "CDO_DEMO_DB".into(),
            execution_type: 3,
            clone_source_table: PROD.into(),
            clone_source_table_type: Some("TRANSIENT TABLE".into()),
            table_namespace: Some(NS.into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let ready = clone_resp.ready_to_clone.unwrap();
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: ready.request_id,
        last_modified_epoch: Some(clone_epoch),
        table_type: Some("TRANSIENT TABLE".into()),
        ..Default::default()
    })
    .await
    .unwrap();

    let dev = sql
        .submit_enriched_sql(submit(DEV, "dev_clone_demo", clone_epoch, raw_epoch, ""))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response_variant(&dev),
        "ready_to_execute",
        "no recorded source logic => cannot prove equivalence => must EXECUTE"
    );
}

/// Fixture replay: the real register->confirm->submit from clone_happy_path.
/// This pins the captured RegisterClone/Confirm shapes against our server end to
/// end (the submit's reuse depends on cross-session history absent from the
/// file, so only the register+confirm successes are asserted here).
#[tokio::test]
async fn clone_happy_path_register_confirm_shapes() {
    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_happy_path.jsonl"
    );
    let entries = diff::load_golden(FIXTURE).expect("load clone_happy_path");
    let register = entries
        .iter()
        .find(|e| e.method == "RegisterClone")
        .unwrap();
    let confirm_entry = entries
        .iter()
        .find(|e| e.method == "ConfirmExecution")
        .unwrap();

    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut clone = CloneClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let creq: qc::CloneRequest = serde_json::from_value(register.request.clone()).unwrap();
    let cresp = clone
        .register_clone(support::captured_request(creq, register))
        .await
        .unwrap()
        .into_inner();
    let ready = cresp.ready_to_clone.expect("ready_to_clone");
    assert_eq!(ready.explained_decision.as_ref().unwrap().decision, 3);

    let mut confirm_req: qc::ConfirmExecutionRequest =
        serde_json::from_value(confirm_entry.request.clone()).unwrap();
    confirm_req.request_id = ready.request_id;
    assert!(
        exec.confirm_execution(support::captured_request(confirm_req, confirm_entry))
            .await
            .unwrap()
            .into_inner()
            .success
    );
}
