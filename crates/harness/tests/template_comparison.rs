//! COMPARE_UNRENDERED_CODE template identity (issue #2, finding 6) —
//! live-captured from api.state.dbt.com (Fusion client, Snowflake).
//!
//! With `compare_unrendered_code = true`, the documented contract is that a
//! rebuild occurs ONLY when BOTH the unrendered Jinja template AND the rendered
//! SQL have changed; equivalently, a node is REUSED when EITHER is unchanged.
//! Two cells are live-verified:
//!
//! - template SAME, rendered CHANGED -> SKIP (golden/fixtures/
//!   compare_unrendered_code.jsonl: body_hash aace1291 both, sql 'aaa' -> 'bbb',
//!   second submit skips).
//! - template CHANGED, rendered SAME -> SKIP (golden/fixtures/
//!   template_compare_unrendered.jsonl: a Jinja-comment-only edit changes
//!   body_hash ea3ff25d -> 6466c9cd, rendered SQL identical, second submit
//!   skips; experiments/template_comparison.sh).
//!
//! BUG this pins/fixes: the server used `template = node_body_hash`
//! EXCLUSIVELY for the logic hash when `compare_unrendered_code = true`,
//! ignoring the rendered SQL. So a template-changed/rendered-same submit got a
//! different fingerprint and EXECUTED, contradicting the hosted SKIP. The fix
//! stores BOTH the rendered hash and the template hash and reuses when EITHER
//! matches the recorded row.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, response_variant, start_server};
use tonic::transport::Channel;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/template_compare_unrendered.jsonl"
);

/// A `compare_unrendered_code=true` submit with a given rendered SQL + body
/// (template) hash over one upstream.
fn cuc_submit(sql: &str, body_hash: &str) -> qc::SubmitEnrichedSqlRequest {
    qc::SubmitEnrichedSqlRequest {
        target_table: Some("\"DB\".\"S\".\"TPL\"".into()),
        dialect: "snowflake".into(),
        default_catalog: "DB".into(),
        default_schema: Some("S".into()),
        execution_type: 1,
        sql: sql.into(),
        tolerate_nondeterminism: true,
        compare_unrendered_code: true,
        tables: vec![qc::TableModifiedInfo {
            name: "\"DB\".\"S\".\"TPL\"".into(),
            last_modified_epoch: Some(100),
        }],
        table_namespace: Some("tpl-ns".into()),
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.jaffle.tpl".into(),
            project_id: Some("p".into()),
            target_name: "prod".into(),
            node_body_hash: Some(body_hash.into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn submit(sql: &mut SqlClient<Channel>, req: qc::SubmitEnrichedSqlRequest) -> String {
    response_variant(
        &sql.submit_enriched_sql(req)
            .await
            .expect("submit")
            .into_inner(),
    )
    .to_string()
}

async fn submit_execute_id(
    sql: &mut SqlClient<Channel>,
    req: qc::SubmitEnrichedSqlRequest,
) -> String {
    match sql
        .submit_enriched_sql(req)
        .await
        .expect("submit")
        .into_inner()
        .response
    {
        Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => r.request_id,
        other => panic!("expected ready_to_execute, got {other:?}"),
    }
}

async fn confirm(exec: &mut ExecutionClient<Channel>, request_id: String) {
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id,
        last_modified_epoch: Some(100),
        table_type: Some("TABLE".into()),
        ..Default::default()
    })
    .await
    .expect("confirm");
}

/// Template CHANGED, rendered SAME -> SKIP (the finding-6 fix).
#[tokio::test]
async fn template_changed_rendered_same_skips() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Build + confirm with template A, rendered R.
    let rid = submit_execute_id(
        &mut sql,
        cuc_submit("select 1 as id, 42 as amount", "body-A"),
    )
    .await;
    confirm(&mut exec, rid).await;

    // Only the template (body hash) changes; rendered SQL identical -> SKIP.
    assert_eq!(
        submit(
            &mut sql,
            cuc_submit("select 1 as id, 42 as amount", "body-B")
        )
        .await,
        "skip_execution",
        "cuc: a template-only change with identical rendered SQL must skip"
    );
}

/// Template SAME, rendered CHANGED -> SKIP (the other documented cell).
#[tokio::test]
async fn rendered_changed_template_same_skips() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let rid = submit_execute_id(
        &mut sql,
        cuc_submit("select 'aaa' as run_id, 1 as x", "body-A"),
    )
    .await;
    confirm(&mut exec, rid).await;

    // Rendered SQL changes, template (body hash) identical -> SKIP.
    assert_eq!(
        submit(
            &mut sql,
            cuc_submit("select 'bbb' as run_id, 1 as x", "body-A")
        )
        .await,
        "skip_execution",
        "cuc: a rendered-only change with identical template must skip"
    );
}

/// Template CHANGED AND rendered CHANGED -> EXECUTE (only when BOTH change).
#[tokio::test]
async fn template_and_rendered_both_changed_executes() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let rid = submit_execute_id(
        &mut sql,
        cuc_submit("select 1 as id, 42 as amount", "body-A"),
    )
    .await;
    confirm(&mut exec, rid).await;

    assert_eq!(
        submit(
            &mut sql,
            cuc_submit("select 1 as id, 99 as amount", "body-B")
        )
        .await,
        "ready_to_execute",
        "cuc: a change to BOTH template and rendered SQL must execute"
    );
}

/// A genuine DEPENDENCY change still invalidates reuse under cuc (both the
/// rendered and template identities fold in dependency definitions).
#[tokio::test]
async fn dependency_change_executes_under_cuc() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let dep = |q: &str| qc::QueryDependency {
        name: "\"DB\".\"S\".\"V\"".into(),
        query: q.into(),
        default_catalog: "DB".into(),
        default_schema: "S".into(),
    };
    let mut base = cuc_submit("select * from DB.S.V", "body-A");
    base.query_dependencies = vec![dep("select 1 as id")];
    let rid = submit_execute_id(&mut sql, base.clone()).await;
    confirm(&mut exec, rid).await;

    // Same template + rendered, but the dependency DEFINITION changed -> execute.
    let mut changed = base.clone();
    changed.query_dependencies = vec![dep("select 2 as id")];
    assert_eq!(
        submit(&mut sql, changed).await,
        "ready_to_execute",
        "cuc: a changed dependency definition must still execute"
    );
}

/// Pin the hosted truth directly from the live capture: cuc=true, the two real
/// submits share rendered SQL, carry DIFFERENT body hashes, and the second is a
/// SKIP.
#[test]
fn hosted_template_changed_rendered_same_shape_is_pinned() {
    let entries = diff::load_golden(FIXTURE).expect("load template fixture");
    let submits: Vec<&diff::GoldenEntry> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .collect();
    assert_eq!(submits.len(), 2, "baseline + template-changed re-submit");

    for s in &submits {
        assert_eq!(
            s.request["compare_unrendered_code"].as_bool(),
            Some(true),
            "both submits set compare_unrendered_code"
        );
    }
    let body = |i: usize| {
        submits[i].request["dbt_node_state"]["node_body_hash"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let rendered = |i: usize| {
        submits[i].request["sql"]
            .as_str()
            .unwrap()
            .trim()
            .to_string()
    };
    assert_ne!(body(0), body(1), "the template (body hash) changed");
    assert_eq!(
        rendered(0),
        rendered(1),
        "the rendered SQL is byte-identical"
    );
    assert_eq!(
        diff::decision_variant(&submits[0].response).unwrap(),
        "ready_to_execute",
        "first build executes"
    );
    assert_eq!(
        diff::decision_variant(&submits[1].response).unwrap(),
        "skip_execution",
        "a template-only change with identical rendered SQL SKIPS (hosted truth)"
    );
}
