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
use support::scenario::{model, outcome, Scenario};
use support::start_server;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/template_compare_unrendered.jsonl"
);

const TARGET: &str = "\"DB\".\"S\".\"TPL\"";

/// Template CHANGED, rendered SAME -> SKIP (the finding-6 fix).
#[tokio::test]
async fn template_changed_rendered_same_skips() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    // A `compare_unrendered_code=true` submit with a given rendered SQL + body
    // (template) hash over one upstream.
    let cuc = model(TARGET)
        .catalog("DB")
        .schema("S")
        .compare_unrendered_code()
        .own_epoch(Some(100))
        .namespace("tpl-ns")
        .unique_id("model.jaffle.tpl")
        .project("p")
        .target_name("prod");
    let cuc_submit = |sql: &str, body_hash: &str| cuc.clone().sql(sql).body(body_hash);

    // Build + confirm with template A, rendered R.
    sc.build_with(
        cuc_submit("select 1 as id, 42 as amount", "body-A"),
        outcome().epoch(100).table(),
    )
    .await;

    // Only the template (body hash) changes; rendered SQL identical -> SKIP.
    assert_eq!(
        sc.verdict(cuc_submit("select 1 as id, 42 as amount", "body-B"))
            .await,
        "skip_execution",
        "cuc: a template-only change with identical rendered SQL must skip"
    );
}

/// Template SAME, rendered CHANGED -> SKIP (the other documented cell).
#[tokio::test]
async fn rendered_changed_template_same_skips() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let cuc = model(TARGET)
        .catalog("DB")
        .schema("S")
        .compare_unrendered_code()
        .own_epoch(Some(100))
        .namespace("tpl-ns")
        .unique_id("model.jaffle.tpl")
        .project("p")
        .target_name("prod");
    let cuc_submit = |sql: &str, body_hash: &str| cuc.clone().sql(sql).body(body_hash);

    sc.build_with(
        cuc_submit("select 'aaa' as run_id, 1 as x", "body-A"),
        outcome().epoch(100).table(),
    )
    .await;

    // Rendered SQL changes, template (body hash) identical -> SKIP.
    assert_eq!(
        sc.verdict(cuc_submit("select 'bbb' as run_id, 1 as x", "body-A"))
            .await,
        "skip_execution",
        "cuc: a rendered-only change with identical template must skip"
    );
}

/// Template CHANGED AND rendered CHANGED -> EXECUTE (only when BOTH change).
#[tokio::test]
async fn template_and_rendered_both_changed_executes() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let cuc = model(TARGET)
        .catalog("DB")
        .schema("S")
        .compare_unrendered_code()
        .own_epoch(Some(100))
        .namespace("tpl-ns")
        .unique_id("model.jaffle.tpl")
        .project("p")
        .target_name("prod");
    let cuc_submit = |sql: &str, body_hash: &str| cuc.clone().sql(sql).body(body_hash);

    sc.build_with(
        cuc_submit("select 1 as id, 42 as amount", "body-A"),
        outcome().epoch(100).table(),
    )
    .await;

    assert_eq!(
        sc.verdict(cuc_submit("select 1 as id, 99 as amount", "body-B"))
            .await,
        "ready_to_execute",
        "cuc: a change to BOTH template and rendered SQL must execute"
    );
}

/// A genuine DEPENDENCY change still invalidates reuse under cuc (both the
/// rendered and template identities fold in dependency definitions).
#[tokio::test]
async fn dependency_change_executes_under_cuc() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let cuc = model(TARGET)
        .catalog("DB")
        .schema("S")
        .compare_unrendered_code()
        .own_epoch(Some(100))
        .namespace("tpl-ns")
        .unique_id("model.jaffle.tpl")
        .project("p")
        .target_name("prod")
        .sql("select * from DB.S.V")
        .body("body-A");
    let with_dep = |q: &str| {
        cuc.clone()
            .dependency_in("\"DB\".\"S\".\"V\"", q, "DB", "S")
    };

    let base = with_dep("select 1 as id");
    sc.build_with(base, outcome().epoch(100).table()).await;

    // Same template + rendered, but the dependency DEFINITION changed -> execute.
    let changed = with_dep("select 2 as id");
    assert_eq!(
        sc.verdict(changed).await,
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
