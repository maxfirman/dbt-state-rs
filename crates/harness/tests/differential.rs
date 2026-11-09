//! Differential tests: replay real golden requests against our server and
//! assert the decisions match the hosted service's observed behavior.
//!
//! Requires a running Postgres (the project's dbt-state-pg container).
//! Set DATABASE_URL or rely on the default localhost:55441 dsn. Each test runs
//! in its own uniquely-named schema so runs are isolated and repeatable.

use dbt_state_harness::diff::{self, GoldenEntry};
use dbt_state_proto::query_cache as qc;

// Shared isolated-schema server setup (see tests/support.rs). Included via
// #[path] so differential.rs and robustness.rs share one copy of the helpers
// while keeping the server/sqlx/tokio deps dev-only.
#[path = "support.rs"]
mod support;
use support::demo::{self, transient_at, DEV_SCHEMA, NS, PROD_SCHEMA};
use support::rows;
use support::scenario::{into_clone, into_skip, outcome, ready_id, seed, variant, view, Scenario};
use support::{channel, start_server};

const GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/golden_20261008T222744.061Z.jsonl"
);

/// Decode a golden request JSON value (a captured SubmitEnrichedSqlRequest).
fn golden_request<T: serde::de::DeserializeOwned>(e: &GoldenEntry) -> T {
    serde_json::from_value(e.request.clone()).expect("decode SubmitEnrichedSqlRequest")
}

fn find_first_execute(entries: &[GoldenEntry]) -> &GoldenEntry {
    entries
        .iter()
        .find(|e| {
            e.method == "SubmitEnrichedSQL"
                && diff::decision_variant(&e.response).as_deref() == Some("ready_to_execute")
        })
        .expect("a ready_to_execute golden entry")
}

/// Core causal loop: a never-seen node must EXECUTE; after ConfirmExecution,
/// an identical submit must SKIP. This is the heart of the protocol and is
/// fully reproducible from an empty store.
#[tokio::test]
async fn execute_then_confirm_then_skip() {
    let entries = diff::load_golden(GOLDEN).expect("load golden");
    let exec_entry = find_first_execute(&entries);
    let req: qc::SubmitEnrichedSqlRequest = golden_request(exec_entry);

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // 1) First submit of a never-seen fingerprint -> EXECUTE.
    let resp1 = sc.submit(req.clone()).await;
    let variant1 = variant(&resp1);
    assert_eq!(variant1, "ready_to_execute", "first submit must execute");

    let request_id = ready_id(&resp1);
    assert!(!request_id.is_empty());

    // 2) Confirm the execution.
    // The captured outcome, echoing the submit's labels.
    let built = req.labels.iter().fold(
        outcome().epoch(1_791_498_542_120).runtime(3205),
        |o, (k, v)| o.label(k, v),
    );
    let confirm = built.confirm(&request_id);
    let cresp = sc.try_confirm(confirm).await.expect("confirm");
    assert!(cresp.success, "confirm must succeed for a known request_id");
    assert_eq!(cresp.request_id, request_id);

    // 3) Identical submit again -> SKIP (no-op), because the fingerprint is now
    //    confirmed and upstream data is unchanged.
    let resp2 = sc.submit(req.clone()).await;
    assert_eq!(
        variant(&resp2),
        "skip_execution",
        "second identical submit must skip"
    );
}

/// ValidateClientVersion must return is_supported=true, matching the real service.
#[tokio::test]
async fn validate_client_version_supported() {
    use qc::client_validation_client::ClientValidationClient;
    let server = start_server().await;
    let addr = server.addr;
    let ch = channel(addr).await;
    let mut client = ClientValidationClient::new(ch);
    let resp = client
        .validate_client_version(qc::ValidateClientVersionRequest {
            dbt_run_cache_version: "2.0.6".to_string(),
        })
        .await
        .expect("validate")
        .into_inner();
    assert!(resp.is_supported);
}

/// Item 1: a confirmed row's stored execution_runtime_ms must be echoed back on
/// the subsequent SKIP. Confirm with runtime 3205, then an identical submit
/// skips and reports execution_runtime_ms == 3205.
#[tokio::test]
async fn skip_echoes_confirmed_runtime_ms() {
    let entries = diff::load_golden(GOLDEN).expect("load golden");
    let exec_entry = find_first_execute(&entries);
    let req: qc::SubmitEnrichedSqlRequest = golden_request(exec_entry);

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    // Execute, then confirm with a known runtime.
    let request_id = sc.execute_id(req.clone()).await;
    // The captured outcome, echoing the submit's labels.
    let built = req.labels.iter().fold(
        outcome().epoch(1_791_498_542_120).runtime(3205),
        |o, (k, v)| o.label(k, v),
    );
    assert!(sc.confirm(&request_id, built).await);

    // Identical submit -> SKIP with echoed runtime.
    let skip = sc.expect_skip(req.clone()).await;
    assert_eq!(
        skip.execution_runtime_ms,
        Some(3205),
        "skip must echo the confirmed runtime"
    );
}

/// Item 2: RecordExecutions inserts confirmed rows directly. Record 2
/// executions, assert records_stored==2 and that 2 rows exist in the store.
#[tokio::test]
async fn record_executions_stores_batch() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let mk = |table: &str, hash: &str| {
        view(table)
            .sql("select 1")
            .no_tables()
            .unique_id(&format!("model.jaffle.{table}"))
            .target_name("prod")
            .body(hash)
            .node_state(|state| {
                state.project_name = "jaffle".into();
                state.resource_type = "model".into();
                state.profile_name = "default".into();
            })
            .record(outcome().epoch(1_791_498_000_000).table().runtime(1500))
    };

    let stored = sc
        .record([
            mk("\"DB\".\"S\".\"A\"", "ha"),
            mk("\"DB\".\"S\".\"B\"", "hb"),
        ])
        .await
        .expect("record");
    assert_eq!(stored, 2, "both records must be stored");

    // Assert the rows actually exist and are confirmed.
    let confirmed = rows::executions(&server)
        .await
        .iter()
        .filter(|row| row.is_confirmed())
        .count();
    assert_eq!(confirmed, 2, "two confirmed rows must exist");
}

/// Item 3: SubmitValues (seeds) execute -> confirm -> skip loop, matched on
/// values_hash. First submit of a new values_hash executes; after confirm an
/// identical submit skips.
#[tokio::test]
async fn submit_values_execute_confirm_skip() {
    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let req = seed("\"DB\".\"S\".\"SEED\"")
        .values("md5deadbeef")
        .own_epoch(Some(1_791_498_000_000));

    // 1) never-seen seed -> EXECUTE.
    let resp1 = sc.submit_values(req.clone()).await;
    let request_id = ready_id(&resp1);
    assert!(!request_id.is_empty());

    // 2) confirm.
    assert!(
        sc.confirm(&request_id, outcome().epoch(1_791_498_000_000).runtime(777))
            .await
    );

    // 3) identical seed submit -> SKIP, echoing the recorded runtime.
    let skip = into_skip(sc.submit_values(req.clone()).await);
    assert_eq!(skip.execution_runtime_ms, Some(777));

    // A different values_hash must still EXECUTE (no match).
    let req2 = req.values("md5feedface");
    assert_eq!(sc.values_verdict(req2).await, "ready_to_execute");
}

/// Item 4: the Explain service now re-derives the structured tree on demand.
/// A submitted node's `execution_decision_id` yields its cataloged explain tree
/// via `GetExplainMessages` (ADR 0005); the SelectorService remains implemented.
#[tokio::test]
async fn explain_rederives_tree_and_selector_is_implemented() {
    use qc::explain_client::ExplainClient;

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;
    let ch = sc.channel();

    // Submit a never-seen node so it EXECUTEs (first build). The server mints an
    // execution_decision_id and records the decision (best-effort capture).
    let entries = diff::load_golden(GOLDEN).expect("load golden");
    let req: qc::SubmitEnrichedSqlRequest = golden_request(find_first_execute(&entries));
    let exec = sc.expect_execute(req).await;
    let edid = exec.execution_decision_id.expect("minted edid");

    // An UNTRACKED id yields no message (tracked ids are omitted when absent).
    let mut explain = ExplainClient::new(ch.clone());
    let untracked = explain
        .get_explain_messages(qc::GetExplainMessagesRequest {
            execution_decision_ids: vec!["definitely-not-a-real-edid".to_string()],
        })
        .await
        .expect("get_explain_messages")
        .into_inner();
    assert!(
        untracked.messages.is_empty(),
        "untracked ids must be omitted from the response"
    );

    // The tracked id yields exactly one re-derived message. Capture is a
    // best-effort async write, so poll briefly until it lands.
    let mut got = None;
    for _ in 0..50 {
        let r = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![edid.clone()],
            })
            .await
            .expect("get_explain_messages")
            .into_inner();
        if let Some(m) = r.messages.into_iter().next() {
            got = Some(m);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let msg = got.expect("the tracked decision's explain message must be re-derived");
    assert_eq!(msg.execution_decision_id, edid);
    assert_eq!(msg.decision, 1, "EXECUTE decision int");
    // First build → the table-analysis-only shape (catalog "First build").
    assert_eq!(
        msg.explain_lines.len(),
        1,
        "first build: table analysis only"
    );
    let ta = &msg.explain_lines[0];
    assert!(ta.text.starts_with("table analysis ("));
    assert_eq!(ta.children.len(), 2);
    assert_eq!(ta.children[0].text, "the model table does not exist");
    assert_eq!(ta.children[0].marker, Some(2));
    assert_eq!(
        ta.children[1].text,
        "there is no other table that the model could be cloned from"
    );

    // SelectorService.GetStateSelection is implemented (state:* selectors). With
    // an empty node set it returns an empty selection — not UNIMPLEMENTED.
    let sel = sc
        .try_select(qc::SelectorRequest {
            target: "prod".to_string(),
            selector_criteria: 1,
            project_id: "p".to_string(),
            nodes: Vec::new(),
        })
        .await
        .expect("get_state_selection");
    assert!(sel.is_empty(), "empty submission must select nothing");
}

/// F2 REGRESSION — the CLONE explain tree must render the ACTUAL clone source,
/// not an empty string. A cross-name CLONE captured via `SubmitEnrichedSQL`
/// (the production `ReadyToClone` path) must persist `clone_source` so the
/// explain renderer emits `the model table could be cloned from "<source>"`.
/// Before the fix, `capture_submit` hard-coded `clone_source: None`, so the
/// re-derived tree showed `could be cloned from ""` (empty). This drives the
/// WHOLE path end-to-end (submit → best-effort capture → GetExplainMessages)
/// without ever handing the source to the renderer manually.
#[tokio::test]
async fn explain_clone_tree_renders_actual_clone_source() {
    use qc::explain_client::ExplainClient;

    // A cross-name CLONE submit: same `table_namespace` + schema-abstracted
    // logic as an already-built equivalent under a DIFFERENT physical name.
    let submit = |target: &str, schema: &str| {
        demo::model(target, schema, "model.jaffle_shop.f2_clone_probe")
            .sql(&format!("select id from CDO_DEMO_DB.{schema}.lp_up_a"))
            .namespace(NS)
    };

    let server = start_server().await;
    let sc = Scenario::connect(&server).await;

    let a = "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"F2_CLONE_PROBE\"";
    let b = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"F2_CLONE_PROBE\"";
    let a_epoch = 1_791_747_221_679i64;

    // Build + confirm the equivalent under name A.
    sc.build_with(submit(a, PROD_SCHEMA), transient_at(a_epoch).runtime(15275))
        .await;

    // Submit for absent B (same namespace + logic) → ReadyToClone A→B. This is
    // the production SubmitEnrichedSQL CLONE path whose capture must persist the
    // source.
    let rb = sc.submit(submit(b, DEV_SCHEMA)).await;
    assert_eq!(
        variant(&rb),
        "ready_to_clone",
        "cross-name equivalent must clone"
    );
    let c = into_clone(rb);
    assert_eq!(c.clone_source, a, "the response's clone source is A");
    let edid = c
        .execution_decision_id
        .clone()
        .expect("clone minted an execution_decision_id");

    // Re-derive the explain tree for the CLONE decision. Capture is a
    // best-effort async write, so poll briefly until it lands.
    let mut explain = ExplainClient::new(sc.channel());
    let mut got = None;
    for _ in 0..50 {
        let r = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![edid.clone()],
            })
            .await
            .expect("get_explain_messages")
            .into_inner();
        if let Some(m) = r.messages.into_iter().next() {
            got = Some(m);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let msg = got.expect("the CLONE decision's explain message must be re-derived");
    assert_eq!(msg.decision, 3, "READY_TO_CLONE decision int");

    // The clone-source line is children[1] of the `table analysis` section. It
    // must name the ACTUAL source relation A — NOT an empty string.
    let ta = &msg.explain_lines[0];
    assert!(ta.text.starts_with("table analysis ("));
    let clone_line = &ta.children[1];
    assert_eq!(
        clone_line.text,
        format!("the model table could be cloned from {a}"),
        "the rendered clone source must be the actual source relation, not empty"
    );
    assert!(
        !clone_line.text.ends_with("could be cloned from "),
        "clone source must never render empty (the F2 regression)"
    );
    assert_eq!(clone_line.marker, Some(1), "MARKER_SUCCESS");
}
