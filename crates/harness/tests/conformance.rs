//! Strategy #1 — exhaustive golden conformance replay.
//!
//! Replays EVERY captured request from the real `api.state.dbt.com` traffic
//! against our server, in recorded order, and asserts our decisions are
//! causally consistent with the hosted service.
//!
//! Why not a naive "assert every response matches": the captured runs begin
//! with SKIP decisions that depend on state the real service already held
//! before our capture window. An empty server cannot reproduce those leading
//! skips. So we assert the part that IS causally reproducible from empty:
//!
//!   * When the real verdict is `ready_to_execute`, our server (which has not
//!     yet confirmed that fingerprint in this replay) MUST also execute.
//!   * When the real verdict is `skip_execution` AND we have already confirmed
//!     that fingerprint earlier in THIS replay, our server MUST also skip
//!     (the reproducible execute→confirm→skip transition).
//!   * Leading `skip_execution`s for fingerprints we have not yet seen are
//!     "baseline" skips (pre-capture state); we don't assert on them but we DO
//!     hydrate our server (via RecordExecutions) so downstream causal
//!     assertions have the same baseline the real service had.
//!   * `ValidateClientVersion` and `SubmitEnrichedSQLSpeculative` must match
//!     their real verdicts exactly (is_supported=true / undecided).
//!
//! This turns the full real-traffic sample into an automatic conformance gate
//! and is the strongest guard against future regressions in decision logic.

#[path = "support.rs"]
mod support;

use std::collections::HashMap;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;

use qc::client_validation_client::ClientValidationClient;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;

const CORPORA: &[&str] = &[
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/golden_20261008T222744.061Z.jsonl"
    ),
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/golden_20261009T083754.534Z.jsonl"
    ),
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/golden_20261009T100637.316Z.jsonl"
    ),
];

/// Fingerprint key identifying a node execution for causal tracking.
/// Includes default_schema so we only assert causal skips WITHIN the same
/// environment. Cross-environment matching (same table_namespace+body_hash,
/// different schema) is a known deferral-state gap tracked separately — see
/// `cross_environment_namespace_skip_is_a_known_gap`.
fn fingerprint(req: &serde_json::Value) -> String {
    let target = req
        .get("target_table")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let etype = req
        .get("execution_type")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let body = req
        .get("dbt_node_state")
        .and_then(|s| s.get("node_body_hash"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let schema = req
        .get("default_schema")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    format!("{schema}|{target}|{etype}|{body}")
}

fn real_variant(resp: &serde_json::Value) -> Option<String> {
    diff::decision_variant(resp)
}

async fn run_corpus(path: &str) {
    let entries = diff::load_golden(path).unwrap_or_else(|e| panic!("load {path}: {e}"));
    assert!(!entries.is_empty(), "corpus {path} is empty");

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch.clone());
    let mut validate = ClientValidationClient::new(ch);

    // Fingerprints our server has confirmed during THIS replay (causally
    // reproducible => skips must match). Maps fingerprint -> our request_id of
    // the last ready_to_execute (so we can confirm with OUR id).
    let mut confirmed: std::collections::HashSet<String> = Default::default();
    let mut pending_rid: HashMap<String, String> = HashMap::new();
    // Map the node name -> fingerprint of the last submit, so a following
    // ConfirmExecution (which carries only labels) can be correlated.
    let mut last_fp_by_node: HashMap<String, String> = HashMap::new();

    let mut asserted_execute = 0usize;
    let mut asserted_skip = 0usize;
    let mut baseline_skips = 0usize;

    for (i, e) in entries.iter().enumerate() {
        match e.method.as_str() {
            "ValidateClientVersion" => {
                let resp = validate
                    .validate_client_version(qc::ValidateClientVersionRequest {
                        dbt_run_cache_version: "2.0.6".into(),
                    })
                    .await
                    .expect("validate")
                    .into_inner();
                let real = e.response.get("is_supported").and_then(|v| v.as_bool());
                assert_eq!(
                    Some(resp.is_supported),
                    real,
                    "entry {i}: validate mismatch"
                );
            }
            "SubmitEnrichedSQLSpeculative" => {
                let req: qc::SubmitEnrichedSqlRequest =
                    serde_json::from_value(e.request.clone()).expect("decode speculative");
                let resp = sql
                    .submit_enriched_sql_speculative(req)
                    .await
                    .expect("speculative")
                    .into_inner();
                // Real service returned `undecided` for every speculative call.
                let got = match resp.response {
                    Some(qc::submit_sql_speculative_response::Response::Undecided(_)) => {
                        "undecided"
                    }
                    Some(qc::submit_sql_speculative_response::Response::SkipExecution(_)) => {
                        "skip_execution"
                    }
                    Some(
                        qc::submit_sql_speculative_response::Response::ReadyToExecuteUntracked(_),
                    ) => "ready_to_execute_untracked",
                    Some(qc::submit_sql_speculative_response::Response::ReadyToClone(_)) => {
                        "ready_to_clone"
                    }
                    None => "none",
                };
                let real = real_variant(&e.response);
                assert_eq!(
                    Some(got.to_string()),
                    real,
                    "entry {i}: speculative mismatch"
                );
            }
            "SubmitEnrichedSQL" => {
                let node = e
                    .request
                    .get("labels")
                    .and_then(|l| l.get("dbt_node_name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let fp = fingerprint(&e.request);
                last_fp_by_node.insert(node.clone(), fp.clone());

                let req: qc::SubmitEnrichedSqlRequest =
                    serde_json::from_value(e.request.clone()).expect("decode submit");
                let resp = sql
                    .submit_enriched_sql(req)
                    .await
                    .expect("submit")
                    .into_inner();
                let ours = match &resp.response {
                    Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => {
                        pending_rid.insert(fp.clone(), r.request_id.clone());
                        "ready_to_execute"
                    }
                    Some(qc::submit_sql_response::Response::SkipExecution(_)) => "skip_execution",
                    Some(qc::submit_sql_response::Response::ReadyToClone(_)) => "ready_to_clone",
                    None => "none",
                };
                let real = real_variant(&e.response).unwrap_or_default();

                match real.as_str() {
                    "ready_to_execute" => {
                        // Reproducible from empty: a fingerprint we have not
                        // confirmed must execute for us too.
                        if !confirmed.contains(&fp) {
                            assert_eq!(
                                ours, "ready_to_execute",
                                "entry {i} node {node}: real executed (unseen fp) but we did not"
                            );
                            asserted_execute += 1;
                        }
                    }
                    "skip_execution" => {
                        if confirmed.contains(&fp) {
                            // Causally reproducible skip.
                            assert_eq!(
                                ours, "skip_execution",
                                "entry {i} node {node}: real skipped a confirmed fp but we did not"
                            );
                            asserted_skip += 1;
                        } else {
                            // Baseline skip (pre-capture state). Hydrate our
                            // store so later causal assertions share the baseline.
                            baseline_skips += 1;
                            hydrate_confirmed(&mut exec, &e.request).await;
                            confirmed.insert(fp.clone());
                        }
                    }
                    other => panic!("entry {i}: unexpected real submit variant {other}"),
                }
            }
            "ConfirmExecution" => {
                // Correlate to our pending request_id by node fingerprint.
                let node = e
                    .request
                    .get("labels")
                    .and_then(|l| l.get("dbt_node_name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if let Some(fp) = last_fp_by_node.get(&node).cloned() {
                    if let Some(rid) = pending_rid.get(&fp).cloned() {
                        let lme = e
                            .request
                            .get("last_modified_epoch")
                            .and_then(|v| v.as_i64());
                        let rt = e
                            .request
                            .get("execution_runtime_ms")
                            .and_then(|v| v.as_i64());
                        let resp = exec
                            .confirm_execution(qc::ConfirmExecutionRequest {
                                request_id: rid,
                                last_modified_epoch: lme,
                                failed_to_clone: false,
                                table_type: None,
                                execution_results: None,
                                execution_runtime_ms: rt,
                                labels: Default::default(),
                            })
                            .await
                            .expect("confirm")
                            .into_inner();
                        assert!(resp.success, "entry {i}: our confirm must succeed");
                        confirmed.insert(fp);
                    }
                }
            }
            "RegisterClone" | "ResolveDeferredRelations" => {
                // Covered by dedicated differential tests; skip in the lifecycle replay.
            }
            other => panic!("entry {i}: unhandled method {other}"),
        }
    }

    // Guard against the replay silently asserting nothing meaningful.
    assert!(
        asserted_execute + asserted_skip > 0,
        "corpus {path}: no causal assertions were made (execute={asserted_execute} skip={asserted_skip} baseline={baseline_skips})"
    );
    eprintln!(
        "conformance {path}: asserted_execute={asserted_execute} asserted_skip={asserted_skip} baseline_skips={baseline_skips}"
    );
}

/// Hydrate our store with a confirmed execution for a baseline-skip request, so
/// subsequent causal assertions share the same baseline the real service had.
/// Uses RecordExecutions (the bypass path) with the submit's own fields.
async fn hydrate_confirmed(
    exec: &mut ExecutionClient<tonic::transport::Channel>,
    req: &serde_json::Value,
) {
    let sql_req: qc::SubmitEnrichedSqlRequest = match serde_json::from_value(req.clone()) {
        Ok(r) => r,
        Err(_) => return,
    };
    let enriched = qc::SqlExecution {
        target_table: sql_req.target_table.clone(),
        dialect: sql_req.dialect.clone(),
        default_catalog: sql_req.default_catalog.clone(),
        execution_type: sql_req.execution_type,
        sql: sql_req.sql.clone(),
        tables: sql_req.tables.clone(),
        query_dependencies: sql_req.query_dependencies.clone(),
        semantic_extras: sql_req.semantic_extras.clone(),
        labels: sql_req.labels.clone(),
        dbt_node_state: sql_req.dbt_node_state.clone(),
        default_schema: sql_req.default_schema.clone(),
        from_speculative_submit: false,
        table_namespace: sql_req.table_namespace.clone(),
    };
    let record = qc::ExecutionRecord {
        outcome: Some(qc::ExecutionOutcome {
            last_modified_epoch: None,
            table_type: None,
            execution_results: None,
            execution_runtime_ms: None,
        }),
        input: Some(qc::execution_record::Input::EnrichedSql(enriched)),
    };
    let _ = exec
        .record_executions(qc::RecordExecutionsRequest {
            records: vec![record],
        })
        .await;
}

#[tokio::test]
async fn conformance_replay_all_corpora() {
    for path in CORPORA {
        run_corpus(path).await;
    }
}

/// Cross-environment reuse (IMPLEMENTED): the hosted service matches a
/// node across environments by `table_namespace` + `node_body_hash`, ignoring
/// the physical `default_schema`. In corpus 2, `accepted_values_...` is executed
/// in prod then SKIPPED in dev_clone despite the differing
/// upstream schema/epoch — because the logical node (same table_namespace+body
/// hash) was already built in prod and the data is reused via deferral.
///
/// Our server now matches confirmed state by table_namespace + node_body_hash
/// (store::find_confirmed_by_namespace) and compares upstream freshness by
/// logical identity (decision::logical_relation_key), so it reproduces this
/// cross-environment skip.
#[tokio::test]
async fn cross_environment_namespace_skip() {
    // Replay corpus 2's accepted_values node: execute under prod schema, confirm,
    // then submit under dev schema with the same table_namespace+body_hash and
    // assert SKIP (what the real service does).
    let path = CORPORA[1];
    let entries = diff::load_golden(path).expect("load corpus 2");

    let prod = entries.iter().find(|e| {
        e.method == "SubmitEnrichedSQL"
            && e.request.get("default_schema").and_then(|v| v.as_str()) == Some("prod")
            && e.request
                .get("labels")
                .and_then(|l| l.get("dbt_node_name"))
                .and_then(|v| v.as_str())
                .map(|n| n.contains("accepted_values"))
                .unwrap_or(false)
    });
    let dev = entries.iter().find(|e| {
        e.method == "SubmitEnrichedSQL"
            && e.request.get("default_schema").and_then(|v| v.as_str()) == Some("dev_clone")
            && e.request
                .get("labels")
                .and_then(|l| l.get("dbt_node_name"))
                .and_then(|v| v.as_str())
                .map(|n| n.contains("accepted_values"))
                .unwrap_or(false)
    });
    let (prod, dev) = (prod.expect("prod submit"), dev.expect("dev submit"));
    assert_eq!(
        real_variant(&dev.response).as_deref(),
        Some("skip_execution"),
        "real service skips the dev submit (cross-env reuse)"
    );

    let (addr, _schema) = support::start_server().await;
    let ch = support::channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    // Execute + confirm under prod schema.
    let prod_req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(prod.request.clone()).unwrap();
    let r = sql
        .submit_enriched_sql(prod_req)
        .await
        .unwrap()
        .into_inner();
    let rid = match r.response {
        Some(qc::submit_sql_response::Response::ReadyToExecute(x)) => x.request_id,
        _ => panic!("prod should execute"),
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: rid,
        last_modified_epoch: Some(1_791_535_090_914),
        failed_to_clone: false,
        table_type: None,
        execution_results: None,
        execution_runtime_ms: Some(1000),
        labels: Default::default(),
    })
    .await
    .unwrap();

    // Submit under dev schema → should SKIP (currently FAILS: executes).
    let dev_req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(dev.request.clone()).unwrap();
    let r2 = sql.submit_enriched_sql(dev_req).await.unwrap().into_inner();
    assert!(
        matches!(
            r2.response,
            Some(qc::submit_sql_response::Response::SkipExecution(_))
        ),
        "dev submit should skip via cross-env table_namespace reuse"
    );
}
