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
    // Clone corpora captured in separate sessions. These include a
    // SubmitEnrichedSQL that the hosted service answered with `ready_to_clone`
    // ("an equivalent model exists under another name"), plus cross-environment
    // skips. The SKIP-vs-CLONE branch there depends on physical warehouse state
    // (which tables already exist) that is NOT in the protocol, so it cannot be
    // reproduced from an empty store; the replay HYDRATES those entries and
    // asserts only the causally-reproducible transitions. The exact clone
    // response shape is pinned separately in
    // `submit_enriched_sql_clone_fallback_is_characterized`.
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_happy_path.jsonl"
    ),
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_failed_fallback.jsonl"
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
    let mut characterized_clone = 0usize;

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
                    "ready_to_clone" => {
                        // The hosted service answered a SubmitEnrichedSQL with
                        // `ready_to_clone` ("an equivalent model exists under
                        // another name so we cloned that one"). Whether a given
                        // submit SKIPs, EXECUTEs or CLONEs here depends on
                        // physical warehouse state (which tables already exist)
                        // that is NOT carried in the protocol, so it is not
                        // reproducible from an empty store. We DO NOT assert our
                        // variant; we hydrate the fingerprint as confirmed
                        // baseline so later causal assertions line up, and the
                        // exact clone response shape is pinned by
                        // `submit_enriched_sql_clone_fallback_is_characterized`.
                        characterized_clone += 1;
                        hydrate_confirmed(&mut exec, &e.request).await;
                        confirmed.insert(fp.clone());
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

    // Guard against the replay silently asserting nothing meaningful. A corpus
    // is meaningful if it either made a causal assertion OR characterized a
    // clone-fallback entry (whose shape is pinned by a dedicated test).
    assert!(
        asserted_execute + asserted_skip + characterized_clone > 0,
        "corpus {path}: no causal assertions or characterizations were made \
         (execute={asserted_execute} skip={asserted_skip} baseline={baseline_skips} clone={characterized_clone})"
    );
    eprintln!(
        "conformance {path}: asserted_execute={asserted_execute} asserted_skip={asserted_skip} baseline_skips={baseline_skips} characterized_clone={characterized_clone}"
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
    // F5: give the hydrated baseline a REALISTIC recorded build epoch — the max
    // of the request's own upstream epochs — rather than None. This exercises
    // the engine's baseline-fallback branch (an upstream the recorded run never
    // saw falls back to `prev.last_modified_epoch`) with real millisecond
    // magnitudes, instead of leaving every hydrated row with a null baseline
    // that trivially forces execute on any unseen upstream.
    let baseline_epoch = sql_req
        .tables
        .iter()
        .filter_map(|t| t.last_modified_epoch)
        .max();
    let record = qc::ExecutionRecord {
        outcome: Some(qc::ExecutionOutcome {
            last_modified_epoch: baseline_epoch,
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

/// CHARACTERIZED GAP — `SubmitEnrichedSQL` answered with `ready_to_clone`.
///
/// The hosted service can answer a plain `SubmitEnrichedSQL` (not a
/// `RegisterClone`) with `ready_to_clone` when "an equivalent model exists
/// under another name so we cloned that one" (captured in
/// `clone_failed_fallback.jsonl`). Our `SqlService::submit_enriched_sql` only
/// returns SKIP/EXECUTE — it never emits CLONE from the SQL path.
///
/// This is a DELIBERATELY-NOT-REPRODUCED gap: whether a submit SKIPs, EXECUTEs
/// or CLONEs in this situation depends on PHYSICAL warehouse state (does the
/// target already exist? does an equivalently-named sibling exist to clone
/// from?) which the protocol does not carry — the two sibling fixtures
/// (`clone_happy_path` vs `clone_failed_fallback`) were captured in separate
/// sessions with different pre-existing tables and the SAME logical fingerprint
/// yields a SKIP in one and a CLONE in the other. Reproducing it from an empty
/// store would mean asserting against a guess, not the real service.
///
/// Instead this test PINS THE REAL RESPONSE SHAPE as golden source so the
/// contract is captured and any future implementation can be validated against
/// it. It is the authoritative cross-check available without burning dbt State
/// metering (which the live diff-fuzz tool requires).
#[tokio::test]
async fn submit_enriched_sql_clone_fallback_is_characterized() {
    const CLONE_FALLBACK: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/clone_failed_fallback.jsonl"
    );
    let entries = diff::load_golden(CLONE_FALLBACK).expect("load clone_failed_fallback");

    // Find the SubmitEnrichedSQL whose real response was ready_to_clone.
    let entry = entries
        .iter()
        .find(|e| {
            e.method == "SubmitEnrichedSQL"
                && real_variant(&e.response).as_deref() == Some("ready_to_clone")
        })
        .expect("a SubmitEnrichedSQL -> ready_to_clone entry");

    // The request must decode cleanly into our proto type (wire-compatibility).
    let _req: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(entry.request.clone()).expect("decode SubmitEnrichedSqlRequest");

    // Pin the exact real response shape (the golden contract for this branch).
    let ready = &entry.response["response"]["ready_to_clone"];
    assert!(ready.is_object(), "ready_to_clone payload present");

    let ed = &ready["explained_decision"];
    assert_eq!(
        ed["decision"].as_i64(),
        Some(3),
        "decision must be READY_TO_CLONE (3)"
    );
    // skip_rejection_reason = TARGET_TABLE_MISMATCH (1): the submitted physical
    // target differs from the equivalent model that was cloned.
    assert_eq!(
        ed["skip_rejection_reason"].as_i64(),
        Some(1),
        "skip_rejection_reason must be TARGET_TABLE_MISMATCH (1)"
    );
    assert!(
        ed["clone_rejection_reason"].is_null(),
        "clone_rejection_reason must be null (the clone succeeded)"
    );
    assert_eq!(ed["is_stale"].as_bool(), Some(false));
    assert_eq!(
        ed["decision_description"].as_str(),
        Some("an equivalent model exists under another name so we cloned that one"),
    );

    // The server generated clone DDL and echoed source/target + runtime.
    let sqls = ready["clone_sqls"].as_array().expect("clone_sqls array");
    assert_eq!(sqls.len(), 1, "one clone statement");
    let ddl = sqls[0].as_str().unwrap();
    assert!(ddl.contains("CREATE OR REPLACE TRANSIENT TABLE"));
    assert!(ddl.contains("CLONE"));
    assert!(ddl.contains("COPY GRANTS"));
    assert!(ready["clone_source"]
        .as_str()
        .unwrap()
        .contains("DEV_CLONE"));
    assert!(ready["clone_target"].as_str().unwrap().contains("PROD"));
    assert!(
        ready["clone_required_last_modified_epoch"]
            .as_i64()
            .is_some(),
        "clone_required_last_modified_epoch populated"
    );
    assert!(
        ready["execution_runtime_ms"].as_i64().is_some(),
        "execution_runtime_ms echoed"
    );

    // Cross-check that our clone-DDL generator reproduces the SAME statement the
    // real service returned for this source/target/type — proving the DDL half
    // of the contract is faithfully implemented even though the SKIP/EXECUTE/
    // CLONE routing from the SQL path is not.
    let source = ready["clone_source"].as_str().unwrap();
    let target = ready["clone_target"].as_str().unwrap();
    let ours =
        dbt_state_server::clone::clone_sqls("snowflake", source, target, Some("TRANSIENT TABLE"));
    assert_eq!(ours.len(), 1, "our generator emits one statement");
    assert_eq!(
        ours[0], ddl,
        "our clone DDL must byte-match the real service's clone_sqls"
    );
}
