//! Offline replay of captured traffic with explicit limits.
//! Captures with pre-existing hosted history are not invented or hydrated.
//! Confirmations correlate by opaque request ID and preserve complete outcomes.
//! Every fixture is loaded; reproducible responses are compared in full after
//! removing IDs. See docs/correctness-handoff.md for unverified behavior.

#[path = "support.rs"]
mod support;
use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use std::collections::{HashMap, HashSet};

#[derive(serde::Deserialize)]
struct KnownGap {
    fixture: String,
    entry: usize,
    local_variant: String,
    reason: String,
    /// Optional field-scoped exemption for a `ready_to_execute` (or other)
    /// payload comparison: the NAMED response fields are stripped from BOTH the
    /// local and hosted payloads before comparing, so an unimplemented or
    /// runtime-dependent field (e.g. `transformed_nodes_by_query`, which holds
    /// the hosted service's volatile-function evaluations) is a VISIBLE,
    /// documented known gap rather than a silent variant-only pass. When present,
    /// the comparison asserts the rest of the payload matches exactly AND that at
    /// least one named field genuinely differs (so a resolved gap is flagged).
    #[serde(default)]
    ignore_fields: Vec<String>,
}

fn real_variant(resp: &serde_json::Value) -> Option<String> {
    diff::decision_variant(resp)
}

fn identity(req: &serde_json::Value) -> String {
    if req["execution_type"].as_i64() == Some(8) {
        format!(
            "test|{}|{}|{}",
            req["dbt_node_state"]["project_id"],
            req["table_namespace"],
            req["dbt_node_state"]["node_unique_id"]
        )
    } else {
        format!("target|{}", req["target_table"])
    }
}

fn without_ids(mut value: serde_json::Value) -> serde_json::Value {
    fn remove_ids(m: &mut serde_json::Map<String, serde_json::Value>) {
        m.remove("request_id");
        m.remove("execution_decision_id");
    }
    if let serde_json::Value::Object(root) = &mut value {
        remove_ids(root);
        if let Some(serde_json::Value::Object(variants)) = root.get_mut("response") {
            for payload in variants.values_mut() {
                if let serde_json::Value::Object(fields) = payload {
                    remove_ids(fields);
                }
            }
        }
    }
    value
}

/// After `without_ids` (and any `ignore_fields`) stripping, decide whether the
/// ONLY remaining difference between a local and hosted `ready_to_execute`
/// payload is the first-build-vs-rebuild `explained_decision` rationale, AND the
/// target had NO confirmed history in the replay (`has_history == false`). This
/// is a REPLAY-STATE artifact: the conformance replay starts from an EMPTY
/// store, so a target that existed in the hosted session is absent locally on
/// its FIRST appearance and the server correctly reports "table did not exist"
/// while hosted — having prior history — reports a rebuild rationale.
///
/// Crucially, missing-history is established from the REPLAY STATE (`has_history`,
/// derived from whether a prior ConfirmExecution for this target was replayed),
/// NOT from the local "did not exist" wording being tested. When the target WAS
/// confirmed earlier in the replay, the server has the same history hosted did
/// and MUST produce the same rationale — so this returns false and any
/// difference is a real mismatch. Review #2 5th follow-up P1 #4 (previously this
/// trusted the local explanation and exempted the whole explained_decision —
/// including rejection codes and is_stale — even after a confirmed baseline).
fn execute_diff_is_only_rebuild_rationale(
    ours: &serde_json::Value,
    real: &serde_json::Value,
    has_history: bool,
) -> bool {
    // Only a FIRST-build (no replay history) may legitimately diverge on the
    // rebuild rationale. With history, the rationale must match exactly.
    if has_history {
        return false;
    }
    let o = &ours["response"]["ready_to_execute"];
    let r = &real["response"]["ready_to_execute"];
    if !o.is_object() || !r.is_object() {
        return false;
    }
    let (o, r) = (o.as_object().unwrap(), r.as_object().unwrap());
    // Every field EXCEPT explained_decision must be byte-equal.
    let keys: HashSet<&String> = o.keys().chain(r.keys()).collect();
    for k in keys {
        if k == "explained_decision" {
            continue;
        }
        if o.get(k) != r.get(k) {
            return false;
        }
    }
    let od = &o["explained_decision"];
    let rd = &r["explained_decision"];
    // Same DECISION int (both EXECUTE); differing rationale only.
    if od["decision"] != rd["decision"] {
        return false;
    }
    if od == rd {
        return false; // no rationale difference => not this artifact
    }
    let local_desc = od["decision_description"].as_str().unwrap_or("");
    let first_build = local_desc.contains("did not exist")
        || local_desc.contains("no prior execution")
        || local_desc.contains("has no prior");
    // Local must be the first-build rationale (the empty-store truth); hosted
    // differs (the rebuild rationale it recorded from real history).
    first_build && od["decision_description"] != rd["decision_description"]
}

/// Strip a set of NAMED response fields from every decision-variant payload
/// (used for field-scoped known-gap comparison). A field may be a top-level
/// payload key (`transformed_nodes_by_query`) OR a one-level dotted path into a
/// nested object (`explained_decision.skip_rejection_reason`). Returns whether
/// ANY named field was actually present/removed, so a comparison can assert the
/// gap is real. Operates on the `without_ids`-normalized shape.
fn strip_named_fields(value: &mut serde_json::Value, fields: &[String]) -> bool {
    let mut removed = false;
    if let serde_json::Value::Object(root) = value {
        if let Some(serde_json::Value::Object(variants)) = root.get_mut("response") {
            for payload in variants.values_mut() {
                if let serde_json::Value::Object(f) = payload {
                    for name in fields {
                        if let Some((parent, child)) = name.split_once('.') {
                            if let Some(serde_json::Value::Object(p)) = f.get_mut(parent) {
                                if p.remove(child).is_some() {
                                    removed = true;
                                }
                            }
                        } else if f.remove(name).is_some() {
                            removed = true;
                        }
                    }
                }
            }
        }
    }
    removed
}

/// Keep ONLY the named fields in every decision-variant payload (the inverse of
/// `strip_named_fields`), used to assert that the exempted fields THEMSELVES
/// differ between local and hosted — so a stale `ignore_fields` exemption is
/// caught. Supports the same top-level and one-level dotted paths.
fn retain_named_fields(value: &mut serde_json::Value, fields: &[String]) {
    let top: HashSet<&str> = fields
        .iter()
        .map(|n| n.split('.').next().unwrap())
        .collect();
    if let serde_json::Value::Object(root) = value {
        if let Some(serde_json::Value::Object(variants)) = root.get_mut("response") {
            for payload in variants.values_mut() {
                if let serde_json::Value::Object(f) = payload {
                    f.retain(|k, _| top.contains(k.as_str()));
                    // For dotted paths, within a retained parent keep only the
                    // named children.
                    for name in fields {
                        if let Some((parent, child)) = name.split_once('.') {
                            if let Some(serde_json::Value::Object(p)) = f.get_mut(parent) {
                                p.retain(|k, _| {
                                    // keep this child if ANY dotted field names it
                                    fields.iter().any(|n| {
                                        n.split_once('.')
                                            .is_some_and(|(pp, cc)| pp == parent && cc == k)
                                    })
                                });
                                let _ = child;
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn comparison_preserves_result_fields_named_like_ids() {
    let response = serde_json::json!({"response": {"skip_execution": {
        "execution_decision_id": "opaque",
        "execution_results": {"fields": {"request_id": {"kind": {"string_value": "result"}}}}
    }}});
    let normalized = without_ids(response);
    assert!(normalized["response"]["skip_execution"]["execution_decision_id"].is_null());
    assert_eq!(
        normalized["response"]["skip_execution"]["execution_results"]["fields"]["request_id"]
            ["kind"]["string_value"],
        "result"
    );
}

#[test]
fn strip_named_fields_removes_only_named_payload_fields() {
    let mut v = serde_json::json!({"response": {"ready_to_execute": {
        "last_modified_query": "q",
        "transformed_nodes_by_query": {"n": {"node_to_func": {"CURRENT_TIMESTAMP()": "t"}}}
    }}});
    let removed = strip_named_fields(&mut v, &["transformed_nodes_by_query".to_string()]);
    assert!(removed);
    assert!(v["response"]["ready_to_execute"]["transformed_nodes_by_query"].is_null());
    assert_eq!(
        v["response"]["ready_to_execute"]["last_modified_query"],
        "q"
    );
    // Absent field => nothing removed.
    assert!(!strip_named_fields(&mut v, &["nonexistent".to_string()]));
}

#[test]
fn strip_named_fields_supports_nested_explained_decision_paths() {
    let mut v = serde_json::json!({"response": {"ready_to_execute": {
        "explained_decision": {"decision": 1, "skip_rejection_reason": 8, "clone_rejection_reason": 7}
    }}});
    let removed = strip_named_fields(
        &mut v,
        &["explained_decision.skip_rejection_reason".to_string()],
    );
    assert!(removed);
    assert!(
        v["response"]["ready_to_execute"]["explained_decision"]["skip_rejection_reason"].is_null()
    );
    // Sibling fields are preserved.
    assert_eq!(
        v["response"]["ready_to_execute"]["explained_decision"]["clone_rejection_reason"],
        7
    );
}

/// The rebuild-rationale exemption is REPLAY-STATE driven and does NOT
/// blanket-ignore the explained_decision (5th follow-up P1 #4).
#[test]
fn rebuild_rationale_exemption_is_history_gated_and_field_narrow() {
    let first_build = serde_json::json!({"response": {"ready_to_execute": {
        "explained_decision": {"decision": 1, "skip_rejection_reason": 6, "clone_rejection_reason": 6,
            "is_stale": false, "decision_description": "model was executed because its table did not exist"}
    }}});
    let hosted_rebuild = serde_json::json!({"response": {"ready_to_execute": {
        "explained_decision": {"decision": 1, "skip_rejection_reason": 6, "clone_rejection_reason": 6,
            "is_stale": false, "decision_description": "model was executed because either its query didn't match or its upstream data is out of date"}
    }}});
    // No history + only the rebuild wording differs → tolerated.
    assert!(execute_diff_is_only_rebuild_rationale(
        &first_build,
        &hosted_rebuild,
        false
    ));
    // WITH history, the same difference is NOT tolerated (the server must match).
    assert!(!execute_diff_is_only_rebuild_rationale(
        &first_build,
        &hosted_rebuild,
        true
    ));
    // A differing is_stale is NOT the rebuild-rationale artifact (must fail).
    let stale_diff = serde_json::json!({"response": {"ready_to_execute": {
        "explained_decision": {"decision": 1, "skip_rejection_reason": 6, "clone_rejection_reason": 6,
            "is_stale": true, "decision_description": "model was executed because its table did not exist"}
    }}});
    assert!(!execute_diff_is_only_rebuild_rationale(
        &first_build,
        &stale_diff,
        false
    ));
    // Differing rejection codes alone (same description) are NOT the artifact.
    let code_diff = serde_json::json!({"response": {"ready_to_execute": {
        "explained_decision": {"decision": 1, "skip_rejection_reason": 8, "clone_rejection_reason": 7,
            "is_stale": false, "decision_description": "model was executed because its table did not exist"}
    }}});
    assert!(!execute_diff_is_only_rebuild_rationale(
        &first_build,
        &code_diff,
        false
    ));
}

#[tokio::test]
async fn conformance_replay_all_corpora() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../golden/fixtures");
    // Characterization-only corpora are interleaved MULTI-RUN captures (the same
    // physical tables reused across sub-runs with deliberately advanced epochs).
    // They are NOT causally replayable from an empty store — cross-run physical
    // state is not in the protocol — so dedicated tests assert their contracts
    // by inspection + targeted replay (see lag_policy.rs). Exclude them here.
    //
    // `selector_state_selection.jsonl` likewise carries a baseline-then-query
    // structure (SubmitEnrichedSQL/SubmitValues record node state; subsequent
    // GetStateSelection queries read it) that the generic SKIP/EXECUTE/CLONE
    // replay does not model — it is driven by `selector_conformance.rs`.
    const CHARACTERIZATION_ONLY: &[&str] = &[
        "lag_multi_upstream_policy.jsonl",
        "selector_state_selection.jsonl",
        // Interleaved clone→rebuild→clone over the SAME physical target (the
        // decision depends on warehouse state not in the protocol). Pinned by
        // clone_submit_fallback.rs::hosted_fallback_fires_over_existing_target.
        "clone_fallback_over_existing_target.jsonl",
        // Baseline-then-query selector capture proving last-write-wins (a node
        // rebuilt in the defer-to env moves the comparison baseline). Pinned by
        // selector_offline.rs::selector_baseline_is_last_write_wins.
        "selector_last_write_wins.jsonl",
        // ResolveDeferredRelations resolution (node_unique_id -> defer-to env
        // FQN). The generic SKIP/EXECUTE/CLONE replay does not model deferral;
        // pinned by deferral.rs.
        "deferral_resolve.jsonl",
        // allow_clones=false (clone_chain_depth_limit=0) suppression of the
        // SQL-submit clone fallback: a single interleaved submit whose decision
        // depends on cross-session equivalent history. Pinned by
        // clone_submit_fallback.rs::allow_clones_false_suppresses_fallback_*.
        "clone_submit_allow_clones_false.jsonl",
        // Interleaved clone→incremental→table over the SAME physical target
        // (execution_type 1→3→1). Proves the current physical state (et=3)
        // supersedes a historical et=1 clone-origin row: the final et=1 submit
        // re-clones rather than reusing the stale row. Decision depends on
        // warehouse state not in the protocol; pinned by
        // clone_history_provenance.rs::
        // clone_adoption_must_not_resurrect_row_of_different_execution_type.
        "clone_rebuild_execution_type.jsonl",
        // Cross-environment clone fallback (dev_clone defer_to prod): a dev build
        // of an absent target clones the cross-env equivalent (ready_to_clone,
        // srr=1), across a different upstream table name. Interleaved
        // cross-session capture; pinned by clone_fallback_xenv.rs.
        "clone_fallback_xenv_fresh.jsonl",
        // RegisterClone source-age vs clone-age distinguishing interval
        // (source build < upstream < clone creation -> SKIP). Contains a
        // RegisterClone RPC the generic SKIP/EXECUTE/CLONE replay does not drive
        // against the server; pinned by clone_chain_provenance.rs.
        "clone_chain_source_age.jsonl",
    ];
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|p| p.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("jsonl"))
        .filter(|p| !CHARACTERIZATION_ONLY.contains(&p.file_name().unwrap().to_str().unwrap()))
        .collect();
    paths.sort();
    assert_eq!(
        paths.len(),
        26,
        "update corpus coverage when adding fixtures"
    );
    let mut total_asserted = 0;
    let mut mismatches = Vec::new();
    let known: Vec<KnownGap> =
        serde_json::from_str(include_str!("../../../golden/replay_gaps.json")).unwrap();
    let mut seen_gaps = HashSet::new();
    for path in paths {
        let entries = diff::load_golden(path.to_str().unwrap()).unwrap();
        let (addr, _) = support::start_server().await;
        let ch = support::channel(addr).await;
        let mut sql = SqlClient::new(ch.clone());
        let mut exec = ExecutionClient::new(ch.clone());
        let mut pending: HashMap<String, (String, String)> = HashMap::new();
        let mut confirmed: HashSet<String> = HashSet::new();
        let mut asserted = 0;
        let mut unknown_baseline = 0;
        let mut gaps = 0;
        for (i, e) in entries.iter().enumerate() {
            let context = format!("{} entry {i}", path.file_name().unwrap().to_string_lossy());
            match e.method.as_str() {
                "SubmitEnrichedSQL" | "SubmitValues" => {
                    let response = if e.method == "SubmitValues" {
                        sql.submit_values(support::captured_request(
                            serde_json::from_value::<qc::SubmitValuesRequest>(e.request.clone())
                                .unwrap(),
                            e,
                        ))
                        .await
                        .unwrap()
                        .into_inner()
                    } else {
                        sql.submit_enriched_sql(support::captured_request(
                            serde_json::from_value::<qc::SubmitEnrichedSqlRequest>(
                                e.request.clone(),
                            )
                            .unwrap(),
                            e,
                        ))
                        .await
                        .unwrap()
                        .into_inner()
                    };
                    let key = format!("{}|{}", support::captured_org(e), identity(&e.request));
                    match real_variant(&e.response).as_deref().unwrap() {
                        "ready_to_execute" => {
                            assert_eq!(
                                support::response_variant(&response),
                                "ready_to_execute",
                                "{context}: hosted rebuild must not skip"
                            );
                            // Compare the FULL execute payload (minus opaque IDs),
                            // not just the variant. Two documented, VISIBLE
                            // categories are tolerated: (1) a field-scoped gap
                            // (e.g. the volatile `transformed_nodes_by_query` map)
                            // declared in replay_gaps.json via ignore_fields —
                            // and the exempted FIELD ITSELF must differ; and (2)
                            // the first-build-vs-rebuild `explained_decision`
                            // rationale, tolerated ONLY when the target has NO
                            // confirmed history in the replay (replay-state, not
                            // the local wording). Anything else is a mismatch.
                            let has_history = confirmed.contains(&key);
                            let response_json = serde_json::to_value(&response).unwrap();
                            let ours_full = without_ids(response_json);
                            let real_full = without_ids(e.response.clone());
                            let mut ours = ours_full.clone();
                            let mut real = real_full.clone();
                            let gap = known.iter().find(|g| {
                                g.fixture == path.file_name().unwrap().to_string_lossy()
                                    && g.entry == i
                                    && !g.ignore_fields.is_empty()
                            });
                            if let Some(gap) = gap {
                                assert!(!gap.reason.trim().is_empty());
                                // The EXEMPTED FIELD(S) themselves must differ —
                                // otherwise the exemption is stale (a different,
                                // undocumented field is what differs).
                                let mut only_o = ours_full.clone();
                                let mut only_r = real_full.clone();
                                retain_named_fields(&mut only_o, &gap.ignore_fields);
                                retain_named_fields(&mut only_r, &gap.ignore_fields);
                                assert_ne!(
                                    only_o, only_r,
                                    "{context}: the ignore_fields {:?} do NOT differ; the \
                                     exemption is stale (something else differs)",
                                    gap.ignore_fields
                                );
                                let r1 = strip_named_fields(&mut ours, &gap.ignore_fields);
                                let r2 = strip_named_fields(&mut real, &gap.ignore_fields);
                                assert!(
                                    r1 || r2,
                                    "{context}: ignore_fields matched nothing; stale exemption"
                                );
                                seen_gaps.insert((gap.fixture.clone(), i));
                                gaps += 1;
                            }
                            if ours != real
                                && !execute_diff_is_only_rebuild_rationale(
                                    &ours,
                                    &real,
                                    has_history,
                                )
                            {
                                mismatches.push(format!(
                                    "{context} (ready_to_execute, has_history={has_history}): \
                                     ours={ours} real={real}"
                                ));
                            }
                            let qc::submit_sql_response::Response::ReadyToExecute(local) =
                                response.response.unwrap()
                            else {
                                unreachable!()
                            };
                            let id = e.response["response"]["ready_to_execute"]["request_id"]
                                .as_str()
                                .unwrap();
                            pending.insert(id.into(), (local.request_id, key));
                            asserted += 1;
                        }
                        "skip_execution" if confirmed.contains(&key) => {
                            let local_variant = support::response_variant(&response);
                            let ours = without_ids(serde_json::to_value(response).unwrap());
                            let real = without_ids(e.response.clone());
                            if let Some(gap) = known.iter().find(|g| {
                                g.fixture == path.file_name().unwrap().to_string_lossy()
                                    && g.entry == i
                            }) {
                                assert!(!gap.reason.trim().is_empty());
                                assert_ne!(
                                    ours, real,
                                    "{context}: gap is resolved; remove its exemption"
                                );
                                assert_eq!(
                                    local_variant, gap.local_variant,
                                    "{context}: characterized local fallback changed"
                                );
                                seen_gaps.insert((gap.fixture.clone(), i));
                                gaps += 1;
                            } else if ours != real {
                                mismatches.push(format!("{context}: ours={ours} real={real}"));
                            }
                            asserted += 1;
                        }
                        "skip_execution" => {
                            unknown_baseline += 1;
                        }
                        "ready_to_clone" => {
                            // REAL CLONE GATING (review #2 2nd follow-up, P1):
                            // compare the local CLONE response to hosted, correlate
                            // its confirmation so the cloned state is tracked, and
                            // distinguish a genuine full-match from a MISSING-SOURCE-
                            // HISTORY gap (the empty local store has no equivalent to
                            // clone, so it executes) and from a variant/policy
                            // mismatch.
                            let local_variant = support::response_variant(&response);
                            let known_gap = known.iter().find(|g| {
                                g.fixture == path.file_name().unwrap().to_string_lossy()
                                    && g.entry == i
                            });
                            if local_variant == "ready_to_clone" {
                                // Full causal clone match: compare the payload
                                // (minus opaque IDs) and correlate the confirm.
                                let qc::submit_sql_response::Response::ReadyToClone(local) =
                                    response.response.clone().unwrap()
                                else {
                                    unreachable!()
                                };
                                let ours = without_ids(serde_json::to_value(&response).unwrap());
                                let real = without_ids(e.response.clone());
                                if ours != real {
                                    mismatches.push(format!(
                                        "{context}: CLONE payload differs ours={ours} real={real}"
                                    ));
                                }
                                let id = e.response["response"]["ready_to_clone"]["request_id"]
                                    .as_str()
                                    .unwrap();
                                pending.insert(id.into(), (local.request_id, key));
                                asserted += 1;
                            } else if let Some(gap) = known_gap {
                                // MISSING SOURCE HISTORY: hosted cloned from a source
                                // whose build is not in THIS fixture, so the empty
                                // local store cannot clone and falls back (executes).
                                // Allowed only when explicitly exempted with a reason.
                                assert!(!gap.reason.trim().is_empty());
                                assert_eq!(
                                    local_variant, gap.local_variant,
                                    "{context}: characterized clone fallback changed"
                                );
                                seen_gaps.insert((gap.fixture.clone(), i));
                                gaps += 1;
                            } else {
                                mismatches.push(format!(
                                    "{context}: hosted ready_to_clone but local {local_variant} \
                                     (missing source history? add a keyed replay_gaps entry)"
                                ));
                            }
                        }
                        other => panic!("{context}: unsupported response {other}"),
                    }
                }
                "ConfirmExecution" => {
                    let mut req: qc::ConfirmExecutionRequest =
                        serde_json::from_value(e.request.clone()).unwrap();
                    if let Some((id, key)) = pending.remove(&req.request_id) {
                        req.request_id = id.clone();
                        let response = exec
                            .confirm_execution(support::captured_request(req, e))
                            .await
                            .unwrap()
                            .into_inner();
                        assert_eq!(response.request_id, id, "{context}: confirmation ID echo");
                        assert_eq!(
                            without_ids(serde_json::to_value(response).unwrap()),
                            without_ids(e.response.clone()),
                            "{context}"
                        );
                        confirmed.insert(key);
                        asserted += 1;
                    } else {
                        unknown_baseline += 1;
                    }
                }
                "ValidateClientVersion" => {
                    let response =
                        qc::client_validation_client::ClientValidationClient::new(ch.clone())
                            .validate_client_version(
                                serde_json::from_value::<qc::ValidateClientVersionRequest>(
                                    e.request.clone(),
                                )
                                .unwrap(),
                            )
                            .await
                            .unwrap()
                            .into_inner();
                    assert_eq!(
                        serde_json::to_value(response).unwrap(),
                        e.response,
                        "{context}"
                    );
                    asserted += 1;
                }
                "SubmitEnrichedSQLSpeculative" => {
                    let response = sql
                        .submit_enriched_sql_speculative(support::captured_request(
                            serde_json::from_value::<qc::SubmitEnrichedSqlRequest>(
                                e.request.clone(),
                            )
                            .unwrap(),
                            e,
                        ))
                        .await
                        .unwrap()
                        .into_inner();
                    assert_eq!(
                        without_ids(serde_json::to_value(response).unwrap()),
                        without_ids(e.response.clone()),
                        "{context}"
                    );
                    asserted += 1;
                }
                "RegisterClone" | "ResolveDeferredRelations" => {
                    gaps += 1;
                }
                other => panic!("{context}: unhandled method {other}"),
            }
        }
        total_asserted += asserted;
        eprintln!(
            "{}: asserted={asserted} unknown_baseline={unknown_baseline} known_gaps={gaps}",
            path.display()
        );
    }
    assert!(total_asserted > 0);
    assert_eq!(
        seen_gaps.len(),
        known.len(),
        "every known gap must be exercised exactly once"
    );
    assert!(
        mismatches.is_empty(),
        "causal mismatches:\n{}",
        mismatches.join("\n")
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
/// This is a DELIBERATELY-NOT-REPRODUCED gap: routing depends on prior physical/provenance state missing from these
/// captures. Target metadata exists in the protocol, but complete initial
/// object and source history is not recorded — the two sibling fixtures
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
        dbt_state_server::clone::clone_sqls("snowflake", source, target, Some("TRANSIENT TABLE"))
            .unwrap();
    assert_eq!(ours.len(), 1, "our generator emits one statement");
    assert_eq!(
        ours[0], ddl,
        "our clone DDL must byte-match the real service's clone_sqls"
    );
}
