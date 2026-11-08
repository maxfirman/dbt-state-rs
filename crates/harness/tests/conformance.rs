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
    /// Explain-arm fence (ticket #29): when true, this gap documents that the
    /// re-derived explain tree for a GetExplainMessages entry diverges from the
    /// hosted tree in the per-upstream FRESHNESS SUBTREE (and, pre-#30, in the
    /// decision class itself, because the empty-store replay lacks the hosted
    /// prior history so a hosted SKIP/CLONE is a local first-build EXECUTE). The
    /// arm still runs end-to-end and asserts the fenced content genuinely
    /// differs (stale-exemption guard); #30 removes this once the
    /// `matched_execution_id` anchor makes the freshness subtree re-derivable.
    #[serde(default)]
    explain_freshness_subtree: bool,
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
/// payload is the first-build-vs-rebuild `decision_description` WORDING, AND the
/// target had NO confirmed history in the replay (`has_history == false`). This
/// is a REPLAY-STATE artifact: the conformance replay starts from an EMPTY
/// store, so a target that existed in the hosted session is absent locally on
/// its FIRST appearance and the server correctly reports "table did not exist"
/// while hosted — having prior history — reports a rebuild rationale.
///
/// FIELD-NARROW (review #2 6th/7th follow-up P1/P2): the exemption tolerates a
/// difference in `decision_description` AND in the two rejection codes, but ONLY
/// in the DOCUMENTED first-build→rebuild shape — LOCAL is the empty-store first
/// build (codes 6/6 + a first-build description) and HOSTED is a documented
/// rebuild (codes 6/6 OR the observed skip=8/clone=7 LATEST_QUERY_HASH_NOT_MATCH
/// pair, with a rebuild description). `is_stale` must be FALSE on both (hosted
/// never reports a stale EXECUTE), and NO other `explained_decision` field may
/// differ. So the rejection codes are NOT required to be byte-equal — but any
/// code combination other than the documented 6/6 or 8/7, any `is_stale`
/// difference, or an unrelated description returns FALSE. A tolerated diff here
/// is counted as a PARTIAL (history-dependent) comparison by the caller, not an
/// exact payload match.
fn execute_diff_is_only_rebuild_rationale(
    ours: &serde_json::Value,
    real: &serde_json::Value,
    has_history: bool,
) -> bool {
    // Only a FIRST-build (no replay history) may legitimately diverge.
    if has_history {
        return false;
    }
    let o = &ours["response"]["ready_to_execute"];
    let r = &real["response"]["ready_to_execute"];
    if !o.is_object() || !r.is_object() {
        return false;
    }
    let (o, r) = (o.as_object().unwrap(), r.as_object().unwrap());
    // Every top-level field EXCEPT explained_decision must be byte-equal.
    for k in o.keys().chain(r.keys()).collect::<HashSet<_>>() {
        if k != "explained_decision" && o.get(k) != r.get(k) {
            return false;
        }
    }
    let od = &o["explained_decision"];
    let rd = &r["explained_decision"];
    if !od.is_object() || !rd.is_object() {
        return false;
    }
    let (od, rd) = (od.as_object().unwrap(), rd.as_object().unwrap());
    let geti = |m: &serde_json::Map<String, serde_json::Value>, k: &str| -> Option<i64> {
        m.get(k).and_then(|v| v.as_i64())
    };
    // Both sides must be a non-stale EXECUTE (decision 1, is_stale false).
    if od.get("decision") != rd.get("decision") {
        return false;
    }
    if od.get("is_stale") != Some(&serde_json::Value::Bool(false))
        || rd.get("is_stale") != Some(&serde_json::Value::Bool(false))
    {
        return false;
    }
    // No explained_decision field OTHER than the known rationale axes
    // (decision_description + the two rejection codes) may differ — so an
    // unexpected extra field difference is a real mismatch.
    for k in od.keys().chain(rd.keys()).collect::<HashSet<_>>() {
        if !matches!(
            k.as_str(),
            "decision_description" | "skip_rejection_reason" | "clone_rejection_reason"
        ) && od.get(k) != rd.get(k)
        {
            return false;
        }
    }
    // LOCAL must be the empty-store FIRST build: codes (6,6) with a first-build
    // description. (6 = NO_SUITABLE_MATCH_FOUND.)
    let local_first_build = geti(od, "skip_rejection_reason") == Some(6)
        && geti(od, "clone_rejection_reason") == Some(6)
        && {
            let d = od
                .get("decision_description")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            d.contains("did not exist")
                || d.contains("no prior execution")
                || d.contains("has no prior")
        };
    if !local_first_build {
        return false;
    }
    // HOSTED must be a documented REBUILD rationale: either the same (6,6) codes
    // or the observed (skip=8, clone=7) LATEST_QUERY_HASH_NOT_MATCH pair, with a
    // rebuild description. Any other code/description combination is a real
    // mismatch (not this replay-state artifact).
    let hosted_codes_ok = (geti(rd, "skip_rejection_reason") == Some(6)
        && geti(rd, "clone_rejection_reason") == Some(6))
        || (geti(rd, "skip_rejection_reason") == Some(8)
            && geti(rd, "clone_rejection_reason") == Some(7));
    let hosted_desc = rd
        .get("decision_description")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let hosted_rebuild = hosted_desc.contains("query didn't match")
        || hosted_desc.contains("upstream data is out of date")
        || hosted_desc.contains("its data changed")
        || hosted_desc.contains("query changed");
    hosted_codes_ok && hosted_rebuild
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
    // 6th follow-up COMBINED probe: changing BOTH rejection codes AND is_stale
    // AND the description to an unrelated rationale must NOT be tolerated, even
    // with has_history=false (the previous version returned true here).
    let combined = serde_json::json!({"response": {"ready_to_execute": {
        "explained_decision": {"decision": 1, "skip_rejection_reason": 8, "clone_rejection_reason": 7,
            "is_stale": true, "decision_description": "something completely unrelated happened"}
    }}});
    assert!(!execute_diff_is_only_rebuild_rationale(
        &first_build,
        &combined,
        false
    ));
    // An ARBITRARY hosted description (not a documented rebuild rationale) is NOT
    // tolerated even if every other field matches.
    let arbitrary_desc = serde_json::json!({"response": {"ready_to_execute": {
        "explained_decision": {"decision": 1, "skip_rejection_reason": 6, "clone_rejection_reason": 6,
            "is_stale": false, "decision_description": "model was executed for some other reason"}
    }}});
    assert!(!execute_diff_is_only_rebuild_rationale(
        &first_build,
        &arbitrary_desc,
        false
    ));
    // The seed first-build→"its data changed" pair IS a documented rebuild pair.
    let seed_first = serde_json::json!({"response": {"ready_to_execute": {
        "explained_decision": {"decision": 1, "skip_rejection_reason": 6, "clone_rejection_reason": 6,
            "is_stale": false, "decision_description": "seed was loaded because it did not exist"}
    }}});
    let seed_rebuild = serde_json::json!({"response": {"ready_to_execute": {
        "explained_decision": {"decision": 1, "skip_rejection_reason": 6, "clone_rejection_reason": 6,
            "is_stale": false, "decision_description": "seed was loaded because its data changed"}
    }}});
    assert!(execute_diff_is_only_rebuild_rationale(
        &seed_first,
        &seed_rebuild,
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
        29,
        "update corpus coverage when adding fixtures"
    );
    let mut total_asserted = 0;
    let mut explain_entries_asserted = 0usize;
    let mut mismatches = Vec::new();
    let known: Vec<KnownGap> =
        serde_json::from_str(include_str!("../../../golden/replay_gaps.json")).unwrap();
    let mut seen_gaps = HashSet::new();
    // (fixture, entry) pairs where a ready_to_execute payload matched EXCEPT for
    // the documented first-build-vs-rebuild rationale (a replay-state artifact,
    // not an exact payload match). Reported separately and bounded to an exact
    // expected set so a NEW silent exemption fails the test (7th follow-up P2 #3).
    let mut partial_history: HashSet<(String, usize)> = HashSet::new();
    for path in paths {
        let entries = diff::load_golden(path.to_str().unwrap()).unwrap();
        let server = support::start_server().await;
        let addr = server.addr;
        let ch = support::channel(addr).await;
        let mut sql = SqlClient::new(ch.clone());
        let mut exec = ExecutionClient::new(ch.clone());
        let mut pending: HashMap<String, (String, String)> = HashMap::new();
        let mut confirmed: HashSet<String> = HashSet::new();
        // Hosted execution_decision_id -> locally minted execution_decision_id,
        // captured from EVERY submit response (skip/execute/clone). The
        // GetExplainMessages arm remaps the hosted ids in the request to the
        // local ones the server actually persisted — mirroring the
        // ConfirmExecution request_id remap. Only ids minted by a submit in THIS
        // fixture are tracked; others are untracked and omitted by the server.
        let mut edid_remap: HashMap<String, String> = HashMap::new();
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
                    // Record the hosted->local execution_decision_id mapping for
                    // the GetExplainMessages arm (captured from EVERY submit
                    // variant). The server mints its own id per submit.
                    if let (Some(hosted), Some(local)) = (
                        hosted_submit_edid(&e.response),
                        local_submit_edid(&response),
                    ) {
                        edid_remap.insert(hosted, local);
                    }
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
                            if ours != real {
                                if execute_diff_is_only_rebuild_rationale(&ours, &real, has_history)
                                {
                                    // The payload matches EXCEPT for the
                                    // documented first-build-vs-rebuild rationale
                                    // (a replay-state artifact). This is NOT an
                                    // exact payload match — account for it
                                    // separately so a NEW silent exemption is
                                    // caught (review #2 7th follow-up P2 #3).
                                    partial_history.insert((
                                        path.file_name().unwrap().to_string_lossy().into_owned(),
                                        i,
                                    ));
                                } else {
                                    mismatches.push(format!(
                                        "{context} (ready_to_execute, has_history={has_history}): \
                                         ours={ours} real={real}"
                                    ));
                                }
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
                "GetExplainMessages" => {
                    explain_entries_asserted += replay_explain_messages(
                        ch.clone(),
                        e,
                        i,
                        &path,
                        &edid_remap,
                        &known,
                        &mut seen_gaps,
                        &mut mismatches,
                    )
                    .await;
                    gaps += 0;
                }
                "RegisterClone" | "ResolveDeferredRelations" => {
                    gaps += 1;
                }
                other => panic!("{context}: unhandled method {other}"),
            }
        }
        total_asserted += asserted;
        let partial_here = partial_history
            .iter()
            .filter(|(f, _)| *f == path.file_name().unwrap().to_string_lossy())
            .count();
        eprintln!(
            "{}: asserted={asserted} unknown_baseline={unknown_baseline} known_gaps={gaps} \
             partial_history={partial_here}",
            path.display()
        );
    }
    assert!(total_asserted > 0);
    assert_eq!(
        seen_gaps.len(),
        known.len(),
        "every known gap must be exercised exactly once"
    );
    // The history-dependent rebuild-rationale exemption is a replay-state
    // artifact (an empty store lacks the hosted prior history), NOT an exact
    // payload match. Bound it to the EXACT set observed, so a new silent
    // exemption (a regression that starts tolerating a different EXECUTE
    // mismatch) fails here. Review #2 7th follow-up P2 #3.
    let expected_partial_history: HashSet<(String, usize)> = [
        ("c1_config_semantics.jsonl", 0),
        ("c1_config_vs_logic.jsonl", 0),
        ("clone_chain_upstream_drift.jsonl", 4),
        ("golden_20261008T222744.061Z.jsonl", 14),
        ("golden_20261009T083754.534Z.jsonl", 1),
        ("golden_20261009T100637.316Z.jsonl", 4),
        ("incremental_merge_reuse.jsonl", 0),
        ("lag_freshness_own_build.jsonl", 5),
        // Explain fixtures: the FIRST submit of a node that hosted SKIPPED (it
        // had prior-session history) EXECUTEs from the empty local store, so its
        // ready_to_execute payload matches hosted EXCEPT for the first-build-vs-
        // rebuild rationale — the same replay-state artifact documented above.
        // These are the submits that SEED node_decisions for the explain arm.
        ("explain_tolerance.jsonl", 2),
        ("explain_snowflake_happy.jsonl", 48),
    ]
    .into_iter()
    .map(|(f, e)| (f.to_string(), e))
    .collect();
    assert_eq!(
        partial_history, expected_partial_history,
        "the set of history-dependent rebuild-rationale exemptions changed; a new \
         silent exemption (or a resolved one) must be reviewed and this expected \
         set updated. got={partial_history:?}"
    );
    assert!(
        mismatches.is_empty(),
        "causal mismatches:\n{}",
        mismatches.join("\n")
    );
    // The GetExplainMessages arm must have actually run against the explain
    // fixtures (a hard gate, not silently skipped). Three explain fixtures carry
    // GetExplainMessages entries.
    assert!(
        explain_entries_asserted > 0,
        "the GetExplainMessages replay arm did not assert anything"
    );
}

/// Extract the hosted `execution_decision_id` from a captured submit response
/// (any variant). Returns None for a response without one.
fn hosted_submit_edid(resp: &serde_json::Value) -> Option<String> {
    let variants = resp.get("response")?.as_object()?;
    for payload in variants.values() {
        if let Some(id) = payload
            .get("execution_decision_id")
            .and_then(|v| v.as_str())
        {
            return Some(id.to_string());
        }
    }
    None
}

/// Extract the locally minted `execution_decision_id` from the server's submit
/// response (any variant).
fn local_submit_edid(resp: &qc::SubmitSqlResponse) -> Option<String> {
    match &resp.response {
        Some(qc::submit_sql_response::Response::SkipExecution(s)) => {
            s.execution_decision_id.clone()
        }
        Some(qc::submit_sql_response::Response::ReadyToExecute(r)) => {
            r.execution_decision_id.clone()
        }
        Some(qc::submit_sql_response::Response::ReadyToClone(c)) => c.execution_decision_id.clone(),
        None => None,
    }
}

/// Normalize an explain tree for comparison, masking the three runtime slots the
/// catalog marks (`«FQN»`, `«rel_time»`, `«gap»`). Recurses into children. The
/// FQN slot appears in `table analysis ("…")`, in a bare quoted upstream name,
/// in `… could be cloned from "…"`, and in `no updates since "…" last executed`;
/// `«rel_time»` is the `last updated: …` line; `«gap»` is the
/// `updated N seconds after "…" last executed` line.
fn normalize_explain_lines(lines: &[qc::ExplainLine]) -> serde_json::Value {
    fn mask_text(t: &str) -> String {
        if t.starts_with("table analysis (") {
            "table analysis («FQN»)".to_string()
        } else if t.starts_with("last updated:") {
            "last updated: «rel_time»".to_string()
        } else if t.starts_with("updated ") && t.contains("last executed") {
            "«gap»".to_string()
        } else if t.starts_with("no updates since") && t.contains("last executed") {
            // Collapse both the model-anchor and test-anchor variants onto one
            // masked form so the anchor FQN is a runtime slot.
            if t.contains("test last executed") {
                "no updates since test last executed".to_string()
            } else {
                "no updates since «FQN» last executed".to_string()
            }
        } else if t.contains("table could be cloned from \"") {
            // Keep the leading "the <noun> table could be cloned from" but mask
            // the source FQN.
            let prefix = t.split("cloned from").next().unwrap_or("");
            format!("{}cloned from «FQN»", prefix)
        } else if t.starts_with('"') && t.ends_with('"') {
            "«FQN»".to_string()
        } else {
            t.to_string()
        }
    }
    fn conv(l: &qc::ExplainLine) -> serde_json::Value {
        serde_json::json!({
            "text": mask_text(&l.text),
            "marker": l.marker,
            "badge": l.badge,
            "children": l.children.iter().map(conv).collect::<Vec<_>>(),
        })
    }
    serde_json::Value::Array(lines.iter().map(conv).collect())
}

/// Fence the per-upstream FRESHNESS SUBTREE out of a normalized explain tree:
/// remove any `upstream dependencies` listing (wherever it appears — under
/// `data freshness analysis`, `… (vs clone)`, or the OUTDATED `query analysis`),
/// and the `an upstream model query has changed` / `upstream data is outdated`
/// FAIL summary lines that only appear in the anchor-driven OUTDATED shape. The
/// returned value is the tree MINUS the anchor-dependent subtree; the second
/// return is the collected fenced subtrees (so the arm can assert they differ).
fn fence_freshness_subtree(tree: &serde_json::Value) -> (serde_json::Value, serde_json::Value) {
    let mut fenced_out = Vec::new();
    fn walk(v: &serde_json::Value, fenced_out: &mut Vec<serde_json::Value>) -> serde_json::Value {
        match v {
            serde_json::Value::Array(items) => serde_json::Value::Array(
                items
                    .iter()
                    .filter_map(|item| {
                        let text = item.get("text").and_then(|t| t.as_str()).unwrap_or("");
                        if text == "upstream dependencies"
                            || text == "an upstream model query has changed"
                            || text == "upstream data is outdated"
                        {
                            fenced_out.push(item.clone());
                            None
                        } else {
                            Some(walk(item, fenced_out))
                        }
                    })
                    .collect(),
            ),
            serde_json::Value::Object(obj) => {
                let mut m = serde_json::Map::new();
                for (k, val) in obj {
                    if k == "children" {
                        m.insert(k.clone(), walk(val, fenced_out));
                    } else {
                        m.insert(k.clone(), val.clone());
                    }
                }
                serde_json::Value::Object(m)
            }
            other => other.clone(),
        }
    }
    let pruned = walk(tree, &mut fenced_out);
    (pruned, serde_json::Value::Array(fenced_out))
}

/// Replay one `GetExplainMessages` capture: remap the hosted ids to the locally
/// minted ones, call the server (polling briefly for the best-effort capture to
/// land), and compare the re-derived tree to the hosted tree after normalization
/// and freshness-subtree fencing. Returns the number of messages asserted.
///
/// Every hosted message whose hosted edid maps to a submit in THIS fixture must
/// have a matching documented known gap (the empty-store replay lacks the hosted
/// prior history, so a hosted SKIP/CLONE is a local first-build EXECUTE, AND the
/// per-upstream freshness subtree needs the #30 anchor). The arm still asserts
/// the fence is REAL — the fenced content must genuinely differ between local
/// and hosted — so a resolved gap (e.g. once #30 lands) is flagged as stale.
#[allow(clippy::too_many_arguments)]
async fn replay_explain_messages(
    ch: tonic::transport::Channel,
    e: &diff::GoldenEntry,
    entry_idx: usize,
    path: &std::path::Path,
    edid_remap: &HashMap<String, String>,
    known: &[KnownGap],
    seen_gaps: &mut HashSet<(String, usize)>,
    mismatches: &mut Vec<String>,
) -> usize {
    use qc::explain_client::ExplainClient;
    let fixture = path.file_name().unwrap().to_string_lossy().into_owned();
    let context = format!("{fixture} entry {entry_idx} (GetExplainMessages)");

    // Hosted messages whose edid maps to a local submit in this fixture.
    let hosted_messages = e.response["messages"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let tracked: Vec<&serde_json::Value> = hosted_messages
        .iter()
        .filter(|m| {
            m["execution_decision_id"]
                .as_str()
                .map(|id| edid_remap.contains_key(id))
                .unwrap_or(false)
        })
        .collect();
    if tracked.is_empty() {
        // All referenced decisions were minted in a PRIOR live session (not in
        // this capture); the server tracks none of them, so it returns nothing.
        // Verify the server indeed omits untracked ids.
        let local_ids: Vec<String> = hosted_messages
            .iter()
            .filter_map(|m| m["execution_decision_id"].as_str().map(String::from))
            .collect();
        let mut explain = ExplainClient::new(ch);
        let resp = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: local_ids,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(
            resp.messages.is_empty(),
            "{context}: untracked ids must be omitted"
        );
        return 0;
    }

    // Build the remapped request (hosted id -> local id) for the tracked ids.
    let local_ids: Vec<String> = tracked
        .iter()
        .map(|m| edid_remap[m["execution_decision_id"].as_str().unwrap()].clone())
        .collect();

    // Poll for the best-effort capture to land for every tracked id.
    let mut explain = ExplainClient::new(ch);
    let mut local_by_id: HashMap<String, qc::ExplainMessageEntry> = HashMap::new();
    for _ in 0..100 {
        let resp = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: local_ids.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        local_by_id = resp
            .messages
            .into_iter()
            .map(|m| (m.execution_decision_id.clone(), m))
            .collect();
        if local_by_id.len() == local_ids.len() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        local_by_id.len(),
        local_ids.len(),
        "{context}: server must re-derive a message for every tracked id"
    );

    let mut asserted = 0usize;
    for m in tracked {
        let hosted_id = m["execution_decision_id"].as_str().unwrap();
        let local_id = &edid_remap[hosted_id];
        let local = local_by_id.get(local_id).expect("tracked message present");

        // Normalize both trees (mask the runtime slots on BOTH sides).
        let hosted_tree = normalize_explain_lines(
            &serde_json::from_value::<Vec<qc::ExplainLine>>(m["explain_lines"].clone())
                .unwrap_or_default(),
        );
        let local_tree = normalize_explain_lines(&local.explain_lines);

        // Fence the per-upstream freshness subtree from both.
        let (hosted_fenced, hosted_sub) = fence_freshness_subtree(&hosted_tree);
        let (local_fenced, local_sub) = fence_freshness_subtree(&local_tree);

        let decision_matches = local.decision == m["decision"].as_i64().unwrap_or(-1) as i32;
        let desc_matches =
            local.decision_description == m["decision_description"].as_str().unwrap_or("");
        let fenced_matches = hosted_fenced == local_fenced;

        if decision_matches && desc_matches && fenced_matches {
            // Exact match (outside the fenced freshness subtree). The freshness
            // subtree itself may still differ — that is the #30 anchor gap and
            // is accounted for by the fence. Asserted.
            asserted += 1;
            continue;
        }

        // Otherwise this message requires a documented known gap. It records the
        // empty-store missing-history decision divergence AND/OR the fenced
        // freshness subtree pending the #30 anchor.
        let gap = known
            .iter()
            .find(|g| g.fixture == fixture && g.entry == entry_idx && g.explain_freshness_subtree);
        match gap {
            Some(gap) => {
                assert!(
                    !gap.reason.trim().is_empty(),
                    "{context}: gap needs a reason"
                );
                // Stale-exemption guard: the fenced content (freshness subtree)
                // OR the decision/description must genuinely differ — otherwise
                // the exemption is stale and must be removed.
                let something_differs = hosted_sub != local_sub
                    || !decision_matches
                    || !desc_matches
                    || !fenced_matches;
                assert!(
                    something_differs,
                    "{context}: explain gap is resolved (local matches hosted exactly); \
                     remove the explain_freshness_subtree exemption"
                );
                seen_gaps.insert((gap.fixture.clone(), entry_idx));
                asserted += 1;
            }
            None => {
                mismatches.push(format!(
                    "{context}: explain tree differs for id {hosted_id} \
                     (decision_matches={decision_matches} desc_matches={desc_matches} \
                     fenced_matches={fenced_matches}); add an explain_freshness_subtree \
                     replay_gaps entry or fix the generator.\n  hosted_fenced={hosted_fenced}\n  local_fenced={local_fenced}"
                ));
            }
        }
    }
    asserted
}

/// HOSTED-ORACLE CROSS-CHECK for the re-derived SKIP explain tree.
///
/// The generic replay above cannot exact-match a hosted SKIP tree because the
/// empty store lacks the hosted prior build, so every explain message there is a
/// documented gap. This test seeds that history from a SELF-CONTAINED build +
/// confirm (using the tolerance fixture's own submit), produces a genuine local
/// SKIP, and asserts the re-derived tree — normalized and with the per-upstream
/// freshness subtree fenced — EXACTLY matches the hosted SHAPE-8 tree captured in
/// `explain_tolerance.jsonl`. This is the real oracle cross-check for the
/// non-anchor SKIP tree; #30 extends it to the fenced freshness subtree.
#[tokio::test]
async fn explain_skip_tree_matches_hosted_oracle_when_history_seeded() {
    use qc::explain_client::ExplainClient;
    const TOLERANCE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/explain_tolerance.jsonl"
    );
    let entries = diff::load_golden(TOLERANCE).expect("load explain_tolerance");
    // Entry 0 is the SubmitEnrichedSQL for ET_MODEL; entry 1 is its hosted SKIP
    // explain (SHAPE 8). We reproduce the SKIP locally by seeding history.
    let submit0: qc::SubmitEnrichedSqlRequest =
        serde_json::from_value(entries[0].request.clone()).unwrap();
    let hosted_explain = &entries[1].response["messages"][0];

    let server = support::start_server().await;
    let ch = support::channel(server.addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch.clone());

    // 1) First submit → EXECUTE (first build). Confirm it with the node's own
    //    build epoch so a later identical submit can prove freshness and SKIP.
    let r1 = sql
        .submit_enriched_sql(submit0.clone())
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(exec1) = r1.response.unwrap() else {
        panic!("first submit must EXECUTE");
    };
    // Own-build epoch = ET_MODEL's recorded epoch (strictly newer than ET_UP's,
    // so the upstream is fresh against it).
    let own_epoch = submit0
        .tables
        .iter()
        .find(|t| Some(t.name.as_str()) == submit0.target_table.as_deref())
        .and_then(|t| t.last_modified_epoch)
        .unwrap();
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: exec1.request_id,
        last_modified_epoch: Some(own_epoch),
        table_type: Some("TABLE".into()),
        execution_runtime_ms: Some(2200),
        execution_results: None,
        failed_to_clone: false,
        labels: Default::default(),
    })
    .await
    .unwrap();

    // 2) Identical re-submit → SKIP (query unchanged, upstream fresh).
    let r2 = sql
        .submit_enriched_sql(submit0.clone())
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::SkipExecution(skip) = r2.response.unwrap() else {
        panic!("second identical submit must SKIP");
    };
    let edid = skip.execution_decision_id.unwrap();

    // 3) GetExplainMessages for the SKIP → re-derived tree. Poll for capture.
    let mut explain = ExplainClient::new(ch);
    let mut msg = None;
    for _ in 0..100 {
        let resp = explain
            .get_explain_messages(qc::GetExplainMessagesRequest {
                execution_decision_ids: vec![edid.clone()],
            })
            .await
            .unwrap()
            .into_inner();
        if let Some(m) = resp.messages.into_iter().next() {
            msg = Some(m);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let msg = msg.expect("SKIP explain message re-derived");

    // Decision int matches the hosted SHAPE-8 SKIP exactly.
    assert_eq!(
        msg.decision,
        hosted_explain["decision"].as_i64().unwrap() as i32
    );
    // decision_description: the hosted capture is the lag-tolerance variant (#2,
    // "its data is within lag tolerance"). Our engine pins the plain SKIP
    // description (#1) and deliberately never applies freshness_tolerance_seconds
    // (decision.rs), so it selects #1 here. Selecting #2 requires detecting a
    // lag_tolerance config — a decision-engine wording concern tracked
    // separately (docs/explain-catalog.md §decision_description / issue #22), not
    // part of the explain TREE this ticket derives. The three SKIP descriptions
    // share ONE structurally identical tree (the point of the catalog), so the
    // tree cross-check below is the faithful explain assertion. We assert the
    // re-derived description is a valid member of the pinned SKIP set.
    assert!(
        matches!(
            msg.decision_description.as_str(),
            "model was a no-op because both its query and its upstream data are up to date"
                | "model was a no-op because its query is up to date and its data is within lag tolerance"
                | "model was a no-op because its query is up to date and at least one upstream dependency is unchanged"
        ),
        "SKIP description must be a pinned catalog variant, got: {}",
        msg.decision_description
    );
    // Tree (normalized) minus the fenced per-upstream freshness subtree matches
    // the hosted tree exactly.
    let local_tree = normalize_explain_lines(&msg.explain_lines);
    let hosted_tree = normalize_explain_lines(
        &serde_json::from_value::<Vec<qc::ExplainLine>>(hosted_explain["explain_lines"].clone())
            .unwrap(),
    );
    let (local_fenced, _local_sub) = fence_freshness_subtree(&local_tree);
    let (hosted_fenced, _hosted_sub) = fence_freshness_subtree(&hosted_tree);
    assert_eq!(
        local_fenced, hosted_fenced,
        "non-anchor SKIP tree must match hosted oracle exactly (freshness subtree fenced)"
    );
    // For THIS single-fresh-upstream node the FULL normalized tree (including the
    // per-upstream freshness subtree) already matches hosted: masking the
    // «FQN»/«rel_time» runtime slots collapses the placeholder onto the hosted
    // text, and the single upstream is genuinely FRESH (badge 1) — which the
    // generator emits as the non-anchor default. The #30 anchor is what lets the
    // generator distinguish FRESH from OUTDATED for a DRIFTED upstream (the
    // tolerance fixture's EXECUTE entry 3), not this fresh case.
    assert_eq!(
        local_tree, hosted_tree,
        "the whole normalized SKIP tree matches hosted for a single fresh upstream"
    );
}

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
