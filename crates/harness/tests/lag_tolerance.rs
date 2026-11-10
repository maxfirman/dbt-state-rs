//! `lag_tolerance` — hosted behavior characterized live on 2026-11-09/10
//! (api.state.dbt.com through the recording proxy, which forwards client
//! metadata). Implemented in the engine per ADR 0007; `lag_tolerance_*` and
//! `lag_external_modification_linked` also replay in `conformance.rs`. These
//! tests pin the hosted facts by fixture inspection.
//!
//! * `lag_tolerance_docs_example.jsonl`: the docs' worked example, scaled down,
//!   through the real dbt 2.0.11 client (`experiments/lag_docs_example.sh`):
//!   ltd_m (3m tolerance) over ltd_up (0s) over a source. A source change that
//!   dbt propagates by rebuilding ltd_up is reused within the tolerance, and
//!   rebuilt once ltd_m's last build is older than the tolerance.
//! * `lag_external_modification_linked.jsonl`: real client, ALL policy,
//!   tolerance 0. lp_model2 was built in the same run as lp_up_a, so it
//!   recorded lp_up_a at exactly lp_up_a's build epoch; an EXTERNAL
//!   MODIFICATION of lp_up_a then rebuilds lp_model2 despite ALL.
//! * `lag_tolerance_matrix.jsonl`: crafted submit/confirm sequences with
//!   synthetic epochs (`experiments/lag_matrix.py`), one variable at a time.
//! * `lag_upstream_query_listing.jsonl` (`lag_matrix.py stale_on_execute`),
//!   `lag_copy_candidates.jsonl` (`copy_candidates.py`) and
//!   `lag_seed_upstream.jsonl` (`seed_upstream.py`) also replay in
//!   `conformance.rs`; `clone_chain_upstream_history.jsonl`
//!   (`clone_chain_provenance.sh`) is characterization only.
//!
//! Rules the captures support (ADR 0007):
//!   1. Upstream drift: an upstream's current epoch is GREATER than the epoch
//!      recorded for it at the node's last confirmed build (`cdb_m`: a change
//!      between submit and confirm still counts).
//!   2. Upstream query change: when the build that produced an upstream's
//!      current contents differs from the one the node consumed (rebuilt with
//!      other logic, or externally modified after the consumed build), hosted
//!      reports "an upstream model query has changed" and rebuilds regardless
//!      of policy and tolerance (`lk*`, `lb*`, `ext*`, `soe*`).
//!   3. Policy: ANY rebuilds if any upstream drifted; ALL only if all did.
//!   4. Tolerance: a rebuild the policy requires becomes a reuse while the
//!      node's build age (hosted clock now minus the ConfirmExecution epoch)
//!      is under the tolerance (`recz_m`, `bacback_m`, `bacfwd_m`, `bao_m`,
//!      `bah_m`).
//!   5. Descriptions: "both ... up to date" when tolerance is 0 or every
//!      upstream epoch equals its recorded epoch; otherwise "within lag
//!      tolerance" while the build age is under the tolerance; otherwise "at
//!      least one upstream dependency is unchanged".

use dbt_state_harness::diff;
use std::collections::HashMap;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../../golden/fixtures/");

const BOTH: &str = "model was a no-op because both its query and its upstream data are up to date";
const WITHIN: &str =
    "model was a no-op because its query is up to date and its data is within lag tolerance";
const ATLEAST1: &str = "model was a no-op because its query is up to date and at least one upstream dependency is unchanged";
const OUTDATED: &str =
    "model was executed because either its query didn't match or its upstream data is out of date";

/// (variant, is_stale, description) of every SubmitEnrichedSQL, per node.
fn decisions(fixture: &str) -> HashMap<String, Vec<(String, bool, String)>> {
    let entries = diff::load_golden(&format!("{FIXTURES}{fixture}")).expect("load fixture");
    let mut out: HashMap<String, Vec<_>> = HashMap::new();
    for e in entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL" || e.method == "SubmitValues")
    {
        let node = e.request["labels"]["dbt_node_name"]
            .as_str()
            .unwrap()
            .to_string();
        let v = diff::decision_variant(&e.response).unwrap();
        let ed = &e.response["response"][&v]["explained_decision"];
        out.entry(node).or_default().push((
            v,
            ed["is_stale"].as_bool().unwrap_or(false),
            ed["decision_description"]
                .as_str()
                .unwrap_or("")
                .to_string(),
        ));
    }
    out
}

fn assert_last(
    d: &HashMap<String, Vec<(String, bool, String)>>,
    node: &str,
    want: (&str, bool, &str),
) {
    let got = d[node].last().unwrap();
    assert_eq!(
        (got.0.as_str(), got.1, got.2.as_str()),
        want,
        "hosted decision for {node}"
    );
}

#[test]
fn docs_worked_example_reproduces_on_the_real_client() {
    let d = decisions("lag_tolerance_docs_example.jsonl");
    let m: Vec<_> = d["ltd_m"]
        .iter()
        .map(|x| (x.0.as_str(), x.1, x.2.as_str()))
        .collect();
    assert_eq!(
        m,
        vec![
            (
                "ready_to_execute",
                false,
                "model was executed because its table did not exist"
            ),
            ("skip_execution", true, WITHIN),
            ("ready_to_execute", false, OUTDATED),
            ("skip_execution", false, BOTH),
        ],
        "first build, reuse within tolerance, rebuild after it, then up to date"
    );
    let up: Vec<_> = d["ltd_up"].iter().map(|x| x.0.as_str()).collect();
    assert_eq!(
        up,
        vec![
            "ready_to_execute",
            "ready_to_execute",
            "skip_execution",
            "skip_execution"
        ]
    );
}

#[test]
fn external_modification_of_a_consumed_build_rebuilds_under_all_policy() {
    let d = decisions("lag_external_modification_linked.jsonl");
    assert_last(&d, "lp_model2", ("ready_to_execute", false, OUTDATED));
}

#[test]
fn crafted_matrix_decisive_outcomes() {
    let d = decisions("lag_tolerance_matrix.jsonl");
    let cases: &[(&str, (&str, bool, &str))] = &[
        // Recorded rebuild of the upstream: tolerance applies to build age.
        ("ltx235949_recy_m", ("skip_execution", true, WITHIN)),
        ("ltx000034_reco_m", ("ready_to_execute", false, OUTDATED)),
        ("ltx000123_recx_m", ("ready_to_execute", false, OUTDATED)),
        ("ltx000123_recz_m", ("ready_to_execute", false, OUTDATED)),
        // Sources behave like recorded upstream changes.
        ("ltx235949_srcy_m", ("skip_execution", true, WITHIN)),
        ("ltx000123_srco_m", ("ready_to_execute", false, OUTDATED)),
        // External modification of a build the node consumed: always rebuilds.
        ("ltx235915_exty_m", ("ready_to_execute", false, OUTDATED)),
        ("ltx235949_extu_m", ("ready_to_execute", false, OUTDATED)),
        ("ltx000123_exts_m", ("ready_to_execute", false, OUTDATED)),
        ("ltx000123_exto_m1", ("ready_to_execute", false, OUTDATED)),
        ("ltx001030_lkeq_m2", ("ready_to_execute", false, OUTDATED)),
        (
            "ltx001030_lkminus300_m2",
            ("ready_to_execute", false, OUTDATED),
        ),
        // Recorded later than the upstream's build: plain data drift.
        ("ltx001030_lkplus3000_m2", ("skip_execution", true, WITHIN)),
        ("ltx001249_lb1_m", ("skip_execution", true, WITHIN)),
        // ALL policy and descriptions.
        ("ltx000123_altr_m", ("ready_to_execute", false, OUTDATED)),
        ("ltx001030_pao_m", ("skip_execution", true, ATLEAST1)),
        ("ltx001412_dsall_m", ("skip_execution", true, BOTH)),
        ("ltx001412_dsdec_t0_m", ("skip_execution", false, BOTH)),
        ("ltx001412_dsdec_young_m", ("skip_execution", false, WITHIN)),
        ("ltx001412_dsdec_old_m", ("skip_execution", false, ATLEAST1)),
        // Drift is against the recorded upstream epoch, not the build epoch.
        ("ltx001249_cdb_m", ("ready_to_execute", false, OUTDATED)),
        // Build age = hosted clock minus the ConfirmExecution epoch.
        ("ltx001030_bacback_m", ("ready_to_execute", false, OUTDATED)),
        ("ltx001030_bacfwd_m", ("skip_execution", true, WITHIN)),
        ("ltx001249_bao_m", ("skip_execution", true, WITHIN)),
        ("ltx001249_bah_m", ("skip_execution", true, WITHIN)),
        // Unconfirmed executes change nothing.
        ("ltx001412_ucs_m", ("ready_to_execute", false, OUTDATED)),
    ];
    for (node, want) in cases {
        assert_last(&d, node, *want);
    }
    // An own-target epoch later than the confirmed build always rebuilds,
    // even 1s later.
    for node in ["ltx001030_ot1000_u", "ltx001030_ot10000_u"] {
        assert_last(
            &d,
            node,
            (
                "ready_to_execute",
                false,
                "model was executed because the table was modified after the cached execution",
            ),
        );
    }
}

/// A changed upstream SQL rebuilt by dbt, and an upstream rebuilt by dbt after
/// the node consumed it in an externally modified state, are both upstream
/// query changes (rule 2). Under ALL policy the latter was answered with a
/// result copy marked `is_stale` ("but it's old, so we will clone …"): the
/// equivalent's lineage still qualified (see `copy_candidate_arms`).
#[test]
fn upstream_query_change_arms() {
    let d = decisions("lag_upstream_query_listing.jsonl");
    assert_last(
        &d,
        "ltx093419_soeany_m2",
        ("ready_to_execute", false, OUTDATED),
    );
    assert_last(
        &d,
        "ltx093419_soesql_m",
        ("ready_to_execute", false, OUTDATED),
    );
    let all = d["ltx093419_soeall_m2"].last().unwrap();
    assert_eq!((all.0.as_str(), all.1), ("ready_to_clone", true));
}

/// The clone chain with its upstream's builds in the capture (Q5 recapture):
/// dev CC_MODEL was copied while consuming dev CC_UP at an epoch no later than
/// CC_UP's own (copy) build; CC_UP was then externally modified, and hosted
/// rebuilt CC_MODEL with a 45m tolerance and a 30s-old build. So rule 2
/// covers copies: their consumed producer is the upstream's copy build.
#[test]
fn clone_chain_external_modification_of_consumed_upstream_rebuilds() {
    let entries = diff::load_golden(&format!("{FIXTURES}clone_chain_upstream_history.jsonl"))
        .expect("load fixture");
    let up = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CC_UP_092242\"";
    let model = "\"CDO_DEMO_DB\".\"DEV_CLONE_DEMO\".\"CC_MODEL_092242\"";
    let epoch_of = |e: &diff::GoldenEntry, name: &str| {
        e.request["tables"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["name"] == name)
            .and_then(|t| t["last_modified_epoch"].as_i64())
    };
    let submits: Vec<_> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL" && e.request["target_table"] == model)
        .collect();
    let (copy, rebuild) = (submits[0], submits[1]);
    let up_copy_id = entries
        .iter()
        .find(|e| e.method == "SubmitEnrichedSQL" && e.request["target_table"] == up)
        .and_then(|e| e.response["response"]["ready_to_clone"]["request_id"].as_str())
        .unwrap();
    let up_build = entries
        .iter()
        .find(|e| e.method == "ConfirmExecution" && e.request["request_id"] == up_copy_id)
        .and_then(|e| e.request["last_modified_epoch"].as_i64())
        .unwrap();
    let consumed = epoch_of(copy, up).unwrap();
    let current = epoch_of(rebuild, up).unwrap();
    assert!(consumed <= up_build && up_build < current);
    assert_eq!(rebuild.request["freshness_tolerance_seconds"], 2700);
    assert_eq!(
        diff::decision_variant(&rebuild.response).unwrap(),
        "ready_to_execute"
    );
}

/// Seeds as upstreams (`experiments/seed_upstream.py`, 2026-11-10): a seed
/// reloaded with changed values or semantic config is an upstream query change
/// and rebuilds its consumer within tolerance (A, B, F), as does an External
/// modification of the seed's table (C, E). A same-values reload is a data
/// change, reused within tolerance (G), and identity is compared on logic, so
/// v1 -> v2 -> v1 is reused too (H). Hosted SKIPS the seed itself when only
/// its table was modified externally (C), where the engine rebuilds it.
#[test]
fn seed_upstream_arms() {
    let d = decisions("lag_seed_upstream.jsonl");
    for node in [
        "sdx113212_a_m",
        "sdx113228_b_m",
        "sdx113228_c_m",
        "sdx113228_e_m",
        "sdx113228_f_m",
    ] {
        assert_last(&d, node, ("ready_to_execute", false, OUTDATED));
    }
    assert_last(&d, "sdx113228_d_m", ("skip_execution", false, BOTH));
    for node in ["sdx113335_g_m", "sdx113335_h_m"] {
        assert_last(&d, node, ("skip_execution", true, WITHIN));
    }
    assert_last(
        &d,
        "sdx113228_c_s",
        (
            "skip_execution",
            false,
            "seed was a no-op because its data has not changed",
        ),
    );
}

/// (variant, is_stale, skip rejection, clone rejection) of a node's last
/// SubmitEnrichedSQL.
fn last_codes(fixture: &str, node: &str) -> (String, bool, Option<i64>, Option<i64>) {
    let entries = diff::load_golden(&format!("{FIXTURES}{fixture}")).expect("load fixture");
    let e = entries
        .iter()
        .rev()
        .find(|e| e.method == "SubmitEnrichedSQL" && e.request["labels"]["dbt_node_name"] == node)
        .unwrap_or_else(|| panic!("no submit for {node}"));
    let v = diff::decision_variant(&e.response).unwrap();
    let ed = &e.response["response"][&v]["explained_decision"];
    (
        v,
        ed["is_stale"].as_bool().unwrap_or(false),
        ed["skip_rejection_reason"].as_i64(),
        ed["clone_rejection_reason"].as_i64(),
    )
}

/// Which build hosted reuses or copies when a target's latest build no longer
/// qualifies (`experiments/copy_candidates.py`, 2026-11-10; replayed in
/// `conformance.rs`):
/// * An equivalent under another name that qualifies for this submit is
///   copied over the existing target (`fe`: fresh; `st`, `wba`: stale within
///   tolerance, judged on the equivalent's build age), unless the target's own
///   table was modified externally (`ome`) or the equivalent is stale beyond
///   tolerance too (`esb`). A stale equivalent does not end the search: an
///   older build of another equivalent that is current is copied (`sne`).
/// * A rebuild while an older build of the target would still qualify
///   reports LATEST_QUERY_HASH_NOT_MATCH / TARGET_TABLE_MATCH, `is_stale` as
///   that build would be reused (`oball`, `obany`, `obf`); a stale older build
///   changes nothing (`obn`).
/// * A dropped target with the latest build's logic reports
///   TARGET_TABLE_DOES_NOT_EXIST / TARGET_TABLE_MATCH (`dt` second submit);
///   with changed logic, NO_SUITABLE_MATCH_FOUND (`dt` third submit).
#[test]
fn copy_candidate_arms() {
    let f = "lag_copy_candidates.jsonl";
    let copy = |stale| ("ready_to_clone".to_string(), stale, Some(1), None);
    let execute = |stale, srr, crr| ("ready_to_execute".to_string(), stale, Some(srr), Some(crr));
    assert_eq!(last_codes(f, "ltx115931_fe_m2"), copy(false));
    assert_eq!(last_codes(f, "ltx115931_st_m2"), copy(true));
    assert_eq!(last_codes(f, "ltx120154_wba_m2"), copy(true));
    assert_eq!(last_codes(f, "ltx120154_ome_m2"), execute(false, 6, 6));
    assert_eq!(last_codes(f, "ltx120154_esb_m2"), execute(false, 6, 6));
    assert_eq!(last_codes(f, "ltx115931_oball_m2"), execute(true, 8, 7));
    assert_eq!(last_codes(f, "ltx115931_obany_m2"), execute(true, 8, 7));
    assert_eq!(last_codes(f, "ltx120154_obf_m2"), execute(false, 8, 7));
    assert_eq!(last_codes(f, "ltx120154_obn_m2"), execute(false, 6, 6));
    assert_eq!(last_codes(f, "ltx115931_de_m2"), copy(false));
    // The newest equivalent is stale; an older, current one is copied.
    assert_eq!(last_codes(f, "ltx151050_sne_m2"), copy(false));

    let entries = diff::load_golden(&format!("{FIXTURES}{f}")).unwrap();
    let dt: Vec<_> = entries
        .iter()
        .filter(|e| {
            e.method == "SubmitEnrichedSQL"
                && e.request["labels"]["dbt_node_name"] == "ltx115931_dt_m"
        })
        .map(|e| {
            let ed = &e.response["response"]["ready_to_execute"]["explained_decision"];
            (
                ed["skip_rejection_reason"].as_i64(),
                ed["clone_rejection_reason"].as_i64(),
                ed["decision_description"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let did_not_exist = "model was executed because its table did not exist".to_string();
    assert_eq!(
        dt,
        [
            (Some(6), Some(6), did_not_exist.clone()),
            (Some(4), Some(7), did_not_exist.clone()),
            (Some(6), Some(6), did_not_exist),
        ]
    );

    // A dropped target (`lag_dropped_targets.jsonl`): the same-upstream
    // equivalent is judged as for an existing target, so a stale copy needs a
    // lag tolerance (`dset600`), and 4/7 needs the dropped build to be
    // reusable (`dset0`, `ddc`: stale, so 6/6).
    let f = "lag_dropped_targets.jsonl";
    assert_eq!(last_codes(f, "ltx130355_dset0_m2"), execute(false, 6, 6));
    assert_eq!(last_codes(f, "ltx130355_dset600_m2"), copy(true));
    assert_eq!(last_codes(f, "ltx130458_ddc_m2"), execute(false, 6, 6));
}
