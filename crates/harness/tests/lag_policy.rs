//! MULTI-UPSTREAM LAG / require_fresh_data_from (ANY vs ALL) + is_stale —
//! characterized live against api.state.dbt.com (Fusion client, Snowflake,
//! target prod_demo) via the recording proxy (experiments/lag_experiment.sh).
//!
//! Model `lp_model2` cross-joins TWO independent upstreams (lp_up_a, lp_up_b).
//! Each upstream's physical table is recreated in place with IDENTICAL content
//! (`create or replace table … as select <const>`), advancing its warehouse
//! modification epoch WITHOUT any dbt SQL change — isolating pure DATA freshness
//! from the upstream-logic-change bypass. `require_fresh_data_from` maps to the
//! wire field `stale_upstream_policy` (any=0, all=1); `lag_tolerance` maps to
//! `freshness_tolerance_seconds`.
//!
//! VERIFIED hosted truth table (fixture lag_multi_upstream_policy.jsonl):
//!   ANY, no drift            -> skip_execution,   is_stale=false
//!   ANY, an upstream drifts   -> ready_to_execute,  is_stale=false
//!   ALL, one-of-two drifts    -> skip_execution,   is_stale=TRUE   (tolerated)
//!   ALL, all drift            -> ready_to_execute,  is_stale=false
//!
//! KEY FINDINGS:
//!   * ALL policy: rebuild only when EVERY direct parent drifted; otherwise the
//!     reuse is a SKIP that reports is_stale=TRUE (knowingly serving slightly
//!     stale data). This holds even at lag_tolerance=0 — proving the ALL skip is
//!     PURE POLICY, not lag absorption.
//!   * ANY policy: an upstream newer than the own build rebuilds even within
//!     tolerance (own-build anchor; lag_tolerance does not absorb). is_stale is
//!     FALSE on every execute.
//!   * `is_stale` is true ONLY on a tolerated-stale reuse. The engine now sets
//!     it on Verdict::Skip (decision.rs::evaluate_freshness). See
//!     docs/correctness-handoff.md §C.

#[path = "support.rs"]
mod support;

use dbt_state_harness::diff;
use dbt_state_proto::query_cache as qc;
use qc::execution_client::ExecutionClient;
use qc::sql_client::SqlClient;
use support::{channel, response_variant, start_server};

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/lag_multi_upstream_policy.jsonl"
);

fn lp_model2_decisions() -> Vec<(i64, i64, String, bool)> {
    // (policy, tol, decision_variant, is_stale) for each lp_model2 submit.
    let entries = diff::load_golden(FIXTURE).expect("load lag fixture");
    entries
        .iter()
        .filter(|e| {
            e.method == "SubmitEnrichedSQL"
                && e.request["labels"]["dbt_node_name"].as_str() == Some("lp_model2")
        })
        .map(|e| {
            let policy = e.request["stale_upstream_policy"].as_i64().unwrap_or(0);
            let tol = e.request["freshness_tolerance_seconds"]
                .as_i64()
                .unwrap_or(0);
            let v = diff::decision_variant(&e.response).unwrap();
            let ed = &e.response["response"][&v]["explained_decision"];
            let is_stale = ed["is_stale"].as_bool().unwrap_or(false);
            (policy, tol, v, is_stale)
        })
        .collect()
}

/// Pin the robustly-verified hosted facts by fixture inspection (no server).
///
/// NOTE: this corpus ALSO contains ANY-policy skips with is_stale=true and the
/// description "…its data is within lag tolerance" (entries where an upstream is
/// newer than the own build by LESS than lag_tolerance). That is live evidence
/// that `lag_tolerance` CAN gate a rebuild under ANY policy — contradicting the
/// prior "own-build anchor, tolerance never absorbs" conclusion. The exact rule
/// (which clock the tolerance is measured against) is NOT yet isolated, so the
/// engine conservatively still rebuilds on any ANY-drift (the SAFE direction).
/// See docs/correctness-handoff.md §C "OPEN: lag_tolerance gating".
#[test]
fn hosted_policy_is_stale_truth_table() {
    let decisions = lp_model2_decisions();
    assert!(!decisions.is_empty(), "fixture has lp_model2 submits");

    // ROBUST FACT 1: every EXECUTE reports is_stale=false (verified across all
    // ANY and ALL executes).
    for (_pol, _tol, v, is_stale) in &decisions {
        if v == "ready_to_execute" {
            assert!(!is_stale, "hosted execute must report is_stale=false");
        }
    }
    // ROBUST FACT 2: is_stale=true occurs ONLY on a skip (tolerated staleness),
    // never on an execute.
    for (_pol, _tol, v, is_stale) in &decisions {
        if *is_stale {
            assert_eq!(v, "skip_execution", "is_stale=true only on a skip");
        }
    }
    // ROBUST FACT 3: an ALL-policy (1) skip with is_stale=true exists even at
    // lag_tolerance=0 — proving the ALL skip is PURE POLICY (one-of-N parents
    // fresh), independent of lag.
    assert!(
        decisions
            .iter()
            .any(|(p, t, v, s)| *p == 1 && *t == 0 && v == "skip_execution" && *s),
        "ALL-policy tolerated-stale skip must occur at lag_tolerance=0"
    );
    // ROBUST FACT 4: an ALL-policy all-drift execute exists.
    assert!(
        decisions
            .iter()
            .any(|(p, _t, v, _s)| *p == 1 && v == "ready_to_execute"),
        "corpus must contain an ALL-policy all-drift execute"
    );
    // CHARACTERIZED (open): at least one ANY-policy skip with is_stale=true and a
    // non-zero tolerance exists — the lag_tolerance-gating evidence. Pinned so a
    // future engine change that implements it can be validated against this.
    assert!(
        decisions
            .iter()
            .any(|(p, t, v, s)| *p == 0 && *t > 0 && v == "skip_execution" && *s),
        "corpus must retain the ANY-policy within-lag-tolerance skip evidence"
    );
}

fn tbl(name: &str, epoch: i64) -> qc::TableModifiedInfo {
    qc::TableModifiedInfo {
        name: name.into(),
        last_modified_epoch: Some(epoch),
    }
}

/// Controlled replay against our server: ALL policy, two upstreams, only one
/// drifted → SKIP with is_stale=TRUE (reproduces the hosted contract). Then flip
/// to ANY → the same drift EXECUTES with is_stale=FALSE.
#[tokio::test]
async fn server_reproduces_all_policy_tolerated_stale_skip() {
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;
    let mut sql = SqlClient::new(ch.clone());
    let mut exec = ExecutionClient::new(ch);

    let target = "\"DB\".\"S\".\"LP_MODEL2\"";
    let up_a = "\"DB\".\"S\".\"LP_UP_A\"";
    let up_b = "\"DB\".\"S\".\"LP_UP_B\"";
    let build = 1_000_000i64;

    let make = |policy: i32, a_epoch: i64, b_epoch: i64| qc::SubmitEnrichedSqlRequest {
        target_table: Some(target.into()),
        dialect: "snowflake".into(),
        default_catalog: "DB".into(),
        default_schema: Some("S".into()),
        execution_type: 1,
        sql: format!("select a.id from {up_a} a cross join {up_b} b"),
        tolerate_nondeterminism: true,
        freshness_tolerance_seconds: 300,
        stale_upstream_policy: policy,
        tables: vec![tbl(target, build), tbl(up_a, a_epoch), tbl(up_b, b_epoch)],
        dbt_node_state: Some(qc::DbtNodeState {
            node_unique_id: "model.p.lp_model2".into(),
            node_body_hash: Some("body".into()),
            project_id: Some("p".into()),
            ..Default::default()
        }),
        ..Default::default()
    };

    // Baseline build (both upstreams at/under the build epoch) + confirm.
    let r0 = sql
        .submit_enriched_sql(make(1, build, build))
        .await
        .unwrap()
        .into_inner();
    let qc::submit_sql_response::Response::ReadyToExecute(r) = r0.response.unwrap() else {
        panic!("first build must execute");
    };
    exec.confirm_execution(qc::ConfirmExecutionRequest {
        request_id: r.request_id,
        last_modified_epoch: Some(build),
        ..Default::default()
    })
    .await
    .unwrap();

    // ALL policy, only upstream A drifted (A newer than build, B not) → SKIP,
    // is_stale=TRUE.
    let all = sql
        .submit_enriched_sql(make(1, build + 50_000, build))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&all), "skip_execution");
    let qc::submit_sql_response::Response::SkipExecution(s) = all.response.unwrap() else {
        unreachable!()
    };
    assert!(
        s.explained_decision.unwrap().is_stale,
        "ALL-policy partial-drift skip must report is_stale=true"
    );

    // ANY policy, same drift → EXECUTE, is_stale=FALSE.
    let any = sql
        .submit_enriched_sql(make(0, build + 50_000, build))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&any), "ready_to_execute");
    let qc::submit_sql_response::Response::ReadyToExecute(e) = any.response.unwrap() else {
        unreachable!()
    };
    assert!(
        !e.explained_decision.unwrap().is_stale,
        "an execute must report is_stale=false"
    );

    // ALL policy, BOTH upstreams drifted → EXECUTE, is_stale=FALSE.
    let both = sql
        .submit_enriched_sql(make(1, build + 50_000, build + 90_000))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response_variant(&both), "ready_to_execute");
}
