//! Conformance: dbt State-powered `state:*` selectors
//! (`SelectorService.GetStateSelection`) reproduce the REAL hosted service.
//!
//! GOLDEN SOURCE (live, api.state.dbt.com, dbt v2 client, Snowflake). Captured
//! via the recording proxy with `experiments/selector_experiment.sh`: a
//! jaffle-shop baseline built in `prod_demo` records each node's `dbt_node_state`
//! (the hosted comparison state), then `dbt compile --select state:<crit>` from
//! `dev_clone` (`defer_to_target: prod_demo`) issues `GetStateSelection` for
//! every `SelectorCriteria` and change type. The fixture
//! `golden/fixtures/selector_state_selection.jsonl` holds the baseline
//! submits/confirms and the 67 distinct selector request/response pairs.
//!
//! The hosted comparison is a PURE hash comparison of the client-supplied
//! `DbtNodeData` against the recorded node state, keyed by (org, `project_id`,
//! `target` env, `node_unique_id`). We seed the recorded state directly from the
//! captured `dbt_node_state` (exactly what the hosted service recorded — the
//! `SubmitEnrichedSQL` decision path is irrelevant to the selector), then replay
//! each captured `GetStateSelection` request through OUR server and assert the
//! returned `node_unique_ids` SET-equals the hosted response.
//!
//! This pins every verified rule: new/old and modified/unmodified are exact
//! complements; `modified = new ∪ body ∪ configs ∪ persisted_descriptions ∪
//! macros ∪ contract` (relation EXCLUDED); each `modified.X` isolates its own
//! component; a new node matches every `modified.*`; untracked node types are
//! always new/modified.

#[path = "support.rs"]
mod support;

use std::collections::BTreeSet;

use dbt_state_harness::diff::{load_golden, GoldenEntry};
use dbt_state_proto::query_cache as qc;
use qc::selector_service_client::SelectorServiceClient;
use serde_json::Value;
use support::{channel, request_with_org, schema_pool, start_server};

const FIXTURE: &str = "../../golden/fixtures/selector_state_selection.jsonl";

/// A recorded node-state row seeded from a captured baseline `dbt_node_state`.
struct Recorded {
    org_id: String,
    project_id: String,
    target_name: String,
    node_unique_id: String,
    body: Option<String>,
    configs: Option<String>,
    persisted_descriptions: Option<String>,
    macros: Option<String>,
    contract: Option<String>,
    database_representation: Option<String>,
}

fn s(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(|x| x.as_str()).map(|x| x.to_string())
}

fn org_of(entry: &GoldenEntry) -> String {
    entry
        .metadata
        .iter()
        .find(|(k, _)| k == "x-organization-id")
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| "local".to_string())
}

/// Reconstruct the recorded comparison state: the last `dbt_node_state` per
/// (org, project_id, target_name, node_unique_id) across the baseline submits.
fn baseline_from(entries: &[GoldenEntry]) -> Vec<Recorded> {
    use std::collections::BTreeMap;
    let mut map: BTreeMap<(String, String, String, String), Recorded> = BTreeMap::new();
    for e in entries {
        if e.method != "SubmitEnrichedSQL" && e.method != "SubmitValues" {
            continue;
        }
        let ns = match e.request.get("dbt_node_state") {
            Some(ns) if ns.is_object() => ns,
            _ => continue,
        };
        let (uid, tgt, pid) = match (
            s(ns, "node_unique_id"),
            s(ns, "target_name"),
            s(ns, "project_id"),
        ) {
            (Some(u), Some(t), Some(p)) => (u, t, p),
            _ => continue,
        };
        let org = org_of(e);
        map.insert(
            (org.clone(), pid.clone(), tgt.clone(), uid.clone()),
            Recorded {
                org_id: org,
                project_id: pid,
                target_name: tgt,
                node_unique_id: uid,
                body: s(ns, "node_body_hash"),
                configs: s(ns, "node_configs_hash"),
                persisted_descriptions: s(ns, "node_persisted_descriptions_hash"),
                macros: s(ns, "node_macros_hash"),
                contract: s(ns, "node_contract_hash"),
                database_representation: s(ns, "node_database_representation"),
            },
        );
    }
    map.into_values().collect()
}

/// Parse a captured `GetStateSelection` request into a typed `SelectorRequest`.
fn selector_request(entry: &GoldenEntry) -> qc::SelectorRequest {
    serde_json::from_value(entry.request.clone()).expect("decode SelectorRequest")
}

#[tokio::test]
async fn get_state_selection_reproduces_hosted_responses() {
    let entries = load_golden(FIXTURE).expect("load selector fixture");
    let (addr, schema) = start_server().await;
    let pool = schema_pool(&schema).await;

    // Seed the recorded comparison state directly (the captured dbt_node_state
    // IS what the hosted service recorded; the SQL decision path is irrelevant
    // to the selector).
    for r in baseline_from(&entries) {
        sqlx::query(
            r#"
            INSERT INTO node_states (
                org_id, project_id, target_name, node_unique_id,
                node_body_hash, node_configs_hash, node_persisted_descriptions_hash,
                node_macros_hash, node_contract_hash, node_database_representation
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (org_id, project_id, target_name, node_unique_id) DO UPDATE SET
                node_body_hash = EXCLUDED.node_body_hash,
                node_configs_hash = EXCLUDED.node_configs_hash,
                node_persisted_descriptions_hash = EXCLUDED.node_persisted_descriptions_hash,
                node_macros_hash = EXCLUDED.node_macros_hash,
                node_contract_hash = EXCLUDED.node_contract_hash,
                node_database_representation = EXCLUDED.node_database_representation
            "#,
        )
        .bind(&r.org_id)
        .bind(&r.project_id)
        .bind(&r.target_name)
        .bind(&r.node_unique_id)
        .bind(&r.body)
        .bind(&r.configs)
        .bind(&r.persisted_descriptions)
        .bind(&r.macros)
        .bind(&r.contract)
        .bind(&r.database_representation)
        .execute(&pool)
        .await
        .expect("seed node_states");
    }

    let ch = channel(addr).await;
    let mut queries = 0usize;
    for e in &entries {
        if e.method != "GetStateSelection" {
            continue;
        }
        queries += 1;
        let req = selector_request(e);
        let org = org_of(e);
        let expected: BTreeSet<String> = e
            .response
            .get("node_unique_ids")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let resp = SelectorServiceClient::new(ch.clone())
            .get_state_selection(request_with_org(req.clone(), &org))
            .await
            .expect("get_state_selection")
            .into_inner();
        let got: BTreeSet<String> = resp.node_unique_ids.into_iter().collect();

        assert_eq!(
            got, expected,
            "criterion {} (target={}, sent={} nodes): selection set mismatch\n  only ours: {:?}\n  only hosted: {:?}",
            req.selector_criteria,
            req.target,
            req.nodes.len(),
            got.difference(&expected).collect::<Vec<_>>(),
            expected.difference(&got).collect::<Vec<_>>(),
        );
    }
    assert!(
        queries >= 60,
        "expected the full captured query corpus, got {queries}"
    );
}

/// LAST-WRITE-WINS conformance (live-captured,
/// golden/fixtures/selector_last_write_wins.jsonl,
/// experiments/selector_last_write_wins.sh). Replays the capture IN ORDER
/// through our server: `stg_customers` is recorded (V1), `state:modified` is
/// queried (not selected), then `stg_customers` is re-recorded with a changed
/// body (V2), and `state:modified` is queried again (now selected). Our
/// latest-wins `node_states` upsert must reproduce BOTH captured selector
/// responses, proving the baseline tracks the NEWEST recorded execution.
#[tokio::test]
async fn last_write_wins_baseline_reproduces_hosted() {
    const LWW_FIXTURE: &str = "../../golden/fixtures/selector_last_write_wins.jsonl";
    let entries = load_golden(LWW_FIXTURE).expect("load last-write-wins fixture");
    let (addr, _schema) = start_server().await;
    let ch = channel(addr).await;

    let mut selector_checks = 0usize;
    for e in &entries {
        let org = org_of(e);
        match e.method.as_str() {
            // Replay each record through the real record path (latest wins).
            "SubmitEnrichedSQL" => {
                let req: qc::SubmitEnrichedSqlRequest =
                    serde_json::from_value(e.request.clone()).expect("decode submit");
                qc::sql_client::SqlClient::new(ch.clone())
                    .submit_enriched_sql(request_with_org(req, &org))
                    .await
                    .expect("replay submit");
            }
            "GetStateSelection" => {
                let req = selector_request(e);
                let expected: BTreeSet<String> = e
                    .response
                    .get("node_unique_ids")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                let resp = SelectorServiceClient::new(ch.clone())
                    .get_state_selection(request_with_org(req.clone(), &org))
                    .await
                    .expect("replay selector")
                    .into_inner();
                let got: BTreeSet<String> = resp.node_unique_ids.into_iter().collect();
                // The captured corpus for this fixture sent a subset of the full
                // project; assert set-equality on the stg_customers membership,
                // which is the discriminating signal for last-write-wins.
                let uid = "model.jaffle_shop.stg_customers";
                assert_eq!(
                    got.contains(uid),
                    expected.contains(uid),
                    "stg_customers modified-membership must match hosted at query {selector_checks}"
                );
                selector_checks += 1;
            }
            _ => {}
        }
    }
    assert_eq!(
        selector_checks, 2,
        "expected the before/after selector queries from the capture"
    );
}
