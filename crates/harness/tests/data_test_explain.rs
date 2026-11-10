//! DATA-TEST EXECUTE Explain (#75, spec #40): hosted facts pinned from the
//! captures, read straight from the fixtures. The generic conformance replay
//! asserts that the server renders the same trees; these tests pin what the
//! captures show about the inputs that do NOT shape the tree.
//!
//! - `explain_data_test_history.jsonl` (client 2.0.11,
//!   `experiments/explain_capture.py data_test_execute`): a passing (`t75p`),
//!   a failing (`t75f`) and a `store_failures` (`t75s`) singular test over one
//!   source, through first execution, an unchanged rerun, a logic change and
//!   upstream data drift.
//! - `explain_incomplete_test_results.jsonl` (crafted,
//!   `experiments/incomplete_test_results.py`): a data test confirmed with
//!   `execution_results: null` or only `failures`, then resubmitted.

use std::collections::HashMap;

use dbt_state_harness::diff::{self, GoldenEntry};
use serde_json::Value;

const HISTORY: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/explain_data_test_history.jsonl"
);
const INCOMPLETE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/explain_incomplete_test_results.jsonl"
);

fn variant(e: &GoldenEntry) -> (&str, &Value) {
    let (k, v) = e.response["response"]
        .as_object()
        .and_then(|o| o.iter().next())
        .expect("submit response variant");
    (k.as_str(), v)
}

/// The tree with its runtime slots masked: an upstream's quoted name and its
/// `last updated` time.
fn masked(lines: &Value) -> Value {
    let lines = lines.as_array().expect("explain lines");
    Value::Array(
        lines
            .iter()
            .map(|l| {
                let text = l["text"].as_str().unwrap_or_default();
                let text = if text.starts_with("last updated:") {
                    "last updated: «rel_time»"
                } else if text.starts_with('"') && text.ends_with('"') {
                    "«FQN»"
                } else {
                    text
                };
                serde_json::json!({
                    "text": text,
                    "marker": l["marker"],
                    "badge": l["badge"],
                    "children": masked(&l["children"]),
                })
            })
            .collect(),
    )
}

/// Node name (logical prefix, e.g. `t75p`) of every data-test submit, keyed by
/// the decision id it minted.
fn decisions(entries: &[GoldenEntry]) -> HashMap<String, (usize, String)> {
    entries
        .iter()
        .enumerate()
        .filter(|(_, e)| e.method == "SubmitEnrichedSQL")
        .map(|(i, e)| {
            let name = e.request["labels"]["dbt_node_name"].as_str().unwrap();
            let edid = variant(e).1["execution_decision_id"].as_str().unwrap();
            (edid.to_string(), (i, name[..4].to_string()))
        })
        .collect()
}

/// Every explained decision: (submit entry index, node prefix, decision, tree).
fn explained(entries: &[GoldenEntry]) -> Vec<(usize, String, i64, Value)> {
    let minted = decisions(entries);
    let mut out: Vec<_> = entries
        .iter()
        .filter(|e| e.method == "GetExplainMessages")
        .flat_map(|e| e.response["messages"].as_array().unwrap().clone())
        .map(|m| {
            let (i, node) = minted[m["execution_decision_id"].as_str().unwrap()].clone();
            (
                i,
                node,
                m["decision"].as_i64().unwrap(),
                masked(&m["explain_lines"]),
            )
        })
        .collect();
    out.sort_by_key(|(i, ..)| *i);
    out
}

/// The test outcome does not shape the tree: in every round the passing and
/// the failing test get the same decision and the same tree, although the
/// failing test is confirmed with `should_error: true` and the passing one is
/// not.
#[test]
fn passing_and_failing_tests_render_the_same_tree() {
    let entries = diff::load_golden(HISTORY).expect("load fixture");
    let outcome = |prefix: &str| -> Vec<bool> {
        let request_ids: Vec<&str> = entries
            .iter()
            .filter(|e| {
                e.method == "SubmitEnrichedSQL"
                    && e.request["labels"]["dbt_node_name"]
                        .as_str()
                        .is_some_and(|n| n.starts_with(prefix))
            })
            .filter_map(|e| variant(e).1["request_id"].as_str())
            .collect();
        entries
            .iter()
            .filter(|e| {
                e.method == "ConfirmExecution"
                    && request_ids.contains(&e.request["request_id"].as_str().unwrap_or_default())
            })
            .map(|e| {
                e.request["execution_results"]["fields"]["should_error"]["kind"]["bool_value"]
                    .as_bool()
                    .expect("complete results")
            })
            .collect()
    };
    assert_eq!(outcome("t75p"), vec![false; 3], "the passing test passes");
    assert_eq!(outcome("t75f"), vec![true; 3], "the failing test fails");

    let trees = explained(&entries);
    let of = |prefix: &str| -> Vec<(i64, Value)> {
        trees
            .iter()
            .filter(|(_, n, ..)| n == prefix)
            .map(|(_, _, d, t)| (*d, t.clone()))
            .collect()
    };
    let (passing, failing) = (of("t75p"), of("t75f"));
    assert_eq!(passing.len(), 4, "four rounds explained");
    assert_eq!(passing, failing);
    let decisions: Vec<i64> = passing.iter().map(|(d, _)| *d).collect();
    assert_eq!(
        decisions,
        vec![1, 0, 1, 1],
        "EXECUTE, SKIP, EXECUTE, EXECUTE"
    );
}

/// One EXECUTE tree for a first execution, a logic change and upstream data
/// drift: the tested upstream, no badge, its `last updated` time.
#[test]
fn data_test_execute_tree_does_not_depend_on_the_rebuild_reason() {
    let entries = diff::load_golden(HISTORY).expect("load fixture");
    let expected = serde_json::json!([{
        "text": "data freshness analysis", "marker": null, "badge": null,
        "children": [{
            "text": "upstream dependencies", "marker": null, "badge": null,
            "children": [{
                "text": "«FQN»", "marker": null, "badge": null,
                "children": [{
                    "text": "last updated: «rel_time»", "marker": null, "badge": null,
                    "children": [],
                }],
            }],
        }],
    }]);
    let executes: Vec<_> = explained(&entries)
        .into_iter()
        .filter(|(_, _, d, _)| *d == 1)
        .collect();
    assert_eq!(executes.len(), 8, "3 first, 2 logic change, 3 drift");
    for (i, node, _, tree) in executes {
        assert_eq!(tree, expected, "entry {i} ({node})");
    }
}

/// The `store_failures` test's audit relation (a table named after the test)
/// is reported in `tables` once it exists, but hosted neither rebuilds for it
/// nor lists it in the tree (divergence #88).
#[test]
fn store_failures_audit_relation_is_reported_but_not_listed() {
    let entries = diff::load_golden(HISTORY).expect("load fixture");
    let submits: Vec<(&str, Vec<String>)> = entries
        .iter()
        .filter(|e| {
            e.method == "SubmitEnrichedSQL"
                && e.request["labels"]["dbt_node_name"]
                    .as_str()
                    .is_some_and(|n| n.starts_with("t75s"))
        })
        .map(|e| {
            let name = e.request["labels"]["dbt_node_name"].as_str().unwrap();
            let audit = format!("\"{}\"", name.to_uppercase());
            assert!(
                e.request["target_table"].is_null(),
                "a data test has no target"
            );
            let tables = e.request["tables"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap().to_string())
                .map(|t| {
                    if t.ends_with(&audit) {
                        "audit".into()
                    } else {
                        t
                    }
                })
                .collect();
            (variant(e).0, tables)
        })
        .collect();
    let shape: Vec<(&str, usize, bool)> = submits
        .iter()
        .map(|(v, t)| (*v, t.len(), t.iter().any(|n| n == "audit")))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("ready_to_execute", 1, false),
            ("skip_execution", 2, true),
            ("skip_execution", 2, true),
            ("ready_to_execute", 2, true),
        ]
    );

    let raw: Vec<&Value> = entries
        .iter()
        .filter(|e| e.method == "GetExplainMessages")
        .flat_map(|e| e.response["messages"].as_array().unwrap())
        .collect();
    let minted = decisions(&entries);
    for m in raw {
        let (_, node) = &minted[m["execution_decision_id"].as_str().unwrap()];
        if node != "t75s" {
            continue;
        }
        let text = m["explain_lines"].to_string();
        assert!(text.contains("T75SRC_"), "the source is listed");
        assert!(
            !text.contains("\\\"T75S_"),
            "the audit relation is not listed: {text}"
        );
    }
}

/// Crafted: hosted reuses a data test confirmed without complete results and
/// serves what was confirmed (divergence #87: the server rebuilds).
#[test]
fn hosted_serves_incomplete_cached_test_results() {
    let entries = diff::load_golden(INCOMPLETE).expect("load fixture");
    let confirmed: Vec<Value> = entries
        .iter()
        .filter(|e| e.method == "ConfirmExecution")
        .map(|e| e.request["execution_results"].clone())
        .collect();
    assert_eq!(
        confirmed,
        vec![
            Value::Null,
            serde_json::json!({"fields": {"failures": {"kind": {"int_value": 0}}}}),
        ]
    );
    let resubmits: Vec<(&str, Value)> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .map(variant)
        .filter(|(v, _)| *v == "skip_execution")
        .map(|(v, b)| (v, b["execution_results"].clone()))
        .collect();
    assert_eq!(
        resubmits,
        vec![
            ("skip_execution", serde_json::json!({"fields": {}})),
            (
                "skip_execution",
                serde_json::json!({"fields": {"failures": {"kind": {"int_value": 0}}}})
            ),
        ]
    );
    let trees = explained(&entries);
    assert_eq!(trees.len(), 2);
    for (_, _, decision, tree) in trees {
        assert_eq!(decision, 0, "explained as a SKIP");
        assert_eq!(
            tree[0]["children"][0]["text"], "the test query has not changed",
            "the ordinary data-test SKIP tree"
        );
    }
}
