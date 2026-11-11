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
//! - `explain_store_failures_audit.jsonl` (client 2.0.11,
//!   `experiments/explain_capture.py store_failures_audit`): a failing
//!   `store_failures` test and a `store_failures_as: view` test, first
//!   execution and unchanged rerun (#88).
//! - `explain_store_failures_audit_schema.jsonl` and
//!   `explain_store_failures_audit_alias.jsonl` (client 2.0.11,
//!   `experiments/explain_capture.py store_failures_audit_schema` /
//!   `store_failures_audit_alias`): `store_failures` tests whose audit relation
//!   is in a separate audit schema or under a configured alias (#106).

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
const AUDIT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../golden/fixtures/explain_store_failures_audit.jsonl"
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
/// nor lists it in the tree. The server treats it as the test's own write
/// (#88, `execution.rs::is_own_audit_relation`); the conformance replay
/// asserts the same decisions and trees.
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

/// The two `store_failures` cases `explain_data_test_history.jsonl` lacks
/// (`explain_store_failures_audit.jsonl`): a failing test, whose audit table
/// holds its failing row, and a `store_failures_as: view` test, whose audit
/// relation is a view. On the unchanged rerun each reports its audit relation
/// in `tables` (not as a query dependency), and hosted SKIPs both, serving the
/// failing test its cached failure, with trees that list only the source.
#[test]
fn failing_and_view_store_failures_tests_skip_without_listing_the_audit_relation() {
    let entries = diff::load_golden(AUDIT).expect("load fixture");
    let submits: Vec<(String, &str, Vec<&str>, Value)> = entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .map(|e| {
            let name = e.request["labels"]["dbt_node_name"].as_str().unwrap();
            let audit = format!(
                "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"{}\"",
                name.to_uppercase()
            );
            assert_eq!(e.request["query_dependencies"], serde_json::json!([]));
            let tables = e.request["tables"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap())
                .map(|t| {
                    if t == audit {
                        "audit"
                    } else if t.contains("\"T88SRC_") {
                        "source"
                    } else {
                        "other"
                    }
                })
                .collect();
            let (v, body) = variant(e);
            let kind = e.request["semantic_extras"]["store_failures_as"]
                .as_str()
                .unwrap();
            (
                format!("{}:{kind}", &name[..4]),
                v,
                tables,
                body["execution_results"]["fields"]["failures"].clone(),
            )
        })
        .collect();
    let failures = |n: i64| serde_json::json!({"kind": {"int_value": n}});
    let row = |node: &str, v, tables: &[&'static str], f| (node.to_string(), v, tables.to_vec(), f);
    assert_eq!(
        submits,
        vec![
            row(
                "t88f:\"table\"",
                "ready_to_execute",
                &["source"],
                Value::Null
            ),
            row(
                "t88v:\"view\"",
                "ready_to_execute",
                &["source"],
                Value::Null
            ),
            row(
                "t88v:\"view\"",
                "skip_execution",
                &["source", "audit"],
                failures(0)
            ),
            row(
                "t88f:\"table\"",
                "skip_execution",
                &["audit", "source"],
                failures(1)
            ),
        ]
    );

    let trees = explained(&entries);
    assert_eq!(trees.len(), 2, "both unchanged reruns explained");
    for (i, node, decision, tree) in trees {
        assert_eq!(decision, 0, "entry {i} ({node}) explained as a SKIP");
        assert_eq!(
            tree[1]["children"][0]["children"].as_array().unwrap().len(),
            1,
            "entry {i} ({node}): only the source is listed"
        );
    }
    let raw: Vec<String> = entries
        .iter()
        .filter(|e| e.method == "GetExplainMessages")
        .map(|e| e.response.to_string())
        .collect();
    for text in raw {
        assert!(text.contains("T88SRC_"), "the source is listed");
        assert!(
            !text.contains("\\\"T88F_") && !text.contains("\\\"T88V_"),
            "no audit relation is listed: {text}"
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

/// What the client sends for a `store_failures` test whose audit relation is
/// not `<target schema>.<test name>` (#106), and what hosted decides on the
/// unchanged rerun. Per submit: the node (logical prefix), the response
/// variant, `default_schema`, and `tables` with the source and the audit
/// relation named.
fn audit_case(
    path: &str,
    audit: impl Fn(&str) -> String,
) -> Vec<(String, String, String, Vec<String>)> {
    let entries = diff::load_golden(path).expect("load fixture");
    entries
        .iter()
        .filter(|e| e.method == "SubmitEnrichedSQL")
        .map(|e| {
            let name = e.request["labels"]["dbt_node_name"].as_str().unwrap();
            assert!(
                e.request["target_table"].is_null(),
                "a data test has no target"
            );
            assert_eq!(e.request["query_dependencies"], serde_json::json!([]));
            assert_eq!(e.request["semantic_extras"]["store_failures"], "true");
            let audit = audit(name);
            let tables = e.request["tables"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap())
                .map(|t| match t {
                    t if t == audit => "audit".to_string(),
                    t if t.starts_with("\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"T106")
                        && t.contains("SRC_") =>
                    {
                        "source".to_string()
                    }
                    t => t.to_string(),
                })
                .collect();
            (
                name.split('_').next().unwrap().to_string(),
                variant(e).0.to_string(),
                e.request["default_schema"].as_str().unwrap().to_string(),
                tables,
            )
        })
        .collect()
}

fn row(
    node: &str,
    v: &str,
    schema: &str,
    tables: &[&str],
) -> (String, String, String, Vec<String>) {
    let tables = tables.iter().map(|t| t.to_string()).collect();
    (node.into(), v.into(), schema.into(), tables)
}

/// Every Explain tree in the fixture is a SKIP that lists only the source.
fn skip_trees_list_only_the_source(path: &str) {
    let entries = diff::load_golden(path).expect("load fixture");
    let messages: Vec<&Value> = entries
        .iter()
        .filter(|e| e.method == "GetExplainMessages")
        .flat_map(|e| e.response["messages"].as_array().unwrap())
        .collect();
    assert_eq!(messages.len(), 2, "both unchanged reruns explained");
    for m in messages {
        assert_eq!(m["decision"], 0, "explained as a SKIP");
        let upstreams = &masked(&m["explain_lines"])[1]["children"][0]["children"];
        assert_eq!(
            upstreams.as_array().unwrap().len(),
            1,
            "one upstream listed"
        );
        let text = m["explain_lines"].to_string();
        assert!(text.contains("SRC_"), "the source is listed: {text}");
    }
}

/// `explain_store_failures_audit_schema.jsonl`: the run's schema macro puts
/// the tests in `<target schema>_dbt_test__audit`. The client sends that
/// schema as `default_schema` and reports the audit relation there, so the
/// request names it exactly as `default_catalog`.`default_schema`.`dbt_node_name`
/// (the #88 rule). Hosted SKIPs the unchanged reruns, listing only the source,
/// which stays in the target schema.
#[test]
fn audit_relation_in_a_separate_schema_is_named_by_the_request() {
    const AUDIT_SCHEMA: &str = "PROD_STATE_DEMO_DBT_TEST__AUDIT";
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/explain_store_failures_audit_schema.jsonl"
    );
    let submits = audit_case(path, |name| {
        format!(
            "\"CDO_DEMO_DB\".\"{AUDIT_SCHEMA}\".\"{}\"",
            name.to_uppercase()
        )
    });
    let schema = AUDIT_SCHEMA.to_lowercase();
    assert_eq!(
        submits,
        vec![
            row("t106s", "ready_to_execute", &schema, &["source"]),
            row("t106v", "ready_to_execute", &schema, &["source"]),
            row("t106s", "skip_execution", &schema, &["source", "audit"]),
            row("t106v", "skip_execution", &schema, &["source", "audit"]),
        ]
    );
    skip_trees_list_only_the_source(path);
}

/// `explain_store_failures_audit_alias.jsonl`: tests with a configured
/// `alias` (`<test>_aud`). The audit relation is in the target schema under
/// the alias, and the alias appears in no other request field (labels,
/// semantic extras, node state) nor in the ConfirmExecution. Hosted SKIPs the
/// unchanged reruns anyway and lists only the source. The server cannot name
/// the aliased relation from the request and rebuilds (keyed replay gaps,
/// #40 divergence rule).
#[test]
fn aliased_audit_relation_is_not_named_by_the_request() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../golden/fixtures/explain_store_failures_audit_alias.jsonl"
    );
    let submits = audit_case(path, |name| {
        format!(
            "\"CDO_DEMO_DB\".\"PROD_STATE_DEMO\".\"{}_AUD\"",
            name.to_uppercase()
        )
    });
    assert_eq!(
        submits,
        vec![
            row("t106a", "ready_to_execute", "prod_state_demo", &["source"]),
            row("t106b", "ready_to_execute", "prod_state_demo", &["source"]),
            row(
                "t106a",
                "skip_execution",
                "prod_state_demo",
                &["source", "audit"]
            ),
            row(
                "t106b",
                "skip_execution",
                "prod_state_demo",
                &["source", "audit"]
            ),
        ]
    );
    for e in diff::load_golden(path).expect("load fixture") {
        if !matches!(
            e.method.as_str(),
            "SubmitEnrichedSQL" | "SubmitEnrichedSQLSpeculative" | "ConfirmExecution"
        ) {
            continue;
        }
        let mut request = e.request.clone();
        request["tables"] = Value::Null;
        let text = request.to_string().to_lowercase();
        assert!(
            !text.contains("_aud"),
            "the alias is only in `tables`: {text}"
        );
    }
    skip_trees_list_only_the_source(path);
}
