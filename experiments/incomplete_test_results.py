#!/usr/bin/env python3
"""Crafted probes: a data test confirmed without complete cached results (#75, spec #40).

The real client always confirms a data test with `failures`, `should_warn`
and `should_error` (explain_data_test_history.jsonl). These probes confirm a
data test with less and resubmit it unchanged, to see whether hosted reuses
the result and how it explains the decision:

  none     ConfirmExecution with `execution_results: null`
  partial  ConfirmExecution with only `failures` (no should_warn / should_error)

Each probe: data test T over source S (fresh, never-created names in
CDO_DEMO_DB.PROD_STATE_DEMO; no warehouse is touched) is submitted (first
execution), confirmed with the incomplete results, then submitted again with
the same request, and Explain is asked for that second decision. The request
copies the shape the 2.0.11 client sends for a singular data test.

Every call goes through `PROBE_URL`: point it at your own recording proxy for
a hosted capture, or at a local server for a dry run.

Usage: incomplete_test_results.py PROBE [PROBE ...]   (or `all`)
Prints one JSON line per step; a failed RPC prints an `error` row and the
remaining probes still run.
"""
import json
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lag_matrix as lm  # noqa: E402
from lag_matrix import DB, NAMESPACE, fqn, h, now_ms, probe  # noqa: E402

RUN = f"{time.strftime('%H%M%S')}{uuid.uuid4().hex[:6]}"

RESULTS = {
    "none": None,
    "partial": {"fields": {"failures": {"kind": {"int_value": 0}}}},
}


def request(test, source, source_epoch):
    uid = f"test.jaffle_shop.{test}"
    return {
        "target_table": None,
        "dialect": "snowflake",
        "default_catalog": DB,
        "execution_type": 8,
        "sql": f"select id, v from {DB}.prod_state_demo.{source} where id < 0",
        "tables": [{"name": fqn(source), "last_modified_epoch": source_epoch}],
        "query_dependencies": [],
        "semantic_extras": {
            "fail_calc": '"count(*)"',
            "severity": '"ERROR"',
            "error_if": '"!= 0"',
            "warn_if": '"!= 0"',
        },
        "freshness_tolerance_seconds": 0,
        "lenient_dependencies": [],
        "tolerate_nondeterminism": True,
        "labels": {
            "dbt_node_unique_id": uid,
            "dbt_node_fqn": f"jaffle_shop.{test}",
            "dbt_node_name": test,
        },
        "clone_time_travel_limit": None,
        "clone_table_properties": None,
        "stale_upstream_policy": 0,
        "clone_chain_depth_limit": None,
        "dbt_node_state": {
            "node_unique_id": uid,
            "target_name": "prod_demo",
            "project_name": "jaffle_shop",
            "resource_type": "test",
            "node_hash": h("n" + test),
            "node_body_hash": h("b" + test),
            "node_configs_hash": None,
            "node_persisted_descriptions_hash": None,
            "node_macros_hash": None,
            "node_contract_hash": None,
            "profile_name": "snowflake",
            "project_id": "70437463667433",
            "node_database_representation": None,
        },
        "default_schema": "prod_state_demo",
        "compare_unrendered_code": False,
        "table_namespace": NAMESPACE,
        "ignore_external_modifications": False,
        "allow_clones": True,
        "is_defer_to_profile": False,
        "defer_enabled": True,
    }


def submit(req):
    ((variant, body),) = probe("submit", req)["response"].items()
    return variant, body


def row(probe_id, step, test, variant, body, **facts):
    ed = body.get("explained_decision") or {}
    return {"probe": probe_id, "step": step, "node": test, "variant": variant,
            "srr": ed.get("skip_rejection_reason"), "crr": ed.get("clone_rejection_reason"),
            "is_stale": ed.get("is_stale"), "description": ed.get("decision_description"),
            **facts}


def run_probe(probe_id):
    test, source = f"itr{RUN}_{probe_id}_t", f"itr{RUN}_{probe_id}_s"
    req = request(test, source, now_ms() - 3_600_000)
    v, b = submit(req)
    print(json.dumps(row(probe_id, "first", test, v, b)), flush=True)
    assert v == "ready_to_execute", f"{test}: expected a first execution, got {v}"
    c = probe("confirm", {
        "request_id": b["request_id"],
        "last_modified_epoch": None,
        "failed_to_clone": False,
        "table_type": None,
        "execution_results": RESULTS[probe_id],
        "execution_runtime_ms": 1000,
        "labels": req["labels"],
    })
    print(json.dumps({"probe": probe_id, "step": "confirm", "node": test, "confirm": c}), flush=True)
    time.sleep(2)
    v, b = submit(req)
    out = row(probe_id, "resubmit", test, v, b)
    if b.get("execution_decision_id"):
        time.sleep(2)
        out["explain"] = probe("explain", {"execution_decision_ids": [b["execution_decision_id"]]})
    print(json.dumps(out), flush=True)


if __name__ == "__main__":
    lm.RUN = RUN
    wanted = sys.argv[1:] or ["all"]
    names = list(RESULTS) if wanted == ["all"] else wanted
    failed = False
    for n in names:
        try:
            run_probe(n)
        except (SystemExit, AssertionError) as e:
            failed = True
            print(json.dumps({"probe": n, "error": str(e)}), flush=True)
    sys.exit(1 if failed else 0)
