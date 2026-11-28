#!/usr/bin/env python3
"""Warehouse-free tests for the #271 per-case record (`register_clone_origin.py`).

A synthetic capture of case I's merge incremental: prod builds it, the
pre-step copies it into dev, the post-copy submit SKIPs and the dev relation
keeps prod's marker while the Reference holds dev's. Run:
python3 experiments/register_clone_origin_test.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import register_clone_origin as rco  # noqa: E402

RUN = "120000abcdef"
NAMES = {n: f"{n}_{RUN}" for n in ("ri_in", "ri_up", "ri_dm", "ri_ds", "rs_in", "rs_up",
                                   "rs_fix", "rs_dm", "rs_ds")}
PROD, DEV = '"CDO_DEMO_DB"."PROD_STATE_DEMO"', '"CDO_DEMO_DB"."DEV_CLONE_DEMO"'


def rel(schema, logical):
    return f'{schema}."{NAMES[logical].upper()}"'


def submit(schema, logical, rid, variant, decision, tables):
    payload = {"request_id": rid, "execution_decision_id": f"e-{rid}",
               "explained_decision": {"decision": decision, "skip_rejection_reason": None,
                                      "clone_rejection_reason": None,
                                      "decision_description": "d"}}
    return {"method": "SubmitEnrichedSQL",
            "request": {"target_table": rel(schema, logical), "execution_type": 3,
                        "freshness_tolerance_seconds": 0,
                        "tables": [{"name": n, "last_modified_epoch": e} for n, e in tables],
                        "labels": {"dbt_node_name": NAMES[logical]}},
            "response": {"response": {variant: payload}}}


def rows():
    return [
        submit(PROD, "ri_dm", "p1", "ready_to_execute", 1, [(rel(PROD, "ri_up"), 10)]),
        {"method": "ConfirmExecution", "request": {"request_id": "p1", "last_modified_epoch": 20},
         "response": {"success": True}},
        {"method": "RegisterClone",
         "request": {"target_table": rel(DEV, "ri_dm"), "execution_type": 3,
                     "clone_source_table": rel(PROD, "ri_dm"),
                     "clone_source_last_modified_epoch": 19,
                     "labels": {"dbt_node_name": NAMES["ri_dm"]}},
         "response": {"ready_to_clone": {"request_id": "c1", "execution_decision_id": "",
                                         "explained_decision": {"decision": 3,
                                                                "skip_rejection_reason": 5}}}},
        {"method": "ConfirmExecution", "request": {"request_id": "c1", "last_modified_epoch": 40},
         "response": {"success": True}},
        submit(DEV, "ri_dm", "d1", "skip_execution", 0,
               [(rel(DEV, "ri_dm"), 40), (rel(DEV, "ri_up"), 30)]),
        {"method": "GetExplainMessages", "request": {"execution_decision_ids": ["e-d1"]},
         "response": {"messages": [{"execution_decision_id": "e-d1", "decision": 0,
                                    "explain_lines": [{"text": "table analysis",
                                                       "children": [{"text": "exists",
                                                                     "marker": 1}]}]}]}},
    ]


def steps():
    read = lambda schema, logical: {"kind": "Read",  # noqa: E731
                                    "relation": f"CDO_DEMO_DB.{schema}.{NAMES[logical]}"}
    dev_build = {"kind": "Build", "target": "dev_clone", "select": [NAMES["ri_dm"]]}
    return {"run": RUN, "scenario": "register_clone_input_origin", "names": NAMES, "steps": [
        {"kind": "Build", "target": "prod_demo", "select": [NAMES["ri_dm"]]},
        read("PROD_STATE_DEMO", "ri_dm"),                # 1 prod
        dev_build,                                        # 2 first dev round
        read("DEV_CLONE_DEMO", "ri_dm"),                  # 3 served
        dev_build,                                        # 4 rerun
        read("DEV_CLONE_DEMO", "ri_dm"),                  # 5 after_rerun
        read("REF_STATE_DEMO", "ri_dm"),                  # 6 reference
    ]}


def reads(served="prod"):
    rel_ = lambda schema: f"CDO_DEMO_DB.{schema}.{NAMES['ri_dm']}"  # noqa: E731
    return [{"step": 1, "relation": rel_("PROD_STATE_DEMO"), "rows": [{"ID": 1, "V": "prod"}]},
            {"step": 3, "relation": rel_("DEV_CLONE_DEMO"), "rows": [{"ID": 1, "V": served}]},
            {"step": 5, "relation": rel_("DEV_CLONE_DEMO"), "rows": [{"ID": 1, "V": served}]},
            {"step": 6, "relation": rel_("REF_STATE_DEMO"), "rows": [{"ID": 1, "V": "dev"}]}]


def test_a_post_copy_skip_serving_prod_is_the_wrong_marker():
    rec = rco.record(rows(), steps(), reads())["i_dm"]
    assert rec["markers"] == {"prod": "prod", "served": "prod", "after_rerun": "prod",
                              "reference": "dev"}, rec["markers"]
    assert rec["wrong_marker"] is True
    [grant] = rec["register_clone"]
    assert (grant["clone_source_table"], grant["target_table"]) == (
        rel(PROD, "ri_dm"), rel(DEV, "ri_dm"))
    assert grant["confirm"] == {"success": True, "failed_to_clone": None,
                                "last_modified_epoch": 40}
    [first] = rec["dev_submits"]
    assert first["decision"] == "SKIP"
    assert first["inputs"] == [{"name": rel(DEV, "ri_up"), "last_modified_epoch": 30}]
    assert first["own_target_epoch"] == 40
    assert first["explain"]["lines"] == ["table analysis", "  exists [1]"]


def test_the_reference_marker_served_is_no_finding():
    assert rco.record(rows(), steps(), reads(served="dev"))["i_dm"]["wrong_marker"] is False


def test_a_missing_read_is_inconclusive():
    partial = [r for r in reads() if r["step"] != 6]
    assert rco.record(rows(), steps(), partial)["i_dm"]["wrong_marker"] is None


def test_a_snapshot_serves_its_current_row():
    history = [{"ID": 1, "V": "prod", "DBT_VALID_TO": "2026-11-28"},
               {"ID": 1, "V": "dev", "DBT_VALID_TO": None}]
    assert rco.marker(history, snapshot=True) == "dev"
    assert rco.marker(history, snapshot=False) == ["dev", "prod"]
    assert rco.marker(None, snapshot=True) is None


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
        print(f"PASS: {t.__name__}")
    print(f"ALL {len(tests)} REGISTER CLONE ORIGIN TESTS PASSED")
