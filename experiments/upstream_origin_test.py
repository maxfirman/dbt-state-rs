#!/usr/bin/env python3
"""Warehouse-free tests for the #237 per-case record (`upstream_origin.py`).

A synthetic capture of two cases: B (dev upstream copied by RegisterClone,
downstream copied on submit, rows equal to the Reference) and D (dev upstream
built, downstream copied from prod while the Reference serves the dev
marker). Run: python3 experiments/upstream_origin_test.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import upstream_origin as uo  # noqa: E402

RUN = "120000abcdef"
NAMES = {f"o{c}_{k}": f"o{c}_{k}_{RUN}" for c in "abcd" for k in ("in", "up", "dn")}
P, D = '"CDO_DEMO_DB"."PROD_STATE_DEMO"', '"CDO_DEMO_DB"."DEV_CLONE_DEMO"'


def rel(schema, logical):
    return f'{schema}."{NAMES[logical].upper()}"'


def submit(schema, logical, et, variant, rid, decision, desc, srr=None, crr=None, source=None,
           allow=True, depth=None, tolerance=0, method="SubmitEnrichedSQL", tables=()):
    payload = {"request_id": rid, "execution_decision_id": f"e-{rid}",
               "explained_decision": {"decision": decision, "skip_rejection_reason": srr,
                                      "clone_rejection_reason": crr, "is_stale": False,
                                      "decision_description": desc}}
    if source:
        payload.update(clone_source=source, clone_target=rel(schema, logical),
                       clone_required_last_modified_epoch=1000,
                       clone_sqls=[f"CREATE OR REPLACE TRANSIENT TABLE {rel(schema, logical)} "
                                   f"CLONE {source} COPY GRANTS"])
    return {"method": method,
            "request": {"target_table": rel(schema, logical), "execution_type": et,
                        "sql": f"select id, v from x_{logical}", "allow_clones": allow,
                        "tables": [{"name": n, "last_modified_epoch": e} for n, e in tables],
                        "clone_chain_depth_limit": depth, "freshness_tolerance_seconds": tolerance,
                        "labels": {"dbt_node_name": NAMES[logical]}},
            "response": {"response": {variant: payload}}}


def register_clone(logical, rid, source):
    payload = {"request_id": rid, "clone_source": source, "execution_decision_id": "",
               "explained_decision": {"decision": 3, "skip_rejection_reason": 5,
                                      "clone_rejection_reason": None, "decision_description": ""}}
    return {"method": "RegisterClone",
            "request": {"target_table": rel(D, logical), "execution_type": 3,
                        "clone_source_table": source, "clone_chain_depth_limit": None,
                        "labels": {"dbt_node_name": NAMES[logical]}},
            "response": {"ready_to_clone": payload}}


def confirm(rid, failed=False, success=True):
    return {"method": "ConfirmExecution",
            "request": {"request_id": rid, "failed_to_clone": failed, "last_modified_epoch": 2000},
            "response": {"success": success, "request_id": rid}}


CLONE = "an equivalent model exists under another name so we cloned that one"
ROWS = [
    # prod
    submit(P, "ob_up", 3, "ready_to_execute", "pbu", 1, "did not exist", 6, 6), confirm("pbu"),
    submit(P, "od_up", 3, "ready_to_execute", "pdu", 1, "did not exist", 6, 6), confirm("pdu"),
    submit(P, "ob_dn", 1, "ready_to_execute", "pbd", 1, "did not exist", 6, 6), confirm("pbd"),
    submit(P, "od_dn", 1, "ready_to_execute", "pdd", 1, "did not exist", 6, 6,
           tables=[(rel(P, "od_up"), 1990)]), confirm("pdd"),
    # B first dev round: upstream copied by RegisterClone, then its post-copy SKIP; downstream copied
    register_clone("ob_up", "rbu", rel(P, "ob_up")), confirm("rbu"),
    submit(D, "ob_up", 3, "skip_execution", "sbu", 0, "no-op"),
    submit(D, "ob_dn", 1, "undecided", "spec", 4, "", method="SubmitEnrichedSQLSpeculative"),
    submit(D, "ob_dn", 1, "ready_to_clone", "dbd", 3, CLONE, 1, source=rel(P, "ob_dn")),
    confirm("dbd"),
    # D first dev round: upstream built, downstream copied from prod
    submit(D, "od_up", 3, "ready_to_execute", "dud", 1, "did not exist", 1, 6), confirm("dud"),
    submit(D, "od_dn", 1, "ready_to_clone", "ddd", 3, CLONE, 1, source=rel(P, "od_dn"),
           tables=[(rel(D, "od_up"), 2000)]),
    confirm("ddd"),
    # next unchanged round
    submit(D, "ob_up", 3, "skip_execution", "sbu2", 0, "no-op"),
    submit(D, "ob_dn", 1, "skip_execution", "sbd2", 0, "no-op"),
    submit(D, "od_up", 3, "skip_execution", "sud2", 0, "no-op"),
    submit(D, "od_dn", 1, "skip_execution", "sdd2", 0, "no-op"),
]


def read(step, schema, logical, v):
    return {"step": step, "relation": f"CDO_DEMO_DB.{schema}.{NAMES[logical]}",
            "rows": [{"ID": 1, "V": v}]}


STEPS = {"run": RUN, "names": NAMES,
         "steps": [{"kind": "Build"}, {"kind": "Read"}, {"kind": "Read"}, {"kind": "Read"},
                   {"kind": "Read"}, {"kind": "Read"}, {"kind": "Read"},
                   {"kind": "Reference"}, {"kind": "Read"}, {"kind": "Read"}]}
READS = [read(1, "PROD_STATE_DEMO", "ob_dn", "prod"), read(2, "PROD_STATE_DEMO", "od_dn", "prod"),
         read(3, "DEV_CLONE_DEMO", "ob_dn", "prod"), read(4, "DEV_CLONE_DEMO", "od_dn", "prod"),
         read(5, "DEV_CLONE_DEMO", "ob_dn", "prod"), read(6, "DEV_CLONE_DEMO", "od_dn", "prod"),
         read(8, "DEV_CLONE_DEMO", "ob_dn", "prod"), read(9, "DEV_CLONE_DEMO", "od_dn", "dev")]

passed = 0


def check(name, got, want):
    global passed
    if got != want:
        print(f"FAIL: {name}\n  got  {got!r}\n  want {want!r}")
        sys.exit(1)
    passed += 1
    print(f"PASS: {name}")


rec = uo.record(ROWS, STEPS, READS)
b, d = rec["b"], rec["d"]

check("the dev upstream's origin is a confirmed RegisterClone copy of prod's (B)",
      (b["dev_upstream"]["origin"], b["dev_upstream"]["copy_source"], b["dev_upstream"]["confirmed"]),
      ("copied", rel(P, "ob_up"), True))
check("the dev upstream's origin is an independent confirmed build (D)",
      (d["dev_upstream"]["origin"], d["dev_upstream"]["confirmed"]), ("built", True))
first = d["dev_downstream"][0]
check("the first dev downstream decision is an ordinary submit, with its source and codes",
      (first["method"], first["decision"], first["codes"], first["source"]),
      ("SubmitEnrichedSQL", "CLONE", [1, None], rel(P, "od_dn")))
check("a speculative submit is not the downstream decision",
      [x["method"] for x in b["dev_downstream"]], ["SubmitEnrichedSQL", "SubmitEnrichedSQL"])
check("the copy permissions are taken from the downstream's own submit",
      (first["allow_clones"], first["clone_chain_depth_limit"], first["freshness_tolerance_seconds"]),
      (True, None, 0))
check("the downstream's confirmation is matched by request id",
      (first["confirm"]["success"], first["confirm"]["failed_to_clone"]), (True, False))
check("the next unchanged submit is recorded",
      (d["dev_downstream"][1]["decision"], d["dev_downstream"][1]["confirm"]), ("SKIP", None))
check("no pre-copy of the downstream", (b["downstream_register_clones"], d["downstream_register_clones"]),
      (0, 0))
check("each downstream submit's upstream epoch is set against that upstream's confirmed epochs",
      (d["prod_downstream"][0]["upstream_epochs"], first["upstream_epochs"]),
      ([{"name": rel(P, "od_up"), "submitted": 1990, "confirmed": [2000]}],
       [{"name": rel(D, "od_up"), "submitted": 2000, "confirmed": [2000]}]))
check("served and Reference rows are attributed by phase",
      (d["rows"]["prod"], d["rows"]["served"], d["rows"]["after_next"], d["rows"]["reference"]),
      ([{"ID": 1, "V": "prod"}], [{"ID": 1, "V": "prod"}], [{"ID": 1, "V": "prod"}],
       [{"ID": 1, "V": "dev"}]))
check("rows that differ from the Reference are a wrong-marker finding (D)", d["wrong_marker"], True)
check("rows equal to the Reference are not a finding (B)", b["wrong_marker"], False)
check("cases absent from the capture are not recorded", sorted(rec), ["b", "d"])
text = uo.render(rec)
check("the rendered record names each case and flags the finding",
      ("B" in text and "D" in text and "WRONG MARKER" in text), True)

print(f"\nALL {passed} UPSTREAM ORIGIN TESTS PASSED")
