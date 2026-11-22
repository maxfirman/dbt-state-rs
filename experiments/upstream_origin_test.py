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
PROD_SCHEMA, DEV_SCHEMA = '"CDO_DEMO_DB"."PROD_STATE_DEMO"', '"CDO_DEMO_DB"."DEV_CLONE_DEMO"'


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
            "request": {"target_table": rel(DEV_SCHEMA, logical), "execution_type": 3,
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
    submit(PROD_SCHEMA, "ob_up", 3, "ready_to_execute", "pbu", 1, "did not exist", 6, 6), confirm("pbu"),
    submit(PROD_SCHEMA, "od_up", 3, "ready_to_execute", "pdu", 1, "did not exist", 6, 6), confirm("pdu"),
    submit(PROD_SCHEMA, "ob_dn", 1, "ready_to_execute", "pbd", 1, "did not exist", 6, 6), confirm("pbd"),
    submit(PROD_SCHEMA, "od_dn", 1, "ready_to_execute", "pdd", 1, "did not exist", 6, 6,
           tables=[(rel(PROD_SCHEMA, "od_up"), 1990)]), confirm("pdd"),
    # B first dev round: upstream copied by RegisterClone, then its post-copy SKIP; downstream copied
    register_clone("ob_up", "rbu", rel(PROD_SCHEMA, "ob_up")), confirm("rbu"),
    submit(DEV_SCHEMA, "ob_up", 3, "skip_execution", "sbu", 0, "no-op"),
    submit(DEV_SCHEMA, "ob_dn", 1, "undecided", "spec", 4, "", method="SubmitEnrichedSQLSpeculative"),
    submit(DEV_SCHEMA, "ob_dn", 1, "ready_to_clone", "dbd", 3, CLONE, 1, source=rel(PROD_SCHEMA, "ob_dn")),
    confirm("dbd"),
    # D first dev round: upstream built, downstream copied from prod
    submit(DEV_SCHEMA, "od_up", 3, "ready_to_execute", "dud", 1, "did not exist", 1, 6), confirm("dud"),
    submit(DEV_SCHEMA, "od_dn", 1, "ready_to_clone", "ddd", 3, CLONE, 1, source=rel(PROD_SCHEMA, "od_dn"),
           tables=[(rel(DEV_SCHEMA, "od_up"), 2000)]),
    confirm("ddd"),
    # next unchanged round
    submit(DEV_SCHEMA, "ob_up", 3, "skip_execution", "sbu2", 0, "no-op"),
    submit(DEV_SCHEMA, "ob_dn", 1, "skip_execution", "sbd2", 0, "no-op"),
    submit(DEV_SCHEMA, "od_up", 3, "skip_execution", "sud2", 0, "no-op"),
    submit(DEV_SCHEMA, "od_dn", 1, "skip_execution", "sdd2", 0, "no-op"),
]


def read(step, schema, logical, v):
    return {"step": step, "relation": f"CDO_DEMO_DB.{schema}.{NAMES[logical]}",
            "rows": [{"ID": 1, "V": v}]}


def build(target, *logical):
    return {"kind": "Build", "select": [NAMES[n] for n in logical], "target": target}


def read_step(schema, logical, target="dev_clone"):
    return {"kind": "Read", "relation": f"CDO_DEMO_DB.{schema}.{NAMES[logical]}", "target": target}


# The planned steps (steps.json): a read's phase comes from the step it was planned at.
STEPS = {"run": RUN, "names": NAMES, "steps": [
    build("prod_demo", "ob_up", "od_up"),                         # 0
    build("prod_demo", "ob_dn", "od_dn"),                         # 1
    read_step("PROD_STATE_DEMO", "ob_dn", "prod_demo"),           # 2
    read_step("PROD_STATE_DEMO", "od_dn", "prod_demo"),           # 3
    build("dev_clone", "ob_up", "ob_dn"),                         # 4 first dev round, B
    read_step("DEV_CLONE_DEMO", "ob_up"),                         # 5
    read_step("DEV_CLONE_DEMO", "ob_dn"),                         # 6
    build("dev_noclone", "od_up"),                                # 7 D's upstream without copy permission
    build("dev_clone", "od_up", "od_dn"),                         # 8 first dev round, D
    read_step("DEV_CLONE_DEMO", "od_dn"),                         # 9
    build("dev_clone", "ob_up", "ob_dn"),                         # 10 next unchanged round
    read_step("DEV_CLONE_DEMO", "ob_dn"),                         # 11
    build("dev_clone", "od_up", "od_dn"),                         # 12
    read_step("DEV_CLONE_DEMO", "od_dn"),                         # 13
    {"kind": "Reference", "select": [NAMES[n] for n in ("ob_up", "ob_dn", "od_up", "od_dn")],
     "target": "dev_clone"},                                      # 14
    read_step("DEV_CLONE_DEMO", "ob_dn"),                         # 15
    read_step("DEV_CLONE_DEMO", "od_dn"),                         # 16
]}
READS = [read(2, "PROD_STATE_DEMO", "ob_dn", "prod"), read(3, "PROD_STATE_DEMO", "od_dn", "prod"),
         read(5, "DEV_CLONE_DEMO", "ob_up", "prod"), read(6, "DEV_CLONE_DEMO", "ob_dn", "prod"),
         read(9, "DEV_CLONE_DEMO", "od_dn", "prod"), read(11, "DEV_CLONE_DEMO", "ob_dn", "prod"),
         read(13, "DEV_CLONE_DEMO", "od_dn", "prod"), read(15, "DEV_CLONE_DEMO", "ob_dn", "prod"),
         read(16, "DEV_CLONE_DEMO", "od_dn", "dev")]

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
      ("copied", rel(PROD_SCHEMA, "ob_up"), True))
check("the dev upstream's origin is an independent confirmed build (D)",
      (d["dev_upstream"]["origin"], d["dev_upstream"]["confirmed"]), ("built", True))
first = d["dev_downstream"][0]
check("the first dev downstream decision is an ordinary submit, with its source and codes",
      (first["method"], first["decision"], first["codes"], first["source"]),
      ("SubmitEnrichedSQL", "CLONE", [1, None], rel(PROD_SCHEMA, "od_dn")))
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
      ([{"name": rel(PROD_SCHEMA, "od_up"), "submitted": 1990, "confirmed": [2000]}],
       [{"name": rel(DEV_SCHEMA, "od_up"), "submitted": 2000, "confirmed": [2000]}]))
check("served and Reference rows are attributed by phase",
      (d["rows"]["prod"], d["rows"]["served"], d["rows"]["after_next"], d["rows"]["reference"]),
      ([{"ID": 1, "V": "prod"}], [{"ID": 1, "V": "prod"}], [{"ID": 1, "V": "prod"}],
       [{"ID": 1, "V": "dev"}]))
check("rows that differ from the Reference are a wrong-marker finding (D)", d["wrong_marker"], True)
check("rows equal to the Reference are not a finding (B)", b["wrong_marker"], False)
check("each planned read of a case is listed with its run step and phase",
      [(x["phase"], x["step"], x["relation"].split(".")[1]) for x in d["reads"]],
      [("prod", 3, "PROD_STATE_DEMO"), ("served", 9, "DEV_CLONE_DEMO"),
       ("after_next", 13, "DEV_CLONE_DEMO"), ("reference", 16, "DEV_CLONE_DEMO")])
check("no measurement is missing in a complete capture", (b["missing"], d["missing"]), ([], []))
check("cases absent from the capture are not recorded", sorted(rec), ["b", "d"])

# The origin follows the confirmed outcome: a granted copy that the client
# confirms as a fallback build (failed_to_clone) is a build, not a copy.
fell_back = [confirm("rbu", failed=True) if r.get("method") == "ConfirmExecution"
             and r["request"]["request_id"] == "rbu" else r for r in ROWS]
fb = uo.record(fell_back, STEPS, READS)["b"]["dev_upstream"]
check("a copy confirmed as a client fallback is a build, with the granted source kept apart",
      (fb["origin"], fb["copy_source"], fb["granted_source"]),
      ("built after failed copy", None, rel(PROD_SCHEMA, "ob_up")))
unconfirmed = [r for r in ROWS if not (r.get("method") == "ConfirmExecution"
                                       and r["request"]["request_id"] == "rbu")]
check("a copy grant without an accepted confirmation is unconfirmed",
      uo.record(unconfirmed, STEPS, READS)["b"]["dev_upstream"]["origin"], "unconfirmed copy")

# A missing measurement is inconclusive, never a verdict.
no_ref = uo.record(ROWS, STEPS, [x for x in READS if x["step"] != 16])["d"]
check("without the Reference read, the finding is inconclusive",
      (no_ref["wrong_marker"], no_ref["missing"], no_ref["rows"]["reference"]),
      (None, ["reference"], None))
wrong_relation = [dict(rd, relation=rd["relation"].replace(NAMES["od_dn"], NAMES["ob_dn"]))
                  if rd["step"] == 16 else rd for rd in READS]
mislabelled = uo.record(ROWS, STEPS, wrong_relation)["d"]
check("a read of a different relation at the Reference step is inconclusive",
      (mislabelled["wrong_marker"], mislabelled["missing"]), (None, ["reference"]))
no_first = uo.record(ROWS, STEPS, [x for x in READS if x["step"] != 9])["d"]
check("without the first dev read, the repeat read keeps its own phase",
      (no_first["rows"]["served"], no_first["rows"]["after_next"], no_first["wrong_marker"],
       no_first["missing"]),
      (None, [{"ID": 1, "V": "prod"}], None, ["served"]))
check("an inconclusive case is rendered as such",
      "INCONCLUSIVE (missing: reference)" in uo.render({"d": no_ref}), True)
text = uo.render(rec)
check("the rendered record names each case and flags the finding",
      ("B" in text and "D" in text and "WRONG MARKER" in text), True)

# --- evidence extract: one credential-free document per case, across runs ---
with_meta = [dict(r, metadata={"x-dbt-invocation-id": "i"}, response_metadata={"k": "v"})
             for r in ROWS]
docs = uo.extract([{"label": "hosted run 2", "service": "hosted", "fixture": "f.jsonl",
                    "rows": with_meta, "steps": STEPS, "reads": READS},
                   {"label": "rust rehearsal", "service": "dbt-state-server", "fixture": None,
                    "rows": with_meta, "steps": STEPS, "reads": READS}])
check("the extract has one document per recorded case", sorted(docs), ["B", "D"])
run = docs["D"]["runs"][0]
check("each run names its label, service, run id and fixture",
      (run["label"], run["service"], run["run"], run["fixture"]),
      ("hosted run 2", "hosted", RUN, "f.jsonl"))
check("each run lists every planned read with its phase, relation and rows",
      [(x["phase"], x["relation"], x["rows"]) for x in run["reads"]][-1],
      ("reference", f"CDO_DEMO_DB.DEV_CLONE_DEMO.{NAMES['od_dn']}", [{"ID": 1, "V": "dev"}]))
check("each run keeps the case's RPCs (decisions and their confirmations) in capture order",
      [(r["method"], r["request"].get("request_id") or r["request"]["labels"]["dbt_node_name"])
       for r in run["rpcs"]][:3],
      [("SubmitEnrichedSQL", NAMES["od_up"]), ("ConfirmExecution", "pdu"),
       ("SubmitEnrichedSQL", NAMES["od_dn"])])
check("the extract drops call metadata",
      any("metadata" in r or "response_metadata" in r for r in run["rpcs"]), False)
check("each run carries the verdict for the case",
      [(x["label"], x["wrong_marker"]) for x in docs["D"]["runs"]],
      [("hosted run 2", True), ("rust rehearsal", True)])

print(f"\nALL {passed} UPSTREAM ORIGIN TESTS PASSED")
