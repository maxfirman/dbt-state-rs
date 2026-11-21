#!/usr/bin/env python3
"""#200: which data-test history a hosted SKIP reuses.

Two tools for the per-Target data-test history characterization:

`table WINDOW.jsonl [...]`
    One line per data-test `SubmitEnrichedSQL` in a capture: the node, its
    `target_name` and `default_schema`, the decision, codes and description,
    the confirmed runtime of an execution, and for a SKIP the echoed
    `execution_runtime_ms` attributed to the confirmed execution(s) in the
    window with that runtime (same unique id? same SQL?). A hosted SKIP echoes
    the runtime of the execution it reused, so this names whose run each SKIP
    reused.

`probes`
    Crafted one-factor cells, sent with `target/debug/state-probe` through
    `PROBE_URL` (your own recording proxy for a hosted capture, or a local
    server for a rehearsal). Each cell submits a base data test and confirms it
    with a runtime no other confirm uses, then submits a variant that differs
    in exactly one factor (`FACTORS`). When the variant executes, it is
    confirmed with its own runtime and the base is submitted again: a SKIP that
    echoes the base runtime means the factor separates two histories; a rebuild
    means the variant's run replaced the base's. Cells in `REVISIT` then submit
    the variant again. Every SKIP is explained. No warehouse is touched: the
    requests name fresh, never-created per-run relations in CDO_DEMO_DB demo
    schemas with a made-up epoch, in the shape client 2.0.11 sends for a
    singular data test.

`probes-order`
    Two crafted same-key cells: two unique ids with one SQL are both submitted
    before either confirms (as when one invocation runs both), confirmed in
    the reverse order with one failing result, then each submitted again. The
    echoes say whether a SKIP reuses the latest confirm, the latest submit or
    the unique id's own run (`ORDER_CELLS`).

Self-test: python3 experiments/data_test_history_test.py
"""
import copy
import json
import os
import re
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DB = "CDO_DEMO_DB"
DEMO_SCHEMAS = {"PROD_STATE_DEMO", "DEV_CLONE_DEMO"}
NAMESPACE = "fc38a5ba0eceb701c6ab7c6b882e28b0"  # the demo connection namespace
PROJECT_ID = "70437463667433"  # jaffle-shop's dbt Cloud project
DATA_TEST = 8
SUBMIT_BUDGET = 40

# --- echo attribution ------------------------------------------------------------------


def normalized_sql(sql):
    """SQL with surrounding whitespace trimmed and inner runs collapsed."""
    return " ".join((sql or "").split())


def _variant(row):
    resp = (row.get("response") or {}).get("response") or {}
    if len(resp) != 1:
        return None, {}
    ((name, body),) = resp.items()
    return name, body or {}


def _failures(results):
    kind = (((results or {}).get("fields") or {}).get("failures") or {}).get("kind") or {}
    return kind.get("int_value", kind.get("double_value"))


DECISIONS = {0: "SKIP", 1: "EXECUTE", 3: "CLONE"}


def data_test_rows(rows):
    """One dict per data-test SubmitEnrichedSQL (see the module doc)."""
    out, by_rid = [], {}
    for i, r in enumerate(rows):
        req = r.get("request") or {}
        if r.get("method") == "ConfirmExecution":
            row = by_rid.get(req.get("request_id"))
            if row is not None and (r.get("response") or {}).get("success") is True:
                row["confirmed_runtime"] = req.get("execution_runtime_ms")
                row["confirmed_failures"] = _failures(req.get("execution_results"))
                row["confirm_entry"] = i
            continue
        if r.get("method") != "SubmitEnrichedSQL" or req.get("execution_type") != DATA_TEST:
            continue
        variant, body = _variant(r)
        ed = body.get("explained_decision") or {}
        state = req.get("dbt_node_state") or {}
        row = {
            "entry": i,
            "uid": state.get("node_unique_id") or (req.get("labels") or {}).get("dbt_node_unique_id"),
            "node": (req.get("labels") or {}).get("dbt_node_name"),
            "target_name": state.get("target_name"),
            "default_schema": req.get("default_schema"),
            "project_id": state.get("project_id"),
            "table_namespace": req.get("table_namespace"),
            "sql": req.get("sql"),
            "variant": variant,
            "decision": DECISIONS.get(ed.get("decision"), ed.get("decision")),
            "srr": ed.get("skip_rejection_reason"),
            "crr": ed.get("clone_rejection_reason"),
            "description": ed.get("decision_description"),
            "edid": body.get("execution_decision_id"),
            "echoed_runtime": body.get("execution_runtime_ms") if variant == "skip_execution" else None,
            "failures": _failures(body.get("execution_results")) if variant == "skip_execution" else None,
            "confirmed_runtime": None,
            "confirmed_failures": None,
            "confirm_entry": None,
            "matches": [],
            "own_latest": None,
        }
        if variant == "skip_execution":
            done = [x for x in out if x["confirm_entry"] is not None and x["confirm_entry"] < i]
            row["matches"] = [
                {"entry": x["entry"], "uid": x["uid"], "target_name": x["target_name"],
                 "same_uid": x["uid"] == row["uid"],
                 "same_sql": normalized_sql(x["sql"]) == normalized_sql(row["sql"])}
                for x in done if x["confirmed_runtime"] == row["echoed_runtime"]]
            own = [x for x in done if x["uid"] == row["uid"]]
            if own:
                last = max(own, key=lambda x: x["confirm_entry"])
                row["own_latest"] = {"entry": last["entry"], "runtime": last["confirmed_runtime"]}
        if variant == "ready_to_execute" and body.get("request_id"):
            by_rid[body["request_id"]] = row
        out.append(row)
    return out


def describe(row):
    if row["variant"] == "skip_execution":
        if not row["matches"]:
            who = "no confirmed run in the window"
        else:
            who = "; ".join(
                f"{'own' if m['same_uid'] else m['uid']} run at {m['entry']} ({m['target_name']}, "
                f"{'same' if m['same_sql'] else 'other'} SQL)" for m in row["matches"])
        return f"echo {row['echoed_runtime']} failures {row['failures']} <- {who}"
    if row["confirmed_runtime"] is not None:
        return f"confirmed {row['confirmed_runtime']} failures {row['confirmed_failures']} at {row['confirm_entry']}"
    return "not confirmed"


def print_table(rows):
    for r in data_test_rows(rows):
        print(f"{r['entry']:4d} {r['node']} [{r['target_name']}/{r['default_schema']}] "
              f"{r['decision']} {r['srr']}/{r['crr']} {r['description']!r} — {describe(r)}")


# --- crafted one-factor probes -------------------------------------------------------------


def fqn(schema, name):
    return f'"{DB}"."{schema.upper()}"."{name.upper()}"'


def _set(path):
    def mutate(req, value):
        obj = req
        *head, last = path.split(".")
        for k in head:
            obj = obj[k]
        obj[last] = value
    return mutate


def _uid(req, run, cell):
    test = f"dth_{run}_{cell}_t2"
    uid = f"test.jaffle_shop.{test}"
    req["labels"] = {"dbt_node_unique_id": uid, "dbt_node_fqn": f"jaffle_shop.{test}",
                     "dbt_node_name": test}
    req["dbt_node_state"]["node_unique_id"] = uid


def _hashes(req, run, cell):
    st = req["dbt_node_state"]
    st["node_hash"] = _h(f"n2{run}{cell}")
    st["node_body_hash"] = _h(f"b2{run}{cell}")
    st["node_configs_hash"] = _h(f"c2{run}{cell}")


def _dev_relation(req, run, cell, schema="dev_clone_demo"):
    src = _source(run, cell)
    req["sql"] = f"select id, v from {DB}.{schema}.{src} where id < 0"
    req["tables"] = [{"name": fqn(schema, src), "last_modified_epoch": req["tables"][0]["last_modified_epoch"]}]


def _dev_target(req, run, cell):
    _dev_relation(req, run, cell)
    req["default_schema"] = "dev_clone_demo"
    req["dbt_node_state"]["target_name"] = "dev_clone"


FACTORS = {
    "target_name": {
        "what": "dbt_node_state.target_name prod_demo -> dev_clone",
        "paths": {"dbt_node_state.target_name"},
        "apply": lambda req, run, cell: _set("dbt_node_state.target_name")(req, "dev_clone")},
    "default_schema": {
        "what": "default_schema prod_state_demo -> dev_clone_demo (SQL fully qualified)",
        "paths": {"default_schema"},
        "apply": lambda req, run, cell: _set("default_schema")(req, "dev_clone_demo")},
    "unique_id": {
        "what": "another test unique id (labels and node_unique_id), same SQL and hashes",
        "paths": {"labels.dbt_node_unique_id", "labels.dbt_node_fqn", "labels.dbt_node_name",
                  "dbt_node_state.node_unique_id"},
        "apply": _uid},
    "project_id": {
        "what": "dbt_node_state.project_id -> a project id no project uses",
        "paths": {"dbt_node_state.project_id"},
        "apply": lambda req, run, cell: _set("dbt_node_state.project_id")(req, "99999999999999")},
    "table_namespace": {
        "what": "table_namespace -> another 32-hex namespace",
        "paths": {"table_namespace"},
        "apply": lambda req, run, cell: _set("table_namespace")(req, _h(f"ns{run}"))},
    "evaluation": {
        "what": "tolerate_nondeterminism true -> false (evaluation requested), deterministic SQL",
        "paths": {"tolerate_nondeterminism"},
        "apply": lambda req, run, cell: _set("tolerate_nondeterminism")(req, False)},
    "severity": {
        "what": "semantic_extras.severity ERROR -> WARN",
        "paths": {"semantic_extras.severity"},
        "apply": lambda req, run, cell: _set("semantic_extras.severity")(req, '"WARN"')},
    "node_hashes": {
        "what": "client node_hash, node_body_hash and node_configs_hash",
        "paths": {"dbt_node_state.node_hash", "dbt_node_state.node_body_hash",
                  "dbt_node_state.node_configs_hash"},
        "apply": _hashes},
    "sql_whitespace": {
        "what": "a leading newline and trailing spaces in the SQL",
        "paths": {"sql"},
        "apply": lambda req, run, cell: _set("sql")(req, "\n" + req["sql"] + "  ")},
    "input_relation": {
        "what": "the SQL reads the dev relation of the same name (a failing run), same "
                "target_name and default_schema",
        "paths": {"sql", "tables"},
        "apply": _dev_relation},
    "dev_target": {
        "what": "the dev Target's SQL, relation, default_schema and target_name (a dev "
                "relation no model built or copied), same client hashes",
        "paths": {"sql", "tables", "default_schema", "dbt_node_state.target_name"},
        "apply": _dev_target},
    "dev_target_unrendered": {
        "what": "compare_unrendered_code on both: the dev Target's SQL, relation, "
                "default_schema and target_name, same client hashes",
        "paths": {"sql", "tables", "default_schema", "dbt_node_state.target_name"},
        "apply": _dev_target},
}
# Cells whose variant is submitted once more after the base's resubmit.
REVISIT = {"unique_id", "input_relation", "target_name", "default_schema"}
# Variant confirms that report a failing test (the base always passes).
FAILING = {"input_relation"}


def _h(s):
    import hashlib

    return hashlib.md5(s.encode()).hexdigest()


def _source(run, cell):
    return f"dth_{run}_{cell}_s"


def base_request(run, cell, epoch):
    test, src = f"dth_{run}_{cell}_t", _source(run, cell)
    uid = f"test.jaffle_shop.{test}"
    return {
        "target_table": None,
        "dialect": "snowflake",
        "default_catalog": DB,
        "execution_type": DATA_TEST,
        "sql": f"select id, v from {DB}.prod_state_demo.{src} where id < 0",
        "tables": [{"name": fqn("prod_state_demo", src), "last_modified_epoch": epoch}],
        "query_dependencies": [],
        "semantic_extras": {"fail_calc": '"count(*)"', "severity": '"ERROR"',
                            "error_if": '"!= 0"', "warn_if": '"!= 0"'},
        "freshness_tolerance_seconds": 0,
        "lenient_dependencies": [],
        "tolerate_nondeterminism": True,
        "labels": {"dbt_node_unique_id": uid, "dbt_node_fqn": f"jaffle_shop.{test}",
                   "dbt_node_name": test},
        "clone_time_travel_limit": None,
        "clone_table_properties": None,
        "stale_upstream_policy": 0,
        "clone_chain_depth_limit": None,
        "dbt_node_state": {
            "node_unique_id": uid, "target_name": "prod_demo", "project_name": "jaffle_shop",
            "resource_type": "test", "node_hash": _h(f"n{run}{cell}"),
            "node_body_hash": _h(f"b{run}{cell}"), "node_configs_hash": None,
            "node_persisted_descriptions_hash": None, "node_macros_hash": None,
            "node_contract_hash": None, "profile_name": "snowflake", "project_id": PROJECT_ID,
            "node_database_representation": None,
        },
        "default_schema": "prod_state_demo",
        "compare_unrendered_code": cell == "dev_target_unrendered",
        "table_namespace": NAMESPACE,
        "ignore_external_modifications": False,
        "allow_clones": True,
        "is_defer_to_profile": False,
        "defer_enabled": True,
    }


def plan(run, epoch):
    cells = []
    for k, factor in enumerate(FACTORS):
        base = base_request(run, factor, epoch)
        var = copy.deepcopy(base)
        FACTORS[factor]["apply"](var, run, factor)
        cells.append({"cell": factor, "factor": factor, "what": FACTORS[factor]["what"],
                      "base": base, "variant": var,
                      "runtimes": {"base": 41_000 + 100 * k + 11, "variant": 41_000 + 100 * k + 22}})
    return cells


def max_submits(cells):
    return sum(4 if c["cell"] in REVISIT else 3 for c in cells)


# Same-key order cells: which of two executions with one key a SKIP reuses.
ORDER_CELLS = {
    # cell -> which of the pair fails; the pair is always confirmed second-first.
    "fail_confirmed_last": {"first": True, "second": False},
    "fail_submitted_last": {"first": False, "second": True},
}


def plan_order(run, epoch):
    cells = []
    for k, (cell, failing) in enumerate(ORDER_CELLS.items()):
        first = base_request(run, cell, epoch)
        second = copy.deepcopy(first)
        _uid(second, run, cell)
        cells.append({"cell": cell, "first": first, "second": second,
                      "confirm_order": ["second", "first"], "failing": dict(failing),
                      "runtimes": {"first": 43_000 + 100 * k + 11,
                                   "second": 43_000 + 100 * k + 22}})
    return cells


def order_submits(cells):
    return 4 * len(cells)


def diff_paths(a, b, prefix=""):
    if isinstance(a, dict) and isinstance(b, dict):
        out = set()
        for k in set(a) | set(b):
            out |= diff_paths(a.get(k), b.get(k), f"{prefix}{k}.")
        return out
    return set() if a == b else {prefix.rstrip(".")}


_REL = re.compile(r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)(?:\.(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)){2}')


def sql_relations(sql):
    return _REL.findall(sql)


def demo_relation(name, run):
    parts = re.findall(r'"([^"]+)"|([A-Za-z_][A-Za-z0-9_$]*)', name)
    parts = [q or u.upper() for q, u in parts]
    return (len(parts) == 3 and parts[0].upper() == DB and parts[1].upper() in DEMO_SCHEMAS
            and run.upper() in parts[2].upper())


def _results(failing):
    fields = {
        "failures": {"kind": {"int_value": 1 if failing else 0}},
        "should_warn": {"kind": {"bool_value": False}},
        "should_error": {"kind": {"bool_value": bool(failing)}},
    }
    return {"fields": fields}


class _Runner:
    """Sends the crafted calls through `PROBE_URL` and prints one JSON line per call."""

    def __init__(self, out=print):
        import lag_matrix as lm

        self.lm, self.out = lm, out

    def emit(self, **row):
        self.out(json.dumps(row))

    def submit(self, cell, step, req):
        v, b = self.lm.submit_request(req)
        ed = b.get("explained_decision") or {}
        row = {"cell": cell, "step": step, "variant": v, "decision": ed.get("decision"),
               "srr": ed.get("skip_rejection_reason"), "crr": ed.get("clone_rejection_reason"),
               "description": ed.get("decision_description"),
               "echoed_runtime": b.get("execution_runtime_ms"),
               "echoed_failures": _failures(b.get("execution_results"))}
        if v == "skip_execution" and b.get("execution_decision_id"):
            time.sleep(1)
            row["explain"] = self.lm.probe(
                "explain", {"execution_decision_ids": [b["execution_decision_id"]]})
        self.emit(**row)
        return v, b

    def confirm(self, cell, step, req, body, runtime, failing=False):
        c = self.lm.probe("confirm", {
            "request_id": body["request_id"], "last_modified_epoch": None, "failed_to_clone": False,
            "table_type": None, "execution_results": _results(failing),
            "execution_runtime_ms": runtime, "labels": req["labels"]})
        self.emit(cell=cell, step=f"{step} confirm", runtime=runtime, failing=failing, confirm=c)
        time.sleep(2)


def run_cells(cells, out=print):
    r = _Runner(out)
    for c in cells:
        cell, rt = c["cell"], c["runtimes"]
        r.emit(cell=cell, what=c["what"])
        v, b = r.submit(cell, "base", c["base"])
        if v != "ready_to_execute":
            r.emit(cell=cell, error=f"base first submit answered {v}; cell abandoned")
            continue
        r.confirm(cell, "base", c["base"], b, rt["base"])
        v, b = r.submit(cell, "variant", c["variant"])
        if v != "ready_to_execute":
            continue
        r.confirm(cell, "variant", c["variant"], b, rt["variant"], failing=cell in FAILING)
        v, b = r.submit(cell, "base again", c["base"])
        if v == "ready_to_execute":
            r.confirm(cell, "base again", c["base"], b, rt["base"] + 1)
        if cell in REVISIT:
            v, b = r.submit(cell, "variant again", c["variant"])
            if v == "ready_to_execute":
                r.confirm(cell, "variant again", c["variant"], b, rt["variant"] + 1,
                          failing=cell in FAILING)


def run_order(cells, out=print):
    r = _Runner(out)
    for c in cells:
        cell = c["cell"]
        r.emit(cell=cell, failing=c["failing"], runtimes=c["runtimes"],
               confirm_order=c["confirm_order"])
        bodies = {}
        for role in ("first", "second"):
            v, bodies[role] = r.submit(cell, role, c[role])
            if v != "ready_to_execute":
                r.emit(cell=cell, error=f"{role} answered {v}; cell abandoned")
                break
        else:
            for role in c["confirm_order"]:
                r.confirm(cell, role, c[role], bodies[role], c["runtimes"][role],
                          failing=c["failing"][role])
            for role in ("first", "second"):
                v, b = r.submit(cell, f"{role} again", c[role])
                if v == "ready_to_execute":
                    r.confirm(cell, f"{role} again", c[role], b, c["runtimes"][role] + 1,
                              failing=c["failing"][role])


def main(argv):
    if argv[:1] == ["table"] and len(argv) > 1:
        for path in argv[1:]:
            with open(path) as fh:
                print_table([json.loads(line) for line in fh if line.strip()])
        return 0
    if argv[:1] in (["probes"], ["probes-order"]):
        if not os.environ.get("PROBE_URL"):
            print("REFUSED: set PROBE_URL to your own recording proxy (or a local server)")
            return 2
        import lag_matrix as lm

        run = time.strftime("%H%M%S") + uuid.uuid4().hex[:6]
        lm.RUN = run
        epoch = int(time.time() * 1000) - 3_600_000
        if argv[0] == "probes-order":
            cells = plan_order(run, epoch)
            print(json.dumps({"run": run, "cells": len(cells), "max_submits": order_submits(cells)}))
            run_order(cells)
            return 0
        cells = plan(run, epoch)
        print(json.dumps({"run": run, "cells": len(cells), "max_submits": max_submits(cells)}))
        run_cells(cells)
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
