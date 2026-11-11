#!/usr/bin/env python3
"""Warehouse-free tests for the Explain tree census (#70). Builds synthetic
captures in temp files and also loads the tracked fixtures. No warehouse, no
proxy. Exit 0 on success.

Run: python3 experiments/explain_census_test.py
"""
import json
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import explain_census as ec  # noqa: E402

passed = 0


def ok(name):
    global passed
    passed += 1
    print(f"PASS: {name}")


def check(name, cond, detail=""):
    if not cond:
        print(f"FAIL: {name}{' — ' + detail if detail else ''}")
        sys.exit(1)
    ok(name)


def submit(edid, execution_type, variant="ready_to_execute"):
    return {
        "service": "SQL", "method": "SubmitEnrichedSQL",
        "request": {"execution_type": execution_type, "labels": {"dbt_node_name": "n"}},
        "response": {"response": {variant: {"execution_decision_id": edid}}},
    }


def values(edid):
    return {
        "service": "SQL", "method": "SubmitValues",
        "request": {"labels": {"dbt_node_name": "s"}},
        "response": {"response": {"skip_execution": {"execution_decision_id": edid}}},
    }


def register_clone(edid, execution_type):
    return {
        "service": "Clone", "method": "RegisterClone",
        "request": {"execution_type": execution_type},
        "response": {"ready_to_clone": {"execution_decision_id": edid}, "response": None},
    }


def speculative(edid, execution_type):
    return {
        "service": "SQL", "method": "SubmitEnrichedSQLSpeculative",
        "request": {"execution_type": execution_type, "labels": {"dbt_node_name": "t"}},
        "response": {"response": {"skip_execution": {"execution_decision_id": edid}}},
    }


def line(text, marker=None, badge=None, children=None):
    return {"text": text, "marker": marker, "badge": badge, "children": children or []}


def explain(edid, decision, lines):
    return {
        "service": "Explain", "method": "GetExplainMessages",
        "request": {"execution_decision_ids": [edid]},
        "response": {"messages": [{
            "execution_decision_id": edid, "decision": decision,
            "decision_description": "", "explain_lines": lines,
        }]},
    }


tmpdir = tempfile.mkdtemp(prefix="explain-census-test-")


def write(name, rows):
    path = os.path.join(tmpdir, name)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return path


# --- INFO marker detected and named by fixture and entry ---------------------
info = write("info.jsonl", [
    submit("e-snap", 7),
    explain("e-snap", 1, [line("table analysis", children=[
        line("something informative", marker=3),
        line("the model table exists already", marker=1, badge=8),
    ])]),
])
c = ec.census([info])
cell = c.cells.get(("SNAPSHOT", "EXECUTE"))
check("INFO: tree counted in the SNAPSHOT × EXECUTE cell", cell is not None and cell.trees == 1)
check("INFO: nested INFO marker counted", cell.markers.get("INFO") == 1, repr(cell.markers))
check("INFO: lines without a marker counted as none", cell.markers.get(ec.NONE) == 1, repr(cell.markers))
check("INFO: finding names the fixture and entry index",
      [(os.path.basename(f.fixture), f.entry, f.field, f.value) for f in c.findings] == [("info.jsonl", 1, "marker", "INFO")],
      repr(c.findings))

# --- unspecified badge: explicit 0 is a finding, null is not ----------------
unspec = write("unspec.jsonl", [
    submit("e-full", 1, "skip_execution"),
    explain("e-full", 0, [
        line("query analysis", children=[line("the model query has not changed", marker=1, badge=0)]),
        line("data freshness analysis", marker=0),
    ]),
])
c = ec.census([unspec])
cell = c.cells[("FULL", "SKIP")]
check("UNSPECIFIED: explicit badge 0 counted as EB_UNSPECIFIED", cell.badges.get("EB_UNSPECIFIED") == 1, repr(cell.badges))
check("UNSPECIFIED: null badges counted as none, not unspecified", cell.badges.get(ec.NONE) == 2, repr(cell.badges))
check("UNSPECIFIED: explicit marker 0 counted as EM_UNSPECIFIED", cell.markers.get("EM_UNSPECIFIED") == 1, repr(cell.markers))
check("UNSPECIFIED: both explicit zeros are findings",
      sorted((f.field, f.value) for f in c.findings) == [("badge", "EB_UNSPECIFIED"), ("marker", "EM_UNSPECIFIED")],
      repr(c.findings))

# --- decision mapped to node kind through the submit that minted its id ------
minted = write("minted.jsonl", [
    submit("e-view", 10, "skip_execution"),
    values("e-seed"),
    register_clone("e-copy", 3),
    speculative("e-spec", 8),
    explain("e-spec", 0, [line("query analysis")]),
    explain("e-seed", 0, [line("table analysis")]),
    explain("e-view", 0, [line("query analysis")]),
    explain("e-copy", 3, [line("table analysis")]),
    explain("e-orphan", 1, [line("table analysis")]),
    explain("e-other-file", 0, []),
])
other = write("other.jsonl", [submit("e-other-file", 2, "skip_execution")])
c = ec.census([minted, other])
check("MINT: SubmitValues mints a VALUES (seed) decision", c.cells[("VALUES", "SKIP")].trees == 1)
check("MINT: SubmitEnrichedSQL mints its request execution_type", c.cells[("VIEW", "SKIP")].trees == 1)
check("MINT: RegisterClone mints its request execution_type", c.cells[("MERGE", "CLONE")].trees == 1)
check("MINT: a speculative SKIP mints its request execution_type (#103)",
      c.cells.get(("DBT_DATA_TEST", "SKIP")) is not None and c.cells[("DBT_DATA_TEST", "SKIP")].trees == 1)
check("MINT: an id minted in another input file still maps", c.cells[("APPEND", "SKIP")].trees == 1)
check("MINT: an empty tree is counted as empty", c.cells[("APPEND", "SKIP")].empty == 1)
check("MINT: an id no submit minted lands in the unmapped cell", c.cells[(ec.UNMAPPED, "EXECUTE")].trees == 1)
check("MINT: the unmapped tree is a finding",
      [(os.path.basename(f.fixture), f.entry, f.field) for f in c.findings] == [("minted.jsonl", 8, "node_kind")],
      repr(c.findings))
check("MINT: total tree count", c.trees == 6, str(c.trees))

# --- origin: real client (by version) vs crafted state-probe ----------------
CLIENT_META = [["x-request-id", "r"], ["x-system-user-id", ""], ["x-os-name", "linux"]]
PROBE_META = [["x-request-id", "r"], ["x-os-name", "linux"]]


def with_meta(row, meta):
    return dict(row, metadata=meta)


origin = write("origin.jsonl", [
    with_meta(submit("e-probe", 1), PROBE_META),
    with_meta(submit("e-early", 1), CLIENT_META),
    {"service": "ClientValidation", "method": "ValidateClientVersion",
     "metadata": CLIENT_META, "request": {"dbt_run_cache_version": "2.0.11"}, "response": {}},
    with_meta(submit("e-client", 1), CLIENT_META),
    explain("e-probe", 1, [line("table analysis")]),
    explain("e-early", 1, [line("table analysis")]),
    explain("e-client", 1, [line("table analysis")]),
])
c = ec.census([origin])
check("ORIGIN: trees counted by the origin of their minting submit",
      dict(c.cells[("FULL", "EXECUTE")].origins) == {
          ec.CRAFTED: 1, "client (version unrecorded)": 1, "client 2.0.11": 1},
      repr(c.cells[("FULL", "EXECUTE")].origins))
check("ORIGIN: report prints the origin split", "client 2.0.11=1" in ec.render(c), ec.render(c))

# --- extra capture directories are read recursively -------------------------
paths = ec.input_paths([tmpdir], include_fixtures=False)
check("PATHS: a directory argument yields its .jsonl files",
      sorted(os.path.basename(p) for p in paths) == ["info.jsonl", "minted.jsonl", "origin.jsonl", "other.jsonl", "unspec.jsonl"],
      repr(paths))
check("PATHS: tracked fixtures are included by default",
      any(p.endswith("explain_happy_history.jsonl") for p in ec.input_paths([])))

# --- the report names every finding -----------------------------------------
report = ec.render(ec.census([info, unspec]))
check("REPORT: INFO finding is printed with its fixture and entry", "info.jsonl entry 1" in report, report)
check("REPORT: unspecified finding is printed", "EB_UNSPECIFIED" in report and "unspec.jsonl entry 1" in report, report)

# --- tracked corpus: every Explain tree maps to the submit that minted it ---
c = ec.census(ec.input_paths([]))
check("CORPUS: tracked fixtures contain Explain trees", c.trees > 0, str(c.trees))
check("CORPUS: no tracked Explain tree is unmapped",
      not any(k == ec.UNMAPPED for (k, _) in c.cells), repr([f for f in c.findings if f.field == "node_kind"]))

for name in os.listdir(tmpdir):
    os.unlink(os.path.join(tmpdir, name))
os.rmdir(tmpdir)

print(f"\nALL {passed} EXPLAIN CENSUS TESTS PASSED")
