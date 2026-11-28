#!/usr/bin/env python3
"""Per-case record of an `explain_capture.py register_clone_input_origin` run (#271).

For each case (I independent input, S shared-input control) and each
downstream (merge incremental `dm`, snapshot `ds`) it prints:

- the prod downstream build and its confirmation;
- dev's upstream (case I): its build without copy permission and its rows;
- every dev `RegisterClone` of the downstream: the source and destination
  it names, the grant, and the copy's confirmation (`failed_to_clone` is the
  client's fallback);
- every dev downstream `SubmitEnrichedSQL` in order (the first post-copy
  submit, then the unchanged rerun): decision, reason codes, description, the
  inputs it read with their epochs, and its Explain tree;
- the rows dev served after each dev round and the rows of the
  independent-schema Reference. For a snapshot the compared row is the
  current one (`dbt_valid_to` null) of the marker key. A served marker other
  than the Reference's is the wrong-marker finding.

Usage:
  register_clone_origin.py WORK_DIR [--json]
  register_clone_origin.py extract OUT_FILE LABEL=SERVICE=WORK_DIR ...
      one credential-free evidence document for every run (no call metadata)
Self-test: python3 experiments/register_clone_origin_test.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from decision_rows import DECISION, KIND, variant  # noqa: E402

CASES = {"i": "I independent input (dev upstream built with a dev marker)",
         "s": "S shared-input control (both read prod's upstream)"}
DOWNSTREAMS = {"dm": "merge incremental", "ds": "snapshot"}
PROD, DEV, REF = "PROD_STATE_DEMO", "DEV_CLONE_DEMO", "REF_STATE_DEMO"


def _node(row):
    return ((row.get("request") or {}).get("labels") or {}).get("dbt_node_name")


def _schema_of(name):
    parts = [p.strip('"').upper() for p in (name or "").split(".")]
    return parts[1] if len(parts) == 3 else None


def _schema(row):
    return _schema_of((row.get("request") or {}).get("target_table"))


def _confirms(rows):
    out = {}
    for r in rows:
        if r.get("method") == "ConfirmExecution":
            req, resp = r.get("request") or {}, r.get("response") or {}
            out[req.get("request_id")] = {"success": resp.get("success"),
                                          "failed_to_clone": req.get("failed_to_clone"),
                                          "last_modified_epoch": req.get("last_modified_epoch")}
    return out


def _explains(rows):
    """execution_decision_id -> the Explain message hosted (or the server) returned."""
    out = {}
    for r in rows:
        if r.get("method") == "GetExplainMessages":
            for m in (r.get("response") or {}).get("messages") or []:
                out[m.get("execution_decision_id")] = m
    return out


def _render(lines, depth=0):
    out = []
    for ln in lines or []:
        mark = "".join(f" [{x}]" for x in (ln.get("marker"), ln.get("badge")) if x)
        out.append("  " * depth + (ln.get("text") or "") + mark)
        out += _render(ln.get("children"), depth + 1)
    return out


def _decision(row, confirms, explains):
    v, p = variant(row)
    req = row.get("request") or {}
    ed = p.get("explained_decision") or {}
    edid = p.get("execution_decision_id")
    msg = explains.get(edid) if edid else None
    return {
        "method": row.get("method"),
        "variant": v,
        "decision": DECISION.get(ed.get("decision"), ed.get("decision")),
        "codes": [ed.get("skip_rejection_reason"), ed.get("clone_rejection_reason")],
        "description": ed.get("decision_description"),
        "execution_type": req.get("execution_type"),
        "target_table": req.get("target_table"),
        "clone_source_table": req.get("clone_source_table"),
        "clone_source_last_modified_epoch": req.get("clone_source_last_modified_epoch"),
        "freshness_tolerance_seconds": req.get("freshness_tolerance_seconds"),
        "allow_clones": req.get("allow_clones"),
        "inputs": [{"name": t.get("name"), "last_modified_epoch": t.get("last_modified_epoch")}
                   for t in req.get("tables") or [] if t.get("name") != req.get("target_table")],
        "own_target_epoch": next((t.get("last_modified_epoch") for t in req.get("tables") or []
                                  if t.get("name") == req.get("target_table")), None),
        "execution_decision_id": edid,
        "confirm": confirms.get(p.get("request_id")),
        "explain": None if msg is None else {
            "decision": DECISION.get(msg.get("decision"), msg.get("decision")),
            "description": msg.get("decision_description"),
            "lines": _render(msg.get("explain_lines"))},
    }


def _decisions(rows, node, schema, methods, confirms, explains):
    return [_decision(r, confirms, explains) for r in rows
            if r.get("method") in methods and _node(r) == node and _schema(r) == schema
            and variant(r)[0]]


def _reads(steps, reads, name):
    """{phase: rows} for every planned Read of relation `name`, by schema and dev round."""
    by_read = {(rd["step"], rd["relation"]): rd["rows"] for rd in reads}
    out, dev_builds = {}, 0
    for i, s in enumerate(steps["steps"]):
        sel = {x.upper() for x in s.get("select") or ()}
        if s.get("kind") == "Build" and s.get("target") == "dev_clone" and name.upper() in sel:
            dev_builds += 1
        elif s.get("kind") == "Read":
            schema, obj = s["relation"].split(".")[1:]
            if obj.upper() != name.upper():
                continue
            phase = {PROD: "prod", REF: "reference"}.get(schema.upper()) or {
                0: "before_dev_round", 1: "served", 2: "after_rerun"}.get(
                    dev_builds, f"after_round_{dev_builds}")
            out[phase] = by_read.get((i, s["relation"]))
    return out


def _get(row, key):
    return row.get(key.upper(), row.get(key.lower()))


def marker(rows, snapshot):
    """The marker the relation serves for key 1: a snapshot's current row, else the row."""
    if rows is None:
        return None
    rows = [r for r in rows if _get(r, "id") in (1, "1")]
    if snapshot:
        rows = [r for r in rows if _get(r, "dbt_valid_to") is None]
    vals = sorted({str(_get(r, "v")) for r in rows})
    return vals[0] if len(vals) == 1 else (vals or None)


def record(rows, steps, reads):
    names = steps["names"]
    confirms, explains = _confirms(rows), _explains(rows)
    out = {}
    for c in CASES:
        for d in DOWNSTREAMS:
            dn = names[f"r{c}_{d}"]
            phases = _reads(steps, reads, dn)
            markers = {p: marker(v, d == "ds") for p, v in phases.items()}
            measured = [markers.get(p) for p in ("served", "after_rerun", "reference")]
            out[f"{c}_{d}"] = {
                "case": c, "downstream": d, "node": dn,
                "prod_build": _decisions(rows, dn, PROD, ("SubmitEnrichedSQL",), confirms,
                                         explains),
                "register_clone": _decisions(rows, dn, DEV, ("RegisterClone",), confirms,
                                             explains),
                "dev_submits": _decisions(rows, dn, DEV, ("SubmitEnrichedSQL",), confirms,
                                          explains),
                "rows": phases,
                "markers": markers,
                "wrong_marker": None if None in measured else (
                    markers["served"] != markers["reference"]
                    or markers["after_rerun"] != markers["reference"]),
            }
        if c == "i":
            up = names["ri_up"]
            out["i_up"] = {
                "node": up,
                "dev_builds": _decisions(rows, up, DEV, ("SubmitEnrichedSQL",), confirms,
                                         explains),
                "rows": _reads(steps, reads, up),
                # Dev's input, copied into the Reference schema before the
                # Reference ran over it.
                "reference_input": _reads(steps, reads, names["ri_in"]).get("reference"),
            }
    return out


def _line(d):
    conf = d["confirm"]
    c = ("no confirm" if conf is None else
         f"confirm success={conf['success']} failed_to_clone={conf['failed_to_clone']} "
         f"epoch={conf['last_modified_epoch']}")
    src = (f" {d['clone_source_table']}@{d['clone_source_last_modified_epoch']} -> "
           f"{d['target_table']}" if d["method"] == "RegisterClone" else "")
    ins = "".join(f"; reads {u['name']}@{u['last_modified_epoch']}" for u in d["inputs"])
    return (f"{d['method']}{src} {d['decision']} {d['codes'][0]}/{d['codes'][1]} "
            f"{d['description']!r}; {c}; tolerance={d['freshness_tolerance_seconds']} "
            f"et={KIND.get(d['execution_type'], d['execution_type'])}{ins}")


def render(rec):
    out = []
    for key, r in rec.items():
        if key == "i_up":
            out.append(f"## I dev upstream {r['node']}")
            out += [f"  {_line(d)}" for d in r["dev_builds"]]
            out.append(f"  rows: {json.dumps(r['rows'])}")
            continue
        verdict = {True: "  ** WRONG MARKER **", None: "  INCONCLUSIVE", False: ""}[
            r["wrong_marker"]]
        out.append(f"## {CASES[r['case']]}, {DOWNSTREAMS[r['downstream']]} {r['node']}{verdict}")
        out += [f"  prod: {_line(d)}" for d in r["prod_build"]]
        out += [f"  {_line(d)}" for d in r["register_clone"]]
        for i, d in enumerate(r["dev_submits"]):
            out.append(f"  dev submit #{i + 1}: {_line(d)}")
            if d["explain"]:
                out += [f"      {ln}" for ln in d["explain"]["lines"]] or ["      (empty tree)"]
        out.append(f"  markers: {json.dumps(r['markers'])}")
    return "\n".join(out)


def _load(work):
    def load(name):
        with open(os.path.join(work, name)) as fh:
            return fh.read()

    rows = [json.loads(line) for line in load("window.jsonl").splitlines() if line.strip()]
    state_free = json.loads(load("state_free.json"))
    client = json.loads(load("client.json"))
    return rows, json.loads(load("steps.json")), json.loads(load("reads.json")), state_free, client


def extract(runs):
    """One document: per run its client, State-free invocations and the per-case record."""
    doc = []
    for label, service, (rows, steps, reads, state_free, client) in runs:
        doc.append({
            "label": label, "service": service, "run": steps["run"],
            "scenario": steps["scenario"], "names": steps["names"],
            "client": {k: client[k] for k in ("dbt_version", "dbt_sha256",
                                              "wire_dbt_run_cache_versions")},
            "reference": [x for x in state_free if x["kind"] == "Reference"],
            "state_free_rpcs": sum(x["state_rpcs"] for x in state_free),
            "cases": record(rows, steps, reads),
        })
    return doc


def main(argv):
    if argv[:1] == ["extract"] and len(argv) >= 3:
        runs = []
        for spec in argv[2:]:
            label, service, work = spec.split("=", 2)
            runs.append((label, service, _load(work)))
        with open(argv[1], "w") as fh:
            json.dump(extract(runs), fh, indent=1)
            fh.write("\n")
        print(f"wrote {argv[1]}")
        return 0
    if not argv or argv[0].startswith("-"):
        print(__doc__)
        return 2
    rows, steps, reads, _, _ = _load(argv[0])
    rec = record(rows, steps, reads)
    print(json.dumps(rec, indent=1) if "--json" in argv else render(rec))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
