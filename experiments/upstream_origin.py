#!/usr/bin/env python3
"""Per-case record of the #237 upstream-origin capture (spec #236, Experiment 1).

Reads one `explain_capture.py upstream_origin` work directory and prints, for
each case (A same physical upstream, B confirmed copy, C independent build, D
independent build with a dev marker):

- the prod downstream build (the one FULL candidate) and its confirmation;
- the dev upstream's origin, judged by its confirmed outcome: copied (and
  from which source), built, built after a failed copy (the client's
  fallback, `failed_to_clone`) or unconfirmed, with every dev upstream
  decision;
- every ordinary dev downstream submit in order (the first dev build, then
  the next unchanged one): decision, reason codes, description, the source
  named by a grant, the copy permissions the submit carried, and the
  confirmation (or client fallback, `failed_to_clone`);
- the rows prod holds, the rows dev served after the first and the next dev
  build, and the rows the State-disabled Reference execution produced, each
  attributed to the planned Read step it came from. Served rows other than the
  Reference's are a wrong-marker correctness finding; a missing read makes
  the case inconclusive.

Usage:
  upstream_origin.py WORK_DIR [--json]
  upstream_origin.py extract OUT_DIR LABEL=SERVICE=WORK_DIR[=REFERENCE_DIR] ...
      one credential-free evidence document per case (OUT_DIR/<case>.json);
      SERVICE `hosted` names the scenario's promoted fixture; REFERENCE_DIR is
      the run's State-free Reference follow-up (`*_reference` scenario,
      `--run-id` of the run)
Self-test: python3 experiments/upstream_origin_test.py
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from decision_rows import DECISION, KIND, variant  # noqa: E402

CASES = ("a", "b", "c", "d")
LABEL = {"a": "A same physical upstream", "b": "B dev upstream copied from prod",
         "c": "C dev upstream built independently, identical rows",
         "d": "D dev upstream built independently, dev marker"}
DEV, PROD = "DEV_CLONE_DEMO", "PROD_STATE_DEMO"


def _node(row):
    return ((row.get("request") or {}).get("labels") or {}).get("dbt_node_name")


def _schema(row):
    t = (row.get("request") or {}).get("target_table") or ""
    return DEV if f'"{DEV}"' in t.upper() else PROD if f'"{PROD}"' in t.upper() else None


def _confirms(rows):
    out = {}
    for r in rows:
        if r.get("method") == "ConfirmExecution":
            req, resp = r.get("request") or {}, r.get("response") or {}
            out[req.get("request_id")] = {"success": resp.get("success"),
                                          "failed_to_clone": req.get("failed_to_clone"),
                                          "last_modified_epoch": req.get("last_modified_epoch")}
    return out


def _confirmed_epochs(rows, confirms):
    """Target relation -> the epochs its accepted confirmations recorded, in order."""
    out = {}
    for r in rows:
        if r.get("method") in ("SubmitEnrichedSQL", "RegisterClone"):
            conf = confirms.get(variant(r)[1].get("request_id"))
            if conf and conf["success"] is True:
                out.setdefault(r["request"]["target_table"], []).append(conf["last_modified_epoch"])
    return out


def _decision(row, confirms, confirmed=None):
    v, p = variant(row)
    req = row.get("request") or {}
    ed = p.get("explained_decision") or {}
    upstream = [{"name": t.get("name"), "submitted": t.get("last_modified_epoch"),
                 "confirmed": (confirmed or {}).get(t.get("name"), [])}
                for t in req.get("tables") or [] if t.get("name") != req.get("target_table")]
    return {
        "method": row.get("method"),
        "variant": v,
        "decision": DECISION.get(ed.get("decision"), ed.get("decision")),
        "codes": [ed.get("skip_rejection_reason"), ed.get("clone_rejection_reason")],
        "description": ed.get("decision_description"),
        "is_stale": ed.get("is_stale"),
        "execution_type": req.get("execution_type"),
        "source": p.get("clone_source"),
        "clone_sqls": p.get("clone_sqls"),
        "clone_required_last_modified_epoch": p.get("clone_required_last_modified_epoch"),
        "execution_runtime_ms": p.get("execution_runtime_ms"),
        "execution_decision_id": p.get("execution_decision_id"),
        "allow_clones": req.get("allow_clones"),
        "clone_chain_depth_limit": req.get("clone_chain_depth_limit"),
        "freshness_tolerance_seconds": req.get("freshness_tolerance_seconds"),
        "sql": req.get("sql"),
        "tables": req.get("tables"),
        "upstream_epochs": upstream,
        "confirm": confirms.get(p.get("request_id")),
    }


def _decisions(rows, node, schema, confirms, methods=("SubmitEnrichedSQL",)):
    confirmed = _confirmed_epochs(rows, confirms)
    return [_decision(r, confirms, confirmed) for r in rows
            if r.get("method") in methods and _node(r) == node and _schema(r) == schema
            and variant(r)[0]]


def _origin(variant_name, conf):
    """What the dev Target holds after a decision, judged by its confirmed outcome."""
    if variant_name == "skip_execution":
        return "reused"
    kind = {"ready_to_clone": "copy", "ready_to_execute": "build"}.get(variant_name, variant_name)
    if not conf or conf.get("success") is not True:
        return f"unconfirmed {kind}"
    if variant_name == "ready_to_clone":
        return "built after failed copy" if conf.get("failed_to_clone") else "copied"
    return "built"


def _upstream(rows, node, confirms):
    """The dev upstream's origin: the RegisterClone pre-step if granted, else its first submit."""
    copies = [r for r in rows if r.get("method") == "RegisterClone" and _node(r) == node
              and _schema(r) == DEV]
    decisions = _decisions(rows, node, DEV, confirms)
    granted = next((r for r in copies if variant(r)[0] == "ready_to_clone"), None)
    first = _decision(granted, confirms) if granted is not None else (
        decisions[0] if decisions else None)
    if first is None:
        return {"origin": None, "copy_source": None, "granted_source": None, "confirmed": False,
                "register_clone": None, "decisions": decisions}
    origin = _origin(first["variant"], first["confirm"])
    return {"origin": origin,
            "copy_source": first["source"] if origin == "copied" else None,
            "granted_source": first["source"],
            "confirmed": bool(first["confirm"]) and first["confirm"]["success"] is True,
            "register_clone": first if granted is not None else None,
            "decisions": decisions}


PHASES = ("prod", "served", "after_next", "reference")


def _planned_reads(steps, name):
    """Every planned Read of relation `name` (unqualified): (step index, phase, relation).

    A read's phase comes from the steps before it: a prod-schema read is
    `prod`; a dev read after a Reference selecting the node is `reference`;
    otherwise the number of `dev_clone` builds selecting the node so far makes
    it `served` (1) or `after_next` (2).
    """
    out, dev_builds, referenced = [], 0, False
    for i, s in enumerate(steps["steps"]):
        sel = {x.upper() for x in s.get("select") or ()}
        if s.get("kind") == "Build" and s.get("target") == "dev_clone" and name.upper() in sel:
            dev_builds += 1
        elif s.get("kind") == "Reference" and name.upper() in sel:
            referenced = True
        elif s.get("kind") == "Read":
            _, schema, obj = s["relation"].split(".")
            if obj.upper() != name.upper():
                continue
            if schema.upper() == PROD:
                phase = "prod"
            elif referenced:
                phase = "reference"
            else:
                phase = {1: "served", 2: "after_next"}.get(dev_builds, f"after_build_{dev_builds}")
            out.append((i, phase, s["relation"]))
    return out


def _rows_by_phase(steps, reads, name):
    """({phase: rows, or None when not measured}, [every planned read with its rows])."""
    by_read = {(rd["step"], rd["relation"]): rd["rows"] for rd in reads}
    rows = dict.fromkeys(PHASES)
    listed = []
    for step, phase, relation in _planned_reads(steps, name):
        got = by_read.get((step, relation))
        listed.append({"phase": phase, "step": step, "relation": relation, "rows": got})
        if got is not None:
            rows[phase] = got
    return rows, listed


def record(rows, steps, reads):
    """The per-case record for every case whose downstream the capture holds.

    `wrong_marker` is True when a dev read after the first or the next dev
    build differs from the Reference read, False when all agree, and None
    (inconclusive) when any of those three reads is missing.
    """
    names = steps["names"]
    confirms = _confirms(rows)
    out = {}
    for c in CASES:
        dn, up = names.get(f"o{c}_dn"), names.get(f"o{c}_up")
        if not dn or not any(_node(r) == dn for r in rows):
            continue
        rows_dn, reads_dn = _rows_by_phase(steps, reads, dn)
        missing = [p for p in ("served", "after_next", "reference") if rows_dn[p] is None]
        rows_up, reads_up = (None, [])
        if c != "a":
            rows_up, reads_up = _rows_by_phase(steps, reads, up)
        out[c] = {
            "names": {"input": names.get(f"o{c}_in"), "upstream": up, "downstream": dn},
            "prod_downstream": _decisions(rows, dn, PROD, confirms),
            "prod_upstream": _decisions(rows, up, PROD, confirms),
            "dev_upstream": _upstream(rows, up, confirms) if c != "a" else None,
            "dev_downstream": _decisions(rows, dn, DEV, confirms),
            "dev_downstream_speculative": _decisions(rows, dn, DEV, confirms,
                                                     ("SubmitEnrichedSQLSpeculative",)),
            "downstream_register_clones": sum(r.get("method") == "RegisterClone"
                                              and _node(r) == dn for r in rows),
            "rows": rows_dn,
            "upstream_rows": rows_up,
            "reads": reads_dn,
            "upstream_reads": reads_up,
            "missing": missing,
            "wrong_marker": None if missing else any(
                _canon(rows_dn[p]) != _canon(rows_dn["reference"])
                for p in ("served", "after_next")),
        }
    return out


def _canon(rows):
    return sorted(json.dumps(r, sort_keys=True) for r in rows)


def _vals(rows):
    return "—" if rows is None else ",".join(str(r.get("V", r.get("v"))) for r in rows) or "(empty)"


def _line(d):
    conf = d["confirm"]
    c = ("no confirm" if conf is None else
         f"confirm success={conf['success']} failed_to_clone={conf['failed_to_clone']}")
    src = f" source {d['source']}" if d["source"] else ""
    return (f"{d['method']} {d['decision']} {d['codes'][0]}/{d['codes'][1]} "
            f"{d['description']!r}{src}; {c}; allow_clones={d['allow_clones']} "
            f"clone_chain_depth_limit={d['clone_chain_depth_limit']} "
            f"freshness_tolerance_seconds={d['freshness_tolerance_seconds']} "
            f"et={KIND.get(d['execution_type'], d['execution_type'])}"
            + "".join(f"; reads {u['name']} at {u['submitted']} (confirmed {u['confirmed']})"
                      for u in d.get("upstream_epochs") or []))


def render(rec):
    out = []
    for c, r in rec.items():
        verdict = ("  ** WRONG MARKER **" if r["wrong_marker"] else
                   f"  INCONCLUSIVE (missing: {', '.join(r['missing'])})"
                   if r["wrong_marker"] is None else "")
        out.append(f"## {LABEL[c]}{verdict}")
        for d in r["prod_downstream"]:
            out.append(f"  prod downstream: {_line(d)}")
        u = r["dev_upstream"]
        if u:
            out.append(f"  dev upstream origin: {u['origin']} (copy source {u['copy_source']}, "
                       f"granted source {u['granted_source']}, confirmed {u['confirmed']})")
            if u["register_clone"]:
                out.append(f"    RegisterClone: {_line(u['register_clone'])}")
            for d in u["decisions"]:
                out.append(f"    {_line(d)}")
        for i, d in enumerate(r["dev_downstream"]):
            out.append(f"  dev downstream #{i + 1}: {_line(d)}")
        for d in r["dev_downstream_speculative"]:
            out.append(f"  (speculative) {_line(d)}")
        out.append(f"  downstream RegisterClone calls: {r['downstream_register_clones']}")
        rows = r["rows"]
        out.append(f"  rows v: prod={_vals(rows['prod'])} served={_vals(rows['served'])} "
                   f"after_next={_vals(rows['after_next'])} reference={_vals(rows['reference'])}")
        if r["upstream_rows"]:
            ur = r["upstream_rows"]
            out.append(f"  dev upstream rows v: served={_vals(ur['served'])} "
                       f"after_next={_vals(ur['after_next'])} reference={_vals(ur['reference'])}")
    return "\n".join(out)


# The promoted fixture of each scenario's hosted capture.
FIXTURES = {"upstream_origin": "golden/fixtures/explain_upstream_origin_incremental.jsonl",
            "upstream_origin_table": "golden/fixtures/explain_upstream_origin_table.jsonl"}


def _case_rpcs(rows, nodes):
    """The capture rows of `nodes` (decisions) and their confirmations, without call metadata."""
    mine = [r for r in rows if _node(r) in nodes and r.get("method") != "ConfirmExecution"]
    ids = {variant(r)[1].get("request_id") for r in mine} - {None, ""}
    mine_ids = {id(r) for r in mine}
    keep = [r for r in rows if id(r) in mine_ids
            or r.get("method") == "ConfirmExecution"
            and (r.get("request") or {}).get("request_id") in ids]
    drop = ("metadata", "response_metadata")
    return [{k: v for k, v in r.items() if k not in drop} for r in keep]


def _verified_reference(run, follow, case, served):
    """The State-free Reference follow-up (`--run-id`) of `run` for one case.

    Refuses a follow-up of another run, one whose capture recorded any RPC,
    or one with a Read or Reference invocation that did not pass
    `--no-manage-state` or failed.
    """
    run_id = run["steps"].get("run")
    if follow["steps"].get("run") != run_id:
        raise ValueError(f"the Reference follow-up is of run {follow['steps'].get('run')}, "
                         f"not {run_id}")
    calls = follow["state_free"]
    rpcs = sum(x["state_rpcs"] for x in calls)
    if follow["rows"] or rpcs:
        raise ValueError(f"the Reference follow-up of {run_id} reached dbt State "
                         f"({len(follow['rows'])} captured RPC(s))")
    for x in calls:
        if "--no-manage-state" not in x["argv"] or x["exit"] != 0:
            raise ValueError(f"follow-up step {x['step']} of {run_id} did not run State-free: "
                             f"{x['argv']} exit {x['exit']}")
    ref = [x for x in calls if x["kind"] == "Reference"]
    if len(ref) != 1:
        raise ValueError(f"the follow-up of {run_id} has {len(ref)} Reference invocations")
    names = follow["steps"]["names"]
    by_read = {(rd["step"], rd["relation"]): rd["rows"] for rd in follow["reads"]}
    inputs = [{"phase": "input", "step": i, "relation": st["relation"],
               "rows": by_read.get((i, st["relation"]))}
              for i, st in enumerate(follow["steps"]["steps"])
              if st.get("kind") == "Read"
              and st["relation"].split(".")[2].upper() == names[f"o{case}_in"].upper()]
    rows_dn, reads_dn = _rows_by_phase(follow["steps"], follow["reads"], names[f"o{case}_dn"])
    reads_up = ([] if case == "a" else
                _rows_by_phase(follow["steps"], follow["reads"], names[f"o{case}_up"])[1])
    reference = rows_dn["reference"]
    missing = [p for p, got in (("served", served["served"]),
                                ("after_next", served["after_next"]),
                                ("reference", reference)) if got is None]
    return {
        "run": run_id, "scenario": follow["steps"].get("scenario"),
        "client": {k: follow["client"][k] for k in ("dbt_version", "dbt_sha256")},
        "invocation": {k: ref[0][k] for k in ("argv", "exit", "state_rpcs")},
        "state_rpcs": rpcs, "capture_rpcs": len(follow["rows"]),
        "inputs": inputs, "reads": [x for x in reads_dn if x["phase"] == "reference"],
        "upstream_reads": [x for x in reads_up if x["phase"] == "reference"],
        "missing": missing,
        "wrong_marker": None if missing else any(
            _canon(served[p]) != _canon(reference) for p in ("served", "after_next")),
    }


def extract(runs):
    """One evidence document per case across `runs`, keyed by the case letter.

    Each run is {label, service, fixture, rows, steps, reads}, optionally with
    `verified_reference`: its State-free Reference follow-up {rows, steps,
    reads, state_free, client}. A document holds, per run, the decisions'
    record summary, every planned read with its run step, phase, relation and
    rows, the case's RPCs without call metadata, and the verified Reference
    with its inputs, invocation and verdict.
    """
    docs = {}
    for run in runs:
        rec = record(run["rows"], run["steps"], run["reads"])
        for c, r in rec.items():
            doc = docs.setdefault(c.upper(), {"case": c.upper(), "description": LABEL[c],
                                              "runs": []})
            u = r["dev_upstream"]
            doc["runs"].append({
                "label": run["label"], "service": run["service"], "fixture": run["fixture"],
                "run": run["steps"].get("run"), "scenario": run["steps"].get("scenario"),
                "names": r["names"],
                "dev_upstream_origin": u and {k: u[k] for k in
                                              ("origin", "copy_source", "granted_source",
                                               "confirmed")},
                "dev_downstream_decisions": [
                    {k: d[k] for k in ("decision", "codes", "description", "source",
                                       "allow_clones", "clone_chain_depth_limit",
                                       "freshness_tolerance_seconds", "confirm")}
                    for d in r["dev_downstream"]],
                "wrong_marker": r["wrong_marker"], "missing": r["missing"],
                "reads": r["reads"], "upstream_reads": r["upstream_reads"],
                "rpcs": _case_rpcs(run["rows"], {r["names"]["upstream"],
                                                 r["names"]["downstream"]}),
            })
            if run.get("verified_reference"):
                doc["runs"][-1]["verified_reference"] = _verified_reference(
                    run, run["verified_reference"], c, r["rows"])
    return docs


def _load(work):
    def load(name):
        with open(os.path.join(work, name)) as fh:
            return fh.read()

    rows = [json.loads(line) for line in load("window.jsonl").splitlines() if line.strip()]
    return rows, json.loads(load("steps.json")), json.loads(load("reads.json"))


def main(argv):
    if argv[:1] == ["extract"] and len(argv) >= 3:
        out_dir, runs = argv[1], []
        for spec in argv[2:]:
            label, service, work, *ref_dir = spec.split("=", 3)
            rows, steps, reads = _load(work)
            fixture = FIXTURES.get(steps.get("scenario")) if service == "hosted" else None
            run = {"label": label, "service": service, "fixture": fixture,
                   "rows": rows, "steps": steps, "reads": reads}
            if ref_dir:
                f_rows, f_steps, f_reads = _load(ref_dir[0])
                with open(os.path.join(ref_dir[0], "state_free.json")) as fh:
                    state_free = json.load(fh)
                with open(os.path.join(ref_dir[0], "client.json")) as fh:
                    client = json.load(fh)
                run["verified_reference"] = {"rows": f_rows, "steps": f_steps, "reads": f_reads,
                                             "state_free": state_free, "client": client}
            runs.append(run)
        os.makedirs(out_dir, exist_ok=True)
        for case, doc in extract(runs).items():
            with open(os.path.join(out_dir, f"{case}.json"), "w") as fh:
                json.dump(doc, fh, indent=1)
                fh.write("\n")
            print(f"wrote {os.path.join(out_dir, case + '.json')}")
        return 0
    if not argv or argv[0].startswith("-"):
        print(__doc__)
        return 2
    rec = record(*_load(argv[0]))
    print(json.dumps(rec, indent=1) if "--json" in argv else render(rec))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
