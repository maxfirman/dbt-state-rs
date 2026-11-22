#!/usr/bin/env python3
"""Per-case record of the #237 upstream-origin capture (spec #236, Experiment 1).

Reads one `explain_capture.py upstream_origin` work directory and prints, for
each case (A same physical upstream, B confirmed copy, C independent build, D
independent build with a dev marker):

- the prod downstream build (the one FULL candidate) and its confirmation;
- the dev upstream's origin: RegisterClone copy (and its source and
  confirmation) or its own build, with every dev upstream decision;
- every ordinary dev downstream submit in order (the first dev build, then
  the next unchanged one): decision, reason codes, description, the source
  named by a grant, the copy permissions the submit carried, and the
  confirmation (or client fallback, `failed_to_clone`);
- the rows prod holds, the rows dev served after the first and the next dev
  build, and the rows the State-disabled Reference execution produced. Served
  rows other than the Reference's are a wrong-marker correctness finding.

Usage: upstream_origin.py WORK_DIR [--json]
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


def _upstream(rows, node, confirms):
    copies = [r for r in rows if r.get("method") == "RegisterClone" and _node(r) == node
              and _schema(r) == DEV]
    decisions = _decisions(rows, node, DEV, confirms)
    granted = next((r for r in copies if variant(r)[0] == "ready_to_clone"), None)
    if granted is not None:
        v, p = variant(granted)
        conf = confirms.get(p.get("request_id")) or {}
        return {"origin": "copied", "copy_source": p.get("clone_source"),
                "confirmed": conf.get("success") is True and not conf.get("failed_to_clone"),
                "register_clone": _decision(granted, confirms), "decisions": decisions}
    first = decisions[0] if decisions else None
    origin = {"ready_to_execute": "built", "ready_to_clone": "copied on submit",
              "skip_execution": "skipped"}.get(first and first["variant"])
    conf = (first or {}).get("confirm") or {}
    return {"origin": origin, "copy_source": (first or {}).get("source"),
            "confirmed": conf.get("success") is True and not conf.get("failed_to_clone"),
            "register_clone": None, "decisions": decisions}


def _rows_by_phase(steps, reads, name):
    """{prod, served, after_next, reference} rows of relation `name` (unqualified)."""
    ref_at = next((i for i, s in enumerate(steps["steps"]) if s.get("kind") == "Reference"), None)
    out = {"prod": None, "served": None, "after_next": None, "reference": None}
    dev_state = []
    for rd in reads:
        db, schema, obj = rd["relation"].split(".")
        if obj.upper() != name.upper():
            continue
        if schema.upper() == PROD:
            out["prod"] = rd["rows"]
        elif ref_at is not None and rd["step"] > ref_at:
            out["reference"] = rd["rows"]
        else:
            dev_state.append(rd["rows"])
    if dev_state:
        out["served"] = dev_state[0]
    if len(dev_state) > 1:
        out["after_next"] = dev_state[1]
    return out


def record(rows, steps, reads):
    """The per-case record for every case whose downstream the capture holds."""
    names = steps["names"]
    confirms = _confirms(rows)
    out = {}
    for c in CASES:
        dn, up = names.get(f"o{c}_dn"), names.get(f"o{c}_up")
        if not dn or not any(_node(r) == dn for r in rows):
            continue
        rows_dn = _rows_by_phase(steps, reads, dn)
        rows_up = _rows_by_phase(steps, reads, up) if c != "a" else None
        served = [x for x in (rows_dn["served"], rows_dn["after_next"]) if x is not None]
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
            "wrong_marker": (rows_dn["reference"] is not None
                             and any(_canon(x) != _canon(rows_dn["reference"]) for x in served)),
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
        out.append(f"## {LABEL[c]}" + ("  ** WRONG MARKER **" if r["wrong_marker"] else ""))
        for d in r["prod_downstream"]:
            out.append(f"  prod downstream: {_line(d)}")
        u = r["dev_upstream"]
        if u:
            out.append(f"  dev upstream origin: {u['origin']} (source {u['copy_source']}, "
                       f"confirmed {u['confirmed']})")
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


def main(argv):
    if not argv or argv[0].startswith("-"):
        print(__doc__)
        return 2
    work = argv[0]

    def load(name):
        with open(os.path.join(work, name)) as fh:
            return fh.read()

    rows = [json.loads(line) for line in load("window.jsonl").splitlines() if line.strip()]
    rec = record(rows, json.loads(load("steps.json")), json.loads(load("reads.json")))
    print(json.dumps(rec, indent=1) if "--json" in argv else render(rec))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
