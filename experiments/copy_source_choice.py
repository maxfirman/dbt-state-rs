#!/usr/bin/env python3
"""#239: which existing physical result hosted State offers to copy when several
Snowflake builds compete as the source of an ordinary FULL submit.

Each history is fresh and isolated (its own model, object name and upstream
names). Up to three source builds of one model (one unique id, one output
object name, one catalog and connection namespace) sit in distinct schemas;
a destination of the same model in a fourth schema is absent and submits
last. Every submit is FULL with Lag tolerance 0, stale upstream policy ANY,
`allow_clones: true`, `defer_enabled: true` and no chain or time-travel limit,
in the shape client 2.0.11 sends for a table model. No warehouse is touched:
the requests name never-created relations in `CDO_DEMO_DB` with made-up,
wall-clock-based epochs.

Sources (`ROLES`):

  old  `CSC_OLD`: reads the shared upstream `CSC_UP.<src>` after its change,
       at the epoch the destination reports: fresh, the oldest completion
       except in the `_late` controls.
  mid  `CSC_MID`: reads `CSC_UP_ALT.<src>`, another relation of the same
       name, so its SQL is the same once schemas are abstracted but its
       physical input differs. It reads it at B-60s, or in
       `mid_current_alone` at the epoch the destination reports for
       `CSC_UP.<src>`.
  new  `CSC_NEW`: reads `CSC_UP.<src>` before its change: stale for the
       destination (tolerance 0). A build that read the upstream before it
       changed was necessarily submitted first.

Every history (`HISTORIES`) follows one timeline from its base B: `new`
submits at B, the upstream changes at B+0.5s, `old` submits at B+1.5s and
`mid` at B+3s. The completion epochs are B+4s, B+5s and B+6s in the
history's completion order (old < mid < new, or reversed in `_late`). The
confirms are delivered from B+7s, 1.5 s apart, in the history's
confirmation order, each after its epoch. A `_rev` control differs from
its base history only in confirmation order, and a `_late` control from
the `_rev` one only in completion order. The destination submits 2 s after
the last confirm and is explained 2 s later. A grant names its source
(`clone_source`) and echoes its build epoch and runtime; every confirmed
runtime in a run is distinct.

Usage:
  copy_source_choice.py plan                 print the plan and submit count (no RPC)
  copy_source_choice.py run [HISTORY ...]    send it through `PROBE_URL` (your own
                                             recording proxy, or a local server)
  copy_source_choice.py table CAPTURE.jsonl  one result per history in a capture

Self-test: python3 experiments/copy_source_choice_test.py
"""
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROBE = os.path.join(REPO, "target", "debug", "state-probe")
DB = "CDO_DEMO_DB"
NAMESPACE = "fc38a5ba0eceb701c6ab7c6b882e28b0"  # the demo connection namespace
PROJECT_ID = "70437463667433"  # jaffle-shop's dbt Cloud project
FULL = 1
SUBMIT_BUDGET = 40
GAP_MS = 1500

ROLES = ("old", "mid", "new")
SCHEMAS = {"old": "CSC_OLD", "mid": "CSC_MID", "new": "CSC_NEW", "dest": "CSC_DEST"}
UPSTREAM, UPSTREAM_ALT = "CSC_UP", "CSC_UP_ALT"

HISTORIES = {
    "old_alone": {"sources": ["old"], "confirm": ["old"], "completion": ["old"],
                  "what": "positive control: the oldest fresh source alone"},
    "mid_alone": {"sources": ["mid"], "confirm": ["mid"], "completion": ["mid"],
                  "what": "the middle source (other physical input) alone"},
    "mid_current_alone": {"sources": ["mid"], "confirm": ["mid"], "completion": ["mid"],
                          "mid_read": "current",
                          "what": "as mid_alone, the middle source reading its relation at "
                                  "the epoch the destination reports for its own"},
    "new_alone": {"sources": ["new"], "confirm": ["new"], "completion": ["new"],
                  "what": "control: the newer stale source alone"},
    "old_mid": {"sources": ["old", "mid"], "confirm": ["old", "mid"],
                "completion": ["old", "mid"],
                "what": "oldest and middle, confirmed in completion order"},
    "old_mid_rev": {"sources": ["old", "mid"], "confirm": ["mid", "old"],
                    "completion": ["old", "mid"],
                    "what": "as old_mid, confirmed in reverse completion order"},
    "old_mid_late": {"sources": ["old", "mid"], "confirm": ["mid", "old"],
                     "completion": ["mid", "old"],
                     "what": "as old_mid_rev, but the fresh source completes last"},
    "old_new": {"sources": ["old", "new"], "confirm": ["old", "new"],
                "completion": ["old", "new"],
                "what": "oldest and newer stale, confirmed in completion order"},
    "old_new_rev": {"sources": ["old", "new"], "confirm": ["new", "old"],
                    "completion": ["old", "new"],
                    "what": "as old_new, confirmed in reverse completion order"},
    "old_new_late": {"sources": ["old", "new"], "confirm": ["new", "old"],
                     "completion": ["new", "old"],
                     "what": "as old_new_rev, but the fresh source completes last"},
    "old_mid_new": {"sources": ["old", "mid", "new"], "confirm": ["old", "mid", "new"],
                    "completion": ["old", "mid", "new"],
                    "what": "all three, confirmed in completion order"},
    "old_mid_new_rev": {"sources": ["old", "mid", "new"], "confirm": ["new", "mid", "old"],
                        "completion": ["old", "mid", "new"],
                        "what": "as old_mid_new, confirmed in reverse completion order"},
    "old_mid_new_late": {"sources": ["old", "mid", "new"], "confirm": ["new", "mid", "old"],
                         "completion": ["new", "mid", "old"],
                         "what": "as old_mid_new_rev, but the fresh source completes last"},
}


# Offsets from a history's base (ms).
SUBMIT_AT = {"new": 0, "old": 1500, "mid": 3000}
UPSTREAM_CHANGE_AT = 500
COMPLETION_SLOTS = (4000, 5000, 6000)  # the history's completion order, oldest first
FIRST_CONFIRM_AT = 7000
EARLIER = 60_000  # the upstreams' modification before the history starts


# --- names -------------------------------------------------------------------------------


def object_name(run, history):
    return f"csc{run}_{history}"


def fqn(schema, name):
    return f'"{DB}"."{schema.upper()}"."{name.upper()}"'


def upstream(run, history, alternate=False):
    return fqn(UPSTREAM_ALT if alternate else UPSTREAM, f"{object_name(run, history)}_src")


_PART = r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)'
_REL = re.compile(rf"{_PART}\.{_PART}\.{_PART}")


def split_fqn(name):
    """(catalog, schema, object), unquoted and upper-cased."""
    parts = re.findall(r'"([^"]+)"|([A-Za-z_][A-Za-z0-9_$]*)', name)
    return tuple((q or u).upper() for q, u in parts)


def sql_relations(sql):
    return _REL.findall(sql)


def abstract_schemas(sql):
    """`sql` with the schema of each three-part name replaced by one token per
    distinct schema, in sorted order (as the Snowflake copy identity does)."""
    schemas = sorted({split_fqn(r)[1] for r in sql_relations(sql)})
    return _REL.sub(lambda m: "{}.__ns{}__.{}".format(
        split_fqn(m.group(0))[0], schemas.index(split_fqn(m.group(0))[1]),
        split_fqn(m.group(0))[2]), sql)


def demo_relation(name, run):
    parts = split_fqn(name)
    return len(parts) == 3 and parts[0] == DB and f"CSC{run}".upper() in parts[2]


def _h(s):
    return hashlib.md5(s.encode()).hexdigest()


# --- the plan ------------------------------------------------------------------------------


def model_request(run, history, role, upstream_name, upstream_epoch):
    """The FULL submit of `role`'s build of the history's model (client 2.0.11 shape)."""
    name = object_name(run, history)
    uid = f"model.jaffle_shop.{name}"
    schema = SCHEMAS[role]
    _, up_schema, up_obj = split_fqn(upstream_name)
    return {
        "target_table": fqn(schema, name),
        "dialect": "snowflake",
        "default_catalog": DB,
        "execution_type": FULL,
        "sql": f"\nselect id, v from {DB}.{up_schema.lower()}.{up_obj.lower()}",
        "tables": [{"name": upstream_name, "last_modified_epoch": upstream_epoch}],
        "query_dependencies": [],
        "semantic_extras": {"lookback": "1", "on_schema_change": '"ignore"'},
        "freshness_tolerance_seconds": 0,
        "lenient_dependencies": [],
        "tolerate_nondeterminism": True,
        "labels": {"dbt_node_fqn": f"jaffle_shop.marts.{name}", "dbt_node_name": name,
                   "dbt_node_unique_id": uid},
        "clone_time_travel_limit": None,
        "clone_table_properties": None,
        "stale_upstream_policy": 0,
        "clone_chain_depth_limit": None,
        "dbt_node_state": {
            "node_unique_id": uid, "target_name": f"csc_{role}", "project_name": "jaffle_shop",
            "resource_type": "model", "node_hash": _h(f"n{name}"),
            "node_body_hash": _h(f"b{name}"), "node_configs_hash": _h(f"c{name}"),
            "node_persisted_descriptions_hash": None, "node_macros_hash": None,
            "node_contract_hash": "298a88779c630d09cfb26e926f3af91b",
            "profile_name": "snowflake", "project_id": PROJECT_ID,
            "node_database_representation": None,
        },
        "default_schema": schema.lower(),
        "compare_unrendered_code": False,
        "table_namespace": NAMESPACE,
        "ignore_external_modifications": False,
        "allow_clones": True,
        "is_defer_to_profile": False,
        "defer_enabled": True,
    }


def runtime_ms(history, role):
    k = list(HISTORIES).index(history)
    return 51_000 + 100 * k + 11 * (ROLES.index(role) + 1)


def plan_history(run, history, base):
    """Every step of one history, at absolute wall-clock ms from `base`."""
    h = HISTORIES[history]
    up, alt = upstream(run, history), upstream(run, history, alternate=True)
    before, after = base - EARLIER, base + UPSTREAM_CHANGE_AT
    mid_epoch = after if h.get("mid_read") == "current" else before
    reads = {"old": (up, after), "mid": (alt, mid_epoch), "new": (up, before)}
    steps = []
    for role in sorted(h["sources"], key=SUBMIT_AT.get):
        steps.append({"op": "submit", "role": role, "at": base + SUBMIT_AT[role],
                      "request": model_request(run, history, role, *reads[role])})
    labels = {s["role"]: s["request"]["labels"] for s in steps}
    completed = {r: base + COMPLETION_SLOTS[k] for k, r in enumerate(h["completion"])}
    at = base + FIRST_CONFIRM_AT
    for role in h["confirm"]:
        steps.append({"op": "confirm", "role": role, "at": at, "request": {
            "request_id": None, "last_modified_epoch": completed[role],
            "failed_to_clone": False, "table_type": "TRANSIENT TABLE",
            "execution_results": None, "execution_runtime_ms": runtime_ms(history, role),
            "labels": labels[role]}})
        at += GAP_MS
    at += 2000 - GAP_MS
    steps.append({"op": "submit", "role": "dest", "at": at,
                  "request": model_request(run, history, "dest", up, after)})
    steps.append({"op": "explain", "role": "dest", "at": at + 2000, "request": None})
    return {"history": history, "what": h["what"], "base": base, "steps": steps}


def max_submits(names=None):
    """One submit per source plus the destination's, for `names` (default: all)."""
    return sum(len(HISTORIES[n]["sources"]) + 1 for n in (names or HISTORIES))


# --- reading a capture -------------------------------------------------------------------------

_ROLE_OF_SCHEMA = {v: k for k, v in SCHEMAS.items()}


def _variant(row):
    resp = (row.get("response") or {}).get("response") or {}
    if len(resp) != 1:
        return None, {}
    ((name, body),) = resp.items()
    return name, body or {}


def _sent_at(row):
    md = dict(row.get("metadata") or [])
    v = md.get("x-submitted-at-epoch")
    return int(v) if v else None


def _explain_names(response):
    names = []

    def walk(lines):
        for line in lines or []:
            names.extend(sql_relations(line.get("text") or ""))
            walk(line.get("children"))

    for m in (response or {}).get("messages") or []:
        walk(m.get("explain_lines"))
    return names


def history_rows(rows):
    """One result per history in a capture (see the module doc)."""
    hist, order, by_rid, by_edid = {}, [], {}, {}

    def get(name):
        if name not in hist:
            hist[name] = {"object": name, "history": name.split("_", 1)[1], "sources": [], "confirm_order": [], "completion": {},
                          "runtime": {}, "dest_entry": None, "result": None, "codes": None,
                          "description": None, "is_stale": None, "agrees": None,
                          "explain_names": None, "inconclusive": [], "_fqn": {},
                          "_flagged": set()}
            order.append(name)
        return hist[name]

    for i, r in enumerate(rows):
        req = r.get("request") or {}
        if r.get("method") == "SubmitEnrichedSQL" and req.get("target_table"):
            _, schema, obj = split_fqn(req["target_table"])
            role = _ROLE_OF_SCHEMA.get(schema)
            if role is None:
                continue
            h = get(obj.lower())
            h["_fqn"][role] = req["target_table"]
            variant, body = _variant(r)
            sent = _sent_at(r)
            for t in req.get("tables") or []:
                if sent is not None and (t.get("last_modified_epoch") or 0) > sent:
                    h["inconclusive"].append(f"{role} submit at entry {i} reports a future epoch")
            if role == "dest":
                if h["dest_entry"] is not None:
                    h["inconclusive"].append(f"destination submitted again at entry {i}")
                    continue
                h["dest_entry"] = i
                if any(t["name"] == req["target_table"] for t in req.get("tables") or []):
                    h["inconclusive"].append("destination not absent")
                ed = body.get("explained_decision") or {}
                h["codes"] = f"{ed.get('skip_rejection_reason')}/{ed.get('clone_rejection_reason')}"
                h["description"], h["is_stale"] = ed.get("decision_description"), ed.get("is_stale")
                h["_dest"] = (variant, body)
                if body.get("execution_decision_id"):
                    by_edid[body["execution_decision_id"]] = h
                continue
            h["sources"].append({"role": role, "entry": i, "variant": variant,
                                 "reads": {t["name"]: t.get("last_modified_epoch")
                                           for t in req.get("tables") or []}})
            if variant == "ready_to_execute":
                by_rid[body.get("request_id")] = (h, role)
            else:
                h["inconclusive"].append(f"{role} submit answered {variant}, not a first build")
                h["_flagged"].add(role)
        elif r.get("method") == "ConfirmExecution" and req.get("request_id") in by_rid:
            h, role = by_rid[req["request_id"]]
            if (r.get("response") or {}).get("success") is not True:
                h["inconclusive"].append(f"{role} confirm at entry {i} refused")
                h["_flagged"].add(role)
                continue
            sent = _sent_at(r)
            if sent is not None and (req.get("last_modified_epoch") or 0) > sent:
                h["inconclusive"].append(f"{role} confirmed at entry {i} with a future epoch")
            h["confirm_order"].append(role)
            h["completion"][role] = req.get("last_modified_epoch")
            h["runtime"][role] = req.get("execution_runtime_ms")
        elif r.get("method") == "GetExplainMessages":
            for edid in req.get("execution_decision_ids") or []:
                if edid in by_edid:
                    by_edid[edid]["explain_names"] = _explain_names(r.get("response"))

    out = []
    for name in order:
        h = hist[name]
        flagged = h.pop("_flagged")
        for s in h["sources"]:
            if s["role"] not in h["completion"] and s["role"] not in flagged:
                h["inconclusive"].append(f"{s['role']} not confirmed")
        roles_of = {v: k for k, v in h.pop("_fqn").items()}
        h["completion_order"] = sorted(h["completion"], key=h["completion"].get)
        variant, body = h.pop("_dest", (None, {}))
        if h["dest_entry"] is None:
            h["inconclusive"].append("destination never submitted")
        elif variant == "ready_to_clone":
            src = body.get("clone_source")
            role = roles_of.get(src)
            h["result"] = f"CLONE {role or src}"
            h["agrees"] = role is not None \
                and body.get("clone_required_last_modified_epoch") == h["completion"].get(role) \
                and body.get("execution_runtime_ms") == h["runtime"].get(role)
        elif variant == "ready_to_execute":
            h["result"] = f"BUILD {h['codes']}"
        elif variant == "skip_execution":
            h["result"] = "SKIP"
        else:
            h["result"] = variant
        if h["explain_names"] is not None:
            h["explain_names"] = [roles_of.get(n, n) for n in h["explain_names"]]
        out.append(h)
    return out


def print_table(rows):
    for h in history_rows(rows):
        print(json.dumps(h))


# --- running it ----------------------------------------------------------------------------------


def _probe(op, body, invocation):
    env = dict(os.environ, PROBE_INVOCATION=invocation, PROBE_SESSION_ID=invocation)
    out = subprocess.run([PROBE, op], input=json.dumps(body), capture_output=True, text=True,
                         env=env)
    if out.returncode != 0:
        raise RuntimeError(f"state-probe {op} failed: {out.stderr.strip()}")
    return json.loads(out.stdout)


METHOD = {"submit": "SubmitEnrichedSQL", "confirm": "ConfirmExecution",
          "explain": "GetExplainMessages"}


def run_history(run, history, out=print):
    """Send one history on schedule; return its rows in the capture format."""
    plan = plan_history(run, history, int(time.time() * 1000) + 500)
    out(json.dumps({"history": history, "what": plan["what"], "base": plan["base"]}))
    invocations = {r: str(uuid.uuid4()) for r in list(ROLES) + ["dest"]}
    rows, rids, edid = [], {}, None
    for s in plan["steps"]:
        delay = s["at"] / 1000 - time.time()
        if delay > 0:
            time.sleep(delay)
        sent = int(time.time() * 1000)
        body = s["request"]
        if s["op"] == "confirm":
            if s["role"] not in rids:
                out(json.dumps({"history": history, "step": "confirm", "role": s["role"],
                                "skipped": "its submit was not a first build"}))
                continue
            body = dict(body, request_id=rids[s["role"]])
        if s["op"] == "explain":
            if not edid:
                continue
            body = {"execution_decision_ids": [edid]}
        try:
            resp = _probe(s["op"], body, invocations[s["role"]])
        except RuntimeError as e:
            out(json.dumps({"history": history, "step": s["op"], "role": s["role"],
                            "error": str(e)}))
            break
        late = sent - s["at"]
        out(json.dumps({"history": history, "step": s["op"], "role": s["role"],
                        "planned": s["at"], "sent": sent, "late_ms": late, "response": resp}))
        rows.append({"method": METHOD[s["op"]], "request": body, "response": resp,
                     "metadata": [["x-submitted-at-epoch", str(sent)]]})
        if s["op"] == "submit":
            variant, b = _variant(rows[-1])
            if s["role"] != "dest" and variant == "ready_to_execute":
                rids[s["role"]] = b["request_id"]
            if s["role"] == "dest":
                edid = b.get("execution_decision_id")
    return rows


def main(argv):
    if argv[:1] == ["table"] and len(argv) > 1:
        for path in argv[1:]:
            with open(path) as fh:
                print_table([json.loads(line) for line in fh if line.strip()])
        return 0
    if argv[:1] == ["plan"]:
        run = "000000plan00"
        for name in HISTORIES:
            p = plan_history(run, name, 0)
            print(json.dumps({"history": name, "what": p["what"],
                              "steps": [(s["op"], s["role"], s["at"]) for s in p["steps"]]}))
        print(json.dumps({"histories": len(HISTORIES), "max_submits": max_submits(),
                          "budget": SUBMIT_BUDGET}))
        return 0
    if argv[:1] == ["run"]:
        if not os.environ.get("PROBE_URL"):
            print("REFUSED: set PROBE_URL to your own recording proxy (or a local server)")
            return 2
        names = argv[1:] or list(HISTORIES)
        unknown = [n for n in names if n not in HISTORIES]
        if unknown:
            print(f"unknown histories: {unknown}")
            return 2
        run = time.strftime("%H%M%S") + uuid.uuid4().hex[:6]
        print(json.dumps({"run": run, "histories": names, "max_submits": max_submits(names)}))
        rows = []
        for n in names:
            rows += run_history(run, n)
        for h in history_rows(rows):
            print(json.dumps({"summary": h}))
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
