#!/usr/bin/env python3
"""Score the candidate hosted lag_tolerance rule against every golden fixture.

For each model SubmitEnrichedSQL whose latest confirmed build of the same
target (within the same fixture) has the same SQL, execution type and
semantic extras, predict the hosted decision, is_stale and skip description:

  drift(u)   = current epoch > epoch recorded for u at that build
  producer(u, e) = identity of the latest confirmed build of target u if e is
               not after its epoch, else None (changed outside dbt / source)
  flagged(u) = producer(u) recorded at the build != producer(u) now
               ("an upstream model query has changed")
  any flagged                     -> EXECUTE
  policy stale (ANY any / ALL all drifted):
      build age < tolerance       -> SKIP, is_stale
      otherwise                   -> EXECUTE
  otherwise                       -> SKIP, is_stale = any drift
  skip description: BOTH if tol == 0 or every upstream equals its recorded
  epoch; WITHIN if build age < tol; else ATLEAST1.
  build age = hosted receipt time (recorded_at) - ConfirmExecution epoch.

Rows whose own target is missing or newer than the build are skipped (that is
own-target qualification, a separate rule). Usage: lag_rule_check.py [files]
"""
import datetime
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURES = os.path.join(HERE, "..", "golden", "fixtures")
DESC = {
    "both": "model was a no-op because both its query and its upstream data are up to date",
    "within": "model was a no-op because its query is up to date and its data is within lag tolerance",
    "atleast1": "model was a no-op because its query is up to date and at least one upstream dependency is unchanged",
}
MODEL_TYPES = {1, 2, 3, 4, 5, 6, 7}


def ms(ts):
    return int(datetime.datetime.fromisoformat(ts).timestamp() * 1000)


def identity(q):
    """Logic identity of a submit; a seed's is its values and semantic config."""
    return (q.get("sql"), q.get("execution_type"), json.dumps(q.get("semantic_extras"), sort_keys=True),
            json.dumps(q.get("query_dependencies"), sort_keys=True), q.get("values_hash"))


def check(path):
    rows = [json.loads(l) for l in open(path)]
    pending = {}   # hosted request_id -> (submit, producers of its inputs)
    builds = {}    # target -> (confirm epoch, submit request, recorded producers)
    results = []

    def producer(name, epoch):
        """Which build produced relation `name` at `epoch`: the identity of the
        latest confirmed build of that target if `epoch` is not after it,
        else None (changed outside dbt, a source, or unknown)."""
        b = builds.get(name)
        if b is None or b[0] is None or epoch is None or epoch > b[0]:
            return None
        return identity(b[1])

    for r in rows:
        m, q = r["method"], r["request"]
        if m == "SubmitValues":
            (variant, body), = r["response"]["response"].items()
            if variant == "ready_to_execute":
                pending[body["request_id"]] = ({**q, "tables": []}, {})
            continue
        if m == "SubmitEnrichedSQL":
            (variant, body), = r["response"]["response"].items()
            target = q.get("target_table")
            producers = {t["name"]: producer(t["name"], t["last_modified_epoch"])
                         for t in q["tables"] if t["name"] != target}
            if variant in ("ready_to_execute", "ready_to_clone") and "request_id" in body:
                pending[body["request_id"]] = (q, producers)
            et = q.get("execution_type")
            if not target or et not in MODEL_TYPES or target not in builds:
                continue
            epoch, built, recorded_producers = builds[target]
            if identity(built) != identity(q) or epoch is None:
                continue
            own = [t["last_modified_epoch"] for t in q["tables"] if t["name"] == target]
            if len(own) != 1 or own[0] is None or own[0] > epoch:
                continue
            cur = {t["name"]: t["last_modified_epoch"] for t in q["tables"] if t["name"] != target}
            rec = {t["name"]: t["last_modified_epoch"] for t in built["tables"] if t["name"] != target}
            ed = body.get("explained_decision") or {}
            hosted = (variant, bool(ed.get("is_stale")), ed.get("decision_description"))
            if set(cur) != set(rec):
                pred = ("ready_to_execute", False, None)
            else:
                tol = q.get("freshness_tolerance_seconds") or 0
                age = ms(r["recorded_at"]) - epoch
                drift = {n: (c is None or rec[n] is None or c > rec[n]) for n, c in cur.items()}
                flagged = any(recorded_producers.get(n) != producers.get(n) for n in cur)
                n_drift = sum(drift.values())
                policy_all = q.get("stale_upstream_policy") == 1
                stale = n_drift > 0 and (n_drift == len(cur) if policy_all else True)
                if flagged or (stale and not (tol > 0 and age < tol * 1000)):
                    pred = ("ready_to_execute", False, None)
                else:
                    if tol == 0 or all(cur[n] == rec[n] for n in cur):
                        d = DESC["both"]
                    elif age < tol * 1000:
                        d = DESC["within"]
                    else:
                        d = DESC["atleast1"]
                    if et == 7:
                        d = d.replace("model was", "snapshot was", 1)
                    pred = ("skip_execution", n_drift > 0, d)
            ok = pred[0] == hosted[0] and (pred[0] != "skip_execution" or pred[1:] == hosted[1:])
            results.append((ok, q["labels"].get("dbt_node_name"), hosted, pred))
        elif m == "ConfirmExecution":
            sub = pending.pop(q["request_id"], None)
            if sub is not None and sub[0].get("target_table"):
                builds[sub[0]["target_table"]] = (q.get("last_modified_epoch"), sub[0], sub[1])
    return results


paths = sys.argv[1:] or sorted(glob.glob(os.path.join(FIXTURES, "*.jsonl")))
total = bad = 0
for p in paths:
    res = check(p)
    total += len(res)
    for ok, node, hosted, pred in res:
        if not ok:
            bad += 1
            print(f"MISMATCH {os.path.basename(p)} {node}: hosted={hosted} predicted={pred}")
print(f"{total} decidable rows, {bad} mismatches")
