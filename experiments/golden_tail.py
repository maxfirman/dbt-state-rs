#!/usr/bin/env python3
"""Print the SubmitEnrichedSQL decisions and confirms in a golden log after
line N: golden_tail.py GOLDEN_FILE N [NODE_PREFIX]."""
import json
import sys

path, start = sys.argv[1], int(sys.argv[2])
prefix = sys.argv[3] if len(sys.argv) > 3 else ""
for i, line in enumerate(open(path)):
    if i < start:
        continue
    e = json.loads(line)
    req = e["request"]
    name = (req.get("labels") or {}).get("dbt_node_name", "")
    if not name.startswith(prefix):
        continue
    if e["method"] == "ConfirmExecution":
        print(json.dumps({"i": i, "confirm": name, "epoch": req["last_modified_epoch"]}))
    elif e["method"] == "SubmitEnrichedSQL":
        (variant, body), = e["response"]["response"].items()
        ed = body.get("explained_decision") or {}
        print(json.dumps({
            "i": i,
            "node": name,
            "variant": variant,
            "tol": req["freshness_tolerance_seconds"],
            "is_stale": ed.get("is_stale"),
            "description": ed.get("decision_description"),
            "tables": {t["name"].split(".")[-1].strip('"'): t["last_modified_epoch"]
                       for t in req["tables"]},
            "submitted_at": dict(e["metadata"]).get("x-submitted-at-epoch"),
            "client": None,
        }))
