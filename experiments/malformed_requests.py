#!/usr/bin/env python3
"""Crafted probes for reuse qualification on malformed requests (#104, 104c).

This server fails reuse qualification (`no_reuse_candidate.failed_reuse_qualification`)
for a submit whose identity matches its latest build when the request is
malformed: empty SQL, or a query dependency with an empty name or query, or
listed twice (`reuse_candidate.rs::sql_qualifies`). Client 2.0.11 never sends
these, so each probe is a crafted `state-probe` request against fresh,
never-created names in CDO_DEMO_DB.PROD_STATE_DEMO. No warehouse is touched;
epochs are made up (Unix ms, wall-clock based). Every call goes through
whatever `PROBE_URL` points at: the recording proxy for a hosted capture, or a
local server for a dry run.

Each probe: model M over one view V is built (submitted and confirmed) with
the malformed request, then the identical request is submitted again, then
Explain is asked for that decision.

  e1   empty SQL (`sql: ""`)
  e2   a query dependency with an empty query
  e3   a query dependency with an empty name
  e4   the same query dependency listed twice

Names carry a random suffix so a rerun never reuses hosted State history.

Usage: malformed_requests.py PROBE [PROBE ...]   (or `all`)
Prints one JSON line per step (pipe into experiments/lag_show.py); a failed
RPC prints an `error` row and the remaining probes still run.
"""
import json
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lag_matrix as lm  # noqa: E402
from lag_matrix import DB, SCHEMA, Node, Source, now_ms, report  # noqa: E402
from lag_matrix import submit_request as submit  # noqa: E402

RUN = f"{time.strftime('%H%M%S')}{uuid.uuid4().hex[:6]}"


def model(probe_id):
    v = Source(probe_id, "v")
    v.name = f"mal{RUN}_{probe_id}_v"
    m = Node(probe_id, "m", [v], tol=0)
    m.name = f"mal{RUN}_{probe_id}_m"
    return v, m


def dependency(v):
    return {
        "name": v.fqn,
        "query": f"create or replace view {v.name.upper()} as (select 1 as id)",
        "default_catalog": DB,
        "default_schema": SCHEMA,
    }


def malformed(probe_id, shape):
    """Build M with the malformed request `shape(request, v)`, then resubmit it."""
    v, m = model(probe_id)
    req = m.request()
    shape(req, v)
    variant, body = submit(req)
    report(probe_id, "build", m, variant, body)
    if variant != "ready_to_execute":
        raise SystemExit(f"{m.name}: expected a first build, got {variant}")
    m.confirm(body["request_id"], now_ms())
    time.sleep(2)
    again = m.request()
    shape(again, v)
    variant, body = submit(again)
    report(probe_id, "resubmit", m, variant, body, explain=True)


def empty_sql(req, v):
    req["sql"] = ""


def empty_dependency_query(req, v):
    req["query_dependencies"] = [dict(dependency(v), query="")]


def empty_dependency_name(req, v):
    req["query_dependencies"] = [dict(dependency(v), name="")]


def duplicated_dependency(req, v):
    req["query_dependencies"] = [dependency(v), dependency(v)]


PROBES = {
    "e1": lambda: malformed("e1", empty_sql),
    "e2": lambda: malformed("e2", empty_dependency_query),
    "e3": lambda: malformed("e3", empty_dependency_name),
    "e4": lambda: malformed("e4", duplicated_dependency),
}

if __name__ == "__main__":
    lm.RUN = RUN
    wanted = sys.argv[1:] or ["all"]
    names = list(PROBES) if wanted == ["all"] else wanted
    failed = False
    for n in names:
        try:
            PROBES[n]()
        except (SystemExit, AssertionError) as e:
            failed = True
            print(json.dumps({"arm": n, "error": str(e)}), flush=True)
    sys.exit(1 if failed else 0)
