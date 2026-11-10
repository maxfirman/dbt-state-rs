#!/usr/bin/env python3
"""Crafted probes for the three ambiguous rebuild reasons (#71, spec #40).

Client 2.0.11 never sends these requests (docs/explain-catalog.md, "Ambiguous
rebuild reasons"), so each probe is a crafted `state-probe` request against
fresh, never-created names in CDO_DEMO_DB.PROD_STATE_DEMO. No warehouse is
touched; epochs are made up (Unix ms, wall-clock based). Every call goes
through whatever `PROBE_URL` points at: the recording proxy for a hosted
capture, or a local server for a dry run.

Each probe: model M over one source S is built and confirmed, then M is
submitted with ambiguous evidence about its Target relation, then Explain is
asked for that decision.

  d1   the Target relation reported twice (same epoch)
  d2   the Target relation reported with no epoch
  d3   the submit has no target (`target_table` unset)
  d3e  the submit has an empty target (`target_table: ""`)
  d4   M is confirmed without an epoch, then submitted with its Target
       relation (known epoch), S, and `ignore_external_modifications=true`

Names carry a random suffix so a rerun never reuses hosted State history.

Usage: ambiguous_rebuilds.py PROBE [PROBE ...]   (or `all`)
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
from lag_matrix import Node, Source, now_ms, probe, report  # noqa: E402

RUN = f"{time.strftime('%H%M%S')}{uuid.uuid4().hex[:6]}"


def model(probe_id):
    s = Source(probe_id, "s")
    s.name = f"amb{RUN}_{probe_id}_s"
    m = Node(probe_id, "m", [s], tol=0)
    m.name = f"amb{RUN}_{probe_id}_m"
    return s, m


def submit(request):
    resp = probe("submit", request)["response"]
    ((variant, body),) = resp.items()
    return variant, body


def built(probe_id, epoch=True):
    """M over S, built and confirmed (at the current time, or without an
    epoch)."""
    s, m = model(probe_id)
    v, b = submit(m.request())
    report(probe_id, "build", m, v, b)
    assert v == "ready_to_execute", f"{m.name}: expected a first build, got {v}"
    m.confirm(b["request_id"], now_ms() if epoch else None)
    time.sleep(2)
    return s, m


def ambiguous(probe_id, m, request, **facts):
    v, b = submit(request)
    report(probe_id, "ambiguous", m, v, b, explain=True, **facts)


def probe_d1():
    _, m = built("d1")
    req = m.request()
    own = {"name": m.fqn, "last_modified_epoch": m.epoch}
    req["tables"] = [own, dict(own)] + req["tables"][1:]
    ambiguous("d1", m, req)


def probe_d2():
    _, m = built("d2")
    req = m.request()
    req["tables"][0]["last_modified_epoch"] = None
    ambiguous("d2", m, req)


def probe_d3():
    _, m = built("d3")
    req = m.request()
    req["target_table"] = None
    ambiguous("d3", m, req)


def probe_d3e():
    _, m = built("d3e")
    req = m.request()
    req["target_table"] = ""
    ambiguous("d3e", m, req)


def probe_d4():
    _, m = built("d4", epoch=False)
    m.epoch = now_ms() - 1000  # the table exists, at a known epoch
    req = m.request()
    req["ignore_external_modifications"] = True
    ambiguous("d4", m, req)


PROBES = {k[6:]: v for k, v in globals().items() if k.startswith("probe_")}

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
