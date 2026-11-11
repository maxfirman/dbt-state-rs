#!/usr/bin/env python3
"""Crafted probes: an upstream whose epoch is unknown (#104, 104a).

Client 2.0.11 never sends an upstream without an epoch: it leaves a table
whose epoch it cannot read out of `tables` and skips the State submit
(`metadata_complete = false`, dbt-fusion
`crates/dbt-tasks-core/src/run_cache/run_cache_service.rs`,
`collect_table_modified_infos`). So each probe is a crafted `state-probe`
request against fresh, never-created names in CDO_DEMO_DB.PROD_STATE_DEMO. No
warehouse is touched; epochs are made up (Unix ms, wall-clock based). Every
call goes through whatever `PROBE_URL` points at: the recording proxy for a
hosted capture, or a local server for a dry run.

Each probe builds and confirms a FULL model M (default Lag tolerance 2700 s),
waits two seconds, resubmits it with the same identity, then asks Explain for
that decision.

  u1  M over S, built with S's epoch; resubmitted with S unknown (`null`)
  u2  M over S, built with S unknown; resubmitted with S's epoch
  u3  M over S and T, ALL policy, built with both epochs; resubmitted with S
      unknown and T unchanged
  u4  M over S, built and resubmitted with S unknown

Names carry a random suffix so a rerun never reuses hosted State history.

Usage: unknown_epoch_probes.py PROBE [PROBE ...]   (or `all`)
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
from lag_matrix import ALL, Node, Source, now_ms, report  # noqa: E402
from lag_matrix import submit_request as submit  # noqa: E402

RUN = f"{time.strftime('%H%M%S')}{uuid.uuid4().hex[:6]}"


def model(probe_id, n_up=1, policy=lm.ANY):
    ups = []
    for i in range(n_up):
        s = Source(probe_id, f"s{i}")
        s.name = f"unk{RUN}_{probe_id}_s{i}"
        ups.append(s)
    m = Node(probe_id, "m", ups, policy=policy)
    m.name = f"unk{RUN}_{probe_id}_m"
    return ups, m


def build(probe_id, m, upstream_epochs=None):
    v, b = submit(m.request(upstream_epochs=upstream_epochs))
    report(probe_id, "build", m, v, b)
    assert v == "ready_to_execute", f"{m.name}: expected a first build, got {v}"
    m.confirm(b["request_id"], now_ms())
    time.sleep(2)


def resubmit(probe_id, m, upstream_epochs=None, **facts):
    v, b = submit(m.request(upstream_epochs=upstream_epochs))
    report(probe_id, "resubmit", m, v, b, explain=True, **facts)


def probe_u1():
    (s,), m = model("u1")
    build("u1", m)
    resubmit("u1", m, {s.name: None}, current="unknown", recorded="known")


def probe_u2():
    (s,), m = model("u2")
    build("u2", m, {s.name: None})
    resubmit("u2", m, current="known", recorded="unknown")


def probe_u3():
    (s, t), m = model("u3", n_up=2, policy=ALL)
    build("u3", m)
    resubmit("u3", m, {s.name: None}, current="s unknown, t unchanged", policy="ALL")


def probe_u4():
    (s,), m = model("u4")
    build("u4", m, {s.name: None})
    resubmit("u4", m, {s.name: None}, current="unknown", recorded="unknown")


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
