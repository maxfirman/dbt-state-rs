#!/usr/bin/env python3
"""Crafted probes for rebuilds over a RegisterClone copy (#104, 104c).

Client 2.0.11 sends `Clone.RegisterClone` only for incremental models and
snapshots (the dev-clone pre-step,
`run_cache_dev_clone.rs::dev_clone_candidate_for_node`). A FULL submit lands
on such a copy only after the node's materialization changes to `table`. The
live campaign `explain_capture.py copy_logic_mismatch` covers that change with
unchanged SQL; these probes add a FULL submit whose logic also changed, which
this server records as `no_reuse_candidate.copy_logic_mismatch`.

Each probe is a crafted `state-probe` sequence against fresh, never-created
names in CDO_DEMO_DB (no warehouse is touched; epochs are made up, Unix ms,
wall-clock based), sent to whatever `PROBE_URL` points at:

  1. a model P in PROD_STATE_DEMO (an append incremental unless stated, over
     one source S unless stated) is built and confirmed;
  2. RegisterClone copies P into D in DEV_CLONE_DEMO (a copy of P's kind),
     and the copy is confirmed as the client does;
  3. D is submitted as described below, then Explain is asked for it.

  x1   D as a table (FULL) with other logic (`where id >= 1`)
  x2   as x1, P and D without upstreams
  x3   D as an append incremental with other logic, without upstreams
  x4   D as a view (VIEW, a view's semantic extras), same SQL, P without
       upstreams
  x5   P a snapshot, D the snapshot with other logic, without upstreams
  x6-x9  as x3 for merge, insert_overwrite, delete+insert and microbatch
       (P and D of that strategy)
  x10  D as a custom node (DBT_CUSTOM, a custom node's semantic extras),
       same SQL, P without upstreams
  x11  D as an append incremental with other logic, its Target relation
       not reported (dropped after the copy)
  x12  as x11, D as a table (FULL)
  x13-x16  as x11 for merge, insert_overwrite, delete+insert and microbatch
       (P and D of that strategy)
  x17  as x11, P a snapshot and D the snapshot
  x18  D as a view (VIEW) with other logic over a dropped copy, P without
       upstreams (a view reports none)
  x19  as x18, D as a custom node (DBT_CUSTOM)

Usage: copy_logic_probes.py PROBE [PROBE ...]   (or `all`)
Prints one JSON line per step; a failed RPC prints an `error` row and the
remaining probes still run.
"""
import json
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lag_matrix as lm  # noqa: E402
from lag_matrix import DB, Node, Source, now_ms, probe, report  # noqa: E402
from lag_matrix import submit_request as submit  # noqa: E402

RUN = f"{time.strftime('%H%M%S')}{uuid.uuid4().hex[:6]}"
APPEND, FULL = 2, 1
DEV = "DEV_CLONE_DEMO"


def dev_fqn(name):
    return f'"{DB}"."{DEV}"."{name.upper()}"'


MERGE, INSERT_OVERWRITE, DELETE_INSERT, MICROBATCH = 3, 4, 5, 6
SNAPSHOT, VIEW, CUSTOM = 7, 10, 11
INCREMENTAL = {"lookback": "1", "on_schema_change": '"ignore"'}
UNIQUE_KEY = dict(INCREMENTAL, unique_key='"id"')
EXTRAS = {
    MERGE: UNIQUE_KEY,
    DELETE_INSERT: UNIQUE_KEY,
    MICROBATCH: dict(INCREMENTAL, event_time='"ordered_at"',
                     __microbatch_event_time_start="2026-11-01T00:00:00+00:00",
                     __microbatch_event_time_end="2026-11-03T00:00:00+00:00"),
    SNAPSHOT: {"unique_key": '"id"'},
    VIEW: {"lookback": "1"},
    CUSTOM: {"lookback": "1"},
}


def copied(probe_id, upstream=True, kind=APPEND):
    """P built in prod and copied into D by RegisterClone; returns (p, d_request)."""
    ups = []
    if upstream:
        s = Source(probe_id, "s")
        s.name = f"cpl{RUN}_{probe_id}_s"
        ups = [s]
    p = Node(probe_id, "m", ups)
    p.name = f"cpl{RUN}_{probe_id}_m"
    req = shaped(p.request(), kind)
    variant, body = submit(req)
    report(probe_id, "prod build", p, variant, body)
    if variant != "ready_to_execute":
        raise SystemExit(f"{p.name}: expected a first build, got {variant}")
    p.confirm(body["request_id"], now_ms())
    time.sleep(2)

    labels = req["labels"]
    clone = probe("clone", {
        "target_table": dev_fqn(p.name),
        "dialect": "snowflake",
        "default_catalog": DB,
        "execution_type": kind,
        "clone_source_table": p.fqn,
        "clone_source_last_modified_epoch": p.epoch,
        "labels": labels,
        "clone_source_table_type": "TRANSIENT TABLE",
        "table_properties": None,
        "clone_chain_depth_limit": None,
        "table_namespace": lm.NAMESPACE,
    })
    ready = clone.get("ready_to_clone")
    print(json.dumps({"arm": probe_id, "step": "register clone", "node": p.name,
                      "variant": "ready_to_clone" if ready else "unable_to_clone",
                      "response": clone}), flush=True)
    if not ready:
        raise SystemExit(f"{p.name}: RegisterClone refused")
    copy_epoch = now_ms()
    r = probe("confirm", {
        "request_id": ready["request_id"],
        "last_modified_epoch": copy_epoch,
        "failed_to_clone": False,
        "table_type": "TRANSIENT TABLE",
        "execution_results": {"fields": {}},
        "execution_runtime_ms": None,
        "labels": labels,
    })
    if not r.get("success"):
        raise SystemExit(f"copy confirm failed for {p.name}: {r}")
    time.sleep(2)

    dev = shaped(p.request(), kind)
    dev["target_table"] = dev_fqn(p.name)
    dev["tables"] = [{"name": dev_fqn(p.name), "last_modified_epoch": copy_epoch}] + [
        t for t in dev["tables"] if t["name"] != p.fqn]
    dev["default_schema"] = DEV.lower()
    dev["dbt_node_state"]["target_name"] = "dev_clone"
    dev["defer_enabled"] = True
    dev["allow_clones"] = True
    p.epoch = copy_epoch
    return p, dev


def shaped(req, kind):
    """`req` as the client sends `kind` (semantic extras, Lag tolerance, resource type)."""
    req["execution_type"] = kind
    req["semantic_extras"] = dict(EXTRAS.get(kind, INCREMENTAL))
    if kind == SNAPSHOT:
        req["dbt_node_state"]["resource_type"] = "snapshot"
    elif kind == VIEW:
        req["freshness_tolerance_seconds"] = 0
    return req


def other_logic(dev):
    dev["sql"] += " where 1 = 1" if "where" not in dev["sql"] else " and 1 = 1"


def explain_rebuild(probe_id, step, p, dev):
    variant, body = submit(dev)
    report(probe_id, step, p, variant, body, explain=True)


def probe_x1():
    p, dev = copied("x1")
    shaped(dev, FULL)
    dev["sql"] += " where id >= 1"
    explain_rebuild("x1", "FULL over the APPEND copy, other logic", p, dev)


def probe_x2():
    p, dev = copied("x2", upstream=False)
    shaped(dev, FULL)
    other_logic(dev)
    explain_rebuild("x2", "FULL over the APPEND copy, other logic, no upstreams", p, dev)


def probe_x3():
    p, dev = copied("x3", upstream=False)
    other_logic(dev)
    explain_rebuild("x3", "APPEND over the APPEND copy, other logic, no upstreams", p, dev)


def probe_x4():
    p, dev = copied("x4", upstream=False)
    shaped(dev, VIEW)
    explain_rebuild("x4", "VIEW over the APPEND copy, same SQL, no upstreams", p, dev)


def probe_x5():
    p, dev = copied("x5", upstream=False, kind=SNAPSHOT)
    other_logic(dev)
    explain_rebuild("x5", "SNAPSHOT over the SNAPSHOT copy, other logic, no upstreams", p, dev)


def strategy_probe(probe_id, kind):
    def run():
        p, dev = copied(probe_id, upstream=False, kind=kind)
        other_logic(dev)
        explain_rebuild(probe_id, f"execution type {kind} over its copy, other logic, "
                        "no upstreams", p, dev)
    return run


def probe_x10():
    p, dev = copied("x10", upstream=False)
    shaped(dev, CUSTOM)
    explain_rebuild("x10", "DBT_CUSTOM over the APPEND copy, same SQL, no upstreams", p, dev)


def dropped_probe(probe_id, kind, copy_kind=APPEND, upstream=True):
    def run():
        p, dev = copied(probe_id, upstream=upstream, kind=copy_kind)
        shaped(dev, kind)
        other_logic(dev)
        dev["tables"] = [t for t in dev["tables"] if t["name"] != dev["target_table"]]
        explain_rebuild(probe_id, f"execution type {kind} over a dropped copy, other logic",
                        p, dev)
    return run


PROBES = {k[6:]: v for k, v in globals().items() if k.startswith("probe_")}
PROBES.update({"x11": dropped_probe("x11", APPEND), "x12": dropped_probe("x12", FULL)})
PROBES.update({f"x{i}": dropped_probe(f"x{i}", kind, copy_kind=kind)
               for i, kind in ((13, MERGE), (14, INSERT_OVERWRITE), (15, DELETE_INSERT),
                               (16, MICROBATCH), (17, SNAPSHOT))})
PROBES.update({"x18": dropped_probe("x18", VIEW, upstream=False),
               "x19": dropped_probe("x19", CUSTOM, upstream=False)})
PROBES.update({f"x{i}": strategy_probe(f"x{i}", kind)
               for i, kind in ((6, MERGE), (7, INSERT_OVERWRITE), (8, DELETE_INSERT),
                               (9, MICROBATCH))})

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
