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

`e1-<T>` and `e4-<T>` send the same probe as execution type T (2 append,
4 insert_overwrite, 7 snapshot, 10 view, 11 custom, 8 data test), with
that kind's semantic extras; a data test has no Target relation and is
confirmed with complete results.

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
from lag_matrix import DB, SCHEMA, Node, Source, now_ms, probe, report  # noqa: E402
from lag_matrix import submit_request as submit  # noqa: E402

RUN = f"{time.strftime('%H%M%S')}{uuid.uuid4().hex[:6]}"


INCREMENTAL = {"lookback": "1", "on_schema_change": '"ignore"'}
EXTRAS = {
    1: INCREMENTAL,
    2: INCREMENTAL,
    4: INCREMENTAL,
    7: {"unique_key": '"id"'},
    8: {"error_if": '"!= 0"', "severity": '"ERROR"', "fail_calc": '"count(*)"',
        "warn_if": '"!= 0"'},
    10: {"lookback": "1"},
    11: {"lookback": "1"},
}
COMPLETE_TEST_RESULTS = {"fields": {
    "failures": {"kind": {"int_value": 0}},
    "should_warn": {"kind": {"bool_value": False}},
    "should_error": {"kind": {"bool_value": False}},
}}


class Probed(Node):
    """A crafted node of execution type `kind`."""

    def __init__(self, arm, name, upstreams, kind):
        super().__init__(arm, name, upstreams, tol=0)
        self.kind = kind

    def request(self, **kw):
        req = super().request(**kw)
        req["execution_type"] = self.kind
        req["semantic_extras"] = dict(EXTRAS[self.kind])
        state = req["dbt_node_state"]
        if self.kind == 7:
            state["resource_type"] = "snapshot"
        elif self.kind == 8:
            uid = f"test.jaffle_shop.{self.name}"
            req["target_table"] = None
            req["tables"] = [t for t in req["tables"] if t["name"] != self.fqn]
            req["labels"]["dbt_node_unique_id"] = state["node_unique_id"] = uid
            state["resource_type"] = "test"
            state["node_configs_hash"] = None
            state["node_contract_hash"] = None
        return req

    def confirm(self, request_id, epoch):
        if self.kind != 8:
            return super().confirm(request_id, epoch)
        r = probe("confirm", {
            "request_id": request_id,
            "last_modified_epoch": None,
            "failed_to_clone": False,
            "table_type": None,
            "execution_results": COMPLETE_TEST_RESULTS,
            "execution_runtime_ms": 1000,
            "labels": {"dbt_node_name": self.name},
        })
        if not r.get("success"):
            raise SystemExit(f"confirm failed for {self.name}: {r}")


def model(probe_id, kind=1):
    v = Source(probe_id, "v")
    v.name = f"mal{RUN}_{probe_id}_v".replace("-", "_")
    m = Probed(probe_id, "m", [v], kind)
    m.name = f"mal{RUN}_{probe_id}_m".replace("-", "_")
    return v, m


def dependency(v):
    return {
        "name": v.fqn,
        "query": f"create or replace view {v.name.upper()} as (select 1 as id)",
        "default_catalog": DB,
        "default_schema": SCHEMA,
    }


def malformed(probe_id, shape, kind=1):
    """Build M with the malformed request `shape(request, v)`, then resubmit it."""
    v, m = model(probe_id, kind)
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
for _kind in (2, 4, 7, 8, 10, 11):
    PROBES[f"e1-{_kind}"] = lambda k=_kind: malformed(f"e1-{k}", empty_sql, k)
    PROBES[f"e4-{_kind}"] = lambda k=_kind: malformed(f"e4-{k}", duplicated_dependency, k)

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
