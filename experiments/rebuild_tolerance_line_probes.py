#!/usr/bin/env python3
"""Crafted probes: which data-only rebuilds past a non-zero Lag tolerance
carry the `exceeds lag tolerance` line (#104, 104a).

The live snapshot `fd_sn` (explain_freshness_cells.jsonl, run 2) rebuilt past
its 45 minute tolerance with an equivalent snapshot elsewhere (1 / 3) and its
SHAPE 9 tree ended with `the snapshot was last built … exceeds lag tolerance`.
The FULL rebuilds past a tolerance in lag_tolerance_matrix.jsonl (no
equivalent, 6 / 6) have no such line. These probes separate the node kind
from the equivalent. Each one builds X over a crafted source S with a 20 s
tolerance, waits 25 s, moves S's epoch, then resubmits X and explains it.
With `eq`, an equivalent Y (same SQL, another name, same kind) is built
after the move, so it is current when X is resubmitted.

  f0   FULL, no equivalent (control)
  s1   SNAPSHOT, no equivalent
  s2   SNAPSHOT, equivalent
  i1   INSERT_OVERWRITE, no equivalent
  i2   INSERT_OVERWRITE, equivalent

Nothing is created in the warehouse; epochs are wall-clock ms. Names carry
a random suffix. Calls go through `PROBE_URL` (your own recording proxy).

Usage: rebuild_tolerance_line_probes.py PROBE [PROBE ...]   (or `all`)
"""
import json
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lag_matrix as lm  # noqa: E402
from lag_matrix import Node, Source, now_ms, report  # noqa: E402
from lag_matrix import submit_request as submit  # noqa: E402

RUN = f"{time.strftime('%H%M%S')}{uuid.uuid4().hex[:6]}"
TOL = 20


class Kind(Node):
    """A crafted node of another execution type."""

    def __init__(self, probe_id, name, upstreams, execution_type):
        super().__init__(probe_id, name, upstreams, tol=TOL)
        self.name = f"rtl{RUN}_{probe_id}_{name}"
        self.execution_type = execution_type

    def request(self, **kw):
        req = super().request(**kw)
        req["execution_type"] = self.execution_type
        if self.execution_type == 7:
            req["dbt_node_state"]["resource_type"] = "snapshot"
        return req


def build(probe_id, node, step):
    v, b = submit(node.request())
    report(probe_id, step, node, v, b)
    assert v == "ready_to_execute", f"{node.name}: expected a build, got {v}"
    node.confirm(b["request_id"], now_ms())


def probe(probe_id, execution_type, equivalent):
    s = Source(probe_id, "s")
    s.name = f"rtl{RUN}_{probe_id}_s"
    x = Kind(probe_id, "x", [s], execution_type)
    build(probe_id, x, "build_x")
    time.sleep(TOL + 5)
    s.bump()
    if equivalent:
        y = Kind(probe_id, "y", [s], execution_type)
        build(probe_id, y, "build_equivalent_y")
        time.sleep(1)
    v, b = submit(x.request())
    report(probe_id, "resubmit_x", x, v, b, explain=True, tol=TOL,
           equivalent=equivalent, execution_type=execution_type)


PROBES = {
    "f0": lambda: probe("f0", 1, False),
    "s1": lambda: probe("s1", 7, False),
    "s2": lambda: probe("s2", 7, True),
    "i1": lambda: probe("i1", 4, False),
    "i2": lambda: probe("i2", 4, True),
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
