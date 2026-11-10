#!/usr/bin/env python3
"""Which build hosted reuses or copies when a target's latest build no longer
qualifies, and the rejection codes it reports. Crafted calls through the
recording proxy, like lag_matrix.py (whose helpers this imports).

Hypothesis under test: a build is matched on its logic AND the producers of
its upstreams; hosted searches the target's own history and equivalent
targets for a match that also passes the requesting node's freshness policy.

Usage: copy_candidates.py ARM [ARM ...]   (or `all`)
"""
import sys
import time

import lag_matrix as lm
from lag_matrix import ALL, ANY, Node, external_bump, now_ms, report


def build(node, **kw):
    """Submit; confirm an execute or a copy at the current time."""
    v, b = node.submit(**kw)
    if v in ("ready_to_execute", "ready_to_clone"):
        node.confirm(b["request_id"], now_ms())
    return v, b


def rebuild_upstream(u):
    """dbt rebuilds a leaf upstream with the same logic: its table changed."""
    u.epoch = now_ms() + 1  # own target modified -> it executes
    v, b = u.submit()
    assert v == "ready_to_execute", v
    u.confirm(b["request_id"], now_ms() + 2)


def pair(arm, tol=0, policy=ANY, n_up=1):
    """Leaf upstreams; M built; M2 (same SQL, another name) created after it,
    which the cross-name fallback copies from M."""
    ups = [Node(arm, f"u{i}") for i in range(n_up)]
    for u in ups:
        assert build(u)[0] == "ready_to_execute"
    m = Node(arm, "m", ups, tol=tol, policy=policy)
    assert build(m)[0] == "ready_to_execute"
    m2 = Node(arm, "m2", ups, tol=tol, policy=policy)
    v, _ = build(m2)
    report(arm, "m2_created", m2, v, {})
    return ups, m, m2


def arm_fresh_equivalent():
    """M2 stale by data (ANY, tol 0) while equivalent M was rebuilt fresh."""
    (u,), m, m2 = pair("fe")
    time.sleep(2)
    rebuild_upstream(u)
    v, b = build(m)
    report("fresh_equivalent", "m_rebuilt", m, v, b)
    time.sleep(1)
    v, b = m2.submit()
    report("fresh_equivalent", "m2", m2, v, b, explain=True)


def arm_stale_equivalent_within_tolerance():
    """soeall replica under ANY with a 600s tolerance: M2 consumed U0 while
    externally modified; U0 then rebuilt by dbt; M consumed U0's original
    build and is young."""
    ups = [Node("st", f"u{i}") for i in range(2)]
    for u in ups:
        build(u)
    m = Node("st", "m", ups, tol=600)
    build(m)
    time.sleep(1)
    external_bump(ups[0])
    m2 = Node("st", "m2", ups, tol=600)
    v, b = build(m2)
    report("stale_equivalent", "m2_created", m2, v, b)
    time.sleep(1)
    rebuild_upstream(ups[0])
    time.sleep(1)
    v, b = m2.submit()
    report("stale_equivalent", "m2", m2, v, b, explain=True)


def arm_older_own_build():
    """M2 alone, two builds: b1 consumed U0's build, b2 consumed U0 externally
    modified; U0 is then rebuilt by dbt. Under ALL (U1 unchanged) b1's lineage
    matches again and is tolerated; under ANY it is stale."""
    for label, policy in (("all", ALL), ("any", ANY)):
        ups = [Node(f"ob{label}", f"u{i}") for i in range(2)]
        for u in ups:
            build(u)
        m2 = Node(f"ob{label}", "m2", ups, policy=policy)
        build(m2)
        time.sleep(1)
        external_bump(ups[0])
        v, b = build(m2)
        report("older_own_build", f"{label}_b2", m2, v, b)
        time.sleep(1)
        rebuild_upstream(ups[0])
        time.sleep(1)
        v, b = m2.submit()
        report("older_own_build", label, m2, v, b, explain=True)


def arm_dropped_target():
    """A built target is dropped (absent from tables); resubmitted with the
    same logic, then (after a rebuild) with changed logic while dropped."""
    m = Node("dt", "m")
    build(m)
    m.epoch = None
    v, b = m.submit()
    report("dropped_target", "same_logic", m, v, b, explain=True)
    m.confirm(b["request_id"], now_ms())
    m.epoch = None
    base = m.sql
    m.sql = lambda: base() + ", 2 as extra"
    v, b = m.submit()
    report("dropped_target", "changed_logic", m, v, b, explain=True)


def arm_dropped_with_equivalent():
    """Target dropped while an equivalent under another name exists."""
    (u,), m, m2 = pair("de")
    m2.epoch = None
    v, b = m2.submit()
    report("dropped_with_equivalent", "m2", m2, v, b, explain=True)


def arm_dropped_with_stale_equivalent():
    """Target dropped while the equivalent M (same upstream) is stale: U0
    rebuilt by dbt after M's build. ANY, tolerance 0, then 600s. Is the
    equivalent judged when there is no own build (copy or execute)?"""
    for label, tol in (("t0", 0), ("t600", 600)):
        (u,), m, m2 = pair(f"dse{label}", tol=tol)
        time.sleep(2)
        rebuild_upstream(u)
        time.sleep(1)
        m2.epoch = None
        v, b = m2.submit()
        report("dropped_with_stale_equivalent", label, m2, v, b, explain=True)


def arm_dropped_direct_with_stale_copy():
    """As dropped_with_stale_equivalent t0, but M2 was BUILT (not copied) and
    M copied from it: do the 4/7 codes of a dropped target depend on whether
    its latest row is a copy?"""
    (u,) = [Node("ddc", "u0")]
    build(u)
    m2 = Node("ddc", "m2", [u], tol=0)
    assert build(m2)[0] == "ready_to_execute"
    m = Node("ddc", "m", [u], tol=0)
    v, b = build(m)
    report("dropped_direct_with_stale_copy", "m_created", m, v, b)
    time.sleep(2)
    rebuild_upstream(u)
    time.sleep(1)
    m2.epoch = None
    v, b = m2.submit()
    report("dropped_direct_with_stale_copy", "m2", m2, v, b, explain=True)


def arm_older_build_not_reusable():
    """As older_own_build, but ANY with tolerance 0, so the older matching
    build is itself stale. 8/7, or 6/6?"""
    ups = [Node("obn", f"u{i}") for i in range(2)]
    for u in ups:
        build(u)
    m2 = Node("obn", "m2", ups, tol=0, policy=ANY)
    build(m2)
    time.sleep(1)
    external_bump(ups[0])
    build(m2)
    time.sleep(1)
    rebuild_upstream(ups[0])
    time.sleep(1)
    v, b = m2.submit()
    report("older_build_not_reusable", "m2", m2, v, b, explain=True)


def arm_older_build_fresh():
    """The older matching build is fresh: b1 consumed U0's build; b2 consumed
    U0 externally modified; U0's table is then restored to b1's epoch (as if
    the external write were undone). Codes and is_stale?"""
    (u,) = [Node("obf", "u0")]
    build(u)
    built_u = u.epoch
    m2 = Node("obf", "m2", [u], tol=0)
    build(m2)
    time.sleep(1)
    external_bump(u)
    build(m2)
    time.sleep(1)
    u.epoch = built_u
    v, b = m2.submit()
    report("older_build_fresh", "m2", m2, v, b, explain=True)


def arm_own_modified_with_equivalent():
    """M2's own table modified externally while equivalent M is current."""
    (u,), m, m2 = pair("ome")
    time.sleep(1)
    m2.epoch = now_ms()
    v, b = m2.submit()
    report("own_modified_with_equivalent", "m2", m2, v, b, explain=True)


def arm_whose_build_age():
    """M2 built first (old); M copied from it later (young); U0 then rebuilt.
    ANY, tolerance 20s, M2 older than 20s, M younger: copy from M (the
    equivalent's age counts) or execute (the requesting node's age counts)?"""
    (u,) = [Node("wba", "u0")]
    build(u)
    m2 = Node("wba", "m2", [u], tol=20)
    build(m2)
    time.sleep(22)
    m = Node("wba", "m", [u], tol=20)
    v, b = build(m)
    report("whose_build_age", "m_created", m, v, b)
    time.sleep(1)
    rebuild_upstream(u)
    time.sleep(1)
    v, b = m2.submit()
    report("whose_build_age", "m2", m2, v, b, explain=True)


def arm_equivalent_stale_beyond():
    """Own M2 stale beyond tolerance and the equivalent M stale too (both old,
    ANY, tolerance 20s): execute with which codes?"""
    (u,), m, m2 = pair("esb", tol=20)
    time.sleep(22)
    rebuild_upstream(u)
    time.sleep(1)
    v, b = m2.submit()
    report("equivalent_stale_beyond", "m2", m2, v, b, explain=True)


ARMS = {k[4:]: v for k, v in globals().items() if k.startswith("arm_")}

if __name__ == "__main__":
    lm.RUN = time.strftime("%H%M%S")
    names = list(ARMS) if sys.argv[1:] in ([], ["all"]) else sys.argv[1:]
    for n in names:
        ARMS[n]()
