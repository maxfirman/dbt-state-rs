#!/usr/bin/env python3
"""Crafted-gRPC lag_tolerance experiment matrix against the hosted dbt State.

Sends synthetic SubmitEnrichedSQL / ConfirmExecution / GetExplainMessages calls
through the recording proxy (experiments/proxy_up.sh) with
`target/debug/state-probe`, so every exchange is captured in the golden log.
No warehouse is touched: the requests name fresh, never-created relations in
CDO_DEMO_DB.PROD_STATE_DEMO and carry made-up epochs (Unix ms, wall-clock
based), so the warehouse-clock skew of the real client is absent.

Each ARM varies how an upstream change arises:
  recorded build  : the upstream node U is itself submitted and confirmed at
                    its new epoch (what `dbt build` of U does);
  external        : U's table epoch advances with no U submit (an external
                    modification);
  source          : the upstream is a source (no node, never submitted);
and who has seen the new epoch before the decisive submit.

Usage: lag_matrix.py ARM [ARM ...]   (or `all`)
Prints one JSON line per decisive step; exits nonzero on an RPC failure.
"""
import hashlib
import json
import os
import subprocess
import sys
import time
import uuid

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROBE = os.path.join(REPO, "target", "debug", "state-probe")
DB, SCHEMA = "CDO_DEMO_DB", "PROD_STATE_DEMO"
NAMESPACE = "fc38a5ba0eceb701c6ab7c6b882e28b0"  # prod_demo connection namespace
RUN = time.strftime("%H%M%S")
INVOCATION = str(uuid.uuid4())

ANY, ALL = 0, 1


def now_ms():
    return int(time.time() * 1000)


def fqn(name):
    return f'"{DB}"."{SCHEMA}"."{name.upper()}"'


def probe(op, body, shift_ms=0):
    env = dict(os.environ, PROBE_INVOCATION=INVOCATION, PROBE_SESSION_ID=INVOCATION)
    if shift_ms:
        env["PROBE_SUBMITTED_SHIFT_MS"] = str(shift_ms)
    out = subprocess.run(
        [PROBE, op], input=json.dumps(body), capture_output=True, text=True, env=env
    )
    if out.returncode != 0:
        raise SystemExit(f"state-probe {op} failed: {out.stderr.strip()}")
    return json.loads(out.stdout)


def submit_request(request, shift_ms=0):
    """Send a crafted SubmitEnrichedSQL; (variant, payload) of the answer."""
    ((variant, body),) = probe("submit", request, shift_ms)["response"].items()
    return variant, body


def h(s):
    return hashlib.md5(s.encode()).hexdigest()


class Node:
    """A crafted dbt model node. `upstreams` are Node or Source objects."""

    def __init__(self, arm, name, upstreams=(), tol=2700, policy=ANY):
        self.name = f"ltx{RUN}_{arm}_{name}".lower()
        self.upstreams = list(upstreams)
        self.tol, self.policy = tol, policy
        self.epoch = None  # own table epoch (None: does not exist)

    @property
    def fqn(self):
        return fqn(self.name)

    def sql(self):
        if not self.upstreams:
            return f"\nselect 1 as id, '{self.name}' as val"
        parts = [f"{DB}.prod_state_demo.{u.name} u{i}" for i, u in enumerate(self.upstreams)]
        return "\nselect u0.id from " + " cross join ".join(parts)

    def request(self, upstream_epochs=None, tol=None, policy=None):
        tables = []
        if self.epoch is not None:
            tables.append({"name": self.fqn, "last_modified_epoch": self.epoch})
        for u in self.upstreams:
            e = (upstream_epochs or {}).get(u.name, u.epoch)
            tables.append({"name": u.fqn, "last_modified_epoch": e})
        uid = f"model.jaffle_shop.{self.name}"
        return {
            "target_table": self.fqn,
            "dialect": "snowflake",
            "default_catalog": DB,
            "execution_type": 1,
            "sql": self.sql(),
            "tables": tables,
            "query_dependencies": [],
            "semantic_extras": {"lookback": "1", "on_schema_change": '"ignore"'},
            "freshness_tolerance_seconds": self.tol if tol is None else tol,
            "lenient_dependencies": [],
            "tolerate_nondeterminism": True,
            "labels": {
                "dbt_node_fqn": f"jaffle_shop.marts.{self.name}",
                "dbt_node_name": self.name,
                "dbt_node_unique_id": uid,
            },
            "clone_time_travel_limit": None,
            "clone_table_properties": None,
            "stale_upstream_policy": self.policy if policy is None else policy,
            "clone_chain_depth_limit": None,
            "dbt_node_state": {
                "node_unique_id": uid,
                "target_name": "prod_demo",
                "project_name": "jaffle_shop",
                "resource_type": "model",
                "node_hash": h("n" + self.name),
                "node_body_hash": h("b" + self.name),
                "node_configs_hash": h("c" + self.name),
                "node_persisted_descriptions_hash": None,
                "node_macros_hash": None,
                "node_contract_hash": "298a88779c630d09cfb26e926f3af91b",
                "profile_name": "snowflake",
                "project_id": "70437463667433",
                "node_database_representation": None,
            },
            "default_schema": "prod_state_demo",
            "compare_unrendered_code": False,
            "table_namespace": NAMESPACE,
            "ignore_external_modifications": False,
            "allow_clones": None,
            "is_defer_to_profile": False,
            "defer_enabled": False,
        }

    def submit(self, shift_ms=0, **kw):
        return submit_request(self.request(**kw), shift_ms)

    def confirm(self, request_id, epoch):
        r = probe(
            "confirm",
            {
                "request_id": request_id,
                "last_modified_epoch": epoch,
                "failed_to_clone": False,
                "table_type": "TRANSIENT TABLE",
                "execution_results": None,
                "execution_runtime_ms": 1000,
                "labels": {"dbt_node_name": self.name},
            },
        )
        if not r.get("success"):
            raise SystemExit(f"confirm failed for {self.name}: {r}")
        self.epoch = epoch

    def build(self):
        """Submit; if hosted says execute, confirm at the current wall time."""
        variant, body = self.submit()
        if variant == "ready_to_execute":
            self.confirm(body["request_id"], now_ms())
        return variant, body


class Source:
    """A dbt source: never submitted, only reported in downstream `tables`."""

    def __init__(self, arm, name):
        self.name = f"ltx{RUN}_{arm}_{name}".lower()
        self.epoch = now_ms() - 3_600_000

    @property
    def fqn(self):
        return fqn(self.name)

    def bump(self):
        self.epoch = now_ms()


def external_bump(node):
    """The upstream table changes outside dbt: its epoch advances, no submit."""
    node.epoch = now_ms()


def recorded_rebuild(node, source):
    """dbt rebuilds the upstream: its source changed, it executes and confirms."""
    source.bump()
    variant, _ = node.build()
    assert variant == "ready_to_execute", f"{node.name}: expected rebuild, got {variant}"


def report(arm, step, node, variant, body, explain=False, **facts):
    ed = body.get("explained_decision") or {}
    row = {
        "arm": arm,
        "step": step,
        "node": node.name,
        "variant": variant,
        "is_stale": ed.get("is_stale"),
        "srr": ed.get("skip_rejection_reason"),
        "crr": ed.get("clone_rejection_reason"),
        "description": ed.get("decision_description"),
        "build_age_s": round((now_ms() - node.epoch) / 1000, 1) if node.epoch else None,
        **facts,
    }
    if explain and body.get("execution_decision_id"):
        time.sleep(2)
        row["explain"] = probe(
            "explain", {"execution_decision_ids": [body["execution_decision_id"]]}
        )
    print(json.dumps(row), flush=True)
    return row


def fresh_chain(arm, tol, policy=ANY, n_up=1):
    """Sources S_i -> models U_i -> model M, all built and confirmed now."""
    sources = [Source(arm, f"s{i}") for i in range(n_up)]
    # tol 0: a source change must rebuild U (a recorded build of U).
    ups = [Node(arm, f"u{i}", [s], tol=0) for i, s in enumerate(sources)]
    for u in ups:
        v, _ = u.build()
        assert v == "ready_to_execute"
    m = Node(arm, "m", ups, tol=tol, policy=policy)
    v, _ = m.build()
    assert v == "ready_to_execute"
    return sources, ups, m


# ---- arms --------------------------------------------------------------------


def arm_rec_young():
    """Recorded build of U, M's build younger than the tolerance (600s)."""
    (s,), (u,), m = fresh_chain("recy", 600)
    time.sleep(5)
    recorded_rebuild(u, s)
    time.sleep(2)
    v, b = m.submit()
    report("rec_young", "after_recorded_rebuild", m, v, b, explain=True, tol=600)


def arm_rec_old():
    """Recorded build of U, M's build older than the tolerance (20s)."""
    (s,), (u,), m = fresh_chain("reco", 20)
    time.sleep(30)
    recorded_rebuild(u, s)
    time.sleep(2)
    v, b = m.submit()
    report("rec_old", "after_recorded_rebuild", m, v, b, explain=True, tol=20)


def arm_rec_buildold_upyoung():
    """tol 20s. U rebuilt (recorded) 25s after M's build, probe 1s later:
    build age ~26s > tol, upstream change age ~1s < tol."""
    (s,), (u,), m = fresh_chain("recx", 20)
    time.sleep(25)
    recorded_rebuild(u, s)
    v, b = m.submit()
    report("rec_buildold_upyoung", "probe", m, v, b, tol=20)


def arm_rec_buildyoung_upold():
    """Build age < tol but the recorded upstream change is older than tol:
    tol 40s. U rebuilt 2s after M's build; probe at ~30s (build age 30 < 40,
    upstream age 28 < 40) and at ~50s (both > 40)."""
    (s,), (u,), m = fresh_chain("recz", 40)
    time.sleep(2)
    recorded_rebuild(u, s)
    time.sleep(28)
    v, b = m.submit()
    report("rec_t30", "probe_30s", m, v, b, tol=40)
    if v == "skip_execution":
        time.sleep(20)
        v, b = m.submit()
        report("rec_t50", "probe_50s", m, v, b, tol=40)


def arm_ext_young():
    """External modification of U, M young (600s): control (expect EXECUTE)."""
    _, (u,), m = fresh_chain("exty", 600)
    time.sleep(5)
    external_bump(u)
    time.sleep(2)
    v, b = m.submit()
    report("ext_young", "after_external", m, v, b, explain=True, tol=600)


def arm_ext_seen_by_upstream_unconfirmed():
    """External modification of U, then U is SUBMITTED at the new epoch (it
    executes: its own target changed) but NOT confirmed; then M."""
    _, (u,), m = fresh_chain("extu", 600)
    time.sleep(5)
    external_bump(u)
    v, _ = u.submit()
    report("ext_u_unconfirmed", "u_submit", u, v, {})
    time.sleep(2)
    v, b = m.submit()
    report("ext_u_unconfirmed", "m_submit", m, v, b, tol=600)


def arm_ext_seen_by_self_all_then_any():
    """Two upstreams; external modification of U0 only. M submits under ALL
    (policy skip), then under ANY, same epochs, tolerance 600 throughout."""
    _, (u0, u1), m = fresh_chain("exts", 600, policy=ALL, n_up=2)
    time.sleep(5)
    external_bump(u0)
    v, b = m.submit(policy=ALL)
    report("ext_self", "all_submit", m, v, b, tol=600)
    time.sleep(2)
    v, b = m.submit(policy=ANY)
    report("ext_self", "any_submit", m, v, b, explain=True, tol=600)


def arm_ext_seen_by_self_all_then_any_tol0():
    """As ext_self but tolerance 0 throughout (separates tolerance from the
    per-node ledger)."""
    _, (u0, u1), m = fresh_chain("ext0", 0, policy=ALL, n_up=2)
    time.sleep(5)
    external_bump(u0)
    v, b = m.submit(policy=ALL)
    report("ext_self_tol0", "all_submit", m, v, b, tol=0)
    time.sleep(2)
    v, b = m.submit(policy=ANY)
    report("ext_self_tol0", "any_submit", m, v, b, tol=0)


def arm_ext_seen_by_other_node():
    """External modification of U; ANOTHER consumer M2 (two upstreams, ALL)
    submits at the new epoch first; then M (ANY, tol 600)."""
    sources, (u, w), m2 = fresh_chain("exto", 600, policy=ALL, n_up=2)
    m = Node("exto", "m1", [u], tol=600)
    v, _ = m.build()
    assert v == "ready_to_execute"
    time.sleep(5)
    external_bump(u)
    v, b = m2.submit()
    report("ext_other", "m2_all_submit", m2, v, b)
    time.sleep(2)
    v, b = m.submit()
    report("ext_other", "m_submit", m, v, b, tol=600)


def arm_source_young():
    """M reads a SOURCE directly; source changes; M young (600s)."""
    s = Source("srcy", "s")
    m = Node("srcy", "m", [s], tol=600)
    v, _ = m.build()
    assert v == "ready_to_execute"
    time.sleep(5)
    s.bump()
    time.sleep(2)
    v, b = m.submit()
    report("source_young", "after_source_change", m, v, b, explain=True, tol=600)


def arm_source_old():
    """M reads a SOURCE directly; M's build older than the tolerance (20s)."""
    s = Source("srco", "s")
    m = Node("srco", "m", [s], tol=20)
    v, _ = m.build()
    assert v == "ready_to_execute"
    time.sleep(30)
    s.bump()
    time.sleep(2)
    v, b = m.submit()
    report("source_old", "after_source_change", m, v, b, tol=20)


def arm_unconfirmed_execute():
    """ANY tol 0: external change -> M EXECUTE, not confirmed; resubmit."""
    _, (u,), m = fresh_chain("unc", 0)
    time.sleep(5)
    external_bump(u)
    v, b = m.submit()
    report("unconfirmed", "first", m, v, b, tol=0)
    time.sleep(2)
    v, b = m.submit()
    report("unconfirmed", "resubmit", m, v, b, tol=0)


def arm_all_alternating():
    """ALL tol 0, two upstreams changing one after the other externally."""
    _, (u0, u1), m = fresh_chain("alt", 0, policy=ALL, n_up=2)
    time.sleep(5)
    external_bump(u0)
    v, b = m.submit()
    report("all_alternating", "u0_changed", m, v, b, tol=0)
    time.sleep(5)
    external_bump(u1)
    v, b = m.submit()
    report("all_alternating", "u1_changed", m, v, b, tol=0)


def arm_all_alternating_recorded():
    """ALL tol 0, two upstreams rebuilt by dbt one after the other."""
    (s0, s1), (u0, u1), m = fresh_chain("altr", 0, policy=ALL, n_up=2)
    time.sleep(5)
    recorded_rebuild(u0, s0)
    v, b = m.submit()
    report("all_alt_recorded", "u0_rebuilt", m, v, b, tol=0)
    time.sleep(5)
    recorded_rebuild(u1, s1)
    v, b = m.submit()
    report("all_alt_recorded", "u1_rebuilt", m, v, b, tol=0)


def leaf_chain(arm, tol, policy=ANY, n_up=1):
    """Leaf models U_i (no upstreams, like the fixture's lp_up_a) -> M."""
    ups = [Node(arm, f"u{i}", tol=2700) for i in range(n_up)]
    for u in ups:
        v, _ = u.build()
        assert v == "ready_to_execute"
    m = Node(arm, "m", ups, tol=tol, policy=policy)
    v, _ = m.build()
    assert v == "ready_to_execute"
    return ups, m


def arm_leaf_self_external():
    """A leaf model whose own table is externally modified, resubmitted."""
    (u,), _ = leaf_chain("lfs", 600)
    time.sleep(5)
    external_bump(u)
    v, b = u.submit()
    report("leaf_self_external", "u_submit", u, v, b, explain=True)


def arm_leaf_all_then_any():
    """Fixture rows 47-59 replica: leaf upstreams, ALL tol 0, external bump
    of U0; M (ALL/0); U0 resubmitted; M (ANY/300)."""
    (u0, u1), m = leaf_chain("lfa", 0, policy=ALL, n_up=2)
    time.sleep(5)
    external_bump(u0)
    v, b = m.submit(policy=ALL, tol=0)
    report("leaf_all_any", "m_all_tol0", m, v, b, explain=True)
    v, b = u0.submit()
    report("leaf_all_any", "u0_submit", u0, v, b)
    time.sleep(2)
    v, b = m.submit(policy=ANY, tol=300)
    report("leaf_all_any", "m_any_tol300", m, v, b, explain=True)


def arm_leaf_any_external():
    """Leaf upstream externally modified, ANY tol 600, no U resubmit."""
    (u,), m = leaf_chain("lfx", 600)
    time.sleep(5)
    external_bump(u)
    v, b = m.submit()
    report("leaf_any_external", "m_submit", m, v, b, explain=True, tol=600)


def arm_leaf_any_external_then_upstream_submit():
    """Leaf upstream externally modified, then U resubmitted, then M (ANY 600)."""
    (u,), m = leaf_chain("lfu", 600)
    time.sleep(5)
    external_bump(u)
    v, b = u.submit()
    report("leaf_any_ext_u", "u_submit", u, v, b)
    time.sleep(2)
    v, b = m.submit()
    report("leaf_any_ext_u", "m_submit", m, v, b, explain=True, tol=600)


def arm_link_offset():
    """Does an external modification of U read as an upstream query change
    only when M's build recorded U at exactly U's confirmed build epoch?
    M is built reporting U at E+d for d in (0, -300ms, +3000ms); then U is
    externally modified; M submits ANY, tol 600, young."""
    for label, d in (("eq", 0), ("minus300", -300), ("plus3000", 3000)):
        u = Node(f"lk{label}", "u0")
        v, _ = u.build()
        assert v == "ready_to_execute"
        e = u.epoch
        m2 = Node(f"lk{label}", "m2", [u], tol=600)
        v, b = m2.submit(upstream_epochs={u.name: e + d})
        assert v == "ready_to_execute"
        m2.confirm(b["request_id"], now_ms())
        time.sleep(4)
        external_bump(u)
        v, b = m2.submit()
        report("link_offset", f"d={d}", m2, v, b, explain=True, tol=600)


def arm_own_target_slack():
    """Leaf U confirmed at E; its own table reported at E+k later."""
    for k in (1000, 2500, 4000, 10000):
        u = Node(f"ot{k}", "u")
        v, b = u.submit()
        e = now_ms()
        u.confirm(b["request_id"], e)
        u.epoch = e + k
        time.sleep(1)
        v, b = u.submit()
        report("own_target_slack", f"k={k}", u, v, b)


def arm_partial_all_old_build():
    """ALL tol 20: U0 rebuilt (recorded) after M's build is older than tol."""
    (s0, s1), (u0, u1), m = fresh_chain("pao", 20, policy=ALL, n_up=2)
    time.sleep(25)
    recorded_rebuild(u0, s0)
    v, b = m.submit()
    report("partial_all_old_build", "u0_rebuilt", m, v, b, explain=True, tol=20)


def arm_anchor_source():
    """Anchor: M over a source, confirmed at C = now; next submit reports M's
    own table at C-10s and the source at C-5s (after the real table, before
    the confirm stamp). ANY tol 0."""
    s = Source("anc", "s")
    m = Node("anc", "m", [s], tol=0)
    v, b = m.submit()
    c = now_ms()
    m.confirm(b["request_id"], c)
    m.epoch = c - 10_000
    s.epoch = c - 5_000
    time.sleep(1)
    v, b = m.submit()
    report("anchor_source", "own_lt_upstream_lt_confirm", m, v, b, explain=True)


def arm_build_age_clock():
    """Is build age measured from the confirm epoch or from receipt time?
    M over a source, tol 60, confirmed with an epoch back-dated by 100s
    (BACK) or forward-dated by 100s (FWD); then a source change."""
    for label, shift in (("back", -100_000), ("fwd", 100_000)):
        s = Source(f"bac{label}", "s")
        m = Node(f"bac{label}", "m", [s], tol=60)
        v, b = m.submit()
        m.confirm(b["request_id"], now_ms() + shift)
        time.sleep(2)
        s.epoch = max(now_ms(), m.epoch + 1000)
        v, b = m.submit()
        report("build_age_clock", label, m, v, b, explain=True, tol=60)


def arm_changed_during_build():
    """Source changes AFTER M's submit but BEFORE M's confirm stamp: recorded
    S=e1, current S=e2, confirm C>e2. ANY tol 0. Recorded-upstream anchor
    -> EXECUTE; own-build anchor -> SKIP."""
    s = Source("cdb", "s")
    m = Node("cdb", "m", [s], tol=0)
    v, b = m.submit()
    s.bump()
    time.sleep(1)
    m.confirm(b["request_id"], now_ms())
    time.sleep(1)
    v, b = m.submit()
    report("changed_during_build", "probe", m, v, b, explain=True, tol=0)


def arm_link_boundary():
    """M recorded U at E+d for small positive d, then U external change."""
    for d in (1, 500):
        u = Node(f"lb{d}", "u0")
        v, _ = u.build()
        e = u.epoch
        m = Node(f"lb{d}", "m", [u], tol=600)
        v, b = m.submit(upstream_epochs={u.name: e + d})
        assert v == "ready_to_execute"
        m.confirm(b["request_id"], now_ms())
        time.sleep(3)
        external_bump(u)
        v, b = m.submit()
        report("link_boundary", f"d=+{d}", m, v, b, tol=600)


def arm_own_target_learned_from_downstream():
    """Leaf U externally modified; downstream M submits (flagged, executes)
    and CONFIRMS; then U submits with that epoch."""
    (u,), m = leaf_chain("otl", 600)
    time.sleep(3)
    external_bump(u)
    v, b = m.submit()
    report("own_target_learned", "m_submit", m, v, b)
    m.confirm(b["request_id"], now_ms())
    time.sleep(1)
    v, b = u.submit()
    report("own_target_learned", "u_submit_after_m_confirm", u, v, b)


def arm_build_age_confirm_vs_own():
    """tol 60: M confirmed at C=now, but later reports its own table at
    C-100s. Source changes. BA from C -> SKIP; from own epoch -> EXECUTE."""
    s = Source("bao", "s")
    m = Node("bao", "m", [s], tol=60)
    v, b = m.submit()
    c = now_ms()
    m.confirm(b["request_id"], c)
    m.epoch = c - 100_000
    time.sleep(2)
    s.bump()
    v, b = m.submit()
    report("build_age_confirm_vs_own", "probe", m, v, b, explain=True, tol=60)


def arm_build_age_header_clock():
    """tol 60: M confirmed now; source changes; submit with
    x-submitted-at-epoch shifted +100s. Header clock -> EXECUTE."""
    s = Source("bah", "s")
    m = Node("bah", "m", [s], tol=60)
    v, b = m.submit()
    m.confirm(b["request_id"], now_ms())
    time.sleep(2)
    s.bump()
    v, b = m.submit(shift_ms=100_000)
    report("build_age_header_clock", "shift+100s", m, v, b, explain=True, tol=60)


def arm_unconfirmed_source_path():
    """ANY tol 0, source change -> M EXECUTE, NOT confirmed; resubmit."""
    s = Source("ucs", "s")
    m = Node("ucs", "m", [s], tol=0)
    v, _ = m.build()
    time.sleep(2)
    s.bump()
    v, b = m.submit()
    report("unconfirmed_source", "first", m, v, b)
    time.sleep(2)
    v, b = m.submit()
    report("unconfirmed_source", "resubmit", m, v, b)


def arm_descriptions():
    """Skip descriptions: fresh-but-decreased upstream epoch, and a tol-0 ALL
    partial skip with its explain tree."""
    for label, tol, wait in (("dec_t0", 0, 2), ("dec_young", 20, 2), ("dec_old", 20, 25)):
        s = Source(f"ds{label}", "s")
        m = Node(f"ds{label}", "m", [s], tol=tol)
        v, _ = m.build()
        time.sleep(wait)
        s.epoch -= 300
        v, b = m.submit()
        report("descriptions", label, m, v, b, explain=True, tol=tol)
    s0, s1 = Source("dsall", "s0"), Source("dsall", "s1")
    m = Node("dsall", "m", [s0, s1], tol=0, policy=ALL)
    v, _ = m.build()
    time.sleep(2)
    s0.bump()
    v, b = m.submit()
    report("descriptions", "all_partial_tol0", m, v, b, explain=True, tol=0)


def arm_stale_on_execute():
    """When does an EXECUTE report is_stale=true? Two leaf upstreams; U0 is
    consumed while externally modified (no producing build), then rebuilt by
    dbt (its own target changed -> it executes and confirms), U1 unchanged."""
    for label, policy in (("all", ALL), ("any", ANY)):
        (u0, u1), m = leaf_chain(f"soe{label}", 0, policy=policy, n_up=2)
        time.sleep(2)
        external_bump(u0)
        m2 = Node(f"soe{label}", "m2", [u0, u1], tol=0, policy=policy)
        v, b = m2.submit()
        m2.confirm(b["request_id"], now_ms())  # consumed U0 while external
        time.sleep(2)
        v, b = u0.submit()  # own target modified -> executes
        assert v == "ready_to_execute", v
        u0.confirm(b["request_id"], now_ms())
        time.sleep(1)
        v, b = m2.submit()
        report("stale_on_execute", label, m2, v, b, explain=True)
    # U0's SQL changes and dbt rebuilds it (a genuine upstream query change).
    (u0, u1), m = leaf_chain("soesql", 0, policy=ALL, n_up=2)
    time.sleep(2)
    u0.name_sql = True
    base = u0.sql
    u0.sql = lambda: base() + ", 2 as extra"
    v, b = u0.submit()
    assert v == "ready_to_execute", v
    u0.confirm(b["request_id"], now_ms())
    time.sleep(1)
    v, b = m.submit()
    report("stale_on_execute", "upstream_sql_changed_all", m, v, b, explain=True)


ARMS = {k[4:]: v for k, v in globals().items() if k.startswith("arm_")}

if __name__ == "__main__":
    wanted = sys.argv[1:] or ["all"]
    names = list(ARMS) if wanted == ["all"] else wanted
    for n in names:
        ARMS[n]()
