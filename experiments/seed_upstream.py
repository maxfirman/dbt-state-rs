#!/usr/bin/env python3
"""Crafted-gRPC characterization: a model over a dbt SEED under lag_tolerance.

Open question (docs/adr/0007): is a seed reloaded with CHANGED values (new
values_hash) a data change for its consumers (lag tolerance applies) or an
upstream query change (always rebuild)?

Sends synthetic SubmitValues / SubmitEnrichedSQL / ConfirmExecution /
GetExplainMessages through the recording proxy (experiments/proxy_up.sh) with
`target/debug/state-probe`. No warehouse is touched: names are fresh and never
created, epochs are made up (Unix ms, wall-clock based).

Each arm: seed S is loaded and confirmed, model M over S is built and
confirmed (ANY policy), then S changes in some way and M is submitted a few
seconds later (build age << tolerance), with Explain.

  A  S reloaded with changed values (new values_hash), M tol 600
  B  as A, M tol 0
  C  S's own table externally modified, S resubmitted with same values
     (executes), confirmed at a new epoch, M tol 600
  D  S unchanged and resubmitted (control: S skips), M tol 600
  E  S's table externally modified, no S resubmit, M tol 600
  F  S semantic config change only (same values), S reload, M tol 600
  G  S table reported missing, S reloaded with the SAME values and
     confirmed at a new epoch (a same-values recorded reload), M tol 600
  H  S reloaded v1 -> v2 -> v1 (each confirmed), M (consumed v1) tol 600

Usage: seed_upstream.py ARM [ARM ...]   (or `all`)
Prints one JSON line per step (pipe into experiments/lag_show.py).
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lag_matrix as lm  # noqa: E402
from lag_matrix import ANY, DB, NAMESPACE, fqn, h, now_ms, probe  # noqa: E402

RUN = time.strftime("%H%M%S")


class Seed:
    def __init__(self, arm, name="s"):
        self.name = f"sdx{RUN}_{arm}_{name}".lower()
        self.values_hash = h("v1" + self.name)
        self.semantic_extras = {}
        self.epoch = None  # own table epoch (None: does not exist)

    @property
    def fqn(self):
        return fqn(self.name)

    def request(self):
        uid = f"seed.jaffle_shop.{self.name}"
        return {
            "target_table": self.fqn,
            "dialect": "snowflake",
            "default_catalog": DB,
            "values_hash": self.values_hash,
            "semantic_extras": self.semantic_extras,
            "last_modified_epoch": self.epoch,
            "labels": {
                "dbt_node_name": self.name,
                "dbt_node_fqn": f"jaffle_shop.seeds.{self.name}",
                "dbt_node_unique_id": uid,
            },
            "clone_time_travel_limit": None,
            "clone_table_properties": None,
            "clone_chain_depth_limit": None,
            "dbt_node_state": {
                "node_unique_id": uid,
                "target_name": "prod_demo",
                "project_name": "jaffle_shop",
                "resource_type": "seed",
                "node_hash": self.values_hash,
                "node_body_hash": self.values_hash,
                "node_configs_hash": None,
                "node_persisted_descriptions_hash": None,
                "node_macros_hash": None,
                "node_contract_hash": None,
                "profile_name": "snowflake",
                "project_id": "70437463667433",
                "node_database_representation": None,
            },
            "table_namespace": NAMESPACE,
            "allow_clones": None,
            "is_defer_to_profile": False,
            "defer_enabled": False,
        }

    def submit(self):
        resp = probe("values", self.request())["response"]
        (variant, body), = resp.items()
        return variant, body

    def confirm(self, request_id, epoch):
        r = probe(
            "confirm",
            {
                "request_id": request_id,
                "last_modified_epoch": epoch,
                "failed_to_clone": False,
                "table_type": None,
                "execution_results": None,
                "execution_runtime_ms": 1500,
                "labels": {
                    "dbt_node_unique_id": f"seed.jaffle_shop.{self.name}",
                    "dbt_node_name": self.name,
                },
            },
        )
        if not r.get("success"):
            raise SystemExit(f"confirm failed for {self.name}: {r}")
        self.epoch = epoch

    def build(self):
        variant, body = self.submit()
        if variant == "ready_to_execute":
            self.confirm(body["request_id"], now_ms())
        return variant, body


def report(arm, step, node, variant, body, explain=False, **facts):
    return lm.report(arm, step, node, variant, body, explain=explain, **facts)


def base(arm, tol=600):
    s = Seed(arm)
    v, b = s.build()
    report(arm, "seed_first_load", s, v, b)
    assert v == "ready_to_execute", v
    m = lm.Node(arm, "m", [s], tol=tol, policy=ANY)
    m.name = f"sdx{RUN}_{arm}_m".lower()
    v, b = m.build()
    report(arm, "m_first_build", m, v, b)
    assert v == "ready_to_execute", v
    return s, m


def decisive(arm, m, tol):
    time.sleep(2)
    v, b = m.submit()
    report(arm, "m_decisive", m, v, b, explain=True, tol=tol)


def arm_A(tol=600, arm="a"):
    """Seed reloaded with changed values."""
    s, m = base(arm, tol)
    time.sleep(3)
    s.values_hash = h("v2" + s.name)
    v, b = s.build()
    report(arm, "seed_reload_changed_values", s, v, b, explain=True)
    decisive(arm, m, tol)


def arm_B():
    arm_A(tol=0, arm="b")


def arm_C():
    """Seed's own table externally modified; seed resubmitted, same values."""
    s, m = base("c")
    time.sleep(3)
    s.epoch = now_ms()  # external modification of the seed's table
    v, b = s.build()
    report("c", "seed_resubmit_after_external", s, v, b, explain=True)
    decisive("c", m, 600)


def arm_D():
    """Control: seed unchanged and resubmitted."""
    s, m = base("d")
    time.sleep(3)
    v, b = s.build()
    report("d", "seed_resubmit_unchanged", s, v, b)
    decisive("d", m, 600)


def arm_E():
    """Seed's table externally modified; no seed resubmit."""
    s, m = base("e")
    time.sleep(3)
    s.epoch = now_ms()
    decisive("e", m, 600)


def arm_F():
    """Seed semantic config change only (same values)."""
    s, m = base("f")
    time.sleep(3)
    s.semantic_extras = {"quote_columns": "true"}
    v, b = s.build()
    report("f", "seed_reload_config_change", s, v, b, explain=True)
    decisive("f", m, 600)


def arm_G():
    """Same-values recorded reload: the seed's table is reported missing, so
    hosted reloads it with unchanged values; confirmed at a new epoch."""
    s, m = base("g")
    time.sleep(3)
    s.epoch = None  # table dropped
    v, b = s.build()
    report("g", "seed_reload_same_values_missing_table", s, v, b, explain=True)
    decisive("g", m, 600)


def arm_H():
    """Values changed then changed back (v1 -> v2 -> v1), each reload
    confirmed; M consumed the v1 build."""
    s, m = base("h")
    time.sleep(3)
    v1 = s.values_hash
    s.values_hash = h("v2" + s.name)
    v, b = s.build()
    report("h", "seed_reload_v2", s, v, b)
    time.sleep(1)
    s.values_hash = v1
    v, b = s.build()
    report("h", "seed_reload_back_to_v1", s, v, b, explain=True)
    decisive("h", m, 600)


ARMS = {k[4:]: v for k, v in globals().items() if k.startswith("arm_")}

if __name__ == "__main__":
    wanted = sys.argv[1:] or ["all"]
    names = list(ARMS) if wanted == ["all"] else wanted
    for n in names:
        ARMS[n]()
