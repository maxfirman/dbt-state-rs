#!/usr/bin/env python3
"""Shared experiment ORACLES (review #2 6th follow-up, P2).

The driver validation logic previously lived inline in each shell heredoc, so
the warehouse-free fault tests could only exercise the helper functions, not the
actual decision the driver prints. These functions ARE the driver oracles: the
shell drivers call them, and the fault tests call them too, so a fault test
drives the exact code path the live run uses.

Each oracle takes the recording-proxy golden file + the model name and either
returns a result dict (prints are the caller's job) or raises
``trace_validate.ValidationError``.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import trace_validate as tv  # noqa: E402


def source_age_oracle(golden_file: str, model: str) -> dict:
    """The registerclone_source_age driver oracle. Enforces the complete causal
    chain (source submit → source confirm → RegisterClone → clone-creation
    confirm → dev resubmit, in order), correlates the clone source/target, and
    checks the distinguishing interval T1 < upstream < T3. Raises on any gap."""
    rows = tv.load(golden_file)
    tv.reject_intervening(rows, model)
    chain = [
        r
        for r in rows
        if model in json.dumps(r)
        and r["method"] in ("SubmitEnrichedSQL", "ConfirmExecution", "RegisterClone")
    ]
    dev_entry = tv.find_decision(chain, "SubmitEnrichedSQL", "dev_clone_demo")
    rc = tv.require_source_age_chain(chain, dev_entry)

    dev_v, dev_p = tv._resp_variant(dev_entry)
    dev_req = dev_entry.get("request", {}) or {}
    dev_up = {
        t["name"].split(".")[-1].strip('"'): t.get("last_modified_epoch")
        for t in dev_req.get("tables", [])
    }
    t1, t3 = rc["source_build_epoch"], rc["clone_creation_epoch"]
    if t1 is None or t3 is None:
        tv._fail(f"missing correlated source-build/clone-creation epochs: T1={t1} T3={t3}")
    up_only = [e for k, e in dev_up.items() if e is not None and "MODEL" not in k.upper()]
    if not up_only:
        tv._fail("no upstream epoch on the dev resubmit")
    up = max(up_only)
    if not (t1 < up < t3):
        tv._fail(
            f"distinguishing interval broken - need confirmed source build {t1} "
            f"< upstream {up} < clone creation {t3}"
        )
    return {"clone_source": rc["clone_source"], "t1": t1, "up": up, "t3": t3, "dev_decision": dev_v}


# arm -> (freshness_tolerance_seconds, tolerate_nondeterminism) wire expectation.
_ARM_WIRE = {
    "stale_postol": (2700, True),
    "stale_zerotol": (0, True),
    "volatile": (2700, False),
    "volatile_det": (2700, False),
}


def clone_eligibility_oracle(golden_file: str, model: str, arm: str) -> dict:
    """The clone_eligibility driver oracle. Audits intervening history, checks
    the wire fields reached the request, and:
      * CLONE arm: the selected source must have a confirmed baseline BEFORE the
        decision and the staleness interval must hold; OR
      * NON-CLONE (EXECUTE) arm: the equivalent it would have cloned must STILL
        have a confirmed baseline before the decision — an EXECUTE alone is not
        evidence of suppression (require_present_baseline_for_execute).
    Raises on any gap."""
    rows = tv.load(golden_file)
    tv.reject_intervening(rows, model)
    chain = [
        r
        for r in rows
        if model in json.dumps(r) and r["method"] in ("SubmitEnrichedSQL", "ConfirmExecution")
    ]
    dev_entry = tv.find_decision(chain, "SubmitEnrichedSQL", "dev_clone_demo")
    dev_v, dev_p = tv._resp_variant(dev_entry)
    dev_req = dev_entry.get("request", {}) or {}
    dev_up = {
        t["name"].split(".")[-1].strip('"'): t.get("last_modified_epoch")
        for t in dev_req.get("tables", [])
    }
    exp = _ARM_WIRE.get(arm)
    if exp is None:
        tv._fail(f"unknown arm {arm!r}")
    if (dev_req.get("freshness_tolerance_seconds"), dev_req.get("tolerate_nondeterminism")) != exp:
        tv._fail(
            f"VARCHECK FAILED arm={arm} expected ftol/tnd {exp} got "
            f"({dev_req.get('freshness_tolerance_seconds')},"
            f"{dev_req.get('tolerate_nondeterminism')})"
        )
    dev_index = tv.index_of(chain, dev_entry)

    # The equivalent this consumer would reuse is the SAME basename in the prod
    # schema (the source arm builds it there). Derive it from the dev target.
    dev_target = dev_req.get("target_table") or ""
    equivalent = dev_target.replace("DEV_CLONE_DEMO", "PROD_STATE_DEMO").replace(
        "dev_clone_demo", "prod_state_demo"
    )

    if dev_v == "ready_to_clone":
        src_epoch = tv.require_confirmed_source(
            chain, dev_p.get("clone_source"), before_index=dev_index
        )
        up_epochs = [e for e in dev_up.values() if e is not None]
        if not up_epochs:
            tv._fail("ready_to_clone with no upstream epoch to check staleness")
        creq = dev_p.get("clone_required_last_modified_epoch")
        if creq not in (None, src_epoch):
            tv._fail(f"clone_required epoch {creq} disagrees with confirmed source build {src_epoch}")
        if not (max(up_epochs) > src_epoch):
            tv._fail(
                f"staleness interval broken - upstream {max(up_epochs)} is NOT newer "
                f"than the confirmed source build {src_epoch}"
            )
        return {"decision": dev_v, "src_epoch": src_epoch, "up": max(up_epochs)}
    else:
        # NON-CLONE (EXECUTE): still require the equivalent's confirmed baseline
        # before the treatment — an EXECUTE is only evidence of suppression if
        # the thing it would have cloned demonstrably existed.
        base_epoch = tv.require_present_baseline_for_execute(chain, dev_entry, equivalent)
        return {"decision": dev_v, "equivalent": equivalent, "baseline_epoch": base_epoch}
