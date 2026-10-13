#!/usr/bin/env python3
"""Warehouse-free failure-path tests for the shared trace validator
(review #2 4th follow-up, P2 #5). Runs against the COMMITTED fixtures and the
reviewer's exact fault matrix. No warehouse, no proxy. Exit 0 on success.

Run: python3 experiments/trace_validate_test.py
"""
import copy
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import trace_validate as tv  # noqa: E402

REPO = os.path.dirname(HERE)
ZEROTOL = os.path.join(REPO, "golden/fixtures/clone_fallback_zero_tolerance.jsonl")
VOLATILE = os.path.join(REPO, "golden/fixtures/clone_fallback_volatile.jsonl")
SRCAGE = os.path.join(REPO, "golden/fixtures/clone_chain_source_age.jsonl")

passed = 0


def ok(name):
    global passed
    passed += 1
    print(f"PASS: {name}")


def expect_reject(name, fn):
    try:
        fn()
    except tv.ValidationError:
        ok(name)
        return
    print(f"FAIL: {name} — expected ValidationError, got none")
    sys.exit(1)


def expect_accept(name, fn):
    try:
        fn()
    except tv.ValidationError as e:
        print(f"FAIL: {name} — unexpected rejection: {e}")
        sys.exit(1)
    ok(name)


def dev_clone_source(trace):
    dev = tv.find_decision(trace, "SubmitEnrichedSQL", "dev_clone_demo")
    _, p = tv._resp_variant(dev)
    return p.get("clone_source")


# --- clone_eligibility (zero-tolerance) ------------------------------------
zt = tv.load(ZEROTOL)
expect_accept(
    "zero-tol clean: selected clone_source has a confirmed baseline",
    lambda: tv.require_confirmed_source(zt, dev_clone_source(zt)),
)

# Fault (a): delete the SELECTED SOURCE's submit+confirm baseline (entries 0,1).
zt_no_base = [e for i, e in enumerate(zt) if i not in (0, 1)]
expect_reject(
    "zero-tol fault (a): deleted source baseline is rejected",
    lambda: tv.require_confirmed_source(zt_no_base, dev_clone_source(zt_no_base)),
)

# Fault variant: source confirm present but FAILED (success=False).
zt_bad_confirm = copy.deepcopy(zt)
for e in zt_bad_confirm:
    if e.get("method") == "ConfirmExecution":
        e["response"] = {"success": False}
        break
expect_reject(
    "zero-tol fault: unsuccessful source confirm is rejected",
    lambda: tv.require_confirmed_source(zt_bad_confirm, dev_clone_source(zt_bad_confirm)),
)

# --- clone_eligibility (volatile) ------------------------------------------
vol = tv.load(VOLATILE)
vol_dev = tv.find_decision(vol, "SubmitEnrichedSQL", "dev_clone_demo")
_, vol_p = tv._resp_variant(vol_dev)
assert vol_p.get("clone_source") in (None, ""), "volatile dev must not clone"
ok("volatile: dev executes (no clone_source to spoof)")

# --- registerclone_source_age ----------------------------------------------
sa = tv.load(SRCAGE)
expect_accept(
    "source-age clean: successful RegisterClone fully correlates",
    lambda: tv.require_successful_register_clone(sa),
)

# Fault (c1): RegisterClone responds unable_to_clone.
sa_unable = copy.deepcopy(sa)
for e in sa_unable:
    if e.get("method") == "RegisterClone":
        e["response"] = {"unable_to_clone": {"request_id": "x"}, "response": {}}
expect_reject(
    "source-age fault (c1): unable_to_clone is rejected",
    lambda: tv.require_successful_register_clone(sa_unable),
)

# Fault (c2): swap the clone-creation confirm to an UNRELATED request_id.
sa_badid = copy.deepcopy(sa)
confirms = [e for e in sa_badid if e.get("method") == "ConfirmExecution"]
confirms[-1]["request"]["request_id"] = "00000000-dead-beef-0000-000000000000"
expect_reject(
    "source-age fault (c2): unrelated clone-confirm id breaks correlation",
    lambda: tv.require_successful_register_clone(sa_badid),
)

# Fault (c3): delete the SOURCE's submit+confirm (no confirmed baseline).
sa_nosrc = [e for i, e in enumerate(sa) if i not in (0, 1)]
expect_reject(
    "source-age fault (c3): missing confirmed source build is rejected",
    lambda: tv.require_successful_register_clone(sa_nosrc),
)

# Fault (c4): inject an intervening RecordExecutions for the model.
sa_rec = copy.deepcopy(sa)
model = "rc_model_185510"
sa_rec.insert(4, {"method": "RecordExecutions", "request": {"note": model}, "response": {}})
expect_reject(
    "source-age fault (c4): intervening RecordExecutions is rejected",
    lambda: tv.reject_intervening(sa_rec, model),
)

print(f"\nALL {passed} VALIDATOR FAULT-PATH TESTS PASSED")
