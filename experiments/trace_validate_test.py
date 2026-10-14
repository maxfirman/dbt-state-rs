#!/usr/bin/env python3
"""Warehouse-free failure-path tests for the shared trace validator
(review #2 4th follow-up, P2 #5). Runs against the COMMITTED fixtures and the
reviewer's exact fault matrix. No warehouse, no proxy. Exit 0 on success.

Run: python3 experiments/trace_validate_test.py
"""
import copy
import json
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

# ---- 5th follow-up faults -------------------------------------------------

# (d1) EVENT ORDERING: move the SOURCE submit+confirm to AFTER the RegisterClone.
# A later confirmation must not retroactively justify the earlier clone decision.
sa_src_after = copy.deepcopy(sa)
src_submit = sa_src_after.pop(0)      # entry 0 (source submit)
src_confirm = sa_src_after.pop(0)     # entry 1 (source confirm)
sa_src_after.extend([src_submit, src_confirm])  # now AFTER the RegisterClone
expect_reject(
    "source-age fault (d1): source baseline confirmed AFTER the clone is rejected (event order)",
    lambda: tv.require_successful_register_clone(sa_src_after),
)

# (d2) SPECULATIVE METHOD: an intervening SubmitEnrichedSQLSpeculative (the real
# proxy method, which has no from_speculative_submit flag) must be caught.
sa_spec = copy.deepcopy(sa)
sa_spec.insert(
    4,
    {"method": "SubmitEnrichedSQLSpeculative", "request": {"target_table": model}, "response": {}},
)
expect_reject(
    "source-age fault (d2): intervening SubmitEnrichedSQLSpeculative is rejected",
    lambda: tv.reject_intervening(sa_spec, model),
)

# (d3) PHYSICAL OUTCOME: the clone-creation confirm acknowledges the RPC but sets
# failed_to_clone=true. A failed physical clone is NOT a usable baseline.
sa_failed = copy.deepcopy(sa)
for e in sa_failed:
    if e.get("method") == "ConfirmExecution" and (e.get("request", {}) or {}).get(
        "request_id"
    ) == "01a1317a-e7fe-7e3f-9312-99e6f50bfc42":
        e["request"]["failed_to_clone"] = True
expect_reject(
    "source-age fault (d3): failed_to_clone confirm is not a successful clone",
    lambda: tv.require_successful_register_clone(sa_failed),
)

# (d4) MIRROR DISAGREEMENT: the two RegisterClone mirrors disagree on source.
sa_mirror = copy.deepcopy(sa)
for e in sa_mirror:
    if e.get("method") == "RegisterClone":
        e["response"]["response"]["ready_to_clone_v1"]["clone_source"] = "DB.OTHER.THING"
expect_reject(
    "source-age fault (d4): disagreeing RegisterClone mirrors are rejected",
    lambda: tv.require_successful_register_clone(sa_mirror),
)

# (d5) VOLATILE EXECUTE MISSING-BASELINE: a negative treatment that EXECUTEs is
# only evidence of SUPPRESSION if the equivalent it should have cloned has a
# confirmed baseline. Deleting the source baseline must make the EXECUTE-path
# check reject (the previous review's fault that stayed accepted).
# Build a tiny synthetic volatile-style trace: a prod source built+confirmed,
# then a dev EXECUTE for the "same" equivalent.
vol_trace = [
    {
        "method": "SubmitEnrichedSQL",
        "request": {"target_table": "DB.PROD.M", "default_schema": "prod"},
        "response": {"response": {"ready_to_execute": {"request_id": "p1"}}},
    },
    {"method": "ConfirmExecution", "request": {"request_id": "p1", "last_modified_epoch": 100},
     "response": {"success": True}},
    {
        "method": "SubmitEnrichedSQL",
        "request": {"target_table": "DB.DEV.M", "default_schema": "dev"},
        "response": {"response": {"ready_to_execute": {"request_id": "d1"}}},
    },
]
dev_exec = vol_trace[2]
expect_accept(
    "volatile EXECUTE: present baseline for the equivalent is accepted",
    lambda: tv.require_present_baseline_for_execute(vol_trace, dev_exec, "DB.PROD.M"),
)
vol_no_base = [e for i, e in enumerate(vol_trace) if i not in (0, 1)]
dev_exec2 = vol_no_base[0]
expect_reject(
    "volatile EXECUTE fault: deleted equivalent baseline makes the EXECUTE non-evidence",
    lambda: tv.require_present_baseline_for_execute(vol_no_base, dev_exec2, "DB.PROD.M"),
)

# ---- 6th follow-up: exercise the ACTUAL DRIVER ORACLES (not just helpers) ----
import oracles  # noqa: E402
import tempfile  # noqa: E402


def _tmp(trace):
    f = tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False)
    for r in trace:
        f.write(json.dumps(r) + "\n")
    f.close()
    return f.name


ZT_MODEL = "ce_model_214942"
VOL_MODEL = "ce_model_215158"
SA_MODEL = "rc_model_185510"

# The clean fixtures must pass the real oracles.
expect_accept(
    "ORACLE clone_eligibility zero-tol clean passes",
    lambda: oracles.clone_eligibility_oracle(ZEROTOL, ZT_MODEL, "stale_zerotol"),
)
expect_accept(
    "ORACLE clone_eligibility volatile clean passes",
    lambda: oracles.clone_eligibility_oracle(VOLATILE, VOL_MODEL, "volatile"),
)
expect_accept(
    "ORACLE source_age clean passes",
    lambda: oracles.source_age_oracle(SRCAGE, SA_MODEL),
)

# (e1) WIRED volatile baseline: deleting the source baseline (entries 0/1) from
# the volatile capture must make the ACTUAL clone_eligibility oracle REJECT (the
# reviewer's exact end-to-end fault that previously printed CE_OK / exit 0).
vol_del = _tmp([e for i, e in enumerate(tv.load(VOLATILE)) if i not in (0, 1)])
expect_reject(
    "ORACLE clone_eligibility volatile deleted-baseline is rejected end-to-end",
    lambda: oracles.clone_eligibility_oracle(vol_del, VOL_MODEL, "volatile"),
)

# (e2) source confirm BEFORE source submit — the real source-age oracle rejects.
sa_rows = tv.load(SRCAGE)
sa_swap = copy.deepcopy(sa_rows)
sa_swap[0], sa_swap[1] = sa_swap[1], sa_swap[0]
sa_swap_f = _tmp(sa_swap)
expect_reject(
    "ORACLE source_age fault: source confirm before its submit is rejected",
    lambda: oracles.source_age_oracle(sa_swap_f, SA_MODEL),
)

# (e3) clone-creation confirm AFTER the dev resubmit — rejected by the oracle.
sa_late = copy.deepcopy(sa_rows)
_c3 = sa_late.pop(3)
sa_late.append(_c3)
sa_late_f = _tmp(sa_late)
expect_reject(
    "ORACLE source_age fault: clone confirm after resubmit is rejected",
    lambda: oracles.source_age_oracle(sa_late_f, SA_MODEL),
)

# (e4) both mirrors' clone_source = same UNBUILT relation — rejected (no
# confirmed baseline for the returned source).
# (e4) RESPONSE-ONLY clone_source mutation (review #2 7th follow-up P2 #2): change
# ONLY both response mirrors' clone_source to an unbuilt relation, LEAVING
# request.clone_source_table intact. The validator must correlate the RETURNED
# source (not the request), so this must be rejected — either because the
# returned source has no confirmed baseline OR because it disagrees with the
# request. (Previously this test also mutated request.clone_source_table, which
# made the request-baseline check reject a DIFFERENT fault.)
sa_resp_src = copy.deepcopy(sa_rows)
for e in sa_resp_src:
    if e.get("method") == "RegisterClone":
        e["response"]["ready_to_clone"]["clone_source"] = "\"DB\".\"X\".\"UNBUILT\""
        e["response"]["response"]["ready_to_clone_v1"]["clone_source"] = "\"DB\".\"X\".\"UNBUILT\""
        # request.clone_source_table is intentionally LEFT UNCHANGED.
sa_resp_src_f = _tmp(sa_resp_src)
expect_reject(
    "ORACLE source_age fault: RESPONSE-ONLY unbuilt clone_source is rejected",
    lambda: oracles.source_age_oracle(sa_resp_src_f, SA_MODEL),
)

# (e5) RESPONSE-ONLY clone_target mutation: change ONLY both response mirrors'
# clone_target to a DIFFERENT target, leaving request.target_table intact. The
# returned target must match the request (and the resubmit), so this is rejected.
sa_resp_tgt = copy.deepcopy(sa_rows)
for e in sa_resp_tgt:
    if e.get("method") == "RegisterClone":
        e["response"]["ready_to_clone"]["clone_target"] = "\"DB\".\"X\".\"OTHER_TARGET\""
        e["response"]["response"]["ready_to_clone_v1"]["clone_target"] = "\"DB\".\"X\".\"OTHER_TARGET\""
        # request.target_table is intentionally LEFT UNCHANGED.
sa_resp_tgt_f = _tmp(sa_resp_tgt)
expect_reject(
    "ORACLE source_age fault: RESPONSE-ONLY mismatched clone_target is rejected",
    lambda: oracles.source_age_oracle(sa_resp_tgt_f, SA_MODEL),
)

for _f in (vol_del, sa_swap_f, sa_late_f, sa_resp_src_f, sa_resp_tgt_f):
    os.unlink(_f)

print(f"\nALL {passed} VALIDATOR FAULT-PATH TESTS PASSED")
