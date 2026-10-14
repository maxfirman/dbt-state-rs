#!/usr/bin/env python3
"""Shared recording-proxy trace validator (review #2 4th+5th follow-up, P2 #5).

The experiment oracles accepted incomplete / uncorrelated evidence. This module
centralizes correlation so the shell heredocs do not each re-implement (and
re-bug) it. The 5th-follow-up hardening adds:

  * EVENT ORDERING — a decision may only be justified by a baseline that was
    SUCCESSFULLY CONFIRMED at an EARLIER position in the trace; a later
    confirmation cannot retroactively justify an earlier decision.
  * METHOD RECOGNITION — a speculative submit is the proxy METHOD
    ``SubmitEnrichedSQLSpeculative`` (its request has no ``from_speculative_submit``
    field), so intervening-history checks match on the method name, not a flag.
  * PHYSICAL OUTCOME — a ConfirmExecution with ``failed_to_clone=true`` is a
    successful RPC acknowledgement of a FAILED physical clone; it does NOT
    establish a usable baseline.
  * MIRROR AGREEMENT — a RegisterClone's two response mirrors
    (``ready_to_clone`` and ``response.ready_to_clone_v1``) must agree on
    request_id and clone source/target.
  * BASELINE PREREQUISITE for a NEGATIVE treatment — an EXECUTE used as evidence
    that an available equivalent was SUPPRESSED must still prove that equivalent's
    confirmed baseline + a positive control existed; an EXECUTE alone proves
    nothing about suppression.

A trace is the list of recording-proxy JSON entries (one per line), each a dict
with at least ``{"method": str, "request": {...}, "response": {...}}``. All
helpers raise ValidationError (message starts with "ERROR:") on failure.
"""
import json
from typing import Optional

SUBMIT_METHODS = ("SubmitEnrichedSQL", "SubmitEnrichedSQLSpeculative")
SPECULATIVE_METHOD = "SubmitEnrichedSQLSpeculative"


class ValidationError(Exception):
    pass


def _fail(msg: str):
    raise ValidationError("ERROR: " + msg)


def load(path: str) -> list[dict]:
    return [json.loads(l) for l in open(path) if l.strip()]


def _resp_variant(entry: dict) -> tuple[Optional[str], dict]:
    """Return (variant_name, payload) for a decision entry, or (None, {})."""
    rr = (entry.get("response", {}) or {}).get("response", {}) or {}
    if not isinstance(rr, dict):
        return None, {}
    v = next(iter(rr.keys()), None)
    return v, (rr.get(v) or {})


def _resp_request_id(entry: dict) -> Optional[str]:
    _, p = _resp_variant(entry)
    rid = p.get("request_id") if isinstance(p, dict) else None
    if rid:
        return rid
    for mirror in ("ready_to_clone", "unable_to_clone"):
        m = (entry.get("response", {}) or {}).get(mirror)
        if isinstance(m, dict) and m.get("request_id"):
            return m["request_id"]
    return None


def _confirm_is_successful(entry: dict) -> bool:
    """A ConfirmExecution establishes a usable baseline only when the RPC
    succeeded AND the physical outcome did not fail. ``failed_to_clone=true``
    (on either the request or the response) means the clone physically failed
    even though the RPC was acknowledged — NOT a usable baseline."""
    resp = entry.get("response", {}) or {}
    req = entry.get("request", {}) or {}
    if not resp.get("success"):
        return False
    if req.get("failed_to_clone") or resp.get("failed_to_clone"):
        return False
    return True


def confirmed_request_ids(trace: list[dict], before_index: Optional[int] = None) -> set[str]:
    """request_ids that a SUCCESSFUL (and physically non-failed) ConfirmExecution
    acknowledged. When ``before_index`` is given, only confirmations STRICTLY
    BEFORE that position count (event ordering)."""
    out = set()
    for i, e in enumerate(trace):
        if before_index is not None and i >= before_index:
            break
        if e.get("method") != "ConfirmExecution":
            continue
        req = e.get("request", {}) or {}
        if _confirm_is_successful(e) and req.get("request_id"):
            out.add(req["request_id"])
    return out


def confirmed_targets(trace: list[dict], before_index: Optional[int] = None) -> dict[str, int]:
    """Map each SUCCESSFULLY-CONFIRMED decision's target_table FQN to the
    confirmation's last_modified_epoch (baseline build epoch). EVENT-ORDERED on
    two axes (5th+6th follow-up P2): (1) with ``before_index`` set, both the
    decision AND its confirmation occur strictly before that position; and
    (2) a confirmation is only paired with its decision when the SUBMIT/CLONE
    that produced the response request_id occurs at an EARLIER index than the
    ConfirmExecution — a confirm that precedes its own submit does not establish
    a baseline."""
    # Map response request_id -> (submit index, target FQN) for decisions.
    submit_of: dict[str, tuple[int, str]] = {}
    for i, e in enumerate(trace):
        if before_index is not None and i >= before_index:
            break
        if e.get("method") not in (*SUBMIT_METHODS, "RegisterClone"):
            continue
        rid = _resp_request_id(e)
        tgt = (e.get("request", {}) or {}).get("target_table")
        if rid and tgt:
            submit_of.setdefault(rid, (i, tgt))
    out: dict[str, int] = {}
    for i, e in enumerate(trace):
        if before_index is not None and i >= before_index:
            break
        if e.get("method") != "ConfirmExecution":
            continue
        req = e.get("request", {}) or {}
        rid = req.get("request_id")
        if not (_confirm_is_successful(e) and rid):
            continue
        match submit_of.get(rid):
            # The submit that produced this request_id must be EARLIER than the
            # confirm acknowledging it.
            case (submit_i, tgt) if submit_i < i:
                out[tgt] = req.get("last_modified_epoch")
            case _:
                continue
    return out


def index_of(trace: list[dict], entry: dict) -> int:
    for i, e in enumerate(trace):
        if e is entry:
            return i
    _fail("entry not found in trace")


def require_confirmed_source(
    trace: list[dict], clone_source: str, before_index: Optional[int] = None
) -> int:
    """Assert the clone_source FQN was BUILT and SUCCESSFULLY CONFIRMED in this
    trace STRICTLY BEFORE the decision (event ordering); return its confirmed
    build epoch. Rejects a missing baseline and a baseline confirmed only AFTER
    the decision."""
    if not clone_source:
        _fail("clone decision has no clone_source to correlate")
    confirmed = confirmed_targets(trace, before_index)
    if clone_source not in confirmed:
        _fail(
            f"the selected clone_source {clone_source!r} has NO successfully-"
            f"confirmed build BEFORE the decision (confirmed-before targets: "
            f"{sorted(confirmed)}); cannot claim a stale-source clone"
        )
    return confirmed[clone_source]


def find_decision(trace: list[dict], method: str, schema: str) -> dict:
    """Return the single decision entry for (method, default_schema), or fail."""
    hits = [
        e
        for e in trace
        if e.get("method") == method
        and (e.get("request", {}) or {}).get("default_schema") == schema
    ]
    if not hits:
        _fail(f"no {method} captured for schema {schema!r}")
    return hits[-1]


def reject_intervening(trace: list[dict], model: str, forbidden=("RecordExecutions",)):
    """Fail if a FORBIDDEN method OR a SPECULATIVE submission
    (``SubmitEnrichedSQLSpeculative`` — recognized by METHOD, since its request
    has no ``from_speculative_submit`` field) references the model anywhere in
    the trace. The curated chain must not hide such intervening history."""
    for e in trace:
        if model not in json.dumps(e):
            continue
        method = e.get("method")
        if method in forbidden:
            _fail(f"intervening {method} for {model} would be hidden by the chain filter")
        if method == SPECULATIVE_METHOD or (e.get("request", {}) or {}).get(
            "from_speculative_submit"
        ):
            _fail(
                f"speculative submission ({method}) for {model} must not be filtered "
                f"from the causal chain"
            )


def register_clone_entry(trace: list[dict]) -> dict:
    hits = [e for e in trace if e.get("method") == "RegisterClone"]
    if not hits:
        _fail("no RegisterClone event captured (did an incremental model clone via pre_clone?)")
    if len(hits) > 1:
        _fail(f"expected exactly one RegisterClone, got {len(hits)}")
    return hits[0]


def _mirrors_agree(resp: dict):
    """The RegisterClone top-level ``ready_to_clone`` mirror and the
    ``response.ready_to_clone_v1`` oneof must agree on request_id and the clone
    source/target. Disagreeing mirrors are rejected (5th follow-up P2 #5)."""
    top = resp.get("ready_to_clone") or {}
    v1 = (resp.get("response", {}) or {}).get("ready_to_clone_v1") or {}
    for field in ("request_id", "clone_source", "clone_target"):
        if top.get(field) != v1.get(field):
            _fail(
                f"RegisterClone mirrors disagree on {field!r}: "
                f"ready_to_clone={top.get(field)!r} vs ready_to_clone_v1={v1.get(field)!r}"
            )


def require_successful_register_clone(trace: list[dict]) -> dict:
    """Assert the RegisterClone returned the SUCCESSFUL ready_to_clone variant,
    its two mirrors AGREE, its clone_source_table has a SUCCESSFULLY-CONFIRMED
    source build BEFORE the clone decision (event ordering), and its OWN response
    request_id is acknowledged by a successful, physically-non-failed
    clone-creation confirm. Rejects unable_to_clone, disagreeing mirrors,
    failed_to_clone confirms, out-of-order baselines and unrelated confirm ids."""
    rc = register_clone_entry(trace)
    rc_index = index_of(trace, rc)
    resp = rc.get("response", {}) or {}
    top = resp.get("ready_to_clone")
    v1 = (resp.get("response", {}) or {}).get("ready_to_clone_v1")
    if resp.get("unable_to_clone"):
        _fail("RegisterClone returned unable_to_clone — not a successful clone")
    if not top or not v1:
        _fail("RegisterClone response is not a populated ready_to_clone (both mirrors)")
    _mirrors_agree(resp)
    rid = v1.get("request_id") or top.get("request_id")
    if not rid:
        _fail("RegisterClone response carries no request_id to correlate its confirm")

    # Correlate the RETURNED (response-mirror) source/target — the physical
    # relations the clone actually describes — with the request AND the trace,
    # NOT merely the request fields. Both mirrors agree (checked above), so read
    # from v1. Review #2 7th follow-up P2 #2: changing only the response source to
    # an unbuilt relation (or the response target to a different one) previously
    # still passed because validation used the request fields.
    returned_source = v1.get("clone_source")
    returned_target = v1.get("clone_target")
    if not returned_source or not returned_target:
        _fail("RegisterClone response mirrors carry an empty clone_source/clone_target")
    req = rc.get("request", {}) or {}
    req_source = req.get("clone_source_table")
    req_target = req.get("target_table")
    # Returned source must match the requested source (request↔response identity).
    if req_source and returned_source != req_source:
        _fail(
            f"RegisterClone returned clone_source {returned_source!r} does not match the "
            f"requested clone_source_table {req_source!r}"
        )
    # Returned target must match the requested target.
    if req_target and returned_target != req_target:
        _fail(
            f"RegisterClone returned clone_target {returned_target!r} does not match the "
            f"requested target_table {req_target!r}"
        )
    # The RETURNED source must itself have a confirmed source build BEFORE the
    # clone — this is the authoritative physical-source check (not the request).
    src_epoch = require_confirmed_source(trace, returned_source, before_index=rc_index)
    src = returned_source
    # The clone-creation confirm must acknowledge THIS request_id, successfully
    # AND physically (not failed_to_clone), AFTER the clone decision.
    t3 = None
    clone_confirm_index = None
    for i, e in enumerate(trace):
        if i <= rc_index or e.get("method") != "ConfirmExecution":
            continue
        creq = e.get("request", {}) or {}
        if creq.get("request_id") == rid and _confirm_is_successful(e):
            t3 = creq.get("last_modified_epoch")
            clone_confirm_index = i
            break
    if t3 is None:
        _fail(
            f"the clone-creation confirm for request_id {rid} is missing, "
            f"unsuccessful, physically failed (failed_to_clone), or out of order"
        )
    return {
        "clone_source": src,
        "clone_target": returned_target,
        "source_build_epoch": src_epoch,
        "clone_creation_epoch": t3,
        "register_index": rc_index,
        "clone_confirm_index": clone_confirm_index,
    }


def require_source_age_chain(trace: list[dict], dev_resubmit: dict) -> dict:
    """Enforce the FULL causal chain in event order for the source-age
    experiment (6th follow-up P2): source submit → source confirm → RegisterClone
    → clone-creation confirm → dev resubmit, with the dev resubmit occurring
    STRICTLY AFTER the clone-creation confirm. Builds on
    `require_successful_register_clone` (which already orders source-submit <
    source-confirm < RegisterClone < clone-confirm). Returns the correlated
    facts plus the ordered indices."""
    rc = require_successful_register_clone(trace)
    resubmit_index = index_of(trace, dev_resubmit)
    if not (rc["clone_confirm_index"] < resubmit_index):
        _fail(
            f"dev resubmit (index {resubmit_index}) must come AFTER the "
            f"clone-creation confirm (index {rc['clone_confirm_index']}); the "
            f"reuse decision cannot precede the clone it reuses"
        )
    # The resubmit must target the RETURNED clone_target (the physical relation
    # the clone response actually describes), correlating the reuse to the clone
    # it reuses — not merely the request target.
    returned_target = rc["clone_target"]
    resubmit_target = (dev_resubmit.get("request", {}) or {}).get("target_table")
    if returned_target and resubmit_target and returned_target != resubmit_target:
        _fail(
            f"dev resubmit target {resubmit_target!r} does not match the RETURNED clone "
            f"target {returned_target!r}; the reuse is not correlated to the clone"
        )
    return rc


def require_present_baseline_for_execute(trace: list[dict], treatment: dict, equivalent: str):
    """For a NEGATIVE treatment that EXECUTEs (used as evidence that an available
    equivalent was SUPPRESSED, e.g. the volatile case), require that the claimed
    `equivalent` FQN actually has a SUCCESSFULLY-CONFIRMED baseline BEFORE the
    treatment. An EXECUTE alone proves nothing about suppression unless the
    equivalent it would have cloned demonstrably existed. Review #2 5th follow-up
    P2 #5 (the EXECUTE path previously never checked a baseline)."""
    t_index = index_of(trace, treatment)
    confirmed = confirmed_targets(trace, before_index=t_index)
    if equivalent not in confirmed:
        _fail(
            f"negative treatment EXECUTEs but the equivalent {equivalent!r} it should "
            f"have suppressed has NO confirmed baseline before the treatment "
            f"(confirmed-before: {sorted(confirmed)}); the EXECUTE is not evidence of suppression"
        )
    return confirmed[equivalent]
