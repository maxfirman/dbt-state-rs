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
    confirmation's last_modified_epoch (baseline build epoch). EVENT-ORDERED:
    with ``before_index`` set, both the decision AND its confirmation must occur
    strictly before that position, so a later confirmation cannot justify an
    earlier decision (5th follow-up P2 #5)."""
    confirm_epoch: dict[str, Optional[int]] = {}
    for i, e in enumerate(trace):
        if before_index is not None and i >= before_index:
            break
        if e.get("method") != "ConfirmExecution":
            continue
        req = e.get("request", {}) or {}
        if _confirm_is_successful(e) and req.get("request_id"):
            confirm_epoch[req["request_id"]] = req.get("last_modified_epoch")
    out: dict[str, int] = {}
    for i, e in enumerate(trace):
        if before_index is not None and i >= before_index:
            break
        if e.get("method") not in (*SUBMIT_METHODS, "RegisterClone"):
            continue
        rid = _resp_request_id(e)
        tgt = (e.get("request", {}) or {}).get("target_table")
        if rid and tgt and rid in confirm_epoch:
            out[tgt] = confirm_epoch[rid]
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
    # The clone source must have a confirmed source build BEFORE the clone.
    src = (rc.get("request", {}) or {}).get("clone_source_table")
    src_epoch = require_confirmed_source(trace, src, before_index=rc_index)
    # The clone-creation confirm must acknowledge THIS request_id, successfully
    # AND physically (not failed_to_clone), AFTER the clone decision.
    t3 = None
    for i, e in enumerate(trace):
        if i <= rc_index or e.get("method") != "ConfirmExecution":
            continue
        req = e.get("request", {}) or {}
        if req.get("request_id") == rid and _confirm_is_successful(e):
            t3 = req.get("last_modified_epoch")
            break
    if t3 is None:
        _fail(
            f"the clone-creation confirm for request_id {rid} is missing, "
            f"unsuccessful, physically failed (failed_to_clone), or out of order"
        )
    return {"clone_source": src, "source_build_epoch": src_epoch, "clone_creation_epoch": t3}


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
