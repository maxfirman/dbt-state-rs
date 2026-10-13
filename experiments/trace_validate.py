#!/usr/bin/env python3
"""Shared recording-proxy trace validator (review #2 4th follow-up, P2 #5).

The experiment oracles previously accepted incomplete / uncorrelated evidence:
they read a method name and some epochs without checking that the SELECTED clone
source actually had a captured, SUCCESSFULLY-CONFIRMED build in the trace, so
deleting the source's submit/confirm baseline (or swapping a RegisterClone to
`unable_to_clone` with an unrelated confirmation id) still "passed". This module
centralizes correlation so the shell heredocs do not each re-implement (and
re-bug) it.

A trace is the list of recording-proxy JSON entries (one per line). Each entry
is a dict with at least:
    {"method": str, "request": {...}, "response": {...}}

Correlation facts we enforce:
  * every SubmitEnrichedSQL/RegisterClone decision carries a response
    request_id; a later ConfirmExecution with the SAME request_id and
    success=True is its confirmation;
  * a clone's `clone_source` FQN must equal the target_table of a submit/clone
    that was itself SUCCESSFULLY confirmed in this trace (a confirmed baseline);
  * decisions must be the expected successful variant (not unable_to_clone /
    an error) to count as evidence.

All helpers raise ValidationError (message starts with "ERROR:") on failure so
callers can `print(e); sys.exit(1)`.
"""
import json
from typing import Any, Optional


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
    # RegisterClone also mirrors ready_to_clone at the top level.
    for mirror in ("ready_to_clone", "unable_to_clone"):
        m = (entry.get("response", {}) or {}).get(mirror)
        if isinstance(m, dict) and m.get("request_id"):
            return m["request_id"]
    return None


def confirmed_request_ids(trace: list[dict]) -> set[str]:
    """request_ids that a SUCCESSFUL ConfirmExecution acknowledged."""
    out = set()
    for e in trace:
        if e.get("method") != "ConfirmExecution":
            continue
        req = e.get("request", {}) or {}
        ok = (e.get("response", {}) or {}).get("success")
        if ok and req.get("request_id"):
            out.add(req["request_id"])
    return out


def confirmed_targets(trace: list[dict]) -> dict[str, int]:
    """Map each SUCCESSFULLY-CONFIRMED decision's target_table FQN to the
    confirmation's last_modified_epoch (the baseline build epoch).

    A target is "confirmed" when its decision entry's response request_id is
    acknowledged by a successful ConfirmExecution carrying that id.
    """
    # request_id -> confirm last_modified_epoch (successful only)
    confirm_epoch: dict[str, Optional[int]] = {}
    for e in trace:
        if e.get("method") != "ConfirmExecution":
            continue
        req = e.get("request", {}) or {}
        if (e.get("response", {}) or {}).get("success") and req.get("request_id"):
            confirm_epoch[req["request_id"]] = req.get("last_modified_epoch")
    out: dict[str, int] = {}
    for e in trace:
        if e.get("method") not in ("SubmitEnrichedSQL", "RegisterClone"):
            continue
        rid = _resp_request_id(e)
        tgt = (e.get("request", {}) or {}).get("target_table")
        if rid and tgt and rid in confirm_epoch:
            out[tgt] = confirm_epoch[rid]
    return out


def require_confirmed_source(trace: list[dict], clone_source: str) -> int:
    """Assert the clone_source FQN was BUILT and SUCCESSFULLY CONFIRMED in this
    trace; return its confirmed build epoch. Rejects a missing baseline."""
    if not clone_source:
        _fail("clone decision has no clone_source to correlate")
    confirmed = confirmed_targets(trace)
    if clone_source not in confirmed:
        _fail(
            f"the selected clone_source {clone_source!r} has NO successfully-"
            f"confirmed build in the trace (confirmed targets: "
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
    """Fail if a FORBIDDEN method (or a speculative submission) references the
    model anywhere in the trace — the curated chain must not hide it."""
    for e in trace:
        if model not in json.dumps(e):
            continue
        if e.get("method") in forbidden:
            _fail(f"intervening {e['method']} for {model} would be hidden by the chain filter")
        if (e.get("request", {}) or {}).get("from_speculative_submit"):
            _fail(f"speculative submission for {model} must not be filtered from the causal chain")


def register_clone_entry(trace: list[dict]) -> dict:
    """The single RegisterClone entry, or fail."""
    hits = [e for e in trace if e.get("method") == "RegisterClone"]
    if not hits:
        _fail("no RegisterClone event captured (did an incremental model clone via pre_clone?)")
    if len(hits) > 1:
        _fail(f"expected exactly one RegisterClone, got {len(hits)}")
    return hits[0]


def require_successful_register_clone(trace: list[dict]) -> dict:
    """Assert the RegisterClone returned the SUCCESSFUL ready_to_clone variant
    (NOT unable_to_clone / an error), that its clone_source_table has a
    SUCCESSFULLY-CONFIRMED source build in this trace, and that its OWN response
    request_id is acknowledged by a successful clone-creation confirm. Returns a
    dict of correlated facts. Rejects reviewer fault (c) (unable_to_clone +
    unrelated confirmation id)."""
    rc = register_clone_entry(trace)
    resp = rc.get("response", {}) or {}
    # Must be ready_to_clone (top mirror) AND response.ready_to_clone_v1 oneof.
    top = resp.get("ready_to_clone")
    v1 = (resp.get("response", {}) or {}).get("ready_to_clone_v1")
    if resp.get("unable_to_clone"):
        _fail("RegisterClone returned unable_to_clone — not a successful clone")
    if not top or not v1:
        _fail("RegisterClone response is not a populated ready_to_clone (both mirrors)")
    rid = v1.get("request_id") or top.get("request_id")
    if not rid:
        _fail("RegisterClone response carries no request_id to correlate its confirm")
    # The clone-creation confirm must acknowledge THIS request_id, successfully.
    if rid not in confirmed_request_ids(trace):
        _fail(f"the clone-creation confirm for request_id {rid} is missing or unsuccessful")
    # The clone source must have a confirmed source build.
    src = (rc.get("request", {}) or {}).get("clone_source_table")
    src_epoch = require_confirmed_source(trace, src)
    # The clone-creation confirm epoch (T3) correlated by request_id.
    t3 = None
    for e in trace:
        if e.get("method") == "ConfirmExecution":
            req = e.get("request", {}) or {}
            if req.get("request_id") == rid and (e.get("response", {}) or {}).get("success"):
                t3 = req.get("last_modified_epoch")
    return {"clone_source": src, "source_build_epoch": src_epoch, "clone_creation_epoch": t3}
