#!/usr/bin/env python3
"""Shared slicer for `dbt state explain` captures.

Renders Explain.GetExplainMessages + Explain.GetUpstreamDependencyChanges
entries (and the Submit/Clone decisions they reference) from a golden JSONL,
mapping proto enum ints back to names. Used by explain_experiment.sh (#20),
explain_clone_deferral.sh (#23) and explain_tolerance.sh (#24).

Usage: explain_slice.py <golden.jsonl> <before_line_count>
"""
import json
import sys
from collections import Counter

RESULT = {0: "SKIP_EXECUTION", 1: "READY_TO_EXECUTE", 3: "READY_TO_CLONE", 4: "UNKNOWN"}
MARKER = {0: "EM_UNSPECIFIED", 1: "SUCCESS", 2: "FAIL", 3: "INFO"}
BADGE = {
    0: "EB_UNSPECIFIED", 1: "FRESH", 2: "WITHIN_TOLERANCE", 3: "OUTDATED",
    4: "NODE_QUERY_CHANGED", 5: "NODE_QUERY_UNCHANGED", 6: "UPSTREAM_QUERY_CHANGED",
    7: "UPSTREAM_QUERY_UNCHANGED", 8: "TARGET_TABLE_EXISTS",
}


def _name(d, key, table):
    v = d.get(key)
    return table.get(v, v) if v is not None else None


def render_lines(lines, depth=0, out=None):
    out = out if out is not None else []
    for ln in (lines or []):
        mk = _name(ln, "marker", MARKER)
        bd = _name(ln, "badge", BADGE)
        tag = " ".join(t for t in [f"[{mk}]" if mk else "", f"<{bd}>" if bd else ""] if t)
        out.append("  " * depth + f"- {ln.get('text', '')}  {tag}".rstrip())
        render_lines(ln.get("children"), depth + 1, out)
    return out


def submit_decision(r):
    """Return (variant, execution_decision_id, request_id, explained_decision) for a Submit row."""
    resp = (r.get("response", {}) or {}).get("response", {}) or {}
    variant = next(iter(resp.keys()), None)
    inner = resp.get(variant) or {}
    return variant, inner.get("execution_decision_id"), inner.get("request_id"), (inner.get("explained_decision") or {})


def slice_window(rows):
    census = Counter((r["service"], r["method"]) for r in rows)
    print("## METHOD CENSUS (capture window)")
    for (svc, m), n in sorted(census.items()):
        print(f"  {svc}.{m}: {n}")
    print()

    print("## Explain.GetUpstreamDependencyChanges")
    any_udc = False
    for r in rows:
        if r.get("method") != "GetUpstreamDependencyChanges":
            continue
        any_udc = True
        req = r.get("request", {}) or {}
        resp = r.get("response", {}) or {}
        print("  request.execution_ids =", json.dumps(req.get("execution_ids", [])))
        for dc in resp.get("dependency_changes", []) or []:
            print(f"  execution_id={dc.get('execution_id')}")
            for line in render_lines(dc.get("lines")):
                print("    " + line)
    if not any_udc:
        print("  (none fired in this window)")
    print()

    print("## Explain.GetExplainMessages")
    for r in rows:
        if r.get("method") != "GetExplainMessages":
            continue
        req = r.get("request", {}) or {}
        resp = r.get("response", {}) or {}
        print("  request.execution_decision_ids =", json.dumps(req.get("execution_decision_ids", [])))
        for e in resp.get("messages", []) or []:
            dec = _name(e, "decision", RESULT)
            print(f"  --- entry execution_decision_id={e.get('execution_decision_id')} decision={dec} ---")
            print(f"      decision_description: {e.get('decision_description')!r}")
            for line in render_lines(e.get("explain_lines")):
                print("      " + line)
    print()

    print("## Submit / Clone decisions this window (edid -> node, for correlation)")
    for r in rows:
        m = r.get("method", "")
        req = r.get("request", {}) or {}
        node = (req.get("labels", {}) or {}).get("dbt_node_name", "")
        if m in ("SubmitEnrichedSQL", "SubmitValues"):
            variant, edid, rid, ed = submit_decision(r)
            print(f"  SUBMIT node={node} et={req.get('execution_type')} variant={variant} "
                  f"edid={edid} request_id={rid} decision={_name(ed, 'decision', RESULT)} "
                  f"is_stale={ed.get('is_stale')} tol_s={req.get('freshness_tolerance_seconds')} "
                  f"desc={ed.get('decision_description')!r}")
        elif m == "RegisterClone":
            resp = r.get("response", {}) or {}
            rc = resp.get("ready_to_clone") or {}
            utc = resp.get("unable_to_clone") or {}
            ed = rc.get("explained_decision") or utc.get("explained_decision") or {}
            print(f"  CLONE src={req.get('clone_source_table')} tgt={req.get('target_table')} "
                  f"ready={bool(rc)} unable={bool(utc)} "
                  f"edid={(rc or utc).get('execution_decision_id')} "
                  f"decision={_name(ed, 'decision', RESULT)} desc={ed.get('decision_description')!r}")
        elif m == "ResolveDeferredRelations":
            resp = r.get("response", {}) or {}
            fqns = resp.get("fqn_by_unique_id", {}) or {}
            print(f"  RESOLVE_DEFERRED n_ids={len(req.get('node_unique_ids', []))} n_resolved={len(fqns)}")


def main():
    gf, before = sys.argv[1], int(sys.argv[2])
    if not gf:
        print("(no golden file)")
        return
    rows = [json.loads(l) for l in open(gf) if l.strip()][before:]
    slice_window(rows)


if __name__ == "__main__":
    main()
