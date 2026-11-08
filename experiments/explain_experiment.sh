#!/usr/bin/env bash
# Capture real `dbt state explain` traffic against the hosted service (ticket #20
# of map #19 — "faithful warehouse-general dbt state explain support").
#
# GOAL: ground the explain spec in evidence instead of inference. Drive the
# Fusion client through the recording proxy so every Explain RPC
# (GetExplainMessages, GetUpstreamDependencyChanges) and the Submit decisions
# they reference are captured to a golden JSONL, then slice + summarise the
# newly-captured Explain entries to answer the ticket's required findings.
#
# SNOWFLAKE HAPPY PATH on jaffle-shop (prod_demo target):
#   1. Full `dbt build --target prod_demo` (records every node's state so a
#      second run can SKIP the unchanged ones).
#   2. `dbt state explain` (plain) + `dbt state explain --verbose` on the
#      all-fresh tree  -> every node a SKIP, data tests included.
#   3. Change ONE staging model's SQL (adds a harmless column) so the next build
#      EXECUTEs it + its downstream while the rest SKIP. Rebuild.
#   4. `dbt state explain` (plain) + `dbt state explain --verbose` again  -> now
#      a SKIP / EXECUTE / DATA-TEST mix in one explain.
#
# This gives, in one capture: a SKIP (unchanged model), an EXECUTE (changed
# model + downstream), and a DATA TEST (jaffle-shop's generic not_null/unique/
# expression_is_true tests), so we see both the plain summary line and the
# `--verbose` ExplainLine tree per node kind.
#
# Clone/deferral explain is explicitly a FOLLOW-ON capture (map "Not yet
# specified"), NOT this ticket.
#
# DISCIPLINE: starts from a clean jaffle-shop tree, `git restore`s on exit, and
# only ever mutates CDO_DEMO_DB (target prod_demo -> schema prod_state_demo).
# Run the recording proxy first:  experiments/proxy_up.sh
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
PROFILE="${PROFILE:-snowflake}"
TARGET="${TARGET:-prod_demo}"

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true; }
trap cleanup EXIT

export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
DBT="dbt --profile $PROFILE --target $TARGET --skip-semantic-manifest-validation"

gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
lc(){ local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }

# Confirm the proxy is actually listening before we burn a Snowflake build.
if ! (timeout 2 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  echo "!! recording proxy not listening on 127.0.0.1:$PORT — run experiments/proxy_up.sh first" >&2
  exit 1
fi

echo "############ EXPLAIN CAPTURE (target $TARGET) ############"

echo "### 1. full build in $TARGET (records node state) ###"
$DBT build >/tmp/exp_build1.txt 2>&1 || true
grep -iE "Reused|Succeeded|Error|Failed|Completed|Done" /tmp/exp_build1.txt | sed 's/^ *//' | tail -6

echo "### 2a. state explain (plain) on all-fresh tree ###"
A=$(lc)
$DBT state explain >/tmp/exp_explain_fresh.txt 2>&1 || true
sed 's/^ *//' /tmp/exp_explain_fresh.txt | tail -30

echo "### 2b. state explain --verbose on all-fresh tree ###"
$DBT state explain --verbose >/tmp/exp_explain_fresh_v.txt 2>&1 || true
sed 's/^ *//' /tmp/exp_explain_fresh_v.txt | tail -40

echo "### 3. change ONE staging model (stg_customers) to force an EXECUTE ###"
# Add a harmless constant column to the FINAL projection: alters the rendered SQL
# -> NODE_QUERY_CHANGED on stg_customers and UPSTREAM_QUERY_CHANGED on its
# downstream (customers), while every other node stays unchanged (SKIP).
# stg_customers.sql ends in `select * from renamed`; rewrite only that line.
python3 - <<'PY'
import pathlib
p = pathlib.Path("models/staging/stg_customers.sql")
src = p.read_text()
old = "select * from renamed"
new = "select *, 1 as _explain_probe from renamed"
assert old in src, "expected final projection `select * from renamed` not found"
p.write_text(src.replace(old, new))
PY
tail -3 models/staging/stg_customers.sql

echo "### 3b. rebuild (EXECUTE changed + downstream, SKIP the rest) ###"
$DBT build >/tmp/exp_build2.txt 2>&1 || true
grep -iE "Reused|Succeeded|Error|Failed|Completed|Done" /tmp/exp_build2.txt | sed 's/^ *//' | tail -8

echo "### 4a. state explain (plain) on the mixed tree ###"
B=$(lc)
$DBT state explain >/tmp/exp_explain_mixed.txt 2>&1 || true
sed 's/^ *//' /tmp/exp_explain_mixed.txt | tail -30

echo "### 4b. state explain --verbose on the mixed tree ###"
$DBT state explain --verbose >/tmp/exp_explain_mixed_v.txt 2>&1 || true
sed 's/^ *//' /tmp/exp_explain_mixed_v.txt | tail -60

echo
echo "############ CAPTURED EXPLAIN TRAFFIC (entries after full build) ############"
python3 - "$(gf)" "$A" <<'PY'
import json, sys
gf, before = sys.argv[1], int(sys.argv[2])
rows = [json.loads(l) for l in open(gf) if l.strip()][before:]

RESULT = {0: "SKIP_EXECUTION", 1: "READY_TO_EXECUTE", 3: "READY_TO_CLONE", 4: "UNKNOWN"}
MARKER = {0: "EM_UNSPECIFIED", 1: "SUCCESS", 2: "FAIL", 3: "INFO"}
BADGE  = {0: "EB_UNSPECIFIED", 1: "FRESH", 2: "WITHIN_TOLERANCE", 3: "OUTDATED",
          4: "NODE_QUERY_CHANGED", 5: "NODE_QUERY_UNCHANGED", 6: "UPSTREAM_QUERY_CHANGED",
          7: "UPSTREAM_QUERY_UNCHANGED", 8: "TARGET_TABLE_EXISTS"}

def name(d, k, table):
    v = d.get(k)
    return table.get(v, v) if v is not None else None

def render_lines(lines, depth=0):
    out = []
    for ln in (lines or []):
        prefix = "  " * depth
        marker = name(ln, "marker", MARKER)
        badge = name(ln, "badge", BADGE)
        tag = " ".join(t for t in [f"[{marker}]" if marker else "", f"<{badge}>" if badge else ""] if t)
        out.append(f"{prefix}- {ln.get('text','')}  {tag}".rstrip())
        out += render_lines(ln.get("children"), depth + 1)
    return out

# --- method census: which RPCs fired, and how many ---
from collections import Counter
census = Counter((r["service"], r["method"]) for r in rows)
print("## METHOD CENSUS (this capture window)")
for (svc, m), n in sorted(census.items()):
    print(f"  {svc}.{m}: {n}")
print()

# --- GetUpstreamDependencyChanges: what execution_ids are sent + lines returned ---
print("## Explain.GetUpstreamDependencyChanges")
for r in rows:
    if r["method"] != "GetUpstreamDependencyChanges":
        continue
    req = r.get("request", {}) or {}
    resp = r.get("response", {}) or {}
    print("  request.execution_ids =", json.dumps(req.get("execution_ids", [])))
    for dc in resp.get("dependency_changes", []) or []:
        print(f"  execution_id={dc.get('execution_id')}")
        for line in render_lines(dc.get("lines")):
            print("    " + line)
print()

# --- GetExplainMessages: the real ExplainMessageEntry tree per node ---
print("## Explain.GetExplainMessages")
for r in rows:
    if r["method"] != "GetExplainMessages":
        continue
    req = r.get("request", {}) or {}
    resp = r.get("response", {}) or {}
    print("  request.execution_decision_ids =", json.dumps(req.get("execution_decision_ids", [])))
    for e in resp.get("messages", []) or []:
        dec = name(e, "decision", RESULT)
        print(f"  --- entry execution_decision_id={e.get('execution_decision_id')} "
              f"decision={dec} ---")
        print(f"      decision_description: {e.get('decision_description')!r}")
        for line in render_lines(e.get("explain_lines")):
            print("      " + line)
print()

# --- Submit decisions in this window: correlate execution_decision_id -> node ---
print("## SUBMIT decisions this window (execution_decision_id -> node, for finding #2 correlation)")
for r in rows:
    if r["method"] not in ("SubmitEnrichedSQL", "SubmitValues"):
        continue
    req = r.get("request", {}) or {}
    node = (req.get("labels", {}) or {}).get("dbt_node_name", "")
    et = req.get("execution_type")
    resp = (r.get("response", {}) or {}).get("response", {}) or {}
    variant = next(iter(resp.keys()), None)
    inner = resp.get(variant) or {}
    edid = inner.get("execution_decision_id")
    rid = inner.get("request_id")
    ed = inner.get("explained_decision", {}) or {}
    print(f"  node={node} exec_type={et} variant={variant} "
          f"execution_decision_id={edid} request_id={rid} "
          f"decision={name(ed,'decision',RESULT)} desc={ed.get('decision_description')!r}")
PY

echo
echo "NOTE: promote the newest golden file to golden/fixtures/ once reviewed:"
echo "  cp \"$(gf)\" <repo>/golden/fixtures/explain_snowflake_happy.jsonl  # after trimming to Explain + referenced Submit entries"
