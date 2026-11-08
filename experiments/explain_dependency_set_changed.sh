#!/usr/bin/env bash
# Capture real `dbt state explain` traffic for a node that hits a DEPENDENCY-SET
# CHANGE — the node's genuine upstream NAME SET changes between a first confirmed
# build and a second submit, while the node's own SQL/config fingerprint stays
# matchable (ticket #32). Our engine's `decision::upstream_freshness` returns
# `FreshnessEvidence::DependencySetChanged` for exactly this, and
# `UpstreamFreshness.name_set_diff` (added/removed/ambiguous) is computed but
# UNRENDERED (`TODO(#30 follow-up)`). We have NEVER captured what the hosted
# service (api.state.dbt.com) renders here. This driver characterizes it live.
#
# THE HARD PART: in normal dbt a `ref()` appears in the rendered SQL, so changing
# an upstream changes the node's own SQL too -> NODE_QUERY_CHANGED EXECUTE, which
# is NOT the dependency-set-change path. We must change the node's resolved
# upstream *physical table set* (the `tables` closure dbt sends in
# SubmitEnrichedSQLRequest.tables) while keeping the node's rendered SQL
# byte-identical. The real service's `tables` field carries the TRANSITIVE
# closure of physical upstreams (verified: order_items lists RAW_* sources behind
# its staging refs). So we hold the consumer's SQL constant and change what its
# single upstream resolves to BEHIND the ref:
#
#   A1 = view: select 1 as id
#   A2 = view: select 2 as id
#   B  = view: select * from {{ ref('A1') }}      (phase 1)  then  ref('A2') (phase 2)
#   C  = table: select * from {{ ref('B') }}      (CONSTANT rendered SQL both phases)
#
# Phase 1: build A1,A2,B(->A1),C. C's tables closure = {C, B, A1}.
# Phase 2: repoint B to A2 (B's own SQL changes -> B EXECUTEs, expected), rebuild
#   B, then re-submit C. C's rendered SQL is unchanged (still `select * from B`)
#   so node_sql_hash matches the phase-1 row, but its upstream closure is now
#   {C, B, A2}: A1 removed, A2 added -> a genuine upstream NAME-SET change on C
#   with a matchable own fingerprint. We then drive `dbt state explain` on C.
#
# We DISCOVER whether the hosted service actually (a) lands C on a dependency-set
# EXECUTE vs a node-query EXECUTE vs a SKIP, and (b) what the explain tree says —
# whether the added/removed dependency names are surfaced. The recorded Submit
# decision for C is the oracle for which path hosted took BEFORE we read explain.
#
# A SECOND scenario (ambiguous/duplicate) is attempted if the first does not
# produce a dependency-set change, per the ticket's candidate list.
#
# DISCIPLINE: clean jaffle-shop tree, `git restore` on exit, UNIQUE per-run
# names, only ever mutate CDO_DEMO_DB. Snowflake only (DuckDB does not trigger
# State). Run experiments/proxy_up.sh first.
set -uo pipefail
PROJ=~/projects/jaffle-shop
REPO=/home/develop/projects/dbt-state-rs
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
A1="dsc_a1_$ID"; A2="dsc_a2_$ID"; B="dsc_b_$ID"; C="dsc_c_$ID"

cd "$PROJ" || { echo "FATAL: no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$A1.sql models/marts/$A2.sql models/marts/$B.sql models/marts/$C.sql; }
trap cleanup EXIT
mkdir -p models/marts
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
DBT="dbt --profile snowflake --target prod_demo --skip-semantic-manifest-validation"
fail(){ echo "FATAL: $1"; exit 1; }

if ! (timeout 2 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  fail "recording proxy not listening on 127.0.0.1:$PORT — run experiments/proxy_up.sh first"
fi

gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
lc(){ local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }

# $1 = which upstream B points at (A1 or A2)
write_models(){
  local bdep="$1"
  cat > models/marts/$A1.sql <<SQL
{{ config(materialized='view') }}
select 1 as id
SQL
  cat > models/marts/$A2.sql <<SQL
{{ config(materialized='view') }}
select 2 as id
SQL
  cat > models/marts/$B.sql <<SQL
{{ config(materialized='view') }}
select * from {{ ref('$bdep') }}
SQL
  # C's rendered SQL is CONSTANT across both phases: `select * from <B fqn>`.
  cat > models/marts/$C.sql <<SQL
{{ config(materialized='table') }}
select * from {{ ref('$B') }}
SQL
}

run_build(){ # $1=log $2=expected_ok $3+=select
  local log="$1" expected="$2"; shift 2
  $DBT build --select "$@" >"$log" 2>&1
  local rc=$?
  [ $rc -eq 0 ] || { cat "$log"; fail "dbt build exited $rc"; }
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected nodes ok"; }
}

echo "############ EXPLAIN: DEPENDENCY-SET CHANGE id=$ID ############"

echo "### phase 1: build A1,A2,B(->$A1),C  (C upstream closure = {C,B,A1}) ###"
write_models "$A1"
run_build /tmp/dsc_p1.txt 4 "$A1" "$A2" "$B" "$C"
grep -iE "Succeeded|Reused|Completed" /tmp/dsc_p1.txt | sed 's/^ *//' | tail -5

echo
echo "### phase 2a: repoint B to $A2 and rebuild B (B's own SQL changed -> EXECUTE) ###"
write_models "$A2"
run_build /tmp/dsc_p2b.txt 1 "$B"
grep -iE "Succeeded|Reused|Completed" /tmp/dsc_p2b.txt | sed 's/^ *//' | tail -3

echo
echo "### phase 2b: re-submit C (rendered SQL unchanged; upstream closure now {C,B,$A2}) ###"
P2=$(lc)
# `dbt build` on C alone: same rendered SQL, changed upstream name set.
$DBT build --select "$C" >/tmp/dsc_p2c.txt 2>&1 || true
grep -iE "Succeeded|Reused|Cloned|Completed|SKIP|skip" /tmp/dsc_p2c.txt | sed 's/^ *//' | tail -5

echo
echo "### phase 3: dbt state explain (plain + verbose) on C ###"
$DBT state explain --select "$C" >/tmp/dsc_explain.txt 2>&1 || true
sed 's/^ *//' /tmp/dsc_explain.txt | tail -25
$DBT state explain --verbose --select "$C" >/tmp/dsc_explain_v.txt 2>&1 || true
echo "--- verbose ---"
sed 's/^ *//' /tmp/dsc_explain_v.txt | tail -40

echo
echo "############ CAPTURED TRAFFIC — phase2/explain window (C = $C) ############"
python3 "$REPO/experiments/explain_slice.py" "$(gf)" "$P2"

echo
echo "############ ORACLE: C's Submit decision + tables closure both phases ############"
python3 - "$(gf)" "$C" "$A1" "$A2" <<'PY'
import json, sys
gf, C, A1, A2 = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
rows = [json.loads(l) for l in open(gf) if l.strip()]
RESULT = {0: "SKIP_EXECUTION", 1: "READY_TO_EXECUTE", 3: "READY_TO_CLONE", 4: "UNKNOWN"}
subs = []
for r in rows:
    if r.get("method") != "SubmitEnrichedSQL":
        continue
    req = r.get("request", {}) or {}
    if (req.get("labels", {}) or {}).get("dbt_node_name") != C:
        continue
    resp = (r.get("response", {}) or {}).get("response", {}) or {}
    v = next(iter(resp.keys()), None)
    p = resp.get(v) or {}
    ed = p.get("explained_decision", {}) or {}
    tables = [t.get("name") for t in req.get("tables", [])]
    subs.append({
        "decision": v,
        "decision_int": ed.get("decision"),
        "decision_name": RESULT.get(ed.get("decision")),
        "srr": ed.get("skip_rejection_reason"),
        "is_stale": ed.get("is_stale"),
        "desc": ed.get("decision_description"),
        "has_A1": any(A1.upper() in (t or "").upper() for t in tables),
        "has_A2": any(A2.upper() in (t or "").upper() for t in tables),
        "tables": tables,
        "sql": req.get("sql", "").strip(),
    })
print(f"captured {len(subs)} Submit(s) for C={C}:")
for i, s in enumerate(subs):
    print(f"  [{i}] decision={s['decision']} ({s['decision_name']}) srr={s['srr']} "
          f"is_stale={s['is_stale']} hasA1={s['has_A1']} hasA2={s['has_A2']}")
    print(f"      desc={s['desc']!r}")
    print(f"      tables={s['tables']}")
    print(f"      sql={s['sql']!r}")

# FINDING classification (printed, not asserted — this is DISCOVERY, #32).
if len(subs) >= 2:
    first, last = subs[0], subs[-1]
    same_sql = first["sql"] == last["sql"]
    set_changed = (first["has_A1"] and not last["has_A1"]) and (last["has_A2"] and not first["has_A2"])
    print()
    print(f"FINDING: C rendered SQL identical across phases: {same_sql}")
    print(f"FINDING: C upstream name set changed A1->A2: {set_changed}")
    print(f"FINDING: phase-2 C decision: {last['decision_name']} (desc={last['desc']!r})")
    if same_sql and set_changed:
        print("=> Dependency-set change is REACHABLE live with a matchable own fingerprint.")
    elif not same_sql:
        print("=> WARNING: C's rendered SQL differed; hosted may have seen NODE_QUERY_CHANGED, "
              "not a pure dependency-set change. Iterate the scenario.")
    else:
        print("=> C's upstream set did not change as intended; iterate the scenario.")
else:
    print("=> fewer than 2 C submits captured; cannot compare. Check build logs.")
PY

echo
echo "NOTE: if a dependency-set change was reached, promote the newest golden to"
echo "  golden/fixtures/explain_dependency_set_changed.jsonl (trim to C's two Submits,"
echo "  the ConfirmExecution for phase 1, and the Explain RPCs). Record in the #32"
echo "  resolution comment what hosted rendered: sections, line text, markers, badges,"
echo "  and WHETHER the added/removed dependency names (A1/A2) are surfaced anywhere."
