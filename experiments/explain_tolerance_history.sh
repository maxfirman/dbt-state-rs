#!/usr/bin/env bash
# #34: HISTORY-BEARING re-capture of the within-lag-tolerance SKIP + upstream-
# OUTDATED EXECUTE explain trees (replaces the empty-store gaps
# explain_tolerance.jsonl entries 1 & 3).
#
# The original explain_tolerance.jsonl captured the SKIP/EXECUTE explain but the
# establishing build's ConfirmExecution fell OUTSIDE the promoted slice, so on
# empty-store replay ET_MODEL first-builds and the re-derived tree diverges in
# decision CLASS. This driver captures ONE session in which the baseline
# build+ConfirmExecution, the epoch bump, and every `state explain` all live in
# the SAME window, so the promoted fixture seeds the replay store from real
# hosted traffic and the main conformance gate asserts the hosted SKIP / OUTDATED
# EXECUTE trees directly (no MISSING-HISTORY gap).
#
# Mechanics mirror explain_tolerance.sh: a constant-content upstream (et_up) and
# a model (et_model) with a LARGE lag_tolerance over it. We:
#   1. build et_up + et_model  -> records et_model's Submit(EXECUTE)+Confirm
#      (this is the PRIOR STATE the replay needs).
#   2. re-submit et_model UNCHANGED -> genuinely-fresh SKIP; state explain ->
#      the SKIP tree with a per-upstream FRESH listing, asserted against SEEDED
#      history (its hosted decision_description is the lag-tolerance variant,
#      which our engine does not emit — a field-scoped gap, NOT missing history).
#   3. bump et_up's epoch -> the upstream is OUTDATED (verified live: lag_tolerance
#      does NOT absorb the bump, so this is an EXECUTE, not a within-tolerance
#      SKIP, which is unreachable on this hosted version) -> state explain ->
#      the SHAPE-9 OUTDATED-upstream EXECUTE tree.
#
# DISCIPLINE: clean tree, git restore on exit, unique per-run names, CDO_DEMO_DB
# only, Snowflake only. Run experiments/proxy_up.sh first.
set -uo pipefail
PROJ=~/projects/jaffle-shop
REPO=/home/develop/projects/dbt-state-rs
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
LAG="${LAG:-240h}"
ID="${1:-$(date +%H%M%S)}"
UP="eth_up_$ID"; MODEL="eth_model_$ID"

cd "$PROJ" || { echo "FATAL: no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$UP.sql models/marts/$MODEL.sql macros/_eth_recreate.sql 2>/dev/null || true; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
DBT="dbt --profile snowflake --target prod_demo --skip-semantic-manifest-validation"
fail(){ echo "FATAL: $1"; exit 1; }
if ! (timeout 2 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  fail "recording proxy not listening on 127.0.0.1:$PORT — run experiments/proxy_up.sh first"
fi
gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
lc(){ local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }
run_build(){ local log="$1" expected="$2"; shift 2
  $DBT build --select "$@" >"$log" 2>&1; local rc=$?
  [ $rc -eq 0 ] || { cat "$log"; fail "dbt build exited $rc"; }
  # A node that "Warned" still built (e.g. clone-fallback-to-execute); count it.
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned|Warned" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected nodes ok"; }
}

cat > models/marts/$UP.sql <<SQL
{{ config(materialized='table') }}
select 1 as id, '$ID' as val
SQL
cat > models/marts/$MODEL.sql <<SQL
{{ config(materialized='table', state={'lag_tolerance': '$LAG', 'require_fresh_data_from': 'any'}) }}
select id, val from {{ ref('$UP') }}
SQL

recreate_up(){ # recreate et_up in place, identical content -> advance epoch only
  local fqn="CDO_DEMO_DB.prod_state_demo.$UP"
  cat > macros/_eth_recreate.sql <<MACRO
{% macro _eth_recreate() %}
  {% do run_query("create or replace table $fqn as (select 1 as id, '$ID' as val)") %}
  {{ log("recreated $fqn (epoch advanced, content identical)", info=true) }}
{% endmacro %}
MACRO
  $DBT run-operation _eth_recreate >/tmp/eth_recreate.txt 2>&1 || true
  rm -f macros/_eth_recreate.sql
  tail -2 /tmp/eth_recreate.txt
}

echo "############ #34 HISTORY-BEARING TOLERANCE (lag=$LAG) id=$ID ############"
# Slice START: capture the baseline build+Confirm IN WINDOW so replay seeds state.
START=$(lc)

echo "### 1. baseline build (et_up + et_model) — records Submit(EXECUTE)+Confirm ###"
run_build /tmp/eth_base.txt 2 "+$MODEL"
grep -iE "Succeeded|Reused" /tmp/eth_base.txt | sed 's/^ *//' | tail -3

echo "### 2. re-submit UNCHANGED -> genuinely-fresh SKIP over seeded history ###"
# A second build with NO upstream change: the model is fresh against its own
# recorded build epoch -> hosted SKIP. This is the history-bearing SKIP tree.
$DBT build --select "$MODEL" >/tmp/eth_skip_build.txt 2>&1 || true
grep -iE "Reused|Succeeded|no-op|SKIP" /tmp/eth_skip_build.txt | sed 's/^ *//' | tail -2
$DBT state explain --verbose --select "$MODEL" >/tmp/eth_skip.txt 2>&1 || true
echo "--- fresh-SKIP explain ---"; sed 's/^ *//' /tmp/eth_skip.txt | tail -18

echo "### 3. bump upstream epoch -> upstream OUTDATED -> SHAPE-9 EXECUTE ###"
# Verified live (docs/explain-catalog.md): lag_tolerance does NOT absorb an
# upstream epoch bump, so this yields the OUTDATED-upstream EXECUTE tree
# (NOT a within-tolerance SKIP, which is unreachable on this hosted version).
recreate_up
$DBT build --select "$MODEL" >/tmp/eth_outdated_build.txt 2>&1 || true
grep -iE "Reused|Succeeded" /tmp/eth_outdated_build.txt | sed 's/^ *//' | tail -2
$DBT state explain --verbose --select "$MODEL" >/tmp/eth_outdated.txt 2>&1 || true
echo "--- OUTDATED-upstream EXECUTE explain ---"; sed 's/^ *//' /tmp/eth_outdated.txt | tail -20

echo
echo "############ CAPTURED TRAFFIC — full history-bearing window ############"
python3 "$REPO/experiments/explain_slice.py" "$(gf)" "$START"

echo
echo "############ ORACLE: window has the model's Submit + ConfirmExecution ############"
python3 - "$(gf)" "$START" "$MODEL" <<'PY'
import json, sys
gf, start, model = sys.argv[1], int(sys.argv[2]), sys.argv[3]
rows = [json.loads(l) for l in open(gf) if l.strip()][start:]
subs = [r for r in rows if r.get("method") in ("SubmitEnrichedSQL",)
        and (r.get("request",{}).get("labels",{}) or {}).get("dbt_node_name")==model]
confs = [r for r in rows if r.get("method")=="ConfirmExecution"
         and (r.get("request",{}).get("labels",{}) or {}).get("dbt_node_name")==model]
explains = [r for r in rows if r.get("method")=="GetExplainMessages"]
print(f"model={model}: submits={len(subs)} confirms={len(confs)} explain_calls={len(explains)}")
ok = len(subs) >= 1 and len(confs) >= 1 and len(explains) >= 1
print("HISTORY-BEARING:" , "YES — baseline Submit+Confirm and explain all in window" if ok
      else "NO — window is missing the Submit/Confirm/explain; widen the slice")
if not ok: sys.exit(1)
PY

echo
echo "NOTE: promote the newest golden to golden/fixtures/explain_tolerance_history.jsonl"
echo "  (trim to: baseline Submit(EXECUTE)+ConfirmExecution for $MODEL and $UP, the"
echo "   within-tolerance re-Submit, and the GetExplainMessages calls — drop telemetry)."
echo "  Then remove explain_tolerance.jsonl entries 1 & 3 from golden/replay_gaps.json."
