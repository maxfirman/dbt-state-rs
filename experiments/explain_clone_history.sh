#!/usr/bin/env bash
# #34: HISTORY-BEARING re-capture of the CLONE explain trees (replaces the
# empty-store MISSING-SOURCE-HISTORY gaps explain_clone_deferral.jsonl entries
# 3/4/5/7/8).
#
# The original explain_clone_deferral.jsonl built the clone SOURCE in a prior
# live session OUTSIDE the promoted slice, so on empty-store replay the dev
# target cannot clone (no source) and first-builds — a decision-class divergence.
# This driver captures ONE window in which the clone SOURCE build+Confirm (prod),
# the dev CLONE (ready_to_clone) + its Confirm, and the dev `state explain` all
# live together, so the promoted fixture seeds the source and the main gate
# asserts the hosted CLONE explain tree directly.
#
# Flow (lifted from explain_clone_deferral.sh, source build kept IN WINDOW):
#   1. build the clone source in prod_demo               -> Submit(EXECUTE)+Confirm
#   2. drop the dev target, build it in dev_clone        -> ready_to_clone + Confirm
#   3. dbt state explain (dev_clone) on the clone target -> CLONE explain tree
#
# DISCIPLINE: clean tree, git restore on exit, unique per-run names, only ever
# mutate CDO_DEMO_DB (prod_state_demo + dev_clone_demo). Snowflake only. Run
# experiments/proxy_up.sh first.
set -uo pipefail
PROJ=~/projects/jaffle-shop
REPO=/home/develop/projects/dbt-state-rs
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
SRC="ch_src_$ID"; NODE="ch_probe_$ID"

cd "$PROJ" || { echo "FATAL: no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$SRC.sql models/marts/$NODE.sql macros/_chd_drop.sql 2>/dev/null || true; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
DBT_PROD="dbt --profile snowflake --target prod_demo --skip-semantic-manifest-validation"
DBT_DEV="dbt --profile snowflake --target dev_clone --skip-semantic-manifest-validation"
fail(){ echo "FATAL: $1"; exit 1; }
if ! (timeout 2 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  fail "recording proxy not listening on 127.0.0.1:$PORT — run experiments/proxy_up.sh first"
fi
gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
lc(){ local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }

# Source model + a clone-eligible probe that selects it (unique content so no
# pre-existing cross-env equivalent interferes).
cat > models/marts/$SRC.sql <<SQL
{{ config(materialized='table') }}
select 1 as id, '$ID' as tag
SQL
cat > models/marts/$NODE.sql <<SQL
{{ config(materialized='table') }}
select id, tag from {{ ref('$SRC') }}
SQL

drop_obj(){ # strictly scoped to CDO_DEMO_DB demo schemas
  local obj="$1" target="$2"
  cat > macros/_chd_drop.sql <<'MACRO'
{% macro _chd_drop(fqn) %}
  {% set u = fqn | upper %}
  {% if 'CDO_DEMO_DB.DEV_CLONE_DEMO' not in u and 'CDO_DEMO_DB.PROD_STATE_DEMO' not in u %}
    {{ exceptions.raise_compiler_error("refusing to drop outside CDO_DEMO_DB demo schemas: " ~ fqn) }}
  {% endif %}
  {% do run_query("drop table if exists " ~ fqn) %}
  {% do run_query("drop view if exists " ~ fqn) %}
  {{ log("dropped " ~ fqn, info=true) }}
{% endmacro %}
MACRO
  dbt run-operation _chd_drop --args "{fqn: $obj}" --profile snowflake --target "$target" \
    --skip-semantic-manifest-validation >/tmp/chd_drop.txt 2>&1 || true
  rm -f macros/_chd_drop.sql
  tail -1 /tmp/chd_drop.txt
}

echo "############ #34 HISTORY-BEARING CLONE explain id=$ID ############"

echo "### [reset] drop probe + source in both schemas so clone can fire fresh ###"
drop_obj "CDO_DEMO_DB.dev_clone_demo.$NODE"  dev_clone
drop_obj "CDO_DEMO_DB.prod_state_demo.$NODE" prod_demo
drop_obj "CDO_DEMO_DB.dev_clone_demo.$SRC"   dev_clone
drop_obj "CDO_DEMO_DB.prod_state_demo.$SRC"  prod_demo

# Slice START: the clone SOURCE build+Confirm must be IN WINDOW.
START=$(lc)

echo "### 1. build clone SOURCE ($NODE) in prod_demo -> Submit(EXECUTE)+Confirm ###"
$DBT_PROD build --select "$SRC" "$NODE" >/tmp/chd_prod.txt 2>&1 || true
grep -iE "Succeeded|Reused|Cloned|Warned|Error|Failed" /tmp/chd_prod.txt | sed 's/^ *//' | tail -6
grep -qiE "Succeeded|Reused|Cloned|Warned" /tmp/chd_prod.txt || fail "prod source build produced no built node"

echo "### 1b. build the UPSTREAM in dev_clone too, so dev \$NODE reads the DEV copy ###"
# Without this, the dev \$NODE submit resolves its upstream to the PROD schema
# (deferral), making its SQL reference TWO schemas (prod upstream + dev self) —
# which changes the schema-abstracted namespace-logic identity vs the prod build
# (single schema) and defeats the cross-name clone equivalence match. Building
# the upstream in dev keeps the dev submit SCHEMA-UNIFORM (dev only), so its
# logic identity collapses to the same relative schema token as prod's.
$DBT_DEV build --select "$SRC" >/tmp/chd_dev_src.txt 2>&1 || true
grep -iE "Succeeded|Reused|Cloned|Warned" /tmp/chd_dev_src.txt | sed 's/^ *//' | tail -2

echo "### 2. build in dev_clone (expect CLONE from prod source) ###"
$DBT_DEV build --select "$NODE" >/tmp/chd_dev.txt 2>&1 || true
grep -iE "Reused|Succeeded|Cloned|clone|Error|Failed" /tmp/chd_dev.txt | sed 's/^ *//' | tail -8

echo "### 3. state explain --verbose (dev_clone) on the cloned target ###"
$DBT_DEV state explain --verbose --select "$NODE" >/tmp/chd_explain.txt 2>&1 || true
sed 's/^ *//' /tmp/chd_explain.txt | tail -30

echo
echo "############ CAPTURED TRAFFIC — full history-bearing window ############"
python3 "$REPO/experiments/explain_slice.py" "$(gf)" "$START"

echo
echo "############ ORACLE: window has source EXECUTE+Confirm then dev CLONE + explain ############"
python3 - "$(gf)" "$START" "$NODE" "$SRC" <<'PY'
import json, sys
gf, start, node, src = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
rows = [json.loads(l) for l in open(gf) if l.strip()][start:]
def kind(r):
    resp=(r.get("response",{}) or {}).get("response",{}) or {}
    return next(iter(resp.keys()), None)
def node_of(r): return (r.get("request",{}).get("labels",{}) or {}).get("dbt_node_name","")
src_exec = [r for r in rows if r.get("method")=="SubmitEnrichedSQL" and node_of(r)==src and kind(r)=="ready_to_execute"]
clones   = [r for r in rows if r.get("method")=="SubmitEnrichedSQL" and node_of(r)==node and kind(r)=="ready_to_clone"]
confs    = [r for r in rows if r.get("method")=="ConfirmExecution"]
explains = [r for r in rows if r.get("method")=="GetExplainMessages"]
print(f"source-EXECUTE={len(src_exec)} dev-CLONE={len(clones)} confirms={len(confs)} explain_calls={len(explains)}")
ok = len(src_exec)>=1 and len(clones)>=1 and len(confs)>=1 and len(explains)>=1
print("HISTORY-BEARING CLONE:", "YES" if ok else "NO — dev did not clone (check reset / source build)")
if not ok: sys.exit(1)
PY

echo
echo "NOTE: promote the newest golden to golden/fixtures/explain_clone_history.jsonl"
echo "  (trim to the prod source Submit(EXECUTE)+Confirm, the dev ready_to_clone + Confirm,"
echo "   and the GetExplainMessages call; drop telemetry). Remove explain_clone_deferral.jsonl"
echo "   entries 3/4/5/7/8 from golden/replay_gaps.json once the CLONE tree is asserted."
