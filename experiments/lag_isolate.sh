#!/usr/bin/env bash
# RIGOROUS lag_tolerance gating isolation (single upstream, ANY policy).
#
# Overturns/confirms whether lag_tolerance gates a rebuild. Controls the clock
# precisely and submits at several delays after an upstream data change.
#
# Clean sequence (drops all objects first; only CDO_DEMO_DB.prod_state_demo):
#   t0  drop lp_up_a + lg_model; build both fresh (model incorporates upstream v1)
#   t1  recreate lp_up_a in place (identical content) -> upstream v2 epoch > build
#   then re-submit lg_model at a sequence of DELAYS, logging wall-clock, the
#   captured epochs, build-age, upstream-age, decision, is_stale, description.
#
# A single run submits at: 5s, then (LAG+15)s after the upstream change, so one
# run brackets the lag_tolerance boundary.
#
# Usage: lag_isolate.sh LAG     (LAG e.g. 60s)
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/lag_golden}"
PORT="${PORT:-50099}"
LAG="$1"
LAG_S=${LAG%s}
cd "$PROJ"; git restore . 2>/dev/null||true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true; rm -f models/marts/lp_up_a.sql models/marts/lg_model.sql macros/_exp_m.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

cat > models/marts/lp_up_a.sql <<'SQL'
{{ config(materialized='table') }}
select 1 as id, 'a' as val
SQL
cat > models/marts/lg_model.sql <<SQL
{{ config(materialized='table', state={'lag_tolerance': '$LAG', 'require_fresh_data_from': 'any'}) }}
select id from {{ ref('lp_up_a') }}
SQL

op(){ cat > macros/_exp_m.sql <<MACRO
{% macro _exp_m(stmt) %}{% do run_query(stmt) %}{{ log("ran: " ~ stmt, info=true) }}{% endmacro %}
MACRO
dbt run-operation _exp_m --args "{stmt: \"$1\"}" --profile snowflake --target prod_demo --skip-semantic-manifest-validation >/tmp/lag_iso_op.txt 2>&1 || true
rm -f macros/_exp_m.sql; }

echo "############ lag_isolate LAG=$LAG ############"
op "drop table if exists CDO_DEMO_DB.prod_state_demo.lg_model"
op "drop table if exists CDO_DEMO_DB.prod_state_demo.lp_up_a"
echo "[t0 $(date +%s)] fresh build model+upstream"
dbt build --profile snowflake --target prod_demo --select +lg_model --skip-semantic-manifest-validation >/tmp/lag_iso_build.txt 2>&1 || true
grep -iE "Succeeded|Reused" /tmp/lag_iso_build.txt | tail -2

echo "[t1 $(date +%s)] recreate upstream (v2, identical content)"
op "create or replace table CDO_DEMO_DB.prod_state_demo.lp_up_a as select 1 as id, 'a' as val"
UP_CHANGE=$(date +%s)

submit_at(){ # $1 = delay seconds since upstream change
  local target=$(( UP_CHANGE + $1 )) nows
  nows=$(date +%s)
  local wait=$(( target - nows )); [ "$wait" -gt 0 ] && sleep "$wait"
  GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl|head -1); local B=$(wc -l<"$GF")
  local age=$(( $(date +%s) - UP_CHANGE ))
  dbt build --profile snowflake --target prod_demo --select lg_model --skip-semantic-manifest-validation >/tmp/lag_iso_sub.txt 2>&1 || true
  GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl|head -1)
  python3 - "$GF" "$B" "$age" "$LAG" <<'PY'
import json,sys
gf,b,age,lag=sys.argv[1],int(sys.argv[2]),sys.argv[3],sys.argv[4]
rows=[json.loads(l) for l in open(gf) if l.strip()][b:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']
    if req.get('labels',{}).get('dbt_node_name','')!='lg_model': continue
    resp=r.get('response',{}).get('response',{}) or {}; v=next(iter(resp.keys()),None)
    ed=(resp.get(v) or {}).get('explained_decision',{})
    t={x['name'].split('.')[-1].strip('"'):x.get('last_modified_epoch') for x in req.get('tables',[])}
    own=t.get('LG_MODEL'); up=t.get('LP_UP_A')
    gap=None if (own is None or up is None) else round((up-own)/1000.0,1)
    print("DEC "+json.dumps({"lag":lag,"since_upstream_change_s":int(age),"decision":v,
      "is_stale":ed.get('is_stale'),"upstream_minus_build_s":gap,
      "desc":ed.get('decision_description','')[:80]}))
PY
}

echo "--- submit ~8s after upstream change (within lag=$LAG) ---"; submit_at 8
echo "--- submit ~$((LAG_S+20))s after upstream change (beyond lag=$LAG) ---"; submit_at $((LAG_S+20))
