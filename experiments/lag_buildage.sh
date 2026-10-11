#!/usr/bin/env bash
# Controlled lag BUILD-AGE experiment (single upstream, ANY policy).
#
# Isolates the question: does lag_tolerance gate on the model's OWN build age
# (wall-clock since its last build) rather than only the own-build epoch anchor?
#
# Sequence (all against prod_demo, only CDO_DEMO_DB.prod_state_demo mutated):
#   1. Fresh-build lp_up_a and lp_model3 (records model build epoch E_build).
#   2. Recreate lp_up_a in place (identical content) -> upstream epoch E_up > E_build.
#   3. Sleep DELAY seconds.
#   4. Re-submit lp_model3 and record decision + epochs + is_stale.
#
# With upstream strictly newer than the fresh build and ANY policy:
#   - If the service SKIPS while DELAY < lag_tolerance and EXECUTES when
#     DELAY > lag_tolerance, then lag_tolerance IS a build-age gate.
#   - If it EXECUTES regardless of DELAY, the own-build-epoch anchor holds and
#     lag_tolerance does not gate.
#
# Usage: lag_buildage.sh LABEL LAG DELAY
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/lag_golden}"
PORT="${PORT:-50099}"
LABEL="$1"; LAG="$2"; DELAY="$3"
cd "$PROJ"; git restore . 2>/dev/null||true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true; rm -f models/marts/lp_up_a.sql models/marts/lp_model3.sql macros/_exp_recreate.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

cat > models/marts/lp_up_a.sql <<'SQL'
{{ config(materialized='table') }}
select 1 as id, 'a' as val
SQL
cat > models/marts/lp_model3.sql <<SQL
{{ config(materialized='table', state={'lag_tolerance': '$LAG', 'require_fresh_data_from': 'any'}) }}
select id from {{ ref('lp_up_a') }}
SQL

echo "############ $LABEL lag=$LAG delay=${DELAY}s ############"
# 1) Fresh build the model (force a real build by full-refresh so build epoch is NOW).
dbt build --profile snowflake --target prod_demo --select +lp_model3 --full-refresh --skip-semantic-manifest-validation >/tmp/lba_build.txt 2>&1 || true
grep -iE "Succeeded|Reused|Error" /tmp/lba_build.txt | sed 's/^ *//' | tail -3
GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

# 2) Advance upstream epoch in place (identical content).
cat > macros/_exp_recreate.sql <<'MACRO'
{% macro _exp_recreate() %}
  {% do run_query("create or replace table CDO_DEMO_DB.prod_state_demo.lp_up_a as select 1 as id, 'a' as val") %}
  {{ log("recreated lp_up_a", info=true) }}
{% endmacro %}
MACRO
dbt run-operation _exp_recreate --profile snowflake --target prod_demo --skip-semantic-manifest-validation >/tmp/lba_recreate.txt 2>&1 || true
rm -f macros/_exp_recreate.sql

# 3) Sleep to age the build relative to lag_tolerance.
echo "--- sleeping ${DELAY}s to age the build ---"; sleep "$DELAY"

# 4) Re-submit (no upstream rebuild).
dbt build --profile snowflake --target prod_demo --select lp_model3 --skip-semantic-manifest-validation >/tmp/lba_resubmit.txt 2>&1 || true
grep -iE "Reused|Succeeded|lag tolerance|Error" /tmp/lba_resubmit.txt | sed 's/^ *//' | tail -3

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
python3 - "$GF" "$BEFORE" "$LABEL" "$LAG" "$DELAY" <<'PY'
import json,sys
gf,before,label,lag,delay=sys.argv[1],int(sys.argv[2]),sys.argv[3],sys.argv[4],sys.argv[5]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']
    if req.get('labels',{}).get('dbt_node_name','')!='lp_model3': continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); ed=(resp.get(v) or {}).get('explained_decision',{})
    tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
    own=tabs.get('LP_MODEL3'); up=tabs.get('LP_UP_A')
    gap=None if (own is None or up is None) else (up-own)/1000.0
    print("DEC "+json.dumps({"label":label,"lag":lag,"delay_s":int(delay),"decision":v,
      "tol_s":req.get('freshness_tolerance_seconds'),"is_stale":ed.get('is_stale'),
      "own_build":own,"upstream":up,"upstream_minus_build_s":gap}))
PY
