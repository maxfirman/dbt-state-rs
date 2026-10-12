#!/usr/bin/env bash
# Characterize the PHYSICAL-STATE-BEFORE-FILTERS invariant for the
# execution_type transition (review issue #1 follow-up, P1 variant 1).
#
# find_latest_confirmed_target filters by execution_type BEFORE picking the
# latest confirmed row for the target. The reviewer's concern: if a target's
# CURRENT physical state is a different materialization strategy than an older
# (clone-origin) row, the older row can still be selected for clone adoption.
#
# ModelExecutionType: FULL=1 (table), MERGE=3 (incremental merge). A dbt model
# switched from materialized='table' to materialized='incremental' sends et=1
# then et=3 for the SAME physical target -> this transition IS client-reachable.
#
# Timeline (CDO_DEMO_DB only):
#   A. Build model logic L1 as TABLE (et=1) under PROD name (equivalent source).
#   B. Build the SAME model as TABLE (et=1) under DEV (absent) -> cross-name
#      CLONE A->DEV (clone-origin row, et=1, logic L1). Confirm.
#   C. Rebuild DEV as INCREMENTAL (et=3). DEV's CURRENT physical state is now an
#      ordinary et=3 build; the et=1 clone-origin row is HISTORICAL.
#   D. Re-submit DEV as TABLE (et=1) with logic L1 again.
#      QUESTION: does hosted reuse the stale et=1 clone-origin row (SKIP), or
#      does the current et=3 state block that stale reuse (BUILD / re-CLONE)?
#        - SKIP via the stale et=1 row  => our latest-first fix would be WRONG.
#        - anything but that stale SKIP => fix is safe & hosted-consistent.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_ptf}"
PORT="${PORT:-50099}"
NODE=ptf_probe
cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true; rm -f models/marts/${NODE}.sql macros/_exp_drop.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

write_table()  { cat > models/marts/${NODE}.sql <<SQL
{{ config(materialized='table') }}
select order_id, customer_id, ordered_at from {{ ref('stg_orders') }}
SQL
}
write_incr()   { cat > models/marts/${NODE}.sql <<SQL
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_id') }}
select order_id, customer_id, ordered_at from {{ ref('stg_orders') }}
{% if is_incremental() %}where ordered_at > (select max(ordered_at) from {{ this }}){% endif %}
SQL
}
drop_obj(){ cat > macros/_exp_drop.sql <<'M'
{% macro _exp_drop(fqn) %}{% set u = fqn|upper %}{% if 'CDO_DEMO_DB.DEV_CLONE_DEMO' not in u and 'CDO_DEMO_DB.PROD_STATE_DEMO' not in u %}{{ exceptions.raise_compiler_error("refuse "~fqn) }}{% endif %}{% do run_query("drop table if exists "~fqn) %}{% do run_query("drop view if exists "~fqn) %}{{ log("dropped "~fqn, info=true) }}{% endmacro %}
M
  dbt run-operation _exp_drop --args "{fqn: $1}" --profile snowflake --target "$2" --skip-semantic-manifest-validation >/tmp/ptf_drop.txt 2>&1 || true
  rm -f macros/_exp_drop.sql; }

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")
dbt build --profile snowflake --target prod_demo --select stg_orders --skip-semantic-manifest-validation >/dev/null 2>&1||true
dbt build --profile snowflake --target dev_clone  --select stg_orders --skip-semantic-manifest-validation >/dev/null 2>&1||true

echo "### A: build L1 as TABLE under PROD"
write_table
drop_obj "CDO_DEMO_DB.prod_state_demo.$NODE" prod_demo
drop_obj "CDO_DEMO_DB.dev_clone_demo.$NODE"  dev_clone
dbt build --profile snowflake --target prod_demo --select $NODE --skip-semantic-manifest-validation >/tmp/ptf_A.txt 2>&1||true
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/ptf_A.txt|tail -2

echo "### B: build L1 as TABLE under DEV (absent) -> expect cross-name CLONE"
dbt build --profile snowflake --target dev_clone --select $NODE --skip-semantic-manifest-validation >/tmp/ptf_B.txt 2>&1||true
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/ptf_B.txt|tail -3

echo "### C: rebuild DEV as INCREMENTAL (et=3) -> DEV current state is et=3"
write_incr
dbt build --profile snowflake --target dev_clone --select $NODE --full-refresh --skip-semantic-manifest-validation >/tmp/ptf_C1.txt 2>&1||true
# second run so it takes the incremental (merge, et=3) path, not full-refresh
dbt build --profile snowflake --target dev_clone --select $NODE --skip-semantic-manifest-validation >/tmp/ptf_C2.txt 2>&1||true
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/ptf_C2.txt|tail -3

echo "### D: re-submit DEV as TABLE (et=1), logic L1 again"
write_table
dbt build --profile snowflake --target dev_clone --select $NODE --skip-semantic-manifest-validation >/tmp/ptf_D.txt 2>&1||true
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/ptf_D.txt|tail -3

echo "### DECISIONS (DEV target, node=$NODE) ###"
GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
python3 - "$GF" "$BEFORE" "$NODE" <<'PY'
import json,sys
gf,before,node=sys.argv[1],int(sys.argv[2]),sys.argv[3]
if not gf: print("(no golden file)"); sys.exit(0)
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    m=r['method']; req=r.get('request',{})or{}; resp=r.get('response',{})or{}
    tgt=str(req.get('target_table') or '')
    if 'DEV_CLONE_DEMO' not in tgt.upper(): continue
    if m not in ('SubmitEnrichedSQL','Clone'): continue
    nd=(req.get('labels',{})or{}).get('dbt_node_name','')
    if nd and nd!=node: continue
    rr=resp.get('response',{})or{}; v=next(iter(rr.keys()),None) if isinstance(rr,dict) else None
    ed=(rr.get(v)or{}).get('explained_decision',{}) if (v and isinstance(rr.get(v),dict)) else {}
    print(json.dumps({"m":m,"et":req.get('execution_type'),"decision":v,
      "srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
      "descr":ed.get('decision_description')}))
PY
