#!/usr/bin/env bash
# Probe: is the post-clone SKIP logic-sensitive?
#
# 1. Drop dev object, build source in prod, build in dev_clone -> CLONE fires.
#    (The cloned dev object has the PROD source's logic.)
# 2. Change the dev model SQL (add a column) and rebuild in dev_clone.
#    - If the hosted service EXECUTES, the post-clone reuse is LOGIC-SENSITIVE
#      (it compares the submit's SQL against the clone source's logic).
#    - If it SKIPS, reuse after a clone is PHYSICAL-ONLY (target + freshness).
#
# This disambiguates how to key clone-origin history in dbt-state-rs.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
NODE=clone_probe
cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true; rm -f models/marts/clone_probe.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

write_model() { # $1 = extra select cols
  cat > models/marts/clone_probe.sql <<SQL
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_id') }}
select order_id, customer_id, ordered_at, subtotal, tax_paid, order_total$1 from {{ ref('stg_orders') }}
{% if is_incremental() %}where ordered_at > '1900-01-01'{% endif %}
SQL
}
drop_dev(){ cat > macros/_exp_drop.sql <<'M'
{% macro _exp_drop(fqn) %}{% if 'CDO_DEMO_DB.DEV_CLONE_DEMO' not in fqn|upper %}{{ exceptions.raise_compiler_error("refuse "~fqn) }}{% endif %}{% do run_query("drop table if exists "~fqn) %}{% do run_query("drop view if exists "~fqn) %}{{ log("dropped "~fqn, info=true) }}{% endmacro %}
M
dbt run-operation _exp_drop --args "{fqn: CDO_DEMO_DB.dev_clone_demo.$NODE}" --profile snowflake --target dev_clone --skip-semantic-manifest-validation >/tmp/probe_drop.txt 2>&1 || true
rm -f macros/_exp_drop.sql; }

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

# prebuild stg_orders both targets
dbt build --profile snowflake --target prod_demo --select stg_orders --skip-semantic-manifest-validation >/dev/null 2>&1||true
dbt build --profile snowflake --target dev_clone --select stg_orders --skip-semantic-manifest-validation >/dev/null 2>&1||true

echo "### STEP A: base logic, drop dev, build prod source, build dev -> expect CLONE"
write_model ""
drop_dev
dbt build --profile snowflake --target prod_demo --select $NODE --skip-semantic-manifest-validation >/tmp/probe_prodA.txt 2>&1||true
grep -iE "Succeeded|Reused|clone" /tmp/probe_prodA.txt|tail -1
dbt build --profile snowflake --target dev_clone --select $NODE --skip-semantic-manifest-validation >/tmp/probe_devA.txt 2>&1||true
grep -iE "Succeeded|Reused|Cloned|clone" /tmp/probe_devA.txt|tail -2

echo "### STEP B: CHANGE dev model SQL (add col), rebuild dev -> logic-sensitive?"
write_model ", upper(customer_id) as _probe_logic_change"
dbt build --profile snowflake --target dev_clone --select $NODE --skip-semantic-manifest-validation >/tmp/probe_devB.txt 2>&1||true
grep -iE "Succeeded|Reused|Cloned|Error|Failed" /tmp/probe_devB.txt|tail -3

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl|head -1||true)
python3 - "$GF" "$BEFORE" <<'PY'
import json,sys
gf,before=sys.argv[1],int(sys.argv[2])
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    m=r['method']; req=r.get('request',{})or{}; resp=r.get('response',{})or{}
    tgt=str(req.get('target_table') or req.get('clone_source_table') or '')
    if 'DEV_CLONE_DEMO' not in tgt.upper() and m not in ('RegisterClone',): continue
    rr=resp.get('response',{})or{}; v=next(iter(rr.keys()),None) if isinstance(rr,dict) else None
    if m=='RegisterClone':
        print("CLONE: ready=%s src_epoch=%s"%(bool(resp.get('ready_to_clone')), req.get('clone_source_last_modified_epoch')))
    elif m=='SubmitEnrichedSQL':
        ed=(rr.get(v)or{}).get('explained_decision',{})
        print("SUBMIT %s decision=%s srr=%s crr=%s"%(tgt[-30:],v,ed.get('skip_rejection_reason'),ed.get('clone_rejection_reason')))
PY
