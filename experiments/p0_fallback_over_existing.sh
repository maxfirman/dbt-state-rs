#!/usr/bin/env bash
# Probe: does the SQL-submit cross-name clone fallback fire OVER a target that
# ALREADY has its own confirmed build history with DIFFERENT logic?
#
# This isolates the open conformance question behind review issue #1 P0#2's
# compounding gap and the fix in services.rs (gate the fallback on
# `!target_has_confirmed_history`). Table materialization uses the SQL-submit
# clone path (not RegisterClone), so it is the right vehicle.
#
# Timeline (all on CDO_DEMO_DB only):
#   A. Build model logic L1 under PROD name  (records equivalent under name A).
#   B. Drop DEV, build model logic L1 under DEV name (DEV absent) -> expect the
#      cross-name clone fallback A->DEV (ready_to_clone). Confirm it.
#   C. Rebuild DEV with DIFFERENT logic L2 (added column). DEV now holds L2 and
#      has its OWN confirmed history.
#   D. Re-submit DEV with logic L1 again. An equivalent (A, logic L1) still
#      exists under another name, AND DEV exists holding L2.
#      QUESTION: does hosted CLONE A->DEV again, or BUILD (ready_to_execute)?
#        - CLONE  => our `!target_has_confirmed_history` gate is WRONG; revert.
#        - BUILD  => our gate matches hosted (fallback only for absent target).
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
NODE=clone_probe
cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true; rm -f models/marts/clone_probe.sql macros/_exp_drop.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

write_model() { # $1 = extra select cols -> changes the logic identity
  cat > models/marts/clone_probe.sql <<SQL
{{ config(materialized='table') }}
select order_id, customer_id, ordered_at, subtotal, tax_paid, order_total$1 from {{ ref('stg_orders') }}
SQL
}
drop_obj(){ # $1 fqn  $2 target
  cat > macros/_exp_drop.sql <<'M'
{% macro _exp_drop(fqn) %}{% set u = fqn|upper %}{% if 'CDO_DEMO_DB.DEV_CLONE_DEMO' not in u and 'CDO_DEMO_DB.PROD_STATE_DEMO' not in u %}{{ exceptions.raise_compiler_error("refuse "~fqn) }}{% endif %}{% do run_query("drop table if exists "~fqn) %}{% do run_query("drop view if exists "~fqn) %}{{ log("dropped "~fqn, info=true) }}{% endmacro %}
M
  dbt run-operation _exp_drop --args "{fqn: $1}" --profile snowflake --target "$2" --skip-semantic-manifest-validation >/tmp/p0_drop.txt 2>&1 || true
  rm -f macros/_exp_drop.sql; }

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

# prebuild stg_orders both targets (outside the reporting window)
dbt build --profile snowflake --target prod_demo  --select stg_orders --skip-semantic-manifest-validation >/dev/null 2>&1||true
dbt build --profile snowflake --target dev_clone   --select stg_orders --skip-semantic-manifest-validation >/dev/null 2>&1||true

echo "### A: build L1 under PROD (equivalent under name A)"
write_model ""
drop_obj "CDO_DEMO_DB.prod_state_demo.$NODE" prod_demo
drop_obj "CDO_DEMO_DB.dev_clone_demo.$NODE"  dev_clone
dbt build --profile snowflake --target prod_demo --select $NODE --skip-semantic-manifest-validation >/tmp/p0_prodA.txt 2>&1||true
grep -iE "Succeeded|Reused|clone|Error" /tmp/p0_prodA.txt|tail -2

echo "### B: build L1 under DEV (DEV absent) -> expect cross-name CLONE A->DEV"
dbt build --profile snowflake --target dev_clone --select $NODE --skip-semantic-manifest-validation >/tmp/p0_devB.txt 2>&1||true
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/p0_devB.txt|tail -3

echo "### C: rebuild DEV with DIFFERENT logic L2 (added column) -> DEV now holds L2"
write_model ", upper(customer_id) as _probe_l2"
dbt build --profile snowflake --target dev_clone --select $NODE --skip-semantic-manifest-validation >/tmp/p0_devC.txt 2>&1||true
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/p0_devC.txt|tail -3

echo "### D: re-submit DEV with logic L1 again (equivalent A exists; DEV exists holding L2)"
echo "###    => CLONE means gate wrong; BUILD means gate matches hosted"
write_model ""
dbt build --profile snowflake --target dev_clone --select $NODE --skip-semantic-manifest-validation >/tmp/p0_devD.txt 2>&1||true
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/p0_devD.txt|tail -3

echo "### DECISIONS (DEV target only) ###"
GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
python3 - "$GF" "$BEFORE" <<'PY'
import json,sys
gf,before=sys.argv[1],int(sys.argv[2])
if not gf: print("(no golden file)"); sys.exit(0)
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
step=0
for r in rows:
    m=r['method']; req=r.get('request',{})or{}; resp=r.get('response',{})or{}
    tgt=str(req.get('target_table') or '')
    if 'DEV_CLONE_DEMO' not in tgt.upper(): continue
    rr=resp.get('response',{})or{}; v=next(iter(rr.keys()),None) if isinstance(rr,dict) else None
    if m=='SubmitEnrichedSQL' and (req.get('labels',{}) or {}).get('dbt_node_name')==None:
        pass
    if m=='SubmitEnrichedSQL':
        node=(req.get('labels',{})or{}).get('dbt_node_name','')
        if node and node!='clone_probe': continue
        ed=(rr.get(v)or{}).get('explained_decision',{}) if v else {}
        print(json.dumps({"m":m,"node":node,"tgt":tgt[-28:],"decision":v,
          "srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
          "descr":ed.get('decision_description')}))
PY
