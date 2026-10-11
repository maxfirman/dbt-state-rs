#!/usr/bin/env bash
# Lag / data-freshness experiment driver (multi-upstream).
#
# Builds, in prod_demo, two INDEPENDENT upstream tables (lp_up_a, lp_up_b) and a
# model lp_model2 selecting from BOTH, with a configurable lag_tolerance and
# require_fresh_data_from (any|all). It then advances the modification epoch of
# selected upstream(s) by RECREATING them in place with IDENTICAL content
# (create or replace table ... as select <constant>), which changes ONLY the
# warehouse modification time, isolating pure DATA freshness from the
# upstream-logic-change bypass. Finally it re-submits lp_model2 and records the
# decision.
#
# Usage:
#   lag_experiment.sh LABEL POLICY LAG BUMP
#     POLICY : any | all
#     LAG    : lag_tolerance string, e.g. 5m, 0, 24h
#     BUMP   : which upstream(s) to recreate (advance epoch): none|a|b|both
#
# Only mutates CDO_DEMO_DB.prod_state_demo.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/lag_golden}"
PORT="${PORT:-50099}"
LABEL="$1"; POLICY="$2"; LAG="$3"; BUMP="$4"
cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true; rm -f models/marts/lp_up_a.sql models/marts/lp_up_b.sql models/marts/lp_model2.sql macros/_exp_recreate.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

# Two independent upstream tables (table materialization) with constant content.
cat > models/marts/lp_up_a.sql <<'SQL'
{{ config(materialized='table') }}
select 1 as id, 'a' as val
SQL
cat > models/marts/lp_up_b.sql <<'SQL'
{{ config(materialized='table') }}
select 2 as id, 'b' as val
SQL
# The model under test: selects from BOTH upstreams, with lag + policy config.
cat > models/marts/lp_model2.sql <<SQL
{{ config(materialized='table', state={'lag_tolerance': '$LAG', 'require_fresh_data_from': '$POLICY'}) }}
select a.id as a_id, b.id as b_id from {{ ref('lp_up_a') }} a cross join {{ ref('lp_up_b') }} b
SQL

recreate_upstream(){ # $1 = a|b ; recreate in place with IDENTICAL content -> advances epoch only
  local which="$1" val
  [ "$which" = "a" ] && val="select 1 as id, 'a' as val" || val="select 2 as id, 'b' as val"
  local fqn="CDO_DEMO_DB.prod_state_demo.lp_up_$which"
  cat > macros/_exp_recreate.sql <<MACRO
{% macro _exp_recreate() %}
  {% do run_query("create or replace table $fqn as $val") %}
  {{ log("recreated $fqn (epoch advanced, content identical)", info=true) }}
{% endmacro %}
MACRO
  dbt run-operation _exp_recreate --profile snowflake --target prod_demo --skip-semantic-manifest-validation >/tmp/lag_recreate_$which.txt 2>&1 || true
  rm -f macros/_exp_recreate.sql
  tail -2 /tmp/lag_recreate_$which.txt
}

echo "############ $LABEL policy=$POLICY lag=$LAG bump=$BUMP ############"
# 1) Build everything in prod so lp_model2 has a confirmed baseline.
echo "--- build lp_model2 (+upstreams) baseline ---"
dbt build --profile snowflake --target prod_demo --select +lp_model2 --skip-semantic-manifest-validation >/tmp/lag_base.txt 2>&1 || true
grep -iE "Reused|Succeeded|Error|Failed" /tmp/lag_base.txt | sed 's/^ *//' | tail -6

# Snapshot golden after baseline so only the re-submit decision is reported.
GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

# 2) Advance upstream epoch(s) in place (pure data freshness, no logic change).
case "$BUMP" in
  a) recreate_upstream a ;;
  b) recreate_upstream b ;;
  both) recreate_upstream a; recreate_upstream b ;;
  none) echo "(no upstream bump)" ;;
esac

# 3) Re-submit lp_model2 ONLY (do not rebuild upstreams) and capture the decision.
echo "--- re-submit lp_model2 after bump=$BUMP ---"
dbt build --profile snowflake --target prod_demo --select lp_model2 --skip-semantic-manifest-validation >/tmp/lag_resubmit.txt 2>&1 || true
grep -iE "Reused|Succeeded|Error|Failed" /tmp/lag_resubmit.txt | sed 's/^ *//' | tail -4

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
python3 - "$GF" "$BEFORE" "$LABEL" <<'PY'
import json,sys
gf,before,label=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']; name=req.get('labels',{}).get('dbt_node_name','')
    if name!='lp_model2': continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None)
    ed=(resp.get(v) or {}).get('explained_decision',{})
    tol=req.get('freshness_tolerance_seconds'); pol=req.get('stale_upstream_policy')
    tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
    print("DEC "+json.dumps({"label":label,"node":name,"decision":v,
      "tol_s":tol,"policy":pol,"srr":ed.get('skip_rejection_reason'),
      "is_stale":ed.get('is_stale'),"tables":tabs}))
PY
