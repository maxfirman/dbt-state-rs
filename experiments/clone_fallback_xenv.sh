#!/usr/bin/env bash
# Cross-environment clone-fallback freshness (issue #2, finding 2a) — PROPER.
#
# Uses the dev_clone (defer_to_target: prod_demo, allow_clones: true) flow where
# the cross-name/cross-schema clone fallback is KNOWN to fire: a dev build of an
# absent target defers to prod and the hosted service offers to CLONE the prod
# object into the dev target.
#
# QUESTION: does the hosted service still offer the clone when the prod SOURCE
# was built against an OLDER upstream than the dev submit's upstream (i.e. is the
# offered clone source freshness-gated against the requested upstream)?
#
# ARMS (ARG1 = fresh|stale), UNIQUE per-run names (hosted history is NOT reset by
# dropping warehouse objects, so every run uses a fresh model/table name):
#   fresh : build prod source AFTER the upstream bump (source incorporates E2),
#           then dev build. Expect ready_to_clone (control: fallback fires).
#   stale : build prod source BEFORE the upstream bump (source holds E1), bump
#           the upstream to E2, then dev build whose upstream is E2. If hosted
#           still offers ready_to_clone from the E1 source, that is the review's
#           "stale source offered" claim; if it executes, hosted gates on source
#           freshness.
#
# Only mutates CDO_DEMO_DB (prod_state_demo + dev_clone_demo).
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ARM="${1:-stale}"
ID="${2:-$(date +%H%M%S)}"
UP="cf2_up_$ID"
MODEL="cf2_model_$ID"

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true;
  rm -f "models/marts/$UP.sql" "models/marts/$MODEL.sql" macros/_exp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

cat > "models/marts/$UP.sql" <<SQL
{{ config(materialized='table') }}
select 1 as id, 100 as amount
SQL
cat > "models/marts/$MODEL.sql" <<SQL
{{ config(materialized='table') }}
select id, amount from {{ ref('$UP') }}
SQL

run_op(){ cat > macros/_exp.sql <<MACRO
{% macro _exp() %}{% do run_query("$1") %}{{ log("ran: $1", info=true) }}{% endmacro %}
MACRO
  dbt run-operation _exp --profile snowflake --target "$2" --skip-semantic-manifest-validation >/tmp/cf2_op.txt 2>&1 || true
  rm -f macros/_exp.sql; tail -1 /tmp/cf2_op.txt; }

bump_prod_upstream(){ run_op "create or replace table CDO_DEMO_DB.prod_state_demo.$UP as (select 1 as id, 100 as amount)" prod_demo; }

echo "############ CROSS-ENV CLONE FALLBACK ($ARM) id=$ID ############"

# Fail hard on a failed precondition build (review #2 testing bullet: experiment
# drivers must not continue past a failed setup/build/query step).
fail(){ echo "FATAL: $1"; exit 1; }
built_ok(){ grep -qiE "Completed successfully|Succeeded|Reused|Cloned" "$1"; }

if [ "$ARM" = "stale" ]; then
  echo "--- 1. build prod $UP + $MODEL (source holds E1) ---"
  dbt build --profile snowflake --target prod_demo --select "$UP" "$MODEL" --skip-semantic-manifest-validation >/tmp/cf2_prod.txt 2>&1
  built_ok /tmp/cf2_prod.txt || { cat /tmp/cf2_prod.txt; fail "prod source build failed"; }
  grep -iE "Succeeded|Reused|Error" /tmp/cf2_prod.txt | sed 's/^ *//' | tail -3
  echo "--- 2. bump prod upstream E1->E2 (source $MODEL NOT rebuilt, stays stale) ---"
  bump_prod_upstream
else
  echo "--- 1. build prod $UP only ---"
  dbt build --profile snowflake --target prod_demo --select "$UP" --skip-semantic-manifest-validation >/tmp/cf2_prod0.txt 2>&1
  built_ok /tmp/cf2_prod0.txt || { cat /tmp/cf2_prod0.txt; fail "prod upstream build failed"; }
  echo "--- 1b. bump prod upstream, THEN build $MODEL (source incorporates E2) ---"
  bump_prod_upstream
  dbt build --profile snowflake --target prod_demo --select "$MODEL" --skip-semantic-manifest-validation >/tmp/cf2_prod.txt 2>&1
  built_ok /tmp/cf2_prod.txt || { cat /tmp/cf2_prod.txt; fail "prod model build failed"; }
  grep -iE "Succeeded|Reused|Error" /tmp/cf2_prod.txt | sed 's/^ *//' | tail -3
fi

# Build the upstream in dev_clone so the dev submit's ref resolves + is current.
echo "--- 3. build $UP in dev_clone (dev upstream now current/E2) ---"
dbt build --profile snowflake --target dev_clone --select "$UP" --skip-semantic-manifest-validation >/tmp/cf2_dev_up.txt 2>&1
built_ok /tmp/cf2_dev_up.txt || { cat /tmp/cf2_dev_up.txt; fail "dev upstream build failed"; }
grep -iE "Succeeded|Reused|Cloned|Error" /tmp/cf2_dev_up.txt | sed 's/^ *//' | tail -2

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

echo "--- 4. build $MODEL in dev_clone (absent target, defers to prod) ---"
dbt build --profile snowflake --target dev_clone --select "$MODEL" --skip-semantic-manifest-validation >/tmp/cf2_dev.txt 2>&1
built_ok /tmp/cf2_dev.txt || { cat /tmp/cf2_dev.txt; fail "dev model build failed"; }
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/cf2_dev.txt | sed 's/^ *//' | tail -6

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
echo "--- captured dev $MODEL decision ---"
python3 - "$GF" "$BEFORE" "$MODEL" "$ARM" <<'PY'
import json,sys
gf,before,model,arm=sys.argv[1],int(sys.argv[2]),sys.argv[3],sys.argv[4]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']; name=req.get('labels',{}).get('dbt_node_name','')
    if name!=model: continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); payload=resp.get(v) or {}
    ed=payload.get('explained_decision',{})
    tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
    print("CF2 "+json.dumps({"arm":arm,"node":name,"decision":v,
      "srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
      "desc":ed.get('decision_description'),
      "clone_source":payload.get('clone_source'),
      "clone_req_epoch":payload.get('clone_required_last_modified_epoch'),
      "default_schema":req.get('default_schema'),"tables":tabs}))
PY
