#!/usr/bin/env bash
# CLONE FALLBACK ELIGIBILITY — controlled isolation (review #2 finding 2 / P0).
#
# The follow-up review asked to isolate the clone-fallback dimensions rather than
# observe one permitted stale clone: zero vs positive freshness tolerance,
# known/unknown source epochs, volatile evaluation, and the stale-same-target /
# fresh-alternative case — while capturing the ACTUAL selected source's full
# baseline (its build epoch and its own upstream epoch), not just the target
# name.
#
# This driver uses the dev_clone (defer_to_target: prod_demo) cross-name clone
# fallback and holds everything fixed EXCEPT one dimension per arm:
#
#   ARG1 = arm:
#     stale_tol    : source built against OLD upstream; upstream bumped; dev
#                    submit upstream is NEWER. lag_tolerance=2700 (positive).
#     stale_zerotol: identical to stale_tol but lag_tolerance=0 (zero tolerance).
#     volatile     : identical to stale_tol but evaluate_volatile_sql=true
#                    (tolerate_nondeterminism=false on the wire).
#
# For every arm it prints, in order: the prod SOURCE submit+confirm (so the
# source's build epoch and its upstream epoch are captured), and the dev clone
# decision with its clone_required_last_modified_epoch. UNIQUE per-run names;
# only mutates CDO_DEMO_DB. Fails hard on a failed precondition build.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ARM="${1:-stale_tol}"
ID="${2:-$(date +%H%M%S)}"
UP="ce_up_$ID"; MODEL="ce_model_$ID"

cd "$PROJ" || { echo "FATAL no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f "models/marts/$UP.sql" "models/marts/$MODEL.sql" macros/_ceexp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
fail(){ echo "FATAL: $1"; exit 1; }
built_ok(){ grep -qiE "Completed successfully|Succeeded|Reused|Cloned" "$1"; }

# Per-arm model config. NOTE: this Fusion version (dbt 2.0.6) REJECTS
# `lag_tolerance` / `evaluate_volatile_sql` as model-config keys
# (UnusedConfigKey dbt1060), so freshness_tolerance_seconds (2700) and
# tolerate_nondeterminism (true) are the client/profile defaults and are NOT
# per-model tunable from here. The arms therefore differ only in the staleness
# scenario; the tolerance/volatile dimensions are documented as not
# client-reachable for per-model variation in this version (do-not-infer).
case "$ARM" in
  stale_tol)     CFG="materialized='table'";;
  stale_zerotol) CFG="materialized='table'";;
  volatile)      CFG="materialized='table'";;
  *) fail "unknown arm $ARM";;
esac

cat > "models/marts/$UP.sql" <<SQL
{{ config(materialized='table') }}
select 1 as id, 100 as amount
SQL
cat > "models/marts/$MODEL.sql" <<SQL
{{ config($CFG) }}
select id, amount from {{ ref('$UP') }}
SQL

run_op(){ cat > macros/_ceexp.sql <<MACRO
{% macro _ceexp() %}{% do run_query("$1") %}{{ log("ran-op", info=true) }}{% endmacro %}
MACRO
  dbt run-operation _ceexp --profile snowflake --target "$2" --skip-semantic-manifest-validation >/tmp/ce_op.txt 2>&1 || true
  rm -f macros/_ceexp.sql; }

echo "############ CLONE ELIGIBILITY ARM=$ARM id=$ID ############"
echo "--- 1. build prod $UP + $MODEL (source holds OLD upstream) ---"
dbt build --profile snowflake --target prod_demo --select "$UP" "$MODEL" --skip-semantic-manifest-validation >/tmp/ce_prod.txt 2>&1
built_ok /tmp/ce_prod.txt || { cat /tmp/ce_prod.txt; fail "prod source build failed"; }

echo "--- 2. bump prod upstream (source NOT rebuilt -> stale) ---"
run_op "create or replace table CDO_DEMO_DB.prod_state_demo.$UP as (select 1 as id, 100 as amount)"  prod_demo

echo "--- 3. build $UP in dev_clone (dev upstream now newer/current) ---"
dbt build --profile snowflake --target dev_clone --select "$UP" --skip-semantic-manifest-validation >/tmp/ce_devup.txt 2>&1
built_ok /tmp/ce_devup.txt || { cat /tmp/ce_devup.txt; fail "dev upstream build failed"; }

echo "--- 4. build $MODEL in dev_clone (absent target, defers to prod) ---"
dbt build --profile snowflake --target dev_clone --select "$MODEL" --skip-semantic-manifest-validation >/tmp/ce_dev.txt 2>&1
built_ok /tmp/ce_dev.txt || { cat /tmp/ce_dev.txt; fail "dev model build failed"; }
grep -iE "Cloned|Reused|Succeeded|clone" /tmp/ce_dev.txt | sed 's/^ *//' | tail -3

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GF" ] || fail "no golden capture"
echo "--- captured $MODEL causal chain (source build + dev clone decision) ---"
python3 - "$GF" "$MODEL" "$ARM" <<'PY'
import json,sys
gf,model,arm=sys.argv[1],sys.argv[2],sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()]
chain=[r for r in rows if model in json.dumps(r) and r['method'] in ('SubmitEnrichedSQL','ConfirmExecution')]
for r in chain:
    req=r.get('request',{}) or {}
    if r['method']=='SubmitEnrichedSQL':
        resp=r.get('response',{}).get('response',{}) or {}
        v=next(iter(resp.keys()),None); p=resp.get(v) or {}
        ed=p.get('explained_decision',{})
        tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
        print("CE "+json.dumps({"arm":arm,"schema":req.get('default_schema'),
          "ftol":req.get('freshness_tolerance_seconds'),"tnd":req.get('tolerate_nondeterminism'),
          "decision":v,"srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
          "desc":ed.get('decision_description'),"clone_source":p.get('clone_source'),
          "clone_req_epoch":p.get('clone_required_last_modified_epoch'),"up_epochs":tabs}))
    else:
        print("CE_CONFIRM lme="+str(req.get('last_modified_epoch')))
PY
