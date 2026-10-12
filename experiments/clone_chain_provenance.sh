#!/usr/bin/env bash
# REGISTERCLONE / CLONE-CHAIN PROVENANCE (review #2 finding 4, P0).
#
# The follow-up review's concern: a clone inherits the SOURCE's logic identity
# but the server anchors a later same-target reuse decision on the CLONE's own
# build epoch, ignoring that the clone's data may predate the current upstream.
# The reviewer gives local counterexamples (source@100, input@200, clone@300 ->
# SKIP). The honest question is what the HOSTED service does on the clone chain:
#
#   1. prod builds the source S over upstream U@E1.
#   2. dev build of an absent target T defers to prod and CLONES S into T
#      (ready_to_clone), confirmed.
#   3. the dev upstream U is bumped (E2 > clone build epoch).
#   4. dev re-submits T over U@E2. Does hosted SKIP (reuse the clone) or EXECUTE
#      (rebuild because the upstream advanced past the clone)?
#
# This isolates clone-content-age vs clone-creation-age: if hosted EXECUTES at
# step 4, a clone's freshness is anchored such that an advanced upstream
# invalidates it (the server must match); if hosted SKIPS, the clone is reused
# despite the newer upstream (the server already does this via the own-build
# anchor).
#
# UNIQUE per-run names; only mutates CDO_DEMO_DB. Fails hard on failed builds.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
UP="cc_up_$ID"; MODEL="cc_model_$ID"

cd "$PROJ" || { echo "FATAL no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f "models/marts/$UP.sql" "models/marts/$MODEL.sql" macros/_ccexp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
fail(){ echo "FATAL: $1"; exit 1; }
built_ok(){ grep -qiE "Completed successfully|Succeeded|Reused|Cloned|No-op|no-op" "$1"; }

cat > "models/marts/$UP.sql" <<SQL
{{ config(materialized='table') }}
select 1 as id, 100 as amount
SQL
cat > "models/marts/$MODEL.sql" <<SQL
{{ config(materialized='table') }}
select id, amount from {{ ref('$UP') }}
SQL

run_op(){ cat > macros/_ccexp.sql <<MACRO
{% macro _ccexp() %}{% do run_query("$1") %}{{ log("ran-op", info=true) }}{% endmacro %}
MACRO
  dbt run-operation _ccexp --profile snowflake --target "$2" --skip-semantic-manifest-validation >/tmp/cc_op.txt 2>&1 || true
  rm -f macros/_ccexp.sql; }

echo "############ CLONE-CHAIN PROVENANCE id=$ID ############"
echo "--- 1. prod build $UP + $MODEL (source S over U@E1) ---"
dbt build --profile snowflake --target prod_demo --select "$UP" "$MODEL" --skip-semantic-manifest-validation >/tmp/cc_prod.txt 2>&1
built_ok /tmp/cc_prod.txt || { cat /tmp/cc_prod.txt; fail "prod build failed"; }

echo "--- 2. dev build $UP, then $MODEL (absent target -> CLONE S into T) ---"
dbt build --profile snowflake --target dev_clone --select "$UP" --skip-semantic-manifest-validation >/tmp/cc_devup.txt 2>&1
built_ok /tmp/cc_devup.txt || { cat /tmp/cc_devup.txt; fail "dev upstream build failed"; }
dbt build --profile snowflake --target dev_clone --select "$MODEL" --skip-semantic-manifest-validation >/tmp/cc_devclone.txt 2>&1
built_ok /tmp/cc_devclone.txt || { cat /tmp/cc_devclone.txt; fail "dev clone build failed"; }
grep -iE "Cloned|Reused|Succeeded" /tmp/cc_devclone.txt | sed 's/^ *//' | tail -2

echo "--- 3. bump dev upstream $UP (E2 > clone build epoch) ---"
run_op "create or replace table CDO_DEMO_DB.dev_clone_demo.$UP as (select 1 as id, 100 as amount)" dev_clone

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l < "$GF")

echo "--- 4. dev re-submit $MODEL over U@E2 (upstream advanced past clone) ---"
dbt build --profile snowflake --target dev_clone --select "$MODEL" --skip-semantic-manifest-validation >/tmp/cc_dev2.txt 2>&1
built_ok /tmp/cc_dev2.txt || { cat /tmp/cc_dev2.txt; fail "dev re-submit failed"; }
grep -iE "Cloned|Reused|Succeeded|No-op|no-op" /tmp/cc_dev2.txt | sed 's/^ *//' | tail -3

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GF" ] || fail "no golden capture"
echo "--- full $MODEL causal chain (clone + post-upstream-bump re-submit) ---"
python3 - "$GF" "$MODEL" <<'PY'
import json,sys
gf,model=sys.argv[1],sys.argv[2]
rows=[json.loads(l) for l in open(gf) if l.strip()]
chain=[r for r in rows if model in json.dumps(r) and r['method'] in ('SubmitEnrichedSQL','ConfirmExecution','RegisterClone')]
for r in chain:
    req=r.get('request',{}) or {}
    if r['method']=='ConfirmExecution':
        print("CC_CONFIRM lme="+str(req.get('last_modified_epoch'))); continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); p=resp.get(v) or {}
    ed=p.get('explained_decision',{})
    tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
    print("CC "+json.dumps({"method":r['method'],"schema":req.get('default_schema'),
      "decision":v,"srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
      "desc":ed.get('decision_description'),"clone_source":p.get('clone_source'),
      "clone_req_epoch":p.get('clone_required_last_modified_epoch'),
      "is_stale":ed.get('is_stale'),"up":tabs}))
PY
