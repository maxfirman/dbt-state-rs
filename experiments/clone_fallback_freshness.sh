#!/usr/bin/env bash
# Clone-fallback freshness experiment (issue #2, finding 2a).
#
# QUESTION: the SQL-submit cross-name clone fallback offers an equivalent model
# built under ANOTHER name to clone into an absent target. Does the hosted
# service still offer that source when the source was built over an OLDER
# upstream than the current submit requests (i.e. is the offered clone source
# freshness-gated against the requested upstream)?
#
# SETUP (prod_demo only, CDO_DEMO_DB.prod_state_demo):
#   - cf_up        : table, constant content (the shared upstream).
#   - cf_src       : table selecting from {{ ref('cf_up') }} (the equivalent
#                    SOURCE under one name).
#   - cf_tgt       : table with an IDENTICAL body to cf_src (the same
#                    schema-abstracted logic under a DIFFERENT physical name).
#
# PROCEDURE:
#   1. Build cf_up + cf_src (records cf_src's ns-logic + its upstream epoch E1).
#   2. Advance cf_up's epoch in place (identical content) to E2 > E1, WITHOUT
#      rebuilding cf_src (so cf_src remains built against the stale E1).
#   3. Build cf_tgt (absent target, same abstracted logic, upstream now E2).
#      Capture its SubmitEnrichedSQL decision.
#
# A ready_to_clone offering cf_src (built against E1) would be the review's
# "stale source offered" claim. A ready_to_execute means hosted does NOT offer a
# stale equivalent. We only report what hosted does.
#
# Only mutates CDO_DEMO_DB.prod_state_demo.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true;
  rm -f models/marts/cf_up.sql models/marts/cf_src.sql models/marts/cf_tgt.sql macros/_exp_recreate.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

cat > models/marts/cf_up.sql <<'SQL'
{{ config(materialized='table') }}
select 1 as id, 100 as amount
SQL
# cf_src and cf_tgt share an IDENTICAL body (same schema-abstracted logic).
BODY="select id, amount from {{ ref('cf_up') }}"
cat > models/marts/cf_src.sql <<SQL
{{ config(materialized='table') }}
$BODY
SQL
cat > models/marts/cf_tgt.sql <<SQL
{{ config(materialized='table') }}
$BODY
SQL

recreate_up(){ # advance cf_up epoch, identical content
  local fqn="CDO_DEMO_DB.prod_state_demo.cf_up"
  cat > macros/_exp_recreate.sql <<MACRO
{% macro _exp_recreate() %}
  {% do run_query("create or replace table $fqn as (select 1 as id, 100 as amount)") %}
  {{ log("recreated $fqn (epoch advanced, content identical)", info=true) }}
{% endmacro %}
MACRO
  dbt run-operation _exp_recreate --profile snowflake --target prod_demo --skip-semantic-manifest-validation >/tmp/cf_recreate.txt 2>&1 || true
  rm -f macros/_exp_recreate.sql
  tail -1 /tmp/cf_recreate.txt
}

echo "############ CLONE FALLBACK FRESHNESS (finding 2a) ############"
echo "--- 1. build cf_up + cf_src (source equivalent, upstream E1) ---"
dbt build --profile snowflake --target prod_demo --select cf_up cf_src --skip-semantic-manifest-validation >/tmp/cf_base.txt 2>&1 || true
grep -iE "Reused|Succeeded|Error|Failed" /tmp/cf_base.txt | sed 's/^ *//' | tail -6

echo "--- 2. advance cf_up epoch E1 -> E2 (cf_src NOT rebuilt, stays stale) ---"
recreate_up

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

echo "--- 3. build cf_tgt (absent target, same abstracted logic, upstream E2) ---"
dbt build --profile snowflake --target prod_demo --select cf_tgt --skip-semantic-manifest-validation >/tmp/cf_tgt.txt 2>&1 || true
grep -iE "Reused|Succeeded|Cloned|clone|Error|Failed" /tmp/cf_tgt.txt | sed 's/^ *//' | tail -8

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
echo "--- captured cf_tgt / cf_src decisions ---"
python3 - "$GF" "$BEFORE" <<'PY'
import json,sys
gf,before=sys.argv[1],int(sys.argv[2])
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']; name=req.get('labels',{}).get('dbt_node_name','')
    if name not in ('cf_tgt','cf_src'): continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None)
    payload=resp.get(v) or {}
    ed=payload.get('explained_decision',{})
    tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
    print("CF_DEC "+json.dumps({"node":name,"decision":v,
      "srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
      "desc":ed.get('decision_description'),
      "clone_source":payload.get('clone_source'),
      "clone_req_epoch":payload.get('clone_required_last_modified_epoch'),
      "tables":tabs}))
PY
