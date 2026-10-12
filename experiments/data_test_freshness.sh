#!/usr/bin/env bash
# Data-test freshness experiment (issue #2, finding 1).
#
# QUESTION: when a data test is confirmed (ConfirmExecution carries test
# results but NO object last_modified_epoch, because a test produces no object),
# and an UPSTREAM it depends on later changes (its modification epoch advances),
# does the hosted service SKIP (reuse the cached pass/fail) or EXECUTE (re-run
# the test)?
#
# SETUP (prod_demo only, inside CDO_DEMO_DB):
#   - dt_up        : table materialization, constant content (the tested data).
#   - dt_test.sql  : a SINGULAR data test selecting from {{ ref('dt_up') }} that
#                    returns 0 rows (passes) while the content is unchanged.
#
# PROCEDURE:
#   1. Build +dt_test so the test is confirmed passing against dt_up@epoch0.
#   2. Recreate dt_up IN PLACE with IDENTICAL content (advances only its
#      warehouse modification epoch — pure data-freshness, no logic change).
#   3. Re-run ONLY the test (do not rebuild dt_up) and capture the data-test
#      SUBMIT decision (execution_type 8) from the recording proxy.
#
# A SKIP here would reuse a stale cached result after the tested data's epoch
# moved; an EXECUTE re-runs the test. We only report what hosted actually does.
#
# Only mutates CDO_DEMO_DB.prod_state_demo.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true;
  rm -f models/marts/dt_up.sql data-tests/dt_test.sql macros/_exp_recreate.sql; }
trap cleanup EXIT
mkdir -p models/marts data-tests macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

# Upstream table with constant content.
cat > models/marts/dt_up.sql <<'SQL'
{{ config(materialized='table') }}
select 1 as id, 100 as amount
union all
select 2 as id, 200 as amount
SQL

# Singular data test: fails if any amount is negative (never, so it passes).
cat > data-tests/dt_test.sql <<'SQL'
select * from {{ ref('dt_up') }} where amount < 0
SQL

recreate_up(){ # recreate dt_up in place with IDENTICAL content -> advance epoch only
  local fqn="CDO_DEMO_DB.prod_state_demo.dt_up"
  cat > macros/_exp_recreate.sql <<MACRO
{% macro _exp_recreate() %}
  {% do run_query("create or replace table $fqn as (select 1 as id, 100 as amount union all select 2 as id, 200 as amount)") %}
  {{ log("recreated $fqn (epoch advanced, content identical)", info=true) }}
{% endmacro %}
MACRO
  dbt run-operation _exp_recreate --profile snowflake --target prod_demo --skip-semantic-manifest-validation >/tmp/dt_recreate.txt 2>&1 || true
  rm -f macros/_exp_recreate.sql
  tail -2 /tmp/dt_recreate.txt
}

echo "############ DATA-TEST FRESHNESS ############"
echo "--- build +dt_test baseline (test confirmed passing) ---"
dbt build --profile snowflake --target prod_demo --select +dt_test --skip-semantic-manifest-validation >/tmp/dt_base.txt 2>&1 || true
grep -iE "Reused|Succeeded|Pass|Error|Failed|Warn" /tmp/dt_base.txt | sed 's/^ *//' | tail -8

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

echo "--- advance dt_up epoch (identical content) ---"
recreate_up

echo "--- re-run ONLY dt_test after upstream epoch advanced ---"
dbt build --profile snowflake --target prod_demo --select dt_test --skip-semantic-manifest-validation >/tmp/dt_resubmit.txt 2>&1 || true
grep -iE "Reused|Succeeded|Pass|Error|Failed|Warn" /tmp/dt_resubmit.txt | sed 's/^ *//' | tail -6

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
echo "--- captured data-test (execution_type 8) decisions after upstream bump ---"
python3 - "$GF" "$BEFORE" <<'PY'
import json,sys
gf,before=sys.argv[1],int(sys.argv[2])
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']
    et=req.get('execution_type')
    if et not in (8,'DBT_DATA_TEST',): continue
    name=req.get('labels',{}).get('dbt_node_name','')
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None)
    ed=(resp.get(v) or {}).get('explained_decision',{})
    tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
    print("DT_DEC "+json.dumps({"node":name,"exec_type":et,"decision":v,
      "srr":ed.get('skip_rejection_reason'),"is_stale":ed.get('is_stale'),
      "desc":ed.get('decision_description'),"tables":tabs}))
PY
