#!/usr/bin/env bash
# DATA-TEST compare_unrendered_code (review #2 finding 6 FOLLOW-UP).
#
# QUESTION: for a DATA TEST (execution_type 8) with compare_unrendered_code=true,
# does changing ONLY the unrendered template (a Jinja comment that leaves the
# rendered SQL byte-identical) cause EXECUTE, or does the hosted service reuse
# the cached test outcome (SKIP), the same dual-identity rule proven for models?
#
# PROCEDURE (prod_demo, CDO_DEMO_DB.prod_state_demo), UNIQUE per-run names:
#   1. Build an upstream model dtt_up_<id> (so the test has data to query).
#   2. Add a singular data test dtt_<id> with compare_unrendered_code enabled,
#      build it (ready_to_execute), confirm.
#   3. Re-run the test with a CHANGED Jinja comment (template bytes change,
#      rendered SQL identical). Capture the decision.
#
# SAFE/expected (dual identity): the re-run SKIPS. Only mutates CDO_DEMO_DB.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
UP="dtt_up_$ID"; TST="dtt_$ID"

cd "$PROJ" || { echo "FATAL no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f "models/marts/$UP.sql" "data-tests/$TST.sql"; }
trap cleanup EXIT
mkdir -p models/marts data-tests
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
fail(){ echo "FATAL: $1"; exit 1; }
built_ok(){ grep -qiE "Completed successfully|Succeeded|Reused|Cloned|PASS|No-op|no-op" "$1"; }

cat > "models/marts/$UP.sql" <<SQL
{{ config(materialized='table') }}
select 1 as id, 42 as amount
SQL

# A singular data test with compare_unrendered_code enabled. The Jinja comment
# changes the unrendered template bytes without changing the rendered SQL.
write_test(){ # $1 = comment text
  cat > "data-tests/$TST.sql" <<SQL
{{ config(state={"compare_unrendered_code": true}) }}
{# $1 #}
select id from {{ ref('$UP') }} where amount < 0
SQL
}

echo "############ DATA-TEST CUC id=$ID ############"
echo "--- 1. build upstream $UP ---"
dbt build --profile snowflake --target prod_demo --select "$UP" --skip-semantic-manifest-validation >/tmp/dtt_up.txt 2>&1
built_ok /tmp/dtt_up.txt || { cat /tmp/dtt_up.txt; fail "upstream build failed"; }

echo "--- 2. build data test $TST (comment A) ---"
write_test "comment A"
dbt build --profile snowflake --target prod_demo --select "$TST" --skip-semantic-manifest-validation >/tmp/dtt_a.txt 2>&1
built_ok /tmp/dtt_a.txt || { cat /tmp/dtt_a.txt; fail "test build A failed"; }
grep -iE "PASS|Reused|no-op|Error" /tmp/dtt_a.txt | sed 's/^ *//' | tail -3

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l < "$GF")

echo "--- 3. re-run $TST with CHANGED jinja comment (rendered identical) ---"
write_test "comment B changed only in template"
dbt build --profile snowflake --target prod_demo --select "$TST" --skip-semantic-manifest-validation >/tmp/dtt_b.txt 2>&1
built_ok /tmp/dtt_b.txt || { cat /tmp/dtt_b.txt; fail "test build B failed"; }
grep -iE "PASS|Reused|no-op|Error" /tmp/dtt_b.txt | sed 's/^ *//' | tail -3

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GF" ] || fail "no golden capture"
echo "--- captured $TST decisions (execution_type should be 8) ---"
python3 - "$GF" "$BEFORE" "$TST" <<'PY'
import json,sys
gf,before,tst=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()]
# include the earlier baseline submit too (before BEFORE) for context
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']; name=req.get('labels',{}).get('dbt_node_name','')
    if tst not in name: continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); payload=resp.get(v) or {}
    ed=payload.get('explained_decision',{})
    ns=req.get('dbt_node_state',{}) or {}
    print("DTT_DEC "+json.dumps({"node":name,"decision":v,
      "exec_type":req.get('execution_type'),
      "cuc":req.get('compare_unrendered_code'),
      "body_hash":ns.get('node_body_hash'),
      "sql":req.get('sql','').strip()[:70],
      "desc":ed.get('decision_description')}))
PY
