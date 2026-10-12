#!/usr/bin/env bash
# Data-test freshness CONTROL (issue #2, finding 1).
#
# Establishes the baseline that an UNCHANGED data test (same upstream epoch)
# SKIPs on re-run, so the EXECUTE observed in data_test_freshness.sh is
# attributable to the upstream epoch advancing (not to the test never skipping).
#
# PROCEDURE (prod_demo, CDO_DEMO_DB only):
#   1. Build +dt_test (test confirmed passing against dt_up@epoch0).
#   2. Re-run ONLY dt_test WITHOUT touching dt_up (same upstream epoch).
#   3. Capture the data-test SUBMIT decision — expect SKIP if hosted reuses an
#      unchanged test result.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true;
  rm -f models/marts/dt_up.sql data-tests/dt_test.sql; }
trap cleanup EXIT
mkdir -p models/marts data-tests
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

cat > models/marts/dt_up.sql <<'SQL'
{{ config(materialized='table') }}
select 1 as id, 100 as amount
union all
select 2 as id, 200 as amount
SQL
cat > data-tests/dt_test.sql <<'SQL'
select * from {{ ref('dt_up') }} where amount < 0
SQL

echo "############ DATA-TEST FRESHNESS CONTROL (no bump) ############"
echo "--- build +dt_test baseline ---"
dbt build --profile snowflake --target prod_demo --select +dt_test --skip-semantic-manifest-validation >/tmp/dtc_base.txt 2>&1 || true
grep -iE "Reused|Succeeded|Pass|Error|Failed" /tmp/dtc_base.txt | sed 's/^ *//' | tail -6

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

echo "--- re-run ONLY dt_test WITHOUT bumping dt_up (same epoch) ---"
dbt build --profile snowflake --target prod_demo --select dt_test --skip-semantic-manifest-validation >/tmp/dtc_resubmit.txt 2>&1 || true
grep -iE "Reused|Succeeded|Pass|Error|Failed|No-op|no-op" /tmp/dtc_resubmit.txt | sed 's/^ *//' | tail -6

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
echo "--- captured data-test decision on identical re-run ---"
python3 - "$GF" "$BEFORE" <<'PY'
import json,sys
gf,before=sys.argv[1],int(sys.argv[2])
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']
    if req.get('execution_type')!=8: continue
    name=req.get('labels',{}).get('dbt_node_name','')
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None)
    ed=(resp.get(v) or {}).get('explained_decision',{})
    tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
    print("CTRL_DT_DEC "+json.dumps({"node":name,"decision":v,
      "srr":ed.get('skip_rejection_reason'),"is_stale":ed.get('is_stale'),
      "desc":ed.get('decision_description'),"tables":tabs}))
PY
