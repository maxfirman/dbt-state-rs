#!/usr/bin/env bash
# Finding 3 DECISIVE: dependency-binding / data conflation (two-oracle).
#
# Builds two models whose rendered SQL differs ONLY in the dependency TABLE name
# (same schema), referencing upstreams with GENUINELY DIFFERENT row values:
#   v3a_<id> = select 1 as id      m3a_<id> = select id from {{ ref('v3a') }}
#   v3b_<id> = select 2 as id      m3b_<id> = select id from {{ ref('v3b') }}
#
# PROCEDURE:
#   1. Build v3a,v3b,m3a in prod_demo (m3a's data = {1}).
#   2. Build v3b in dev_clone (so m3b's ref resolves there), then build m3b in
#      dev_clone (absent target, defers to prod). m3b SHOULD contain {2}.
#   3. TWO ORACLES:
#      (a) hosted decision for the m3b dev submit (clone vs execute, clone_source);
#      (b) the ACTUAL contents of CDO_DEMO_DB.dev_clone_demo.m3b in Snowflake.
#
# If m3b is ready_to_clone from m3a AND dev m3b contains {1}, that is a GENUINE
# unsafe data conflation. If m3b contains {2} (or hosted executed), reuse is safe.
#
# UNIQUE per-run names (hosted history is not reset by dropping objects).
# Only mutates CDO_DEMO_DB (prod_state_demo + dev_clone_demo).
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
VA="v3a_$ID"; VB="v3b_$ID"; MA="m3a_$ID"; MB="m3b_$ID"

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true;
  rm -f models/marts/$VA.sql models/marts/$VB.sql models/marts/$MA.sql models/marts/$MB.sql macros/_exp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

cat > models/marts/$VA.sql <<SQL
{{ config(materialized='table') }}
select 1 as id
SQL
cat > models/marts/$VB.sql <<SQL
{{ config(materialized='table') }}
select 2 as id
SQL
cat > models/marts/$MA.sql <<SQL
{{ config(materialized='table') }}
select id from {{ ref('$VA') }}
SQL
cat > models/marts/$MB.sql <<SQL
{{ config(materialized='table') }}
select id from {{ ref('$VB') }}
SQL

query_contents(){ # $1 = fqn ; print the rows via a run-operation
  cat > macros/_exp.sql <<MACRO
{% macro _exp() %}
  {% set r = run_query("select id from $1 order by id") %}
  {% if execute %}{{ log("CONTENTS $1 = " ~ r.columns[0].values(), info=true) }}{% endif %}
{% endmacro %}
MACRO
  dbt run-operation _exp --profile snowflake --target dev_clone --skip-semantic-manifest-validation >/tmp/f3_q.txt 2>&1 || true
  rm -f macros/_exp.sql
  grep "CONTENTS" /tmp/f3_q.txt | tail -1
}

echo "############ FINDING 3 DATA CONFLATION id=$ID ############"
echo "--- 1. build v3a,v3b,m3a in prod_demo (m3a data={1}) ---"
dbt build --profile snowflake --target prod_demo --select "$VA" "$VB" "$MA" --skip-semantic-manifest-validation >/tmp/f3_prod.txt 2>&1 || true
grep -iE "Succeeded|Reused|Error" /tmp/f3_prod.txt | sed 's/^ *//' | tail -4

echo "--- 2a. build v3b in dev_clone (so m3b ref resolves) ---"
dbt build --profile snowflake --target dev_clone --select "$VB" --skip-semantic-manifest-validation >/tmp/f3_devup.txt 2>&1 || true
grep -iE "Succeeded|Reused|Cloned|Error" /tmp/f3_devup.txt | sed 's/^ *//' | tail -2

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

echo "--- 2b. build m3b in dev_clone (absent target, defers to prod) ---"
dbt build --profile snowflake --target dev_clone --select "$MB" --skip-semantic-manifest-validation >/tmp/f3_dev.txt 2>&1 || true
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/f3_dev.txt | sed 's/^ *//' | tail -6

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
echo "--- ORACLE 1: hosted decision for dev $MB ---"
python3 - "$GF" "$BEFORE" "$MB" <<'PY'
import json,sys
gf,before,mb=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']; name=req.get('labels',{}).get('dbt_node_name','')
    if name!=mb: continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); p=resp.get(v) or {}
    ed=p.get('explained_decision',{})
    print("F3_DEC "+json.dumps({"node":name,"decision":v,
      "srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
      "desc":ed.get('decision_description'),"clone_source":p.get('clone_source'),
      "sql":req.get('sql','').strip()}))
PY
echo "--- ORACLE 2: actual contents of dev_clone_demo.$MB ---"
query_contents "CDO_DEMO_DB.dev_clone_demo.$MB"
echo "(expected safe = [2]; conflation = [1])"
