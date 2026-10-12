#!/usr/bin/env bash
# Finding 3 CONTROL: same two-model structure but IDENTICAL upstream data.
#
# v3a_<id> = select 1 as id   v3b_<id> = select 1 as id   (SAME content)
# m3a = select id from v3a    m3b = select id from v3b
#
# If hosted CLONES m3a->m3b here (identical dependency content) but EXECUTED in
# the different-data run (finding3_data_conflation.sh), then hosted's clone
# eligibility considers the dependency DEFINITION/data, not just the model name
# — i.e. reuse is SAFE and the server's schema-only abstraction is the
# conservative side. If hosted executes here too, the dependency NAME alone
# blocks cloning.
#
# Two-oracle: hosted decision + dev m3b contents (both safe = [1] either way).
# UNIQUE per-run names. Only mutates CDO_DEMO_DB.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
VA="w3a_$ID"; VB="w3b_$ID"; MA="n3a_$ID"; MB="n3b_$ID"

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true;
  rm -f models/marts/$VA.sql models/marts/$VB.sql models/marts/$MA.sql models/marts/$MB.sql macros/_exp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

# IDENTICAL content for both upstreams.
for V in "$VA" "$VB"; do
cat > models/marts/$V.sql <<SQL
{{ config(materialized='table') }}
select 1 as id
SQL
done
cat > models/marts/$MA.sql <<SQL
{{ config(materialized='table') }}
select id from {{ ref('$VA') }}
SQL
cat > models/marts/$MB.sql <<SQL
{{ config(materialized='table') }}
select id from {{ ref('$VB') }}
SQL

query_contents(){
  cat > macros/_exp.sql <<MACRO
{% macro _exp() %}
  {% set r = run_query("select id from $1 order by id") %}
  {% if execute %}{{ log("CONTENTS $1 = " ~ r.columns[0].values(), info=true) }}{% endif %}
{% endmacro %}
MACRO
  dbt run-operation _exp --profile snowflake --target dev_clone --skip-semantic-manifest-validation >/tmp/f3c_q.txt 2>&1 || true
  rm -f macros/_exp.sql; grep "CONTENTS" /tmp/f3c_q.txt | tail -1; }

echo "############ FINDING 3 CONTROL (identical upstream data) id=$ID ############"
dbt build --profile snowflake --target prod_demo --select "$VA" "$VB" "$MA" --skip-semantic-manifest-validation >/tmp/f3c_prod.txt 2>&1 || true
grep -iE "Succeeded|Reused|Error" /tmp/f3c_prod.txt | sed 's/^ *//' | tail -3
dbt build --profile snowflake --target dev_clone --select "$VB" --skip-semantic-manifest-validation >/tmp/f3c_devup.txt 2>&1 || true

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")
echo "--- build $MB in dev_clone (absent, defers to prod) ---"
dbt build --profile snowflake --target dev_clone --select "$MB" --skip-semantic-manifest-validation >/tmp/f3c_dev.txt 2>&1 || true
grep -iE "Succeeded|Reused|Cloned|clone|Error" /tmp/f3c_dev.txt | sed 's/^ *//' | tail -5

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
    print("F3C_DEC "+json.dumps({"node":name,"decision":v,
      "srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
      "desc":ed.get('decision_description'),"clone_source":p.get('clone_source'),
      "sql":req.get('sql','').strip()}))
PY
echo "--- ORACLE 2: contents of dev_clone_demo.$MB ---"
query_contents "CDO_DEMO_DB.dev_clone_demo.$MB"
