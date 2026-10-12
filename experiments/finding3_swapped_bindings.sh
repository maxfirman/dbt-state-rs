#!/usr/bin/env bash
# Finding 3 (review #2 follow-up) DECISIVE: swapped dependency BINDINGS.
#
# The follow-up review's exact counterexample: an UNCHANGED root query that
# cross-joins two dependencies and projects from the FIRST one, with the two
# dependency DEFINITIONS swapped between the two references. The sorted multiset
# of dependency query texts is identical; only the binding differs. Output
# differs (projects the first ref's value).
#
#   up1_<id> = select 1 as id      up2_<id> = select 2 as id   (prod + dev)
#   consA    = select a.id from {{ref(up1)}} a cross join {{ref(up2)}} b  -> {1}
#   consB    = select a.id from {{ref(up2)}} a cross join {{ref(up1)}} b  -> {2}
#            (consB's rendered SQL references up2 FIRST — the binding is swapped
#             relative to consA; both reference the same two upstreams.)
#
# PROCEDURE (two oracles, UNIQUE per-run names, only mutates CDO_DEMO_DB):
#   1. Build up1, up2, consA in prod_demo (consA data = {1}).
#   2. Build up1, up2 in dev_clone (so consB's refs resolve there).
#   3. Build consB in dev_clone (absent target, defers to prod).
#      ORACLE 1: hosted decision for the consB dev submit (clone vs execute).
#      ORACLE 2: ACTUAL contents of dev_clone_demo.consB in Snowflake.
#
# SAFE (binding preserved): hosted EXECUTES consB and dev contents = {2}.
# UNSAFE (binding discarded): hosted CLONES consB from consA and contents = {1}.
#
# Fails hard on a failed precondition build (no blanket `|| true` on the steps
# whose success the oracle depends on).
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
U1="sb_up1_$ID"; U2="sb_up2_$ID"; CA="sb_consa_$ID"; CB="sb_consb_$ID"

cd "$PROJ" || { echo "FATAL: no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$U1.sql models/marts/$U2.sql models/marts/$CA.sql \
        models/marts/$CB.sql macros/_sbexp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

cat > models/marts/$U1.sql <<SQL
{{ config(materialized='table') }}
select 1 as id
SQL
cat > models/marts/$U2.sql <<SQL
{{ config(materialized='table') }}
select 2 as id
SQL
# consA projects a.id where a = up1 (first ref) -> {1}
cat > models/marts/$CA.sql <<SQL
{{ config(materialized='table') }}
select a.id from {{ ref('$U1') }} a cross join {{ ref('$U2') }} b
SQL
# consB SWAPS the binding: first ref is up2 -> projects a.id = up2's value -> {2}
cat > models/marts/$CB.sql <<SQL
{{ config(materialized='table') }}
select a.id from {{ ref('$U2') }} a cross join {{ ref('$U1') }} b
SQL

fail(){ echo "FATAL: $1"; exit 1; }
built_ok(){ grep -qiE "Completed successfully|Succeeded|Reused|Cloned" "$1"; }

query_contents(){ # $1 = fqn
  cat > macros/_sbexp.sql <<MACRO
{% macro _sbexp() %}
  {% set r = run_query("select id from $1 order by id") %}
  {% if execute %}{{ log("CONTENTS $1 = " ~ r.columns[0].values(), info=true) }}{% endif %}
{% endmacro %}
MACRO
  dbt run-operation _sbexp --profile snowflake --target dev_clone \
    --skip-semantic-manifest-validation >/tmp/sb_q.txt 2>&1 || true
  rm -f macros/_sbexp.sql
  grep "CONTENTS" /tmp/sb_q.txt | tail -1
}

echo "############ FINDING 3 SWAPPED BINDINGS id=$ID ############"
echo "--- 1. build up1,up2,consA in prod_demo (consA data={1}) ---"
dbt build --profile snowflake --target prod_demo --select "$U1" "$U2" "$CA" \
  --skip-semantic-manifest-validation >/tmp/sb_prod.txt 2>&1
built_ok /tmp/sb_prod.txt || { cat /tmp/sb_prod.txt; fail "prod precondition build failed"; }
grep -iE "Succeeded|Reused|Error|Completed" /tmp/sb_prod.txt | sed 's/^ *//' | tail -4

echo "--- 2. build up1,up2 in dev_clone (so consB refs resolve) ---"
dbt build --profile snowflake --target dev_clone --select "$U1" "$U2" \
  --skip-semantic-manifest-validation >/tmp/sb_devup.txt 2>&1
built_ok /tmp/sb_devup.txt || { cat /tmp/sb_devup.txt; fail "dev upstream build failed"; }
grep -iE "Succeeded|Reused|Cloned|Error|Completed" /tmp/sb_devup.txt | sed 's/^ *//' | tail -3

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l < "$GF")

echo "--- 3. build consB in dev_clone (absent target, defers to prod) ---"
dbt build --profile snowflake --target dev_clone --select "$CB" \
  --skip-semantic-manifest-validation >/tmp/sb_dev.txt 2>&1
built_ok /tmp/sb_dev.txt || { cat /tmp/sb_dev.txt; fail "dev consB build failed"; }
grep -iE "Succeeded|Reused|Cloned|clone|Error|Completed" /tmp/sb_dev.txt | sed 's/^ *//' | tail -6

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GF" ] || fail "no golden capture file produced"
echo "--- ORACLE 1: hosted decision for dev $CB ---"
python3 - "$GF" "$BEFORE" "$CB" <<'PY'
import json,sys
gf,before,cb=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
found=False
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']; name=req.get('labels',{}).get('dbt_node_name','')
    if name!=cb: continue
    found=True
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); p=resp.get(v) or {}
    ed=p.get('explained_decision',{})
    print("SB_DEC "+json.dumps({"node":name,"decision":v,
      "srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
      "desc":ed.get('decision_description'),"clone_source":p.get('clone_source'),
      "deps":[d.get('name') for d in req.get('query_dependencies',[])],
      "sql":req.get('sql','').strip()}))
if not found: print("SB_DEC {\"error\":\"no consB submit captured\"}")
PY
echo "--- ORACLE 2: actual contents of dev_clone_demo.$CB ---"
query_contents "CDO_DEMO_DB.dev_clone_demo.$CB"
echo "(SAFE = [2] / executed; UNSAFE conflation = [1] / cloned from consA)"
