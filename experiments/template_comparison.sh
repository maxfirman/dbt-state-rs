#!/usr/bin/env bash
# Template-comparison experiment (issue #2, finding 6).
#
# QUESTION: with compare_unrendered_code (the client sends the UNRENDERED body
# and compares templates), does changing ONLY the template (a Jinja comment /
# whitespace that leaves the rendered SQL byte-identical) cause EXECUTE, or does
# the hosted service fall back to comparing rendered SQL and SKIP? The
# documented rule (compare-unrendered-code) says the rendered SQL is compared
# when the template changes; both must change to invalidate logic reuse.
#
# Uses a UNIQUE per-run model name so hosted decision history (which is NOT
# cleared by warehouse mutation) cannot carry over between runs.
#
# PROCEDURE (prod_demo, CDO_DEMO_DB.prod_state_demo):
#   1. Build model tpl_<id> with persist-docs/compare_unrendered_code enabled.
#   2. Re-run with a changed Jinja COMMENT (template bytes change; rendered SQL
#      identical). Capture the decision.
#
# Only mutates CDO_DEMO_DB.prod_state_demo.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%s)}"
NODE="tpl_$ID"

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true; rm -f "models/marts/$NODE.sql" "models/marts/$NODE.yml"; }
trap cleanup EXIT
mkdir -p models/marts
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

# compare_unrendered_code is a model/project config. Set it in the model config
# so the Fusion client sends compare_unrendered_code=true on the submit. A Jinja
# comment changes the UNRENDERED template bytes while leaving rendered SQL
# byte-identical.
write_model(){ # $1 = jinja comment text (changes template bytes, not rendered SQL)
  cat > "models/marts/$NODE.sql" <<SQL
{{ config(materialized='table', state={"compare_unrendered_code": true}) }}
{# $1 #}
select 1 as id, 42 as amount
SQL
}

echo "############ TEMPLATE COMPARISON (finding 6) node=$NODE ############"

echo "--- 1. build $NODE (template comment A) ---"
write_model "comment A"
dbt build --profile snowflake --target prod_demo --select "$NODE" --skip-semantic-manifest-validation >/tmp/tpl_base.txt 2>&1 || true
grep -iE "Reused|Succeeded|Error" /tmp/tpl_base.txt | sed 's/^ *//' | tail -4

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

echo "--- 2. re-run $NODE with CHANGED jinja comment (rendered SQL identical) ---"
write_model "comment B changed only in template"
dbt build --profile snowflake --target prod_demo --select "$NODE" --skip-semantic-manifest-validation >/tmp/tpl_change.txt 2>&1 || true
grep -iE "Reused|Succeeded|Error" /tmp/tpl_change.txt | sed 's/^ *//' | tail -4

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
echo "--- captured $NODE decision + compare_unrendered_code flag + body hashes ---"
python3 - "$GF" "$BEFORE" "$NODE" <<'PY'
import json,sys
gf,before,node=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']; name=req.get('labels',{}).get('dbt_node_name','')
    if name!=node: continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); payload=resp.get(v) or {}
    ed=payload.get('explained_decision',{})
    ns=req.get('dbt_node_state',{}) or {}
    print("TPL_DEC "+json.dumps({"node":name,"decision":v,
      "compare_unrendered_code":req.get('compare_unrendered_code'),
      "node_body_hash":ns.get('node_body_hash'),
      "sql_head":(req.get('sql','')[:60]),
      "desc":ed.get('decision_description')}))
PY
