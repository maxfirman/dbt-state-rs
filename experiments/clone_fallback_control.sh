#!/usr/bin/env bash
# Clone-fallback CONTROL (issue #2, finding 2a): does the cross-name clone
# fallback fire AT ALL in a same-environment (prod-only) build when a fresh
# equivalent exists under another name? Establishes whether the prod-only setup
# can isolate the freshness question, or whether the fallback is specific to the
# cross-environment (dev_clone) flow.
#
# SETUP: cf_up (table), cf_src + cf_tgt share an identical body. Build cf_up +
# cf_src, then build cf_tgt WITHOUT any upstream bump (cf_src is FRESH).
# Only mutates CDO_DEMO_DB.prod_state_demo.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null||true;
  rm -f models/marts/cf_up.sql models/marts/cf_src.sql models/marts/cf_tgt.sql; }
trap cleanup EXIT
mkdir -p models/marts
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

cat > models/marts/cf_up.sql <<'SQL'
{{ config(materialized='table') }}
select 1 as id, 100 as amount
SQL
BODY="select id, amount from {{ ref('cf_up') }}"
cat > models/marts/cf_src.sql <<SQL
{{ config(materialized='table') }}
$BODY
SQL
cat > models/marts/cf_tgt.sql <<SQL
{{ config(materialized='table') }}
$BODY
SQL

echo "############ CLONE FALLBACK CONTROL (fresh source, same env) ############"
echo "--- build cf_up + cf_src ---"
dbt build --profile snowflake --target prod_demo --select cf_up cf_src --skip-semantic-manifest-validation >/tmp/cfc_base.txt 2>&1 || true
grep -iE "Reused|Succeeded|Error" /tmp/cfc_base.txt | sed 's/^ *//' | tail -4

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l<"$GF")

echo "--- build cf_tgt (absent, same body as fresh cf_src) ---"
dbt build --profile snowflake --target prod_demo --select cf_tgt --skip-semantic-manifest-validation >/tmp/cfc_tgt.txt 2>&1 || true
grep -iE "Reused|Succeeded|Cloned|clone|Error" /tmp/cfc_tgt.txt | sed 's/^ *//' | tail -6

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null|head -1||true)
python3 - "$GF" "$BEFORE" <<'PY'
import json,sys
gf,before=sys.argv[1],int(sys.argv[2])
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']; name=req.get('labels',{}).get('dbt_node_name','')
    if name!='cf_tgt': continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); payload=resp.get(v) or {}
    ed=payload.get('explained_decision',{})
    print("CFC_DEC "+json.dumps({"node":name,"decision":v,
      "desc":ed.get('decision_description'),"clone_source":payload.get('clone_source')}))
PY
