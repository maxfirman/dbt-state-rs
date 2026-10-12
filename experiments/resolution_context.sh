#!/usr/bin/env bash
# RESOLUTION-CONTEXT DISTINCTNESS (review #2 3rd follow-up, P0 #1b/c).
#
# Does hosted distinguish two consumers with the SAME abstract logic whose
# dependency resolves to GENUINELY DIFFERENT physical data? The consumer's own
# schema is held fixed-shape across prod/dev (the normal cross-env case), but its
# upstream VIEW resolves to different data.
#
#   prod: cons = select id from {{ref(up)}} ; up(prod) = select 1 as id   -> {1}
#   dev : cons = select id from {{ref(up)}} ; up(dev)  = select 2 as id   -> {2}
#
# This is the finding-3 shape (dependency data differs) in the resolution-context
# framing. SAFE: hosted EXECUTES the dev cons (dev contents {2}); UNSAFE: hosted
# clones prod cons (dev contents {1}).
#
# UNIQUE per-run names; only mutates CDO_DEMO_DB; exit-status-checked builds;
# oracles asserted.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
UP="rctx_up_$ID"; CONS="rctx_cons_$ID"

cd "$PROJ" || { echo "FATAL no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$UP.sql models/marts/$CONS.sql macros/_rctx.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
fail(){ echo "FATAL: $1"; exit 1; }
run_build(){ local target="$1" log="$2" expected="$3"; shift 3
  dbt build --profile snowflake --target "$target" --select "$@" --skip-semantic-manifest-validation >"$log" 2>&1
  local rc=$?; [ $rc -eq 0 ] || { cat "$log"; fail "build exited $rc ($target)"; }
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected ok ($target)"; }
}
query_contents(){
  cat > macros/_rctx.sql <<MACRO
{% macro _rctx() %}{% set r = run_query("select id from $1 order by id") %}{% if execute %}{{ log("CONTENTS $1 = " ~ r.columns[0].values(), info=true) }}{% endif %}{% endmacro %}
MACRO
  dbt run-operation _rctx --profile snowflake --target dev_clone --skip-semantic-manifest-validation >/tmp/rctx_q.txt 2>&1
  local rc=$?; rm -f macros/_rctx.sql
  [ $rc -eq 0 ] || { cat /tmp/rctx_q.txt; fail "contents query failed"; }
  grep "CONTENTS" /tmp/rctx_q.txt | tail -1
}

cat > models/marts/$CONS.sql <<SQL
{{ config(materialized='table') }}
select id from {{ ref('$UP') }}
SQL

echo "############ RESOLUTION-CONTEXT DISTINCTNESS id=$ID ############"
echo "--- 1. prod: up=select 1, build up+cons (cons={1}) ---"
cat > models/marts/$UP.sql <<SQL
{{ config(materialized='view') }}
select 1 as id
SQL
run_build prod_demo /tmp/rctx_prod.txt 2 "$UP" "$CONS"

echo "--- 2. dev: up=select 2 (DIFFERENT data), build up ---"
cat > models/marts/$UP.sql <<SQL
{{ config(materialized='view') }}
select 2 as id
SQL
run_build dev_clone /tmp/rctx_devup.txt 1 "$UP"

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l < "$GF")

echo "--- 3. dev: build cons (absent, defers to prod; dep data differs) ---"
run_build dev_clone /tmp/rctx_dev.txt 1 "$CONS"

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GF" ] || fail "no golden capture"
echo "--- ORACLE 1: hosted decision for dev $CONS (asserted) ---"
python3 - "$GF" "$BEFORE" "$CONS" <<'PY' || { echo "FATAL: oracle1 failed"; exit 1; }
import json,sys
gf,before,cons=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
dec=None
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']
    if req.get('labels',{}).get('dbt_node_name','')!=cons: continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); p=resp.get(v) or {}
    dec=v
    print("RCTX_DEC "+json.dumps({"decision":v,"clone_source":p.get('clone_source'),
      "n_deps":len(req.get('query_dependencies',[])),
      "deps":[(d.get('name'),d.get('default_schema'),d.get('query','').strip()[:30]) for d in req.get('query_dependencies',[])]}))
if dec is None: print("ERROR: no cons submit"); sys.exit(1)
# Record the hosted decision; both outcomes are informative (do not force).
print("RCTX_HOSTED_DECISION="+dec)
PY
echo "--- ORACLE 2: actual contents of dev_clone_demo.$CONS ---"
contents=$(query_contents "CDO_DEMO_DB.dev_clone_demo.$CONS")
echo "$contents"
echo "(SAFE/distinguished = [2] / executed; conflation = [1] / cloned)"
