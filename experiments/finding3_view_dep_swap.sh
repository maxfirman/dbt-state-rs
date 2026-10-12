#!/usr/bin/env bash
# Finding 3 (review #2 2nd follow-up) — DEPENDENCY-DEFINITION swap with NONEMPTY
# query_dependencies and a CONSTANT root SQL (the shape the reviewer asked for).
#
# The earlier swapped-binding capture had empty query_dependencies (the swap was
# in the root SQL text), so it replayed even on pre-fix code. This drives the
# ACTUAL bug: VIEW dependencies (which Fusion sends as query_dependencies with
# their DDL), referenced by a consumer whose rendered root SQL is held constant,
# with the two view DEFINITIONS swapped between the two references.
#
#   up1 = view selecting 1 as id ; up2 = view selecting 2 as id
#   cons = select a.id from {{ref(up1)}} a cross join {{ref(up2)}} b   -> {1}
#
# Build cons in prod (deps: up1=select 1, up2=select 2). Then in dev, build cons
# with up1/up2 definitions SWAPPED (up1 now selects 2, up2 selects 1) — the
# consumer's rendered SQL ref order is unchanged, only the view DEFINITIONS
# behind each ref differ. Two oracles: hosted decision + dev warehouse contents.
#
# SAFE (binding preserved): hosted EXECUTES cons in dev, contents = {2}.
# UNSAFE (binding discarded): hosted CLONES from prod cons, contents = {1}.
#
# UNIQUE per-run names; only mutates CDO_DEMO_DB; fails hard on failed builds.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
U1="dv_up1_$ID"; U2="dv_up2_$ID"; CONS="dv_cons_$ID"

cd "$PROJ" || { echo "FATAL no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$U1.sql models/marts/$U2.sql models/marts/$CONS.sql macros/_dvexp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
fail(){ echo "FATAL: $1"; exit 1; }
# Reliable precondition check: dbt's EXIT STATUS is authoritative. We also
# require that the EXPECTED number of nodes actually succeeded/reused/cloned,
# so a partial multi-node build (one node ok, another failed) is caught. The
# "Errors and Warnings" banner / deferral HttpError warning are NOT failures.
run_build(){ # $1=target $2=logfile $3=expected_ok_count $4+=select args
  local target="$1" log="$2" expected="$3"; shift 3
  dbt build --profile snowflake --target "$target" --select "$@" \
    --skip-semantic-manifest-validation >"$log" 2>&1
  local rc=$?
  [ $rc -eq 0 ] || { cat "$log"; fail "dbt build exited $rc (target=$target)"; }
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected nodes ok (target=$target)"; }
}

# $1 = up1 definition value, $2 = up2 definition value
write_models(){
  cat > models/marts/$U1.sql <<SQL
{{ config(materialized='view') }}
select $1 as id
SQL
  cat > models/marts/$U2.sql <<SQL
{{ config(materialized='view') }}
select $2 as id
SQL
  cat > models/marts/$CONS.sql <<SQL
{{ config(materialized='table') }}
select a.id from {{ ref('$U1') }} a cross join {{ ref('$U2') }} b
SQL
}

query_contents(){
  cat > macros/_dvexp.sql <<MACRO
{% macro _dvexp() %}{% set r = run_query("select id from $1 order by id") %}{% if execute %}{{ log("CONTENTS $1 = " ~ r.columns[0].values(), info=true) }}{% endif %}{% endmacro %}
MACRO
  dbt run-operation _dvexp --profile snowflake --target dev_clone --skip-semantic-manifest-validation >/tmp/dv_q.txt 2>&1 || true
  rm -f macros/_dvexp.sql
  grep "CONTENTS" /tmp/dv_q.txt | tail -1
}

echo "############ FINDING 3 VIEW-DEP SWAP id=$ID ############"
echo "--- 1. prod: up1=1, up2=2, build cons (deps nonempty; cons={1}) ---"
write_models 1 2
run_build prod_demo /tmp/dv_prod.txt 3 "$U1" "$U2" "$CONS"
grep -iE "Succeeded|Completed" /tmp/dv_prod.txt | sed 's/^ *//' | tail -4

echo "--- 2. dev: build up1,up2 SWAPPED (up1=2, up2=1) ---"
write_models 2 1
run_build dev_clone /tmp/dv_devup.txt 2 "$U1" "$U2"

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l < "$GF")

echo "--- 3. dev: build cons (absent target, defers to prod; deps SWAPPED) ---"
run_build dev_clone /tmp/dv_dev.txt 1 "$CONS"
grep -iE "Cloned|Reused|Succeeded|Completed" /tmp/dv_dev.txt | sed 's/^ *//' | tail -4

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GF" ] || fail "no golden capture"
echo "--- ORACLE 1: hosted decision + query_dependencies for dev $CONS ---"
python3 - "$GF" "$BEFORE" "$CONS" <<'PY'
import json,sys
gf,before,cons=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
found=False
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']; name=req.get('labels',{}).get('dbt_node_name','')
    if name!=cons: continue
    found=True
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); p=resp.get(v) or {}
    ed=p.get('explained_decision',{})
    deps=[(d.get('name'),d.get('query','').strip()[:40]) for d in req.get('query_dependencies',[])]
    print("DV_DEC "+json.dumps({"node":name,"decision":v,
      "srr":ed.get('skip_rejection_reason'),"desc":ed.get('decision_description'),
      "clone_source":p.get('clone_source'),"n_deps":len(deps),"deps":deps,
      "sql":req.get('sql','').strip()}))
if not found: print("DV_DEC {\"error\":\"no cons submit captured\"}")
PY
echo "--- ORACLE 2: actual contents of dev_clone_demo.$CONS ---"
query_contents "CDO_DEMO_DB.dev_clone_demo.$CONS"
echo "(SAFE = [2] / executed; UNSAFE conflation = [1] / cloned from prod cons)"
