#!/usr/bin/env bash
# RESOLUTION-CONTEXT characterization (review #2 4th follow-up, P1 #2).
#
# The 3rd-follow-up version only changed the dependency BODY (select 1 -> select
# 2), which was ALREADY distinguished by the definition-body hash. The reviewer
# asked to hold the definition TEXT constant and vary ONLY the resolution
# context, with the dependency resolving to GENUINELY DIFFERENT physical data,
# plus a same-body equivalent-environment CONTROL.
#
# Both arms keep the consumer root SQL and the upstream view's DEFINITION TEXT
# byte-identical. The only thing that changes is the resolution context + the
# physical data the view reads.
#
#   up (view) DEFINITION TEXT, identical in both arms:
#       select id from {{ ref('leaf') }}
#   leaf (table): CONTROL builds the SAME data in dev as prod ({1});
#                 TREATMENT builds DIFFERENT data in dev ({9}) while the view
#                 TEXT is unchanged — i.e. the dependency resolves, through the
#                 same compiled SQL, to a leaf that now holds different data.
#
# CONTROL (same body + same data): hosted SHOULD clone/reuse (cross-env positive
#   that MUST remain supported) -> asserted ready_to_clone.
# TREATMENT (same body, different leaf DATA via the dev build of leaf): the OPEN
#   question. We RECORD the hosted decision and the dev contents; we do NOT force
#   a verdict (do-not-infer). If hosted EXECUTES, the context genuinely matters
#   and this is a real gap in namespace_logic_hash; if it CLONES, hosted keys on
#   the definition body alone and our behavior already matches.
#
# UNIQUE per-run names; only mutates CDO_DEMO_DB; exit-status-checked builds.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
LEAF="rctx_leaf_$ID"; UP="rctx_up_$ID"
CONS_CTL="rctx_cons_ctl_$ID"; CONS_TRT="rctx_cons_trt_$ID"

cd "$PROJ" || { echo "FATAL no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$LEAF.sql models/marts/$UP.sql \
        models/marts/$CONS_CTL.sql models/marts/$CONS_TRT.sql macros/_rctx.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
fail(){ echo "FATAL: $1"; exit 1; }
run_build(){ local target="$1" log="$2" expected="$3"; shift 3
  dbt build --profile snowflake --target "$target" --select "$@" --skip-semantic-manifest-validation >"$log" 2>&1
  local rc=$?; [ $rc -eq 0 ] || { cat "$log"; fail "build exited $rc ($target)"; }
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned|Warned" "$log")
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
decision_for(){ python3 - "$1" "$2" "$3" <<'PY'
import json,sys
gf,before,node=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']
    if req.get('labels',{}).get('dbt_node_name','')!=node: continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); p=resp.get(v) or {}
    print("RCTX_DEC "+json.dumps({"node":node,"decision":v,"clone_source":p.get('clone_source'),
      "deps":[(d.get('name'),d.get('default_schema'),d.get('query','').strip()[:40]) for d in req.get('query_dependencies',[])]}))
    print("DECISION="+str(v)); sys.exit(0)
print("DECISION=__NONE__"); sys.exit(1)
PY
}

# The upstream view DEFINITION TEXT is IDENTICAL across arms (references leaf).
write_up(){ cat > models/marts/$UP.sql <<SQL
{{ config(materialized='view') }}
select id from {{ ref('$LEAF') }}
SQL
}
write_leaf(){ cat > models/marts/$LEAF.sql <<SQL
{{ config(materialized='table') }}
select $1 as id
SQL
}

echo "############ RESOLUTION-CONTEXT (same-body) id=$ID ############"
cat > models/marts/$CONS_CTL.sql <<SQL
{{ config(materialized='table') }}
select id from {{ ref('$UP') }}
SQL
cat > models/marts/$CONS_TRT.sql <<SQL
{{ config(materialized='table') }}
select id from {{ ref('$UP') }}
SQL

echo "--- 1. prod: leaf={1}, build leaf+up+both cons ---"
write_leaf 1; write_up
run_build prod_demo /tmp/rctx_prod.txt 4 "$LEAF" "$UP" "$CONS_CTL" "$CONS_TRT"

echo "--- 2a. CONTROL: dev builds leaf with SAME data {1} + up ---"
write_leaf 1; write_up
run_build dev_clone /tmp/rctx_ctl_up.txt 2 "$LEAF" "$UP"
CF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true); CB=0; [ -n "$CF" ] && CB=$(wc -l < "$CF")
run_build dev_clone /tmp/rctx_ctl_cons.txt 1 "$CONS_CTL"
CF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
echo "--- ORACLE CONTROL: same-body cross-env positive must CLONE ---"
C_OUT=$(decision_for "$CF" "$CB" "$CONS_CTL") || { echo "$C_OUT"; fail "control: no submit captured"; }
echo "$C_OUT" | grep RCTX_DEC
C_DEC=$(echo "$C_OUT" | sed -n 's/^DECISION=//p')
[ "$C_DEC" = "ready_to_clone" ] || fail "CONTROL expected ready_to_clone (same body+data cross-env positive) but got '$C_DEC'"

echo "--- 2b. TREATMENT: dev rebuilds leaf with DIFFERENT data {9}; up TEXT unchanged ---"
write_leaf 9; write_up
run_build dev_clone /tmp/rctx_trt_up.txt 2 "$LEAF" "$UP"
TF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true); TB=0; [ -n "$TF" ] && TB=$(wc -l < "$TF")
run_build dev_clone /tmp/rctx_trt_cons.txt 1 "$CONS_TRT"
TF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
echo "--- OBSERVE TREATMENT: identical body, different leaf DATA (do NOT force) ---"
T_OUT=$(decision_for "$TF" "$TB" "$CONS_TRT") || { echo "$T_OUT"; fail "treatment: no submit captured"; }
echo "$T_OUT" | grep RCTX_DEC
T_DEC=$(echo "$T_OUT" | sed -n 's/^DECISION=//p')
contents=$(query_contents "CDO_DEMO_DB.dev_clone_demo.$CONS_TRT")
echo "$contents"
echo "RCTX_TREATMENT_HOSTED_DECISION=$T_DEC"
echo "RCTX_TREATMENT_CONTENTS=$contents"
echo "(If decision=ready_to_execute AND contents=[9]: context genuinely matters."
echo " If decision=ready_to_clone AND contents=[1]: hosted keys on body only.)"
