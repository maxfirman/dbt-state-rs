#!/usr/bin/env bash
# Quoted-identifier case equivalence/distinctness (review #2 4th follow-up, P0).
#
# Snowflake default identifier resolution (QUOTED_IDENTIFIERS_IGNORE_CASE=false):
#   * an UNQUOTED identifier resolves to its UPPERCASE spelling, so unquoted
#     INPUT is the SAME relation as quoted "INPUT";
#   * a QUOTED identifier is case-sensitive, so "input" is a DIFFERENT relation.
#   (https://docs.snowflake.com/en/sql-reference/identifiers-syntax)
#
# This measures how the HOSTED dbt State service keys a dependency reference's
# identifier case. The consumer selects from an upstream relation by a HARD-CODED
# fully-qualified name so we control the quoting the client sends.
#
# ARM A (POSITIVE, should CLONE): prod builds cons referencing the source by its
#   UNQUOTED fqn (DB.SCHEMA.SRC). dev builds cons referencing the SAME relation
#   by its QUOTED-UPPERCASE fqn ("DB"."SCHEMA"."SRC"). These are the same
#   relation, so a correct service REUSES/CLONES in dev.
#
# ARM B (NEGATIVE, should EXECUTE): dev builds cons referencing the source by a
#   QUOTED-LOWERCASE table component ("DB"."SCHEMA"."src"), which is a DISTINCT,
#   non-existent relation. The identity differs, so the service cannot CLONE the
#   prod cons; it must EXECUTE (and in fact the ref will error at the warehouse,
#   which itself proves the names are not interchangeable — but the DECISION is
#   what we assert from the capture, taken BEFORE the warehouse step).
#
# UNIQUE per-run names; only mutates CDO_DEMO_DB; restores jaffle-shop on exit.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
SRC="qc_src_$ID"; CONS="qc_cons_$ID"
PRODDB="CDO_DEMO_DB"; PRODSCHEMA="prod_state_demo"

cd "$PROJ" || { echo "FATAL no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$SRC.sql models/marts/$CONS.sql macros/_qcexp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
fail(){ echo "FATAL: $1"; exit 1; }

run_build(){ # $1=target $2=logfile $3=expected_ok_count $4+=select args ; returns rc
  local target="$1" log="$2" expected="$3"; shift 3
  dbt build --profile snowflake --target "$target" --select "$@" \
    --skip-semantic-manifest-validation >"$log" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then return $rc; fi
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned|Warned" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected nodes ok (target=$target)"; }
  return 0
}

# $1 = the fully-qualified reference spelling for the source relation.
write_cons(){
  cat > models/marts/$CONS.sql <<SQL
{{ config(materialized='table') }}
select id from $1
SQL
}
write_src(){
  cat > models/marts/$SRC.sql <<SQL
{{ config(materialized='table') }}
select 1 as id, '$ID' as run_tag
SQL
}

decision_for(){ # $1=golden-file $2=before-count $3=node ; prints DEC line, sets global DEC
  python3 - "$1" "$2" "$3" <<'PY'
import json,sys
gf,before,node=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='SubmitEnrichedSQL': continue
    req=r['request']
    if req.get('labels',{}).get('dbt_node_name','')!=node: continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); p=resp.get(v) or {}
    ed=p.get('explained_decision',{})
    print("QC_DEC "+json.dumps({"node":node,"decision":v,
      "clone_source":p.get('clone_source'),
      "desc":ed.get('decision_description'),
      "sql":req.get('sql','').strip()}))
    print("DECISION="+str(v))
    sys.exit(0)
print("DECISION=__NONE__"); sys.exit(1)
PY
}

echo "############ QUOTED-IDENTIFIER CASE id=$ID ############"
echo "--- 1. prod: build src + cons(ref = UNQUOTED $PRODDB.$PRODSCHEMA.$SRC) ---"
write_src
write_cons "$PRODDB.$PRODSCHEMA.$SRC"
run_build prod_demo /tmp/qc_prod.txt 2 "$SRC" "$CONS" || { cat /tmp/qc_prod.txt; fail "prod build failed"; }
# Build src in dev so the clone has a fresh source present in dev as well.
run_build dev_clone /tmp/qc_devsrc.txt 1 "$SRC" || { cat /tmp/qc_devsrc.txt; fail "dev src build failed"; }

echo "--- 2. ARM A (POSITIVE): dev build cons(ref = QUOTED-UPPERCASE) -> expect CLONE/REUSE ---"
GFA_BEFORE_FILE=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true); A_BEFORE=0
[ -n "$GFA_BEFORE_FILE" ] && A_BEFORE=$(wc -l < "$GFA_BEFORE_FILE")
write_cons "\"$PRODDB\".\"$PRODSCHEMA\".\"$SRC\""
run_build dev_clone /tmp/qc_armA.txt 1 "$CONS" || true   # decision is captured regardless of warehouse rc
GFA=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GFA" ] || fail "no golden capture (arm A)"
echo "--- ORACLE A: dev decision for QUOTED-UPPERCASE ref (assert ready_to_clone) ---"
A_OUT=$(decision_for "$GFA" "$A_BEFORE" "$CONS") || { echo "$A_OUT"; fail "arm A: no cons submit captured"; }
echo "$A_OUT" | grep "QC_DEC"
A_DEC=$(echo "$A_OUT" | sed -n 's/^DECISION=//p')
[ "$A_DEC" = "ready_to_clone" ] || fail "arm A expected ready_to_clone (uppercase-quoted == unquoted) but got '$A_DEC'"

echo "--- 3. ARM B (NEGATIVE): dev build cons(ref = QUOTED-LOWERCASE table) -> expect EXECUTE ---"
GFB_BEFORE_FILE=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true); B_BEFORE=0
[ -n "$GFB_BEFORE_FILE" ] && B_BEFORE=$(wc -l < "$GFB_BEFORE_FILE")
write_cons "\"$PRODDB\".\"$PRODSCHEMA\".\"$(echo "$SRC" | tr 'A-Z' 'a-z')\""
run_build dev_clone /tmp/qc_armB.txt 1 "$CONS" || true   # ref may error at warehouse; decision captured first
GFB=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GFB" ] || fail "no golden capture (arm B)"
echo "--- ORACLE B: dev decision for QUOTED-LOWERCASE ref (assert ready_to_execute) ---"
B_OUT=$(decision_for "$GFB" "$B_BEFORE" "$CONS") || { echo "$B_OUT"; fail "arm B: no cons submit captured"; }
echo "$B_OUT" | grep "QC_DEC"
B_DEC=$(echo "$B_OUT" | sed -n 's/^DECISION=//p')
[ "$B_DEC" = "ready_to_execute" ] || fail "arm B expected ready_to_execute (lowercase-quoted is a distinct relation) but got '$B_DEC'"

echo "BOTH ARMS OK: hosted keys quoted-uppercase == unquoted (CLONE) and quoted-lowercase distinct (EXECUTE)."
