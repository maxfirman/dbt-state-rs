#!/usr/bin/env bash
# #32 final reachability probe. The two prior variants showed (S) an upstream
# FQN is embedded in rendered SQL so an added/removed name always changes
# node_sql_hash, and (D) the client DEDUPES duplicate refs so an ambiguous set is
# never submitted. This probe tries the one remaining way to ADD a genuine
# upstream to the `tables` set WITHOUT changing the emitted SQL fingerprint: a
# `ref()` inside a trailing SQL COMMENT. dbt registers the dependency from the
# ref() regardless of where it sits, but comments are (per harness.md) normalized
# away by the server fingerprint. IF the client both (a) adds A2 to `tables` and
# (b) keeps node_sql_hash equal to the no-comment build, we'd finally reach a
# genuine added-name set with a matchable fingerprint — the exact
# DependencySetChanged precondition. Oracle = C's two submits: did `tables` gain
# A2 while the decision stayed matchable (SKIP / reused) rather than
# NODE_QUERY_CHANGED EXECUTE?
#
# DISCOVERY. Clean tree, git restore on exit, unique names, CDO_DEMO_DB only,
# Snowflake only. Run experiments/proxy_up.sh first.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
A1="cr_a1_$ID"; A2="cr_a2_$ID"; C="cr_c_$ID"

cd "$PROJ" || { echo "FATAL: no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$A1.sql models/marts/$A2.sql models/marts/$C.sql; }
trap cleanup EXIT
mkdir -p models/marts
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
DBT="dbt --profile snowflake --target prod_demo --skip-semantic-manifest-validation"
fail(){ echo "FATAL: $1"; exit 1; }
if ! (timeout 2 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  fail "recording proxy not listening on 127.0.0.1:$PORT — run experiments/proxy_up.sh first"
fi
gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
run_build(){ local log="$1" expected="$2"; shift 2
  $DBT build --select "$@" >"$log" 2>&1; local rc=$?
  [ $rc -eq 0 ] || { cat "$log"; fail "dbt build exited $rc"; }
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected nodes ok"; }
}

cat > models/marts/$A1.sql <<SQL
{{ config(materialized='table') }}
select 1 as id
SQL
cat > models/marts/$A2.sql <<SQL
{{ config(materialized='table') }}
select 2 as id
SQL

echo "############ #32 COMMENT-REF PROBE id=$ID ############"
run_build /tmp/cr_base.txt 2 "$A1" "$A2"

echo "### phase 1: C refs only A1 (A2 absent from deps) ###"
cat > models/marts/$C.sql <<SQL
{{ config(materialized='table') }}
select * from {{ ref('$A1') }}
SQL
run_build /tmp/cr_c1.txt 1 "$C"

echo "### phase 2: add A2 via a trailing comment ref; resubmit C ###"
cat > models/marts/$C.sql <<SQL
{{ config(materialized='table') }}
select * from {{ ref('$A1') }}
-- depends on {{ ref('$A2') }}
SQL
$DBT build --select "$C" >/tmp/cr_c2.txt 2>&1 || true
grep -iE "Succeeded|Reused|SKIP|skip|Completed|Error" /tmp/cr_c2.txt | sed 's/^ *//' | tail -4
$DBT state explain --verbose --select "$C" >/tmp/cr_explain.txt 2>&1 || true
echo "--- explain ---"; sed 's/^ *//' /tmp/cr_explain.txt | tail -22

echo
echo "############ ORACLE: C submits — did tables gain A2 with a matchable decision? ############"
python3 - "$(gf)" "$C" "$A1" "$A2" <<'PY'
import json, sys
gf, C, A1, A2 = sys.argv[1:5]
rows=[json.loads(l) for l in open(gf) if l.strip()]
RESULT={0:"SKIP",1:"EXECUTE",3:"CLONE",4:"UNKNOWN"}
subs=[]
for r in rows:
    if r.get("method")!="SubmitEnrichedSQL": continue
    req=r.get("request",{}) or {}
    if (req.get("labels",{}) or {}).get("dbt_node_name")!=C: continue
    resp=(r.get("response",{}) or {}).get("response",{}) or {}
    v=next(iter(resp.keys()),None); p=resp.get(v) or {}
    ed=p.get("explained_decision",{}) or {}
    subs.append((req.get("tables",[]), ed, req.get("sql","").strip(),
                 [d.get("name") for d in req.get("query_dependencies",[])]))
for i,(tbls,ed,sql,qd) in enumerate(subs):
    names=[t.get("name") for t in tbls]
    print(f"  [{i}] decision={RESULT.get(ed.get('decision'))} srr={ed.get('skip_rejection_reason')}")
    print(f"      desc={ed.get('decision_description')!r}")
    print(f"      tables={names}")
    print(f"      query_dependencies={qd}")
    print(f"      sql={sql!r}")
if len(subs)>=2:
    def hasA(names,a): return any(a.upper() in (n or "").upper() for n in names)
    t0=[t.get("name") for t in subs[0][0]]; t1=[t.get("name") for t in subs[-1][0]]
    sql_same = subs[0][2]==subs[-1][2]
    a2_added = (not hasA(t0,A2)) and hasA(t1,A2)
    print()
    print(f"FINDING: C rendered SQL identical across phases: {sql_same}")
    print(f"FINDING: A2 added to tables set in phase 2: {a2_added}")
    print(f"FINDING: phase-2 decision: {RESULT.get(subs[-1][1].get('decision'))} "
          f"desc={subs[-1][1].get('decision_description')!r}")
    if a2_added and sql_same:
        print("=> REACHABLE: a genuine added-upstream name set WITH a matchable fingerprint. "
              "Inspect the explain tree above for a dependency-set subtree.")
    else:
        print("=> NOT reachable this way: either A2 did not enter the dependency set, or the "
              "comment changed the fingerprint. Dependency-set change remains unreachable live.")
PY
