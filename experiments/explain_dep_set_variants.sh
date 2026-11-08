#!/usr/bin/env bash
# #32 follow-on probes. Iteration 1 (explain_dependency_set_changed.sh) showed a
# VIEW indirection does NOT move the consumer's `tables` set and hosted rebuilt
# via UPSTREAM_QUERY_CHANGED, not a dependency-set change. This driver tests the
# two remaining ticket candidates for a genuine added/removed/AMBIGUOUS upstream
# NAME-SET change with a MATCHABLE own fingerprint, and whether hosted has any
# distinct "dependency set changed" rendering:
#
#   VARIANT S (ref SWAP): C = select * from ref(A1)  -> rebuild as ref(A2).
#     EXPECTED to prove the coupling: swapping the upstream changes C's rendered
#     SQL (the FQN is embedded), so node_sql_hash changes too. Oracle = did C's
#     rendered SQL change between the two submits? If YES, an added/removed name
#     change is UNREACHABLE with a matchable fingerprint (structural finding).
#
#   VARIANT D (DUPLICATE/ambiguous): C = select x.id from ref(A1) x join ref(A1) y.
#     Two refs to the SAME upstream. Does hosted's `tables` carry a duplicate
#     (ambiguous set) or dedupe to one? Oracle = C's submitted `tables` multiset.
#     Rebuild-to-single-ref compares the ambiguous-vs-clean decision.
#
# Both variants also drive `dbt state explain` so we capture whatever hosted
# renders. DISCOVERY, not assertion. Clean tree, git restore on exit, unique
# names, CDO_DEMO_DB only, Snowflake only. Run experiments/proxy_up.sh first.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
A1="dv_a1_$ID"; A2="dv_a2_$ID"; CS="dv_cs_$ID"; CD="dv_cd_$ID"

cd "$PROJ" || { echo "FATAL: no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$A1.sql models/marts/$A2.sql models/marts/$CS.sql models/marts/$CD.sql; }
trap cleanup EXIT
mkdir -p models/marts
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
DBT="dbt --profile snowflake --target prod_demo --skip-semantic-manifest-validation"
fail(){ echo "FATAL: $1"; exit 1; }

if ! (timeout 2 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  fail "recording proxy not listening on 127.0.0.1:$PORT — run experiments/proxy_up.sh first"
fi
gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
lc(){ local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }

run_build(){ local log="$1" expected="$2"; shift 2
  $DBT build --select "$@" >"$log" 2>&1; local rc=$?
  [ $rc -eq 0 ] || { cat "$log"; fail "dbt build exited $rc"; }
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected nodes ok"; }
}

base_models(){
  cat > models/marts/$A1.sql <<SQL
{{ config(materialized='table') }}
select 1 as id
SQL
  cat > models/marts/$A2.sql <<SQL
{{ config(materialized='table') }}
select 1 as id
SQL
}

echo "############ #32 VARIANTS id=$ID ############"
base_models
run_build /tmp/dv_base.txt 2 "$A1" "$A2"

echo
echo "===================== VARIANT S: ref SWAP ====================="
cat > models/marts/$CS.sql <<SQL
{{ config(materialized='table') }}
select * from {{ ref('$A1') }}
SQL
run_build /tmp/dv_cs1.txt 1 "$CS"       # phase 1: C -> A1
SWAP=$(lc)
cat > models/marts/$CS.sql <<SQL
{{ config(materialized='table') }}
select * from {{ ref('$A2') }}
SQL
$DBT build --select "$CS" >/tmp/dv_cs2.txt 2>&1 || true   # phase 2: C -> A2
grep -iE "Succeeded|Reused|SKIP|skip|Completed" /tmp/dv_cs2.txt | sed 's/^ *//' | tail -3
$DBT state explain --verbose --select "$CS" >/tmp/dv_cs_explain.txt 2>&1 || true
echo "--- explain (swap) ---"; sed 's/^ *//' /tmp/dv_cs_explain.txt | tail -20

echo
echo "===================== VARIANT D: DUPLICATE ref ====================="
cat > models/marts/$CD.sql <<SQL
{{ config(materialized='table') }}
select x.id from {{ ref('$A1') }} x join {{ ref('$A1') }} y on x.id = y.id
SQL
run_build /tmp/dv_cd1.txt 1 "$CD"       # phase 1: C joins A1 to itself
DUP=$(lc)
$DBT build --select "$CD" >/tmp/dv_cd2.txt 2>&1 || true   # phase 2: resubmit, unchanged
grep -iE "Succeeded|Reused|SKIP|skip|Completed" /tmp/dv_cd2.txt | sed 's/^ *//' | tail -3
$DBT state explain --verbose --select "$CD" >/tmp/dv_cd_explain.txt 2>&1 || true
echo "--- explain (duplicate) ---"; sed 's/^ *//' /tmp/dv_cd_explain.txt | tail -20

echo
echo "############ ORACLE: submits for $CS and $CD (tables multiset + decision) ############"
python3 - "$(gf)" "$CS" "$CD" "$A1" "$A2" <<'PY'
import json, sys
gf, CS, CD, A1, A2 = sys.argv[1:6]
rows = [json.loads(l) for l in open(gf) if l.strip()]
RESULT = {0: "SKIP", 1: "EXECUTE", 3: "CLONE", 4: "UNKNOWN"}
def dump(node):
    subs=[]
    for r in rows:
        if r.get("method") != "SubmitEnrichedSQL": continue
        req=r.get("request",{}) or {}
        if (req.get("labels",{}) or {}).get("dbt_node_name") != node: continue
        resp=(r.get("response",{}) or {}).get("response",{}) or {}
        v=next(iter(resp.keys()),None); p=resp.get(v) or {}
        ed=p.get("explained_decision",{}) or {}
        subs.append((req.get("tables",[]), ed, req.get("sql","").strip()))
    print(f"### {node}: {len(subs)} submit(s)")
    for i,(tbls,ed,sql) in enumerate(subs):
        names=[t.get("name") for t in tbls]
        print(f"  [{i}] decision={RESULT.get(ed.get('decision'))} srr={ed.get('skip_rejection_reason')} "
              f"is_stale={ed.get('is_stale')}")
        print(f"      desc={ed.get('decision_description')!r}")
        print(f"      tables(n={len(names)})={names}")
        print(f"      sql={sql!r}")
    return subs
s=dump(CS)
print()
d=dump(CD)
print()
# Findings
if len(s)>=2:
    sql_changed = s[0][2] != s[-1][2]
    print(f"VARIANT S FINDING: ref swap changed C's rendered SQL: {sql_changed}")
    if sql_changed:
        print("  => an added/removed upstream NAME-SET change cannot occur with a matchable")
        print("     node_sql_hash: the upstream FQN is embedded in the rendered SQL. The hosted")
        print("     service sees NODE_QUERY_CHANGED, never a pure dependency-set diff.")
if d:
    tbls=[t.get("name") for t in d[0][0]]
    # genuine upstreams = non-own-target A1 occurrences
    a1n=sum(1 for n in tbls if A1.upper() in (n or "").upper())
    print(f"VARIANT D FINDING: duplicate-ref submit carries {a1n} occurrence(s) of {A1} in tables "
          f"(multiset len {len(tbls)}).")
    print("  => if 1, hosted DEDUPES duplicate refs (no ambiguous set is ever submitted);")
    print("     if >1, hosted preserves the duplicate (ambiguous set reachable).")
PY

echo
echo "NOTE: findings feed the #32 resolution comment + #33 build-vs-drop decision."
