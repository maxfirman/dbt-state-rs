#!/usr/bin/env bash
# #34: HISTORY-BEARING re-capture of the happy-path SKIP explain batch (replaces
# the empty-store gaps explain_snowflake_happy.jsonl entries 40/41/74/75).
#
# The original explain_snowflake_happy.jsonl built every jaffle_shop node ONCE
# then explained, so on empty-store replay each node first-builds and its
# re-derived tree diverges in decision CLASS from the hosted SKIP. This driver
# builds a compact self-contained selection, CONFIRMS it (prior state), then
# RE-BUILDS it UNCHANGED so every node SKIPs, all in ONE window — so the promoted
# fixture seeds the replay store and the main gate asserts the hosted SKIP trees
# directly (model SKIP, view SKIP, data-test SKIP with per-upstream FRESH).
#
# The selection deliberately spans the SKIP tree shapes the gap covers:
#   hh_up    (table)  : a source-less model     -> model SKIP, no-upstream shape
#   hh_mid   (table)  : select from hh_up       -> model SKIP, upstream-FRESH shape
#   hh_view  (view)   : select from hh_mid      -> VIEW SKIP shape
#   not_null_hh_mid_id (data test on hh_mid.id) -> DATA-TEST SKIP, upstream-FRESH
#
# DISCIPLINE: clean tree, git restore on exit, unique per-run names, CDO_DEMO_DB
# only, Snowflake only. Run experiments/proxy_up.sh first.
set -uo pipefail
PROJ=~/projects/jaffle-shop
REPO=/home/develop/projects/dbt-state-rs
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
UP="hh_up_$ID"; MID="hh_mid_$ID"; VIEW="hh_view_$ID"

cd "$PROJ" || { echo "FATAL: no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/$UP.sql models/marts/$MID.sql models/marts/$MID.yml models/marts/$VIEW.sql 2>/dev/null || true; }
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
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned|Warned|PASS" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected nodes ok"; }
}

# Unique content so no cross-env equivalent exists to clone.
cat > models/marts/$UP.sql <<SQL
{{ config(materialized='table') }}
select 1 as id, '$ID' as tag
SQL
cat > models/marts/$MID.sql <<SQL
{{ config(materialized='table') }}
select id, tag from {{ ref('$UP') }}
SQL
cat > models/marts/$VIEW.sql <<SQL
{{ config(materialized='view') }}
select id from {{ ref('$MID') }}
SQL
cat > models/marts/$MID.yml <<YML
models:
  - name: $MID
    columns:
      - name: id
        data_tests:
          - not_null
YML

echo "############ #34 HISTORY-BEARING HAPPY-PATH SKIP batch id=$ID ############"
START=$(lc)

echo "### 1. baseline build (table + table + view + data test) + Confirms ###"
run_build /tmp/hh_base.txt 4 "+$VIEW" "$MID"
grep -iE "Succeeded|Reused|PASS" /tmp/hh_base.txt | sed 's/^ *//' | tail -6

echo "### 2. re-build UNCHANGED -> every node SKIPs over seeded history ###"
$DBT build --select "+$VIEW" "$MID" >/tmp/hh_skip.txt 2>&1 || true
grep -iE "Reused|no-op|SKIP|PASS" /tmp/hh_skip.txt | sed 's/^ *//' | tail -6

echo "### 3. state explain --verbose, ONE node per call (covers all 4 SKIP shapes) ###"
# Explain each node in its OWN `dbt state explain` call: a single multi-node
# --select gets collapsed by the client to a narrower GetExplainMessages (and
# method/fqn selectors are rejected or mangled), which silently dropped the data
# test and source-less model before. One call per node guarantees a
# GetExplainMessages (and tree) is captured for each of the four SKIP shapes:
# source-less model, upstream-FRESH model, VIEW, and data test.
TEST_NODE="not_null_${MID}_id"
: > /tmp/hh_explain.txt
# Models resolve by plain name; the generic data test resolves by its fqn
# (jaffle_shop.<path>.<test_name>) — a plain name / method selector does not
# match it in `dbt state explain`.
for n in "$UP" "$MID" "$VIEW" "test.jaffle_shop.${TEST_NODE}.*"; do
  echo "--- explain $n ---" >>/tmp/hh_explain.txt
  $DBT state explain --verbose --select "$n" >>/tmp/hh_explain.txt 2>&1 || true
done
sed 's/^ *//' /tmp/hh_explain.txt | tail -70

echo
echo "############ CAPTURED TRAFFIC — full history-bearing window ############"
python3 "$REPO/experiments/explain_slice.py" "$(gf)" "$START"

echo
echo "############ ORACLE: window has Submit(EXECUTE)+Confirm then SKIP + explain of ALL 4 kinds ############"
python3 - "$(gf)" "$START" "$UP" "$MID" "$VIEW" <<'PY'
import json, sys
gf, start, up, mid, view = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
rows = [json.loads(l) for l in open(gf) if l.strip()][start:]
def kind(r):
    resp=(r.get("response",{}) or {}).get("response",{}) or {}
    return next(iter(resp.keys()), None)
def node_of(r): return (r.get("request",{}).get("labels",{}) or {}).get("dbt_node_name","")
execs = [r for r in rows if r.get("method")=="SubmitEnrichedSQL" and kind(r)=="ready_to_execute"]
confs = [r for r in rows if r.get("method")=="ConfirmExecution"]
# Map every SKIP submit's edid -> node, then see which were explained.
skip_edid = {}
for r in rows:
    if r.get("method")=="SubmitEnrichedSQL" and kind(r)=="skip_execution":
        edid = r["response"]["response"]["skip_execution"].get("execution_decision_id")
        skip_edid[edid] = node_of(r)
explained_nodes = set()
for r in rows:
    if r.get("method")=="GetExplainMessages":
        for i in r["request"].get("execution_decision_ids", []):
            if i in skip_edid:
                explained_nodes.add(skip_edid[i])
test_node = f"not_null_{mid}_id"
want = {up, mid, view, test_node}
print(f"execute-submits={len(execs)} confirms={len(confs)}")
print(f"explained SKIP nodes = {sorted(explained_nodes)}")
missing = want - explained_nodes
ok = len(execs)>=1 and len(confs)>=1 and not missing
print("HISTORY-BEARING + ALL 4 KINDS EXPLAINED:" ,
      "YES" if ok else f"NO — missing explain for {sorted(missing)}")
if not ok: sys.exit(1)
PY

echo
echo "NOTE: promote the newest golden to golden/fixtures/explain_happy_history.jsonl"
echo "  (trim to the baseline EXECUTE+Confirm batch, the unchanged re-SKIP batch, and the"
echo "   GetExplainMessages call(s); drop telemetry). Remove explain_snowflake_happy.jsonl"
echo "   entries 40/41/74/75 from golden/replay_gaps.json once asserted."
