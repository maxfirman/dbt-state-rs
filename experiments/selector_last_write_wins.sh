#!/usr/bin/env bash
# Probe: is the dbt State selector comparison baseline LAST-WRITE-WINS?
#
# The review (issue #1) flags that node_states is written on every Submit/Record
# with no ordering guard, so a later record overwrites an earlier one. Before
# adding any ordering machinery we must know what the HOSTED service does. This
# characterizes the SEQUENTIAL semantics (the only thing a capture can prove):
# does re-recording a node in the defer-to env move the selector baseline to the
# newest recorded content?
#
# Timeline (prod_demo is the defer-to env; selector queried from dev_clone):
#   1. baseline: build the project in prod_demo (records node states as V1).
#   2. query state:modified from dev_clone with the UNCHANGED project ->
#      stg_customers must NOT be selected (its submitted hashes == recorded V1).
#   3. rebuild ONLY stg_customers in prod_demo with a CHANGED body (new column),
#      recording a NEW baseline V2 for it.
#   4. query state:modified from dev_clone with the ORIGINAL project (V1 body):
#        - if stg_customers IS now selected  => baseline moved to V2
#          => LAST-WRITE-WINS (the newest record defines the comparison base).
#        - if it is NOT selected             => baseline is pinned to the FIRST
#          record (or some other rule) => our latest-wins upsert would be WRONG.
#
# Only ever mutates CDO_DEMO_DB. git-restores jaffle-shop after.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
MODEL=stg_customers
cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true; }
trap cleanup EXIT
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
lc(){ local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }
report(){ python3 - "$(gf)" "$1" "$2" <<'PY'
import json,sys
gf,before,label=sys.argv[1],int(sys.argv[2]),sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='GetStateSelection': continue
    req,resp=r['request'],r['response']
    sel=set(resp.get('node_unique_ids',[]))
    hit=[s for s in sel if 'stg_customers' in s]
    print("SEL[%s] crit=%s sent=%d selected=%d stg_customers_selected=%s"%(
        label,req['selector_criteria'],len(req['nodes']),len(sel),bool(hit)))
PY
}

echo "### 1. baseline build in prod_demo (records V1) ###"
git restore . 2>/dev/null || true
dbt build --profile snowflake --target prod_demo --select $MODEL \
  --skip-semantic-manifest-validation >/tmp/lww_base.txt 2>&1 || true
grep -iE "Succeeded|Reused|Error|Completed" /tmp/lww_base.txt | tail -2

echo "### 2. query state:modified (unchanged) -> stg_customers must NOT be selected ###"
B=$(lc)
dbt compile --profile snowflake --target dev_clone --select "state:modified" \
  --skip-semantic-manifest-validation >/tmp/lww_q1.txt 2>&1 || true
report "$B" "clean"

echo "### 3. rebuild stg_customers in prod_demo with CHANGED body (records V2) ###"
# stg_customers is a staging view: add a harmless constant column.
F=models/staging/stg_customers.sql
cp "$F" /tmp/lww_stg_customers.bak
# append a projected column inside the final select by editing the file via python
python3 - "$PROJ/$F" <<'PY'
import sys,re
p=sys.argv[1]; s=open(p).read()
# add ", 1 as _lww_probe" after the first "select" of the final CTE-less select.
# jaffle stg_customers ends with a `select ... from ...`; inject a column.
# Safest: wrap whole model: select *, 1 as _lww_probe from ( <orig> )
open(p,'w').write("select *, 1 as _lww_probe from (\n"+s+"\n)\n")
print("patched",p)
PY
dbt build --profile snowflake --target prod_demo --select $MODEL \
  --skip-semantic-manifest-validation >/tmp/lww_rebuild.txt 2>&1 || true
grep -iE "Succeeded|Reused|Error|Completed" /tmp/lww_rebuild.txt | tail -2
git restore . 2>/dev/null || true   # restore ORIGINAL (V1) project for the next query

echo "### 4. query state:modified with ORIGINAL V1 body -> selected IFF baseline moved to V2 ###"
B=$(lc)
dbt compile --profile snowflake --target dev_clone --select "state:modified" \
  --skip-semantic-manifest-validation >/tmp/lww_q2.txt 2>&1 || true
report "$B" "after-rebuild"

echo "### INTERPRETATION:"
echo "###   stg_customers_selected=True in step 4 => LAST-WRITE-WINS (latest record is the base)."
echo "###   stg_customers_selected=False          => baseline is NOT latest-wins; investigate."
