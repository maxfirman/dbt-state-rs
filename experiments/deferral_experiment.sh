#!/usr/bin/env bash
# Characterize ResolveDeferredRelations against the real hosted service.
#
# When building from dev_clone (defer_to_target: prod_demo) and selecting only a
# downstream node, the Fusion client defers the UNSELECTED upstreams to their
# already-built relations in prod_demo. The run-cache mechanism for resolving
# those physical names is Execution.ResolveDeferredRelations(profile, target,
# project, node_unique_ids) -> map<node_unique_id, fqn>.
#
# Hypothesis: after the upstreams are built in prod_demo, a selective dev_clone
# build (that defers them) calls ResolveDeferredRelations and the hosted service
# returns the prod_demo FQNs for those nodes.
#
# Only mutates CDO_DEMO_DB; git-restores jaffle-shop after.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true; }
trap cleanup EXIT
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
lc(){ local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }

echo "### 1. build the FULL project in prod_demo (records every node's FQN) ###"
dbt build --profile snowflake --target prod_demo --skip-semantic-manifest-validation \
  --vars 'load_source_data: true' >/tmp/def_base.txt 2>&1 || true
tail -2 /tmp/def_base.txt

echo "### 2. selective build in dev_clone of a LEAF model, deferring upstreams ###"
# customers (mart) depends on stg_customers, stg_orders, etc. Selecting only it
# forces its upstreams to be deferred to prod_demo.
B=$(lc)
dbt build --profile snowflake --target dev_clone --select customers \
  --skip-semantic-manifest-validation >/tmp/def_sel.txt 2>&1 || true
grep -iE "defer|Reused|Succeeded|Error|clone" /tmp/def_sel.txt | tail -8

echo "### ResolveDeferredRelations captured this run ###"
python3 - "$(gf)" "$B" <<'PY'
import json,sys
gf,before=sys.argv[1],int(sys.argv[2])
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='ResolveDeferredRelations': continue
    req=r.get('request',{}) or {}; resp=r.get('response',{}) or {}
    ids=req.get('node_unique_ids',[])
    m=resp.get('fqn_by_unique_id',{}) or {}
    print(json.dumps({"profile":req.get('profile_name'),"target":req.get('target_name'),
      "project":req.get('project_name'),"project_id":req.get('project_id'),
      "n_ids":len(ids),"ids":ids,"n_resolved":len(m),"map":m}, indent=2))
PY
