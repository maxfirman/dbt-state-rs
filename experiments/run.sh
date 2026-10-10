#!/usr/bin/env bash
# Generic conformance experiment runner.
#
# Usage:
#   run.sh LABEL SELECT [EDIT_PY] [EXTRA_DBT_ARGS...]
#
#   LABEL     : human label for the experiment (printed, and tagged into output)
#   SELECT    : dbt --select expression (e.g. "customers", "+orders", "my_seed")
#   EDIT_PY   : optional path to a python script that mutates project files;
#               receives the jaffle-shop root as argv[1]. Pass "-" for no edit.
#   EXTRA...  : extra args appended to the dbt build command (e.g. --vars '...',
#               --full-refresh)
#
# Always restores the jaffle-shop working tree first (git restore), so each run
# starts from a known-clean project unless EDIT_PY mutates it.
#
# Emits a compact per-node JSON-lines summary of the NEW captured decisions to
# stdout (lines prefixed "DEC "), plus the dbt per-node status lines.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
LABEL="$1"; SELECT="$2"; EDIT="${3:-"-"}"; shift 3 || true
EXTRA=("$@")

cd "$PROJ"
git restore . 2>/dev/null || true

if [ "$EDIT" != "-" ] && [ -n "$EDIT" ]; then
  python3 "$EDIT" "$PROJ" || { echo "EDIT FAILED: $EDIT"; exit 2; }
fi

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l < "$GF")

export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
dbt build --profile snowflake --target prod_demo --skip-semantic-manifest-validation \
  --select "$SELECT" "${EXTRA[@]}" >/tmp/conf_build.txt 2>&1 || true

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
echo "============== $LABEL (select=$SELECT edit=$EDIT extra=${EXTRA[*]:-none}) =============="
grep -iE "Reused|Succeeded|Cloned|Error|Failed" /tmp/conf_build.txt | sed 's/^ *//' | head -30 || true

python3 - "$GF" "$BEFORE" "$LABEL" <<'PY'
import json,sys
gf, before, label = sys.argv[1], int(sys.argv[2]), sys.argv[3]
if not gf:
    print("DEC (no golden file)"); sys.exit(0)
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
def h(x,n=10): return (str(x)[:n] if x is not None else "·")
for r in rows:
    m=r.get("method","")
    req=r.get("request",{}) or {}
    if m=="SubmitEnrichedSQL" or m=="SubmitValues":
        ns=req.get("dbt_node_state",{}) or {}
        name=req.get("labels",{}).get("dbt_node_name","?")
        resp=r.get("response",{}).get("response",{}) or {}
        v=next(iter(resp.keys()),"?")
        rec={
          "label":label,"method":m,"node":name,"decision":v,
          "et":req.get("execution_type"),
          "body":h(ns.get("node_body_hash")),"cfg":h(ns.get("node_configs_hash")),
          "contract":h(ns.get("node_contract_hash")),"macros":h(ns.get("node_macros_hash")),
          "descr":h(ns.get("node_persisted_descriptions_hash")),
          "uid":ns.get("node_unique_id"),"ns":h(req.get("table_namespace")),
          "target":req.get("target_table"),"vhash":h(req.get("values_hash")),
          "schema":req.get("default_schema"),
        }
        print("DEC "+json.dumps(rec))
    elif m in ("RegisterClone","ConfirmExecution","ResolveDeferredRelations","SubmitEnrichedSQLSpeculative"):
        print("DEC "+json.dumps({"label":label,"method":m,
              "node":req.get("labels",{}).get("dbt_node_name"),
              "target":req.get("target_table") or req.get("clone_source_table")}))
PY
