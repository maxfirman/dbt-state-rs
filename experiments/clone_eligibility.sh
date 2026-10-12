#!/usr/bin/env bash
# CLONE FALLBACK ELIGIBILITY — controlled dimensions (review #2 finding 2 / P0,
# 2nd follow-up P1 #4 + P2 #5).
#
# Isolates clone-fallback dimensions with the CORRECT nested State config (the
# earlier version wrongly used top-level keys and declared them unavailable):
#
#   {{ config(materialized='table', state={'lag_tolerance':'0s',
#                                          'evaluate_volatile_sql': true}) }}
#
# ARG1 = arm (holds everything fixed EXCEPT the named dimension):
#   stale_postol : lag_tolerance=2700s (positive). Stale source (built before an
#                  upstream bump); dev submit upstream is newer.
#   stale_zerotol: lag_tolerance=0s (ZERO tolerance), else identical to stale_postol.
#   volatile     : evaluate_volatile_sql=true (tolerate_nondeterminism=false on
#                  the wire), lag_tolerance=2700s, else identical.
#
# For every arm it prints the prod SOURCE submit+confirm (so the source build
# epoch is captured) and the dev clone decision, PLUS the EFFECTIVE wire fields
# freshness_tolerance_seconds / tolerate_nondeterminism (asserting the intended
# independent variable actually changed). UNIQUE per-run names; only mutates
# CDO_DEMO_DB; FAILS HARD on any failed build/mutation (exit status checked).
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ARM="${1:-stale_postol}"
ID="${2:-$(date +%H%M%S)}"
UP="ce_up_$ID"; MODEL="ce_model_$ID"

cd "$PROJ" || { echo "FATAL no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f "models/marts/$UP.sql" "models/marts/$MODEL.sql" macros/_ceexp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
fail(){ echo "FATAL: $1"; exit 1; }

# Reliable precondition: dbt EXIT STATUS is authoritative; also require the
# expected number of nodes to have succeeded/reused/cloned.
run_build(){ # $1=target $2=log $3=expected_ok $4+=select
  local target="$1" log="$2" expected="$3"; shift 3
  dbt build --profile snowflake --target "$target" --select "$@" \
    --skip-semantic-manifest-validation >"$log" 2>&1
  local rc=$?
  [ $rc -eq 0 ] || { cat "$log"; fail "dbt build exited $rc (target=$target)"; }
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected nodes ok (target=$target)"; }
}
run_op(){ # $1=sql $2=target ; fails on nonzero exit (NOT swallowed)
  cat > macros/_ceexp.sql <<MACRO
{% macro _ceexp() %}{% do run_query("$1") %}{{ log("ran-op-ok", info=true) }}{% endmacro %}
MACRO
  dbt run-operation _ceexp --profile snowflake --target "$2" --skip-semantic-manifest-validation >/tmp/ce_op.txt 2>&1
  local rc=$?; rm -f macros/_ceexp.sql
  [ $rc -eq 0 ] && grep -q "ran-op-ok" /tmp/ce_op.txt || { cat /tmp/ce_op.txt; fail "mutation failed: $1"; }
}

# Per-arm nested State config (verified parseable in dbt 2.0.6).
case "$ARM" in
  stale_postol)  STATE="state={'lag_tolerance': '2700s'}"; PROJ="select id, amount, $ID as runmark from {{ ref('$UP') }}";;
  stale_zerotol) STATE="state={'lag_tolerance': '0s'}"; PROJ="select id, amount, $ID as runmark from {{ ref('$UP') }}";;
  # The volatile arm uses a GENUINE volatile function (current_timestamp) so the
  # evaluate_volatile_sql flag is meaningful: the model's output depends on a
  # nondeterministic expression, which is the original volatile-reuse concern.
  volatile)      STATE="state={'lag_tolerance': '2700s', 'evaluate_volatile_sql': true}"; PROJ="select id, amount, $ID as runmark, current_timestamp() as loaded_at from {{ ref('$UP') }}";;
  *) fail "unknown arm $ARM";;
esac

cat > "models/marts/$UP.sql" <<SQL
{{ config(materialized='table') }}
select 1 as id, 100 as amount
SQL
cat > "models/marts/$MODEL.sql" <<SQL
{{ config(materialized='table', $STATE) }}
$PROJ
SQL

echo "############ CLONE ELIGIBILITY ARM=$ARM id=$ID ############"
echo "--- 1. prod: build $UP + $MODEL (source holds OLD upstream) ---"
run_build prod_demo /tmp/ce_prod.txt 2 "$UP" "$MODEL"

echo "--- 2. prod: bump upstream (source NOT rebuilt -> stale) ---"
run_op "create or replace table CDO_DEMO_DB.prod_state_demo.$UP as (select 1 as id, 100 as amount)" prod_demo

echo "--- 3. dev: build $UP (dev upstream now newer/current) ---"
run_build dev_clone /tmp/ce_devup.txt 1 "$UP"

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)

echo "--- 4. dev: build $MODEL (absent target, defers to prod) ---"
run_build dev_clone /tmp/ce_dev.txt 1 "$MODEL"
grep -iE "Cloned|Reused|Succeeded" /tmp/ce_dev.txt | sed 's/^ *//' | tail -2

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GF" ] || fail "no golden capture"
echo "--- captured $MODEL chain + EFFECTIVE wire fields (independent var check) ---"
python3 - "$GF" "$MODEL" "$ARM" <<'PY' || { echo "FATAL: clone-eligibility assertions failed"; exit 1; }
import json,sys
gf,model,arm=sys.argv[1],sys.argv[2],sys.argv[3]
rows=[json.loads(l) for l in open(gf) if l.strip()]
chain=[r for r in rows if model in json.dumps(r) and r['method'] in ('SubmitEnrichedSQL','ConfirmExecution')]
dev=None
source_build_epoch=None
for r in chain:
    req=r.get('request',{}) or {}
    if r['method']=='ConfirmExecution':
        print("CE_CONFIRM lme="+str(req.get('last_modified_epoch')))
        if source_build_epoch is None:
            source_build_epoch=req.get('last_modified_epoch')
        continue
    resp=r.get('response',{}).get('response',{}) or {}
    v=next(iter(resp.keys()),None); p=resp.get(v) or {}
    ed=p.get('explained_decision',{})
    tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
    rec={"arm":arm,"schema":req.get('default_schema'),
         "ftol":req.get('freshness_tolerance_seconds'),"tnd":req.get('tolerate_nondeterminism'),
         "decision":v,"srr":ed.get('skip_rejection_reason'),"crr":ed.get('clone_rejection_reason'),
         "desc":ed.get('decision_description'),"clone_source":p.get('clone_source'),
         "clone_req_epoch":p.get('clone_required_last_modified_epoch'),"up":tabs}
    print("CE "+json.dumps(rec))
    if req.get('default_schema')=='dev_clone_demo':
        dev=rec

# (1) a dev submit must have been captured.
if dev is None:
    print("ERROR: no dev submit captured"); sys.exit(1)

# (2) the intended independent variable must have reached the wire.
exp={"stale_postol":(2700,True),"stale_zerotol":(0,True),"volatile":(2700,False)}[arm]
if (dev["ftol"], dev["tnd"]) != exp:
    print(f"ERROR: VARCHECK FAILED arm={arm} expected ftol/tnd {exp} got "
          f"({dev['ftol']},{dev['tnd']}) - the independent variable did NOT reach the wire")
    sys.exit(1)

# (3) if the dev submit CLONED, the selected source must have a captured build
# epoch and the staleness interval must hold (upstream NEWER than the source
# build) - otherwise we cannot claim this is a STALE-source clone.
if dev["decision"]=="ready_to_clone":
    if dev["clone_req_epoch"] is None:
        print("ERROR: ready_to_clone with no clone_required_last_modified_epoch"); sys.exit(1)
    up_epochs=[e for e in dev["up"].values() if e is not None]
    if not up_epochs:
        print("ERROR: ready_to_clone with no upstream epoch to check staleness"); sys.exit(1)
    if not (max(up_epochs) > dev["clone_req_epoch"]):
        print(f"ERROR: staleness interval broken - upstream {max(up_epochs)} is NOT newer "
              f"than the clone source epoch {dev['clone_req_epoch']}"); sys.exit(1)
    print(f"CE_OK arm={arm}: STALE-source clone - source epoch {dev['clone_req_epoch']} "
          f"< upstream {max(up_epochs)}; ftol/tnd={exp} reached the wire")
else:
    print(f"CE_OK arm={arm}: decision={dev['decision']} (not a clone); ftol/tnd={exp} verified")
PY
