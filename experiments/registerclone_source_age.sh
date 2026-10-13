#!/usr/bin/env bash
# REGISTERCLONE SOURCE-AGE vs CLONE-AGE (review #2 2nd follow-up, P0 #2).
#
# The reviewer correctly noted that bumping the upstream AFTER a clone cannot
# distinguish a source-content-age policy from a clone-creation-age policy (both
# rebuild when the upstream is newer than BOTH). The distinguishing interval is:
#
#   source built @T1  ->  upstream changed @T2 (T2 > T1; source NOT rebuilt)
#   clone created @T3 (T3 > T2)  ->  resubmit clone target with upstream @T2'
#   where T2 <= T2' <= T3  (upstream NEWER than the source build, OLDER than the
#   clone creation).
#
# If hosted SKIPs the resubmit, it anchors on the CLONE CREATION epoch (the
# server's own-build anchor matches). If hosted EXECUTEs, it anchors on the
# SOURCE CONTENT age (the clone's data predates the upstream) — which the server
# would NOT currently do, and the policy would need implementing.
#
# This drives an INCREMENTAL model (which clones via the RegisterClone RPC, the
# pre_clone path) so the clone creation is a distinct event from the source
# build. UNIQUE per-run names; only mutates CDO_DEMO_DB; fails hard on any failed
# build/mutation (exit-status checked); asserts the captured epochs.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
ID="${1:-$(date +%H%M%S)}"
UP="rc_up_$ID"; MODEL="rc_model_$ID"

cd "$PROJ" || { echo "FATAL no $PROJ"; exit 1; }
git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f "models/marts/$UP.sql" "models/marts/$MODEL.sql" macros/_rcexp.sql; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
fail(){ echo "FATAL: $1"; exit 1; }

# Reliable precondition: dbt EXIT STATUS is authoritative; also require the
# expected node count to have succeeded/reused/cloned.
run_build(){ # $1=target $2=log $3=expected_ok $4+=select
  local target="$1" log="$2" expected="$3"; shift 3
  dbt build --profile snowflake --target "$target" --select "$@" \
    --skip-semantic-manifest-validation >"$log" 2>&1
  local rc=$?
  [ $rc -eq 0 ] || { cat "$log"; fail "dbt build exited $rc (target=$target)"; }
  local ok; ok=$(grep -ciE "Succeeded|Reused|Cloned|No-op|PASS" "$log")
  [ "$ok" -ge "$expected" ] || { cat "$log"; fail "only $ok/$expected ok (target=$target)"; }
}
run_op(){ # $1=sql $2=target ; fails on nonzero exit
  cat > macros/_rcexp.sql <<MACRO
{% macro _rcexp() %}{% do run_query("$1") %}{{ log("ran-op-ok", info=true) }}{% endmacro %}
MACRO
  dbt run-operation _rcexp --profile snowflake --target "$2" --skip-semantic-manifest-validation >/tmp/rc_op.txt 2>&1
  local rc=$?; rm -f macros/_rcexp.sql
  [ $rc -eq 0 ] && grep -q "ran-op-ok" /tmp/rc_op.txt || { cat /tmp/rc_op.txt; fail "mutation failed: $1"; }
}

cat > "models/marts/$UP.sql" <<SQL
{{ config(materialized='table') }}
select 1 as id, 100 as amount
SQL
# Incremental model → clones via RegisterClone (pre_clone) when absent in dev.
cat > "models/marts/$MODEL.sql" <<SQL
{{ config(materialized='incremental', unique_key='id') }}
select id, amount from {{ ref('$UP') }}
{% if is_incremental() %}where id > (select coalesce(max(id),0) from {{ this }}){% endif %}
SQL

echo "############ REGISTERCLONE SOURCE-AGE vs CLONE-AGE id=$ID ############"
echo "--- 1. prod: build $UP + $MODEL (source @T1) ---"
run_build prod_demo /tmp/rc_prod.txt 2 "$UP" "$MODEL"

echo "--- 2. prod: bump upstream $UP (@T2 > T1; source NOT rebuilt) ---"
run_op "create or replace table CDO_DEMO_DB.prod_state_demo.$UP as (select 1 as id, 100 as amount)" prod_demo

echo "--- 3. dev: build $UP (so dev ref resolves) ---"
run_build dev_clone /tmp/rc_devup.txt 1 "$UP"

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true); BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l < "$GF")

echo "--- 4. dev: build $MODEL (absent → RegisterClone @T3 > T2) ---"
run_build dev_clone /tmp/rc_devclone.txt 1 "$MODEL"
grep -iE "Cloned|Reused|Succeeded" /tmp/rc_devclone.txt | sed 's/^ *//' | tail -2

echo "--- 5. dev: resubmit $MODEL (upstream unchanged since clone; T2<=up<=T3) ---"
run_build dev_clone /tmp/rc_dev2.txt 1 "$MODEL"
grep -iE "Cloned|Reused|Succeeded|No-op|no-op" /tmp/rc_dev2.txt | sed 's/^ *//' | tail -2

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
[ -n "$GF" ] || fail "no golden capture"
echo "--- full $MODEL chain (RegisterClone + resubmit); epochs + interval asserted ---"
python3 - "$GF" "$MODEL" "$SCRIPT_DIR" <<'PY' || { echo "FATAL: source-age chain assertions failed"; exit 1; }
import sys
sys.path.insert(0, sys.argv[3])
import oracles, trace_validate as tv
gf, model = sys.argv[1], sys.argv[2]
try:
    r = oracles.source_age_oracle(gf, model)
except tv.ValidationError as e:
    print(e); sys.exit(1)
print(f"RC_OK: successful RegisterClone of {r['clone_source']} (confirmed source build "
      f"{r['t1']}); interval holds (T1={r['t1']} < upstream={r['up']} < T3={r['t3']}); "
      f"hosted dev decision={r['dev_decision']}")
PY
