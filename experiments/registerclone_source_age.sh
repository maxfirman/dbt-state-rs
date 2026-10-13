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
python3 - "$GF" "$MODEL" <<'PY' || { echo "FATAL: source-age chain assertions failed"; exit 1; }
import json,sys
gf,model=sys.argv[1],sys.argv[2]
rows=[json.loads(l) for l in open(gf) if l.strip()]
# COMPLETE RPC history for this model (review #2 4th follow-up P1 #4): the
# curated chain keeps only Submit/Confirm/RegisterClone, but the hosted entry-4
# SKIP could be explained by INTERVENING history the curation drops. Audit ALL
# methods (including RecordExecutions and speculative submissions) and FAIL if
# any such entry is interleaved — so a causal claim is never made over a
# filtered trace.
all_for_model=[r for r in rows if model in json.dumps(r)]
seq=[(r['method'], (r.get('request',{}) or {}).get('from_speculative_submit')) for r in all_for_model]
print("RC_FULL_SEQUENCE "+json.dumps([m for m,_ in seq]))
intervening=[m for m,_ in seq if m in ('RecordExecutions',)]
speculative=[m for m,s in seq if s]
if intervening:
    print(f"ERROR: the raw history contains {intervening} for this model; the curated "
          f"Submit/Confirm/RegisterClone chain would hide it — re-characterize before "
          f"attributing any cause to the entry-4 SKIP"); sys.exit(1)
if speculative:
    print(f"ERROR: the raw history contains {len(speculative)} speculative submission(s); "
          f"these affect reuse and must not be filtered out of the causal chain"); sys.exit(1)
chain=[r for r in rows if model in json.dumps(r)
       and r['method'] in ('SubmitEnrichedSQL','ConfirmExecution','RegisterClone')]
saw_register=False
confirms=[]       # confirm epochs in order: [source build (T1), clone creation (T3)]
dev=None
for r in chain:
    req=r.get('request',{}) or {}
    m=r['method']
    if m=='RegisterClone':
        saw_register=True
        print("RC_REGISTER "+json.dumps({"source":req.get('clone_source_table'),
          "target":req.get('target_table'),
          "src_epoch":req.get('clone_source_last_modified_epoch')}))
    elif m=='ConfirmExecution':
        confirms.append(req.get('last_modified_epoch'))
        print("RC_CONFIRM lme="+str(req.get('last_modified_epoch')))
    else:
        resp=r.get('response',{}).get('response',{}) or {}
        v=next(iter(resp.keys()),None); p=resp.get(v) or {}
        ed=p.get('explained_decision',{})
        tabs={t['name'].split('.')[-1].strip('"'):t.get('last_modified_epoch') for t in req.get('tables',[])}
        rec={"schema":req.get('default_schema'),"decision":v,
             "desc":ed.get('decision_description'),"is_stale":ed.get('is_stale'),"up":tabs}
        print("RC "+json.dumps(rec))
        if req.get('default_schema')=='dev_clone_demo':
            dev=rec

# Assertions: the RegisterClone event occurred, both confirms exist, the dev
# resubmit happened, and the DISTINGUISHING interval holds (upstream NEWER than
# the source build T1 but OLDER than the clone creation T3).
if not saw_register:
    print("ERROR: no RegisterClone event captured (did an incremental model clone via pre_clone?)"); sys.exit(1)
if len(confirms) < 2 or confirms[0] is None or confirms[1] is None:
    print(f"ERROR: expected source-build + clone-creation confirms, got {confirms}"); sys.exit(1)
if dev is None:
    print("ERROR: no dev resubmit captured"); sys.exit(1)
t1, t3 = confirms[0], confirms[1]
up_epochs=[e for e in dev["up"].values() if e is not None
           and True]  # all upstream/own epochs on the resubmit
# the UPSTREAM epoch (not the model's own) must sit strictly between T1 and T3
up_only=[e for k,e in dev["up"].items() if e is not None and "MODEL" not in k.upper()]
if not up_only:
    print("ERROR: no upstream epoch on the dev resubmit"); sys.exit(1)
up=max(up_only)
if not (t1 < up < t3):
    print(f"ERROR: distinguishing interval broken - need source build {t1} < upstream {up} < clone creation {t3}"); sys.exit(1)
print(f"RC_OK: RegisterClone present; interval holds (T1={t1} < upstream={up} < T3={t3}); "
      f"hosted dev decision={dev['decision']}")
PY
