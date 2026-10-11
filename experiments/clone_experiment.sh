#!/usr/bin/env bash
# Clone-scenario experiment driver.
#
# Reproduces the profile-deferral dev-clone flow used to capture the real
# RegisterClone traffic, and parameterizes the knobs we want to characterize:
#   - allow_clones (via the dbt-cloud / state config)
#   - source freshness (rebuild prod source between prod build and dev build)
#   - eligibility (incremental vs table vs snapshot materialization)
#
# It is intentionally idempotent and ALWAYS leaves jaffle-shop restored.
#
# Usage:
#   clone_experiment.sh LABEL MATERIALIZATION [--rebuild-source] [--no-clones]
#
#   MATERIALIZATION : incremental | table | snapshot
#
# Only ever mutates CDO_DEMO_DB (prod_state_demo + dev_clone_demo schemas) in
# Snowflake, via dbt against the `prod_demo` and `dev_clone` profile targets.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
LABEL="$1"; MAT="$2"; shift 2
REBUILD_SOURCE=0; ALLOW_CLONES=1; FRESH_SOURCE=0
for a in "$@"; do
  case "$a" in
    --rebuild-source) REBUILD_SOURCE=1 ;;
    --no-clones) ALLOW_CLONES=0 ;;
    --fresh-source) FRESH_SOURCE=1 ;;
  esac
done

cd "$PROJ"
git restore . 2>/dev/null || true
cleanup() { cd "$PROJ"; git restore . 2>/dev/null || true; rm -f models/marts/clone_probe.sql snapshots/clone_probe_snap.sql 2>/dev/null || true; }
trap cleanup EXIT

# --- define the probe model/snapshot ---------------------------------------
mkdir -p models/marts snapshots
if [ "$MAT" = "snapshot" ]; then
  cat > snapshots/clone_probe_snap.sql <<'SQL'
{% snapshot clone_probe_snap %}
{{ config(target_schema='prod_state_demo', unique_key='order_id',
          strategy='check', check_cols=['order_total']) }}
select order_id, order_total from {{ ref('stg_orders') }}
{% endsnapshot %}
SQL
  NODE=clone_probe_snap
else
  # incremental (merge) or plain table. Based on the proven-good pattern from
  # the real clone_happy_path capture (ref stg_orders, incremental subquery
  # against {{ this }}).
  if [ "$MAT" = "incremental" ]; then
    MATCFG="materialized='incremental', incremental_strategy='merge', unique_key='order_id'"
    BODY="select order_id, customer_id, ordered_at, subtotal, tax_paid, order_total from {{ ref('stg_orders') }}
{% if is_incremental() %}where ordered_at > '1900-01-01'{% endif %}"
  else
    MATCFG="materialized='table'"
    BODY="select order_id, customer_id, ordered_at, subtotal, tax_paid, order_total from {{ ref('stg_orders') }}"
  fi
  cat > models/marts/clone_probe.sql <<SQL
{{ config($MATCFG) }}
$BODY
SQL
  NODE=clone_probe
fi

# --- deferral + clone config on the dev_clone target ------------------------
# The dev-clone flow needs defer_to a built prod environment. We inject it into
# a local copy of profiles.yml knob by setting defer-env via project vars is not
# available; instead rely on the profile `defer_to` already used for the smoke
# build (profile-based auto-deferral to prod). allow_clones is toggled via the
# state config env the client reads.
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

echo "############ $LABEL mat=$MAT rebuild_source=$REBUILD_SOURCE allow_clones=$ALLOW_CLONES ############"

# 0) Ensure the upstream stg_orders exists in BOTH schemas (prod + dev_clone),
#    outside the capture window, so the probe builds without missing refs.
echo "--- [prebuild] stg_orders in prod_demo + dev_clone ---"
dbt build --profile snowflake --target prod_demo --select stg_orders \
  --skip-semantic-manifest-validation >/tmp/clone_pre_prod.txt 2>&1 || true
dbt build --profile snowflake --target dev_clone --select stg_orders \
  --skip-semantic-manifest-validation >/tmp/clone_pre_dev.txt 2>&1 || true
tail -1 /tmp/clone_pre_prod.txt; tail -1 /tmp/clone_pre_dev.txt

# Snapshot the golden file AFTER prebuild so only probe decisions are reported.
GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
BEFORE=0; [ -n "$GF" ] && BEFORE=$(wc -l < "$GF")

# Drop the dev-clone target object so the dev build sees it ABSENT and the
# clone-from-prod path can fire. Strictly scoped to CDO_DEMO_DB.dev_clone_demo.
# Uses a dbt operation so it reuses the profile's authenticated connection.
drop_dev_object() {
  local obj="$1" target="${2:-dev_clone}"
  mkdir -p macros
  cat > macros/_exp_drop.sql <<'MACRO'
{% macro _exp_drop(fqn) %}
  {% set u = fqn | upper %}
  {% if 'CDO_DEMO_DB.DEV_CLONE_DEMO' not in u and 'CDO_DEMO_DB.PROD_STATE_DEMO' not in u %}
    {{ exceptions.raise_compiler_error("refusing to drop outside CDO_DEMO_DB demo schemas: " ~ fqn) }}
  {% endif %}
  {% do run_query("drop table if exists " ~ fqn) %}
  {% do run_query("drop view if exists " ~ fqn) %}
  {{ log("dropped " ~ fqn, info=true) }}
{% endmacro %}
MACRO
  dbt run-operation _exp_drop --args "{fqn: $obj}" --profile snowflake --target "$target" \
    --skip-semantic-manifest-validation >/tmp/clone_drop.txt 2>&1 || true
  rm -f macros/_exp_drop.sql
  tail -3 /tmp/clone_drop.txt
}
if [ "$FRESH_SOURCE" = "1" ]; then
  echo "--- [cleanup] drop prod_state_demo.$NODE so the SOURCE executes+confirms (records logic) ---"
  drop_dev_object "CDO_DEMO_DB.prod_state_demo.$NODE" prod_demo
fi
echo "--- [cleanup] drop dev_clone_demo.$NODE so clone can fire ---"
drop_dev_object "CDO_DEMO_DB.dev_clone_demo.$NODE" dev_clone

# 1) Build the source in prod_demo (the clone source).
echo "--- [prod_demo] build source $NODE ---"
dbt build --profile snowflake --target prod_demo --select "$NODE" \
  --skip-semantic-manifest-validation >/tmp/clone_prod.txt 2>&1 || true
grep -iE "Reused|Succeeded|Cloned|Error|Failed" /tmp/clone_prod.txt | sed 's/^ *//' | head -10

if [ "$REBUILD_SOURCE" = "1" ]; then
  echo "--- [prod_demo] REBUILD source to advance its epoch ---"
  dbt build --profile snowflake --target prod_demo --select "$NODE" --full-refresh \
    --skip-semantic-manifest-validation >/tmp/clone_prod2.txt 2>&1 || true
  grep -iE "Reused|Succeeded|Cloned|Error|Failed" /tmp/clone_prod2.txt | sed 's/^ *//' | head -10
fi

# 2) Build in dev_clone (should trigger a clone-from-prod when the dev object is
#    absent and the model is clone-eligible). allow_clones is a PROFILE config
#    (Fusion has no --no-clone CLI flag); toggle it on the dev_clone target.
echo "--- [dev_clone] build $NODE (expect clone unless allow_clones=false) ---"
PROFILES=~/.dbt/profiles.yml
if [ "$ALLOW_CLONES" = "0" ]; then
  cp "$PROFILES" /tmp/profiles.bak
  sed -i 's/allow_clones: true/allow_clones: false/' "$PROFILES"
  echo "(set allow_clones: false on dev_clone)"
  trap 'cp /tmp/profiles.bak "$PROFILES" 2>/dev/null; rm -f /tmp/profiles.bak; cleanup' EXIT
fi
dbt build --profile snowflake --target dev_clone --select "$NODE" \
  --skip-semantic-manifest-validation >/tmp/clone_dev.txt 2>&1 || true
grep -iE "Reused|Succeeded|Cloned|Error|Failed|clone" /tmp/clone_dev.txt | sed 's/^ *//' | head -15

# --- dump newly captured decisions -----------------------------------------
GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1 || true)
python3 - "$GF" "$BEFORE" "$LABEL" <<'PY'
import json,sys
gf, before, label = sys.argv[1], int(sys.argv[2]), sys.argv[3]
if not gf: print("DEC (no golden file)"); sys.exit(0)
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    m=r.get("method",""); req=r.get("request",{}) or {}; resp=r.get("response",{}) or {}
    rr=resp.get("response",{}) or {}
    v=next(iter(rr.keys()),None) if isinstance(rr,dict) else None
    if m=="RegisterClone":
        rc=resp.get("ready_to_clone") or {}
        utc=resp.get("unable_to_clone")
        ed=(rc or {}).get("explained_decision") or (utc or {}).get("explained_decision") or {}
        print("DEC "+json.dumps({"label":label,"m":m,
          "src":req.get("clone_source_table"),"tgt":req.get("target_table"),
          "et":req.get("execution_type"),"allow_clones":req.get("allow_clones"),
          "src_epoch":req.get("clone_source_last_modified_epoch"),
          "ready":bool(rc),"unable":bool(utc),
          "decision":ed.get("decision"),"srr":ed.get("skip_rejection_reason"),
          "crr":ed.get("clone_rejection_reason"),"descr":ed.get("decision_description"),
          "clone_req_epoch":(rc or {}).get("clone_required_last_modified_epoch")}))
    elif m in ("SubmitEnrichedSQL","SubmitValues"):
        payload=rr.get(v) if v else {}
        ed=(payload or {}).get("explained_decision") or {}
        print("DEC "+json.dumps({"label":label,"m":m,
          "node":req.get("labels",{}).get("dbt_node_name"),
          "tgt":req.get("target_table"),"et":req.get("execution_type"),
          "allow_clones":req.get("allow_clones"),"decision":v,
          "srr":ed.get("skip_rejection_reason"),"crr":ed.get("clone_rejection_reason"),
          "descr":ed.get("decision_description")}))
    elif m in ("ConfirmExecution","ResolveDeferredRelations"):
        print("DEC "+json.dumps({"label":label,"m":m,
          "node":req.get("labels",{}).get("dbt_node_name"),
          "failed_to_clone":req.get("failed_to_clone"),
          "table_type":req.get("table_type")}))
PY
