#!/usr/bin/env bash
# Capture `dbt state explain` for the CLONE decision path and the dev_clone
# profile-deferral flow, and pin Explain.GetUpstreamDependencyChanges
# (ticket #23 of map #19). The Snowflake happy path (#20) never triggered
# GetUpstreamDependencyChanges, never produced a CLONE explain tree, and never
# explained a deferred node.
#
# Mechanics are lifted from experiments/clone_experiment.sh (the proven
# prod_demo/dev_clone deferral+clone flow on jaffle-shop) with a `state explain`
# slice appended via experiments/explain_slice.py (shared with #20/#24).
#
# DISCIPLINE: clean tree -> git restore on exit -> only mutate CDO_DEMO_DB
# (prod_state_demo + dev_clone_demo). Run experiments/proxy_up.sh first.
set -uo pipefail
PROJ=~/projects/jaffle-shop
REPO=/home/develop/projects/dbt-state-rs
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
NODE=clone_probe

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/clone_probe.sql macros/_exp_drop.sql 2>/dev/null || true; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
DBT_PROD="dbt --profile snowflake --target prod_demo --skip-semantic-manifest-validation"
DBT_DEV="dbt --profile snowflake --target dev_clone --skip-semantic-manifest-validation"
# NOTE: every call site MUST include a subcommand (build / state explain) after
# the $DBT_* prefix — these vars are flag prefixes, not a full command.

if ! (timeout 2 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  echo "!! recording proxy not listening on 127.0.0.1:$PORT — run experiments/proxy_up.sh first" >&2
  exit 1
fi

gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
lc(){ local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }

# Clone-eligible probe model (table), selecting a real upstream so dev_clone can
# clone it from prod and a deferred-upstream explain is observable.
cat > models/marts/clone_probe.sql <<'SQL'
{{ config(materialized='table') }}
select order_id, customer_id, ordered_at, subtotal, tax_paid, order_total
from {{ ref('stg_orders') }}
SQL

drop_dev_object(){ # strictly scoped to CDO_DEMO_DB demo schemas
  local obj="$1" target="${2:-dev_clone}"
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
    --skip-semantic-manifest-validation >/tmp/ecd_drop.txt 2>&1 || true
  rm -f macros/_exp_drop.sql
  tail -2 /tmp/ecd_drop.txt
}

echo "############ EXPLAIN: CLONE + DEFERRAL ############"

echo "### [prebuild, outside window] stg_orders in prod_demo + dev_clone ###"
$DBT_PROD build --select stg_orders >/tmp/ecd_pre_prod.txt 2>&1 || true
$DBT_DEV  build --select stg_orders >/tmp/ecd_pre_dev.txt  2>&1 || true
tail -1 /tmp/ecd_pre_prod.txt; tail -1 /tmp/ecd_pre_dev.txt

echo "### [reset] drop probe in both schemas so the clone path can fire ###"
drop_dev_object "CDO_DEMO_DB.dev_clone_demo.$NODE" dev_clone
drop_dev_object "CDO_DEMO_DB.prod_state_demo.$NODE" prod_demo

echo "### 1. build the clone SOURCE in prod_demo ###"
$DBT_PROD build --select "$NODE" >/tmp/ecd_prod.txt 2>&1 || true
grep -iE "Reused|Succeeded|Cloned|Error|Failed" /tmp/ecd_prod.txt | sed 's/^ *//' | head -6

echo "### 2. build in dev_clone (expect CLONE from prod) ###"
$DBT_DEV build --select "$NODE" >/tmp/ecd_dev.txt 2>&1 || true
grep -iE "Reused|Succeeded|Cloned|clone|Error|Failed" /tmp/ecd_dev.txt | sed 's/^ *//' | head -10

# --- CLONE explain: explain the dev_clone target right after the clone ---
echo "### 3a. state explain (dev_clone) after clone — plain ###"
C=$(lc)
$DBT_DEV state explain --select "$NODE" >/tmp/ecd_explain_clone.txt 2>&1 || true
sed 's/^ *//' /tmp/ecd_explain_clone.txt | tail -25

echo "### 3b. state explain --verbose (dev_clone) after clone ###"
$DBT_DEV state explain --verbose --select "$NODE" >/tmp/ecd_explain_clone_v.txt 2>&1 || true
sed 's/^ *//' /tmp/ecd_explain_clone_v.txt | tail -40

# --- DEFERRAL explain: build only the probe's DOWNSTREAM-less leaf in dev_clone
#     while its upstream (stg_orders) is deferred to prod_demo, then explain the
#     whole selection so a deferred-upstream node appears. ---
echo "### 4. selective dev_clone build deferring stg_orders, then explain ###"
D=$(lc)
# Drop the dev probe again so this run re-decides with deferral in play.
drop_dev_object "CDO_DEMO_DB.dev_clone_demo.$NODE" dev_clone
$DBT_DEV build --select "$NODE" >/tmp/ecd_defer_build.txt 2>&1 || true
grep -iE "defer|Reused|Succeeded|Cloned|clone|Error|Failed" /tmp/ecd_defer_build.txt | sed 's/^ *//' | tail -8
$DBT_DEV state explain --verbose >/tmp/ecd_explain_defer_v.txt 2>&1 || true
sed 's/^ *//' /tmp/ecd_explain_defer_v.txt | tail -40

echo
echo "############ CAPTURED EXPLAIN TRAFFIC (entries after clone source build) ############"
python3 "$REPO/experiments/explain_slice.py" "$(gf)" "$C"

echo
echo "NOTE: promote the newest golden to golden/fixtures/explain_clone_deferral.jsonl"
echo "  (trim to Explain + GetUpstreamDependencyChanges + referenced Submit/Clone entries)"
