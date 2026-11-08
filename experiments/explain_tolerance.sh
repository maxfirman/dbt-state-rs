#!/usr/bin/env bash
# Capture `dbt state explain` for a WITHIN-freshness-tolerance tolerated-stale
# SKIP (ticket #24 of map #19): produce the WITHIN_TOLERANCE badge live and
# resolve the #22 decision_description version-skew (#2 `...within lag tolerance`
# vs #3 `...at least one upstream dependency is unchanged`).
#
# Mechanics lifted from experiments/lag_experiment.sh: build a model with a
# non-zero lag_tolerance over an upstream, advance the upstream epoch in place
# (identical content -> pure data-freshness bump), re-submit, then `state
# explain`. Two scenarios in ONE session so #2 vs #3 can be compared on ONE
# hosted version:
#   (A) within-tolerance: bump upstream, tolerance LARGE -> tolerated-stale SKIP
#   (B) unchanged-upstream control: no bump -> genuinely-fresh SKIP
#
# DISCIPLINE: clean tree -> git restore on exit -> only mutate
# CDO_DEMO_DB.prod_state_demo. Run experiments/proxy_up.sh first.
set -uo pipefail
PROJ=~/projects/jaffle-shop
REPO=/home/develop/projects/dbt-state-rs
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/conf_golden}"
PORT="${PORT:-50099}"
# A deliberately LARGE tolerance so a small epoch bump stays within it.
LAG="${LAG:-240h}"

cd "$PROJ"; git restore . 2>/dev/null || true
cleanup(){ cd "$PROJ"; git restore . 2>/dev/null || true;
  rm -f models/marts/et_up.sql models/marts/et_model.sql macros/_exp_recreate.sql 2>/dev/null || true; }
trap cleanup EXIT
mkdir -p models/marts macros
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
DBT="dbt --profile snowflake --target prod_demo --skip-semantic-manifest-validation"

if ! (timeout 2 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  echo "!! recording proxy not listening on 127.0.0.1:$PORT — run experiments/proxy_up.sh first" >&2
  exit 1
fi

gf(){ ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }
lc(){ local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }

# Upstream table (constant content) + a model over it with a lag tolerance.
cat > models/marts/et_up.sql <<'SQL'
{{ config(materialized='table') }}
select 1 as id, 'x' as val
SQL
cat > models/marts/et_model.sql <<SQL
{{ config(materialized='table', state={'lag_tolerance': '$LAG', 'require_fresh_data_from': 'any'}) }}
select id, val from {{ ref('et_up') }}
SQL

recreate_up(){ # recreate et_up in place, identical content -> advance epoch only
  local fqn="CDO_DEMO_DB.prod_state_demo.et_up"
  cat > macros/_exp_recreate.sql <<MACRO
{% macro _exp_recreate() %}
  {% do run_query("create or replace table $fqn as (select 1 as id, 'x' as val)") %}
  {{ log("recreated $fqn (epoch advanced, content identical)", info=true) }}
{% endmacro %}
MACRO
  $DBT run-operation _exp_recreate >/tmp/et_recreate.txt 2>&1 || true
  rm -f macros/_exp_recreate.sql
  tail -2 /tmp/et_recreate.txt
}

echo "############ EXPLAIN: WITHIN-FRESHNESS-TOLERANCE (lag=$LAG) ############"

echo "### build baseline (et_up + et_model) in prod_demo ###"
$DBT build --select +et_model >/tmp/et_base.txt 2>&1 || true
grep -iE "Reused|Succeeded|Error|Failed" /tmp/et_base.txt | sed 's/^ *//' | tail -5

echo
echo "### SCENARIO B (control): unchanged upstream -> explain a genuinely-fresh SKIP ###"
B=$(lc)
$DBT build --select et_model >/tmp/et_ctrl_build.txt 2>&1 || true   # should SKIP
$DBT state explain --verbose --select et_model >/tmp/et_ctrl_explain.txt 2>&1 || true
sed 's/^ *//' /tmp/et_ctrl_explain.txt | tail -20

echo
echo "### SCENARIO A: bump upstream epoch WITHIN tolerance -> tolerated-stale SKIP ###"
recreate_up
A=$(lc)
$DBT build --select et_model >/tmp/et_within_build.txt 2>&1 || true  # expect SKIP (within tolerance)
grep -iE "Reused|Succeeded|Error|Failed" /tmp/et_within_build.txt | sed 's/^ *//' | tail -4
$DBT state explain --verbose --select et_model >/tmp/et_within_explain.txt 2>&1 || true
sed 's/^ *//' /tmp/et_within_explain.txt | tail -25

echo
echo "############ CAPTURED EXPLAIN TRAFFIC — CONTROL window (unchanged upstream) ############"
python3 "$REPO/experiments/explain_slice.py" "$(gf)" "$B"
echo
echo "############ CAPTURED EXPLAIN TRAFFIC — WITHIN-TOLERANCE window (bumped upstream) ############"
python3 "$REPO/experiments/explain_slice.py" "$(gf)" "$A"

echo
echo "NOTE: promote the newest golden to golden/fixtures/explain_tolerance.jsonl"
echo "  (trim to the two Explain windows + referenced Submit entries)."
echo "  Compare the two decision_description strings: settles #22 #2-vs-#3 on ONE version."
