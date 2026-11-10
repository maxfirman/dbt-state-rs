#!/usr/bin/env bash
# The lag_tolerance docs' worked example, scaled down, through the real dbt
# client (jaffle-shop, Snowflake prod_demo) and the recording proxy.
#
# docs.getdbt.com/reference/resource-configs/lag-tolerance: 45m tolerance, a
# job every 30 min, build at 08:00, new data at 08:20 -> reuse at 08:30,
# rebuild at 09:00. Here: a 3m tolerance on ltd_m over ltd_up (lag 0s) over a
# SOURCE ltd_raw. The upstream change is a source change that dbt propagates
# by rebuilding ltd_up (a recorded build), as in normal use.
#
#   R0 t=0      build +ltd_m          first builds
#   (source gets a new row)
#   R1 t~40s    build +ltd_m          expect ltd_up EXECUTE, ltd_m SKIP within tolerance
#   R2 t>180s   build +ltd_m          expect ltd_up SKIP, ltd_m EXECUTE (tolerance exceeded)
#   R3          build +ltd_m          expect both SKIP (up to date)
#
# Requires experiments/proxy_up.sh running. Only mutates CDO_DEMO_DB.prod_state_demo.
set -euo pipefail
REPO=/home/develop/projects/dbt-state-rs
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/lagx_golden}"
PORT="${PORT:-50099}"
TOL="${TOL:-3m}"
TOL_S="${TOL_S:-180}"
RAW=CDO_DEMO_DB.prod_state_demo.ltd_raw
cd "$PROJ"
cleanup(){ cd "$PROJ"; rm -f models/marts/ltd_up.sql models/marts/ltd_m.sql models/marts/_ltd_sources.yml macros/_ltd_op.sql; }
trap cleanup EXIT
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false
if [ "${DIRECT:-0}" = 1 ]; then
  # Control run: the client talks to api.state.dbt.com itself with its own
  # dbt Cloud credential (no proxy, so no capture; read dbt's own output).
  unset RUN_CACHE_API_URL RUN_CACHE_API_SECURE
fi
DBT=(dbt --profile snowflake --target prod_demo --skip-semantic-manifest-validation)

run_sql(){ # $1 = SQL, executed via a throwaway run-operation; fails loudly
  cat > macros/_ltd_op.sql <<MACRO
{% macro _ltd_op() %}{% do run_query("$1") %}{% endmacro %}
MACRO
  "${DBT[@]}" run-operation _ltd_op >/tmp/ltd_op.txt 2>&1 || { cat /tmp/ltd_op.txt; exit 1; }
  rm -f macros/_ltd_op.sql
}

cat > models/marts/_ltd_sources.yml <<'YML'
sources:
  - name: ltd
    database: CDO_DEMO_DB
    schema: prod_state_demo
    tables:
      - name: ltd_raw
YML
cat > models/marts/ltd_up.sql <<'SQL'
{{ config(materialized='table', state={'lag_tolerance': '0s'}) }}
select id from {{ source('ltd', 'ltd_raw') }}
SQL
cat > models/marts/ltd_m.sql <<SQL
{{ config(materialized='table', state={'lag_tolerance': '$TOL'}) }}
select count(*) as n from {{ ref('ltd_up') }}
SQL

GF=$(ls -t "$GOLDEN_DIR"/golden_*.jsonl | head -1)
build(){ # $1 = label
  local before; before=$(wc -l < "$GF")
  "${DBT[@]}" build --select +ltd_m >/tmp/ltd_$1.txt 2>&1 || { cat /tmp/ltd_$1.txt; exit 1; }
  echo "=== $1 ($(date +%T)) ==="
  if [ "${DIRECT:-0}" = 1 ]; then
    grep -E "ltd_(up|m)" /tmp/ltd_$1.txt | sed 's/^ *//'
  else
    python3 "$REPO/experiments/golden_tail.py" "$GF" "$before" ltd_
  fi
}

run_sql "create or replace table $RAW as select 1 as id"
T0=$(date +%s)
build R0
sleep 20
run_sql "insert into $RAW values (2)"
sleep 10
build R1
NOW=$(date +%s); WAIT=$(( TOL_S + 15 - (NOW - T0) )); [ "$WAIT" -gt 0 ] && sleep "$WAIT"
build R2
build R3
