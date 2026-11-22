#!/usr/bin/env bash
# Opt-in live acceptance: the pinned dbt Fusion binary runs a real dbt build
# on Snowflake through the Token broker and the server in oidc mode
# (crates/harness/tests/fusion_snowflake.rs; docs/testing.md, "Live Fusion
# acceptance on Snowflake").
#
#   scripts/fusion-snowflake.sh
#
# DBT_STATE_FUSION_DBT       the pinned dbt binary (default: dbt on PATH);
#                            the test refuses any other (dbt 2.0.11, SHA-256
#                            f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31)
# DBT_STATE_SNOWFLAKE_PROFILES  the profiles.yml to copy from
#                            (default ~/.dbt/profiles.yml)
# DBT_STATE_SNOWFLAKE_PROFILE / _TARGET  the Snowflake output to copy
#                            (default snowflake / cdo_demo)
#
# Only that one output is copied, to a private temporary file the test reads
# and this script deletes. It must be a Snowflake output whose database is
# CDO_DEMO_DB: the run creates, builds and drops its own schema there
# (DBT_STATE_FUSION_<random>) and writes nowhere else. dbt runs with a
# temporary HOME, so ~/.dbt (dbt_cloud.yml, state_auth.json) is neither read
# nor written, and every dbt State URL names the local test deployment, so
# hosted dbt State is never contacted. Needs Postgres at $DATABASE_URL.
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
dbt=${DBT_STATE_FUSION_DBT:-$(command -v dbt || true)}
[ -n "$dbt" ] || { echo "no dbt on PATH; set DBT_STATE_FUSION_DBT" >&2; exit 2; }
profiles=${DBT_STATE_SNOWFLAKE_PROFILES:-$HOME/.dbt/profiles.yml}
profile=${DBT_STATE_SNOWFLAKE_PROFILE:-snowflake}
target=${DBT_STATE_SNOWFLAKE_TARGET:-cdo_demo}

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
umask 077

python3 - "$profiles" "$profile" "$target" >"$work/output.json" <<'PY'
import json, sys
import yaml

path, profile, target = sys.argv[1:]
with open(path) as file:
    profiles = yaml.safe_load(file) or {}
try:
    output = profiles[profile]["outputs"][target]
except (KeyError, TypeError):
    sys.exit(f"{path} has no output {profile}.{target}")
database = output.get("database")
if output.get("type") != "snowflake":
    sys.exit(f"{profile}.{target} is not a Snowflake output")
if not isinstance(database, str) or database.upper() != "CDO_DEMO_DB":
    sys.exit(f"{profile}.{target} does not use database CDO_DEMO_DB")
json.dump(output, sys.stdout)
PY

cd "$repo_dir"
DBT_STATE_FUSION_DBT=$dbt \
DBT_STATE_FUSION_SNOWFLAKE_PROFILE=$work/output.json \
  cargo test -p dbt-state-harness --test fusion_snowflake -- --nocapture
