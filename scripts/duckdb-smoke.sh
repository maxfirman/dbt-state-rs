#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_dir"
uv sync --project integrations/duckdb --locked
export DBT_STATE_DUCKDB_PYTHON="$repo_dir/integrations/duckdb/.venv/bin/python"
cargo test -p dbt-state-harness --test duckdb_python -- --ignored --nocapture
