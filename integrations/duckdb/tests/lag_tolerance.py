"""Lag tolerance on DuckDB (ADR 0007) through real dbt runs.

DuckDB epochs are evidence ordinals, so build age is measured on the server
clock from the build's confirmation. `up` rebuilds on every source change
(tolerance 0); `m` over `up` has a 20s tolerance:

1. a source change dbt propagates by rebuilding `up` is reused by `m` while
   m's build is younger than the tolerance, serving its earlier rows;
2. once m's build is older than the tolerance, `m` rebuilds;
3. an external write to `up` (a dbt-built table) is an upstream query change:
   `m` rebuilds even within its tolerance.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb

TOLERANCE_S = 20

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
(root / "models").mkdir(exist_ok=True)
(root / "dbt_project.yml").write_text(
    'name: lag_cases\nversion: "1.0"\nconfig-version: 2\nprofile: lag\nmodels:\n  lag_cases:\n    +materialized: table\n'
)
(root / "profiles.yml").write_text(
    f"lag:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: {root / 'warehouse.duckdb'}\n      schema: dev\n      threads: 2\n      run_cache_enabled: true\n      run_cache_freshness_tolerance: 0s\n"
)
(root / "models/up.sql").write_text(
    'select id, amount from {{ source("raw", "numbers") }}'
)
(root / "models/m.sql").write_text(
    f"{{{{ config(state={{'lag_tolerance': '{TOLERANCE_S}s'}}) }}}}\n"
    'select count(*) as n, sum(amount) as total from {{ ref("up") }}'
)
(root / "models/schema.yml").write_text(
    "version: 2\nsources:\n  - name: raw\n    schema: raw\n    tables:\n      - name: numbers\n"
)
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute(
        "create schema raw; create table raw.numbers(id integer, amount integer); insert into raw.numbers values (1,10),(2,20)"
    )


def run(expected, m_rows, *args):
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("views_invoke.py")),
            "run",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
            *args,
        ],
        env={**os.environ, "VIEW_EXPECT": json.dumps(expected)},
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    with duckdb.connect(str(root / "warehouse.duckdb")) as db:
        assert db.execute("select n, total from dev.m").fetchall() == m_rows
    return result.stdout


run({"up": "build", "m": "build"}, [(2, 30)])
built = time.monotonic()
run({"up": "skip", "m": "skip"}, [(2, 30)])

# 1. A propagated source change is reused within m's tolerance (stale rows).
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute("insert into raw.numbers values (3,30)")
out = run({"up": "build", "m": "skip"}, [(2, 30)])
assert "within tolerance" in out, out

# 2. Once m's build is older than the tolerance, it rebuilds.
time.sleep(max(0.0, TOLERANCE_S + 2 - (time.monotonic() - built)))
run({"up": "skip", "m": "build"}, [(3, 60)])
run({"up": "skip", "m": "skip"}, [(3, 60)])

# 3. An external write to the consumed dbt-built `up` rebuilds m at once.
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute("update dev.up set amount = amount + 1")
run({"m": "build"}, [(3, 63)], "--select", "m")
