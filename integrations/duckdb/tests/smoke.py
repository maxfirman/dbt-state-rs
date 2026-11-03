"""Real dbt process workload; no protocol replacement or computed result oracle."""

import json
import subprocess
import sys
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
(root / "models").mkdir(exist_ok=True)
(root / "seeds").mkdir(exist_ok=True)
(root / "tests").mkdir(exist_ok=True)
(root / "dbt_project.yml").write_text(
    'name: duckdb_smoke\nversion: "1.0"\nconfig-version: 2\nprofile: smoke\nmodels:\n  duckdb_smoke:\n    +materialized: table\n'
)
(root / "seeds" / "numbers.csv").write_text("id,amount\n1,10\n2,20\n2,20\n")
(root / "models" / "totals.sql").write_text(
    "select id, sum(amount)::bigint as amount from {{ ref('numbers') }} group by id"
)
(root / "tests" / "positive.sql").write_text(
    "select * from {{ ref('totals') }} where amount <= 0"
)
(root / "profiles.yml").write_text(
    f"smoke:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: {root / 'warehouse.duckdb'}\n      schema: dev\n      threads: 2\n      run_cache_enabled: true\n      run_cache_enable_data_tests: true\n"
)
if len(sys.argv) > 2 and sys.argv[2] == "unsupported":
    with duckdb.connect(str(root / "warehouse.duckdb")) as db:
        db.execute("update _dbt_state.database_identity set version = 999")
subprocess.run(
    [
        sys.executable,
        str(Path(__file__).with_name("invoke.py")),
        "build",
        "--project-dir",
        str(root),
        "--profiles-dir",
        str(root),
    ],
    check=True,
    stdin=subprocess.DEVNULL,
)
results = json.loads((root / "target/run_results.json").read_text())["results"]
assert sorted(r["status"] for r in results) == ["pass", "success", "success"], results
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    assert db.execute("select * from dev.totals order by id").fetchall() == [
        (1, 10),
        (2, 40),
    ]
    assert db.execute("select count(*) from dev.numbers").fetchone() == (3,)
