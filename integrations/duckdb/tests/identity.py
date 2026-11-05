"""Independent warehouse results and schema for SQL formatting reuse."""

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
(root / "models").mkdir(exist_ok=True)
(root / "seeds").mkdir(exist_ok=True)
(root / "dbt_project.yml").write_text(
    'name: duckdb_smoke\nversion: "1.0"\nconfig-version: 2\nprofile: smoke\n'
    "models:\n  duckdb_smoke:\n    +materialized: table\n"
)
(root / "seeds/numbers.csv").write_text("id,amount\n1,10\n2,20\n2,20\n3,\n")
(root / "profiles.yml").write_text(
    "smoke:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
    f"      path: {root / 'warehouse.duckdb'}\n"
    "      schema: dev\n      threads: 2\n      run_cache_enabled: true\n"
)
for command, sql, decision, increment in [
    ("build", "select id, amount + 1 as amount from {{ ref('numbers') }}", "build", 1),
    (
        "run",
        "select /* harmless */ id,\n  amount + 1 as amount\n"
        "from {{ ref('numbers') }} -- trailing comment\n",
        "skip",
        1,
    ),
    ("run", "select id, amount + 2 as amount from {{ ref('numbers') }}", "build", 2),
]:
    (root / "models/totals.sql").write_text(sql)
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("invoke.py")),
            command,
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
        ],
        env={**os.environ, "EXPECT_DECISION": decision},
        check=True,
    )
    results = json.loads((root / "target/run_results.json").read_text())["results"]
    assert all(r["status"] in ("success", "pass", "skipped") for r in results), results
    with duckdb.connect(str(root / "warehouse.duckdb")) as db:
        reference = (
            f"select id, amount + {increment} as amount from dev.numbers order by id"
        )
        expected = db.execute(reference).fetchall()
        expected_schema = [
            (r[0], r[1]) for r in db.execute(f"describe {reference}").fetchall()
        ]
        actual = db.execute("select * from dev.totals order by id").fetchall()
        actual_schema = [
            (r[0], r[1]) for r in db.execute("describe dev.totals").fetchall()
        ]
        assert actual == expected == [
            (1, 10 + increment),
            (2, 20 + increment),
            (2, 20 + increment),
            (3, None),
        ]
        assert actual_schema == expected_schema, (actual_schema, expected_schema)
