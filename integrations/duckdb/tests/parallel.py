"""Parallel fan-out/fan-in, literal result oracle and worker failure recovery."""

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
root.mkdir(parents=True)
(root / "models").mkdir()
(root / "dbt_project.yml").write_text(
    'name: parallel\nversion: "1.0"\nconfig-version: 2\nprofile: parallel\n'
    "models:\n  parallel:\n    +materialized: table\n"
)
path = root / "warehouse.duckdb"
# Lag tolerance 0: these cases test that every evidence change propagates
# (lag_tolerance.py covers reuse within a tolerance, ADR 0007).
(root / "profiles.yml").write_text(
    f"parallel:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
    f"      path: {path}\n      schema: dev\n      threads: 4\n      run_cache_enabled: true\n      run_cache_freshness_tolerance: 0s\n"
)
(root / "models/root.sql").write_text("select * from main.inputs")
for branch, expression in (
    ("a", "amount + 1"),
    ("b", "amount * 2"),
    ("c", "amount + 3"),
):
    (root / f"models/branch_{branch}.sql").write_text(
        f"select id, {expression} as amount from {{{{ ref('root') }}}}"
    )
(root / "models/joined.sql").write_text(
    "select a.id, a.amount as a, b.amount as b, c.amount as c "
    "from {{ ref('branch_a') }} a join {{ ref('branch_b') }} b using(id) "
    "join {{ ref('branch_c') }} c using(id)"
)
with duckdb.connect(str(path)) as db:
    db.execute("create table main.inputs(id integer, amount integer)")
    db.execute("insert into main.inputs values (1,10), (1,10), (2,NULL), (3,7)")

names = ["root", "branch_a", "branch_b", "branch_c", "joined"]
expected_rows = [(1, 11, 20, 13)] * 8 + [(2, None, None, None), (3, 8, 14, 10)]


def run(decision, *, failure=False):
    expected = {n: decision for n in names if not (failure and n == "joined")}
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("parallel_invoke.py")),
            "run",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
        ],
        env={
            **os.environ,
            "PARALLEL_EXPECT": json.dumps(expected),
            "PARALLEL_FAIL": str(int(failure)),
            "PARALLEL_OBSERVATIONS": str(root / "observations.json"),
        },
        check=False,
        timeout=120,
        capture_output=True,
        text=True,
    )
    (root / f"run-{len(list(root.glob('run-*.log')))}.log").write_text(
        result.stdout + result.stderr
    )
    assert result.returncode == 0, result.stdout + result.stderr
    artifact = json.loads((root / "observations.json").read_text())
    if failure:
        outcomes = json.loads((root / "target/run_results.json").read_text())["results"]
        statuses = {r["unique_id"].rsplit(".", 1)[-1]: r["status"] for r in outcomes}
        assert statuses["branch_b"] == "error" and statuses["joined"] == "skipped", (
            statuses
        )
    else:
        with duckdb.connect(str(path)) as db:
            actual = db.execute("select * from dev.joined order by all").fetchall()
            schema = db.execute("describe dev.joined").fetchall()
        # Literal independent oracle preserves duplicate join multiplicity,
        # NULLs and column types; no production fingerprints/decisions involved.
        assert actual == expected_rows
        assert [(c[0], c[1]) for c in schema] == [
            ("id", "INTEGER"),
            ("a", "INTEGER"),
            ("b", "INTEGER"),
            ("c", "INTEGER"),
        ]
    return artifact


cold = run("build")
run("skip")
# Changed logic produces identical rows. Every known write must advance the
# shared ordinal, invalidate downstream evidence, then settle to positive SKIP.
(root / "models/root.sql").write_text(
    "select id, amount + 0 as amount from main.inputs"
)
identical = run("build")
assert min(identical["confirmed"].values()) > max(cold["confirmed"].values())
run("skip")
# Parent data changes must become visible to every branch and then the join.
with duckdb.connect(str(path)) as db:
    db.execute("update main.inputs set amount=amount+5")
expected_rows = [(1, 16, 30, 18)] * 8 + [(2, None, None, None), (3, 13, 24, 15)]
run("build")
run("skip")
# All branches are submitted concurrently; one real SQL worker fails. Successful
# peers finish, the fan-in is withheld, and restart must cold-build a new scope.
with duckdb.connect(str(path)) as db:
    db.execute("update main.inputs set amount=amount+0")
(root / "models/root.sql").write_text(
    "select id, amount + 0 + 0 as amount from main.inputs"
)
branch_b = root / "models/branch_b.sql"
sql = branch_b.read_text()
branch_b.write_text(sql + "\n{{ config(pre_hook=\"SELECT CAST('bad' AS BIGINT)\") }}")
failed = run("build", failure=True)
branch_b.write_text(sql)
recovered = run("build")
assert {r["namespace"] for r in failed["nodes"].values()}.isdisjoint(
    {r["namespace"] for r in recovered["nodes"].values()}
)
run("skip")
