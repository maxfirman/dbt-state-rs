"""Hidden physical row identity is outside unordered relation evidence."""

import os
import subprocess
import sys
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
(root / "models").mkdir()
(root / "dbt_project.yml").write_text(
    'name: duckdb_smoke\nversion: "1.0"\nconfig-version: 2\nprofile: smoke\n'
    "models:\n  duckdb_smoke:\n    +materialized: table\n"
)
path = root / "warehouse.duckdb"
(root / "profiles.yml").write_text(
    "smoke:\n  target: dev\n  outputs:\n"
    + "".join(
        f"    {target}:\n      type: duckdb\n      path: {path}\n"
        f"      schema: {target}\n      threads: 2\n      run_cache_enabled: true\n"
        for target in ("dev", "prod")
    )
)
with duckdb.connect(str(path)) as db:
    db.execute(
        "create schema raw; create table raw.inputs(id integer); "
        "insert into raw.inputs values (1), (2)"
    )


def reorder():
    with duckdb.connect(str(path)) as db:
        schema = db.execute("describe raw.inputs").fetchall()
        rows = db.execute("select id from raw.inputs order by id").fetchall()
        first = db.execute(
            "select id from raw.inputs order by rowid limit 1"
        ).fetchone()[0]
        direction = "desc" if first == 1 else "asc"
        db.execute(
            "create or replace table raw.inputs as select id from raw.inputs order by id "
            + direction
        )
        assert db.execute("describe raw.inputs").fetchall() == schema
        assert db.execute("select id from raw.inputs order by id").fetchall() == rows


def run(decision, expected, reference, target="dev"):
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("invoke.py")),
            "run",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
            "--target",
            target,
        ],
        env={**os.environ, "EXPECT_DECISION": decision, "EXPECT_NO_COPY": "1"},
        check=True,
    )
    with duckdb.connect(str(path)) as db:
        actual = db.execute(f"select * from {target}.totals order by all").fetchall()
        current = db.execute(reference + " order by all").fetchall()
        assert actual == current == expected, (actual, current, expected)


model = root / "models/totals.sql"
# Aliases and string literals named rowid do not read hidden physical identity.
model.write_text("select id as rowid, 'rowid' as label from warehouse.raw.inputs")
run("build", [(1, "rowid"), (2, "rowid")], "select id, 'rowid' from raw.inputs")
reorder()
run("skip", [(1, "rowid"), (2, "rowid")], "select id, 'rowid' from raw.inputs")

for query in (
    "select rowid as pos, id from warehouse.raw.inputs",
    'select i."ROWID" as pos, i.id from warehouse.raw.inputs as i',
):
    model.write_text(query)
    with duckdb.connect(str(path)) as db:
        before = db.execute(query + " order by all").fetchall()
    run("build", before, query)
    reorder()
    with duckdb.connect(str(path)) as db:
        after = db.execute(query + " order by all").fetchall()
    assert before != after
    run("build", after, query)

# The outer model never mentions rowid; traversal must check the stored views.
with duckdb.connect(str(path)) as db:
    db.execute(
        "create view raw.positions as select rowid as pos, id from warehouse.raw.inputs; "
        "create view raw.wrapped as select pos, id from warehouse.raw.positions"
    )
model.write_text("select * from warehouse.raw.wrapped")
with duckdb.connect(str(path)) as db:
    before = db.execute("select * from raw.wrapped order by all").fetchall()
run("build", before, "select * from raw.wrapped")
reorder()
with duckdb.connect(str(path)) as db:
    after = db.execute("select * from raw.wrapped order by all").fetchall()
assert before != after
run("build", after, "select * from raw.wrapped")

# A hidden reference in the hot branch cannot authorize compile-time CTAS
# before is_incremental() changes from false to true in an absent dev target.
with duckdb.connect(str(path)) as db:
    db.execute("drop table dev.totals")
model.write_text(
    "{{ config(materialized='incremental', incremental_strategy='delete+insert', "
    "unique_key='id', on_schema_change='fail') }}\n"
    "select {% if is_incremental() %}rowid{% else %}cast(id as bigint){% endif %} "
    "as pos, id from warehouse.raw.inputs"
)
run("build", [(1, 1), (2, 2)], "select cast(id as bigint), id from raw.inputs", "prod")
with duckdb.connect(str(path)) as db:
    before = db.execute("select rowid, id from raw.inputs order by all").fetchall()
run("build", before, "select rowid, id from raw.inputs", "prod")
reorder()
run("build", [(1, 1), (2, 2)], "select cast(id as bigint), id from raw.inputs")
with duckdb.connect(str(path)) as db:
    after = db.execute("select rowid, id from raw.inputs order by all").fetchall()
run("build", after, "select rowid, id from raw.inputs")
