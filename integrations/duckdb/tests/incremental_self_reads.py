"""A repeat incremental execution must not be mistaken for its previous output."""

import os
import subprocess
import sys
from pathlib import Path

import duckdb


root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
(root / "models").mkdir()
(root / "dbt_project.yml").write_text(
    'name: incremental_self\nversion: "1.0"\nconfig-version: 2\nprofile: incremental_self\n'
)
path = root / "warehouse.duckdb"
(root / "profiles.yml").write_text(
    "incremental_self:\n  target: prod\n  outputs:\n"
    + "".join(
        f"    {env}:\n      type: duckdb\n      path: {path}\n"
        f"      schema: {env}\n      threads: 2\n      run_cache_enabled: true\n"
        for env in ("prod", "dev")
    )
)
with duckdb.connect(str(path)) as db:
    db.execute(
        "create schema raw; create table raw.input(id integer, amount integer); "
        "insert into raw.input values (1,10)"
    )

config = (
    "{{ config(materialized='incremental', incremental_strategy='delete+insert', "
    "unique_key='id', on_schema_change='fail') }}\n"
)
model = root / "models/result.sql"


def run(env, decision, rows, registrations=0):
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("incremental_invoke.py")),
            "run",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
            "--target",
            env,
        ],
        env={
            **os.environ,
            "EXPECT_DECISION": decision,
            "EXPECT_REGISTER": str(registrations),
            "WAREHOUSE_PATH": str(path),
        },
        check=True,
    )
    with duckdb.connect(str(path)) as db:
        actual = db.execute(f"select * from {env}.result order by all").fetchall()
        assert actual == rows, (actual, rows)


model.write_text(
    config
    + "{% if is_incremental() %} "
    "select id, amount+1 as amount from {{ this }} "
    "{% else %} select id, amount from warehouse.raw.input {% endif %}"
)
run("prod", "build", [(1, 10)])
run("prod", "build", [(1, 11)])
run("prod", "build", [(1, 12)])
# An absent dev relation must execute its cold branch, not clone a self-updated
# source and then increment the copied rows again during compilation/execution.
run("dev", "build", [(1, 10)], registrations=1)
run("dev", "build", [(1, 11)])

# A strict MAX watermark that is projected unchanged reaches a fixed point.
model.write_text(
    config
    + "select id, amount from warehouse.raw.input "
    "{% if is_incremental() %} "
    "where amount > (select coalesce(max(amount),0) from {{ this }}) "
    "{% endif %}"
)
run("prod", "build", [(1, 12)])
run("prod", "skip", [(1, 12)])

# A watermark is insufficient when its apparent input is a view of the target.
with duckdb.connect(str(path)) as db:
    db.execute("create view raw.previous as select id, amount+1 as amount from prod.result")
model.write_text(
    config
    + "select id, amount from warehouse.raw.previous "
    "where amount > (select coalesce(max(amount),0) from {{ this }})"
)
run("prod", "build", [(1, 13)])
run("prod", "build", [(1, 14)])

# Aliases must not hide the physical self dependency.
model.write_text(config + "select id, amount+1 as amount from {{ this }} as previous")
run("prod", "build", [(1, 15)])
run("prod", "build", [(1, 16)])

# Lowering the projected watermark can expose another input on the next run.
# Ordinary delete+insert first replaces id1=100 with id1=0, then adds id2=-51.
with duckdb.connect(str(path)) as db:
    db.execute("drop view raw.previous")
    db.execute("delete from raw.input; insert into raw.input values (1,101),(2,50)")
    db.execute("delete from prod.result; insert into prod.result values (1,100)")
model.write_text(
    config
    + "select id, amount-101 as amount from warehouse.raw.input "
    "where amount > (select coalesce(max(amount),0) from {{ this }})"
)
run("prod", "build", [(1, 0)])
run("prod", "build", [(1, 0), (2, -51)])
