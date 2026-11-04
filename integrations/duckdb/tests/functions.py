"""Function binding changes cannot certify stale real-dbt results."""

import os
from pathlib import Path
import subprocess
import sys
import duckdb

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
(root / "models").mkdir(exist_ok=True)
(root / "dbt_project.yml").write_text(
    'name: duckdb_smoke\nversion: "1.0"\nconfig-version: 2\nprofile: smoke\n'
    "models:\n  duckdb_smoke:\n    +materialized: table\n"
)
path = root / "warehouse.duckdb"
(root / "profiles.yml").write_text(
    "smoke:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
    f"      path: {path}\n      schema: dev\n      threads: 2\n      run_cache_enabled: true\n"
)
with duckdb.connect(str(path)) as db:
    db.execute(
        "create schema raw; create table raw.inputs(value varchar); insert into raw.inputs values ('AbC')"
    )


def sql(statement):
    with duckdb.connect(str(path)) as db:
        db.execute(statement)


def run(decision, expected, target="dev", reference=None, extra=None):
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
        env={
            **os.environ,
            "EXPECT_DECISION": decision,
            "EXPECT_NO_COPY": "1",
            **(extra or {}),
        },
        check=True,
    )
    with duckdb.connect(str(path)) as db:
        reference = db.execute(
            reference or "select lower(value) as value from raw.inputs"
        ).fetchall()
        actual = db.execute(f"select * from {target}.totals").fetchall()
        assert actual == reference == [(expected,)], (actual, reference)
        assert [
            (r[0], r[1]) for r in db.execute(f"describe {target}.totals").fetchall()
        ] == [("value", "VARCHAR")]


(root / "models/totals.sql").write_text(
    "select lower(value) as value from warehouse.raw.inputs"
)
run("build", "abc")
run("skip", "abc")
# A user table macro cannot forge the catalog evidence used by State.
sql(
    "create macro duckdb_functions() as table select * from system.main.duckdb_functions() where internal"
)
run("build", "abc")
sql(
    "create macro lower(s) as case when s='AbC' then upper(s) else system.main.lower(s) end"
)
run("build", "ABC")
run("build", "ABC")
# A physical view has an unchanged definition, but its bound macro changed.
sql(
    "drop macro duckdb_functions; drop macro lower; create view raw.projected as select lower(value) as value from warehouse.raw.inputs"
)
(root / "models/totals.sql").write_text("select * from warehouse.raw.projected")
run("build", "abc")
run("skip", "abc")
sql(
    "create macro lower(s) as case when s='AbC' then upper(s) else system.main.lower(s) end"
)
run("build", "ABC")

# dbt-duckdb plugins configure each actual execution cursor. The separate
# evidence connection cannot see a cursor-local temporary macro.
sql("drop macro lower")
(root / "models/totals.sql").write_text(
    "select lower(value) as value from warehouse.raw.inputs"
)
run("build", "abc")
run("skip", "abc")
plugin = root / "session_functions.py"
plugin.write_text(
    "from dbt.adapters.duckdb.plugins import BasePlugin\n"
    "class Plugin(BasePlugin):\n"
    "    def configure_cursor(self, cursor):\n"
    "        cursor.execute(\"create temp macro lower(s) as case when s='AbC' then upper(s) else system.main.lower(s) end\")\n"
)
profile = (root / "profiles.yml").read_text()
(root / "profiles.yml").write_text(
    profile + "      plugins:\n        - module: session_functions\n"
)
run(
    "build",
    "ABC",
    reference="select upper(value) as value from raw.inputs",
    extra={
        "PYTHONPATH": str(root) + os.pathsep + os.environ["PYTHONPATH"],
    },
)
(root / "profiles.yml").write_text(profile)
run("build", "abc")
run("skip", "abc")

# Validate both incremental template branches before initialization, even the
# branch that will not execute in the absent development target.
(root / "profiles.yml").write_text(
    "smoke:\n  target: prod\n  outputs:\n"
    + "".join(
        f"    {env}:\n      type: duckdb\n      path: {path}\n      schema: {env}\n"
        "      threads: 2\n      run_cache_enabled: true\n"
        for env in ["prod", "dev"]
    )
)
config = "{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='value', on_schema_change='fail') }}\n"
for branch in ["cold", "hot"]:
    sql("drop table if exists prod.totals; drop table dev.totals")
    template = (
        "{% if is_incremental() %} select "
        + ("lower(value)" if branch == "hot" else "value")
        + " as value from warehouse.raw.inputs {% else %} select "
        + ("lower(value)" if branch == "cold" else "value")
        + " as value from warehouse.raw.inputs {% endif %}"
    )
    (root / "models/totals.sql").write_text(config + template)
    # Populate and confirm an eligible production source before shadowing.
    run(
        "build",
        "abc" if branch == "cold" else "AbC",
        target="prod",
        reference="select "
        + ("lower(value)" if branch == "cold" else "value")
        + " from raw.inputs",
    )
    sql(
        "create macro lower(s) as case when s='AbC' then upper(s) else system.main.lower(s) end"
    )
    run(
        "build",
        "ABC" if branch == "cold" else "AbC",
        target="dev",
        reference="select "
        + ("lower(value)" if branch == "cold" else "value")
        + " from raw.inputs",
    )
    sql("drop macro lower")

# DuckDB also resolves arithmetic operators through mutable function bindings.
# This SQL contains no named callable except the intrinsic CAST.
sql(
    "create table raw.numbers(value integer); insert into raw.numbers values (5); drop table dev.totals"
)
(root / "models/totals.sql").write_text(
    "select cast(value + 1 as varchar) as value from warehouse.raw.numbers"
)
run("build", "6", reference="select cast(value + 1 as varchar) from raw.numbers")
run("skip", "6", reference="select cast(value + 1 as varchar) from raw.numbers")
sql('create macro "+"(a,b) as a-b')
run("build", "4", reference="select cast(value + 1 as varchar) from raw.numbers")
