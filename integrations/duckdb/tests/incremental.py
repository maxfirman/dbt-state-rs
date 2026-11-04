"""Pinned delete+insert behavior; literal expectations preserve retained rows."""

import os
from pathlib import Path
import subprocess
import sys
import duckdb

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
(root / "models").mkdir(exist_ok=True)
(root / "dbt_project.yml").write_text(
    'name: incremental_test\nversion: "1.0"\nconfig-version: 2\nprofile: incremental_test\n'
)
path = root / "warehouse.duckdb"
(root / "profiles.yml").write_text(
    "incremental_test:\n  target: prod\n  outputs:\n"
    + "".join(
        f"    {env}:\n      type: duckdb\n      path: {path}\n      schema: {env}\n      threads: 2\n      run_cache_enabled: true\n"
        for env in ["prod", "dev"]
    )
)
plain = "{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='id', on_schema_change='fail') }}\nselect id, amount from warehouse.raw.input"
(root / "models/result.sql").write_text(plain)
with duckdb.connect(str(path)) as db:
    db.execute(
        "create schema raw; create schema prod; create schema dev; create table raw.input(id integer, amount integer); insert into raw.input values (1,10),(2,20)"
    )


def run(env, decision, rows, register=0, extra=None):
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
            "EXPECT_REGISTER": str(register),
            "WAREHOUSE_PATH": str(path),
            **(extra or {}),
        },
        check=True,
    )
    with duckdb.connect(str(path)) as db:
        assert db.execute(
            f"select * from {env}.result order by id, amount"
        ).fetchall() == sorted(rows)
        assert [
            (r[0], r[1]) for r in db.execute(f"describe {env}.result").fetchall()
        ] == [("id", "INTEGER"), ("amount", "INTEGER")]


def sql(statement):
    with duckdb.connect(str(path)) as db:
        db.execute(statement)


run("prod", "build", [(1, 10), (2, 20)])
run("prod", "skip", [(1, 10), (2, 20)])
sql("update raw.input set amount=11 where id=1; insert into raw.input values (3,30)")
run("prod", "build", [(1, 11), (2, 20), (3, 30)])
run("prod", "skip", [(1, 11), (2, 20), (3, 30)])
# delete+insert retains target rows omitted by the input; full refresh would differ.
sql("delete from raw.input where id=2")
run("prod", "build", [(1, 11), (2, 20), (3, 30)])
run("dev", "skip", [(1, 11), (2, 20), (3, 30)], register=1)
run("dev", "skip", [(1, 11), (2, 20), (3, 30)])
# A changed node cannot populate the absent dev target with obsolete prod rows.
sql("drop table dev.result")
(root / "models/result.sql").write_text(
    plain.replace("select id, amount", "select id, amount+100 as amount")
)
run("dev", "build", [(1, 111), (3, 130)], register=1)
(root / "models/result.sql").write_text(
    plain.replace("unique_key='id'", "unique_key='amount'")
)
sql("drop table dev.result")
run("dev", "build", [(1, 11), (3, 30)], register=1)
(root / "models/result.sql").write_text(plain)
sql("drop table dev.result")
run(
    "dev",
    "build",
    [(1, 11), (2, 20), (3, 30)],
    register=1,
    extra={"INCREMENTAL_COPY_FAILURE": "after_commit"},
)
# A committed CTAS followed by uncertainty is not a confirmed registration.
# Recovery forces ordinary execution before a new stable exact SKIP.
run("dev", "build", [(1, 11), (2, 20), (3, 30)])
run("dev", "skip", [(1, 11), (2, 20), (3, 30)])
sql("drop table prod.result")
run("prod", "build", [(1, 11), (3, 30)])
sql("drop table dev.result")
run(
    "dev",
    "build",
    [(1, 11), (3, 30)],
    register=1,
    extra={"RUN_CACHE_ALLOW_CLONES": "false"},
)
run("dev", "skip", [(1, 11), (3, 30)])
sql("drop table dev.result")
run(
    "dev",
    "build",
    [(1, 11), (3, 30)],
    register=1,
    extra={"INCREMENTAL_COPY_FAILURE": "plan"},
)
# An uncertain precompile outcome invalidates the generation on restart.
run("dev", "build", [(1, 11), (3, 30)])
run("dev", "skip", [(1, 11), (3, 30)])
# Re-record the production source in this generation, then reject an external edit.
sql("drop table prod.result")
run("prod", "build", [(1, 11), (3, 30)])
sql("update prod.result set amount=999; drop table dev.result")
# SQL-submit proposal also validates the edited physical source and builds normally.
run("dev", "copy", [(1, 11), (3, 30)], register=1)
sql("drop table prod.result; drop table dev.result")
# The SQL service may still propose the recorded source; the client rejects
# its missing physical evidence and completes ordinary execution instead.
run("dev", "copy", [(1, 11), (3, 30)])
# A conditional template changes rendered identity after first creation. Record
# the stable branch once, then require genuine reuse and an older-key update.
conditional = (
    plain
    + "\n{% if is_incremental() %} where amount > (select coalesce(max(amount),0) from {{ this }}) {% endif %}"
)
(root / "models/result.sql").write_text(conditional)
run("prod", "build", [(1, 11), (3, 30)])
run("prod", "build", [(1, 11), (3, 30)])
run("prod", "skip", [(1, 11), (3, 30)])
sql("update raw.input set amount=40 where id=1")
run("prod", "build", [(1, 40), (3, 30)])
run("prod", "skip", [(1, 40), (3, 30)])
sql("drop table dev.result")
run("dev", "build", [(1, 40), (3, 30)], register=1)
run("dev", "skip", [(1, 40), (3, 30)])
sql("drop table dev.result")
(root / "models/result.sql").write_text(
    conditional.replace("select id, amount", "select id, amount+100 as amount")
)
# If precompile CTAS happened despite the mismatch, the incremental predicate
# would retain id3=30 instead of creating the correct absent-target full result.
run("dev", "build", [(1, 140), (3, 130)], register=1)
# Unsupported strategies must neither compile-clone nor submit/cache outcomes.
(root / "models/result.sql").write_text(plain.replace("'delete+insert'", "'append'"))
run("dev", "ordinary", [(1, 140), (1, 40), (3, 130), (3, 30)])

# The implicit adapter default and schema/predicate modes outside the pinned
# contract still execute normally, without State traffic or cached outcomes.
for config in [
    plain.replace(", incremental_strategy='delete+insert'", ""),
    plain.replace("on_schema_change='fail'", "on_schema_change='ignore'"),
    plain.replace(
        "on_schema_change='fail'", "on_schema_change='fail', predicates=['id > 0']"
    ),
]:
    sql("drop table dev.result")
    (root / "models/result.sql").write_text(config)
    run("dev", "ordinary", [(1, 40), (3, 30)])
    run("dev", "ordinary", [(1, 40), (3, 30)])
# A custom materialization remains ordinary execution, even when SQL is simple.
(root / "macros").mkdir(exist_ok=True)
(
    root / "macros/custom.sql"
).write_text("""{% materialization custom_table, adapter='duckdb' %}
  {% call statement('main') %} create or replace table {{ this }} as {{ sql }} {% endcall %}
  {% do adapter.commit() %}
  {{ return({'relations': [this]}) }}
{% endmaterialization %}""")
(root / "models/result.sql").write_text(
    "{{ config(materialized='custom_table') }} select id, amount from warehouse.raw.input"
)
run("dev", "ordinary", [(1, 40), (3, 30)])
sql("update raw.input set amount=41 where id=1")
run("dev", "ordinary", [(1, 41), (3, 30)])
# DuckDB's Python materialization accepts a native relation without extra packages.
(root / "models/result.sql").unlink()
(root / "models/result.py").write_text(
    "def model(dbt, session):\n    dbt.config(materialized='table')\n    return session.sql('select id, amount from warehouse.raw.input')\n"
)
run("dev", "ordinary", [(1, 41), (3, 30)])
sql("update raw.input set amount=42 where id=1")
run("dev", "ordinary", [(1, 42), (3, 30)])

(root / "snapshots").mkdir(exist_ok=True)
(root / "snapshots/history.sql").write_text("""{% snapshot history %}
{{ config(target_schema='dev', unique_key='id', strategy='check', check_cols=['amount']) }}
select id, amount from warehouse.raw.input
{% endsnapshot %}""")
for amount in (43, 44):
    sql(f"update raw.input set amount={amount} where id=1")
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("incremental_invoke.py")),
            "snapshot",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
            "--target",
            "dev",
        ],
        env={**os.environ, "EXPECT_DECISION": "ordinary"},
        check=True,
    )
    with duckdb.connect(str(path)) as db:
        assert db.execute(
            "select id, amount from dev.history where dbt_valid_to is null order by id"
        ).fetchall() == [(1, amount), (3, 30)]

# Fixed-schema fail mode rejects changed columns without publishing success.
(root / "models/result.py").unlink()
(root / "models/result.sql").write_text(plain)
run("dev", "build", [(1, 44), (3, 30)])
(root / "models/result.sql").write_text(
    plain.replace("select id, amount", "select id, amount, amount as extra")
)
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
        "dev",
    ],
    env={**os.environ, "EXPECT_FAILURE": "1"},
    check=True,
)
with duckdb.connect(str(path)) as db:
    assert db.execute("select * from dev.result order by id").fetchall() == [
        (1, 44),
        (3, 30),
    ]
    assert len(db.execute("describe dev.result").fetchall()) == 2
