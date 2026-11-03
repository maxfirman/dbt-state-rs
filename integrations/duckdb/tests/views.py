"""Real view DAG decisions and independent rows/schema checks."""

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
(root / "models").mkdir(exist_ok=True)
(root / "dbt_project.yml").write_text(
    'name: view_cases\nversion: "1.0"\nconfig-version: 2\nprofile: views\nmodels:\n  view_cases:\n    +materialized: view\n'
)
(root / "profiles.yml").write_text(
    f"views:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: {root / 'warehouse.duckdb'}\n      schema: dev\n      threads: 4\n      run_cache_enabled: true\n"
)
(root / "models/direct.sql").write_text(
    'select id, amount from {{ source("raw", "numbers") }}'
)
(root / "models/nested.sql").write_text('select * from {{ ref("direct") }}')
(root / "models/result.sql").write_text(
    '{{ config(materialized="table") }} select * from {{ ref("nested") }}'
)
(root / "models/schema.yml").write_text(
    "version: 2\nsources:\n  - name: raw\n    schema: raw\n    tables:\n      - name: numbers\n"
)
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute(
        "create schema raw; create table raw.numbers(id integer, amount integer); insert into raw.numbers values (1,10),(2,20),(2,20)"
    )


def run(
    expected,
    columns=("id", "amount"),
    unknown=False,
    unconfirmed=(),
    reference_sql=None,
):
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("views_invoke.py")),
            "run",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
        ],
        env={
            **os.environ,
            "VIEW_EXPECT": json.dumps(expected),
            "VIEW_UNKNOWN": "1" if unknown else "0",
            "VIEW_UNCONFIRMED": json.dumps(unconfirmed),
        },
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    with duckdb.connect(str(root / "warehouse.duckdb")) as db:
        reference = db.execute(
            reference_sql
            or ("select " + ",".join(columns) + " from raw.numbers order by id,amount")
        ).fetchall()
        for name in ("direct", "nested", "result"):
            actual = db.execute(
                f"select * from dev.{name} order by id,amount"
            ).fetchall()
            assert actual == reference, (name, actual, reference)
            assert [
                r[0] for r in db.execute(f"describe dev.{name}").fetchall()
            ] == list(columns)


run({"direct": "build", "nested": "build", "result": "build"})
run({"direct": "skip", "nested": "skip", "result": "skip"})
# Own-target existence remains separate from upstream definition identity.
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute("drop view dev.nested")
run({"direct": "skip", "nested": "build", "result": "skip"})

# A source write propagates through the completed view DAG in this invocation.
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute("insert into raw.numbers values (3,NULL)")
run({"direct": "build", "nested": "build", "result": "build"})
run({"direct": "skip", "nested": "skip", "result": "skip"})

# Change a physical dependency definition without changing the submitted root.
# Selecting descendants prevents dbt from first overwriting that evidence.
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute(
        "create or replace view dev.direct as select id, amount + 1 as amount from raw.numbers"
    )
result = subprocess.run(
    [
        sys.executable,
        str(Path(__file__).with_name("views_invoke.py")),
        "run",
        "--select",
        "nested result",
        "--project-dir",
        str(root),
        "--profiles-dir",
        str(root),
    ],
    env={
        **os.environ,
        "VIEW_EXPECT": json.dumps({"nested": "build", "result": "build"}),
    },
    check=False,
    capture_output=True,
    text=True,
)
assert result.returncode == 0, result.stdout + result.stderr
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    assert db.execute("select * from dev.result order by id,amount").fetchall() == [
        (1, 11),
        (2, 21),
        (2, 21),
        (3, None),
    ]
# dbt restores its declared direct view. The descendants must not retain the
# changed definition from a previous traversal or compile-time cache.
run({"direct": "build", "nested": "build", "result": "build"})
run({"direct": "skip", "nested": "skip", "result": "skip"})

# Explicit projections keep their declared shape through unrelated schema edits.
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute("alter table raw.numbers add column extra integer default 7")
run({"direct": "build", "nested": "build", "result": "build"})
run({"direct": "skip", "nested": "skip", "result": "skip"})

# Wildcard views have bound schema in DuckDB. Rebuild the whole DAG after both
# addition and removal; inspect actual structure, not merely successful status.
(root / "models/direct.sql").write_text('select * from {{ source("raw", "numbers") }}')
run(
    {"direct": "build", "nested": "build", "result": "build"},
    columns=("id", "amount", "extra"),
)
run(
    {"direct": "skip", "nested": "skip", "result": "skip"},
    columns=("id", "amount", "extra"),
)
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute("alter table raw.numbers drop column extra")
run({"direct": "build", "nested": "build", "result": "build"})
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute("alter table raw.numbers add column added varchar default 'new'")
run(
    {"direct": "build", "nested": "build", "result": "build"},
    columns=("id", "amount", "added"),
)
run(
    {"direct": "skip", "nested": "skip", "result": "skip"},
    columns=("id", "amount", "added"),
)

# Search-path dependent physical definitions are unknown, even when DuckDB can
# currently execute them. Compiled SQL cannot substitute for warehouse evidence.
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute("set schema='raw'")
    db.execute("create view wrapper as select id, amount from numbers")
    db.execute(
        "create schema other; create table other.numbers(id integer, amount integer)"
    )
(root / "models/schema.yml").write_text(
    (root / "models/schema.yml").read_text() + "      - name: wrapper\n"
)
(root / "models/direct.sql").write_text(
    'select id, amount from {{ source("raw", "wrapper") }}'
)
run({"direct": "build", "nested": "build", "result": "build"}, unknown=True)
run({"direct": "build", "nested": "build", "result": "build"}, unknown=True)

# An unsupported deterministic function also cannot become a cache hit merely
# because it has stable output. The ordinary dbt execution still works.
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute(
        "create or replace view raw.wrapper as select id, abs(amount) as amount from raw.numbers"
    )
run({"direct": "build", "nested": "build", "result": "build"}, unknown=True)
run({"direct": "build", "nested": "build", "result": "build"}, unknown=True)

# Physical reorder alone is deliberately not a content change. LIMIT without
# a deterministic ordering is outside reuse eligibility, so a downstream table
# cannot retain the row selected before this rewrite.
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute(
        "create or replace view raw.wrapper as select id, amount from raw.numbers limit 1"
    )
run(
    {"direct": "build", "nested": "build", "result": "build"},
    reference_sql="select id,amount from raw.numbers limit 1",
)
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    db.execute(
        "create temporary table reordered as select * from raw.numbers order by id desc; delete from raw.numbers; insert into raw.numbers select * from reordered"
    )
run(
    {"direct": "build", "nested": "build", "result": "build"},
    reference_sql="select id,amount from raw.numbers limit 1",
)
with duckdb.connect(str(root / "warehouse.duckdb")) as db:
    assert db.execute("select * from dev.result").fetchall() == [(3, None)]
