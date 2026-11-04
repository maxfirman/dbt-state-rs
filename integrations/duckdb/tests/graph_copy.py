"""Real dbt table copies over checked multi-level physical view graphs."""

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
    'name: graph_copy\nversion: "1.0"\nconfig-version: 2\nprofile: graph_copy\nmodels:\n  graph_copy:\n    +materialized: table\n'
)
path = root / "warehouse.duckdb"
(root / "profiles.yml").write_text(
    "graph_copy:\n  target: prod\n  outputs:\n"
    + "".join(
        f"    {env}:\n      type: duckdb\n      path: {path}\n      schema: {env}\n      threads: 2\n      run_cache_enabled: true\n"
        for env in ["prod", "dev"]
    )
)
with duckdb.connect(str(path)) as db:
    db.execute(
        "create schema raw; create schema other; create schema prod; create schema dev"
    )
    db.execute(
        'create table raw."in.put"(id integer, amount integer); insert into raw."in.put" values (1,10),(2,20),(2,20),(3,NULL)'
    )
    db.execute('create table other."in.put" as select * from raw."in.put"')
    for env in ["prod", "dev"]:
        db.execute(f'create view {env}."low.er" as select id, amount from raw."in.put"')
        db.execute(
            f'create view {env}."up.per" as select id, amount from {env}."low.er"'
        )


def run(env, alias, decision, expected=None, source=None):
    (root / "models/result.sql").write_text(
        "{{ config(alias='"
        + alias
        + "') }}\n"
        + f'select id, amount + 1 as amount from "warehouse"."{env}"."up.per"'
    )
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("copy_invoke.py")),
            "run",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
            "--target",
            env,
            "--no-defer",
        ],
        env={
            **os.environ,
            "EXPECT_DECISION": decision,
            "COPY_OBSERVATIONS": str(root / "observations.jsonl"),
            "WAREHOUSE_PATH": str(path),
        },
        check=True,
    )
    observation = json.loads((root / "observations.jsonl").read_text().splitlines()[-1])
    if source:
        assert observation["source"] == f'"warehouse"."{source[0]}"."{source[1]}"', (
            observation
        )
        assert observation["target"] == f'"warehouse"."{env}"."{alias}"', observation
    with duckdb.connect(str(path)) as db:
        rows = db.execute(f'select * from {env}."{alias}" order by id').fetchall()
        reference = db.execute(
            'select id, amount + 1 as amount from raw."in.put" order by id'
        ).fetchall()
        assert rows == (reference if expected is None else expected), rows
        assert [
            (c[0], c[1]) for c in db.execute(f'describe {env}."{alias}"').fetchall()
        ] == [("id", "INTEGER"), ("amount", "INTEGER")]


run("prod", "first", "build")
run("dev", "second", "copy", source=("prod", "first"))
run("dev", "second", "skip")
run("prod", "third", "copy", source=("dev", "second"))
run("prod", "third", "skip")
# The newest opposite-environment source has exhausted the two-hop limit.
run("dev", "fourth", "build")
# A changed view definition cannot inherit the earlier graph result.
with duckdb.connect(str(path)) as db:
    db.execute(
        'create or replace view dev."low.er" as select id, amount + 4 as amount from raw."in.put"'
    )
run("dev", "changed", "build", expected=[(1, 15), (2, 25), (2, 25), (3, None)])
# Identical rows in another physical leaf do not establish equivalence.
with duckdb.connect(str(path)) as db:
    db.execute(
        'create or replace view dev."low.er" as select id, amount from other."in.put"'
    )
run("dev", "unrelated", "build")
# Restore graph; source validity and permitted upstream staleness are separate.
with duckdb.connect(str(path)) as db:
    db.execute(
        'create or replace view dev."low.er" as select id, amount from raw."in.put"'
    )
    db.execute('update raw."in.put" set amount=amount+100')
# Most recent opposite source is depth2, so refresh an ordinary prod result
# explicitly and then advance the input again without changing that source.
os.environ["RUN_CACHE_ALLOW_CLONES"] = "false"
run("prod", "stale_source", "build")
del os.environ["RUN_CACHE_ALLOW_CLONES"]
with duckdb.connect(str(path)) as db:
    db.execute('update raw."in.put" set amount=amount+100')
run(
    "dev",
    "stale_copy",
    "copy",
    expected=[(1, 111), (2, 121), (2, 121), (3, None)],
    source=("prod", "stale_source"),
)
run("dev", "stale_copy", "skip", expected=[(1, 111), (2, 121), (2, 121), (3, None)])
# A search-path-dependent physical definition is deliberately unresolved by
# evidence traversal, although the ordinary DuckDB execution can evaluate it.
with duckdb.connect(str(path)) as db:
    db.execute('create table main."in.put" as select * from raw."in.put"')
    db.execute('create or replace view dev."low.er" as select id, amount from "in.put"')
run("dev", "ambiguous", "build")
run("dev", "ambiguous", "build")
