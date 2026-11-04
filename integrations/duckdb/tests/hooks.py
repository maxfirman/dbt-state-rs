"""Real post-hook identity and pre-hook binding ambiguity regressions."""

import json
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
        "create schema raw; create table raw.inputs(value integer); insert into raw.inputs values (10)"
    )


def run(decision, expected, query):
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("invoke.py")),
            "run",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
        ],
        env={**os.environ, "EXPECT_DECISION": decision, "EXPECT_NO_COPY": "1"},
        check=True,
    )
    with duckdb.connect(str(path)) as db:
        actual = db.execute("select * from dev.totals").fetchall()
        reference = db.execute(query).fetchall()
        assert actual == reference == [(expected,)], (actual, reference)


model = root / "models/totals.sql"
plain = "select value from warehouse.raw.inputs"
for increment in [1, 2]:
    model.write_text(
        "{{ config(post_hook='update {{ this }} set value=value+"
        + str(increment)
        + "') }}\n"
        + plain
    )
    run("build", 10 + increment, f"select value+{increment} from raw.inputs")
    run("skip", 10 + increment, f"select value+{increment} from raw.inputs")
# Removing a hook changes the execution contract too.
model.write_text(plain)
run("build", 10, "select value from raw.inputs")
run("skip", 10, "select value from raw.inputs")
# The temporary macro does not exist until after State inspects the worker.
# Every pre-hook invocation needs ordinary execution rather than reused data.
model.write_text(
    "{{ config(pre_hook='create temp macro \"+\"(a,b) as a-b') }}\nselect value+1 as value from warehouse.raw.inputs"
)
run("build", 9, "select value-1 from raw.inputs")
run("build", 9, "select value-1 from raw.inputs")

# The supported post-hook subset does not claim arbitrary template, function,
# external write or session-context semantics. Each still executes ordinarily.
with duckdb.connect(str(path)) as db:
    db.execute(
        "create table raw.audit(value integer); insert into raw.audit values (0)"
    )
for hook, expected, reference in [
    (
        "{{ 'update' }} {{ this }} set value=value+1",
        11,
        "select value+1 from raw.inputs",
    ),
    ("update {{ this }} set value=abs(value)", 10, "select value from raw.inputs"),
    ("update raw.audit set value=value+1", 10, "select value from raw.inputs"),
    ("create temp macro opaque_hook() as 1", 10, "select value from raw.inputs"),
]:
    model.write_text("{{ config(post_hook=" + json.dumps(hook) + ") }}\n" + plain)
    run("build", expected, reference)
    run("build", expected, reference)
