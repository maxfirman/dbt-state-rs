"""Real prod/dev CTAS and independent contents/schema checks."""

import os
import subprocess
import sys
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=True)
(root / "models").mkdir(exist_ok=True)
(root / "dbt_project.yml").write_text(
    'name: copy_test\nversion: "1.0"\nconfig-version: 2\nprofile: copy_test\nmodels:\n  copy_test:\n    +materialized: table\n'
)
path = root / "warehouse.duckdb"
(root / "profiles.yml").write_text(
    "copy_test:\n  target: prod\n  outputs:\n"
    + "".join(
        f"    {env}:\n      type: duckdb\n      path: {path}\n      schema: {env}\n      threads: 2\n      run_cache_enabled: true\n"
        for env in ["prod", "dev"]
    )
)
(root / "models/result.sql").write_text(
    'select id, amount + 1 as amount from "warehouse"."raw"."input"'
)
with duckdb.connect(str(path)) as db:
    db.execute(
        "create schema raw; create schema prod; create schema dev; create table raw.input(id integer, amount integer); insert into raw.input values (1,10),(2,20),(2,20),(3,NULL)"
    )


def run(target, decision, failure=None, extra=None, increment=1, expected=None):
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
            target,
        ],
        env={
            **os.environ,
            "EXPECT_DECISION": decision,
            "COPY_OBSERVATIONS": str(root / "observations.jsonl"),
            "WAREHOUSE_PATH": str(path),
            **({"COPY_FAILURE": failure} if failure else {}),
            **(extra or {}),
        },
        check=True,
    )
    with duckdb.connect(str(path)) as db:
        actual = db.execute(f"select * from {target}.result order by id").fetchall()
        reference = db.execute(
            f"select id, amount + {increment} as amount from raw.input order by id"
        ).fetchall()
        expected = reference if expected is None else expected
        schema = [
            (r[0], r[1]) for r in db.execute(f"describe {target}.result").fetchall()
        ]
        assert actual == expected, (target, actual, expected)
        assert schema == [("id", "INTEGER"), ("amount", "INTEGER")], schema


run("prod", "build")
run("dev", "copy")
run("dev", "skip")
if os.environ.get("COPY_SCENARIO") == "smoke":
    sys.exit(0)

# A genuine logic change must not inherit the earlier copy's identity.
(root / "models/result.sql").write_text(
    'select id, amount + 2 as amount from "warehouse"."raw"."input"'
)
run("dev", "build", increment=2)
(root / "models/result.sql").write_text(
    'select id, amount + 1 as amount from "warehouse"."raw"."input"'
)
run("dev", "copy")


def drop_dev():
    with duckdb.connect(str(path)) as db:
        db.execute("drop table if exists dev.result")


drop_dev()
run("dev", "build", extra={"RUN_CACHE_ALLOW_CLONES": "false"})
run("dev", "skip")
# Required properties unavailable on the wire must suppress CTAS. Normal dbt
# hooks create the default; checking the warehouse proves it was preserved.
(root / "models/result.sql").write_text(
    '{{ config(post_hook=\'alter table {{ this }} alter amount set default 100\') }}\nselect id, amount + 1 as amount from "warehouse"."raw"."input"'
)
drop_dev()
run("dev", "build")
with duckdb.connect(str(path)) as db:
    assert db.execute("describe dev.result").fetchall()[1][4] == "100"
(root / "models/result.sql").write_text(
    'select id, amount + 1 as amount from "warehouse"."raw"."input"'
)
# Required column constraints also bypass CTAS and remain present in the actual
# table produced by dbt's contract materialization.
(root / "models/schema.yml").write_text(
    "version: 2\nmodels:\n  - name: result\n    config:\n      contract:\n        enforced: true\n"
    "    columns:\n      - name: id\n        data_type: integer\n        constraints:\n          - type: not_null\n"
    "      - name: amount\n        data_type: integer\n"
)
drop_dev()
run("dev", "build")
with duckdb.connect(str(path)) as db:
    assert db.execute("describe dev.result").fetchall()[0][2] == "NO"
(root / "models/schema.yml").unlink()
# Faults occur after server selection. Each ordinary fallback must be confirmed
# as failed_to_clone and remain eligible for a later exact SKIP.
for failure in ["sql", "edit", "property", "drop", "race", "default", "constraint"]:
    drop_dev()
    run("dev", "copy", failure=failure)
    run("dev", "skip")
    # Restore the source through an actual ordinary materialization so each
    # following fault starts with a recorded, physically valid source.
    with duckdb.connect(str(path)) as db:
        db.execute("drop table if exists prod.result")
    run("prod", "build", extra={"RUN_CACHE_ALLOW_CLONES": "false"})
# Same data in a different physical input is not the same computation.
with duckdb.connect(str(path)) as db:
    db.execute(
        "create schema other; create table other.input as select * from raw.input"
    )
(root / "models/result.sql").write_text(
    'select id, amount + 1 as amount from "warehouse"."other"."input"'
)
drop_dev()
run("dev", "build")
(root / "models/result.sql").write_text(
    'select id, amount + 1 as amount from "warehouse"."raw"."input"'
)
# A source whose inputs advanced is intentionally allowed by shared clone
# fallback policy; distinguish this from a source externally edited itself.
with duckdb.connect(str(path)) as db:
    db.execute("update raw.input set amount=amount+100")
drop_dev()
run("dev", "copy", expected=[(1, 11), (2, 21), (2, 21), (3, None)])
run("dev", "skip", expected=[(1, 11), (2, 21), (2, 21), (3, None)])
