"""Unfingerprinted type and session semantics cannot authorize reuse."""

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


def sql(statement):
    with duckdb.connect(str(path)) as db:
        db.execute(statement)


def run(decision, expected, reference, target="dev", nocase=False, timezone=None):
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("invoke.py")),
            "run", "--project-dir", str(root),
            "--profiles-dir", str(root), "--target", target,
        ],
        env={
            **os.environ,
            "EXPECT_DECISION": decision,
            "EXPECT_NO_COPY": "1",
            "PYTHONPATH": os.pathsep.join((
                str(root),
                str(Path(__file__).resolve().parents[1] / "src"),
                os.environ.get("PYTHONPATH", ""),
            )),
        },
        check=True,
    )
    with duckdb.connect(str(path)) as db:
        actual = db.execute(f"select * from {target}.totals order by all").fetchall()
        db.execute("SET scalar_subquery_error_on_multiple_rows=false")
        if nocase:
            db.execute("SET default_collation='nocase'")
        if timezone is not None:
            db.execute("SET TimeZone=?", [timezone])
        current = db.execute(reference + " order by all").fetchall()
        assert actual == current == expected, (actual, current, expected)


model = root / "models/totals.sql"
config = (
    "{{ config(materialized='incremental', incremental_strategy='delete+insert', "
    "unique_key='id', on_schema_change='fail') }}\n"
)
if len(sys.argv) == 2 or sys.argv[2] == "types":
    query = "select cast('low' as priority) < cast('high' as priority) as result"
    sql("create type priority as enum ('low','high')")
    model.write_text(query)
    run("build", [(True,)], query)
    sql("drop type priority; create type priority as enum ('high','low')")
    run("build", [(False,)], query)

    # Built-in casts still permit reuse; a word in a string is not a type read.
    query = "select cast('42' as integer) as n, 'priority' as label"
    model.write_text(query)
    run("build", [(42, "priority")], query)
    run("skip", [(42, "priority")], query)

    # Qualified :: and TRY_CAST must resolve to the same conservative gate.
    for query in (
        "select 'low'::main.priority < 'high'::main.priority as result",
        "select try_cast('low' as priority) < try_cast('high' as priority) as result",
    ):
        model.write_text(query)
        run("build", [(False,)], query)
        run("build", [(False,)], query)

    # The submitted root has no named type; its stored input view does.
    sql("create view raw.ranked as select 'low'::priority < 'high'::priority as result")
    query = "select * from warehouse.raw.ranked"
    model.write_text(query)
    run("build", [(False,)], query)
    sql("drop type priority; create type priority as enum ('low','high')")
    run("build", [(True,)], query)

    # Reject an unsafe hot branch before compile-time development CTAS.
    sql("drop type priority; create type priority as enum ('high','low')")
    model.write_text(
        config + "select id, {% if is_incremental() %} "
        "cast('low' as priority) < cast('high' as priority) "
        "{% else %} true {% endif %} as result from warehouse.raw.inputs"
    )
    cold = "select id, true as result from raw.inputs"
    hot = "select id, false as result from raw.inputs"
    run("build", [(1, True), (2, True)], cold, "prod")
    run("build", [(1, False), (2, False)], hot, "prod")
    sql("drop table dev.totals")
    run("build", [(1, True), (2, True)], cold)
    run("build", [(1, False), (2, False)], hot)
elif sys.argv[2] == "scalar":
    profile = root / "profiles.yml"
    profile.write_text(profile.read_text().replace(
        "      run_cache_enabled: true\n",
        "      run_cache_enabled: true\n      settings:\n"
        "        scalar_subquery_error_on_multiple_rows: false\n",
    ))
    query = "select (select id from warehouse.raw.inputs) as picked"
    model.write_text(query)
    # The independent execution cursor must use the same declared semantics.
    reference = query
    run("build", [(1,)], reference)
    sql("create or replace table raw.inputs as select id from raw.inputs order by id desc")
    run("build", [(2,)], reference)

    # The permissive context must not initialize an absent incremental target,
    # even when the unsafe scalar expression appears only in its hot branch.
    model.write_text(
        config + "select id, {% if is_incremental() %} "
        "(select id from warehouse.raw.inputs) {% else %} -1 {% endif %} "
        "as picked from warehouse.raw.inputs"
    )
    cold = "select id, -1 as picked from raw.inputs"
    hot = "select id, (select id from raw.inputs) as picked from raw.inputs"
    run("build", [(1, -1), (2, -1)], cold, "prod")
    run("build", [(1, 2), (2, 2)], hot, "prod")
    sql("drop table dev.totals")
    run("build", [(1, -1), (2, -1)], cold)
    run("build", [(1, 2), (2, 2)], hot)

    # Explicit strict mode continues to reuse a single-row scalar query.
    sql("drop table dev.totals")
    profile.write_text(profile.read_text().replace(
        "scalar_subquery_error_on_multiple_rows: false",
        "scalar_subquery_error_on_multiple_rows: true",
    ))
    query = "select (select id from warehouse.raw.inputs where id=1) as picked"
    model.write_text(query)
    run("build", [(1,)], query)
    run("skip", [(1,)], query)

    # A cursor plugin can override the strict profile setting. Inspect the
    # session that executes the model, not the separate evidence connection.
    (root / "session_settings.py").write_text(
        "from dbt.adapters.duckdb.plugins import BasePlugin\n"
        "class Plugin(BasePlugin):\n"
        "    def configure_cursor(self, cursor):\n"
        "        cursor.execute('SET scalar_subquery_error_on_multiple_rows=false')\n"
    )
    profile.write_text(profile.read_text().replace(
        "      run_cache_enabled: true\n",
        "      run_cache_enabled: true\n      plugins:\n"
        "        - module: session_settings\n",
    ))
    query = "select (select id from warehouse.raw.inputs) as picked"
    model.write_text(query)
    run("build", [(2,)], query)
    sql("create or replace table raw.inputs as select id from raw.inputs order by id asc")
    run("build", [(1,)], query)

elif sys.argv[2] == "collation":
    query = "select 'a' = 'A' as same"
    model.write_text(query)
    run("build", [(False,)], query)
    run("skip", [(False,)], query)
    (root / "session_collation.py").write_text(
        "from dbt.adapters.duckdb.plugins import BasePlugin\n"
        "class Plugin(BasePlugin):\n"
        "    def configure_cursor(self, cursor):\n"
        "        cursor.execute(\"SET default_collation='nocase'\")\n"
    )
    profile = root / "profiles.yml"
    binary_profile = profile.read_text()
    profile.write_text(binary_profile.replace(
        "      run_cache_enabled: true\n",
        "      run_cache_enabled: true\n      plugins:\n"
        "        - module: session_collation\n",
    ))
    run("build", [(True,)], query, nocase=True)
    run("build", [(True,)], query, nocase=True)

    # Removing the plugin must restore binary results and ordinary reuse.
    collated_profile = profile.read_text()
    profile.write_text(binary_profile)
    run("build", [(False,)], query)
    run("skip", [(False,)], query)

    # An unsupported worker collation must be rejected before compile-time
    # initialization, which could otherwise change the branch of an absent dev.
    profile.write_text(collated_profile)
    model.write_text(
        config + "select id, {% if is_incremental() %} 'a' = 'A' "
        "{% else %} false {% endif %} as same from warehouse.raw.inputs"
    )
    cold = "select id, false as same from raw.inputs"
    hot = "select id, true as same from raw.inputs"
    run("build", [(1, False), (2, False)], cold, "prod")
    run("build", [(1, True), (2, True)], hot, "prod")
    sql("drop table dev.totals")
    run("build", [(1, False), (2, False)], cold)
    run("build", [(1, True), (2, True)], hot)

else:
    zone_file = root / "timezone.txt"
    zone_file.write_text("UTC")
    (root / "session_timezone.py").write_text(
        "from pathlib import Path\n"
        "from dbt.adapters.duckdb.plugins import BasePlugin\n"
        "class Plugin(BasePlugin):\n"
        "    def configure_cursor(self, cursor):\n"
        "        zone = Path(__file__).with_name('timezone.txt').read_text()\n"
        "        cursor.execute('SET TimeZone=?', [zone])\n"
    )
    profile = root / "profiles.yml"
    profile.write_text(profile.read_text().replace(
        "      run_cache_enabled: true\n",
        "      run_cache_enabled: true\n      plugins:\n"
        "        - module: session_timezone\n",
    ))
    query = "select cast(cast('2020-01-01 00:00:00+00' as timestamptz) as varchar) as value"
    model.write_text(query)
    run("build", [("2020-01-01 00:00:00+00",)], query, timezone="UTC")
    zone_file.write_text("America/Los_Angeles")
    run("build", [("2019-12-31 16:00:00-08",)], query, timezone="America/Los_Angeles")

    # Naive timestamps are independent of TimeZone and retain normal reuse.
    query = "select cast(cast('2020-01-01 00:00:00' as timestamp) as varchar) as value"
    model.write_text(query)
    run("build", [("2020-01-01 00:00:00",)], query, timezone="America/Los_Angeles")
    zone_file.write_text("UTC")
    run("skip", [("2020-01-01 00:00:00",)], query, timezone="UTC")

    # TIME WITH TIME ZONE is outside the same conservative cast contract.
    query = "select cast(cast('12:00:00+00' as time with time zone) as varchar) as value"
    model.write_text(query)
    run("build", [("12:00:00+00",)], query, timezone="UTC")
    run("build", [("12:00:00+00",)], query, timezone="UTC")

    # The root never mentions the type. Validate TRY_CAST and the long alias
    # within the stored view definition before certifying its VARCHAR result.
    sql(
        "create view raw.zoned as select cast(try_cast("
        "'2020-01-01 00:00:00+00' as timestamp with time zone) as varchar) as value"
    )
    query = "select * from warehouse.raw.zoned"
    model.write_text(query)
    run("build", [("2020-01-01 00:00:00+00",)], query, timezone="UTC")
    zone_file.write_text("America/Los_Angeles")
    run("build", [("2019-12-31 16:00:00-08",)], query, timezone="America/Los_Angeles")

    # Reject an unsafe hot branch before a dev copy can change which branch
    # compiles for an absent target. Only ordinary cold execution yields 'cold'.
    model.write_text(
        config + "select id, {% if is_incremental() %} "
        "cast(cast('2020-01-01 00:00:00+00' as timestamptz) as varchar) "
        "{% else %} 'cold' {% endif %} as value from warehouse.raw.inputs"
    )
    cold = "select id, 'cold' as value from raw.inputs"
    hot = "select id, '2019-12-31 16:00:00-08' as value from raw.inputs"
    run("build", [(1, "cold"), (2, "cold")], cold, "prod")
    run("build", [(1, "2019-12-31 16:00:00-08"), (2, "2019-12-31 16:00:00-08")], hot, "prod")
    sql("drop table dev.totals")
    run("build", [(1, "cold"), (2, "cold")], cold)
    run("build", [(1, "2019-12-31 16:00:00-08"), (2, "2019-12-31 16:00:00-08")], hot)
