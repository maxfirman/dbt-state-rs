"""Seed reuse across real dbt processes, with literal row and schema oracles."""

import json
import os
import subprocess
import shutil
import sys
from pathlib import Path
import duckdb

root = Path(sys.argv[1])
root.mkdir(parents=True)
for folder in ("models", "seeds"):
    (root / folder).mkdir()
(root / "dbt_project.yml").write_text(
    'name: seed_cases\nversion: "1.0"\nconfig-version: 2\nprofile: seeds\nmodels:\n  seed_cases:\n    +materialized: table\n'
)
(root / "profiles.yml").write_text(
    f"seeds:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: {root / 'warehouse.duckdb'}\n      schema: dev\n      threads: 4\n      run_cache_enabled: true\n"
)
(root / "seeds/numbers.csv").write_text("id,amount\n1,10\n2,20\n2,20\n3,\n")
(root / "models/totals.sql").write_text(
    "select id, sum(amount)::bigint as amount from {{ ref('numbers') }} group by id"
)


def run(
    expected,
    rows,
    totals,
    *,
    args=(),
    fail=False,
    unconfirmed=(),
    types=("INTEGER", "INTEGER"),
    returned_error=False,
    database=None,
):
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("seeds_invoke.py")),
            "build",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
            *args,
        ],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "SEED_RETURN_ERROR": str(int(returned_error)),
            "SEED_EXPECT": json.dumps(expected),
            "SEED_FAIL": str(int(fail)),
            "SEED_UNCONFIRMED": json.dumps(unconfirmed),
        },
    )
    assert result.returncode == 0, result.stdout + result.stderr
    if fail:
        return
    with duckdb.connect(str(database or root / "warehouse.duckdb")) as db:
        assert (
            db.execute("select * from dev.numbers order by id,amount").fetchall()
            == rows
        )
        assert db.execute("select * from dev.totals order by id").fetchall() == totals
        assert [
            (r[0], r[1]) for r in db.execute("describe dev.numbers").fetchall()
        ] == list(zip(("id", "amount"), types))


rows = [(1, 10), (2, 20), (2, 20), (3, None)]
totals = [(1, 10), (2, 40), (3, None)]
run({"numbers": "build", "totals": "build"}, rows, totals)
run({"numbers": "skip", "totals": "skip"}, rows, totals)

if os.environ.get("SEED_SCENARIO") == "returned-error":
    (root / "seeds/numbers.csv").write_text("id,amount\n1,11\n")
    run(
        {"numbers": "build"},
        [],
        [],
        args=("--select", "numbers"),
        fail=True,
        unconfirmed=("numbers",),
        returned_error=True,
    )
    sys.exit(0)

# CSV bytes and semantic configuration travel through SubmitValues, never the
# model SQL route. Four workers must see completion before dependent submission.
(root / "seeds/numbers.csv").write_text("id,amount\n1,11\n2,30\n2,30\n3,\n")
rows, totals = [(1, 11), (2, 30), (2, 30), (3, None)], [(1, 11), (2, 60), (3, None)]
run({"numbers": "build", "totals": "build"}, rows, totals)
run({"numbers": "skip", "totals": "skip"}, rows, totals)
(root / "seeds/schema.yml").write_text(
    "version: 2\nseeds:\n  - name: numbers\n    config:\n      column_types:\n        amount: bigint\n      quote_columns: true\n"
)
run(
    {"numbers": "build", "totals": "build"},
    rows,
    totals,
    args=("--full-refresh",),
    types=("INTEGER", "BIGINT"),
)
run({"numbers": "skip", "totals": "skip"}, rows, totals, types=("INTEGER", "BIGINT"))

# A failed conversion leaves no successful seed confirmation, even with an old
# target present. Retry corrected bytes through a clean generation.
(root / "seeds/numbers.csv").write_text("id,amount\n1,invalid\n")
run({"numbers": "build"}, rows, totals, fail=True, unconfirmed=("numbers",))
(root / "seeds/numbers.csv").write_text("id,amount\n1,11\n2,30\n2,30\n3,\n")
run({"numbers": "build", "totals": "build"}, rows, totals, types=("INTEGER", "BIGINT"))
run({"numbers": "skip", "totals": "skip"}, rows, totals, types=("INTEGER", "BIGINT"))
for sql in ("update dev.numbers set amount=999", "drop table dev.numbers"):
    with duckdb.connect(str(root / "warehouse.duckdb")) as db:
        db.execute(sql)
    run(
        {"numbers": "build", "totals": "build"},
        rows,
        totals,
        types=("INTEGER", "BIGINT"),
    )
    run(
        {"numbers": "skip", "totals": "skip"}, rows, totals, types=("INTEGER", "BIGINT")
    )

# Returned unsuccessful outcomes, including write-only mode, cannot certify an
# extant target after the real runner has executed its warehouse materialization.
(root / "seeds/numbers.csv").write_text("id,amount\n4,99\n")
run(
    {"numbers": "build"},
    [],
    [],
    args=("--select", "numbers"),
    fail=True,
    unconfirmed=("numbers",),
    returned_error=True,
)
profile = (root / "profiles.yml").read_text()
(root / "profiles.yml").write_text(profile + "      run_cache_cache_mode: WRITE_ONLY\n")
run({}, [], [], args=("--select", "numbers"), fail=True, returned_error=True)
(root / "profiles.yml").write_text(profile)
(root / "seeds/numbers.csv").write_text("id,amount\n1,11\n2,30\n2,30\n3,\n")
run({"numbers": "build", "totals": "build"}, rows, totals, types=("INTEGER", "BIGINT"))
run({"numbers": "skip", "totals": "skip"}, rows, totals, types=("INTEGER", "BIGINT"))

# Same relation spelling and copied metadata at a distinct physical path cannot
# inherit successful history from the original file.
(root / "copy").mkdir()
shutil.copy2(root / "warehouse.duckdb", root / "copy/warehouse.duckdb")
profile = (root / "profiles.yml").read_text()
(root / "profiles.yml").write_text(
    profile.replace(str(root / "warehouse.duckdb"), str(root / "copy/warehouse.duckdb"))
)
run(
    {"numbers": "build", "totals": "build"},
    rows,
    totals,
    types=("INTEGER", "BIGINT"),
    database=root / "copy/warehouse.duckdb",
)
(root / "profiles.yml").write_text(profile)
run({"numbers": "skip", "totals": "skip"}, rows, totals, types=("INTEGER", "BIGINT"))

# A legal dbt schema transition to unsupported evidence types executes normally
# on every invocation, and cannot certify the seed merely from its file hash.
(root / "seeds/schema.yml").write_text(
    "version: 2\nseeds:\n  - name: numbers\n    config:\n      column_types:\n        amount: double\n"
)
run(
    {"numbers": "build", "totals": "build"},
    rows,
    totals,
    args=("--full-refresh",),
    types=("INTEGER", "DOUBLE"),
    unconfirmed=("numbers",),
)
run(
    {"numbers": "build", "totals": "build"},
    rows,
    totals,
    types=("INTEGER", "DOUBLE"),
    unconfirmed=("numbers",),
)
