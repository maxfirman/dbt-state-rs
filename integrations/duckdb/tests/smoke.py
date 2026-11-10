"""Real dbt processes and independent warehouse SQL establish expected results."""

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
mode = sys.argv[2]
root.mkdir(parents=True, exist_ok=True)
for name in ("models", "seeds", "tests"):
    (root / name).mkdir(exist_ok=True)
if not (root / "dbt_project.yml").exists():
    (root / "dbt_project.yml").write_text(
        'name: duckdb_smoke\nversion: "1.0"\nconfig-version: 2\nprofile: smoke\nmodels:\n  duckdb_smoke:\n    +materialized: table\n'
    )
    (root / "seeds/numbers.csv").write_text("id,amount\n1,10\n2,20\n2,20\n")
    (root / "models/totals.sql").write_text(
        "select id, sum(amount)::bigint as amount from {{ ref('numbers') }} group by id"
    )
    if root.name == "scalar":
        (root / "models/totals.sql").write_text(
            "select id, amount, id % 2 = 0 as b, concat('é|', chr(0))::varchar as s, amount::decimal(10,2) as d, from_hex('0001ff') as bytes from {{ ref('numbers') }}"
        )
    if root.name == "hooks":
        with (root / "models/totals.sql").open("a") as f:
            f.write("\n{{ config(post_hook='update {{ this }} set amount=amount+1') }}")
    (root / "tests/positive.sql").write_text(
        "select * from {{ ref('totals') }} where amount <= 0"
    )
    # Lag tolerance 0: these cases test that every evidence change propagates
    # (lag_tolerance.py covers reuse within a tolerance, ADR 0007).
    (root / "profiles.yml").write_text(
        f"smoke:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: {root / 'warehouse.duckdb'}\n      schema: dev\n      threads: 2\n      run_cache_enabled: true\n      run_cache_freshness_tolerance: 0s\n      run_cache_enable_data_tests: true\n"
    )

path = str(root / "warehouse.duckdb")
observations = root / "observations.jsonl"
prior = (
    json.loads(observations.read_text().splitlines()[-1])
    if observations.exists()
    else None
)
if mode != "normal":
    with duckdb.connect(path) as db:
        if mode == "input":
            db.execute("insert into dev.numbers values (2, 5), (3, NULL), (3, NULL)")
        elif mode == "target":
            db.execute("update dev.totals set amount=999")
        elif mode == "target-schema":
            db.execute("alter table dev.totals add column surprise boolean")
        elif mode == "drop":
            db.execute("drop table dev.totals")
        elif mode == "schema":
            db.execute("alter table dev.numbers add column extra varchar")
        elif mode == "rewrite":
            db.execute(
                "create or replace table dev.numbers as select * from dev.numbers order by id desc"
            )
        elif mode == "unsupported":
            db.execute("update _dbt_state.database_identity set version = 999")
        elif mode == "sql":
            (root / "models/totals.sql").write_text(
                "select id, sum(amount)::bigint as amount from {{ ref('numbers') }} group by id having count(*) > 0"
            )
        elif mode == "identical":
            # Distinct expression forces actual execution with identical output.
            sql = (root / "models/totals.sql").read_text()
            (root / "models/totals.sql").write_text(
                sql.replace("sum(amount)", "sum(amount) + 0")
            )
        elif mode == "unknown":
            (root / "models/totals.sql").write_text(
                "select id, sum(amount)::double as amount from {{ ref('numbers') }} group by id"
            )

expect = "skip" if mode in ("skip", "rewrite") else "build"
command = "build" if mode == "normal" else "run"
args = [
    sys.executable,
    str(Path(__file__).with_name("invoke.py")),
    command,
    "--project-dir",
    str(root),
    "--profiles-dir",
    str(root),
]
subprocess.run(
    args,
    check=True,
    stdin=subprocess.DEVNULL,
    env={
        **os.environ,
        "EXPECT_DECISION": expect,
        "EXPECT_CONFIRM": "0" if mode in ("unsupported", "unknown") else "1",
        "EVIDENCE_TEST_CLOCK": "1000" if mode == "normal" else "-1000",
        "FAIL_STATE": "1" if mode == "transport" else "0",
        "STATE_OBSERVATION_PATH": str(root / "observations.jsonl"),
    },
)
results = json.loads((root / "target/run_results.json").read_text())["results"]
assert all(r["status"] in ("pass", "success", "skipped") for r in results), results
with duckdb.connect(path) as db:
    # Independently execute the transformation, without State or its fingerprints.
    reference = db.execute(
        "select id, sum(amount)::bigint from dev.numbers group by id order by id"
    ).fetchall()
    if root.name == "scalar":
        reference = db.execute(
            "select id, amount, id % 2 = 0, concat('é|', chr(0))::varchar, amount::decimal(10,2), from_hex('0001ff') from dev.numbers order by id"
        ).fetchall()
    if root.name == "hooks":
        reference = [(i, a + 1) for i, a in reference]
    actual = db.execute("select * from dev.totals order by id").fetchall()
    assert actual == reference, (mode, actual, reference)
    if (
        mode in ("normal", "skip", "rewrite", "sql", "identical")
        and root.name == "second"
    ):
        assert actual == [(1, 10), (2, 40)]
    if mode == "input" and root.name != "scalar":
        assert actual == [(1, 10), (2, 45), (3, None)]
if prior and mode not in ("transport", "unsupported", "unknown"):
    event = json.loads(observations.read_text().splitlines()[-1])
    prior_epoch = next(iter(prior["confirmed_epochs"]), prior["target_epoch"])
    if expect == "skip":
        assert event["target_epoch"] == prior_epoch, (mode, prior, event)
    else:
        assert event["confirmed_epochs"][0] > prior_epoch, (mode, prior, event)
