"""Interrupted real warehouse work never becomes reusable completed intent."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
scripts = Path(__file__).parent
subprocess.run(
    [sys.executable, str(scripts / "smoke.py"), str(root), "normal"], check=True
)
path = root / "warehouse.duckdb"
base = "select id, sum(amount)::bigint as amount from {{ ref('numbers') }} group by id"


def rows():
    with duckdb.connect(str(path)) as db:
        return db.execute("select * from dev.totals order by id").fetchall()


def run(expected):
    subprocess.run(
        [
            sys.executable,
            str(scripts / "invoke.py"),
            "run",
            "--project-dir",
            str(root),
            "--profiles-dir",
            str(root),
        ],
        check=True,
        env={
            **os.environ,
            "EXPECT_DECISION": expected,
            "STATE_OBSERVATION_PATH": str(root / "observations.jsonl"),
        },
    )
    assert rows() == [(1, 10), (2, 40)]
    return json.loads((root / "observations.jsonl").read_text().splitlines()[-1])


prior = run("skip")
for index, phase in enumerate(
    (
        "before-execution",
        "during-execution",
        "execution-error",
        "before-evidence",
        "during-evidence",
        "after-evidence",
        "before-confirm",
    )
):
    if os.environ.get("INTERRUPT_ONLY_PHASE", phase) != phase:
        continue
    # Poison the physical target independently. A pending BUILD cannot certify it.
    with duckdb.connect(str(path)) as db:
        db.execute("update dev.totals set amount=999")
    marker = root / (phase + ".json")
    entered = root / "warehouse-entered.csv"
    sql = base + f" having count(*) > {index} - {index}"
    if phase == "during-execution":
        # COPY proves the warehouse began this hook before termination. The long
        # aggregate keeps real DuckDB SQL in flight; no adapter mock executes it.
        hook = f"COPY (SELECT 1) TO '{entered}'; SELECT sum(i) FROM range(1000000000000) t(i)"
        sql += '\n{{ config(pre_hook="' + hook + '") }}'
    if phase == "execution-error":
        sql += "\n{{ config(pre_hook=\"SELECT CAST('not-an-integer' AS BIGINT)\") }}"
    (root / "models/totals.sql").write_text(sql)
    with (
        (root / (phase + ".log")).open("w") as log,
        subprocess.Popen(
            [
                sys.executable,
                str(scripts / "interrupt_invoke.py"),
                "run",
                "--project-dir",
                str(root),
                "--profiles-dir",
                str(root),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            env={
                **os.environ,
                "INTERRUPT_PHASE": phase,
                "INTERRUPT_MARKER": str(marker),
            },
        ) as process,
    ):
        try:
            if phase == "during-execution":
                deadline = time.monotonic() + 45
                while (
                    not entered.exists()
                    and process.poll() is None
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.05)
                assert entered.exists(), (phase, "warehouse hook did not begin")
                process.kill()
                assert process.wait(timeout=10) < 0
            else:
                code = process.wait(timeout=45)
                assert code == (1 if phase == "execution-error" else 74), (phase, code)
                if phase == "execution-error":
                    result = json.loads((root / "target/run_results.json").read_text())
                    assert result["results"][0]["status"] == "error", result
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
    pending = json.loads(marker.read_text())
    assert pending["namespace"] == prior["namespace"], phase
    assert pending["request_id"], phase
    assert rows() == (
        [(1, 999), (2, 999)]
        if phase in ("before-execution", "during-execution", "execution-error")
        else [(1, 10), (2, 40)]
    ), phase
    # Remove only the interrupted hook, retaining the exact submitted model SQL.
    (root / "models/totals.sql").write_text(sql.split("\n")[0])
    recovered = run("build")
    assert recovered["namespace"] != pending["namespace"], phase
    assert recovered["confirmed_epochs"], phase
    prior = run("skip")
    assert prior["namespace"] == recovered["namespace"], phase
