"""Recovery contracts at the real dbt/public gRPC boundary."""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb

root = Path(sys.argv[1])
subprocess.run(
    [sys.executable, str(Path(__file__).with_name("smoke.py")), str(root), "normal"],
    check=True,
)
path = root / "warehouse.duckdb"


def run(expected, *, command="run", extra=None):
    subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("invoke.py")),
            command,
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
            **(extra or {}),
        },
    )
    with duckdb.connect(str(path)) as db:
        assert db.execute("select * from dev.totals order by id").fetchall() == [
            (1, 10),
            (2, 40),
        ]
    return json.loads((root / "observations.jsonl").read_text().splitlines()[-1])


def reset_database():
    # Reset is a warehouse-only operation: no mode, endpoint or auth settings.
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("RUN_CACHE_", "DBT_STATE_"))
    }
    subprocess.run(
        [sys.executable, "-m", "dbt_state_duckdb", "reset", "--database", str(path)],
        check=True,
        env=env,
    )


before = run("skip")
with duckdb.connect(str(path)) as db:
    db.execute("CREATE SCHEMA personal")
    db.execute(
        "CREATE TABLE personal.memo (id INTEGER PRIMARY KEY, note VARCHAR DEFAULT 'kept')"
    )
    db.execute("INSERT INTO personal.memo(id) VALUES (42)")
    db.execute("CREATE VIEW personal.notes AS SELECT * FROM personal.memo")
reset_database()
with duckdb.connect(str(path)) as db:
    assert db.execute("select * from dev.totals order by id").fetchall() == [
        (1, 10),
        (2, 40),
    ]
    assert db.execute("select * from personal.notes").fetchall() == [(42, "kept")]
after = run("build")
assert after["namespace"] != before["namespace"]
assert run("skip")["namespace"] == after["namespace"]

# Failed confirmation is an uncertain distributed outcome, even if dbt succeeds.
(root / "models/totals.sql").write_text(
    "select id, (sum(amount)+0)::bigint as amount from {{ ref('numbers') }} group by id"
)
run("build", extra={"FAIL_CONFIRM": "1", "EXPECT_CONFIRM": "0"})
failed = json.loads((root / "observations.jsonl").read_text().splitlines()[-1])
recovered = run("build")
assert recovered["namespace"] != failed["namespace"]
run("skip")

# Every damaged scope must cold-build once and then retain healthy reuse.
mutations = [
    "DROP TABLE _dbt_state.database_identity",
    "DROP TABLE _dbt_state.evidence_clock",
    "DROP TABLE _dbt_state.relation_evidence",
    "UPDATE _dbt_state.database_identity SET version=999",
    "UPDATE _dbt_state.database_identity SET generation='corrupt'",
    "UPDATE _dbt_state.evidence_clock SET ordinal=0",
    "UPDATE _dbt_state.relation_evidence SET content_hash='broken'",
    "UPDATE _dbt_state.invocation SET dirty=true",
]
for mutation in mutations:
    prior = json.loads((root / "observations.jsonl").read_text().splitlines()[-1])
    with duckdb.connect(str(path)) as db:
        db.execute(mutation)
    current = run("build")
    assert current["namespace"] != prior["namespace"], mutation
    assert run("skip")["namespace"] == current["namespace"], mutation

# Literal historical metadata was captured using ticket #5's old implementation.
prior = run("skip")
with duckdb.connect(str(path)) as db:
    db.execute("DROP SCHEMA _dbt_state CASCADE")
    db.execute(
        Path(__file__).with_name("fixtures").joinpath("evidence-v1.sql").read_text()
    )
assert run("build")["namespace"] != prior["namespace"]
run("skip")

# A byte-for-byte copy at a different path cannot adopt its original's history.
original_root, original_path = root, path
copied = root / "copy"
copied.mkdir()
for name in ("models", "seeds", "tests"):
    shutil.copytree(root / name, copied / name)
shutil.copy2(root / "dbt_project.yml", copied / "dbt_project.yml")
(copied / "profiles.yml").write_text(
    (root / "profiles.yml")
    .read_text()
    .replace(str(path), str(copied / "warehouse.duckdb"))
)
shutil.copy2(path, copied / "warehouse.duckdb")
original = run("skip")
root, path = copied, copied / "warehouse.duckdb"
assert run("build")["namespace"] != original["namespace"]
run("skip")
root, path = original_root, original_path
assert run("skip")["namespace"] == original["namespace"]

# Recreating the same path gets fresh history, regardless of inode reuse.
prior = run("skip")
path.unlink()
assert run("build", command="build")["namespace"] != prior["namespace"]
run("skip")

# Restoration is explicitly reset before State is contacted again.
backup = root / "old-backup.duckdb"
shutil.copy2(path, backup)
with duckdb.connect(str(path)) as db:
    db.execute("UPDATE dev.numbers SET amount=amount+10")
# Do a separate real build so State history is newer than the restored warehouse.
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
    check=True,
)
shutil.copy2(backup, path)
reset_database()
assert run("build")["namespace"] != prior["namespace"]
run("skip")

# A crash after server confirmation still cannot leave a clean reusable scope.
(root / "models/totals.sql").write_text(
    "select id, sum(amount)::bigint as amount from {{ ref('numbers') }} group by id having count(*)>0"
)
args = [
    sys.executable,
    str(Path(__file__).with_name("invoke.py")),
    "run",
    "--project-dir",
    str(root),
    "--profiles-dir",
    str(root),
]
crashed = subprocess.run(
    args, check=False, env={**os.environ, "CRASH_AFTER_CONFIRM": "1"}
)
assert crashed.returncode == 73
run("build")
run("skip")

# The invocation lock rejects resets/overlapping runs even when DuckDB itself
# would otherwise permit opening the file. Hold at the public submit boundary.
pause = root / "paused"
with subprocess.Popen(
    args, env={**os.environ, "EXPECT_DECISION": "skip", "PAUSE_STATE_PATH": str(pause)}
) as running:
    try:
        deadline = time.monotonic() + 30
        while (
            not pause.exists()
            and running.poll() is None
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        assert pause.exists(), "real dbt did not reach State submit"
        rejected = subprocess.run(
            [
                sys.executable,
                "-m",
                "dbt_state_duckdb",
                "reset",
                "--database",
                str(path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert (
            rejected.returncode != 0 and "Another State invocation" in rejected.stderr
        )
        rejected = subprocess.run(args, check=False, capture_output=True, text=True)
        assert (
            rejected.returncode != 0
            and "Another State invocation" in rejected.stdout + rejected.stderr
        )
    finally:
        pause.with_suffix(".release").touch()
    assert running.wait(timeout=30) == 0
run("skip")
