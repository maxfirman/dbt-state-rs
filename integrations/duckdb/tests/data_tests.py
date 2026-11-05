"""Real dbt data tests, with an independent DuckDB SQL result oracle."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import duckdb

root = Path(sys.argv[1])
project = root / "project"
project.mkdir(parents=True)
(project / "models").mkdir()
(project / "tests").mkdir()
path = root / "warehouse.duckdb"
with duckdb.connect(str(path)) as db:
    db.execute("create schema dev")
    db.execute("create table dev.numbers(id integer, amount integer)")
    db.execute("insert into dev.numbers values (1,10), (2,20), (2,20), (3,NULL)")
(project / "dbt_project.yml").write_text(
    'name: test_outcomes\nversion: "1.0"\nconfig-version: 2\nprofile: local\n'
)
(project / "models/sources.yml").write_text(
    "version: 2\nsources:\n  - name: inputs\n    schema: dev\n    tables:\n      - name: numbers\n"
)
for name, severity, condition in [
    ("passing", "error", "amount < 0"),
    ("failure", "error", "amount > 10"),
    ("warning", "warn", "amount > 10"),
]:
    (project / f"tests/{name}.sql").write_text(
        "{{ config(severity='"
        + severity
        + "') }}\nselect * from {{ source('inputs', 'numbers') }} where "
        + condition
    )


def profile(folder, database):
    (folder / "profiles.yml").write_text(
        "local:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
        + f"      path: {database}\n"
        + "      schema: dev\n      threads: 3\n      run_cache_enabled: true\n      run_cache_enable_data_tests: true\n"
    )


def run(decision, *, folder=project, expected=(0, 2, 2), select=None, sql_error=False):
    observation = root / "observations.json"
    args = [
        sys.executable,
        str(Path(__file__).with_name("data_tests_invoke.py")),
        "test",
        "--project-dir",
        str(folder),
        "--profiles-dir",
        str(folder),
    ]
    if select:
        args += ["--select", select]
    result = subprocess.run(
        args,
        env={**os.environ, "TEST_OBSERVATIONS": str(observation)},
        capture_output=True,
        text=True,
    )
    events = json.loads(observation.read_text())
    assert len(events) == (1 if select else 3), (result.stdout, result.stderr, events)
    assert all(e["decision"] == decision for e in events), (events, result.stdout)
    results = json.loads((folder / "target/run_results.json").read_text())["results"]
    if sql_error:
        assert result.returncode != 0 and results[0]["status"] == "error", results
        assert all(not e["confirmed"] and e["results"] is None for e in events), events
        return events
    by_name = {r["unique_id"].split(".")[-1]: r for r in results}
    assert result.returncode == (1 if any(expected[:2]) else 0), (
        result.returncode,
        result.stdout,
        result.stderr,
    )
    for event in events:
        name = event["uid"].split(".")[-1]
        index = ["passing", "failure", "warning"].index(name)
        failures = expected[index]
        payload = {
            "failures": failures,
            "should_error": bool(failures),
            "should_warn": bool(failures),
        }
        # dbt's wrapper evaluates both predicates independently; severity selects status.
        assert event["results"] == payload, event
        assert event["confirmed"] == (decision == "build"), event
        artifact = by_name[name]
        assert artifact["failures"] == failures, artifact
        if failures:
            assert artifact["status"] == (
                "warn" if name == "warning" else "fail"
            ), artifact
            assert f"Got {failures} results" in artifact["message"], artifact
        else:
            assert artifact["status"] in ("pass", "success", "skipped"), artifact
        assert event["namespace"] and event["project"], event
    return events


profile(project, path)
if len(sys.argv) > 2 and sys.argv[2] == "store-failures":
    (project / "tests/passing.sql").write_text(
        "{{ config(store_failures=true, severity='warn') }}\n"
        "select * from {{ source('inputs', 'numbers') }} where amount > 10"
    )
    for _ in range(2):
        observation = root / "observations.json"
        result = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("data_tests_invoke.py")),
                "test",
                "--project-dir",
                str(project),
                "--profiles-dir",
                str(project),
                "--select",
                "passing",
            ],
            env={**os.environ, "TEST_OBSERVATIONS": str(observation)},
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (result.stdout, result.stderr)
        assert (
            json.loads(observation.read_text()) == []
        ), "stored failures must use ordinary dbt"
        with duckdb.connect(str(path)) as db:
            assert db.execute(
                "select * from dev_dbt_test__audit.passing order by id"
            ).fetchall() == [(2, 20), (2, 20)]
    sys.exit(0)
# Cold execution must be followed by a positive SKIP, including failure and warning.
first = run("build")
second = run("skip")
assert {e["project"] for e in first} == {e["project"] for e in second}
# Same project name and node UID, same file, different local project root.
other = root / "other-project"
shutil.copytree(
    project, other, ignore=shutil.ignore_patterns("target", "logs", ".user.yml")
)
third = run("build", folder=other)
assert {e["namespace"] for e in first} == {e["namespace"] for e in third}
assert {e["project"] for e in first}.isdisjoint(e["project"] for e in third)
run("skip", folder=other)
run("skip")
# Execute the test predicate independently, not through dbt or State serializers.
with duckdb.connect(str(path)) as db:
    assert (
        db.execute("select count(*) from dev.numbers where amount > 10").fetchone()[0]
        == 2
    )
    db.execute("update dev.numbers set amount=-1 where id=2")
    assert (
        db.execute("select count(*) from dev.numbers where amount < 0").fetchone()[0]
        == 2
    )
run("build", expected=(2, 0, 0))
run("skip", expected=(2, 0, 0))
with duckdb.connect(str(path)) as db:
    db.execute("update dev.numbers set amount=20 where id=2")
run("build")
run("skip")
# A copied database file retains embedded metadata but has an independent scope.
copy_path = root / "copy" / "warehouse.duckdb"
copy_path.parent.mkdir()
shutil.copy2(path, copy_path)
profile(project, copy_path)
copy_events = run("build")
assert {e["namespace"] for e in first}.isdisjoint(e["namespace"] for e in copy_events)
run("skip")
profile(project, path)
run("skip")
# Unsupported observed types leave unknown freshness, even when SQL completes.
with duckdb.connect(str(path)) as db:
    db.execute("alter table dev.numbers alter amount type double")
unknown = run("build")
assert all(None in e["epochs"] for e in unknown), unknown
run("build")
# Missing inputs and SQL execution errors have no complete outcome to confirm.
with duckdb.connect(str(path)) as db:
    db.execute("drop table dev.numbers")
run("build", select="passing", sql_error=True)
run("build", select="passing", sql_error=True)
with duckdb.connect(str(path)) as db:
    db.execute("create table dev.numbers(id integer, amount integer)")
    db.execute("insert into dev.numbers values(1, 10), (2, 20), (2,20), (3,NULL)")
# Even an extant input cannot turn a genuine SQL failure into a cached pass.
(project / "tests/passing.sql").write_text(
    "select nonexistent from {{ source('inputs', 'numbers') }}"
)
run("build", select="passing", sql_error=True)
run("build", select="passing", sql_error=True)
