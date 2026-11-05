#!/usr/bin/env python3
"""Build once, then run every real DuckDB acceptance case with bounded workers."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import subprocess
import time

REPO = Path(__file__).resolve().parents[1]
FIXED = (
    'duckdb_python', 'duckdb_functions_python', 'duckdb_identity_python', 'duckdb_recovery',
    'duckdb_interruptions', 'duckdb_parallel', 'duckdb_seeds_python',
    'duckdb_data_tests_python', 'duckdb_copy_python',
    'duckdb_incremental_python', 'duckdb_workflow',
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jobs', type=int, default=4)
    parser.add_argument('--generated-only', action='store_true')
    parser.add_argument('--artifacts', type=Path, default=REPO / 'artifacts/duckdb')
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error('--jobs must be positive')
    os.chdir(REPO)
    started = time.monotonic()
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    # An explicit locked sync is part of the gate, even on a cached environment.
    subprocess.run(['uv', 'sync', '--project', 'integrations/duckdb', '--locked'], check=True)
    python = str(REPO / 'integrations/duckdb/.venv/bin/python')
    (artifacts / 'fixed').mkdir(exist_ok=True)
    env = {**os.environ, 'DBT_STATE_DUCKDB_PYTHON': python,
           'PYTHONPATH': str(REPO / 'integrations/duckdb/src'),
           'DBT_STATE_GENERATED_ARTIFACTS': str(artifacts / 'generated'),
           'TMPDIR': str(artifacts / 'fixed')}
    # Focused replay shortcuts must not silently trim the mandatory fixed matrix.
    for focused in ('COPY_SCENARIO', 'VIEW_SCENARIO', 'SEED_SCENARIO', 'INTERRUPT_ONLY_PHASE'):
        env.pop(focused, None)
    # JSON supplies exact executables even with CARGO_TARGET_DIR, and avoids
    # concurrent cargo processes or guessing hashed executable names.
    build = subprocess.run(['cargo', 'build', '--workspace', '--all-targets', '--message-format=json'],
                           text=True, stdout=subprocess.PIPE)
    (artifacts / 'build.jsonl').write_text(build.stdout)
    build.check_returncode()
    binaries = {}
    for line in build.stdout.splitlines():
        entry = json.loads(line)
        if entry.get('reason') != 'compiler-artifact' or not entry.get('executable'):
            continue
        if entry['profile']['test'] and 'test' in entry['target']['kind']:
            binaries[entry['target']['name']] = entry['executable']
        if entry['target']['name'] == 'dbt-state-server' and not entry['profile']['test']:
            env['DBT_STATE_SERVER_BIN'] = entry['executable']
    assert 'DBT_STATE_SERVER_BIN' in env, 'production server not built'
    built = time.monotonic()
    cases = []
    for target in (() if args.generated_only else FIXED) + ('duckdb_generated',):
        binary = binaries[target]
        listing = subprocess.check_output([binary, '--list', '--ignored', '--format=terse'], text=True)
        names = [line.removesuffix(': test') for line in listing.splitlines() if line.endswith(': test')]
        assert names, f'{target}: no acceptance cases found'
        cases.extend((target, name, binary) for name in names)
    # The independent generator's small literal-oracle tests are mandatory too.
    with (artifacts / 'oracle.log').open('w') as log:
        subprocess.run([python, '-m', 'unittest', 'discover', '-s', 'integrations/duckdb/tests/generated'],
                       env=env, stdout=log, stderr=subprocess.STDOUT, check=True)

    def run(case):
        target, name, binary = case
        tick = time.monotonic()
        with (artifacts / f'{target}-{name}.log').open('w') as log:
            result = subprocess.run([binary, '--ignored', '--exact', name, '--nocapture'],
                                    env=env, stdout=log, stderr=subprocess.STDOUT)
        return dict(target=target, case=name, seconds=round(time.monotonic()-tick, 2),
                    success=result.returncode == 0)

    results = []
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for future in as_completed([pool.submit(run, case) for case in cases]):
            result = future.result()
            results.append(result)
            print(json.dumps(result), flush=True)
    report = dict(jobs=args.jobs, setup_build_seconds=round(built-started, 2),
                  acceptance_seconds=round(time.monotonic()-built, 2),
                  total_seconds=round(time.monotonic()-started, 2), cases=results)
    (artifacts / 'timing.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2), flush=True)
    if not all(result['success'] for result in results):
        raise SystemExit(f'Acceptance failed; logs and replay traces: {artifacts}')


if __name__ == '__main__':
    main()
