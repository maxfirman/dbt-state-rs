#!/usr/bin/env python3
"""Run both #141 causal stages against one backend/store and one fresh pair."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('dbt', 'proxy', 'probe', 'work-dir', 'schema-lease'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--query-identity')
    parser.add_argument('--rust-url')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    args = parser.parse_args()
    work = Path(args.work_dir).resolve()
    if work.exists():
        parser.error('campaign root must be fresh')
    work.mkdir(parents=True)
    shared = []
    for key, value in vars(args).items():
        if key != 'work_dir' and value is not None:
            shared += ['--' + key.replace('_', '-'), value]
    for stage, script in [('base', 'databricks_target_lifecycle.py'),
                          ('continuation', 'databricks_target_lifecycle_continuation.py')]:
        command = [sys.executable, str(Path(__file__).with_name(script)), *shared,
                   '--work-dir', str(work / stage)]
        if stage == 'continuation':
            command += ['--prerequisite', str(work / 'base')]
        result = subprocess.run(command)
        if result.returncode:
            return result.returncode
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
