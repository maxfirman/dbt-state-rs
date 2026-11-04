"""Fresh documented project, real four-worker builds, copy, restore and isolation."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import duckdb

root = Path(sys.argv[1])
project = root / 'project'
shutil.copytree(Path(__file__).resolve().parents[1] / 'example', project)
path = root / 'warehouse.duckdb'
backup = root / 'backup.duckdb'
all_names = {'orders', 'order_view', 'ledger', 'nonnegative'}


def run(target, command='build', *, expect=None, copying=False, failure=False):
    output = root / f'events-{len(list(root.glob("events-*.json")))}.json'
    args = [sys.executable, str(Path(__file__).with_name('workflow_invoke.py')),
            command, '--project-dir', str(project), '--profiles-dir', str(project),
            '--target', target]
    if command == 'build':
        # Stage the shared raw seed first: incremental initialization deliberately
        # accepts literal physical inputs rather than ref()/source() templates.
        args += ['--exclude', 'resource_type:seed']
    result = subprocess.run(args, env={**os.environ, 'DUCKDB_WORKFLOW_PATH': str(path),
                            'WORKFLOW_EVENTS': str(output)}, capture_output=True, text=True, timeout=120)
    output.with_suffix('.log').write_text(result.stdout + result.stderr)
    if failure:
        assert result.returncode != 0, result.stdout + result.stderr
        outcomes = json.loads((project / 'target/run_results.json').read_text())['results']
        statuses = {r['unique_id'].rsplit('.', 1)[-1]: r['status'] for r in outcomes}
        assert statuses['orders'] == 'error' and statuses['order_view'] == 'skipped', statuses
        return
    assert result.returncode == 0, result.stdout + result.stderr
    events = json.loads(output.read_text())
    submitted = {e['uid'].rsplit('.', 1)[-1]: e['decision'] for e in events if e['uid']}
    if expect is not None:
        assert submitted == expect, (submitted, expect, events)
    if copying:
        # A real registration, successful physical CTAS completion and correlated
        # confirmation must initialize the incremental model before SQL submit.
        assert any(e['method'] == 'register_clone' and e['decision'] == 'copy'
                   and e.get('confirmed') for e in events), events
        assert submitted['orders'] == 'copy', events
    return {e['namespace'] for e in events}


def check(target):
    with duckdb.connect(str(path)) as db:
        for name, rows in (
            ('orders', [(1,11), (2,21), (2,21), (3,None)]),
            ('order_view', [(1,11), (2,21), (2,21), (3,None)]),
            ('ledger', [(1,10), (2,20), (2,20), (3,None)]),
        ):
            assert db.execute(f'select * from {target}.{name} order by all').fetchall() == rows
            assert [(r[0],r[1]) for r in db.execute(f'describe {target}.{name}').fetchall()] == [('id','INTEGER'),('amount','INTEGER')]
        assert db.execute('select * from raw.inputs order by all').fetchall() == [(1,10),(2,20),(2,20),(3,None)]


run('prod', 'seed', expect={'inputs': 'build'})
run('prod', 'seed', expect={'inputs': 'skip'})
cold = run('prod', expect=dict.fromkeys(all_names, 'build'))
check('prod')
run('prod', expect=dict.fromkeys(all_names, 'skip'))
run('dev', copying=True)
check('dev')
run('dev', expect=dict.fromkeys(all_names, 'skip'))
# A real failed SQL worker leaves an uncertain invocation. Fix the SQL and
# require every workload to establish fresh evidence together before reusing.
orders = project / 'models/orders.sql'
original_sql = orders.read_text()
orders.write_text('select missing_column from warehouse.raw.inputs')
run('prod', failure=True)
check('prod')
orders.write_text(original_sql)
recovered = run('prod', expect=dict.fromkeys(all_names, 'build'))
assert cold.isdisjoint(recovered)
check('prod')
run('prod', expect=dict.fromkeys(all_names, 'skip'))
# Restore a real snapshot only after explicit reset; all workload types must
# establish fresh scoped history together, then reuse across a process restart.
shutil.copyfile(path, backup)
with duckdb.connect(str(path)) as db:
    db.execute('update raw.inputs set amount=999')
shutil.copyfile(backup, path)
subprocess.run([sys.executable, '-m', 'dbt_state_duckdb', 'reset', '--database', str(path)], check=True)
reset = run('prod', expect=dict.fromkeys(all_names, 'build'))
assert recovered.isdisjoint(reset), (recovered, reset)
check('prod')
run('prod', expect=dict.fromkeys(all_names, 'skip'))
# A copied file with identical embedded metadata and relation names cannot adopt
# the first file's history. Keep the same catalog stem in a different directory.
other = root / 'isolated' / 'warehouse.duckdb'
other.parent.mkdir()
shutil.copyfile(path, other)
path = other
isolated = run('prod', expect=dict.fromkeys(all_names, 'build'))
assert reset.isdisjoint(isolated)
check('prod')
run('prod', expect=dict.fromkeys(all_names, 'skip'))
print('mixed workflow: four workers; seed/table/view/test/incremental BUILD/SKIP; confirmed prod/dev copies; restore/reset; file isolation')
