#!/usr/bin/env python3
"""Pinned-client selector campaign with two fresh run-owned Delta tables.

The baseline run creates two tables; a later data-only UPDATE changes its root.
Every selector invocation compiles. The source archive remains unchanged.
"""
import argparse
import configparser
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
import uuid

from capture_integrity import verify_capture

DIGEST = 'f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31'
CRITERIA = ['modified', 'new', 'unmodified', 'old', 'modified.body',
            'modified.configs', 'modified.relation', 'modified.persisted_descriptions',
            'modified.macros', 'modified.contract']


def certify(work):
    """Check actual client artifacts against full RPC selections and graph closure."""
    work = Path(work)
    captures = list((work / 'golden').glob('*.jsonl'))
    if len(captures) != 1:
        raise RuntimeError('expected one complete recording')
    entries = verify_capture(captures[0])
    if any(e['status']['code'] != 0 for e in entries):
        raise RuntimeError('unexpected rejected RPC')
    if (work / 'root_before_sql.json').exists():
        from databricks_survey import complete_result
        for label, value in [('root_before', '1'), ('child_before', '1'), ('root_after', '2')]:
            evidence = json.loads((work / (label + '_sql.json')).read_text())
            result = complete_result(evidence['response'], require_rows=True)
            if (result['rows'] != [[value]]
                    or [(c['name'], c['type_text']) for c in result['columns']] != [('id', 'INT')]):
                raise RuntimeError('independent complete rows/schema mismatch: ' + label)
    operations = json.loads((work / 'operations.json').read_text())
    selections = {}
    for number, operation in enumerate(operations):
        label = operation['label']
        rejected = label.startswith('inventory_') or label == 'ls_state'
        if operation['returncode'] != (1 if rejected else 0):
            raise RuntimeError('unexpected command outcome: ' + label)
        if rejected:
            log = (work / f'{number}_{label}.log').read_text()
            expected = 'no comparison manifest' if label == 'ls_state' else 'Invalid state selector'
            if expected not in log:
                raise RuntimeError('unexpected unreachable surface: ' + label)
            continue
        target = work / 'targets' / str(number)
        metadata = json.loads((target / 'run_results.json').read_text())['metadata']
        invocation = metadata['invocation_id']
        calls = [e for e in entries if dict(e['metadata']).get('x-dbt-invocation-id') == invocation]
        if operation['argv'][1] == 'run':
            submits = [e for e in calls if e['method'] == 'SubmitEnrichedSQL']
            expected = 'ready_to_execute' if label == 'build' else 'skip_execution'
            if len(submits) != 2 or any(expected not in e['response']['response'] for e in submits):
                raise RuntimeError('baseline must execute then reuse both models')
            if label == 'build':
                confirmed = {e['request']['request_id'] for e in calls
                             if e['method'] == 'ConfirmExecution' and e['response']['success']}
                if confirmed != {e['response']['response'][expected]['request_id'] for e in submits}:
                    raise RuntimeError('baseline lacks complete successful confirmations')
            continue
        queries = [e for e in calls if e['method'] == 'GetStateSelection']
        if len(queries) != 1:
            raise RuntimeError('expected one selector RPC: ' + label)
        selected = {uid.rsplit('.', 1)[-1] for uid in queries[0]['response']['node_unique_ids']}
        compiled = {f.stem for f in (target / 'compiled').rglob('*.sql')}
        closure = set(selected)
        if label == 'body_descendants' and 'root' in closure:
            closure.add('child')
        if label == 'body_ancestors' and 'child' in closure:
            closure.add('root')
        if compiled != closure:
            raise RuntimeError('client graph closure disagrees with full RPC response: ' + label)
        if label.startswith('first_'):
            expected = set() if label in ('first_old', 'first_unmodified') else {'root', 'child'}
            if selected != expected:
                raise RuntimeError('unexpected first baseline: ' + label)
        if label.startswith(('baseline_', 'data_')):
            criterion = label.split('_', 1)[1]
            expected = {'root', 'child'} if criterion in ('old', 'unmodified', 'modified.relation') else set()
            if selected != expected:
                raise RuntimeError('unexpected unchanged/data-only baseline: ' + label)
        if label in ('body_modified.body', 'effective_configs_modified.configs',
                     'macros_modified.macros', 'persisted_descriptions_modified.persisted_descriptions',
                     'contract_modified.contract') and selected != {'root'}:
            raise RuntimeError('changed component did not select root: ' + label)
        selections[label] = sorted(compiled)
    return selections


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('archive', 'work-dir', 'dbt', 'proxy', 'schema', 'run-id'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--local-upstream')
    parser.add_argument('--baseline-work', help='extend this completed owned baseline; never rebuild it')
    args = parser.parse_args()
    import re
    if not re.fullmatch(r'ds_[a-z0-9_]+', args.schema):
        raise ValueError('expected retained demo schema')
    if uuid.UUID(args.run_id).hex != args.run_id:
        raise ValueError('run-id must be a fresh UUID hex string')
    archive, work = Path(args.archive).resolve(), Path(args.work_dir).resolve()
    if hashlib.sha256(Path(args.dbt).read_bytes()).hexdigest() != DIGEST:
        raise ValueError('expected pinned dbt 2.0.11')
    work.mkdir(parents=True, exist_ok=True)
    if any(work.iterdir()):
        raise ValueError('work directory must be empty')
    (work / 'runner.py').write_bytes(Path(__file__).read_bytes())
    project = work / 'project'
    (project / 'models').mkdir(parents=True)
    (project / 'macros').mkdir()
    name = 'selectors_' + args.run_id
    prefix = 's156_' + args.run_id + '_'
    owned = ['cdo_demo_db.' + args.schema + '.' + prefix + n for n in ('root','child')]
    (work / 'ownership.json').write_text(json.dumps(dict(catalog='cdo_demo_db', schema=args.schema,
        owned=owned, retained_sources=str(archive), cleanup='retain; no drops'), indent=2))
    (project / 'dbt_project.yml').write_text(json.dumps(dict(name=name, version='1.0', profile='survey',
        **{'dbt-cloud':{'project-id':args.run_id}})))
    root = "{{ config(materialized='table',alias='" + prefix + "root',file_format='delta',state={'pre_clone':'never'}) }}\nselect {{ selector_value() }} as id where '" + args.run_id + "'='" + args.run_id + "'\n"
    child = "{{ config(materialized='table',alias='" + prefix + "child',file_format='delta',state={'pre_clone':'never'}) }}\nselect * from {{ ref('root') }}\n"
    macro = '{% macro selector_value() %}1{% endmacro %}\n'
    (project / 'models/root.sql').write_text(root)
    (project / 'models/child.sql').write_text(child)
    (project / 'macros/value.sql').write_text(macro)
    profiles = work / 'profiles'
    profiles.mkdir()
    import yaml
    source = yaml.safe_load((archive / 'profiles/profiles.yml').read_text())['survey']['outputs']['prod']
    if source.get('type') != 'databricks' or source.get('catalog') != 'cdo_demo_db':
        raise ValueError('expected demo Databricks profile')
    source = dict(source, schema=args.schema, token="{{ env_var('DBT_ENV_SECRET_DATABRICKS_TOKEN') }}")
    source.pop('defer_to_target', None)
    (profiles / 'profiles.yml').write_text(json.dumps({'survey':{'target':'prod','outputs':{
        'prod':source,'dev':dict(source,defer_to_target='prod')}}}))
    inventory = subprocess.run(['databricks','api','get',
        '/api/2.1/unity-catalog/tables?catalog_name=cdo_demo_db&schema_name=' + args.schema,
        '-o','json'], capture_output=True, text=True, timeout=60)
    if inventory.returncode:
        raise RuntimeError('read-only collision inventory failed')
    listing = json.loads(inventory.stdout)
    if listing.get('next_page_token'):
        raise RuntimeError('incomplete collision inventory')
    present = {t['full_name'] for t in listing.get('tables',[])}
    if args.baseline_work:
        previous = Path(args.baseline_work).resolve()
        if json.loads((previous / 'ownership.json').read_text())['owned'] != owned:
            raise RuntimeError('extension does not own these exact baseline relations')
        captures = list((previous / 'golden').glob('*.jsonl'))
        if len(captures) != 1:
            raise RuntimeError('extension needs one completed baseline capture')
        verify_capture(captures[0])
        if not set(owned).issubset(present):
            raise RuntimeError('extension baseline is missing owned relations')
    elif any(t['name'].startswith(prefix) for t in listing.get('tables',[])):
        raise RuntimeError('owned-name collision')
    (work / 'inventory-before.json').write_text(json.dumps(listing, indent=2))
    config = configparser.ConfigParser(interpolation=None)
    config.read(Path.home() / '.databrickscfg')
    token = config['DEFAULT']['token']
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env = dict(os.environ, DBT_ENV_SECRET_DATABRICKS_TOKEN=token,
               DBT_ENGINE_MANAGE_STATE='true', RUN_CACHE_API_URL=f'127.0.0.1:{port}',
               RUN_CACHE_API_SECURE='false')
    proxy_env = dict(env, PROXY_LISTEN=f'127.0.0.1:{port}', GOLDEN_DIR=str(work / 'golden'),
                     PROXY_UPSTREAM='https://api.state.dbt.com:443', RUST_LOG='warn')
    proxy_env.pop('PROXY_LOCAL_UPSTREAM', None)
    if args.local_upstream:
        proxy_env['PROXY_LOCAL_UPSTREAM'] = args.local_upstream
    operations = []

    def invoke(label, selection, command='compile', target='dev'):
        number = len(operations)
        target_path = work / 'targets' / str(number)
        log_path = work / 'logs' / str(number)
        argv = [args.dbt, command, '--select', selection, '--target', target,
                '--profiles-dir', str(profiles), '--profile', 'survey',
                '--target-path', str(target_path), '--log-path', str(log_path),
                '--manage-state', '--no-version-check', '--skip-semantic-manifest-validation']
        if command not in ('compile','ls','run') or (command == 'run' and selection != 'root child'):
            raise ValueError('command outside owned generated graph')
        result = subprocess.run(argv, cwd=project, env=env, capture_output=True, text=True, timeout=180)
        (work / f'{number}_{label}.log').write_text((result.stdout + result.stderr).replace(token, '[REDACTED]'))
        selected = []
        results = target_path / 'run_results.json'
        if results.exists():
            selected = [r['unique_id'] for r in json.loads(results.read_text())['results']]
        if command == 'compile':
            selected = sorted('model.' + name + '.' + f.stem for f in (target_path / 'compiled' / name / 'models').glob('*.sql'))
        operations.append(dict(label=label, argv=argv, returncode=result.returncode, selected=selected))
        (work / 'operations.json').write_text(json.dumps(operations, indent=2))
        for path in log_path.rglob('*'):
            if path.is_file():
                path.write_text(path.read_text(errors='replace').replace(token, '[REDACTED]'))
        print(label, result.returncode, len(selected), flush=True)

    def sql(label, statement):
        from databricks_survey import complete_result
        # Statements are generated here exclusively for the two exact owned
        # names; no arbitrary SQL argument is accepted.
        payload = dict(warehouse_id=source['http_path'].rsplit('/',1)[-1],
            statement=statement,wait_timeout='30s',disposition='INLINE',format='JSON_ARRAY')
        def api(verb, endpoint, payload=None):
            argv = ['databricks','api',verb,endpoint,'-o','json']
            if payload is not None:
                argv += ['--json',json.dumps(payload)]
            result = subprocess.run(argv,capture_output=True,text=True,timeout=90)
            if result.returncode:
                raise RuntimeError('owned SQL API failed')
            return json.loads(result.stdout)
        response = api('post','/api/2.0/sql/statements',payload)
        deadline = time.monotonic() + 180
        while response.get('status',{}).get('state') in ('PENDING','RUNNING'):
            if time.monotonic() > deadline:
                raise RuntimeError('owned SQL timed out')
            time.sleep(1)
            response = api('get','/api/2.0/sql/statements/' + response['statement_id'])
        (work / (label + '_sql.json')).write_text(json.dumps(dict(statement=statement,response=response),indent=2))
        if response.get('status',{}).get('state') != 'SUCCEEDED':
            raise RuntimeError('owned SQL did not succeed')
        return complete_result(response,require_rows=statement.startswith('SELECT'))

    def run_full():
        for criterion in CRITERIA:
            invoke('first_' + criterion, 'state:' + criterion)
        invoke('build', 'root child', command='run', target='prod')
        if operations[-1]['returncode']:
            raise RuntimeError('owned baseline build failed')
        invoke('reuse', 'root child', command='run', target='prod')
        for criterion in CRITERIA:
            invoke('baseline_' + criterion, 'state:' + criterion)
        for mode in ('body','configs','relation','macros','persisted_descriptions','contract'):
            (project / 'models/root.sql').write_text(root)
            (project / 'macros/value.sql').write_text(macro)
            schema = project / 'models/schema.yml'
            if schema.exists():
                schema.unlink()
            if mode == 'body':
                (project / 'models/root.sql').write_text(root + '-- body change\n')
            elif mode == 'configs':
                (project / 'models/root.sql').write_text("{{ config(tags=['selector_change']) }}\n" + root)
            elif mode == 'relation':
                (project / 'models/root.sql').write_text(root.replace(prefix + 'root',prefix + 'renamed'))
            elif mode == 'macros':
                (project / 'macros/value.sql').write_text(macro.replace('%}1','%}2'))
            elif mode == 'persisted_descriptions':
                (project / 'models/root.sql').write_text("{{ config(persist_docs={'relation':true,'columns':true}) }}\n" + root)
                schema.write_text('version: 2\nmodels:\n  - name: root\n    description: selector description\n    columns:\n      - name: id\n        description: selector column\n')
            else:
                schema.write_text('version: 2\nmodels:\n  - name: root\n    config:\n      contract:\n        enforced: true\n    columns:\n      - name: id\n        data_type: int\n')
            for criterion in CRITERIA:
                invoke(mode + '_' + criterion, 'state:' + criterion)
            if mode == 'body':
                invoke('body_descendants', 'state:modified.body+')
                invoke('body_ancestors', '+state:modified.body')
        (project / 'models/root.sql').write_text(root)
        (project / 'macros/value.sql').write_text(macro)
        (project / 'models/schema.yml').unlink()
        for criterion in ('modified.sql', 'modified.shell', 'unknown'):
            invoke('inventory_' + criterion, 'state:' + criterion)
        invoke('ls_state', 'state:modified', command='ls')

    with (work / 'proxy.log').open('w') as log:
        process = subprocess.Popen([args.proxy], env=proxy_env, stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(200):
                if process.poll() is not None:
                    raise RuntimeError('proxy exited')
                with socket.socket() as sock:
                    if sock.connect_ex(('127.0.0.1', port)) == 0:
                        break
                time.sleep(.1)
            else:
                raise RuntimeError('proxy startup timeout')
            if not args.baseline_work:
                run_full()
            for model, relation in zip(('root','child'),owned):
                result = sql(model + '_before', 'SELECT * FROM ' + relation)
                if result['rows'] != [['1']] or [(c['name'],c['type_text']) for c in result['columns']] != [('id','INT')]:
                    raise RuntimeError('independent literal rows/schema mismatch')
            (project / 'models/root.sql').write_text(root.replace("materialized='table'","materialized='view'"))
            for criterion in CRITERIA:
                invoke('effective_configs_' + criterion, 'state:' + criterion)
            (project / 'models/root.sql').write_text(root)
            sql('data_update', 'UPDATE ' + owned[0] + ' SET id=2')
            result = sql('root_after', 'SELECT * FROM ' + owned[0])
            if (result['rows'] != [['2']]
                    or [(c['name'], c['type_text']) for c in result['columns']] != [('id', 'INT')]):
                raise RuntimeError('independent changed-row mismatch')
            for criterion in CRITERIA:
                invoke('data_' + criterion, 'state:' + criterion)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                process.wait(timeout=30)
    files = list((work / 'golden').glob('*.jsonl'))
    if len(files) != 1:
        raise RuntimeError('expected one capture')
    entries = verify_capture(files[0])
    if any(e['service'] not in ('SelectorService', 'ClientValidation', 'ClientTelemetry', 'Execution', 'SQL') for e in entries):
        raise RuntimeError('unexpected service')
    (work / 'summary.json').write_text(json.dumps(dict(client_sha256=DIGEST,
        rpc_count=len(entries), criteria=sorted({e['request']['selector_criteria'] for e in entries
        if e['method'] == 'GetStateSelection'})), indent=2))
    certify(work)



if __name__ == '__main__':
    main()
