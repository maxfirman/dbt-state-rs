#!/usr/bin/env python3
"""Pinned, opt-in Databricks capability campaign; never a Rust support claim.

Run with `uv run --with pyyaml python experiments/databricks_survey.py --help`.
All SQL is generated here against run-owned cdo_demo_db schemas. Successful and
failed artifacts remain in --work-dir; warehouse objects are deliberately retained
for diagnosis. The runner only stops the proxy process it created.
"""
import argparse
from collections import Counter
import configparser
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

from capture_integrity import verify_capture

DIGEST = 'f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31'
STRATEGIES = ['append', 'delete+insert', 'merge', 'insert_overwrite', 'replace_where', 'microbatch']
PLAN = {'client': '2.0.11', 'sha256': DIGEST, 'catalog': 'cdo_demo_db',
        'incrementals': STRATEGIES, 'snapshots': ['timestamp', 'check'],
        'copies': ['RegisterClone', 'SubmitEnrichedSQL', 'SubmitValues'],
        'freshness': ['insert', 'update', 'delete', 'no_op', 'add_column', 'properties', 'replace', 'drop_recreate', 'own_target', 'view_replace']}


class Refused(Exception):
    pass


class Scope:
    def __init__(self, run):
        if not re.fullmatch(r'\d{8}[a-f0-9]{6}', run):
            raise Refused('run id must be YYYYMMDD plus six hex digits')
        self.prefix = 'ds_' + run
        self.schemas = {self.prefix + '_' + role for role in ('raw', 'prod', 'dev', 'ref', 'prod_raw', 'dev_raw', 'ref_raw', 'prod_dbt_test__audit', 'dev_dbt_test__audit', 'ref_dbt_test__audit')}

    def relation(self, role, name):
        value = f'cdo_demo_db.{self.prefix}_{role}.{name}'
        self.check(value)
        return value

    def check(self, value):
        parts = value.replace('`', '').split('.')
        if (len(parts) != 3 or parts[0] != 'cdo_demo_db' or parts[1] not in self.schemas
                or not re.fullmatch('[a-z][a-z0-9_]*', parts[2])):
            raise Refused('relation is outside this run: ' + value)
        return value


def complete_result(response):
    if response.get('status', {}).get('state') != 'SUCCEEDED':
        raise Refused('statement did not succeed')
    manifest, result = response.get('manifest', {}), response.get('result', {})
    rows = result.get('data_array', [])
    if (manifest.get('truncated') or result.get('next_chunk_internal_link')
            or manifest.get('total_chunk_count', 1) > 1
            or manifest.get('total_row_count', len(rows)) != len(rows)
            or result.get('row_count', len(rows)) != len(rows)):
        raise Refused('incomplete SQL result; refusing a partial physical comparison')
    return {'columns': manifest.get('schema', {}).get('columns', []), 'rows': rows}


def same_result(left, right):
    return (left['columns'] == right['columns']
            and sorted(left['rows'], key=repr) == sorted(right['rows'], key=repr))


def check_snapshot(result, hard_deletes, deleted=False):
    names = [column['name'] for column in result['columns']]
    rows = [dict(zip(names, row)) for row in result['rows']]
    expected_count = 5 if deleted and hard_deletes == 'new_record' else 4
    if len(rows) != expected_count:
        raise Refused('snapshot lost or added complete SCD versions')
    if len({row.get('dbt_scd_id') for row in rows}) != len(rows) or any(not row.get('dbt_scd_id') for row in rows):
        raise Refused('snapshot SCD version ids are missing or duplicated')
    old = [row for row in rows if row.get('id') == '1' and row.get('v') == 'one']
    new = [row for row in rows if row.get('id') == '1' and row.get('v') == 'changed']
    if len(old) != 1 or len(new) != 1 or old[0].get('dbt_valid_to') != new[0].get('dbt_valid_from') or old[0].get('dbt_valid_to') is None or new[0].get('dbt_valid_to') is not None:
        raise Refused('snapshot update lost its closed historical/current interval')
    twos = [row for row in rows if row.get('id') == '2']
    current = [row for row in twos if row.get('dbt_valid_to') is None]
    if deleted and hard_deletes == 'invalidate':
        if current:
            raise Refused('snapshot invalidate left the deleted row current')
    elif deleted and hard_deletes == 'new_record':
        if len(twos) != 2 or len(current) != 1 or str(current[0].get('dbt_is_deleted')).lower() != 'true':
            raise Refused('snapshot new_record did not retain its deletion version')
    elif len(twos) != 1 or len(current) != 1:
        raise Refused('snapshot unexpectedly removed the unchanged/deleted-ignore row')
    if len([row for row in rows if row.get('id') == '3' and row.get('v') == 'three' and row.get('dbt_valid_to') is None]) != 1:
        raise Refused('snapshot inserted row is missing or duplicated')


def check_baseline(first, repeat):
    def submits(rows):
        found = {}
        for row in rows:
            if row.get('method') in ('SubmitValues', 'SubmitEnrichedSQL'):
                request = row['request']
                uid = request.get('labels', {}).get('dbt_node_unique_id') or request.get('dbt_node_state', {}).get('node_unique_id')
                if not uid or uid in found:
                    raise Refused('baseline node identity missing or repeated')
                found[uid] = (row.get('response') or {}).get('response', {})
        return found
    built, reused = submits(first), submits(repeat)
    confirms = {row['request'].get('request_id') for row in first
                if row.get('method') == 'ConfirmExecution' and (row.get('response') or {}).get('success') is True}
    if not built or built.keys() != reused.keys():
        raise Refused('baseline first/repeat node inventories differ or are empty')
    for uid, response in built.items():
        pending = response.get('ready_to_execute')
        if pending is None or pending.get('request_id') not in confirms:
            raise Refused('baseline first build lacks accepted confirmation: ' + uid)
        if 'skip_execution' not in reused[uid]:
            raise Refused('baseline unchanged node did not reuse: ' + uid)
    return sorted(built)


def check_manifest(scope, manifest):
    """A safe connection profile cannot authorize an escaping node override."""
    for node in [*manifest.get('nodes', {}).values(), *manifest.get('sources', {}).values()]:
        if node.get('resource_type') not in ('model', 'seed', 'snapshot', 'test', 'source'):
            continue
        if node.get('config', {}).get('materialized') == 'ephemeral':
            continue
        database, schema = node.get('database'), node.get('schema')
        if database != 'cdo_demo_db' or schema not in scope.schemas:
            raise Refused('manifest relation escapes guarded catalog/schema: ' + str((database, schema)))


def status_for(message):
    text = message.lower()
    if any(x in text for x in ('permission_denied', 'insufficient_privileges', 'permission denied', 'not authorized', 'privilege')):
        return 'warehouse-permission-denied'
    if 'dbdriverfailed' in text or 'database error in' in text:
        return 'warehouse-capability-failed'
    if 'client invocation exited' in text:
        return 'client-rejected'
    if any(x in text for x in ('unsupported', 'not supported', 'feature_not_enabled')):
        return 'warehouse-capability-failed'
    return 'unverified/blocked'


class Campaign:
    def __init__(self, args):
        self.a = args
        self.scope = Scope(args.run_id)
        self.work = Path(args.work_dir).resolve()
        if self.work.exists() and any(self.work.iterdir()):
            raise Refused('work directory must be empty')
        if hashlib.sha256(Path(args.dbt).read_bytes()).hexdigest() != DIGEST:
            raise Refused('dbt binary digest does not match pinned 2.0.11')
        import yaml
        profile = yaml.safe_load(Path(args.profiles).read_text())[args.profile]
        source = profile['outputs'][profile['target']]
        if source.get('type') != 'databricks' or source.get('catalog') != 'cdo_demo_db':
            raise Refused('source profile must be Databricks with catalog cdo_demo_db')
        if any(source.get(key) for key in ('client_secret','oauth_client_secret','password','private_key','location_root')):
            raise Refused('campaign requires the existing token profile without embedded extra credentials or external storage')
        if source.get('database') not in (None, 'cdo_demo_db'):
            raise Refused('source database disagrees with guarded catalog')
        config = configparser.ConfigParser(interpolation=None)
        config.read(Path.home() / '.databrickscfg')
        token = config['DEFAULT']['token']
        cli_host = config['DEFAULT']['host'].removeprefix('https://').rstrip('/')
        if str(source.get('host', '')).removeprefix('https://').rstrip('/') != cli_host:
            raise Refused('dbt profile and Databricks CLI must use the same workspace')
        if str(source.get('http_path', '')).rsplit('/', 1)[-1] != args.warehouse:
            raise Refused('dbt profile and SQL probes must use the same warehouse')
        self.secrets = [token]
        if isinstance(source.get('token'), str) and '{{' not in source['token']:
            self.secrets.append(source['token'])
        self.env = dict(os.environ, DBT_ENV_SECRET_DATABRICKS_TOKEN=token, DATABRICKS_TOKEN=token, DATABRICKS_HOST='https://' + cli_host)
        self.work.mkdir(parents=True, exist_ok=True)
        self.events, self.results = [], []
        self.proxy = None
        self.project = self.work / 'project'
        self.profiles = self.work / 'profiles'
        self.profiles.mkdir()
        outputs = {}
        for role in ('prod', 'dev', 'ref'):
            output = dict(source, schema=self.scope.prefix + '_' + role)
            output.pop('defer_to_target', None)
            output['token'] = "{{ env_var('DBT_ENV_SECRET_DATABRICKS_TOKEN') }}"
            if role == 'dev':
                output['defer_to_target'] = 'prod'
            outputs[role] = output
        self.save('profiles/profiles.yml', {'survey': {'target': 'prod', 'outputs': outputs}})
        version = subprocess.run([args.dbt, '--version'], capture_output=True, text=True, timeout=30)
        if version.returncode or '2.0.11' not in version.stdout:
            raise Refused('pinned client did not report version 2.0.11')
        self.save('client-version.txt', version.stdout + version.stderr)
        self.save('runner.py', Path(__file__).read_text())
        self.save('configuration.json', {'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'plan': PLAN, 'run_id': args.run_id, 'warehouse_id': args.warehouse,
                                        'source_profile': args.profile, 'outputs': outputs,
                                        'dbt_binary': str(Path(args.dbt).resolve()), 'retained_schemas': sorted(self.scope.schemas)})
        self.n = 0

    def clean(self, text):
        for secret in self.secrets:
            if secret:
                text = text.replace(secret, '[REDACTED]')
        return text

    def save(self, name, value):
        path = self.work / name
        path.parent.mkdir(parents=True, exist_ok=True)
        text = value if isinstance(value, str) else json.dumps(value, indent=2)
        path.write_text(self.clean(text))

    def event(self, kind, **values):
        event = {'at': datetime.now(timezone.utc).isoformat(), 'kind': kind, **values}
        self.events.append(event)
        self.save('operations.json', self.events)
        return event

    def api(self, verb, endpoint, payload=None):
        command = ['databricks', 'api', verb, endpoint, '-o', 'json']
        if payload is not None:
            # stdin avoids shell interpolation, credentials stay in CLI's own configuration.
            command += ['--json', json.dumps(payload)]
        result = subprocess.run(command, env=self.env, capture_output=True, text=True, timeout=180)
        if result.returncode:
            raise Refused(self.clean(result.stderr or result.stdout))
        return json.loads(result.stdout)

    def sql(self, label, statement):
        # There is no arbitrary SQL CLI option: all mutation statements are generated
        # below from checked relation names. Recheck every qualified experiment name.
        for relation in re.findall(r'cdo_demo_db\.[a-z0-9_]+\.[a-z0-9_]+', statement):
            if relation == 'cdo_demo_db.information_schema.tables' and statement.startswith('SELECT '):
                if not any("table_schema='" + schema + "'" in statement for schema in self.scope.schemas):
                    raise Refused('information_schema read must identify an owned schema')
            else:
                self.scope.check(relation)
        event = self.event('sql', label=label, statement=statement)
        response = self.api('post', '/api/2.0/sql/statements', {'warehouse_id': self.a.warehouse,
                            'statement': statement, 'wait_timeout': '30s', 'disposition': 'INLINE', 'format': 'JSON_ARRAY'})
        deadline = time.monotonic() + 600
        while response.get('status', {}).get('state') in ('PENDING', 'RUNNING'):
            if time.monotonic() > deadline:
                raise Refused('warehouse statement timeout; id ' + response['statement_id'])
            time.sleep(1)
            response = self.api('get', '/api/2.0/sql/statements/' + response['statement_id'])
        event['state'] = response.get('status', {}).get('state')
        self.save(f'sql/{len(self.events):04d}_{label}.json', response)
        self.save('operations.json', self.events)
        if event['state'] != 'SUCCEEDED':
            raise Refused(json.dumps(response.get('status')))
        return complete_result(response)

    def start(self):
        # The only local process managed by this campaign is this Popen instance.
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        self.env.update(DBT_ENGINE_MANAGE_STATE='true', RUN_CACHE_API_URL=f'127.0.0.1:{port}', RUN_CACHE_API_SECURE='false')
        self.proxy_log = open(self.work / 'proxy.log', 'w')
        env = dict(self.env, PROXY_LISTEN=f'127.0.0.1:{port}', GOLDEN_DIR=str(self.work / 'golden'), PROXY_UPSTREAM='https://api.state.dbt.com:443', RUST_LOG='warn')
        self.proxy = subprocess.Popen([self.a.proxy], env=env, stdout=self.proxy_log, stderr=subprocess.STDOUT)
        for _ in range(120):
            if self.proxy.poll() is not None:
                raise Refused('owned proxy exited before listening; see proxy.log')
            with socket.socket() as sock:
                if sock.connect_ex(('127.0.0.1', port)) == 0:
                    break
            time.sleep(.25)
        else:
            raise Refused('owned proxy startup timeout')
        self.event('proxy', pid=self.proxy.pid, port=port)

    def stop(self):
        if self.proxy is not None and self.proxy.poll() is None:
            self.proxy.send_signal(signal.SIGTERM)
            try:
                self.proxy.wait(30)
            except subprocess.TimeoutExpired:
                self.proxy.kill()
                self.proxy.wait()
        if hasattr(self, 'proxy_log'):
            self.proxy_log.close()
            path = self.work / 'proxy.log'
            path.write_text(self.clean(path.read_text()))

    def wire(self):
        rows = []
        for path in sorted((self.work / 'golden').glob('*.jsonl')):
            rows += [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        return rows

    def dbt(self, label, command, target='prod', managed=True):
        self.n += 1
        args = [self.a.dbt, *command, '--target', target, '--profiles-dir', str(self.profiles), '--profile', 'survey',
                '--target-path', str(self.work / 'targets' / f'{self.n:03d}_{label}'),
                '--log-path', str(self.work / 'logs' / f'{self.n:03d}_{label}'),
                '--no-version-check', '--skip-semantic-manifest-validation', '--manage-state' if managed else '--no-manage-state']
        lo = len(self.wire())
        self.event('dbt', label=label, argv=args, target=target, managed=managed)
        print(f'dbt {self.n}: {label}', flush=True)
        try:
            result = subprocess.run(args, cwd=self.project, env=self.env, capture_output=True, text=True, timeout=1200)
            output, code = result.stdout + result.stderr, result.returncode
        except subprocess.TimeoutExpired as error:
            output = ''.join(chunk.decode(errors='replace') if isinstance(chunk, bytes) else chunk
                             for chunk in (error.stdout or '', error.stderr or ''))
            code = 124
        self.save(f'dbt/{self.n:03d}_{label}.log', output)
        # Native query logs may contain connection credentials: redact immediately.
        logdir = self.work / 'logs' / f'{self.n:03d}_{label}'
        if logdir.exists():
            for path in logdir.rglob('*'):
                if path.is_file():
                    try:
                        path.write_text(self.clean(path.read_text()))
                    except UnicodeError:
                        pass
        time.sleep(.1)
        calls = self.wire()[lo:]
        self.save(f'wire/{self.n:03d}_{label}.json', calls)
        self.event('dbt-result', label=label, returncode=code, calls=len(calls))
        if code:
            raise Refused(f'client invocation exited {code}; dbt/{self.n:03d}_{label}.log: ' + self.clean(output[-4000:]))
        return calls

    def case(self, name, function):
        print('case:', name, flush=True)
        lo = len(self.events)
        try:
            detail = function()
            result = {'case': name, 'status': detail.get('status', 'observed-success') if isinstance(detail, dict) else 'observed-success', 'detail': detail}
        except Exception as error:
            message = self.clean(str(error))
            result = {'case': name, 'status': status_for(message), 'detail': message}
        result['operations_start'] = lo
        result['operations_end'] = len(self.events)
        self.results.append(result)
        self.save('results.json', self.results)
        print(name, result['status'], str(result['detail'])[:200], flush=True)
        return result

    def init_schemas(self):
        version = self.sql('version', 'SELECT current_version() AS runtime_version, current_catalog() AS session_catalog, current_schema() AS session_schema')
        self.save('warehouse.json', version)
        for schema in sorted(self.scope.schemas):
            self.sql('create_schema', f'CREATE SCHEMA cdo_demo_db.{schema}')
        return version

    def setup_project(self, baseline=False):
        if self.project.exists():
            self.project.rename(self.work / 'baseline-project')
        if baseline:
            shutil.copytree(self.a.jaffle_shop, self.project, ignore=shutil.ignore_patterns('.git', 'target', 'logs', 'state-captures', 'dbt_internal_packages', '.venv'))
            # The source is never mutated. Preserve its project paths/configuration.
            import yaml
            project = yaml.safe_load((self.project / 'dbt_project.yml').read_text())
            previous = project['name']
            project['name'] = 'survey_' + self.a.run_id
            project['profile'] = 'survey'
            for key in ('models', 'seeds', 'snapshots', 'tests'):
                if previous in project.get(key, {}):
                    project[key][project['name']] = project[key].pop(previous)
            self.save('project/dbt_project.yml', project)
        else:
            self.project.mkdir()
            for folder in ('models', 'snapshots', 'seeds', 'macros'):
                (self.project / folder).mkdir()
            self.save('project/dbt_project.yml', {'name': 'survey_' + self.a.run_id, 'version': '1.0', 'profile': 'survey'})
            self.save('project/models/sources.yml', {'version': 2, 'sources': [{'name': 'survey', 'database': 'cdo_demo_db', 'schema': self.scope.prefix + '_raw', 'tables': [{'name': 'inputs', 'config': {'event_time': 'ts'}}]}]})

    def write_model(self, name, config, query):
        self.save(f'project/models/{name}.sql', '{{ config(' + config + ') }}\n' + query + '\n')

    def observe(self, label, relation):
        self.scope.check(relation)
        result = self.sql(label + '_rows', f'SELECT * FROM {relation}')
        self.save('physical/' + label + '.json', result)
        props = self.sql(label + '_properties', f'DESCRIBE TABLE EXTENDED {relation}')
        self.save('physical/' + label + '_properties.json', props)
        return result

    def expect_rows(self, label, relation, expected):
        actual = self.observe(label, relation)
        if sorted(actual['rows'], key=repr) != sorted(expected, key=repr):
            raise Refused('physical rows differ from literal operation oracle: ' + label)
        return actual

    def base_inputs(self):
        source = self.scope.relation('raw', 'inputs')
        self.sql('reset_inputs', f"CREATE OR REPLACE TABLE {source} (id BIGINT, v STRING, p DATE, ts TIMESTAMP) USING DELTA")
        self.sql('insert_inputs', f"INSERT INTO {source} VALUES (1,'one',DATE '2026-11-01',TIMESTAMP '2026-11-01 12:00:00'),(2,'two',DATE '2026-11-02',TIMESTAMP '2026-11-02 12:00:00')")
        return source

    def clone_diagnostic(self):
        source = self.base_inputs()
        dest = self.scope.relation('dev', 'diagnostic_clone')
        self.sql('explicit_shallow_clone', f'CREATE TABLE {dest} SHALLOW CLONE {source}')
        return self.observe('diagnostic_clone', dest)

    def baseline(self):
        self.setup_project(baseline=True)
        self.dbt('baseline_inventory', ['parse'], managed=False)
        manifest = json.loads(next((self.work / 'targets').glob('*_baseline_inventory/manifest.json')).read_text())
        check_manifest(self.scope, manifest)
        self.save('baseline-inventory.json', {key: {uid: {field: node.get(field) for field in ('resource_type','database','schema','alias')} for uid,node in manifest.get(key, {}).items()} for key in ('nodes','sources')})
        first = self.dbt('baseline_first', ['build'])
        repeat = self.dbt('baseline_repeat', ['build'])
        summary = []
        for name, calls in [('first', first), ('repeat', repeat)]:
            nodes = []
            for row in calls:
                if row.get('method') in ('SubmitEnrichedSQL', 'SubmitValues'):
                    response = row.get('response') or {}
                    nodes.append({'node': row.get('request', {}).get('labels', {}).get('dbt_node_name'), 'method': row['method'], 'response': response})
            summary.append({'run': name, 'methods': dict(Counter(r['method'] for r in calls)), 'nodes': nodes})
        self.save('baseline.json', summary)
        check_baseline(first, repeat)
        return {'summary': 'baseline.json', 'first': len(summary[0]['nodes']), 'repeat': len(summary[1]['nodes'])}

    def strategy(self, strategy):
        source = self.base_inputs()
        name = 'inc_' + strategy.replace('+', '_').replace('-', '_')
        config = f"materialized='incremental', incremental_strategy='{strategy}', file_format='delta', unique_key='id', state={{'lag_tolerance':'0s'}}"
        query = "select * from {{ source('survey','inputs') }}"
        if strategy == 'append':
            query += " {% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}"
        if strategy == 'insert_overwrite':
            config += ", partition_by=['p']"
            query += " {% if is_incremental() %} where p = DATE '2026-11-01' {% endif %}"
        if strategy == 'replace_where':
            config += ', incremental_predicates=["p = DATE \'2026-11-01\'"]'
            query += " {% if is_incremental() %} where p = DATE '2026-11-01' {% endif %}"
        if strategy == 'microbatch':
            config += ", event_time='ts', begin='2026-11-01', batch_size='day'"
        self.write_model(name, config, query)
        args = ['run', '--select', name]
        if strategy == 'microbatch':
            args += ['--event-time-start', '2026-11-01', '--event-time-end', '2026-11-03']
        first = self.dbt(name + '_first', args)
        self.observe(name + '_first', self.scope.relation('prod', name))
        self.dbt(name + '_repeat', args)
        if strategy != 'append':
            self.sql(name + '_update', f"UPDATE {source} SET v='changed' WHERE id=1")
        self.sql(name + '_insert', f"INSERT INTO {source} VALUES (3,'three',DATE '2026-11-01',TIMESTAMP '2026-11-01 13:00:00')")
        changed = self.dbt(name + '_changed', args)
        actual = self.observe(name + '_changed', self.scope.relation('prod', name))
        # Independent State-disabled target exercises actual existing-target DML,
        # even if hosted freshness policy returns a reuse decision.
        self.dbt(name + '_reference_first', args, target='ref', managed=False)
        self.sql(name + '_second_insert', f"INSERT INTO {source} VALUES (4,'four',DATE '2026-11-01',TIMESTAMP '2026-11-01 14:00:00')")
        self.dbt(name + '_reference_dml', args, target='ref', managed=False)
        reference = self.observe(name + '_reference_dml', self.scope.relation('ref', name))
        initial_value = 'one' if strategy == 'append' else 'changed'
        expected = [['1',initial_value,'2026-11-01','2026-11-01T12:00:00.000Z'],
                    ['2','two','2026-11-02','2026-11-02T12:00:00.000Z'],
                    ['3','three','2026-11-01','2026-11-01T13:00:00.000Z'],
                    ['4','four','2026-11-01','2026-11-01T14:00:00.000Z']]
        expected_schema = [('id','BIGINT'),('v','STRING'),('p','DATE'),('ts','TIMESTAMP')]
        for label, result, rows in [('hosted', actual, expected[:3]), ('reference', reference, expected)]:
            columns = [(column['name'],column['type_text']) for column in result['columns']]
            if columns != expected_schema or sorted(result['rows'], key=repr) != sorted(rows, key=repr):
                raise Refused(label + ' complete strategy result differs from declared schema/operation oracle')
        return {'target_result': actual, 'reference_result': reference, 'first_calls': len(first), 'changed_calls': len(changed)}

    def snapshot(self, strategy, hard_deletes='ignore', target='prod', managed=True):
        source = self.base_inputs()
        name = f'snap_{strategy}_{hard_deletes}'
        config = f"unique_key='id', strategy='{strategy}', hard_deletes='{hard_deletes}', state={{'lag_tolerance':'0s'}}"
        config += ", updated_at='ts'" if strategy == 'timestamp' else ", check_cols=['v']"
        self.save('project/snapshots/' + name + '.sql', '{% snapshot ' + name + ' %}\n{{ config(' + config + ') }}\nselect * from ' + source + '\n{% endsnapshot %}\n')
        args = ['snapshot', '--select', name]
        self.dbt(name + '_first', args, target=target, managed=managed)
        self.observe(name + '_first', self.scope.relation(target, name))
        self.dbt(name + '_repeat', args, target=target, managed=managed)
        self.sql(name + '_update', f"UPDATE {source} SET v='changed', ts=TIMESTAMP '2026-11-03 12:00:00' WHERE id=1")
        self.sql(name + '_insert', f"INSERT INTO {source} VALUES (3,'three',DATE '2026-11-01',TIMESTAMP '2026-11-03 13:00:00')")
        self.dbt(name + '_changed', args, target=target, managed=managed)
        changed = self.observe(name + '_changed', self.scope.relation(target, name))
        self.sql(name + '_delete', f'DELETE FROM {source} WHERE id=2')
        self.dbt(name + '_deleted', args, target=target, managed=managed)
        deleted = self.observe(name + '_deleted', self.scope.relation(target, name))
        check_snapshot(changed, hard_deletes)
        check_snapshot(deleted, hard_deletes, deleted=True)
        return {'changed': changed, 'deleted': deleted}

    def metadata(self, label, relation):
        catalog, schema, table = relation.split('.')
        info = self.sql(label + '_information_schema', f"SELECT * FROM {catalog}.information_schema.tables WHERE table_catalog='{catalog}' AND table_schema='{schema}' AND table_name='{table}'")
        history = self.sql(label + '_history', f'DESCRIBE HISTORY {relation} LIMIT 1')
        self.save('freshness/' + label + '.json', {'information_schema': info, 'history': history, 'observed_at': datetime.now(timezone.utc).isoformat()})
        return info

    def freshness(self):
        source = self.base_inputs()
        name = 'fresh_table'
        self.write_model(name, "materialized='table', state={'lag_tolerance': '0s'}", "select id,v from {{ source('survey','inputs') }}")
        self.write_model('fresh_view', "materialized='view'", "select id,v from {{ source('survey','inputs') }}")
        args = ['run', '--select', name, 'fresh_view']
        self.dbt('fresh_first', args)
        operations = {
            'insert': f"INSERT INTO {source} VALUES (3,'three',DATE '2026-11-01',TIMESTAMP '2026-11-01 13:00:00')",
            'update': f"UPDATE {source} SET v='changed' WHERE id=1",
            'delete': f'DELETE FROM {source} WHERE id=2',
            'no_op': f'UPDATE {source} SET v=v WHERE id=1',
            'add_column': f'ALTER TABLE {source} ADD COLUMN extra STRING',
            'properties': f"ALTER TABLE {source} SET TBLPROPERTIES ('survey.note'='metadata-only')",
            'replace': f"CREATE OR REPLACE TABLE {source} USING DELTA AS SELECT CAST(7 AS BIGINT) id,'replaced' v,DATE '2026-11-01' p,TIMESTAMP '2026-11-01 12:00:00' ts",
            'drop_recreate': [f'DROP TABLE {source}', f"CREATE TABLE {source} USING DELTA AS SELECT CAST(8 AS BIGINT) id,'recreated' v,DATE '2026-11-01' p,TIMESTAMP '2026-11-01 12:00:00' ts"],
        }
        for operation, sql in operations.items():
            def probe(operation=operation, sql=sql):
                before = self.metadata(operation + '_before', source)
                for statement in sql if isinstance(sql, list) else [sql]:
                    self.sql(operation, statement)
                after = self.metadata(operation + '_after', source)
                for attempt in range(3):
                    if before != after:
                        break
                    time.sleep(1)
                    after = self.metadata(operation + '_after_retry_' + str(attempt + 1), source)
                calls = self.dbt('fresh_' + operation, args)
                actual = self.observe('fresh_' + operation, self.scope.relation('prod', name))
                reference = self.sql(operation + '_reference', f'SELECT id,v FROM {source}')
                self.save('freshness/' + operation + '_comparison.json', {'actual': actual, 'reference': reference, 'equal': same_result(actual, reference)})
                if not same_result(actual, reference):
                    raise Refused('dependent physical result differs from independent SELECT')
                return {'metadata_equal': before == after, 'wire_calls': len(calls), 'physical_equal': same_result(actual, reference)}
            self.case('freshness/' + operation, probe)
        def own_target():
            target = self.scope.relation('prod', name)
            self.sql('external_target', f"UPDATE {target} SET v='externally-mutated'")
            calls = self.dbt('fresh_own_target', args)
            self.observe('fresh_own_target', target)
            return {'wire_calls': len(calls)}
        self.case('freshness/own_target', own_target)
        def view_replace():
            view = self.scope.relation('raw', 'view_input')
            self.sql('create_view', f'CREATE VIEW {view} AS SELECT id,v FROM {source}')
            self.write_model('view_dependent', "materialized='table'", 'select * from ' + view)
            self.dbt('view_input_first', ['run', '--select', 'view_dependent'])
            self.sql('replace_view', f"CREATE OR REPLACE VIEW {view} AS SELECT id,concat(v,'!') v FROM {source}")
            calls = self.dbt('view_input_changed', ['run', '--select', 'view_dependent'])
            self.observe('view_input_changed', self.scope.relation('prod', 'view_dependent'))
            return {'wire_calls': len(calls)}
        self.case('freshness/view_replace', view_replace)
        return 'See independent freshness/* case results and exact wire windows'

    def copy(self, kind):
        self.base_inputs()
        name = 'copy_' + kind
        if kind == 'seed':
            self.save('project/seeds/' + name + '.csv', 'id,v\n1,one\n2,two\n')
            command = ['seed', '--select', name]
        elif kind == 'snapshot':
            self.save('project/snapshots/' + name + '.sql', '{% snapshot ' + name + " %}\n{{ config(unique_key='id', strategy='check', check_cols=['v'], state={'lag_tolerance':'0s'}) }}\nselect * from {{ source('survey','inputs') }}\n{% endsnapshot %}")
            command = ['snapshot', '--select', name]
        else:
            config = "materialized='incremental', incremental_strategy='merge', unique_key='id', state={'lag_tolerance':'0s'}" if kind == 'incremental' else "materialized='table', state={'pre_clone':'never','lag_tolerance':'0s'}"
            self.write_model(name, config, "select * from {{ source('survey','inputs') }}")
            command = ['run', '--select', name]
        self.dbt(name + '_prod', command)
        original = self.observe(name + '_prod', self.scope.relation('prod', name))
        calls = self.dbt(name + '_dev', command, target='dev')
        copied = self.observe(name + '_dev', self.scope.relation('dev', name))
        reuse = self.dbt(name + '_repeat', command, target='dev')
        method = 'RegisterClone' if kind in ('incremental', 'snapshot') else 'SubmitValues' if kind == 'seed' else 'SubmitEnrichedSQL'
        relevant = [r for r in calls if r['method'] == method]
        self.save('copies/' + kind + '.json', {'expected_method': method, 'calls': relevant, 'source': original, 'target': copied})
        if not relevant:
            raise Refused(f'{method} absent: inspect auto-deferral/client conditions; no hosted policy conclusion')
        grants = [r for r in relevant if (r.get('response') or {}).get('ready_to_clone')
                  or (r.get('response') or {}).get('response', {}).get('ready_to_clone')]
        if not grants:
            return {'status': 'hosted-rejected', 'outcome': 'hosted-rejected-or-build', 'method': method, 'responses': [r.get('response') for r in relevant], 'physical_equal': same_result(original, copied)}
        for grant in grants:
            response = grant['response']
            ready = response.get('ready_to_clone') or response.get('response', {}).get('ready_to_clone')
            if not ready:
                raise Refused('copy grant has no usable ready_to_clone payload')
            if method == 'RegisterClone' and response.get('response', {}).get('ready_to_clone_v1') != ready:
                raise Refused('RegisterClone response mirrors differ')
            accepted = any(row['method'] == 'ConfirmExecution'
                           and row['request'].get('request_id') == ready['request_id']
                           and (row.get('response') or {}).get('success') is True for row in calls)
            if not accepted:
                raise Refused('copy grant lacks its accepted execution confirmation')
        if not any('skip_execution' in (row.get('response') or {}).get('response', {}) for row in reuse):
            raise Refused('unchanged copied target did not reuse')
        if not same_result(original, copied):
            raise Refused('copied result differs from complete source schema/rows')
        if kind == 'seed':
            self.save('project/seeds/' + name + '.csv', 'id,v\n1,changed\n2,two\n3,three\n')
        else:
            source = self.scope.relation('raw', 'inputs')
            self.sql(name + '_change_source', f"UPDATE {source} SET v='changed', ts=TIMESTAMP '2026-11-03 12:00:00' WHERE id=1")
        changed_calls = self.dbt(name + '_changed_source', command, target='dev')
        changed = self.observe(name + '_changed_source', self.scope.relation('dev', name))
        if not any('changed' in row for row in changed['rows']):
            raise Refused('copied target did not adopt changed source values')
        return {'outcome': 'copy-granted', 'method': method, 'physical_equal': True,
                'reuse_calls': len(reuse), 'changed_source_calls': len(changed_calls)}

    def seed_rebuild_diagnostic(self):
        name = 'seed_rebuild'
        command = ['seed', '--select', name]
        self.save('project/seeds/' + name + '.csv', 'id,v\n1,one\n2,two\n')
        self.dbt('seed_diag_prod_first', command)
        self.dbt('seed_diag_dev_copy', command, target='dev')
        self.dbt('seed_diag_ref_first', command, target='ref', managed=False)
        for role in ('prod', 'dev', 'ref'):
            relation = self.scope.relation(role, name)
            self.observe('seed_diag_' + role, relation)
            schema = relation.split('.')[1]
            self.sql('seed_diag_type_' + role, f"SELECT table_catalog,table_schema,table_name,table_type,data_source_format FROM cdo_demo_db.information_schema.tables WHERE table_schema='{schema}' AND table_name='{name}'")
        self.save('project/seeds/' + name + '.csv', 'id,v\n1,changed\n2,two\n3,three\n')
        self.case('seed-diagnostic/managed-rebuild', lambda: self.dbt('seed_diag_dev_changed', command, target='dev'))
        time.sleep(3)
        self.case('seed-diagnostic/managed-retry', lambda: self.dbt('seed_diag_dev_retry', command, target='dev'))
        self.case('seed-diagnostic/no-state-clone-rebuild', lambda: self.dbt('seed_diag_dev_no_state', command, target='dev', managed=False))
        self.case('seed-diagnostic/no-state-managed-reference', lambda: self.dbt('seed_diag_ref_changed', command, target='ref', managed=False))
        for role in ('dev', 'ref'):
            self.observe('seed_diag_' + role + '_after', self.scope.relation(role, name))
        return 'See seed-diagnostic/* outcomes; failed copied-target rebuilds are retained separately'

    def run(self):
        try:
            self.start()
            if self.case('environment', self.init_schemas)['status'] != 'observed-success':
                return 1
            if 'diagnostic' in self.a.sections:
                self.case('explicit-clone-diagnostic', self.clone_diagnostic)
            if 'baseline' in self.a.sections:
                self.case('jaffle-shop-baseline', self.baseline)
            self.setup_project()
            for kind in (('incremental', 'snapshot', 'table', 'seed') if 'copies' in self.a.sections else ()):
                self.case('copy/' + kind, lambda kind=kind: self.copy(kind))
            for strategy in (STRATEGIES if 'incrementals' in self.a.sections else ()):
                self.case('incremental/' + strategy, lambda strategy=strategy: self.strategy(strategy))
            for strategy in (('timestamp', 'check') if 'snapshots' in self.a.sections else ()):
                for hard_deletes in ('ignore', 'invalidate', 'new_record'):
                    self.case('snapshot/' + strategy + '/' + hard_deletes, lambda strategy=strategy, mode=hard_deletes: self.snapshot(strategy, mode))
            if 'snapshot-reference' in self.a.sections:
                for strategy in ('timestamp', 'check'):
                    self.case('snapshot-reference/' + strategy + '/new_record',
                              lambda strategy=strategy: self.snapshot(strategy, 'new_record', target='ref', managed=False))
            if 'freshness' in self.a.sections:
                self.case('freshness', self.freshness)
            if 'seed-rebuild-diagnostic' in self.a.sections:
                self.case('seed-rebuild-diagnostic', self.seed_rebuild_diagnostic)
        finally:
            self.stop()
            certified = []
            for path in sorted((self.work / 'golden').glob('*.jsonl')):
                try:
                    rows = verify_capture(path)
                    certified.append({'path': str(path.relative_to(self.work)), 'complete': True, 'rows': len(rows), 'methods': dict(Counter(r['method'] for r in rows))})
                except Exception as error:
                    certified.append({'path': str(path.relative_to(self.work)), 'complete': False, 'error': str(error)})
            self.save('capture-certification.json', certified)
            self.save('summary.json', {'plan': PLAN, 'cases': self.results, 'invocations': self.n, 'capture': certified})
        return 0 if self.results and all(r['status'] == 'observed-success' for r in self.results) and certified and all(c['complete'] for c in certified) else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', action='store_true', help='print required inventory; no credentials or live calls')
    parser.add_argument('--sections', nargs='+', choices=['diagnostic','baseline','copies','incrementals','snapshots','freshness','seed-rebuild-diagnostic','snapshot-reference'], default=['diagnostic','baseline','copies','incrementals','snapshots','freshness'])
    parser.add_argument('--dbt')
    parser.add_argument('--proxy')
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--jaffle-shop', default=str(Path.home() / 'projects/jaffle-shop-databricks'))
    parser.add_argument('--work-dir')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    args = parser.parse_args()
    if args.plan:
        print(json.dumps(PLAN))
        return 0
    if not all((args.dbt, args.proxy, args.work_dir)):
        parser.error('--dbt, --proxy and --work-dir required for live execution')
    try:
        return Campaign(args).run()
    except (Refused, OSError, ValueError) as error:
        print('REFUSED:', error, file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
