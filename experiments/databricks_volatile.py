#!/usr/bin/env python3
"""#198: opt-in pinned native Databricks Volatile evaluation captures.

Uses existing evidenced schema containers; never creates schemas. Three fresh
UUID-named tables and a passing singular data test are built twice, explained
with the native client, and independently read through the SQL API. Values
rendered by State remain telemetry; the Warehouse executes the original SQL.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import uuid

from databricks_baseline import Baseline, LeaseScope, check_inventory
from databricks_survey import Campaign, Refused, check_manifest


def check_physical(actual, properties, name, run_id):
    expected = [('id', 'INT', 'INT')]
    if name != 'control':
        expected.append(('value', 'DOUBLE', 'DOUBLE'))
    expected.append(('run_id', 'STRING', 'STRING'))
    schema = [(c['name'], c['type_name'], c['type_text']) for c in actual['columns']]
    if (schema != expected or len(actual['rows']) != 1 or actual['rows'][0][0] != '1'
            or actual['rows'][0][-1] != run_id):
        raise Refused('volatile: physical rows or schema')
    if name != 'control' and not 0 <= float(actual['rows'][0][1]) < 1:
        raise Refused('volatile: Warehouse RAND value outside range')
    props = {r[0]: r[1] for r in properties['rows']}
    if props.get('Type') != 'MANAGED' or props.get('Provider', '').lower() != 'delta':
        raise Refused('volatile: physical relation properties')


class VolatileScope(LeaseScope):
    profile_roles = ('prod',)

    def __init__(self, run, ledger, object_id):
        super().__init__(run, ledger)
        if uuid.UUID(object_id).version != 4 or uuid.UUID(object_id).hex != object_id:
            raise Refused('volatile: expected UUID4 object identity')
        self.relation_prefix = 'p198_' + object_id + '_'
        self.allowed = {f'cdo_demo_db.{self.schema("prod")}.{self.relation_prefix}{name}'
                        for name in ('volatile', 'control', 'consumer')}


class Volatile(Campaign):
    inventory = Baseline.inventory
    run = Baseline.run

    def __init__(self, args):
        self.lease = json.loads(Path(args.schema_lease).read_text())
        scope = VolatileScope(args.run_id, self.lease, args.object_id)
        super().__init__(args, scope)
        self.save('volatile-runner.py', Path(__file__).read_text())
        self.save('schema-lease.json', self.lease)
        self.save('runner-provenance.json', {
            name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
            for name in ('databricks_volatile.py', 'databricks_baseline.py', 'databricks_survey.py')})

    def init_schemas(self):
        for source in self.lease['source_evidence']:
            evidence = json.loads(Path(source['path']).read_text())
            for schema in self.scope.schemas:
                recorded = evidence[schema]
                current = self.api('get', '/api/2.1/unity-catalog/schemas/cdo_demo_db.' + schema)
                if any(current.get(k) != recorded[k] for k in ('full_name', 'owner', 'schema_id')):
                    raise Refused('volatile: schema ownership changed')
        self.before_inventory = self.inventory()
        check_inventory(self.scope, self.lease, self.before_inventory)
        if len(self.before_inventory[self.scope.schema('prod')]) + 4 > 100:
            raise Refused('volatile: insufficient schema capacity')
        self.save('lease-inventory-before.json', self.before_inventory)
        self.save('warehouse.json', self.sql('version', 'SELECT current_version() AS runtime_version'))

    def model(self, name, sql, state):
        alias = self.scope.relation_prefix + name
        self.save('project/models/' + name + '.sql',
                  "{{ config(materialized='table', file_format='delta', alias='" + alias +
                  "', state=" + state + ") }}\n" + sql + '\n')

    def explain(self, label, target_path, submits):
        # Campaign.dbt gives each call separate artifacts. Native Explain must
        # instead read the preceding build's run_results from its target path.
        import subprocess
        lo = len(self.wire())
        self.n += 1
        response_logs = list((self.work / 'logs' / target_path.name / 'run_cache').glob('responses_*.jsonl'))
        if len(response_logs) != 1:
            raise Refused('volatile: expected exactly one native build response log')
        command = [self.a.dbt, 'state', 'explain', '--verbose', '--target', 'prod',
                   '--profile', 'survey', '--profiles-dir', str(self.profiles),
                   '--log-file', str(response_logs[0]),
                   '--target-path', str(target_path), '--log-path', str(self.work / 'logs' / label)]
        self.event('dbt', label=label, argv=command, managed=True)
        try:
            result = subprocess.run(command, cwd=self.project, env=self.env,
                                    capture_output=True, text=True, timeout=180)
            self.save('dbt/' + label + '.log', result.stdout + result.stderr)
        except subprocess.TimeoutExpired as error:
            output = ''.join(chunk.decode(errors='replace') if isinstance(chunk, bytes) else chunk
                             for chunk in (error.stdout or '', error.stderr or ''))
            self.save('dbt/' + label + '.log', output)
            self.event('dbt-result', label=label, returncode=124)
            raise Refused('volatile: native Explain timed out; see dbt/' + label + '.log') from error
        finally:
            for path in (self.work / 'logs' / label).rglob('*'):
                if path.is_file():
                    path.write_text(self.clean(path.read_text()))
        self.event('dbt-result', label=label, returncode=result.returncode)
        if result.returncode:
            raise Refused('volatile: native Explain failed; see dbt/' + label + '.log')
        explained = [r for r in self.wire()[lo:] if r['method'] == 'GetExplainMessages']
        expected = {next(iter(r['response']['response'].values()))['execution_decision_id'] for r in submits}
        messages = [m for row in explained for m in row['response']['messages']]
        if {m['execution_decision_id'] for m in messages} != expected or len(messages) != len(expected):
            raise Refused('volatile: native Explain omitted a decision')
        self.save('wire/' + label + '.json', explained)

    def baseline(self):
        self.setup_project()
        (self.project / 'models/sources.yml').unlink()
        # Even a non-storing test defaults to <target>_dbt_test__audit in
        # its manifest. Keep every node in the evidenced existing container.
        self.save('project/macros/generate_schema_name.sql',
                  '{% macro generate_schema_name(custom_schema_name, node) %}'
                  '{{ target.schema }}{% endmacro %}\n')
        nonce = self.scope.relation_prefix
        self.model('volatile', f"select 1 as id, rand() as value, '{nonce}' as run_id",
                   "{'evaluate_volatile_sql': true, 'pre_clone': 'never'}")
        self.model('control', f"select 1 as id, '{nonce}' as run_id", "{'pre_clone': 'never'}")
        self.model('consumer', "select id, value, run_id from {{ ref('volatile') }}",
                   "{'lag_tolerance': '3600s', 'pre_clone': 'never'}")
        test_name = self.scope.relation_prefix + 'test'
        self.save('project/tests/' + test_name + '.sql',
                  "{{ config(state={'evaluate_volatile_sql': true}) }}\n"
                  "select id, rand() as value from {{ ref('control') }} where id > 100\n")
        self.dbt('inventory', ['parse'], managed=False)
        manifest = json.loads(next((self.work / 'targets').glob('*_inventory/manifest.json')).read_text())
        check_manifest(self.scope, manifest)
        for node in manifest['nodes'].values():
            if node['resource_type'] == 'model':
                self.scope.check('.'.join(node[k] for k in ('database', 'schema', 'alias')))
        previous = None
        for label in ('first', 'rerun'):
            calls = self.dbt(label, ['build'])
            target_path = self.work / 'targets' / f'{self.n:03d}_{label}'
            submits = [r for r in calls if r['method'] == 'SubmitEnrichedSQL']
            if len(submits) != 4:
                raise Refused('volatile: expected four ordinary submits')
            confirms = {r['request']['request_id'] for r in calls
                        if r['method'] == 'ConfirmExecution' and r['response'].get('success')}
            for row in submits:
                response = row['response']['response']
                if 'ready_to_execute' in response and response['ready_to_execute']['request_id'] not in confirms:
                    raise Refused('volatile: missing successful confirmation')
                if label == 'first' and 'ready_to_execute' not in response:
                    raise Refused('volatile: first build unexpectedly reused history')
            self.explain(label + '_explain', target_path, submits)
            physical = {}
            for name in ('volatile', 'control', 'consumer'):
                physical[name] = self.observe(label + '_' + name, self.scope.relation('prod', name))
                properties = json.loads((self.work / 'physical' / (label + '_' + name + '_properties.json')).read_text())
                check_physical(physical[name], properties, name, nonce)
            for name in ('volatile', 'consumer'):
                own = next(r for r in submits if r['request']['labels']['dbt_node_name'] == name)
                expected = physical['volatile'] if 'ready_to_execute' in own['response']['response'] else previous[name]
                if physical[name]['rows'] != expected['rows']:
                    raise Refused('volatile: physical consumer does not reflect its build or reuse')
            self.save('checks/' + label + '.json', {'warehouse_values': physical,
                      'test': 'passing singular test writes no Target relation; result is in run_results.json',
                      'decisions': {r['request']['labels']['dbt_node_name']: r['response'] for r in submits}})
            previous = physical


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dbt', required=True)
    parser.add_argument('--proxy', required=True)
    parser.add_argument('--rust-url', help='opt-in owned loopback Rust server, http://127.0.0.1:PORT')
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--schema-lease', required=True)
    parser.add_argument('--work-dir', required=True)
    parser.add_argument('--object-id', default=uuid.uuid4().hex)
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + uuid.uuid4().hex[:6])
    args = parser.parse_args()
    if args.rust_url and not re.fullmatch(r'http://127\.0\.0\.1:\d+', args.rust_url):
        parser.error('--rust-url must be an owned loopback endpoint')
    return Volatile(args).run()


if __name__ == '__main__':
    raise SystemExit(main())
