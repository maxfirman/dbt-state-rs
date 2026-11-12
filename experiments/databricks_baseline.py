#!/usr/bin/env python3
"""Opt-in #135 table/view lifecycle through hosted or Rust State and real dbt.

Each phase has fresh run-owned relations and its own certified capture. Run hosted
first, promote/replay its complete fixture, then run --rust-url against the
production router. Reference dbt runs always disable State and own their project,
artifacts and relations. Neither expected rows nor schemas come from State.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from capture_integrity import verify_capture
from databricks_survey import Campaign, Refused, Scope, check_manifest, same_result


class LeaseScope(Scope):
    """Only fresh named relations inside explicitly evidenced owned containers."""
    profile_roles = ('prod', 'ref')

    def __init__(self, run, ledger):
        super().__init__(run)
        roles = ledger.get('roles', {})
        if (ledger.get('catalog') != 'cdo_demo_db' or set(roles) != {'prod','ref'}
                or len(set(roles.values())) != 2):
            raise Refused('lease must name two separate owned demo schemas')
        owned = set()
        for source in ledger.get('source_evidence', []):
            path = Path(source['path'])
            if hashlib.sha256(path.read_bytes()).hexdigest() != source['sha256']:
                raise Refused('lease ownership evidence digest changed')
            owned.update(json.loads(path.read_text())['retained_schemas'])
        import re
        if any(schema not in owned or not re.fullmatch(r'ds_\d{8}[a-f0-9]{6}_ref', schema)
               for schema in roles.values()):
            raise Refused('lease schema is not an evidenced run-owned reference schema')
        self.roles = dict(roles)
        self.schemas = set(roles.values())
        self.relation_prefix = 'b_' + run + '_'
        self.allowed = {f'cdo_demo_db.{schema}.{self.relation_prefix}literal_{kind}'
                        for schema in self.schemas for kind in ('table','view')}

    def schema(self, role):
        return self.roles[role]

    def relation(self, role, name):
        value = f'cdo_demo_db.{self.schema(role)}.{self.relation_prefix}{name}'
        return self.check(value)

    def check(self, value):
        if value.replace('`','') not in self.allowed:
            raise Refused('relation is outside the exact fresh lease allowlist: ' + value)
        return value


def check_inventory(scope, ledger, current):
    for schema in scope.schemas:
        objects = current.get(schema)
        if objects is None:
            raise Refused('lease inventory missing schema')
        if any(row['full_name'] in scope.allowed or row['name'].startswith(scope.relation_prefix) for row in objects):
            raise Refused('run-prefixed lease relation already exists')
        expected = ledger.get('preexisting_objects', {}).get(schema)
        if expected is None or any(row not in objects for row in expected):
            raise Refused('lease preexisting inventory changed')


def query(revision, identity=None):
    amount = 10 if revision == 0 else 17
    tag = 'one' if identity is None else 'one_' + identity
    return (f"select cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag "
            f"from values (1,{amount},'{tag}'),(1,{amount},'{tag}'),(2,cast(null as int),cast(null as string)) "
            "as declared(id,amount,tag)")


def assert_physical(actual, properties, revision, kind, identity=None):
    expected_schema = [('id', 'BIGINT', 'LONG'), ('amount', 'INT', 'INT'), ('tag', 'STRING', 'STRING')]
    schema = [(c.get('name'), c.get('type_text'), c.get('type_name')) for c in actual['columns']]
    if schema != expected_schema:
        raise Refused('physical:schema')
    amount = '10' if revision == 0 else '17'
    tag = 'one' if identity is None else 'one_' + identity
    expected = [['1',amount,tag], ['1',amount,tag], ['2',None,None]]
    if Counter(map(tuple, actual['rows'])) != Counter(map(tuple, expected)):
        raise Refused('physical:rows')
    props = {row[0]: row[1] for row in properties['rows']}
    if props.get('Type') != ('MANAGED' if kind == 'table' else 'VIEW'):
        raise Refused('physical:relation-kind')
    if kind == 'table' and props.get('Provider', '').lower() != 'delta':
        raise Refused('physical:provider')


def decisions(calls, expected):
    submits = [r for r in calls if r['method'] == 'SubmitEnrichedSQL']
    if len(submits) != 2 or {r['request']['labels']['dbt_node_name'] for r in submits} != {'literal_table','literal_view'}:
        raise Refused('baseline must submit exactly the two declared models')
    confirms = {r['request']['request_id'] for r in calls if r['method'] == 'ConfirmExecution'
                and (r.get('response') or {}).get('success') is True}
    for row in submits:
        response = row['response']['response']
        if expected not in response:
            raise Refused('expected ' + expected + ' for ' + row['request']['labels']['dbt_node_name'])
        if expected == 'ready_to_execute' and response[expected]['request_id'] not in confirms:
            raise Refused('build lacks successful confirmation')
    return submits


class Baseline(Campaign):
    def __init__(self, args):
        self.query_identity = args.query_identity or args.run_id
        Scope(self.query_identity)  # Restrict the literal to the run-ID alphabet.
        self.lease = json.loads(Path(args.schema_lease).read_text()) if args.schema_lease else None
        super().__init__(args, LeaseScope(args.run_id, self.lease) if self.lease else None)
        self.save('baseline-runner.py', Path(__file__).read_text())
        configuration = json.loads((self.work / 'configuration.json').read_text())
        configuration['baseline'] = {
            'entrypoint_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'proxy_sha256': hashlib.sha256(Path(args.proxy).read_bytes()).hexdigest(),
            'probe_sha256': hashlib.sha256(Path(args.probe).read_bytes()).hexdigest(),
            'backend': 'rust' if args.rust_url else 'hosted',
            'rust_url': args.rust_url,
            'query_identity': self.query_identity,
        }
        self.save('configuration.json', configuration)

    def write_model(self, name, config, sql):
        path = self.project.relative_to(self.work) / 'models' / (name + '.sql')
        if self.lease:
            config += ", alias='" + self.scope.relation_prefix + name + "'"
        self.save(str(path), '{{ config(' + config + ') }}\n' + sql + '\n')

    def init_schemas(self):
        version = self.sql('version', 'SELECT current_version() AS runtime_version, current_catalog() AS session_catalog, current_schema() AS session_schema')
        self.save('warehouse.json', version)
        if self.lease:
            inventory = self.inventory()
            check_inventory(self.scope, self.lease, inventory)
            self.save('schema-lease.json', self.lease)
            self.save('lease-inventory-before.json', inventory)
            self.before_inventory = inventory
        else:
            # This slice needs no raw/dev/audit schemas; conserve serverless quota.
            for role in ('prod', 'ref'):
                self.sql('create_schema', f'CREATE SCHEMA cdo_demo_db.{self.scope.schema(role)}')
        return version

    def inventory(self):
        inventory = {}
        for schema in sorted(self.scope.schemas):
            endpoint = '/api/2.1/unity-catalog/tables?catalog_name=cdo_demo_db&schema_name=' + schema
            response = self.api('get', endpoint)
            if response.get('next_page_token'):
                raise Refused('lease inventory is paginated; refusing incomplete ownership check')
            inventory[schema] = [{'name':row['name'], 'full_name':row['full_name'], 'table_type':row['table_type']}
                                 for row in response.get('tables', [])]
        return inventory

    def explain(self, label, submits):
        ids = [next(iter(r['response']['response'].values()))['execution_decision_id'] for r in submits]
        env = dict(self.env, PROBE_URL='http://' + self.env['RUN_CACHE_API_URL'])
        lo = len(self.wire())
        command = [self.a.probe, 'explain']
        result = subprocess.run(command, input=json.dumps({'execution_decision_ids': ids}),
                                env=env, text=True, capture_output=True, timeout=60)
        self.save('explain/' + label + '.json', {'argv': command, 'request_ids': ids,
                  'returncode': result.returncode, 'stdout': result.stdout, 'stderr': result.stderr})
        if result.returncode:
            raise Refused('Explain failed: ' + self.clean(result.stderr))
        time.sleep(.1)
        calls = self.wire()[lo:]
        responses = [r for r in calls if r['method'] == 'GetExplainMessages']
        if len(responses) != 1:
            raise Refused('Explain was not captured exactly once')
        messages = responses[0]['response']['messages']
        if ({value['execution_decision_id'] for value in messages} != set(ids)
                or len(messages) != len(ids) or any(not value.get('explain_lines') for value in messages)):
            raise Refused('Explain omitted a decision or its complete tree')
        self.save('wire/' + label + '_explain.json', calls)

    def physical(self, label, target, revision):
        results = {}
        for kind in ('table', 'view'):
            relation = self.scope.relation(target, 'literal_' + kind)
            actual = self.observe(label + '_' + kind, relation)
            properties = json.loads((self.work / 'physical' / (label + '_' + kind + '_properties.json')).read_text())
            assert_physical(actual, properties, revision, kind, self.query_identity)
            results[kind] = actual
        return results

    def baseline(self):
        self.setup_project()
        # Literal workloads declare no sources; leased containers must never
        # acquire the survey's unused raw-schema source declaration.
        (self.project / 'models' / 'sources.yml').unlink()
        base = self.project
        reference = self.work / 'reference-project'
        shutil.copytree(base, reference)
        self.save('operation-manifest.json', {'version': 1, 'backend': 'rust' if self.a.rust_url else 'hosted',
                  'query_identity':self.query_identity,
                  'steps': [{'id': label, 'query_revision': rev, 'decision': decision}
                            for label, rev, decision in [('first',0,'ready_to_execute'),('unchanged',0,'skip_execution'),
                                                        ('changed',1,'ready_to_execute'),('changed_unchanged',1,'skip_execution')]],
                  'relations': {role: {kind: self.scope.relation(role, 'literal_' + kind) for kind in ('table','view')}
                                for role in ('prod','ref')},
                  'required_properties': {'table': {'Type':'MANAGED','Provider':'delta'}, 'view': {'Type':'VIEW'}}})
        for label, rev, expected in [('first',0,'ready_to_execute'),('unchanged',0,'skip_execution'),
                                     ('changed',1,'ready_to_execute'),('changed_unchanged',1,'skip_execution')]:
            for project in (base, reference):
                self.project = project
                for kind in ('table','view'):
                    self.write_model('literal_' + kind, "materialized='" + kind + "', file_format='delta', state={'lag_tolerance':'0s','pre_clone':'never'}", query(rev, self.query_identity))
            self.project = base
            if label == 'first':
                self.dbt('inventory', ['parse'], managed=False)
                manifest = json.loads(next((self.work / 'targets').glob('*_inventory/manifest.json')).read_text())
                check_manifest(self.scope, manifest)
                for node in manifest.get('nodes', {}).values():
                    self.scope.check('.'.join([node['database'], node['schema'], node['alias']]))
            calls = self.dbt(label, ['run'])
            submits = decisions(calls, expected)
            self.explain(label, submits)
            actual = self.physical(label, 'prod', rev)
            self.project = reference
            if self.dbt('reference_' + label, ['run'], target='ref', managed=False):
                raise Refused('State-disabled reference unexpectedly sent State RPCs')
            reference_results = self.physical('reference_' + label, 'ref', rev)
            independent = self.sql('reference_select_' + label, query(rev, self.query_identity).replace('select ', 'SELECT ', 1))
            for kind in ('table','view'):
                if not same_result(actual[kind], reference_results[kind]) or not same_result(actual[kind], independent):
                    raise Refused('reference:rows-or-schema')
            self.save('checks/' + label + '.json', {'revision':rev, 'decision':expected,
                      'complete_typed_rows_schema_properties':True, 'independent_dbt_and_select':True})
        # A real bad target must be rejected with the precise data mismatch,
        # never a network/auth/SQL failure counted as mutation sensitivity.
        table = self.scope.relation('prod','literal_table')
        self.sql('mutation_wrong_result', f'UPDATE {table} SET amount=999 WHERE id=1')
        try:
            self.physical('mutation_wrong_result', 'prod', 1)
        except Refused as error:
            if str(error) != 'physical:rows':
                raise
            self.save('checks/mutation.json', {'expected_failure':'physical:rows','observed_failure':str(error)})
        else:
            raise Refused('oracle accepted a corrupted live target')
        return {'steps':4, 'models':2, 'references':'State-disabled dbt plus independent SELECT', 'mutation':'physical:rows'}

    def run(self):
        certified = []
        success = False
        failure = None
        try:
            self.init_schemas()
            self.start()
            self.baseline()
            success = True
        except Exception as error:
            failure = self.clean(str(error))
        finally:
            self.stop()
            for path in sorted((self.work / 'golden').glob('*.jsonl')):
                try:
                    rows = verify_capture(path)
                    certified.append({'path':str(path.relative_to(self.work)), 'complete':True,
                                      'rows':len(rows), 'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
                                      'methods':dict(Counter(r['method'] for r in rows))})
                except Exception as error:
                    certified.append({'path':str(path.relative_to(self.work)), 'complete':False, 'error':self.clean(str(error))})
            if self.lease and hasattr(self, 'before_inventory'):
                try:
                    after = self.inventory()
                    self.save('lease-inventory-after.json', after)
                    if any(row not in after[schema] for schema, rows in self.before_inventory.items() for row in rows):
                        raise Refused('preexisting leased objects were not preserved')
                except Exception as error:
                    failure = (failure + '; ' if failure else '') + self.clean(str(error))
            success = success and failure is None and bool(certified) and all(row['complete'] for row in certified)
            self.save('capture-certification.json', certified)
            self.save('summary.json', {'success':success, 'failure':failure, 'invocations':self.n, 'capture':certified})
        if not success:
            raise Refused(failure or 'missing or incomplete certified capture')
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dbt', required=True)
    parser.add_argument('--proxy', required=True)
    parser.add_argument('--probe', required=True)
    parser.add_argument('--rust-url', help='opt-in insecure production-router endpoint, http://127.0.0.1:PORT')
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--work-dir', required=True)
    parser.add_argument('--schema-lease', help='explicit ownership/inventory ledger for existing run-owned schema containers')
    parser.add_argument('--query-identity', help='shared fresh literal identity for equivalent hosted/Rust phases; defaults to run ID')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    args = parser.parse_args()
    if args.rust_url:
        import re
        if not re.fullmatch(r'http://127\.0\.0\.1:\d+', args.rust_url):
            parser.error('--rust-url must be an owned loopback endpoint')
    try:
        return Baseline(args).run()
    except Exception as error:
        print('FAILED:', str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
