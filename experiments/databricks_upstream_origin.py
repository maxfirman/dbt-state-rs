#!/usr/bin/env python3
"""Run a native dbt 2.0.11 upstream-origin case against hosted State (#238)."""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import shutil
import uuid

import yaml

from capture_integrity import verify_capture
from databricks_survey import Campaign, DIGEST, Refused


def decision_rpcs(calls):
    return [{key: call[key] for key in ('service', 'method', 'recorded_at', 'request', 'response',
                                        'status', 'response_metadata')}
            for call in calls if call['method'] not in ('ValidateClientVersion', 'SubmitTelemetryBatch')]


def ordinary_submit(step):
    submits = [rpc for rpc in step['rpcs'] if rpc['method'] == 'SubmitEnrichedSQL']
    if len(submits) != 1:
        raise Refused('expected one ordinary SQL submit for ' + step['relation'])
    return submits[0]


def compare_reference(record):
    steps = record['steps']
    reference = record['reference']
    reference_sql = reference['sql'].removesuffix(';').strip()
    labels = ('dev_downstream', 'dev_downstream_repeat')
    marker = record['case'] + ('_dev' if record['case'] == 'D' else '_prod')
    if (reference['rpc_count'] or any(ordinary_submit(steps[label])['request']['sql'].strip() != reference_sql
                                      for label in labels)
            or reference['rows'] != [{'marker': marker}]):
        raise Refused('State-disabled reference must execute the dev SELECT and return its expected marker')
    rows = [[row['marker']] for row in reference['rows']]
    return {label: steps[label]['physical']['rows'] == rows for label in labels}


class OriginScope:
    profile_roles = ('prod', 'dev')

    def __init__(self, case, schemas):
        if len(set(schemas.values())) != 2 or not all(
                re.fullmatch('[a-z][a-z0-9_]*', name) for name in schemas.values()):
            raise Refused('supply two distinct demo schema names')
        self.roles = schemas
        self.schemas = set(schemas.values())
        self.prefix = f'p238_{case.lower()}_{uuid.uuid4().hex}'
        self.allowed = {self.relation(role, node) for role in schemas for node in ('raw', 'upstream', 'downstream')}

    def schema(self, role):
        return self.roles[role]

    def relation(self, role, node):
        return f'cdo_demo_db.{self.schema(role)}.{self.prefix}_{node}'

    def check(self, value):
        if value not in self.allowed:
            raise Refused('unexpected relation: ' + value)
        return value


class Origin(Campaign):
    def __init__(self, args):
        scope = OriginScope(args.case, {'prod': args.prod_schema, 'dev': args.dev_schema})
        args.run_id = scope.prefix
        super().__init__(args, scope)
        self.profile = json.loads((self.profiles / 'profiles.yml').read_text())
        self.record = {'case': args.case, 'prefix': scope.prefix, 'schemas': scope.roles,
                       'client': '2.0.11', 'client_sha256': DIGEST, 'steps': {}}

    def configure(self, dev_copies):
        for role, output in self.profile['survey']['outputs'].items():
            output['allow_clones'] = role == 'dev' and dev_copies
            output['threads'] = 1
        self.save('profiles/profiles.yml', self.profile)

    def setup(self):
        scope = self.scope
        # Check the existing schemas and the fresh names before dbt can replace a target.
        for role, schema in scope.roles.items():
            self.api('get', '/api/2.1/unity-catalog/schemas/cdo_demo_db.' + schema)
            names = ','.join("'" + scope.relation(role, node).split('.')[-1] + "'"
                             for node in ('raw', 'upstream', 'downstream'))
            query = ("SELECT table_name FROM cdo_demo_db.information_schema.tables "
                     f"WHERE table_schema='{schema}' AND table_name IN ({names})")
            before = self.sql(role + '_before', query)
            self.record.setdefault('initial_inventory', {})[role] = {'sql': query, 'rows': before['rows']}
            if before['rows']:
                raise Refused('case names already exist')
        self.save('project/dbt_project.yml', {'name': scope.prefix, 'version': '1.0', 'profile': 'survey'})
        self.save('project/macros/generate_schema_name.sql',
                  '{% macro generate_schema_name(custom_schema_name, node) -%}\n'
                  '{{ custom_schema_name or target.schema }}\n{%- endmacro %}\n')
        config = "materialized='table', file_format='delta', state={'pre_clone':'never','lag_tolerance':'0s'}"
        prod_schema = scope.schema('prod')
        self.save('project/models/sources.yml', {'version': 2, 'sources': [
            {'name': 'inputs', 'database': 'cdo_demo_db', 'schema': prod_schema,
             'tables': [{'name': 'raw', 'identifier': scope.prefix + '_raw'}]}]})
        upstream_schema = ', schema=' + repr(prod_schema) if self.a.case == 'A' else ''
        self.write_model('upstream', config + f", alias='{scope.prefix}_upstream'" + upstream_schema,
                         "select * from {{ source('inputs', 'raw') }}")
        self.write_model('downstream', config + f", alias='{scope.prefix}_downstream'",
                         "select * from {{ ref('upstream') }}")
        query = (f"CREATE TABLE {scope.relation('prod', 'raw')} USING DELTA "
                 f"AS SELECT '{self.a.case}_prod' AS marker")
        self.sql('prod_raw', query)
        self.record['raw_setup'] = {'prod': query}

    def managed(self, role, node, *, repeat=False, copies=False):
        label = f'{role}_{node}' + ('_repeat' if repeat else '')
        self.configure(copies)
        calls = self.dbt(label, ['run', '--select', node], target=role)
        relation = self.scope.relation(role, node)
        rows = self.sql(label + '_rows', 'SELECT * FROM ' + relation)
        history = self.sql(label + '_history', 'DESCRIBE HISTORY ' + relation + ' LIMIT 1')
        latest = dict(zip([c['name'] for c in history['columns']], history['rows'][0]))
        decisions = decision_rpcs(calls)
        manifest = self.work / 'targets' / f'{self.n:03d}_{label}' / 'manifest.json'
        state = json.loads(manifest.read_text())['nodes'][f'model.{self.scope.prefix}.{node}']['config']['state']
        step = {'rpc_counts': dict(Counter(c['method'] for c in calls)), 'rpcs': decisions,
                'relation': relation, 'model_state': state, 'physical': rows,
                'delta': {k: latest[k] for k in ('version', 'timestamp', 'operation')}}
        self.record['steps'][label] = step
        self.save('evidence.json', self.record)
        if any(c['method'] == 'RegisterClone' or c['status']['code'] for c in decisions):
            raise Refused('direct pre-copy or unsuccessful RPC: ' + label)
        submit = ordinary_submit(step)
        req = submit['request']
        source_role = 'prod' if self.a.case == 'A' or node == 'upstream' else role
        source = self.scope.relation(source_role, 'raw' if node == 'upstream' else 'upstream')
        inputs = {t['name'].replace('`', '') for t in req['tables']}
        if inputs != ({source, relation} if repeat else {source}):
            raise Refused('unexpected physical inputs or target existence: ' + label)
        if (req['target_table'].replace('`', '') != relation or req['dialect'] != 'databricks'
                or req['execution_type'] != 1 or req['freshness_tolerance_seconds'] != 0
                or req['allow_clones'] != copies or req['clone_chain_depth_limit'] != int(copies)
                or state['pre_clone'] != 'never'):
            raise Refused('unexpected submit target or policy: ' + label)
        variant, value = next(iter(submit['response']['response'].items()))
        confirms = [c for c in decisions if c['method'] == 'ConfirmExecution']
        if variant != 'skip_execution':
            if (len(confirms) != 1 or not confirms[0]['response']['success']
                    or confirms[0]['request']['request_id'] != value['request_id']):
                raise Refused('missing correlated confirmation: ' + label)
        if variant == 'ready_to_clone' and value['clone_source'].replace('`', '') != self.scope.relation('prod', node):
            raise Refused('copy grant named an unexpected source: ' + label)
        if role == 'prod' or node == 'upstream':
            want_copy = role == 'dev' and self.a.case == 'B'
            expected = 'ready_to_clone' if want_copy else 'ready_to_execute'
            marker = self.a.case + ('_dev' if role == 'dev' and self.a.case == 'D' else '_prod')
            if (variant != expected or confirms[0]['request']['failed_to_clone']
                    or rows['rows'] != [[marker]]):
                raise Refused('upstream history or production build did not establish setup: ' + label)
        return variant

    def reference(self):
        command = ['show', '--select', 'downstream', '--output', 'json', '--limit', '-1', '--quiet']
        calls = self.dbt('reference', command, target='dev', managed=False)
        label = f'{self.n:03d}_reference'
        log = (self.work / 'dbt' / (label + '.log')).read_text()
        query = (self.work / 'logs' / label / 'query_log.sql').read_text()
        self.record['reference'] = {'command': ' '.join(['dbt', *command, '--target', 'dev', '--no-manage-state']),
                                    'rpc_count': len(calls), 'rows': json.loads(log.strip()),
                                    'sql': '\n'.join(l for l in query.splitlines() if not l.startswith('--')).strip()}
        self.record['matches_reference'] = compare_reference(self.record)

    def run_case(self):
        self.setup()
        self.start()
        try:
            self.managed('prod', 'upstream')
            self.managed('prod', 'downstream')
            if self.a.case == 'D':
                # Change only the raw row after both production results are materialized.
                # The independently built upstreams execute byte-identical SELECTs.
                query = f"UPDATE {self.scope.relation('prod', 'raw')} SET marker='D_dev' WHERE marker='D_prod'"
                self.sql('change_raw_marker', query)
                self.record['raw_setup']['after_prod_downstream'] = query
            if self.a.case != 'A':
                self.managed('dev', 'upstream', copies=self.a.case == 'B')
            first = self.managed('dev', 'downstream', copies=True)
            self.managed('dev', 'downstream', copies=True, repeat=True)
            self.reference()
        finally:
            self.stop()
        path, = (self.work / 'golden').glob('*.jsonl')
        self.record['certified_rpcs'] = len(verify_capture(path))
        self.save('evidence.json', self.record)
        count = sum(sum(step['rpc_counts'].values()) for step in self.record['steps'].values())
        if count != self.record['certified_rpcs']:
            raise Refused('certified capture contains unattributed calls')
        if self.a.case == 'A' and (first != 'ready_to_clone'
                                   or self.record['steps']['dev_downstream']['delta']['operation'] != 'CLONE'):
            raise Refused('same-input positive control did not perform automatic copy')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', required=True, choices=list('ABCD'))
    parser.add_argument('--work-dir', required=True)
    parser.add_argument('--prod-schema', default='ds_20261112c134ef_prod')
    parser.add_argument('--dev-schema', default='ds_20261112c134ef_dev')
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--dbt', default=shutil.which('dbt'))
    parser.add_argument('--proxy', default=str(Path(__file__).resolve().parents[1] / 'target/debug/record-proxy'))
    args = parser.parse_args()
    profile = yaml.safe_load(Path(args.profiles).read_text())[args.profile]
    args.warehouse = profile['outputs'][profile['target']]['http_path'].rsplit('/', 1)[-1]
    Origin(args).run_case()


if __name__ == '__main__':
    main()
