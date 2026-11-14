#!/usr/bin/env python3
"""Crafted scope/alias characterization; no warehouse SQL or client-reachability claim."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time

from databricks_baseline import Baseline
from databricks_deferral import Deferral
from databricks_survey import Refused


class IsolationProtocol(Baseline):
    rpc = Deferral.rpc

    def init_schemas(self):
        self.save('classification.json', {
            'evidence_class': 'crafted-public-grpc', 'warehouse_sql_executed': False,
            'actual_client_reachable': False, 'synthetic_confirmations': True,
            'org_probe': 'not attempted: hosted OAuth selects organization',
            'node_kind': getattr(self.a, 'node_kind', 'explicit in captured requests'),
            'dialect_aliases': ['spark', 'spark, normalization_strategy = lowercase']})
        self.save('protocol-runner.py', Path(__file__).read_text())

    def submit(self, label, request, expected):
        before = len(self.wire())
        response = self.rpc('submit', request)
        if expected not in response['response']:
            raise Refused(label + ': expected ' + expected)
        self.save('cells/' + label + '.json', {'request': request, 'response': response})
        self.explain(label, [row for row in self.wire()[before:] if row['method'] == 'SubmitEnrichedSQL'])
        return response

    def confirm(self, response, request, epoch):
        result = self.rpc('confirm', {
            'request_id': response['response']['ready_to_execute']['request_id'],
            'last_modified_epoch': epoch, 'failed_to_clone': False,
            'table_type': 'MANAGED' if request['execution_type'] == 1 else 'VIEW', 'execution_results': None,
            'execution_runtime_ms': 159, 'labels': request['labels']})
        if not result['success']:
            raise Refused('synthetic confirmation was rejected')

    def baseline(self):
        rows = [json.loads(line) for line in Path(self.a.template).read_text().splitlines()]
        kind = 1 if self.a.node_kind == 'full' else 10
        base = next(row['request'] for row in rows if row['method'] == 'SubmitEnrichedSQL'
                    and row['request']['execution_type'] == kind)
        epoch = int(time.time() * 1000)
        target = '`' + self.scope.relation('prod', 'literal_table').replace('.', '`.`') + '`'

        def body(cell):
            request = deepcopy(base)
            request.update(dialect='databricks', execution_type=kind, target_table=target,
                           sql="select 159 as id, '" + self.a.run_id + '_' + cell + "' as tag",
                           tables=[], table_namespace='isolation_' + self.a.run_id + '_' + cell,
                           allow_clones=False)
            uid = 'model.isolation_' + self.a.run_id + '.' + cell
            request['dbt_node_state'].update(node_unique_id=uid, project_name='isolation_' + self.a.run_id,
                                             project_id='isolation_' + self.a.run_id)
            request['labels'] = dict(request['labels'], dbt_node_unique_id=uid, dbt_node_name=cell)
            return request

        for name, update in [('namespace', {'table_namespace': 'other_' + self.a.run_id}),
                             ('missing_namespace', {'table_namespace': None}),
                             ('empty_namespace', {'table_namespace': ''}),
                             ('family', {'dialect': 'snowflake'}),
                             ('target', {'target_table': '`' + self.scope.relation('ref', 'literal_table').replace('.', '`.`') + '`'})]:
            request = body(name)
            self.confirm(self.submit(name + '_first', request, 'ready_to_execute'), request, epoch)
            request['tables'] = [{'name': target, 'last_modified_epoch': epoch}]
            self.submit(name + '_same', request, 'skip_execution')
            other = deepcopy(request)
            other.update(update)
            other['tables'] = [{'name': other['target_table'], 'last_modified_epoch': epoch}]
            self.submit(name + '_other_scope', other, 'ready_to_execute')
            other['sql'] = other['sql'].replace('159 as', '160 as')
            self.confirm(self.submit(name + '_other_build', other, 'ready_to_execute'), other, epoch)
            self.submit(name + '_other_reuse', other, 'skip_execution')
            self.submit(name + '_return', request, 'skip_execution')

        for name, alias in [('spark', 'spark'), ('spark_lowercase', 'spark, normalization_strategy = lowercase')]:
            request = body(name)
            self.confirm(self.submit(name + '_first', request, 'ready_to_execute'), request, epoch)
            request['tables'] = [{'name': target, 'last_modified_epoch': epoch}]
            other = deepcopy(request)
            other['dialect'] = alias
            self.submit(name + '_alias_reuse', other, 'skip_execution')
            changed = deepcopy(other)
            changed['sql'] = changed['sql'].replace('159 as', '160 as')
            self.confirm(self.submit(name + '_changed_alias', changed, 'ready_to_execute'), changed, epoch)
            self.submit(name + '_new_alias_reuse', changed, 'skip_execution')
            canonical = deepcopy(changed)
            canonical['dialect'] = 'databricks'
            self.submit(name + '_new_canonical_reuse', canonical, 'skip_execution')
            self.submit(name + '_old_must_not_resurrect', request, 'ready_to_execute')
            for scope, value in [('table_namespace', 'other_alias_' + self.a.run_id),
                                 ('target_table', '`' + self.scope.relation('ref', 'literal_table').replace('.', '`.`') + '`')]:
                negative = deepcopy(changed)
                negative[scope] = value
                negative['tables'] = [{'name': negative['target_table'], 'last_modified_epoch': epoch}]
                self.submit(name + '_other_' + scope, negative, 'ready_to_execute')

        request = body('absent')
        request['table_namespace'] = None
        self.confirm(self.submit('absent_first', request, 'ready_to_execute'), request, epoch)
        request['tables'] = [{'name': target, 'last_modified_epoch': epoch}]
        self.submit('absent_reuse', request, 'skip_execution')
        empty = deepcopy(request)
        empty['table_namespace'] = ''
        self.submit('absent_to_empty', empty, 'skip_execution')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('dbt', 'proxy', 'probe', 'schema-lease', 'work-dir', 'template'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--node-kind', choices=('full', 'view'), default='view')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    args = parser.parse_args()
    args.query_identity = None
    args.rust_url = None
    try:
        return IsolationProtocol(args).run()
    except Exception as error:
        print('FAILED:', str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
