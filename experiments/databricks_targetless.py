#!/usr/bin/env python3
"""Bounded #158 no-object characterization; captures are diagnostic, never fixtures.

Crafted publication establishes protocol behavior, not physical row correctness.
Selected-node show is ordinary pinned-client reachability evidence for exact cells.
No run/build, schema creation, warehouse writes or cleanup is permitted here.
"""
import argparse
from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from capture_integrity import verify_capture
from databricks_baseline import Baseline
from databricks_survey import Refused


class Targetless(Baseline):
    def sql(self, *args):
        raise Refused('targetless campaign prohibits warehouse SQL API operations')

    def dbt(self, label, command, target='prod', managed=True):
        if command[:2] != ['show', '--select'] or len(command) != 7 or command[3:] != ['--output', 'json', '--limit', '-1']:
            raise Refused('only selected-node show with explicit output/limit is permitted')
        return super().dbt(label, command, target, managed)

    def rpc(self, operation, request):
        if operation not in ('submit', 'confirm', 'explain'):
            raise Refused('unsupported crafted operation')
        start = len(self.wire())
        result = subprocess.run([self.a.probe, operation], input=json.dumps(request),
            env=dict(self.env, PROBE_URL='http://' + self.env['RUN_CACHE_API_URL'],
                     PROBE_INVOCATION=self.a.run_id), capture_output=True, text=True, timeout=60)
        self.event('crafted-public-rpc', operation=operation, request=request,
                   returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
        calls = self.wire()[start:]
        if len(calls) != 1:
            raise Refused('crafted operation must have exactly one captured RPC')
        if result.returncode:
            raise Refused(self.clean(result.stderr))
        return json.loads(result.stdout)

    def submit_explain(self, label, request):
        response = self.rpc('submit', request)
        value = next(iter(response['response'].values()))
        self.rpc('explain', {'execution_decision_ids': [value['execution_decision_id']]})
        self.save('checks/' + label + '.json', {'request':request, 'response':response})
        return response

    def crafted(self):
        rows = [json.loads(line) for line in Path(self.a.base_capture).read_text().splitlines()]
        base = deepcopy(next(row['request'] for row in rows if row['method'] == 'SubmitEnrichedSQL'))
        base.update(target_table=None, tables=[], allow_clones=False, query_dependencies=[],
                    compare_unrendered_code=False, is_defer_to_profile=False, defer_enabled=False)
        variants = {
            'sql': lambda q: q.update(sql=q['sql'].replace(' as marker', " || '_changed' as marker")),
            'config': lambda q: q['semantic_extras'].update(on_schema_change='"fail"'),
            'catalog': lambda q: q.update(default_catalog='other_literal_context'),
            'schema': lambda q: q.update(default_schema=self.scope.schema('ref')),
            'project': lambda q: q['dbt_node_state'].update(project_name='other_' + self.a.run_id),
            'uid': lambda q: q['dbt_node_state'].update(node_unique_id='model.other.' + self.a.run_id),
            'execution_type': lambda q: q.update(execution_type=10 if q['execution_type']==1 else 1),
            'namespace_named': lambda q: q.update(table_namespace=q['table_namespace'] + '_foreign'),
            'namespace_absent': lambda q: q.update(table_namespace=None),
            'namespace_empty': lambda q: q.update(table_namespace=''),
            'dependency': lambda q: q['query_dependencies'][0].update(query='select 2 as id'),
        }
        outcomes = []
        for execution_type in (1, 10):
            for index, (dimension, change) in enumerate(variants.items()):
                label = str(execution_type) + '_' + dimension
                marker = self.a.run_id + '_' + label
                q = deepcopy(base)
                q.update(execution_type=execution_type, sql="select '" + marker + "' as marker",
                         table_namespace='targetless_' + marker)
                q['dbt_node_state'].update(node_unique_id='model.' + marker + '.literal', project_name=marker)
                q['labels'] = dict(dbt_node_unique_id=q['dbt_node_state']['node_unique_id'])
                if dimension == 'dependency':
                    # Definition attached to the named CTE; no warehouse relation/epoch is claimed.
                    q['sql'] = "with literal_input as (select 1 as id) select '" + marker + "' as marker from literal_input"
                    q['query_dependencies'] = [dict(name='literal_input', query='select 1 as id',
                                                   default_catalog=q['default_catalog'], default_schema=q['default_schema'])]
                first = self.submit_explain(label + '_first', q)
                ready = first['response'].get('ready_to_execute')
                if ready is None:
                    raise Refused('fresh baseline did not execute: ' + label)
                runtime = 158000 + execution_type * 100 + index
                payload = {'fields': {'diagnostic_marker': {'kind': {'string_value':marker}}}}
                self.rpc('confirm', dict(request_id=ready['request_id'], last_modified_epoch=None,
                    failed_to_clone=False, table_type=None, execution_results=payload,
                    execution_runtime_ms=runtime, labels=q['labels']))
                repeat = self.submit_explain(label + '_repeat', q)
                changed = deepcopy(q)
                change(changed)
                foreign = self.submit_explain(label + '_foreign', changed)
                control = self.submit_explain(label + '_control', q)
                for response in (repeat, control):
                    skip = response['response'].get('skip_execution')
                    if skip is None or skip['execution_runtime_ms'] != runtime or skip['execution_results'] != payload:
                        raise Refused('unchanged complete cached marker positive failed: ' + label)
                outcomes.append(dict(execution_type=execution_type, dimension=dimension,
                    foreign_variant=next(iter(foreign['response'])), foreign_response=foreign,
                    runtime=runtime, marker=marker))
                self.save('matrix.json', outcomes)
                print('crafted', label, outcomes[-1]['foreign_variant'], flush=True)

    def selected(self):
        self.setup_project()
        (self.project / 'models/sources.yml').unlink()
        names = []
        for kind in ('table', 'view'):
            name = 'literal_' + kind + '_' + self.a.run_id
            self.write_model(name, "materialized='" + kind + "',state={'lag_tolerance':'0s','pre_clone':'never'}",
                             "select '" + self.a.run_id + "_" + kind + "' as marker")
            names.append(name)
        analysis = 'literal_analysis_' + self.a.run_id
        self.save('project/analyses/' + analysis + '.sql', "select '" + self.a.run_id + "_analysis' as marker\n")
        names.append(analysis)
        for name in names:
            for suffix, managed in [('first', True), ('repeat', True), ('reference', False)]:
                label = name + '_' + suffix
                start = len(self.wire())
                error = None
                try:
                    self.dbt(label, ['show', '--select', name, '--output', 'json', '--limit', '-1'], managed=managed)
                except Refused as failure:
                    error = str(failure)
                calls = self.wire()[start:]
                log = (self.work / 'dbt' / f'{self.n:03d}_{label}.log').read_text()
                rows = [json.loads(line) for line in log.splitlines() if line.startswith('[{')]
                expected = [{'marker':self.a.run_id + '_' + ('analysis' if name == analysis else name.split('_')[1])}]
                if not error and rows != [expected]:
                    raise Refused('selected show did not return the independently declared literal row')
                if managed and not error and 'dbt State is enabled' not in log:
                    raise Refused('managed selected show did not report State enabled')
                if not managed and calls:
                    raise Refused('State-disabled reference sent State RPCs')
                self.save('checks/' + label + '_reachability.json', dict(error=error, rows=rows, expected=expected,
                    managed=managed, methods=dict(Counter(row['method'] for row in calls)),
                    execution_calls=[row for row in calls if row['method'] in (
                        'SubmitEnrichedSQL', 'SubmitEnrichedSQLSpeculative', 'ConfirmExecution', 'RecordExecutions')]))

    def run(self):
        # Unlike Baseline.run, this path never initializes schemas or executes SQL.
        before = self.inventory()
        self.save('lease-inventory-before.json', before)
        self.save('schema-lease.json', self.lease)
        self.save('targetless-runner.py', Path(__file__).read_text())
        self.save('binary-digests.json', {name:hashlib.sha256(Path(getattr(self.a,name)).read_bytes()).hexdigest()
                                        for name in ('dbt','proxy','probe')})
        failure = None
        try:
            self.start()
            if self.a.mode == 'crafted':
                self.crafted()
            else:
                self.selected()
        except Exception as error:
            failure = self.clean(str(error))
        finally:
            self.stop()
            after = self.inventory()
            self.save('lease-inventory-after.json', after)
            same = {k:sorted(v,key=lambda r:r['full_name']) for k,v in before.items()} == {k:sorted(v,key=lambda r:r['full_name']) for k,v in after.items()}
            certified = []
            for path in sorted((self.work/'golden').glob('*.jsonl')):
                rows = verify_capture(path)
                certified.append(dict(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                                      rows=len(rows), methods=dict(Counter(r['method'] for r in rows))))
            self.save('summary.json', dict(failure=failure, inventory_unchanged=same, captures=certified,
                invocations=self.n, mode=self.a.mode, production_changes=False))
        if failure or not same or not certified:
            raise Refused(failure or 'inventory changed or capture absent')
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','probe','work-dir','schema-lease','run-id','base-capture'):
        parser.add_argument('--'+name, required=True)
    parser.add_argument('--mode', choices=('crafted','selected'), required=True)
    parser.add_argument('--rust-url', help='owned loopback production-router endpoint')
    parser.add_argument('--profiles', default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    args = parser.parse_args()
    args.query_identity = None
    if args.rust_url:
        import re
        if args.mode != 'selected' or not re.fullmatch(r'http://127\.0\.0\.1:\d+', args.rust_url):
            parser.error('--rust-url requires selected mode and an owned loopback endpoint')
    try:
        return Targetless(args).run()
    except Exception as error:
        print('FAILED:', str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
