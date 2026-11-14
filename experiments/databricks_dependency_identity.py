#!/usr/bin/env python3
"""Opt-in #138 pinned-client dependency, template and configuration capture."""
import argparse
import copy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import subprocess
import time

from databricks_baseline import Baseline, LeaseScope, assert_physical, query
from databricks_survey import Refused, same_result, check_manifest


class IdentityScope(LeaseScope):
    def __init__(self, run, ledger):
        super().__init__(run, ledger)
        self.relation_prefix = 'd_' + run + '_'
        names = {'prod': ('input_left', 'input_right', 'left_view', 'right_view',
                          'template', 'config', 'dependency', 'flags'),
                 'ref': ('reference',)}
        self.allowed = {f'cdo_demo_db.{self.schema(role)}.{self.relation_prefix}{name}'
                        for role, members in names.items() for name in members}


class Identity(Baseline):
    def __init__(self, args):
        super().__init__(args)
        if not self.lease:
            raise Refused('identity campaign requires the explicit schema lease')
        self.scope = IdentityScope(args.run_id, self.lease)
        for name in ('proxy', 'probe'):
            owned = self.work / 'bin' / name
            owned.parent.mkdir(exist_ok=True)
            shutil.copy2(getattr(args, name), owned)
            setattr(args, name, str(owned))
        self.save('identity-runner.py', Path(__file__).read_text())
        self.save('identity-scope.json', {'allowed': sorted(self.scope.allowed)})
        self.contract = json.loads(Path(args.contract).read_text()) if args.contract else {}
        self.cells = []
        self.boundaries = []

    def init_schemas(self):
        result = super().init_schemas()
        counts = {s: len(rows) for s, rows in self.before_inventory.items()}
        # Reserve every remaining backend's full phase before live mutations.
        # Diagnostic attempts are already included in the fresh before counts.
        prod_count = 0 if self.a.only_tests else (8 if self.a.all_phases else (5 if self.a.only_dependencies else (1 if self.a.only_supplement else 2)))
        remaining = 1 if self.a.rust_url else 2
        projected = {s: count + remaining * (prod_count if s == self.scope.schema('prod') else (0 if self.a.only_tests else 1))
                     for s, count in counts.items()}
        self.save('capacity-budget.json', {'before': counts, 'projected_two_phases': projected,
                  'limit': 100, 'planned_per_phase': {'prod': prod_count, 'ref': 1}, 'remaining_backends': remaining})
        if max(projected.values()) > 100:
            raise Refused('insufficient capacity for hosted plus Rust plus reference')
        return result

    def project_config(self, **fields):
        self.save(str(self.project.relative_to(self.work) / 'dbt_project.yml'), {
            'name': 'survey_' + self.a.run_id, 'version': '1.0', 'profile': 'survey',
            'models': {'survey_' + self.a.run_id: {('+' + k): v for k, v in fields.items()}}})

    def cell(self, label, node, sql, fields, projection=10, expected=None, reference_revision=0):
        self.project = self.base
        for path in (self.project / 'models').glob('*.sql'):
            path.unlink()
        self.project_config(**fields)
        self.write_model(node, "materialized='table', file_format='delta'", sql)
        if label.endswith('_first'):
            self.dbt(label + '_inventory', ['parse', '--vars', json.dumps({'projection': projection})], managed=False)
            manifest = json.loads(next((self.work / 'targets').glob('*_' + label + '_inventory/manifest.json')).read_text())
            check_manifest(self.scope, manifest)
            for manifest_node in manifest.get('nodes', {}).values():
                self.scope.check('.'.join([manifest_node['database'], manifest_node['schema'], manifest_node['alias']]))
        calls = self.dbt(label, ['run', '--vars', json.dumps({'projection': projection})])
        submits = [r for r in calls if r['method'] == 'SubmitEnrichedSQL']
        if len(submits) != 1:
            raise Refused('cell must submit exactly one model')
        row = submits[0]
        decision, response = next(iter(row['response']['response'].items()))
        confirms = {r['request']['request_id'] for r in calls if r['method'] == 'ConfirmExecution'
                    and (r.get('response') or {}).get('success') is True}
        self.cells.append({'cell': label, 'kind': 'actual-pinned-client', 'request': row['request'],
                           'response': row['response'], 'decision': decision})
        self.save('identity-results.json', self.cells)
        if decision == 'ready_to_execute' and response['request_id'] not in confirms:
            raise Refused('build lacks successful confirmation')
        expected = self.contract.get(label, {}).get('decision', expected)
        if expected and decision != expected:
            raise Refused(label + ': expected ' + expected + ', got ' + decision)
        self.explain(label, submits)
        revision = reference_revision
        if label == 'template_projection' and decision == 'ready_to_execute':
            revision = 1
        self.physical_check(label, node, revision)
        self.cells[-1]['reference_revision'] = revision
        self.save('identity-results.json', self.cells)
        self.project = self.base
        return self.cells[-1]

    def physical_check(self, label, node, revision):
        actual = self.observe(label, self.scope.relation('prod', node))
        props = json.loads((self.work / 'physical' / (label + '_properties.json')).read_text())
        assert_physical(actual, props, revision, 'table', self.query_identity)
        self.project = self.reference
        self.write_model('reference', "materialized='table', file_format='delta'", query(revision, self.query_identity))
        if self.dbt('reference_' + label, ['run'], target='ref', managed=False):
            raise Refused('State-disabled reference emitted RPCs')
        independent = self.observe('reference_' + label, self.scope.relation('ref', 'reference'))
        assert_physical(independent, json.loads((self.work / 'physical' / ('reference_' + label + '_properties.json')).read_text()),
                        revision, 'table', self.query_identity)
        direct = self.sql('reference_select_' + label, query(revision, self.query_identity).replace('select ', 'SELECT ', 1))
        if not same_result(actual, independent) or not same_result(actual, direct):
            raise Refused('reference:rows-or-schema')
        self.project = self.base

    def template_and_config(self):
        sql = query(0, self.query_identity).replace('1,10,', "1,{{ var('projection', 10) }},")
        state = {'compare_unrendered_code': True, 'lag_tolerance': '0s', 'pre_clone': 'never'}
        fields = {'state': state}
        self.save('operation-manifest.json', {'kind': 'actual-pinned-client',
                  'plan': ['template_first', 'template_unchanged', 'template_comment', 'template_projection',
                           'template_both_changed', 'template_false', 'template_true',
                           'config_first', 'config_unchanged', 'config_on_schema_change', 'config_tags'],
                  'permitted_stale_cell': 'template_projection only: same captured body with comparison true',
                  'query_identity': self.query_identity})
        first = self.cell('template_first', 'template', sql, fields, expected='ready_to_execute')
        if not first['request'].get('compare_unrendered_code'):
            raise Refused('project YAML candidate did not emit compare_unrendered_code=true')
        if not (first['request'].get('dbt_node_state') or {}).get('node_body_hash'):
            raise Refused('true comparison lacks actual client body hash')
        self.cell('template_unchanged', 'template', sql, fields, expected='skip_execution')
        comment = sql + '\n{# identity comment #}'
        comment_row = self.cell('template_comment', 'template', comment, fields)
        # A SKIP does not publish a new template. Compare the var change with
        # the body of the actual latest confirmed build, not the last submit.
        recorded_body = comment if comment_row['decision'] == 'ready_to_execute' else sql
        projection_row = self.cell('template_projection', 'template', recorded_body, fields, projection=17)
        current_revision = 1 if projection_row['decision'] == 'ready_to_execute' else 0
        both_revision = 1 - current_revision
        both = sql + '\n{# different identity comment #}'
        projection = 17 if both_revision else 10
        self.cell('template_both_changed', 'template', both, fields, projection=projection,
                  expected='ready_to_execute', reference_revision=both_revision)
        off = {'state': dict(state, compare_unrendered_code=False)}
        self.cell('template_false', 'template', both, off, projection=projection,
                  expected='skip_execution', reference_revision=both_revision)
        self.cell('template_true', 'template', both, fields, projection=projection,
                  expected='skip_execution', reference_revision=both_revision)
        config_sql = query(0, self.query_identity)
        plain = {'state': {'lag_tolerance': '0s', 'pre_clone': 'never'}}
        self.cell('config_first', 'config', config_sql, plain, expected='ready_to_execute')
        self.cell('config_unchanged', 'config', config_sql, plain, expected='skip_execution')
        changed = dict(plain, on_schema_change='fail')
        self.cell('config_on_schema_change', 'config', config_sql, changed)
        self.cell('config_tags', 'config', config_sql, dict(changed, tags=['identity138']))
        return {'cells': len(self.cells)}

    def boundary(self, label, request):
        before = len(self.wire())
        result = subprocess.run([self.a.probe, 'submit'], input=json.dumps(request),
                                env=dict(self.env, PROBE_URL='http://' + self.env['RUN_CACHE_API_URL']),
                                capture_output=True, text=True, timeout=60)
        self.save('boundary/' + label + '.json', {'kind': 'crafted-protocol-boundary',
                  'request': request, 'returncode': result.returncode,
                  'stdout': result.stdout, 'stderr': result.stderr})
        time.sleep(.1)
        rows = [r for r in self.wire()[before:] if r['method'] == 'SubmitEnrichedSQL']
        if len(rows) != 1:
            raise Refused('boundary call was not captured exactly once')
        row = rows[0]
        self.boundaries.append({'cell': label, 'kind': 'crafted-protocol-boundary', **row})
        self.save('boundary-results.json', self.boundaries)
        if not result.returncode:
            self.explain('boundary_' + label, [row])
        return row

    def dependencies(self):
        for name, revision in [('input_left', 0), ('input_right', 1)]:
            self.sql('create_' + name, 'CREATE TABLE ' + self.scope.relation('prod', name)
                     + ' USING DELTA AS ' + query(revision, self.query_identity))
        for name, source in [('left_view', 'input_left'), ('right_view', 'input_right')]:
            self.sql('create_' + name, 'CREATE VIEW ' + self.scope.relation('prod', name)
                     + ' AS SELECT id, amount, tag FROM ' + self.scope.relation('prod', source))
        left = self.scope.relation('prod', 'left_view')
        right = self.scope.relation('prod', 'right_view')
        sql = ('select id, amount, tag from ' + left + ' where id = 1 union all '
               'select id, amount, tag from ' + right + ' where id = 2')
        fields = {'state': {'compare_unrendered_code': True, 'lag_tolerance': '0s', 'pre_clone': 'never'}}
        self.cell('dependency_first', 'dependency', sql, fields, expected='ready_to_execute')
        unchanged = self.cell('dependency_unchanged', 'dependency', sql, fields, expected='skip_execution')
        request = unchanged['request']
        if len(request['query_dependencies']) != 2:
            raise Refused('pinned client did not emit exactly two view bindings')
        mutations = []
        reordered = copy.deepcopy(request)
        reordered['query_dependencies'].reverse()
        mutations.append(('whole_bindings_reordered', reordered))
        swapped = copy.deepcopy(request)
        a, b = swapped['query_dependencies']
        a['query'], b['query'] = b['query'], a['query']
        mutations.append(('definitions_swapped', swapped))
        for field, value in [('default_catalog', 'cdo_demo_db'), ('default_schema', self.scope.schema('ref'))]:
            changed = copy.deepcopy(request)
            changed['query_dependencies'][0][field] = value
            mutations.append(('dependency_' + field, changed))
        extras = copy.deepcopy(request)
        extras['semantic_extras'] = dict(reversed(list(extras['semantic_extras'].items())))
        mutations.append(('extras_reordered', extras))
        changed = copy.deepcopy(request)
        changed['semantic_extras']['on_schema_change'] = '"fail"'
        mutations.append(('same_body_extra_changed', changed))
        for label, body in [('template_body_absent', None), ('template_body_empty', '')]:
            changed = copy.deepcopy(request)
            if body is None:
                changed['dbt_node_state'] = None
            else:
                changed['dbt_node_state']['node_body_hash'] = body
            mutations.append((label, changed))
        changed = copy.deepcopy(request)
        changed['query_dependencies'].append(copy.deepcopy(changed['query_dependencies'][0]))
        mutations.append(('identical_dependency_duplicate', changed))
        changed = copy.deepcopy(request)
        changed['query_dependencies'][0]['query'] = ''
        mutations.append(('empty_dependency_definition', changed))
        for label, changed in mutations:
            self.boundary(label, changed)
        # These crafted BUILD proposals remain unconfirmed; the current physical
        # history and its real warehouse epochs remain the actual first build.
        self.boundary('unchanged_after_pending_boundaries', request)
        for name, source in [('left_view', 'input_right'), ('right_view', 'input_left')]:
            self.sql('replace_' + name, 'CREATE OR REPLACE VIEW ' + self.scope.relation('prod', name)
                     + ' AS SELECT id, amount, tag FROM ' + self.scope.relation('prod', source))
        changed = self.cell('dependency_actual_swapped', 'dependency', sql, fields,
                            expected='ready_to_execute', reference_revision=1)
        if changed['request']['dbt_node_state']['node_body_hash'] != request['dbt_node_state']['node_body_hash']:
            raise Refused('dependency guard did not retain actual body bytes')
        self.cell('dependency_swapped_unchanged', 'dependency', sql, fields,
                  expected='skip_execution', reference_revision=1)
        return {'cells': len(self.cells), 'boundaries': len(self.boundaries)}

    def supplement(self):
        sql = query(0, self.query_identity).replace('1,10,', "1,{{ var('projection', 10) }},")
        state = {'compare_unrendered_code': False, 'lag_tolerance': '0s', 'pre_clone': 'never'}
        plain = {'state': state}
        self.cell('flags_first', 'flags', sql, plain, expected='ready_to_execute')
        self.cell('flags_false_unchanged', 'flags', sql, plain, expected='skip_execution')
        enabled = {'state': dict(state, compare_unrendered_code=True)}
        self.cell('flags_false_to_true', 'flags', sql, enabled, expected='skip_execution')
        self.cell('flags_true_projection_after_false_build', 'flags', sql, enabled,
                  projection=17, expected='ready_to_execute', reference_revision=1)
        # An inline Jinja comment changes body bytes and emits zero SQL bytes.
        pure_comment = '{# inline identity comment #}' + sql
        self.cell('flags_pure_comment', 'flags', pure_comment, enabled, projection=17,
                  expected='skip_execution', reference_revision=1)
        config_changed = dict(enabled, on_schema_change='fail')
        self.cell('flags_true_config_changed', 'flags', sql, config_changed, projection=17,
                  expected='ready_to_execute', reference_revision=1)
        # Same input body, original rendering, actual advertised write-only path.
        self.project = self.base
        self.project_config(**enabled)
        self.write_model('flags', "materialized='table', file_format='delta'", sql)
        calls = self.dbt('flags_record', ['run', '--state-mode', 'write-only', '--vars', '{"projection":10}'])
        records = [r for r in calls if r['method'] == 'RecordExecutions']
        self.save('record-results.json', records)
        if len(records) != 1 or (records[0].get('status') or {}).get('code') != 0:
            raise Refused('write-only did not emit a successful actual RecordExecutions')
        self.physical_check('flags_record', 'flags', 0)
        self.cell('flags_record_to_true', 'flags', sql, enabled, expected='skip_execution')
        self.cell('flags_projection_after_record', 'flags', sql, enabled, projection=17,
                  expected='skip_execution', reference_revision=0)
        return self.tests()

    def tests(self):
        # Pinned singular-test reachability is independent from model config.
        tests = self.base / 'tests'
        tests.mkdir(exist_ok=True)
        test_body = ("{{ config(state={'compare_unrendered_code':true}) }}\n"
                     "select cast(1 as int) as id where {{ var('failures', 0) }} = 1"
                     + " and '" + self.a.run_id + "' = '" + self.a.run_id + "'")
        self.save(str((tests / 'template_identity.sql').relative_to(self.work)), test_body)
        for label, failures in [('test_first', 0), ('test_unchanged', 0), ('test_comment', 0), ('test_projection', 1)]:
            self.save(str((tests / 'template_identity.sql').relative_to(self.work)),
                      ('{# inline test comment #}' if label == 'test_comment' else '') + test_body)
            calls = self.dbt(label, ['test', '--vars', json.dumps({'failures': failures})])
            ordinary = [r for r in calls if r['method'] == 'SubmitEnrichedSQL']
            speculative = [r for r in calls if r['method'] == 'SubmitEnrichedSQLSpeculative'
                           and 'skip_execution' in ((r.get('response') or {}).get('response') or {})]
            submits = ordinary or speculative
            if len(submits) != 1 or not submits[0]['request'].get('compare_unrendered_code'):
                raise Refused('singular-test template mode not reached exactly once')
            row = submits[0]
            decision = next(iter(row['response']['response']))
            expected = 'ready_to_execute' if label == 'test_first' else 'skip_execution'
            if decision != expected:
                raise Refused('singular-test template decision: ' + label)
            self.explain(label, submits)
            direct = self.sql('independent_' + label, 'SELECT cast(1 as int) AS id WHERE 0 = 1')
            if direct['rows'] or [(c['name'], c['type_name']) for c in direct['columns']] != [('id', 'INT')]:
                raise Refused('independent:test-rows-or-schema')
            self.project = self.reference
            reference_tests = self.reference / 'tests'
            reference_tests.mkdir(exist_ok=True)
            self.save(str((reference_tests / 'template_identity.sql').relative_to(self.work)),
                      "select cast(1 as int) as id where {{ var('failures', 0) }} = 1")
            if self.dbt('reference_' + label, ['test', '--vars', '{"failures":0}'], target='ref', managed=False):
                raise Refused('State-disabled test reference emitted RPCs')
            reference_result = json.loads(next((self.work / 'targets').glob('*_reference_' + label + '/run_results.json')).read_text())
            if len(reference_result['results']) != 1 or reference_result['results'][0]['status'] != 'pass' or reference_result['results'][0]['failures'] != 0:
                raise Refused('independent:test-outcome')
            self.project = self.base
            if decision == 'skip_execution':
                results = row['response']['response'][decision]['execution_results']
            else:
                confirms = [r for r in calls if r['method'] == 'ConfirmExecution' and (r.get('response') or {}).get('success')]
                if len(confirms) != 1:
                    raise Refused('test build lacks one successful confirmation')
                results = confirms[0]['request']['execution_results']
            expected_results = {'fields': {'failures': {'kind': {'int_value': 0}},
                                'should_error': {'kind': {'bool_value': False}},
                                'should_warn': {'kind': {'bool_value': False}}}}
            if results != expected_results:
                raise Refused('independent:complete-cached-test-results')
            self.save('test-results/' + label + '.json', {'request': row['request'], 'response': row['response'],
                      'recorded_reference_failures': 0, 'independent': direct, 'cached_results': results})
        return {'cells': len(self.cells), 'tests': 4}

    def baseline(self):
        self.setup_project()
        (self.project / 'models' / 'sources.yml').unlink()
        self.base = self.project
        self.reference = self.work / 'reference-project'
        shutil.copytree(self.base, self.reference)
        if self.a.all_phases:
            self.template_and_config()
            self.query_identity = self.a.dependency_query_identity
            self.dependencies()
            self.query_identity = self.a.supplement_query_identity
            return self.supplement()
        if self.a.only_tests:
            return self.tests()
        if self.a.only_supplement:
            return self.supplement()
        return self.dependencies() if self.a.only_dependencies else self.template_and_config()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('dbt', 'proxy', 'probe', 'work-dir', 'schema-lease'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--rust-url')
    parser.add_argument('--contract', help='required observed decisions for Rust acceptance')
    parser.add_argument('--only-dependencies', action='store_true')
    parser.add_argument('--only-supplement', action='store_true')
    parser.add_argument('--only-tests', action='store_true')
    parser.add_argument('--all-phases', action='store_true')
    parser.add_argument('--dependency-query-identity')
    parser.add_argument('--supplement-query-identity')
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--query-identity')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    args = parser.parse_args()
    if args.rust_url:
        if not args.contract:
            parser.error('--rust-url requires the observed hosted decision contract')
        import re
        if not re.fullmatch(r'http://127\.0\.0\.1:\d+', args.rust_url):
            parser.error('--rust-url must be an owned loopback endpoint')
    try:
        return Identity(args).run()
    except Exception as error:
        print('FAILED:', str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
