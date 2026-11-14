#!/usr/bin/env python3
"""Bounded true-template failing-cache continuation; zero warehouse objects.

R is the immutable confirmed failure. F is fresh literal SQL, independently
executed even when template policy intentionally reuses R after rendering changes.
"""
import hashlib
import json
from pathlib import Path
import sys

from databricks_cached_tests import CachedTests, main, query
from databricks_survey import Refused


def assert_render_delta(prior, current):
    changed = {key for key in set(prior) | set(current) if prior.get(key) != current.get(key)}
    if changed != {'sql'}:
        raise Refused('wire:render-only-field-delta:' + ','.join(sorted(changed)))
    node = current.get('dbt_node_state') or {}
    if (current.get('compare_unrendered_code') is not True
            or not node.get('node_unique_id') or not node.get('node_body_hash')
            or not node.get('node_configs_hash')):
        raise Refused('wire:missing-true-template-identity')


class TemplateTests(CachedTests):
    def __init__(self, args):
        self.active = 'fail_first'
        super().__init__(args)
        self.save('template-runner.py', Path(__file__).read_text())
        self.save('template-provenance.json', {'entrypoint_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
        self.save('operation-manifest.json', {
            'steps': self.plan(), 'warehouse_mutation_allowlist': [],
            'literal_rows': [[1, 2], [1, 2], [2, None]],
            'schema': [['id', 'INT'], ['v', 'INT']],
            'R': [4, True, True], 'fresh_pass_F': [0, False, False],
            'config': self.test_config(), 'compare_unrendered_code': True})

    @staticmethod
    def plan():
        return [('fail_first', 3, 'ready_to_execute'),
                ('fail_repeat', 3, 'skip_execution'),
                ('render_pass', 0, 'skip_execution'),
                ('body_pass_first', 0, 'ready_to_execute'),
                ('body_pass_repeat', 0, 'skip_execution')]

    def compare_mode(self):
        return True

    def test_config(self):
        return {'severity': 'ERROR', 'error_if': '> 0', 'warn_if': '> 0',
                'fail_calc': 'coalesce(sum(v), cast(0 as bigint))'}

    def expected_result(self, rows):
        if rows not in (0, 3):
            raise Refused('oracle:template-literal-ledger')
        return (4, True, True) if rows == 3 else (0, False, False)

    def expected_cached_result(self, rows):
        return (4, True, True) if self.active == 'render_pass' else self.expected_result(rows)

    def nonce(self):
        return (self.a.query_identity or self.a.run_id) + ('b' if self.active.startswith('body_pass') else 'a')

    def rendered_query(self, failures):
        return query(self.nonce(), failures)

    def command_variables(self):
        return ['--vars', json.dumps({'rows_enabled': not ('pass' in self.active)})]

    def test_body(self):
        literal = query(self.nonce(), 3).replace('WHERE 1 = 1', "WHERE {{ 1 if var('rows_enabled') else 0 }} = 1")
        return ("{{ config(store_failures=false, state={'compare_unrendered_code':true}, "
                "severity='ERROR', fail_calc='coalesce(sum(v), cast(0 as bigint))', "
                "error_if='> 0', warn_if='> 0') }}\n" + literal)

    def write_test(self, failures):
        super().write_test(failures)
        self.save('project/tests/literal_outcome.sql', self.test_body())

    def manifest(self, result, failures):
        node = super().manifest(result, failures)
        if (node.get('raw_code') != self.test_body()
                or any(node['config'].get(k) != v for k, v in self.test_config().items())
                or node['config'].get('state', {}).get('compare_unrendered_code') is not True):
            raise Refused('guard:actual-template-declaration')
        return node

    def cell(self, label, failures, decision):
        self.active = label
        super().cell(label, failures, decision)
        current = self.cells[-1]['request']
        prior = self.cells[-2]['request'] if len(self.cells) > 1 else None
        delta = {key: {'before': prior.get(key), 'after': current.get(key)}
                 for key in sorted(set(prior or {}) | set(current))
                 if prior is not None and prior.get(key) != current.get(key)}
        self.save('request-deltas/' + label + '.json', delta)
        self.save('references/ledger_' + label + '.json', {
            'fresh_F': self.expected_result(failures),
            'required_recorded_R': self.expected_cached_result(failures),
            'rendered_sql': self.rendered_query(failures), 'raw_body': self.test_body()})
        if label == 'render_pass':
            assert_render_delta(prior, current)


if __name__ == '__main__':
    sys.exit(main(TemplateTests))
