#!/usr/bin/env python3
"""Bounded source-less cached-test configuration continuation (#143 partial).

A fresh failing declaration must BUILD before any reuse. No warehouse object is
created; every cell keeps the full UID, SQL body and literal rows unchanged.
"""
import hashlib
import json
import sys
from pathlib import Path

from databricks_cached_tests import CachedTests, main, query
from databricks_survey import Refused


# Independent worked expectations for [(1,2), (1,2), (2,NULL)].
CASES = {
    'fail': ({'severity': 'ERROR', 'error_if': '> 0', 'warn_if': '> 0', 'fail_calc': 'count(*)'}, (3, True, True)),
    'severity_warn': ({'severity': 'WARN', 'error_if': '> 0', 'warn_if': '> 0', 'fail_calc': 'count(*)'}, (3, True, True)),
    'threshold_warn': ({'severity': 'ERROR', 'error_if': '> 3', 'warn_if': '> 0', 'fail_calc': 'count(*)'}, (3, False, True)),
    'threshold_pass': ({'severity': 'ERROR', 'error_if': '> 3', 'warn_if': '> 3', 'fail_calc': 'count(*)'}, (3, False, False)),
    'count_v': ({'severity': 'ERROR', 'error_if': '> 2', 'warn_if': '> 0', 'fail_calc': 'count(v)'}, (2, False, True)),
    'sum_v': ({'severity': 'ERROR', 'error_if': '> 3', 'warn_if': '> 0', 'fail_calc': 'sum(v)'}, (4, True, True)),
}


class ConfigTests(CachedTests):
    @staticmethod
    def configure_parser(parser):
        parser.add_argument('--through', choices=CASES, default='severity_warn')

    def __init__(self, args):
        self.active = 'fail'
        super().__init__(args)
        self.save('config-runner.py', Path(__file__).read_text())
        self.save('config-provenance.json', {'entrypoint_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
        self.save('operation-manifest.json', {
            'cells': self.plan(), 'cases': CASES, 'through': args.through,
            'body': self.test_body(), 'warehouse_mutation_allowlist': []})

    def plan(self):
        keys = list(CASES)
        return [(case + suffix, 3, decision)
                for case in keys[:keys.index(self.a.through) + 1]
                for suffix, decision in [('_first', 'ready_to_execute'), ('_repeat', 'skip_execution')]]

    def test_config(self):
        return CASES[self.active][0]

    def expected_result(self, rows):
        if rows != 3:
            raise Refused('oracle:configuration-row-ledger')
        return CASES[self.active][1]

    def command_variables(self):
        return ['--vars', json.dumps(self.test_config())]

    def test_body(self):
        return ("{{ config(store_failures=false, state={'compare_unrendered_code':false}, "
                "severity=var('severity'), fail_calc=var('fail_calc'), "
                "error_if=var('error_if'), warn_if=var('warn_if')) }}\n"
                + query(self.a.query_identity or self.a.run_id, 3))

    def write_test(self, failures):
        super().write_test(failures)
        self.save('project/tests/literal_outcome.sql', self.test_body())

    def manifest(self, result, failures):
        node = super().manifest(result, failures)
        if any(node['config'].get(key) != value for key, value in self.test_config().items()):
            raise Refused('guard:actual-configuration')
        if node.get('raw_code') != self.test_body():
            raise Refused('guard:unchanged-declared-body')
        return node

    def cell(self, label, failures, decision):
        self.active = label.rsplit('_', 1)[0]
        super().cell(label, failures, decision)
        prior = self.cells[-2]['request'] if len(self.cells) > 1 else None
        current = self.cells[-1]['request']
        delta = {key: {'before': prior.get(key), 'after': current.get(key)}
                 for key in sorted(set(prior or {}) | set(current))
                 if prior is not None and prior.get(key) != current.get(key)}
        self.save('request-deltas/' + label + '.json', delta)


if __name__ == '__main__':
    sys.exit(main(ConfigTests))
