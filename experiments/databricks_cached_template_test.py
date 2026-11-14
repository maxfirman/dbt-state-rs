"""Separate recorded-template R from fresh-rendering F at the oracle seam."""
import unittest
import json
import tempfile
from pathlib import Path
from databricks_cached_template import TemplateTests, assert_render_delta
from databricks_cached_tests import assert_payload, payload, CommandResult
from databricks_survey import Refused


class TemplateOracle(unittest.TestCase):
    def test_actual_render_delta_requires_unchanged_complete_template_identity(self):
        import copy
        prior = {'sql': 'SELECT 1', 'compare_unrendered_code': True,
                 'dbt_node_state': {'node_unique_id': 'test.p.literal_outcome',
                                    'node_body_hash': 'body', 'node_configs_hash': 'config'},
                 'semantic_extras': {'severity': '\"ERROR\"'}, 'table_namespace': 'connection'}
        current = {**prior, 'sql': 'SELECT 0'}
        assert_render_delta(prior, current)
        for key, value in [('compare_unrendered_code', False), ('table_namespace', 'other'),
                           ('semantic_extras', {}), ('dbt_node_state', {})]:
            wrong = copy.deepcopy(current)
            wrong[key] = value
            with self.subTest(field=key), self.assertRaises(Refused):
                assert_render_delta(prior, wrong)
        with self.assertRaises(Refused):
            assert_render_delta(prior, prior)

    def test_client_outcome_uses_R_only_for_managed_template_reuse(self):
        runner = object.__new__(TemplateTests)
        runner.active = 'render_pass'
        runner.uid = 'test.p.literal_outcome'
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            artifact = target / 'run_results.json'
            result = CommandResult(1, 'cached logical failure', target, [])
            def write(status, failures):
                artifact.write_text(json.dumps({'results': [
                    {'unique_id': runner.uid, 'status': status, 'failures': failures}]}))
            write('fail', 4)
            runner.outcome(result, 0, True)
            with self.assertRaises(Refused):
                runner.outcome(result, 0, False)
            write('pass', 0)
            result.returncode = 0
            runner.outcome(result, 0, False)
            with self.assertRaises(Refused):
                runner.outcome(result, 0, True)
            for status, failures, code in [('error', 4, 1), ('fail', 4, 2), ('fail', 4, 0)]:
                write(status, failures)
                result.returncode = code
                with self.assertRaises(Refused):
                    runner.outcome(result, 0, True)

    def test_render_only_pass_keeps_confirmed_failure_reference(self):
        runner = object.__new__(TemplateTests)
        runner.active = 'render_pass'
        self.assertEqual(runner.expected_result(0), (0, False, False))
        self.assertEqual(runner.expected_cached_result(0), (4, True, True))
        assert_payload(payload(4), *runner.expected_cached_result(0))
        with self.assertRaises(Refused):
            assert_payload(payload(0), *runner.expected_cached_result(0))
        with self.assertRaises(Refused):
            assert_payload(payload(4), *runner.expected_result(0))
        runner.active = 'body_pass_first'
        self.assertEqual(runner.expected_cached_result(0), (0, False, False))
        with self.assertRaises(Refused):
            assert_payload(payload(4), *runner.expected_cached_result(0))


if __name__ == '__main__':
    unittest.main()
