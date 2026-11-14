"""Configuration outcomes use declared flags independently of client severity."""
import json
from pathlib import Path
import tempfile
import unittest

from databricks_cached_config import ConfigTests
from databricks_cached_tests import CommandResult, assert_payload, payload
from databricks_survey import Refused


class ConfigOutcomes(unittest.TestCase):
    def test_warn_retains_raw_error_flag_but_rejects_fail_exit_and_stale_flags(self):
        runner = object.__new__(ConfigTests)
        runner.active = 'severity_warn'
        runner.uid = 'test.config.literal_outcome'
        self.assertEqual(runner.expected_result(3), (3, True, True))
        assert_payload(payload(3), *runner.expected_result(3))
        with self.assertRaises(Refused):
            assert_payload(payload(3, False, True), *runner.expected_result(3))
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            row = {'unique_id': runner.uid, 'status': 'warn', 'failures': 3}
            (target / 'run_results.json').write_text(json.dumps({'results': [row]}))
            result = CommandResult(0, '', target, [])
            runner.outcome(result, 3, False)
            result.returncode = 1
            with self.assertRaises(Refused):
                runner.outcome(result, 3, False)

    def test_threshold_pass_does_not_erase_nonzero_failures(self):
        runner = object.__new__(ConfigTests)
        runner.active = 'threshold_pass'
        self.assertEqual(runner.expected_result(3), (3, False, False))
        assert_payload(payload(3, False, False), *runner.expected_result(3))
        for stale in (payload(0), payload(3), payload(3, False, True)):
            with self.assertRaises(Refused):
                assert_payload(stale, *runner.expected_result(3))
        runner.uid = 'test.config.literal_outcome'
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            row = {'unique_id': runner.uid, 'status': 'pass', 'failures': 3}
            artifact = target / 'run_results.json'
            artifact.write_text(json.dumps({'results': [row]}))
            runner.outcome(CommandResult(0, '', target, []), 3, False)
            for changed in ({**row, 'failures': 0}, {**row, 'status': 'fail'}):
                artifact.write_text(json.dumps({'results': [changed]}))
                with self.assertRaises(Refused):
                    runner.outcome(CommandResult(0, '', target, []), 3, False)

    def test_count_and_sum_results_reject_null_or_duplicate_loss(self):
        runner = object.__new__(ConfigTests)
        runner.uid = 'test.config.literal_outcome'
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            artifact = target / 'run_results.json'
            for case, count, status, exit_code, wrong_count in [
                    ('count_v', 2, 'warn', 0, 3), ('sum_v', 4, 'fail', 1, 2)]:
                runner.active = case
                row = {'unique_id': runner.uid, 'status': status, 'failures': count}
                artifact.write_text(json.dumps({'results': [row]}))
                runner.outcome(CommandResult(exit_code, '', target, []), 3, False)
                artifact.write_text(json.dumps({'results': [{**row, 'failures': wrong_count}]}))
                with self.assertRaises(Refused):
                    runner.outcome(CommandResult(exit_code, '', target, []), 3, False)


if __name__ == '__main__':
    unittest.main()
