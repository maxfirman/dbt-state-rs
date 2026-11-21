"""Verify #238's hosted decisions and physical results from its portable records."""
import json
from pathlib import Path
import unittest

from databricks_upstream_origin import check_case, compare_reference, ordinary_submit
from databricks_survey import Refused


EVIDENCE = Path(__file__).resolve().parents[1] / 'docs/reports/evidence/databricks-upstream-origin'


def recorded(case):
    return json.loads((EVIDENCE / (case + '.json')).read_text())


class UpstreamOrigin(unittest.TestCase):
    def test_observed_downstream_outcomes(self):
        for case in 'ABCD':
            with self.subTest(case=case):
                record = recorded(case)
                steps = record['steps']
                check_case(record)
                self.assertEqual(compare_reference(record), {
                    'dev_downstream': True, 'dev_downstream_repeat': True,
                })
                first = ordinary_submit(steps['dev_downstream'])['response']['response']
                if case in 'AB':
                    source = first['ready_to_clone']['clone_source'].replace('`', '')
                    self.assertEqual(source, steps['prod_downstream']['relation'])
                    self.assertEqual(steps['dev_downstream']['copy_outcome'], 'confirmed_copy')
                else:
                    self.assertIn('ready_to_execute', first)
                repeat = ordinary_submit(steps['dev_downstream_repeat'])
                self.assertIn('skip_execution', repeat['response']['response'])

    def test_wrong_marker_identifies_the_affected_read(self):
        record = recorded('D')
        record['steps']['dev_downstream']['physical']['rows'] = [['D_prod']]
        self.assertTrue(compare_reference(record)['dev_downstream_repeat'])
        with self.assertRaisesRegex(Refused, 'D_dev.*dev_downstream.*D_prod'):
            check_case(record)


if __name__ == '__main__':
    unittest.main()
