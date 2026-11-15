#!/usr/bin/env python3
"""Independent window ledger faults; no credentials or warehouse writes."""
from collections import Counter
from datetime import date
import unittest
from databricks_moving_windows import plan, replace_window
from databricks_microbatch import assert_typed, SCHEMA
from databricks_survey import Refused


class WindowModel(unittest.TestCase):
    def test_lookback_and_predicate_preserve_the_independent_cumulative_bags(self):
        cases=plan(date(2026,11,15));previous=()
        for c in cases:
            if not c['repeat']:
                previous=replace_window(previous,c['source'],c['window'],c['predicate'])
            self.assertEqual(Counter(previous),Counter(c['R']),c['label'])
        # These errors each lose a different part of the declared operation.
        self.assertNotEqual(Counter(cases[2]['R']),Counter(cases[2]['F']))
        self.assertNotEqual(Counter(cases[4]['R']),Counter(cases[2]['R']))
        self.assertNotEqual(Counter(cases[6]['R']),Counter(cases[6]['F']))
        self.assertNotEqual(Counter(cases[8]['R']),Counter(cases[6]['R']))
        self.assertEqual(len(cases[2]['F']),3) # duplicates are material

    def test_physical_result_seam_rejects_lost_retention_stale_lookback_and_deduplication(self):
        cases=plan(date(2026,11,15))
        columns=[dict(zip(('name','type_text','type_name','position'),c)) for c in SCHEMA]
        for c,wrong in [(cases[2],cases[2]['F']), # replacing the whole target loses retained history
                        (cases[4],cases[2]['R']), # ignoring lookback leaves the old previous-day row
                        (cases[6],cases[6]['F']), # ignoring the predicate loses the excluded current-day row
                        (cases[8],cases[6]['R']), # ignoring widened bounds leaves that row stale
                        (cases[8],tuple(set(cases[8]['R'])))]: # DISTINCT loses a real duplicate
            with self.assertRaises(Refused,msg=c['label']):
                assert_typed({'columns':columns,'rows':wrong},c['R'])


if __name__=='__main__':unittest.main()
