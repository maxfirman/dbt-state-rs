"""Credential-free checks at the opt-in generated replay input boundary."""
import unittest
from databricks_generated import validate_plan
from databricks_survey import Refused


class PlanTests(unittest.TestCase):
    def test_replay_accepts_exported_bounded_plan_and_rejects_arbitrary_sql(self):
        plan = {'version':1, 'seed':0, 'literal':False, 'operations':[
            {'op':'Observe','required':'BUILD','depends_on':None},
            {'op':'Observe','required':'SKIP','depends_on':0},
            {'op':{'Insert':[3,12]},'required':'BUILD','depends_on':1},
            {'op':'Observe','required':'SKIP','depends_on':2}]}
        self.assertEqual(validate_plan(plan), plan)
        plan['operations'][2]['op'] = {'SQL':'DROP SCHEMA anything'}
        with self.assertRaises(Refused):
            validate_plan(plan)

    def test_invalid_source_history_is_refused_before_any_live_setup(self):
        for literal, operations in [
            (False, [{'K':0}]), (False, [{'Update':10}]),
            (True, [{'Literal':10}]), (False, [{'Delete':99}]),
            (False, [{'Insert':[3,12]}, 'Observe', {'Insert':[3,13]}]),
        ]:
            all_ops = ['Observe','Observe'] + operations
            plan = {'version':1,'seed':0,'literal':literal,'operations':[
                {'op':op,'required':'SKIP' if i and op == 'Observe' else 'BUILD',
                 'depends_on':i-1 if i else None} for i,op in enumerate(all_ops)]}
            with self.subTest(operations=operations), self.assertRaises(Refused):
                validate_plan(plan)


if __name__ == '__main__':
    unittest.main()
