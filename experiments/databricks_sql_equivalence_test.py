#!/usr/bin/env python3
"""Credential-free ownership and independent-oracle sensitivity checks."""
import copy
import unittest

from databricks_sql_equivalence import SqlScope, assert_result, cells, cte_cells
from databricks_survey import Refused


class SqlOracleTests(unittest.TestCase):
    def test_separate_campaigns_match_the_two_complete_hosted_histories(self):
        from pathlib import Path
        import json
        contract = json.loads(Path(__file__).with_name('databricks_sql_equivalence_contract.json').read_text())
        main = [label for label, _, _ in cells('cdo_demo_db.owned.input')]
        cte = [label for label, _, _ in cte_cells('cdo_demo_db.owned.input')]
        self.assertEqual((len(main), len(cte)), (26, 4))
        self.assertEqual(set(main) | set(cte), set(contract))
        self.assertFalse(set(main) & set(cte))
        self.assertEqual(main[0], 'first')
        self.assertEqual(cte[0], 'cte_control_first')

    def test_full_multiset_schema_and_nulls_are_required(self):
        # Literal expected result; no SQL parsing or production fingerprinting.
        actual = {'columns':[{'name':'id','type_text':'BIGINT','type_name':'LONG'},
                             {'name':'amount','type_text':'INT','type_name':'INT'},
                             {'name':'tag','type_text':'STRING','type_name':'STRING'}],
                  'rows':[['1','10','one_run'],['1','10','one_run'],['2',None,None]]}
        assert_result(actual,'original','run')
        for mutation in ('duplicate','value','null','column_name','type'):
            changed=copy.deepcopy(actual)
            if mutation=='duplicate': changed['rows'].pop(0)
            if mutation=='value': changed['rows'][0][1]='17'
            if mutation=='null': changed['rows'][2][1]='0'
            if mutation=='column_name': changed['columns'][0]['name']='renamed_id'
            if mutation=='type': changed['columns'][0]['type_text']='INT'
            with self.subTest(mutation=mutation), self.assertRaisesRegex(Refused,'physical:'):
                assert_result(changed,'original','run')

    def test_run_names_are_separate_from_prior_campaigns(self):
        # Exercise the final exact allowlist without constructing a cloud client.
        scope=object.__new__(SqlScope)
        scope.roles={'prod':'owned_a','ref':'owned_b'}
        scope.schemas=set(scope.roles.values())
        scope.relation_prefix='q_fresh_'
        scope.allowed={'cdo_demo_db.owned_a.q_fresh_result'}
        self.assertEqual(scope.relation('prod','result'),'cdo_demo_db.owned_a.q_fresh_result')
        for value in ('cdo_demo_db.owned_a.result','cdo_demo_db.other.q_fresh_result',
                      'cdo_demo_db.owned_a.q_prior_result','other.owned_a.q_fresh_result'):
            with self.subTest(value=value), self.assertRaises(Refused):
                scope.check(value)


if __name__=='__main__':
    unittest.main()
