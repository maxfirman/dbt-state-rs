#!/usr/bin/env python3
"""Credential-free physical-result negative checks for the #140 live seam."""
from copy import deepcopy
import unittest
import json
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from databricks_freshness import assert_rows, FreshnessScope
from databricks_survey import Refused


class FreshnessOracle(unittest.TestCase):
    def test_lease_refuses_prior_objects_and_undeclared_run_relations(self):
        with TemporaryDirectory() as directory:
            evidence = Path(directory) / 'ownership.json'
            roles = {'prod':'ds_20261112a134ef_ref', 'ref':'ds_20261112e134ef_ref'}
            evidence.write_text(json.dumps({'retained_schemas':list(roles.values())}))
            ledger = {'catalog':'cdo_demo_db', 'roles':roles,
                      'source_evidence':[{'path':str(evidence),
                       'sha256':hashlib.sha256(evidence.read_bytes()).hexdigest()}]}
            scope = FreshnessScope('20261112abcdef', ledger)
            self.assertEqual(scope.relation('prod','inputs'),
                             'cdo_demo_db.ds_20261112a134ef_ref.f_20261112abcdef_inputs')
            for value in ['cdo_demo_db.ds_20261112a134ef_ref.inc_append',
                          'cdo_demo_db.ds_20261112a134ef_ref.f_20261112abcdef_undeclared',
                          'cdo_demo_db.ds_20261112a134ef_ref.f_20261112aaaaaa_inputs',
                          'another.ds_20261112a134ef_ref.f_20261112abcdef_inputs']:
                with self.subTest(value=value), self.assertRaises(Refused):
                    scope.check(value)

    def test_rejects_stale_reuse_duplicate_loss_null_and_schema_changes(self):
        expected = [['1','17','tag'],['1','17','tag'],['2',None,None]]
        actual = {'columns':[{'name':n,'type_text':t,'type_name':k} for n,t,k in
                            [('id','BIGINT','LONG'),('amount','INT','INT'),('tag','STRING','STRING')]],
                  'rows':deepcopy(expected)}
        assert_rows(actual, expected)
        for change in ('stale','duplicate','null','schema'):
            wrong = deepcopy(actual)
            if change == 'stale': wrong['rows'][0][1] = '10'
            if change == 'duplicate': wrong['rows'].pop(0)
            if change == 'null': wrong['rows'][-1][-1] = ''
            if change == 'schema': wrong['columns'][1]['type_text'] = 'BIGINT'
            with self.subTest(change=change), self.assertRaisesRegex(Refused, '^physical:' + ('schema' if change == 'schema' else 'rows') + '$'):
                assert_rows(wrong, expected)


if __name__ == '__main__':
    unittest.main()
