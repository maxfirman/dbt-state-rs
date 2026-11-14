#!/usr/bin/env python3
"""Credential-free exact ownership and independent typed-result fault checks."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from databricks_dependency_identity import IdentityScope
from databricks_baseline import assert_physical
from databricks_survey import Refused


class IdentityContractTests(unittest.TestCase):
    def test_allowlist_excludes_other_roles_old_runs_and_prior_objects(self):
        with tempfile.TemporaryDirectory() as temp:
            evidence = Path(temp) / 'ownership.json'
            schemas = ['ds_20261112a134ef_ref', 'ds_20261112e134ef_ref']
            evidence.write_text(json.dumps({'retained_schemas': schemas}))
            lease = {'catalog': 'cdo_demo_db', 'roles': dict(zip(['prod', 'ref'], schemas)),
                     'source_evidence': [{'path': str(evidence), 'sha256': hashlib.sha256(evidence.read_bytes()).hexdigest()}]}
            scope = IdentityScope('20261114a138aa', lease)
            self.assertEqual(len(scope.allowed), 9)
            self.assertEqual(scope.relation('prod', 'dependency'),
                             'cdo_demo_db.ds_20261112a134ef_ref.d_20261114a138aa_dependency')
            self.assertEqual(scope.relation('ref', 'reference'),
                             'cdo_demo_db.ds_20261112e134ef_ref.d_20261114a138aa_reference')
            for value in ['cdo_demo_db.ds_20261112a134ef_ref.inc_merge',
                          'cdo_demo_db.ds_20261112a134ef_ref.d_20261114a138ab_dependency',
                          'cdo_demo_db.ds_20261112e134ef_ref.d_20261114a138aa_dependency',
                          'other.ds_20261112a134ef_ref.d_20261114a138aa_dependency']:
                with self.subTest(value=value), self.assertRaises(Refused):
                    scope.check(value)

    def test_wrong_reuse_duplicate_null_schema_and_properties_fail_precisely(self):
        result = {'columns': [{'name': 'id', 'type_text': 'BIGINT', 'type_name': 'LONG'},
                              {'name': 'amount', 'type_text': 'INT', 'type_name': 'INT'},
                              {'name': 'tag', 'type_text': 'STRING', 'type_name': 'STRING'}],
                  'rows': [['1', '10', 'one_trace'], ['1', '10', 'one_trace'], ['2', None, None]]}
        properties = {'rows': [['Type', 'MANAGED'], ['Provider', 'delta']]}
        assert_physical(result, properties, 0, 'table', 'trace')
        for fault, expected in [('stale', 'rows'), ('duplicate', 'rows'), ('null', 'rows'),
                                ('type', 'schema'), ('order', 'schema'), ('name', 'schema'),
                                ('clone', 'relation-kind'), ('provider', 'provider')]:
            actual, props = copy.deepcopy(result), copy.deepcopy(properties)
            revision = 0
            if fault == 'stale': revision = 1
            if fault == 'duplicate': actual['rows'].pop(0)
            if fault == 'null': actual['rows'][2][1] = '0'
            if fault == 'type': actual['columns'][0]['type_text'] = 'INT'
            if fault == 'order': actual['columns'].reverse()
            if fault == 'name': actual['columns'][0]['name'] = 'wrong'
            if fault == 'clone': props['rows'][0][1] = 'MANAGED_SHALLOW_CLONE'
            if fault == 'provider': props['rows'][1][1] = 'parquet'
            with self.subTest(fault=fault), self.assertRaisesRegex(Refused, '^physical:' + expected + '$'):
                assert_physical(actual, props, revision, 'table', 'trace')


if __name__ == '__main__':
    unittest.main()
