"""Offline probes of #198's capture boundary; no credentials or Warehouse."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from databricks_survey import Refused
from databricks_volatile import VolatileScope, check_physical


class VolatileCapture(unittest.TestCase):
    def test_exact_uuid_objects_in_evidenced_existing_demo_containers_only(self):
        with TemporaryDirectory() as temporary:
            schemas = {'prod': 'ds_2026111244ee6d_ref', 'ref': 'ds_20261112d134ef_ref'}
            path = Path(temporary) / 'ownership.json'
            path.write_text(json.dumps({'retained_schemas': list(schemas.values())}))
            lease = {'catalog': 'cdo_demo_db', 'roles': schemas,
                     'source_evidence': [{'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}]}
            identity = 'd3b0053c3e324a2fb53fbead2db65c85'
            scope = VolatileScope('20261119abcdef', lease, identity)
            relation = 'cdo_demo_db.ds_2026111244ee6d_ref.p198_' + identity + '_volatile'
            self.assertEqual(scope.relation('prod', 'volatile'), relation)
            for bad in (relation.replace('cdo_demo_db', 'other'),
                        relation.replace('44ee6d', 'd134ef'),
                        relation.replace(identity, 'a' * 32), relation + '__dbt_tmp',
                        relation.replace('_volatile', '_old_table')):
                with self.subTest(bad=bad), self.assertRaises(Refused):
                    scope.check(bad)
            path.write_text('{}')
            with self.assertRaisesRegex(Refused, 'digest changed'):
                VolatileScope('20261119abcdef', lease, identity)

    def test_full_physical_contract_rejects_wrong_or_incomplete_results(self):
        good = {'columns': [{'name': 'id', 'type_name': 'INT', 'type_text': 'INT'},
                            {'name': 'value', 'type_name': 'DOUBLE', 'type_text': 'DOUBLE'}],
                'rows': [['1', '0.25']]}
        properties = {'rows': [['Type', 'MANAGED', ''], ['Provider', 'delta', '']]}
        check_physical(good, properties, 'volatile')
        check_physical(good, properties, 'consumer')
        for mutation in ('empty', 'extra', 'id', 'range', 'nan', 'type', 'kind', 'provider'):
            actual, props = deepcopy(good), deepcopy(properties)
            if mutation == 'empty': actual['rows'] = []
            if mutation == 'extra': actual['rows'] *= 2
            if mutation == 'id': actual['rows'][0][0] = '2'
            if mutation == 'range': actual['rows'][0][1] = '1.5'
            if mutation == 'nan': actual['rows'][0][1] = 'NaN'
            if mutation == 'type': actual['columns'][1]['type_name'] = 'STRING'
            if mutation == 'kind': props['rows'][0][1] = 'VIEW'
            if mutation == 'provider': props['rows'][1][1] = 'parquet'
            with self.subTest(mutation=mutation), self.assertRaises(Refused):
                check_physical(actual, props, 'volatile')


if __name__ == '__main__':
    unittest.main()
