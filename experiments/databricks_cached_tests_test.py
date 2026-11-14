"""Offline controls for the no-object cached-result acceptance boundary."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from databricks_survey import Refused
from databricks_cached_tests import (
    assert_payload, assert_literal_rows, select_only, payload, client_sql,
    CachedTests, CommandResult,
)


class CachedResultOracle(unittest.TestCase):
    def test_only_complete_selected_logical_failure_explains_observed_exit_one(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            result = CommandResult(1, 'logical test failure', target, [])
            runner = object.__new__(CachedTests)
            runner.uid = 'test.p.literal_outcome'
            row = {'unique_id': runner.uid, 'status': 'fail', 'failures': 3}
            artifact = target / 'run_results.json'
            artifact.write_text(json.dumps({'results': [row]}))
            runner.outcome(result, 3, False)
            for code in (0, 2, 124):
                result.returncode = code
                with self.assertRaises(Refused):
                    runner.outcome(result, 3, False)
            result.returncode = 1
            for changed in ({**row, 'failures': 0}, {**row, 'status': 'error'},
                            {**row, 'unique_id': 'test.p.other'}):
                artifact.write_text(json.dumps({'results': [changed]}))
                with self.assertRaises(Refused):
                    runner.outcome(result, 3, False)

    def test_complete_typed_failing_payload_rejects_stale_and_malformed_results(self):
        assert_payload(payload(3), 3)
        mutations = [payload(0), payload(2), payload(4)]
        for field, value in [('failures', {'bool_value': True}),
                             ('failures', {'string_value': '3'}),
                             ('should_error', {'bool_value': False}),
                             ('should_warn', {'bool_value': False})]:
            changed = payload(3)
            changed['fields'][field]['kind'] = value
            mutations.append(changed)
        changed = payload(3)
        del changed['fields']['should_warn']
        mutations.append(changed)
        for changed in mutations:
            with self.subTest(changed=changed), self.assertRaises(Refused):
                assert_payload(changed, 3)

    def test_literal_multiset_preserves_duplicate_null_and_schema(self):
        result = {'columns': [{'name': n, 'type_text': 'INT', 'type_name': 'INT', 'position': i}
                              for i, n in enumerate(('id', 'v'))],
                  'rows': [['1', '2'], ['1', '2'], ['2', None]]}
        assert_literal_rows(result, 3)
        for mutate in (lambda r: r['rows'].pop(),
                       lambda r: r['rows'][2].__setitem__(1, '0'),
                       lambda r: r['columns'].reverse(),
                       lambda r: r['columns'][0].__setitem__('type_name', 'BIGINT')):
            changed = copy.deepcopy(result)
            mutate(changed)
            with self.assertRaises(Refused):
                assert_literal_rows(changed, 3)

    def test_select_guard_rejects_metadata_relations_and_mutations(self):
        select_only('SELECT cast(1 as int) AS id')
        for sql in ('DESCRIBE HISTORY x', 'CREATE TABLE x AS SELECT 1',
                    'SELECT * FROM x', 'SELECT 1; DROP TABLE x',
                    'SELECT * FROM `cdo_demo_db`.information_schema.tables'):
            with self.subTest(sql=sql), self.assertRaises(Refused):
                select_only(sql)

    def test_exact_audit_metadata_does_not_authorize_audit_data_or_other_names(self):
        schema = 'ds_20261112a134ef_ref_dbt_test__audit'
        exact = f'DESCRIBE HISTORY `cdo_demo_db`.`{schema}`.`literal_outcome` LIMIT 1'
        self.assertEqual(client_sql(exact, schema), 'read-only-audit-metadata')
        for sql in (exact.replace('literal_outcome', 'other'),
                    f'SELECT * FROM `cdo_demo_db`.`{schema}`.`literal_outcome`'):
            with self.assertRaises(Refused):
                client_sql(sql, schema)


if __name__ == '__main__':
    unittest.main()
