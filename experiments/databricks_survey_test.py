"""Credential-free checks at the campaign's guarded CLI/result boundary."""
import json
from pathlib import Path
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import databricks_survey as survey


class SurveyContract(unittest.TestCase):
    def test_physical_reads_refuse_missing_or_inconsistent_result_envelopes(self):
        from copy import deepcopy
        good = {'status': {'state': 'SUCCEEDED'}, 'manifest': {
            'format': 'JSON_ARRAY', 'truncated': False, 'total_chunk_count': 1,
            'chunks': [{'chunk_index': 0, 'row_offset': 0, 'row_count': 1}],
            'total_row_count': 1, 'schema': {'column_count': 1, 'columns': [
                {'name': 'id', 'type_text': 'BIGINT', 'type_name': 'LONG', 'position': 0}]}},
            'result': {'chunk_index': 0, 'row_offset': 0, 'row_count': 1, 'data_array': [['1']]}}
        self.assertEqual(survey.complete_result(good, require_rows=True)['rows'], [['1']])
        for path in [('manifest',), ('result',), ('manifest', 'schema'),
                     ('manifest', 'chunks'), ('manifest', 'total_row_count'),
                     ('manifest', 'truncated'), ('result', 'row_count'),
                     ('result', 'data_array')]:
            broken = deepcopy(good)
            parent = broken
            for key in path[:-1]:
                parent = parent[key]
            del parent[path[-1]]
            with self.subTest(missing=path), self.assertRaises(survey.Refused):
                survey.complete_result(broken, require_rows=True)
        for field, value in [('row_offset', 1), ('chunk_index', 1), ('row_count', 0),
                             ('data_array', [[]]), ('data_array', [[1]])]:
            broken = deepcopy(good)
            broken['result'][field] = value
            with self.subTest(field=field), self.assertRaises(survey.Refused):
                survey.complete_result(broken, require_rows=True)
        self.assertEqual(survey.complete_result({'status': {'state': 'SUCCEEDED'}}),
                         {'columns': [], 'rows': []})
        empty = deepcopy(good)
        empty['manifest'].update(total_chunk_count=0, total_row_count=0)
        del empty['manifest']['chunks']
        empty['result'] = {}
        self.assertEqual(survey.complete_result(empty, require_rows=True)['rows'], [])

    def test_refuses_unowned_or_off_catalog_relations(self):
        scope = survey.Scope('20261112abcdef')
        self.assertEqual(scope.relation('raw', 'inputs'), 'cdo_demo_db.ds_20261112abcdef_raw.inputs')
        for name in ('workspace.default.inputs', 'cdo_demo_db.shared.inputs', 'cdo_demo_db.ds_20261112abcdef_raw.inputs; DROP SCHEMA x'):
            with self.assertRaises(survey.Refused):
                scope.check(name)

    def test_result_refuses_truncation_and_preserves_duplicates_nulls_and_types(self):
        response = {'status': {'state': 'SUCCEEDED'}, 'manifest': {'truncated': False, 'total_row_count': 3, 'schema': {'columns': [{'name': 'id', 'type_name': 'LONG'}]}}, 'result': {'data_array': [['1'], ['1'], [None]], 'row_count': 3}}
        actual = survey.complete_result(response)
        self.assertEqual(actual, {'columns': [{'name': 'id', 'type_name': 'LONG'}], 'rows': [['1'], ['1'], [None]]})
        response['manifest']['truncated'] = True
        with self.assertRaises(survey.Refused):
            survey.complete_result(response)

    def test_physical_comparison_ignores_row_order_but_preserves_multiplicity_and_schema(self):
        left = {'columns': [{'name': 'id', 'type_text': 'BIGINT'}], 'rows': [['1'], [None], ['1']]}
        right = {'columns': left['columns'], 'rows': [[None], ['1'], ['1']]}
        self.assertTrue(survey.same_result(left, right))
        right['rows'].pop()
        self.assertFalse(survey.same_result(left, right))
        right['rows'].append(['1'])
        right['columns'] = [{'name': 'id', 'type_text': 'INT'}]
        self.assertFalse(survey.same_result(left, right))

    def test_snapshot_oracle_requires_historical_versions_and_delete_semantics(self):
        columns = [{'name': name} for name in ['id','v','dbt_scd_id','dbt_valid_from','dbt_valid_to']]
        result = {'columns': columns, 'rows': [['1','one','old','a','b'],['1','changed','new','b',None],['2','two','two','a',None],['3','three','three','b',None]]}
        survey.check_snapshot(result, 'ignore', deleted=True)
        with self.assertRaises(survey.Refused):
            survey.check_snapshot(result, 'invalidate', deleted=True)
        result['rows'].pop(0)
        with self.assertRaises(survey.Refused):
            survey.check_snapshot(result, 'ignore', deleted=False)

    def test_manifest_refuses_an_escape_even_when_profile_is_safe(self):
        scope = survey.Scope('20261112abcdef')
        manifest = {'nodes': {'model.survey.x': {'resource_type': 'model', 'database': 'cdo_demo_db', 'schema': 'ds_20261112abcdef_prod', 'alias': 'x'}}, 'sources': {}}
        survey.check_manifest(scope, manifest)
        manifest['nodes']['model.survey.x']['database'] = 'workspace'
        with self.assertRaises(survey.Refused):
            survey.check_manifest(scope, manifest)

    def test_new_record_requires_the_deleted_business_key(self):
        columns = [{'name': name} for name in ['id','v','dbt_scd_id','dbt_valid_from','dbt_valid_to','dbt_is_deleted']]
        result = {'columns': columns, 'rows': [
            ['1','one','old','a','b','false'],
            ['1','changed','new','b',None,'false'],
            ['2','two','two','a','c','false'],
            ['3','three','three','b',None,'false'],
            ['2','two','deleted','c',None,'true'],
        ]}
        survey.check_snapshot(result, 'new_record', deleted=True)
        result['rows'][-1][0] = None
        with self.assertRaises(survey.Refused):
            survey.check_snapshot(result, 'new_record', deleted=True)

    def test_baseline_requires_each_build_confirmation_and_unchanged_reuse(self):
        build = {'method': 'SubmitValues', 'request': {'labels': {'dbt_node_unique_id': 'seed.survey.x'}}, 'response': {'response': {'ready_to_execute': {'request_id': 'r1'}}}}
        reuse = {'method': 'SubmitValues', 'request': build['request'], 'response': {'response': {'skip_execution': {}}}}
        with self.assertRaises(survey.Refused):
            survey.check_baseline([build], [reuse])
        confirm = {'method': 'ConfirmExecution', 'request': {'request_id': 'r1'}, 'response': {'success': True}}
        self.assertEqual(survey.check_baseline([build, confirm], [reuse]), ['seed.survey.x'])

    def test_dry_plan_needs_no_credentials_and_lists_every_required_surface(self):
        result = subprocess.run([sys.executable, str(Path(survey.__file__)), '--plan'], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        self.assertEqual(plan['incrementals'], ['append', 'delete+insert', 'merge', 'insert_overwrite', 'replace_where', 'microbatch'])
        self.assertEqual(plan['snapshots'], ['timestamp', 'check'])
        self.assertIn('RegisterClone', plan['copies'])
        self.assertIn('SubmitValues', plan['copies'])


if __name__ == '__main__':
    unittest.main()
