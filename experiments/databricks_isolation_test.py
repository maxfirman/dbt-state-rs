"""Credential-free checks of the concurrent runner's destructive SQL boundary."""
import unittest
import json
from collections import Counter
from pathlib import Path
from databricks_baseline import assert_physical
from databricks_survey import Refused
from databricks_isolation import cleanup_plan


class OwnershipBoundary(unittest.TestCase):
    def test_cleanup_refuses_peer_relation_even_in_same_schema(self):
        own = {'cdo_demo_db.owned.i_a_literal_table'}
        with self.assertRaisesRegex(Refused, 'outside this run'):
            cleanup_plan(own, ['cdo_demo_db.owned.i_b_literal_table'])

    def test_cleanup_names_only_exact_declared_created_relations(self):
        relations = {'cdo_demo_db.owned.i_a_literal_table', 'cdo_demo_db.reference.i_a_literal_table'}
        self.assertEqual(cleanup_plan(relations, relations), [
            'DROP TABLE cdo_demo_db.owned.i_a_literal_table',
            'DROP TABLE cdo_demo_db.reference.i_a_literal_table'])

    def test_cleanup_rejects_quoted_or_injected_input(self):
        own = {'cdo_demo_db.owned.i_a_literal_table'}
        for relation in ('`cdo_demo_db.owned.i_a_literal_table`',
                         'cdo_demo_db.owned.i_a_literal_table; DROP SCHEMA cdo_demo_db.owned'):
            with self.assertRaisesRegex(Refused, 'outside this run'):
                cleanup_plan(own, [relation])

    def test_peer_result_with_equal_schema_and_row_count_is_rejected(self):
        actual = {'columns': [dict(name=n, type_text=t, type_name=k) for n,t,k in
                             [('id','BIGINT','LONG'), ('amount','INT','INT'), ('tag','STRING','STRING')]],
                  'rows': [['1','10','one_20261113a159bb'], ['1','10','one_20261113a159bb'], ['2',None,None]]}
        props = {'rows': [['Type','MANAGED'], ['Provider','delta']]}
        with self.assertRaisesRegex(Refused, '^physical:rows$'):
            assert_physical(actual, props, 0, 'table', '20261113a159ba')

    def test_promoted_captures_pin_complete_confirmations_reuse_and_explain(self):
        fixtures = Path(__file__).resolve().parent.parent / 'golden/fixtures'
        for name, counts in [
                ('scopes', {'SubmitEnrichedSQL':49, 'ConfirmExecution':15, 'GetExplainMessages':49}),
                ('concurrent_a', {'ValidateClientVersion':2, 'SubmitTelemetryBatch':5,
                                  'SubmitEnrichedSQLSpeculative':2, 'SubmitEnrichedSQL':2,
                                  'ConfirmExecution':1, 'GetExplainMessages':3}),
                ('concurrent_b', {'ValidateClientVersion':3, 'SubmitTelemetryBatch':7,
                                  'SubmitEnrichedSQLSpeculative':3, 'SubmitEnrichedSQL':3,
                                  'ConfirmExecution':1, 'GetExplainMessages':4})]:
            rows = [json.loads(line) for line in (fixtures / ('databricks_isolation_' + name + '.jsonl')).read_text().splitlines()]
            self.assertEqual(Counter(row['method'] for row in rows), counts)
            pending, known = set(), set()
            skips = 0
            for row in rows:
                self.assertEqual(row['status']['code'], 0)
                if row['method'] == 'SubmitEnrichedSQL':
                    response = row['response']['response']
                    result = next(iter(response.values()))
                    known.add(result['execution_decision_id'])
                    if 'ready_to_execute' in response:
                        pending.add(result['request_id'])
                    if 'skip_execution' in response:
                        skips += 1
                if row['method'] == 'ConfirmExecution':
                    self.assertIn(row['request']['request_id'], pending)
                    self.assertTrue(row['response']['success'])
                if row['method'] == 'GetExplainMessages':
                    self.assertTrue(set(row['request']['execution_decision_ids']) <= known)
                    self.assertEqual(set(row['request']['execution_decision_ids']),
                                     {message['execution_decision_id'] for message in row['response']['messages']})
                    self.assertTrue(all(message['explain_lines'] for message in row['response']['messages']))
            self.assertGreater(skips, 0)

    def test_additional_controls_keep_complete_statuses_and_typed_confirmations(self):
        fixtures = Path(__file__).resolve().parent.parent / 'golden/fixtures'
        expected = {
            'missing_namespace': (17, 3, {10}),
            'full_controls': (27, 7, {1}),
            'diagnostic_inputs': (16, 4, {10}),
            'diagnostic_alias_scopes': (15, 5, {10}),
            'diagnostic_identity': (14, 2, {10}),
            'refusal_confirmation': (12, 6, {1, 10}),
        }
        for name, (submits, confirms, kinds) in expected.items():
            rows = [json.loads(line) for line in (fixtures / ('databricks_isolation_' + name + '.jsonl')).read_text().splitlines()]
            self.assertEqual(Counter(row['method'] for row in rows),
                             {'SubmitEnrichedSQL':submits, 'GetExplainMessages':submits, 'ConfirmExecution':confirms})
            known, pending, actual_kinds = set(), {}, set()
            for row in rows:
                self.assertEqual(row['status']['code'], 0)
                if row['method'] == 'SubmitEnrichedSQL':
                    actual_kinds.add(row['request']['execution_type'])
                    value = next(iter(row['response']['response'].values()))
                    known.add(value['execution_decision_id'])
                    if 'request_id' in value:
                        pending[value['request_id']] = row['request']['execution_type']
                elif row['method'] == 'ConfirmExecution':
                    self.assertIn(row['request']['request_id'], pending)
                    self.assertTrue(row['response']['success'])
                    if name in ('full_controls', 'refusal_confirmation'):
                        self.assertEqual(row['request']['table_type'],
                                         'MANAGED' if pending[row['request']['request_id']] == 1 else 'VIEW')
                else:
                    self.assertTrue(set(row['request']['execution_decision_ids']) <= known)
                    self.assertEqual(set(row['request']['execution_decision_ids']),
                                     {m['execution_decision_id'] for m in row['response']['messages']})
                    self.assertTrue(all(m['explain_lines'] for m in row['response']['messages']))
            self.assertEqual(actual_kinds, kinds)


if __name__ == '__main__':
    unittest.main()
