"""Independent seed oracle rejects wrong data, widths and physical properties."""
from copy import deepcopy
import unittest
import json
from pathlib import Path
from collections import Counter

from databricks_seeds import assert_seed
from databricks_survey import Refused


class SeedPhysicalContract(unittest.TestCase):
    def test_pinned_hosted_history_requires_loads_reuses_config_and_producer_changes(self):
        fixture = Path(__file__).resolve().parent.parent / 'golden/fixtures/databricks_managed_seeds.jsonl'
        rows = [json.loads(line) for line in fixture.read_text().splitlines()]
        self.assertEqual(Counter(r['method'] for r in rows), {
            'ValidateClientVersion': 20, 'SubmitTelemetryBatch': 41, 'SubmitValues': 10,
            'ConfirmExecution': 10, 'GetExplainMessages': 20,
            'SubmitEnrichedSQLSpeculative': 10, 'SubmitEnrichedSQL': 10})
        for method in ('SubmitValues', 'SubmitEnrichedSQL'):
            submits = [r for r in rows if r['method'] == method]
            self.assertEqual([next(iter(r['response']['response'])) for r in submits],
                             ['ready_to_execute', 'skip_execution'] * 5)
        seeds = [r['request'] for r in rows if r['method'] == 'SubmitValues']
        self.assertNotEqual(seeds[0]['values_hash'], seeds[2]['values_hash'])
        self.assertEqual(seeds[2]['values_hash'], seeds[4]['values_hash'])
        self.assertEqual(json.loads(seeds[2]['semantic_extras']['column_types'])['amount'], 'int')
        self.assertEqual(json.loads(seeds[4]['semantic_extras']['column_types'])['amount'], 'bigint')
        self.assertEqual(json.loads(seeds[6]['semantic_extras']['delimiter']), '|')
        self.assertEqual(seeds[6]['values_hash'], seeds[8]['values_hash'])
        self.assertEqual(seeds[6]['semantic_extras']['quote_columns'], 'false')
        self.assertEqual(seeds[8]['semantic_extras']['quote_columns'], 'true')
        known, pending, explained = set(), set(), set()
        for row in rows:
            self.assertEqual(row['status']['code'], 0)
            if row['method'] in ('SubmitValues', 'SubmitEnrichedSQL'):
                payload = next(iter(row['response']['response'].values()))
                known.add(payload['execution_decision_id'])
                if 'request_id' in payload:
                    pending.add(payload['request_id'])
            if row['method'] == 'ConfirmExecution':
                self.assertIn(row['request']['request_id'], pending)
                self.assertTrue(row['response']['success'])
                pending.remove(row['request']['request_id'])
            if row['method'] == 'GetExplainMessages':
                asked = set(row['request']['execution_decision_ids'])
                self.assertTrue(asked <= known)
                messages = row['response']['messages']
                self.assertEqual({m['execution_decision_id'] for m in messages}, asked)
                self.assertTrue(all(m['explain_lines'] for m in messages))
                explained.update(asked)
        self.assertEqual(pending, set())
        self.assertEqual(explained, known)
        explanations = [r['response']['messages'][0] for r in rows if r['method'] == 'GetExplainMessages']
        for index in (5, 9, 13, 17):
            self.assertIn('an upstream model query has changed', json.dumps(explanations[index]))

    def test_wrong_skip_duplicate_null_type_and_properties_fail_precisely(self):
        actual = {'columns': [{'name': 'id', 'type_text': 'BIGINT', 'type_name': 'LONG'},
                              {'name': 'amount', 'type_text': 'INT', 'type_name': 'INT'},
                              {'name': 'tag', 'type_text': 'STRING', 'type_name': 'STRING'}],
                  'rows': [['1', '10', 'one_20261113abcdef'], ['1', '10', 'one_20261113abcdef'], ['2', None, None]]}
        properties = {'rows': [['Type', 'MANAGED'], ['Provider', 'delta']]}
        assert_seed(actual, properties, 0, '20261113abcdef')
        for mutation, fingerprint in [('stale', 'rows'), ('duplicate', 'rows'), ('null', 'rows'),
                                      ('width', 'schema'), ('order', 'schema'), ('column', 'schema'),
                                      ('clone', 'relation-kind'), ('provider', 'provider')]:
            changed, props = deepcopy(actual), deepcopy(properties)
            revision = 0
            if mutation == 'stale': revision = 1
            if mutation == 'duplicate': changed['rows'].pop(0)
            if mutation == 'null': changed['rows'][2][2] = ''
            if mutation == 'width': changed['columns'][1].update(type_text='BIGINT', type_name='LONG')
            if mutation == 'order': changed['columns'].reverse()
            if mutation == 'column': changed['columns'][1]['name'] = 'wrong'
            if mutation == 'clone': props['rows'][0][1] = 'MANAGED_SHALLOW_CLONE'
            if mutation == 'provider': props['rows'][1][1] = 'parquet'
            with self.subTest(mutation=mutation), self.assertRaisesRegex(Refused, '^physical:' + fingerprint + '$'):
                assert_seed(changed, props, revision, '20261113abcdef')
        with self.assertRaisesRegex(Refused, '^physical:schema$'):
            assert_seed(actual, properties, 0, '20261113abcdef', 'BIGINT')


if __name__ == '__main__':
    unittest.main()
