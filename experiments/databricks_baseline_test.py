"""Offline fault probes for the live baseline's independent physical contract."""
from copy import deepcopy
import unittest
import json
import hashlib
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

import databricks_baseline as baseline
from databricks_survey import Refused


class PhysicalContract(unittest.TestCase):
    def test_promoted_public_history_pins_each_lifecycle_and_both_explain_orders(self):
        fixture = Path(__file__).resolve().parent.parent / 'golden/fixtures/databricks_table_view_baseline.jsonl'
        rows = [json.loads(line) for line in fixture.read_text().splitlines()]
        self.assertEqual(Counter(row['method'] for row in rows), {
            'ValidateClientVersion':4, 'SubmitTelemetryBatch':10,
            'SubmitEnrichedSQLSpeculative':8, 'SubmitEnrichedSQL':8,
            'ConfirmExecution':4, 'GetExplainMessages':5})
        submits = [row for row in rows if row['method']=='SubmitEnrichedSQL']
        confirmed = {row['request']['request_id'] for row in rows
                     if row['method']=='ConfirmExecution' and row['response']['success'] is True}
        for offset, expected in [(0,'ready_to_execute'), (2,'skip_execution'),
                                 (4,'ready_to_execute'), (6,'skip_execution')]:
            pair = submits[offset:offset+2]
            self.assertEqual({row['request']['labels']['dbt_node_name'] for row in pair},
                             {'literal_table','literal_view'})
            for row in pair:
                self.assertIn(expected, row['response']['response'])
                if expected=='ready_to_execute':
                    self.assertIn(row['response']['response'][expected]['request_id'],confirmed)
        known = set()
        requests = []
        for row in rows:
            self.assertEqual(row['status']['code'],0)
            if row['method']=='SubmitEnrichedSQL':
                known.add(next(iter(row['response']['response'].values()))['execution_decision_id'])
            if row['method']=='GetExplainMessages':
                asked = row['request']['execution_decision_ids']
                returned = [m['execution_decision_id'] for m in row['response']['messages']]
                self.assertTrue(set(asked) <= known)
                self.assertEqual(sorted(asked),returned)
                self.assertTrue(all(m['explain_lines'] for m in row['response']['messages']))
                requests.append(asked)
        self.assertTrue(any(left==list(reversed(right)) for i,left in enumerate(requests)
                            for right in requests[i+1:]))

    def test_lease_requires_owned_schemas_and_fresh_run_prefixed_relations(self):
        with TemporaryDirectory() as temporary:
            evidence = Path(temporary) / 'configuration.json'
            roles = {'prod':'ds_20261112a134ef_ref', 'ref':'ds_20261112e134ef_ref'}
            evidence.write_text(json.dumps({'retained_schemas':list(roles.values())}))
            ledger = {'catalog':'cdo_demo_db', 'roles':roles,
                      'source_evidence':[{'path':str(evidence),'sha256':hashlib.sha256(evidence.read_bytes()).hexdigest()}],
                      'preexisting_objects':{schema:[] for schema in roles.values()}}
            scope = baseline.LeaseScope('20261112abcdef', ledger)
            relation = scope.relation('prod','literal_table')
            self.assertEqual(relation, 'cdo_demo_db.ds_20261112a134ef_ref.b_20261112abcdef_literal_table')
            for invalid in ['cdo_demo_db.ds_20261112a134ef_ref.inc_append',
                            'cdo_demo_db.ds_20261112a134ef_ref.literal_table',
                            'cdo_demo_db.ds_20261112a134ef_ref.b_20261112aaaaaa_literal_table',
                            'workspace.default.b_20261112abcdef_literal_table']:
                with self.subTest(invalid=invalid), self.assertRaises(Refused): scope.check(invalid)
            baseline.check_inventory(scope, ledger, {schema:[] for schema in roles.values()})
            collision = {schema:[] for schema in roles.values()}
            collision[roles['prod']] = [{'name':relation.split('.')[-1], 'full_name':relation, 'table_type':'MANAGED'}]
            with self.assertRaisesRegex(Refused, 'already exists'):
                baseline.check_inventory(scope, ledger, collision)
            collision[roles['prod']][0]['name'] += '__dbt_tmp'
            collision[roles['prod']][0]['full_name'] += '__dbt_tmp'
            with self.assertRaisesRegex(Refused, 'already exists'):
                baseline.check_inventory(scope, ledger, collision)
            evidence.write_text('{}')
            with self.assertRaisesRegex(Refused, 'digest changed'):
                baseline.LeaseScope('20261112abcdef', ledger)
            ledger['roles']['prod'] = 'shared'
            with self.assertRaises(Refused): baseline.LeaseScope('20261112abcdef', ledger)

    def test_rejects_wrong_skip_lost_duplicate_null_schema_and_relation_properties(self):
        good = {'columns': [dict(name=n, type_text=t, type_name=k, position=i)
                            for i, (n,t,k) in enumerate([
                                ('id','BIGINT','LONG'), ('amount','INT','INT'), ('tag','STRING','STRING')])],
                'rows': [['1','10','one'], ['1','10','one'], ['2',None,None]]}
        props = {'columns': [], 'rows': [['Type','MANAGED',''], ['Provider','delta','']]}
        baseline.assert_physical(good, props, 0, 'table')
        for mutation in ('value','duplicate','null','type','name','order','kind','provider','wrong-skip'):
            actual, properties = deepcopy(good), deepcopy(props)
            revision = 0
            if mutation == 'value': actual['rows'][0][1] = '11'
            if mutation == 'duplicate': actual['rows'].pop(0)
            if mutation == 'null': actual['rows'][-1][2] = ''
            if mutation == 'type': actual['columns'][1]['type_text'] = 'BIGINT'
            if mutation == 'name': actual['columns'][1]['name'] = 'wrong'
            if mutation == 'order': actual['columns'].reverse()
            if mutation == 'kind': properties['rows'][0][1] = 'VIEW'
            if mutation == 'provider': properties['rows'][1][1] = 'parquet'
            if mutation == 'wrong-skip': revision = 1
            failure = ('physical:relation-kind' if mutation == 'kind' else
                       'physical:provider' if mutation == 'provider' else
                       'physical:schema' if mutation in ('type','name','order') else 'physical:rows')
            with self.subTest(mutation=mutation), self.assertRaisesRegex(Refused, '^' + failure + '$'):
                baseline.assert_physical(actual, properties, revision, 'table')
        view = {'columns': [], 'rows': [['Type','VIEW','']]}
        baseline.assert_physical(good, view, 0, 'view')


if __name__ == '__main__':
    unittest.main()
