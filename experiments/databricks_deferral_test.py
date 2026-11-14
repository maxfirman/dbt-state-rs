"""Credential-free evidence and ownership gates for the deferred-read campaign."""
from collections import Counter
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from databricks_deferral import DeferralScope
from databricks_survey import Refused


class DeferralContract(unittest.TestCase):
    def test_missing_project_id_does_not_drop_actual_pinned_client_deferral(self):
        fixture = Path(__file__).resolve().parent.parent / 'golden/fixtures/databricks_deferral.jsonl'
        rows = [json.loads(line) for line in fixture.read_text().splitlines()]
        pending = {}
        confirmed = {}
        for row in rows:
            if row['method']=='SubmitEnrichedSQL' and 'ready_to_execute' in row['response']['response']:
                body=row['request']
                pending[row['response']['response']['ready_to_execute']['request_id']] = (
                    body['dbt_node_state']['node_unique_id'],body['target_table'])
            if row['method']=='ConfirmExecution' and row['response']['success']:
                uid,target=pending[row['request']['request_id']]
                confirmed[uid]=target
            if row['method']=='ResolveDeferredRelations':
                for uid,target in row['response']['fqn_by_unique_id'].items():
                    self.assertEqual(target,confirmed[uid])
        actual = [r for r in rows if r['method']=='ResolveDeferredRelations'
                  and r['request']['project_id'] is None
                  and r['request']['profile_name']=='survey'
                  and r['request']['project_name'].startswith('survey_')
                  and r['request']['target_name']=='prod']
        self.assertGreaterEqual(len(actual), 2)
        for row in actual:
            for uid, fqn in row['response']['fqn_by_unique_id'].items():
                self.assertEqual(fqn, confirmed[uid])
                self.assertEqual(fqn.count('`'), 6)
        self.assertTrue(any(r['response']['fqn_by_unique_id'] for r in actual))
        self.assertTrue(any(r['method']=='SubmitEnrichedSQL'
                            and r['request']['labels']['dbt_node_name']=='downstream'
                            and 'skip_execution' in r['response']['response'] for r in rows))

    def test_protocol_capture_pins_both_warehouse_lifecycles_without_gaps(self):
        fixture=Path(__file__).resolve().parent.parent/'golden/fixtures/databricks_deferral_scopes.jsonl'
        rows=[json.loads(line) for line in fixture.read_text().splitlines()]
        self.assertEqual(Counter(r['method'] for r in rows), {
            'SubmitEnrichedSQL':16,'ConfirmExecution':10,'ResolveDeferredRelations':46})
        for dialect in ('snowflake','databricks'):
            submits=[r for r in rows if r['method']=='SubmitEnrichedSQL' and r['request']['dialect']==dialect]
            self.assertEqual(len(submits),8)
        self.assertEqual(sum(r['request']['failed_to_clone'] for r in rows if r['method']=='ConfirmExecution'),4)
        self.assertTrue(all(r['status']['code']==0 for r in rows))

    def test_exact_allowlist_preserves_existing_and_other_campaign_relations(self):
        with TemporaryDirectory() as temporary:
            evidence = Path(temporary)/'evidence.json'
            roles = {'prod':'ds_20261112a134ef_ref','ref':'ds_20261112e134ef_ref'}
            evidence.write_text(json.dumps({'retained_schemas':list(roles.values())}))
            ledger={'catalog':'cdo_demo_db','roles':roles,
                    'source_evidence':[{'path':str(evidence),'sha256':hashlib.sha256(evidence.read_bytes()).hexdigest()}]}
            scope=DeferralScope('20261113d155ac',ledger)
            self.assertEqual(scope.schema('dev'),roles['ref'])
            scope.check('cdo_demo_db.'+roles['prod']+'.d_20261113d155ac_upstream')
            for relation in ['inc_append','d_20261113d155aa_upstream','f_20261113d155ac_inputs',
                             'd_20261113d155ac_upstream__dbt_tmp','d_20261113d155ac_undeclared']:
                with self.subTest(relation=relation),self.assertRaises(Refused):
                    scope.check('cdo_demo_db.'+roles['prod']+'.'+relation)


if __name__=='__main__':
    unittest.main()
