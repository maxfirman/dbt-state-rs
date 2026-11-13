"""Credential-free mutation boundaries for the opt-in identity campaign."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from databricks_identity import IdentityScope
from databricks_baseline import check_inventory
from databricks_survey import Refused


class IdentityOwnership(unittest.TestCase):
    def test_only_fresh_exact_names_in_evidenced_containers_are_allowed(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence=Path(directory)/'ownership.json'
            schemas=['ds_20261112a134ef_ref','ds_20261112e134ef_ref']
            evidence.write_text(json.dumps({'retained_schemas':schemas}))
            ledger={'catalog':'cdo_demo_db','roles':dict(zip(['prod','ref'],schemas)),
                    'source_evidence':[{'path':str(evidence),'sha256':hashlib.sha256(evidence.read_bytes()).hexdigest()}],
                    'preexisting_objects':{schema:[] for schema in schemas}}
            scope=IdentityScope('20261112136003',ledger)
            self.assertEqual(len(scope.allowed),6)
            for role in ('prod','ref'):
                for name in ('input','result','input_view'):
                    self.assertEqual(scope.check(scope.relation(role,name)),scope.relation(role,name))
            for relation in ['cdo_demo_db.'+schemas[0]+'.inc_append',
                             'cdo_demo_db.'+schemas[0]+'.i_20261112136002_input',
                             'workspace.'+schemas[0]+'.i_20261112136003_input',
                             'cdo_demo_db.default.i_20261112136003_input',
                             'cdo_demo_db.'+schemas[0]+'.i_20261112136003_result__dbt_tmp']:
                with self.subTest(relation=relation),self.assertRaises(Refused):scope.check(relation)
            inventory={schema:[] for schema in schemas}
            check_inventory(scope,ledger,inventory)
            inventory[schemas[0]]=[{'name':'i_20261112136003_result__dbt_tmp','full_name':'anything'}]
            with self.assertRaisesRegex(Refused,'already exists'):check_inventory(scope,ledger,inventory)
            evidence.write_text('{}')
            with self.assertRaisesRegex(Refused,'digest changed'):IdentityScope('20261112136003',ledger)


if __name__=='__main__':unittest.main()
