"""Fault checks before the exact #153 native campaign reaches a warehouse."""
import json
from pathlib import Path
import tempfile
import unittest

import databricks_submit_copy as p
from databricks_precopy import expected
from databricks_survey import Refused


class SubmitCopyTests(unittest.TestCase):
    def test_plan_is_exact_small_and_new(self):
        plan=p.plan()
        self.assertEqual(plan['managed_per_backend'],len(p.CELLS)*2)
        self.assertEqual(plan['permanent_per_backend'],5)
        names=[n for names in plan['names'].values() for n in names]
        self.assertEqual(len(names),len(set(names)))
        self.assertTrue(all('p152_' not in n and n.startswith('cdo_demo_db.') for n in names))
        self.assertEqual(expected('full','first'),p.FIRST)
        self.assertEqual(expected('full','changed'),p.CHANGED)

    def test_scope_checks_both_phases_and_full_ownership(self):
        with tempfile.TemporaryDirectory() as folder:
            receipt=Path(folder)/'ownership.json'
            receipt.write_text(json.dumps({s:{'full_name':'cdo_demo_db.'+s,'schema_id':p.SCHEMA_IDS[r],'owner':'max.firman@gmail.com'} for r,s in p.SCHEMAS.items()}))
            lease={'source_evidence':[{'path':str(receipt),'sha256':p.digest(receipt)}]}
            scope=p.SubmitScope(p.RUNS['hosted'],lease,'hosted')
            for name in p.plan()['names']['hosted']:scope.check(name)
            with self.assertRaises(Refused):scope.check(p.plan()['names']['rust'][0])
            old=p.OBJECTS['rust']
            try:
                p.OBJECTS['rust']=old[:-1]
                with self.assertRaises((Refused,ValueError)):p.SubmitScope(p.RUNS['hosted'],lease,'hosted')
            finally:p.OBJECTS['rust']=old
            receipt.write_text('{}')
            with self.assertRaises(Refused):p.SubmitScope(p.RUNS['hosted'],lease,'hosted')

    def test_only_merge_copy_hypotheses_accept_measured_builds(self):
        self.assertEqual(p.observed_verdict('merge','ready_to_clone',{'ready_to_execute':{}}),'ready_to_execute')
        for kind,wanted,response in [('full','ready_to_clone',{'ready_to_execute':{}}),
                ('merge','skip_execution',{'ready_to_execute':{}}),
                ('merge','ready_to_clone',{'ready_to_execute':{},'ready_to_clone':{}})]:
            with self.assertRaises(Refused):p.observed_verdict(kind,wanted,response)

    def test_native_boundary_guard_rejects_copy_and_skip_for_every_negative(self):
        for label in ('logic','config','upstream_drift','upstream_unknown','volatile','own_external'):
            p.assert_boundary_verdict('rust',label,'ready_to_execute')
            for wrong in ('ready_to_clone','skip_execution'):
                with self.assertRaises(Refused):p.assert_boundary_verdict('rust',label,wrong)
        for phase in ('hosted','rust'):
            with self.assertRaises(Refused):p.assert_boundary_verdict(phase,'baseline_before','ready_to_execute')

    def test_invocation_rejects_precopy_before_ordinary_response(self):
        c=object.__new__(p.SubmitCopy)
        c.dbt=lambda *args:[{'method':'RegisterClone'}]
        with self.assertRaisesRegex(Refused,'RegisterClone'):c.invocation('full',p.CELLS[1])


if __name__=='__main__':unittest.main()
