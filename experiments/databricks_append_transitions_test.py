"""Faults at the independent cumulative-bag/reference seam for #146."""
from copy import deepcopy
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from databricks_append_transitions import STAGES, Transitions as Campaign, assert_transition_result
from databricks_merge_recovery_test import result
from databricks_survey import Refused


class Transitions(unittest.TestCase):
    def test_operation_ledger_and_declared_snapshots_agree(self):
        previous,previous_kind = [],None
        for kind,source,expected in STAGES:
            if kind == 'merge':
                rows = {r[0]:r for r in previous}
                self.assertEqual(len(rows),len(previous))
                rows.update({r[0]:r for r in source})
                previous = list(rows.values())
            elif kind in ('append','schema') and previous_kind != 'view':
                previous = [r+[None]*(len(source[0])-len(r)) for r in previous]+deepcopy(source)
            else:
                previous = deepcopy(source)
            self.assertCountEqual(previous,expected)
            assert_transition_result(result(expected),expected)
            previous_kind = kind

    def test_observed_native_view_swap_is_bounded_to_exact_owned_names(self):
        target = 'cdo_demo_db.owned.consumer'
        staging,backup = target+'__dbt_stg',target+'__dbt_backup'
        sql = (f'CREATE OR REPLACE VIEW {staging} AS SELECT 1;'
               f'ALTER TABLE {target} RENAME TO {backup};'
               f'ALTER VIEW {staging} RENAME TO {target};DROP TABLE {backup};')
        with TemporaryDirectory() as directory:
            c = object.__new__(Campaign); c.work = Path(directory); c.n = 1
            c.scope = SimpleNamespace(temporary='consumer__dbt_tmp',auxiliary={staging,backup},relation=lambda *_:target)
            c.save = lambda *_:None
            log = c.work/'logs/001_view/query_log.sql'; log.parent.mkdir(parents=True)
            log.write_text(sql)
            with self.assertRaises(Refused): c.audit_native('view',True)
            c.native_target_kind = 'view'
            c.audit_native('view',True)
            for wrong in (sql.replace(staging,'cdo_demo_db.owned.other'),sql+'CREATE VIEW cdo_demo_db.owned.other AS SELECT 1;'):
                log.write_text(wrong)
                with self.assertRaises(Refused): c.audit_native('view',True)

    def test_replacement_deduplication_extra_append_and_old_kind_result_are_rejected(self):
        prior = None
        for kind,source,expected in STAGES:
            wrong = [expected+source,expected[:-1]]
            if source != expected: wrong.append(source)
            if prior is not None and sorted(prior,key=repr)!=sorted(expected,key=repr): wrong.append(prior)
            for rows in wrong:
                with self.subTest(kind=kind,rows=rows),self.assertRaises(Refused):
                    actual = result(expected)
                    actual['rows'] = rows
                    assert_transition_result(actual,expected)
            prior = expected


if __name__=='__main__': unittest.main()
