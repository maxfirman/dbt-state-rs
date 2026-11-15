#!/usr/bin/env python3
"""Exact source-clock SCD supplements and focused oracle fault probes."""
from collections import Counter
from copy import deepcopy
import unittest
from databricks_snapshot_modes import SOURCE, CELLS, histories, plan, assert_source_clock_sql
from databricks_snapshot_oracle import Ledger, assert_history
from databricks_survey import Refused


class SourceClockSnapshots(unittest.TestCase):
    def test_declared_check_history_preserves_noncheck_values_and_null_intervals(self):
        h=histories()
        self.assertEqual(h['noncheck'],h['check_first'])
        self.assertEqual(h['delete'],h['restore'])
        self.assertEqual([r[4] for r in h['reinsert']],['68a9f1c71115cd7d4fdeca852c0e92d2',
            'b71c0538e59d0f5640b186819f2c013e','76031333587e76558302967111f67654',
            '830504a8ffc9b4cbcc8bc3a907b1c705','45af17c77bad66b7221124d1d434bfc4'])
        self.assertEqual(h['null'][0][7],'2026-11-04 12:00:00.100010')
        self.assertIsNone(h['null'][-1][1])
        self.assertEqual(h['restore'][2][7],'2026-11-05 12:00:00.123456')
        self.assertEqual(h['reinsert'][1][7],'2026-11-06 12:00:00.654320')
        ledger=Ledger('check','ignore',source_updated_at=True)
        for label,kind,stage,execute,_ in CELLS:
            if kind!='check':continue
            if execute:ledger=ledger.execute(SOURCE[stage],label)
            assert_history(h[label],ledger,{})
        for column,value in ((0,None),(1,'restored'),(2,'2026-11-03'),(4,'wrong'),
                             (5,'2026-11-04 12:00:00.100000'),(6,'2026-11-05 12:00:00.123456'),(7,None)):
            rows=[list(row) for row in h['reinsert']];rows[0][column]=value
            with self.subTest(column=column),self.assertRaises(Refused):assert_history(rows,ledger,{})

    def test_timestamp_old_equal_and_one_microsecond_advance(self):
        h=histories()
        self.assertEqual(h['timestamp_adopt'],h['full'])
        self.assertEqual(h['older'],h['timestamp_catchup'])
        self.assertEqual(h['equal'],h['older'])
        self.assertEqual(h['advance'][-1][4],'028f0e91378afff569000479d2f7ff09')
        self.assertEqual(h['advance'][2][7],'2026-11-05 12:00:00.123457')
        self.assertEqual(h['advance'][2][5],'2026-11-05 12:00:00.123456')
        self.assertEqual(h['advance'][0][5],'2026-11-01 12:00:00.123450')
        for label,kind,_,execute,_ in CELLS:
            if not execute:self.assertEqual(Counter(h[label]),Counter(h[dict(check_repeat='check_first',timestamp_repeat='timestamp_adopt',null_repeat='null',reinsert_repeat='reinsert',advance_repeat='advance')[label]]))

    def test_clock_guard_distinguishes_metadata_probe_from_mutating_scd_sql(self):
        probe="select * from (select current_timestamp() as dbt_snapshot_time) q where false limit 0;"
        dml="create table exact as select ts as dbt_updated_at,ts as dbt_valid_from;"
        assert_source_clock_sql(probe+dml)
        for bad in (dml.replace('ts as dbt_updated_at','current_timestamp () as dbt_updated_at'),
                    dml.replace('ts as dbt_valid_from','other as dbt_valid_from')):
            with self.assertRaises(Refused):assert_source_clock_sql(probe+bad)

    def test_budget_and_unique_current_scd_ids(self):
        self.assertEqual(sum(c[3] for c in CELLS),12)
        self.assertEqual(sum(c[4] for c in CELLS),8)
        self.assertEqual(len(CELLS),17)
        for rows in histories().values():self.assertEqual(len({r[4] for r in rows}),len(rows))
        for names in plan()['names'].values():self.assertEqual(len(set(names)),5)


if __name__=='__main__':unittest.main()
