"""Independent cumulative operation model and faults at physical/public seams."""
from copy import deepcopy
import unittest
import json
from databricks_merge_recovery import B0,B1,B2,B3,B4,R1,R2,R3,R4,C3,C4,assert_bag,literal,reference
from databricks_survey import Refused


def result(rows):
    return {'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in (C3 if len(rows[0])==3 else C4)],'rows':deepcopy(rows)}


def merge(previous,incoming, minimum=1):
    # Independent keyed operation ledger: adding the nullable column does not
    # backfill source-missing rows; missing keys persist across source replacement.
    rows = {r[0]:r+[None]*(len(incoming[0])-len(r)) for r in previous}
    assert len(rows)==len(previous) and len({r[0] for r in incoming})==len(incoming)
    extras = []
    for row in incoming:
        if row[0] in rows and int(row[0]) < minimum: extras.append(row)
        else: rows[row[0]]=row
    return sorted([*rows.values(),*extras])


class RecoveryOracle(unittest.TestCase):
    def test_declared_snapshots_follow_independent_merge_operations(self):
        self.assertEqual(merge(B0,B1),sorted(R1))
        self.assertEqual(merge(R1,B2),sorted(R2))
        self.assertEqual(merge(R2,B3),sorted(R3))
        self.assertNotEqual(sorted(B1),sorted(R1))
        self.assertNotEqual(sorted(B2),sorted(R2))
        self.assertNotEqual(sorted(B3),sorted(R3))

    def test_failed_target_cannot_publish_schema_or_recovery_rows(self):
        assert_bag(result(B0),B0)
        for wrong in (B1,R1,[r+[None] for r in B0]):
            with self.assertRaisesRegex(Refused,'complete-typed-bag'): assert_bag(result(wrong),B0)

    def test_recovery_rejects_lost_retained_rows_backfill_duplicates_or_schema_drift(self):
        for fault in ('delete','backfill','duplicate','type','order'):
            actual=result(R1)
            if fault=='delete':actual['rows'].pop(1)
            if fault=='backfill':actual['rows'][1][-1]='invented'
            if fault=='duplicate':actual['rows'].append(actual['rows'][0])
            if fault=='type':actual['columns'][-1]['type_text']='INT'
            if fault=='order':actual['columns'].reverse()
            with self.subTest(fault=fault),self.assertRaisesRegex(Refused,'complete-typed-bag'): assert_bag(actual,R1)

    def test_predicate_exclusion_inserts_a_second_key_instead_of_updating_old_row(self):
        self.assertEqual(merge(R3,B4,minimum=2),sorted(R4))
        self.assertNotEqual(merge(R3,B4),sorted(R4))
        assert_bag(result(R4),R4)
        with self.assertRaisesRegex(Refused,'complete-typed-bag'):
            assert_bag(result(merge(R3,B4)),R4)

    def test_four_column_reference_rejects_note_loss_types_duplicates_and_extra_SQL(self):
        rows = [[int(r[0]),int(r[1]),*r[2:]] for r in R1]
        names = ['id','amount','tag','note']
        objects = [dict(zip(names,r)) for r in rows]
        sql = literal(R1)
        reference(json.dumps(objects),'',result(R1),rows,sql,sql+';',sql)
        for fault in ('missing','type','duplicate','extra-sql','duplicate-json-key'):
            broken = deepcopy(objects); query = sql+';'
            if fault=='missing':broken[1].pop('note')
            if fault=='type':broken[0]['note']=1
            if fault=='duplicate':broken.append(broken[0])
            if fault=='extra-sql':query += ' DROP TABLE other;'
            stdout = json.dumps(broken)
            if fault=='duplicate-json-key':stdout = stdout.replace('"note": "n1"','"note": "hidden", "note": "n1"')
            with self.subTest(fault=fault),self.assertRaises(Refused):
                reference(stdout,'',result(R1),rows,sql,query,sql)

    def test_later_predicate_retains_rows_outside_destination_domain(self):
        assert_bag(result(R3),R3)
        self.assertEqual([r for r in R3 if int(r[0])<2],[R1[0]])
        self.assertEqual([r for r in R3 if r[0]=='3'],[B3[0]])
        self.assertEqual([r for r in R3 if r[0]=='5'],[B3[1]])
        self.assertIn("(2,20,'retained',CAST(NULL AS STRING))",literal(R3))


if __name__=='__main__':unittest.main()
