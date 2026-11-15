#!/usr/bin/env python3
"""Independent physical seam faults for composite partition replacement."""
import copy
import json
import unittest
from databricks_overwrite_recovery import C4,C5,A,B,RB,C,RC,assert_bag,literal,reference
from databricks_survey import Refused


def physical(rows):
    schema=C4 if len(rows[0])==4 else C5
    return {'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in schema],
            'rows':copy.deepcopy(rows)}


class Replacement(unittest.TestCase):
    def test_complete_cumulative_multiset(self):
        assert_bag(physical(RB),RB)
        # Fresh source replacement loses unrepresented (A,y)/(NULL,NULL).
        for bad in (B,RB[1:],RB+[RB[0]],[[*r[:4], 'invented'] for r in RB]):
            with self.assertRaises(Refused): assert_bag(physical(bad),RB)
        # Projecting only the first key replaces an unrepresented sibling.
        with self.assertRaises(Refused): assert_bag(physical([r for r in RB if r[0]!='2']),RB)
        with self.assertRaises(Refused): assert_bag(physical([r for r in RB if r[0]!='5']),RB)
        assert_bag(physical(RC),RC)
        with self.assertRaises(Refused): assert_bag(physical(C),RC)

    def test_schema_and_complete_reference(self):
        actual=physical(RB); bad=copy.deepcopy(actual);bad['columns'][3]['name']='wrong'
        with self.assertRaises(Refused):assert_bag(bad,RB)
        rows=[[int(r[0]),int(r[1]),*r[2:]] for r in RB]
        sql=literal(RB)
        reference(json.dumps([dict(zip(['id','amount','p','q','note'],r)) for r in rows]),'',actual,rows,sql,sql+';',sql)
        for badrows in (rows[:-1],[[*r[:4],'invented'] for r in rows]):
            with self.assertRaises(Refused):
                reference(json.dumps([dict(zip(['id','amount','p','q','note'],r)) for r in badrows]),'',actual,rows,sql,sql+';',sql)

if __name__=='__main__':unittest.main()
