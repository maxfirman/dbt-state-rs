"""Fault probes for the public show/typed-result artifact boundary; no warehouse."""
import copy
import json
import unittest

from databricks_readonly_reference import assert_reference
from databricks_survey import Refused

SQL = 'SELECT id, amount, tag FROM cdo_demo_db.owned.source'
ROWS = [[1, 10, 'one'], [1, 10, 'one'], [2, None, None]]
DIRECT = {'columns': [dict(name=n, type_text=t, type_name=k, position=i)
                     for i, (n,t,k) in enumerate([('id','BIGINT','LONG'),('amount','INT','INT'),('tag','STRING','STRING')])],
          'rows': [['1','10','one'],['1','10','one'],['2',None,None]]}
SHOW = json.dumps([dict(zip(('id','amount','tag'),r)) for r in ROWS])

class ReferenceArtifacts(unittest.TestCase):
    def check(self, show=SHOW, direct=None, rows=None, stderr='', query=None, compiled=SQL):
        return assert_reference(show, stderr, DIRECT if direct is None else direct,
                                ROWS if rows is None else rows, SQL,
                                '-- outcome: success\n'+SQL+';\n' if query is None else query, compiled)

    def test_accepts_complete_duplicate_null_rows_and_typed_schema(self):
        self.check(query="-- outcome: success\n  \n  "+SQL+"\n  \n;\n")

    def test_rejects_losses_and_ambiguous_cli_payloads(self):
        for show in [SHOW+'\n'+SHOW, 'noise\n'+SHOW, SHOW.replace('"amount": 10','"amount": "10"'),
                     SHOW.replace('"id": 1','"id": true'), SHOW.replace('"amount": 10','"amount": 10.0'),
                     json.dumps([dict(zip(('id','amount','tag'),r)) for r in ROWS[1:]]),
                     SHOW.replace('"tag": "one"','"tag": "one", "tag": "one"'),
                     SHOW.replace('"amount": null, ',''), SHOW.replace('null','0')]:
            with self.subTest(show=show), self.assertRaises(Refused): self.check(show=show)

    def test_rejects_schema_changes_including_order_and_position(self):
        for column, key, value in [(0,'type_text','STRING'),(0,'type_name','STRING'),(0,'position',1),(0,'position',False),(0,'name','ID')]:
            direct=copy.deepcopy(DIRECT);direct['columns'][column][key]=value
            with self.subTest(key=key), self.assertRaises(Refused):self.check(direct=direct)

    def test_rejects_truncated_or_changed_direct_results(self):
        direct=copy.deepcopy(DIRECT);direct['rows'].pop()
        with self.assertRaises(Refused):self.check(direct=direct)

    def test_requires_exact_readonly_sql_and_clean_stderr(self):
        for kwargs in [dict(stderr='warning'),dict(compiled=SQL+' LIMIT 10'),dict(query=SQL+' LIMIT 10;'),
                       dict(query=SQL+'; CREATE TABLE x AS '+SQL+';'),dict(query=SQL+'; '+SQL+';')]:
            with self.subTest(kwargs=kwargs), self.assertRaises(Refused):self.check(**kwargs)

if __name__=='__main__':unittest.main()
