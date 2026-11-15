"""Warehouse-free fault probes for the bounded native pre-copy evidence guards."""
import json
from pathlib import Path
import tempfile
import unittest

import databricks_precopy as p
from databricks_survey import Refused


class PrecopyTests(unittest.TestCase):
    def test_properties_preserve_every_key_except_valid_new_physical_UUIDs(self):
        source={'delta.minReaderVersion':'3','clusterByAuto':'true',
            'io.unitycatalog.tableId':'11111111-1111-4111-8111-111111111111',
            'delta.rowTracking.materializedRowIdColumnName':'_row-id-col-11111111-1111-4111-8111-111111111111'}
        target={k:v.replace('11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222') for k,v in source.items()}
        self.assertEqual(len(p.assert_copy_properties(source,target)['preserved']),2)
        for key,value in [('delta.minReaderVersion','1'),('clusterByAuto','false'),('io.unitycatalog.tableId','arbitrary'),('delta.rowTracking.materializedRowIdColumnName','_row-id-col-11111111-1111-4111-8111-111111111111')]:
            wrong=dict(target);wrong[key]=value
            with self.assertRaises(Refused):p.assert_copy_properties(source,wrong)
        wrong=dict(target);del wrong['clusterByAuto']
        with self.assertRaises(Refused):p.assert_copy_properties(source,wrong)
    def test_frozen_scd_oracle_retains_old_null_and_inserted_versions(self):
        rows=p.expected('check','changed')
        self.assertEqual(len(rows),4)
        old=next(r for r in rows if r[0]=='1' and r[1]=='one')
        new=next(r for r in rows if r[0]=='1' and r[1]=='changed')
        self.assertEqual(old[7],p.CHANGED[0][3]);self.assertIsNone(new[7])
        self.assertEqual(new[5:7],(p.CHANGED[0][3],)*2)
        self.assertEqual(next(r for r in rows if r[0]=='2')[1],None)
        self.assertEqual(next(r for r in rows if r[0]=='3')[1],'one')
        self.assertEqual(p.expected('merge','changed'),p.CHANGED)

    def test_both_uuid_allocations_checked_before_either_phase(self):
        old=p.OBJECTS['rust']
        try:
            p.OBJECTS['rust']=old[:-1]
            with self.assertRaises((Refused,ValueError)):p.CopyScope(p.RUNS['hosted'],{},'hosted')
        finally:p.OBJECTS['rust']=old

    def test_readonly_reference_rejects_lost_null_duplicate_and_write(self):
        rows=[['1',None],['1',None]]
        direct={'columns':[{'name':n,'type_name':'STRING'} for n in ('id','v')]}
        sql="SELECT cast(id as string) AS id,cast(v as string) AS v FROM VALUES ('1',cast(null as string)),('1',cast(null as string)) AS declared(id,v)"
        stdout=json.dumps([{'id':'1','v':None}]*2)
        self.assertTrue(p.assert_show(stdout,'',direct,rows,sql,sql+';',sql))
        for wrong in ([{'id':'1','v':None}],[{'id':'1','v':'NULL'}]*2):
            with self.assertRaises(Refused):p.assert_show(json.dumps(wrong),'',direct,rows,sql,sql+';',sql)
        with self.assertRaises(Refused):p.assert_show(stdout,'',direct,rows,sql,sql+'; DELETE FROM old;',sql)

    def test_native_guard_requires_actual_exact_clone_and_restricts_writes(self):
        target='cdo_demo_db.dev.fresh';source='cdo_demo_db.prod.fresh'
        ddl='CREATE OR REPLACE TABLE '+target+' SHALLOW CLONE '+source
        with tempfile.TemporaryDirectory() as folder:
            c=object.__new__(p.Precopy);c.work=Path(folder);c.n=1;c.secrets=[];c.completed=set();c.prefix=None;c.prefixes=[]
            path=c.work/'logs/001_copy/query_log.sql';path.parent.mkdir(parents=True)
            path.write_text(ddl+';')
            c.audit_native('copy',target,source,True)
            path.write_text('ALTER\nTABLE '+target+' CLUSTER BY NONE;')
            c.audit_native('copy',target,source,False)
            for bad in (ddl.replace(source,'cdo_demo_db.prod.old'),ddl+'; DROP TABLE '+target,
                        ddl.replace(target,target+'__dbt_stg'),'SELECT 1'):
                path.write_text(bad+';')
                with self.assertRaises(Refused):c.audit_native('copy',target,source,True)
            for bad in ('ALTER TABLE '+target+' RENAME TO old',
                        'ALTER TABLE cdo_demo_db.prod.old CLUSTER BY NONE',
                        'ALTER TABLE '+target+' CLUSTER BY (id)'):
                path.write_text(bad+';')
                with self.assertRaisesRegex(Refused,'unapproved-native-alter'):c.audit_native('copy',target,source,False)


if __name__=='__main__':unittest.main()
