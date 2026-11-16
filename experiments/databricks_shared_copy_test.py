#!/usr/bin/env python3
"""Offline fault probes for #186's literal typed bags, SCD history and write guards."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from databricks_shared_copy import (SharedCopy, SOURCE_SCHEMA, FULL_SCHEMA, FIRST,
    CHANGED, OBJECTS, CELLS, expected, plan)
from databricks_precopy import Precopy
from databricks_snapshot_oracle import (Ledger, NATIVE_SCHEMA, assert_source,
    assert_history, check_schema, micros, SOURCE_PROJECTION_SCHEMA)
from databricks_survey import Refused


class SharedCopyGuards(unittest.TestCase):
    def test_exact_small_graph_budget_and_independent_cumulative_history(self):
        p=plan()
        self.assertEqual(sum(len(kinds) for _,_,kinds,_ in CELLS),15)
        self.assertEqual((p['compile_per_backend'],p['guarded_updates'],p['guarded_drops']),(6,2,1))
        for phase in OBJECTS:
            self.assertEqual(len(set(p['names'][phase])),8)
            self.assertTrue(all('p186_'+OBJECTS[phase]+'_' in n for n in p['names'][phase]))
        self.assertTrue(set(p['names']['hosted']).isdisjoint(p['names']['rust']))
        self.assertEqual(expected('full','source'),(('2','2'),))
        rows=expected('check','changed')
        self.assertEqual(len(rows),3)
        self.assertEqual([r for r in rows if r[0]=='1' and r[7] is None][0][:4],CHANGED[0])
        self.assertEqual([r for r in rows if r[0]=='1' and r[7] is not None][0][7],CHANGED[0][3])
        self.assertEqual(expected('merge','source_modified'),CHANGED)

    def test_mutation_order_refuses_other_objects_before_any_API(self):
        obj=SharedCopy.__new__(SharedCopy);obj.mutations=0
        obj.scope=SimpleNamespace(relation=lambda role,name:role+'.'+name)
        for op in [('change_a','prod.b','update_a'),('sentinel','prod.merge','sentinel'),('change_a','prod.a','drop')]:
            with self.assertRaisesRegex(Refused,'ordered-mutation-budget'):obj.mutate(*op)

    def test_write_guard_refuses_unlisted_and_out_of_order_SQL_before_API(self):
        obj=SharedCopy.__new__(SharedCopy);obj.phase='hosted';obj.write_index=0
        for sql in ('DROP TABLE cdo_demo_db.old.relation','CREATE SCHEMA cdo_demo_db.nope',
                    "UPDATE cdo_demo_db.owned.a SET v='changed'"):
            with self.assertRaisesRegex(Refused,'exact-ordered-SQL-write-budget'):obj.sql('unexpected',sql)
        self.assertEqual(obj.write_index,0)

    def test_exact_INT_source_schema_and_microseconds(self):
        columns=[dict(zip(('name','type_text','type_name','position'),c)) for c in SOURCE_SCHEMA[:3]+SOURCE_PROJECTION_SCHEMA[3:]]
        raw={'columns':columns,'rows':[[*r,str(micros(r[3]))] for r in FIRST]}
        assert_source(raw,FIRST,SOURCE_SCHEMA)
        for mutation in ('type','micros','null','duplicate'):
            bad=copy.deepcopy(raw)
            if mutation=='type':bad['columns'][0]['type_text']='BIGINT'
            if mutation=='micros':bad['rows'][0][4]=str(int(bad['rows'][0][4])+1)
            if mutation=='null':bad['rows'][1][1]='null'
            if mutation=='duplicate':bad['rows'].append(bad['rows'][1])
            with self.subTest(mutation=mutation),self.assertRaises(Refused):assert_source(bad,FIRST,SOURCE_SCHEMA)

    def test_complete_SCD_history_detects_old_version_loss_and_wrong_clock(self):
        ledger=Ledger('check','ignore',source_updated_at=True).execute(FIRST,'first').execute(CHANGED,'changed')
        rows=ledger.rows({});assert_history(rows,ledger,{})
        for mutation in ('lost','clock','hash','null','duplicate'):
            bad=[list(r) for r in rows]
            if mutation=='lost':bad.pop(0)
            if mutation=='clock':bad[0][7]='2026-11-03 12:00:00.000002'
            if mutation=='hash':bad[0][4]='0'*32
            if mutation=='null':bad[1][1]='null'
            if mutation=='duplicate':bad.append(bad[0])
            with self.subTest(mutation=mutation),self.assertRaises(Refused):assert_history(bad,ledger,{})

    def test_existing_physical_oracle_checks_explicit_FULL_bigint_bag(self):
        # Exercise the existing campaign-facing physical interface with independent
        # API responses; the native show is already covered by its own fault probes.
        with tempfile.TemporaryDirectory() as tmp:
            obj=SharedCopy.__new__(SharedCopy);obj.work=Path(tmp);obj.prefixes=[];obj.completed=set()
            obj.scope=SimpleNamespace(relation=lambda role,kind:'cdo_demo_db.owned.full')
            raw={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in FULL_SCHEMA], 'rows':[['2','2']]}
            props={'columns':[],'rows':[['Type','MANAGED'],['Provider','delta']]}
            def save(name,value):
                path=obj.work/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))
            obj.save=save
            def observe(label,target):save('physical/'+label+'_properties.json',props);return raw
            obj.observe=observe
            obj.api=lambda *a:{'data_source_format':'DELTA','table_type':'MANAGED'}
            obj.sql=lambda label,sql: raw if label.endswith('_lossless') else {'columns':[{'name':'id','type_name':'STRING'},{'name':'n','type_name':'STRING'}],'rows':[['2','2']]}
            def check():
                return Precopy.physical(obj,'full','full','dev','first',False,False,expected_rows=(('2','2'),),native_schema=FULL_SCHEMA)
            with patch('databricks_precopy.show_reference'):
                check()
                for mutation in ('wrong_count','wrong_type','missing','duplicate','wrong_NULL'):
                    before=copy.deepcopy(raw)
                    if mutation=='wrong_count':raw['rows'][0][1]='3'
                    if mutation=='wrong_type':raw['columns'][0]['type_text']='INT'
                    if mutation=='missing':raw['rows']=[]
                    if mutation=='duplicate':raw['rows'].append(['2','2'])
                    if mutation=='wrong_NULL':raw['rows'][0][1]=None
                    with self.subTest(mutation=mutation),self.assertRaises(Refused):check()
                    raw.clear();raw.update(before)


if __name__=='__main__':unittest.main()
