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
    CHANGED, OBJECTS, CELLS, KINDS, queries, sql_contract, expected, plan)
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

    def test_timezone_guard_stops_before_baseline_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            obj=SharedCopy.__new__(SharedCopy);obj.work=Path(tmp)
            for zone in ('Etc/UTC','UTC','Europe/London'):
                (obj.work/'warehouse.json').write_text(json.dumps({'rows':[['actual-version',zone]]}))
                with patch('databricks_shared_copy.SubmitCopy.init_schemas'):
                    if zone=='Europe/London':
                        with self.assertRaisesRegex(Refused,'UTC-required-before-DDL'):obj.init_schemas()
                    else:obj.init_schemas()

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

    def coordinator(self,phase='rust'):
        obj=SharedCopy.__new__(SharedCopy);obj.phase=phase
        obj.scope=SimpleNamespace(relation=lambda role,name:f'cdo_demo_db.owned_{role}.{name}')
        obj.sql_queries=queries(obj.scope);obj.namespace=None;obj.source_requests={};obj.confirmed={};obj.shallow=set()
        obj.configure=lambda mode:None;obj.save=lambda *a:None;obj.audit_native=lambda *a:None
        obj.explain=lambda *a:None;obj.physical=lambda *a,**kw:None;obj.remember=lambda *a:None
        return obj

    def calls(self,obj,kind,stage,role,mode):
        target=obj.scope.relation(role,kind);source=obj.scope.relation('prod',kind)
        copying=stage=='copy' or (stage=='source_modified' and obj.phase=='hosted')
        build=stage in ('source','changed','source_modified')
        verdict='ready_to_clone' if copying and kind=='full' else 'ready_to_execute' if build else 'skip_execution'
        runtime=100 if stage in ('source','copy','repeat') else 200
        inputs=[obj.scope.relation('prod','a')]+([] if kind=='check' else [obj.scope.relation('prod','b')])
        req={'sql':obj.sql_queries[kind],'execution_type':{'full':1,'merge':3,'check':7}[kind],
            'target_table':target,'query_dependencies':[],'compare_unrendered_code':False,'table_namespace':'space',
            'allow_clones':role=='dev' and mode!='repair','clone_chain_depth_limit':int(role=='dev' and mode!='repair'),
            'dbt_node_state':{'node_unique_id':'node.'+kind},'tables':[{'name':n,'last_modified_epoch':5} for n in inputs]}
        ready={'request_id':'build','execution_runtime_ms':runtime}
        grant={'request_id':'copy','execution_runtime_ms':100,'clone_required_last_modified_epoch':10,
            'clone_source':source,'clone_target':target,'clone_sqls':['\n        CREATE OR REPLACE TABLE '+target+' SHALLOW CLONE '+source+'\n        ']}
        direct=[]
        if stage=='copy' and kind!='full' or stage=='source_modified':
            direct=[{'method':'RegisterClone','status':{'code':0},'request':{
                'clone_source_table':source,'target_table':target,'table_namespace':'space',
                'labels':{'dbt_node_unique_id':'node.'+kind},'clone_chain_depth_limit':1,'clone_source_last_modified_epoch':11 if stage=='source_modified' else 10},
                'response':{'ready_to_clone':grant if copying else None,
                            'response':{'ready_to_clone_v1':grant} if copying else None}}]
        if verdict=='ready_to_clone':ready=grant
        submit={'method':'SubmitEnrichedSQL','status':{'code':0},'request':req,'response':{'response':{verdict:ready}}}
        def confirm(identity,runtime):
            return {'method':'ConfirmExecution','status':{'code':0},'request':{
                'request_id':identity,'failed_to_clone':False,'last_modified_epoch':10 if role=='prod' else 20,
                'execution_runtime_ms':runtime},'response':{'success':True}}
        confirms=([confirm('copy',None if kind!='full' else 100)] if copying else [])
        if build:confirms.append(confirm('build',runtime))
        return direct+[submit]+confirms

    def test_complete_invocation_history_and_hosted_repair_branch(self):
        for phase in ('rust','hosted'):
            obj=self.coordinator(phase);observed=[]
            obj.physical=lambda *a,**kw:observed.append((a,kw))
            for stage,role,kinds,mode in CELLS:
                for kind in kinds:
                    calls=self.calls(obj,kind,stage,role,mode)
                    obj.dbt=lambda *a,calls=calls:calls
                    obj.invocation(kind,stage,role,mode)
            self.assertEqual(len(observed),15)
            self.assertEqual(observed[-2][1]['copy_repaired'],phase=='hosted')
            self.assertEqual(observed[-2][1]['expected_rows'],CHANGED)
            self.assertEqual(observed[11][1]['expected_rows'],expected('merge','changed_repeat'))

    def test_invocation_rejects_wire_and_causal_corruption_before_physical_check(self):
        for fault in ('sql','type','epoch','UID','source_epoch','adopted_runtime','depth','grant','missing_confirm','extra_confirm','failed_copy'):
            obj=self.coordinator();kind='merge'
            obj.source_requests[kind]={'dbt_node_state':{'node_unique_id':'node.merge'}}
            obj.confirmed[obj.scope.relation('prod',kind)]={'last_modified_epoch':10,'execution_runtime_ms':100}
            calls=self.calls(obj,kind,'copy','dev','copy')
            submit=next(c for c in calls if c['method']=='SubmitEnrichedSQL')
            if fault=='sql':submit['request']['sql']+=' WHERE id=1'
            if fault=='type':submit['request']['execution_type']=1
            if fault=='epoch':submit['request']['tables'][0]['last_modified_epoch']=None
            if fault=='UID':submit['request']['dbt_node_state']['node_unique_id']='foreign'
            if fault=='source_epoch':calls[0]['request']['clone_source_last_modified_epoch']=99
            if fault=='depth':calls[0]['request']['clone_chain_depth_limit']=2
            if fault=='adopted_runtime':submit['response']['response']['skip_execution']['execution_runtime_ms']=999
            if fault=='grant':calls[0]['response']['ready_to_clone']['clone_sqls']=['DROP TABLE old']
            if fault=='missing_confirm':calls.pop()
            if fault=='extra_confirm':
                extra=copy.deepcopy(calls[-1]);extra['request']['request_id']='extra';calls.append(extra)
            if fault=='failed_copy':calls[-1]['request']['failed_to_clone']=True
            obj.dbt=lambda *a:calls
            obj.physical=lambda *a,**kw:self.fail('invalid wire reached physical acceptance')
            with self.subTest(fault=fault),self.assertRaises(Refused):obj.invocation(kind,'copy','dev','copy')

    def test_own_runtime_and_modified_source_refusal_cannot_be_faked(self):
        for fault in ('wrong_runtime','unsafe_grant'):
            obj=self.coordinator();kind='merge';source=obj.scope.relation('prod',kind);target=obj.scope.relation('dev',kind)
            obj.source_requests[kind]={'dbt_node_state':{'node_unique_id':'node.merge'}}
            obj.confirmed[source]={'last_modified_epoch':10,'execution_runtime_ms':100}
            obj.confirmed[target]={'last_modified_epoch':20,'execution_runtime_ms':200}
            if fault=='wrong_runtime':calls=self.calls(obj,kind,'repeat','dev','copy');stage='repeat';mode='copy'
            else:
                obj.phase='hosted';calls=self.calls(obj,kind,'source_modified','dev','refusal');obj.phase='rust'
                stage='source_modified';mode='refusal'
            obj.dbt=lambda *a:calls
            obj.physical=lambda *a,**kw:self.fail('unsafe reuse reached physical acceptance')
            with self.subTest(fault=fault),self.assertRaises(Refused):obj.invocation(kind,stage,'dev',mode)

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
