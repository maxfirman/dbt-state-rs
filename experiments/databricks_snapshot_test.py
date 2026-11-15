#!/usr/bin/env python3
"""Offline #151 SCD oracle fault probes; no warehouse/auth/CLI use."""
import unittest
from databricks_snapshot_oracle import Ledger, assert_history
from databricks_survey import Refused

A=('1','one','2026-11-01','2026-11-01 12:00:00.000000')
B=('2','two','2026-11-02','2026-11-02 12:00:00.000000')
C=('1','changed','2026-11-01','2026-11-03 12:00:00.000000')

class SnapshotHistory(unittest.TestCase):
    def test_timestamp_ignore_keeps_complete_old_and_deleted_rows(self):
        ledger=Ledger('timestamp','ignore').execute((A,B),'first').execute((C,),'change')
        expected=[(*A,'7917108250601dab0d0861c39f893716',A[3],A[3],C[3]),
                  (*B,'5a1f2ffb78f8c1e5bf678bc773ed45b0',B[3],B[3],None),
                  (*C,'4d5fc476db0e41efa12fda35761290b8',C[3],C[3],None)]
        # Hash literals checked independently against the retained macro rule.
        assert_history(expected,ledger,{})
        for index,column,value in ((0,1,'changed'),(1,7,C[3]),(2,4,'wrong')):
            bad=[list(r) for r in expected];bad[index][column]=value
            with self.subTest(column=column),self.assertRaises(Refused):assert_history(bad,ledger,{})

    def test_equal_or_regressed_source_timestamp_preserves_old_business_values(self):
        ledger=Ledger('timestamp','ignore').execute((A,B),'first')
        for time in (A[3],'2026-10-31 12:00:00.000000'):
            later=ledger.execute(((A[0],'mutated',A[2],time),),'equal_or_old')
            assert_history([(*A,'7917108250601dab0d0861c39f893716',A[3],A[3],None),
                            (*B,'5a1f2ffb78f8c1e5bf678bc773ed45b0',B[3],B[3],None)],later,{})

    def test_check_uses_one_bounded_microsecond_statement_clock_for_all_rows(self):
        from databricks_snapshot_oracle import Bounds, micros
        ledger=Ledger('check','ignore').execute((A,B),'first')
        instant='2026-11-12 01:50:34.797584';clock=micros(instant)
        expected=[(*A,'fdd93c28963e0f5acc6d18d69683332a',instant,instant,None),
                  (*B,'20b74b724c366e8193753622fa17d220',instant,instant,None)]
        bounds={'first':Bounds(clock-1000,clock+1000,'before','after','warehouse','first')}
        assert_history(expected,ledger,bounds)
        # Millisecond formatting cannot supply independent full-precision evidence.
        for fault in ('split','split_correct_hash','precision','hash','outside','missing','schema','duplicate','old_business'):
            from copy import deepcopy
            rows=deepcopy(expected);limits=dict(bounds)
            if fault=='split':rows[1]=(*rows[1][:5],'2026-11-12 01:50:34.797585',*rows[1][6:])
            if fault=='split_correct_hash':rows[1]=(*rows[1][:4],'a03c80cbf2fd1b2d32ea748ce93e92a0','2026-11-12 01:50:34.797585','2026-11-12 01:50:34.797585',None)
            if fault=='old_business':rows[0]=(*rows[0][:1],'wrong',*rows[0][2:])
            if fault=='precision':rows[0]=(*rows[0][:5],'2026-11-12 01:50:34.797',*rows[0][6:])
            if fault=='hash':rows[0]=(*rows[0][:4],'wrong',*rows[0][5:])
            if fault=='outside':limits={'first':Bounds(clock+1,clock+1000,'before','after','warehouse','first')}
            if fault=='missing':limits={}
            if fault=='schema':rows[0]=rows[0][:-1]
            if fault=='duplicate':rows.append(rows[0])
            with self.subTest(fault=fault),self.assertRaises(Refused):assert_history(rows,ledger,limits)

    def test_check_preserves_noncheck_business_columns_and_null_transitions(self):
        from databricks_snapshot_oracle import Bounds, micros
        first=Ledger('check','ignore').execute((A,B),'first')
        unchanged=(A[0],A[1],'2026-11-04',C[3])
        retained=first.execute((unchanged,B),'noncheck')
        null=(A[0],None,A[2],A[3])
        changed=retained.execute((null,B),'null').execute((A,B),'restore')
        t0='2026-11-12 01:50:34.797584';t1='2026-11-12 01:50:35.797584';t2='2026-11-12 01:50:36.797584'
        rows=[(*A,'fdd93c28963e0f5acc6d18d69683332a',t0,t0,t1),
              (*B,'20b74b724c366e8193753622fa17d220',t0,t0,None),
              (*null,'740cc3c6d89040f641fa89b96a5263ea',t1,t1,t2),
              (*A,'d1de79a7625aa7f647cb7128d70f2acb',t2,t2,None)]
        bounds={name:Bounds(micros(t)-1,micros(t)+1,'b'+name,'a'+name,'warehouse',name)
                for name,t in [('first',t0),('null',t1),('restore',t2)]}
        assert_history(rows,changed,bounds)
        # Source p/ts changes alone must leave the old complete business tuple.
        rows[0]=(*unchanged,*rows[0][4:])
        with self.assertRaises(Refused):assert_history(rows,changed,bounds)

    def test_invalidate_closes_deleted_version_without_losing_business_columns(self):
        from databricks_snapshot_oracle import Bounds, micros
        ledger=Ledger('timestamp','invalidate').execute((A,B),'first').execute((A,),'delete')
        clock=micros('2026-11-12 01:50:34.797584')
        rows=[(*A,'7917108250601dab0d0861c39f893716',A[3],A[3],None),
              (*B,'5a1f2ffb78f8c1e5bf678bc773ed45b0',B[3],B[3],'2026-11-12 01:50:34.797584')]
        bounds={'delete':Bounds(clock-2,clock+2,'before','after','warehouse','delete')}
        assert_history(rows,ledger,bounds)
        with self.assertRaises(Refused):Ledger('timestamp','new_record')
        with self.assertRaises(Refused):ledger.execute((A,A),'duplicate')

    def test_projection_rejects_lossy_timestamp_even_when_api_display_agrees(self):
        from databricks_snapshot_oracle import decode_projection, PROJECTION_SCHEMA, micros
        names=('name','type_text','type_name','position')
        instant='2026-11-12 01:50:34.797584'
        row=(*A,'7917108250601dab0d0861c39f893716',instant,instant,None,
             str(micros(A[3])),str(micros(instant)),str(micros(instant)),None)
        result={'columns':[dict(zip(names,c)) for c in PROJECTION_SCHEMA],'rows':[row]}
        self.assertEqual(decode_projection(result),[row[:8]])
        from copy import deepcopy
        for fault in ('precision','epoch','null','type','position'):
            bad=deepcopy(result);bad['rows']=[list(row)]
            if fault=='precision':bad['rows'][0][5]=instant[:-3]+'000'
            if fault=='epoch':bad['rows'][0][9]=str(micros(instant)+1)
            if fault=='null':bad['rows'][0][11]='0'
            if fault=='type':bad['columns'][9]['type_name']='STRING'
            if fault=='position':bad['columns'][1]['position']=True
            with self.subTest(fault=fault),self.assertRaises(Refused):decode_projection(bad)

    def test_exact_snapshot_scope_and_call_guard_refuse_other_names_and_extra_calls(self):
        from databricks_snapshots import clearance_contract, SnapshotScope, CallGuard, CELLS
        from copy import deepcopy
        contract=clearance_contract();scope=SnapshotScope('20261114a151aa',contract)
        scope.check('cdo_demo_db.state_20261114_snapshot151.sn_20261114a151aa_inputs')
        for name in ('sn_20261114b151bb_inputs','snap_timestamp_ignore','sn_20261114a151aa_consumer__dbt_stg'):
            with self.subTest(name=name),self.assertRaises(Refused):scope.check('cdo_demo_db.state_20261114_snapshot151.'+name)
        bad=deepcopy(contract);bad['max_final_permanent']+=1
        with self.assertRaises(Refused):SnapshotScope('20261114a151aa',bad)
        guard=CallGuard()
        guard.admit('inventory',['parse'],False)
        for label,stage,decision in CELLS:
            guard.admit(label,['snapshot','--select','consumer'],True)
            guard.admit(label+'_reference',['snapshot','--select','reference'],False)
        with self.assertRaises(Refused):guard.admit('first',['snapshot','--select','consumer'],True)
        with self.assertRaises(Refused):CallGuard().admit('inventory',['run'],False)

    def test_snapshot_wire_requires_own_successful_confirm_or_positive_skip_runtime(self):
        from databricks_snapshots import snapshot_submit
        from copy import deepcopy
        source='cdo_demo_db.owned.inputs';target='cdo_demo_db.owned.consumer'
        request={'dialect':'databricks','execution_type':7,'compare_unrendered_code':False,
          'table_namespace':'owned','query_dependencies':[],'semantic_extras':{'unique_key':'"id"'},
          'ignore_external_modifications':False,'freshness_tolerance_seconds':0,'stale_upstream_policy':0,
          'tables':[{'name':source,'last_modified_epoch':10},{'name':target,'last_modified_epoch':20}],
          'target_table':target,'labels':{'dbt_node_name':'consumer'},'tolerate_nondeterminism':True}
        calls=[{'method':'SubmitEnrichedSQL','request':request,
                'response':{'response':{'skip_execution':{'execution_runtime_ms':42,'explained_decision':{'is_stale':False}}}}}]
        for call in calls:
            call['status']={'code':0};call['service']={'SubmitEnrichedSQL':'SQL','ConfirmExecution':'Execution'}[call['method']]
        snapshot_submit(calls,source,target,True,'skip_execution',42)
        speculative=deepcopy(calls[0]);speculative['method']='SubmitEnrichedSQLSpeculative'
        speculative['request']['tables']=[{'name':source,'last_modified_epoch':None}]
        speculative['response']={'response':{'undecided':{}}}
        snapshot_submit([speculative,*calls],source,target,True,'skip_execution',42)
        for fault in ('epoch','runtime','bool-runtime','clock','extra','tables','strategy','lag','stale','missing-stale','misleading-top-level'):
            value=deepcopy(calls);r=value[0]['request']
            if fault=='epoch':r['tables'][0]['last_modified_epoch']=None
            if fault=='runtime':value[0]['response']['response']['skip_execution']['execution_runtime_ms']=41
            if fault=='bool-runtime':value[0]['response']['response']['skip_execution']['execution_runtime_ms']=True
            if fault=='clock':r['tolerate_nondeterminism']=False
            if fault=='extra':value.append({'method':'RegisterClone'})
            if fault=='tables':r['tables'].append(r['tables'][0])
            if fault=='strategy':r['execution_type']=3
            if fault=='lag':r['freshness_tolerance_seconds']=10
            if fault=='stale':value[0]['response']['response']['skip_execution']['explained_decision']['is_stale']=True
            if fault=='missing-stale':value[0]['response']['response']['skip_execution'].pop('explained_decision')
            if fault=='misleading-top-level':
                skip=value[0]['response']['response']['skip_execution'];skip['is_stale']=False;skip['explained_decision']['is_stale']=True
            with self.subTest(fault=fault),self.assertRaises(Refused):snapshot_submit(value,source,target,True,'skip_execution',42)

    def test_native_snapshot_guard_rejects_other_target_cleanup_and_sql_strategy(self):
        from databricks_snapshots import assert_native_sql
        target='cdo_demo_db.owned.consumer';temp='consumer__dbt_tmp'
        first=f'create table {target} using delta as select 1;'
        change=f'create or replace temporary view {temp} as select 1; merge into {target} using {temp} on true when matched then update set v=1; DROP VIEW IF EXISTS {temp};'
        assert_native_sql(first,target,temp,True)
        assert_native_sql(change,target,temp,False)
        assert_native_sql('select 1;',target,temp,False,True)
        for sql in (change.replace(target,'cdo_demo_db.protected.consumer'),change+' DROP TABLE '+target+';',change.replace('temporary view','view'),change.replace('merge into','insert into'),change.replace('DROP VIEW IF EXISTS '+temp,'DROP VIEW IF EXISTS other')):
            with self.subTest(sql=sql),self.assertRaises(Refused):assert_native_sql(sql,target,temp,False)
        with self.assertRaises(Refused):assert_native_sql(change,target,temp,False,True)

    def test_status_guard_requires_native_snapshot_success_or_reused(self):
        from databricks_snapshots import assert_status
        target='cdo_demo_db.owned.consumer';uid='snapshot.project.consumer'
        result={'results':[{'unique_id':uid,'relation_name':target,'status':'success','failures':None,'adapter_response':{'code':'SUCCESS','query_id':'native'}}]}
        assert_status(result,uid,target,False)
        result['results'][0]['status']='warn'
        with self.assertRaises(Refused):assert_status(result,uid,target,False)

    def test_changed_version_requires_same_closing_and_opening_microsecond(self):
        from databricks_snapshot_oracle import Bounds, micros
        first=micros('2026-11-12 01:50:34.797584');second=first+1000000
        ledger=Ledger('check','ignore').execute((A,),'first').execute((C,),'change')
        bounds={'first':Bounds(first-1,first+1,'b1','a1','w','first'),
                'change':Bounds(second-1,second+1,'b2','a2','w','change')}
        rows=[(*A,'fdd93c28963e0f5acc6d18d69683332a','2026-11-12 01:50:34.797584','2026-11-12 01:50:34.797584','2026-11-12 01:50:35.797584'),
              (*C,'740cc3c6d89040f641fa89b96a5263ea','2026-11-12 01:50:35.797584','2026-11-12 01:50:35.797584',None)]
        assert_history(rows,ledger,bounds)
        bad=[list(r) for r in rows];bad[0][7]='2026-11-12 01:50:35.797585'
        with self.assertRaises(Refused):assert_history(bad,ledger,bounds)
        for fault in ('warehouse','overlap','reuse-query'):
            wrong=dict(bounds)
            if fault=='warehouse':wrong['change']=Bounds(second-1,second+1,'b2','a2','other','change')
            if fault=='overlap':wrong['change']=Bounds(first,second+1,'b2','a2','w','change')
            if fault=='reuse-query':wrong['change']=Bounds(second-1,second+1,'b1','a2','w','change')
            with self.subTest(fault=fault),self.assertRaises(Refused):assert_history(rows,ledger,wrong)

    def test_native_source_projection_verifies_full_typed_source_and_microsecond_precision(self):
        from databricks_snapshot_oracle import assert_source, SOURCE_PROJECTION_SCHEMA, micros
        columns=[dict(zip(('name','type_text','type_name','position'),c)) for c in SOURCE_PROJECTION_SCHEMA]
        value={'columns':columns,'rows':[(*A,str(micros(A[3]))),(*B,str(micros(B[3])))]}
        assert_source(value,(A,B))
        from copy import deepcopy
        for fault in ('missing','old','epoch','duplicate','schema'):
            bad=deepcopy(value);bad['rows']=[list(r) for r in bad['rows']]
            if fault=='missing':bad['rows'].pop()
            if fault=='old':bad['rows'][0][1]='wrong'
            if fault=='epoch':bad['rows'][0][4]=str(micros(A[3])+1)
            if fault=='duplicate':bad['rows'].append(bad['rows'][0])
            if fault=='schema':bad['columns'][0]['type_name']='STRING'
            with self.subTest(fault=fault),self.assertRaises(Refused):assert_source(bad,(A,B))

    def test_execute_confirm_matches_retained_native_wire_shape(self):
        from databricks_snapshots import snapshot_submit
        source='cdo_demo_db.owned.inputs';target='cdo_demo_db.owned.consumer'
        request={'dialect':'databricks','execution_type':7,'compare_unrendered_code':False,
          'table_namespace':'owned','query_dependencies':[],'semantic_extras':{'unique_key':'"id"'},
          'ignore_external_modifications':False,'freshness_tolerance_seconds':0,'stale_upstream_policy':0,
          'tables':[{'name':source,'last_modified_epoch':10}],
          'target_table':target,'labels':{'dbt_node_name':'consumer'},'tolerate_nondeterminism':True}
        calls=[{'method':'SubmitEnrichedSQL','request':request,'response':{'response':{'ready_to_execute':{'request_id':'pending'}}}},
          {'method':'ConfirmExecution','request':{'request_id':'pending','last_modified_epoch':20,'execution_runtime_ms':5617,'failed_to_clone':False},
           'response':{'success':True,'request_id':'pending'}}]
        for call in calls:
            call['status']={'code':0};call['service']={'SubmitEnrichedSQL':'SQL','ConfirmExecution':'Execution'}[call['method']]
        snapshot_submit(calls,source,target,False,'ready_to_execute')
        from copy import deepcopy
        spec=deepcopy(calls[0]);spec['method']='SubmitEnrichedSQLSpeculative';spec['request']['tables'][0]['last_modified_epoch']=None;spec['response']={'response':{'undecided':{}}}
        telemetry={'service':'ClientTelemetry','method':'SubmitTelemetryBatch','status':{'code':0},'request':{'events':[{'event_order':0}]},'response':{'success':True}}
        native=[telemetry,spec,*calls]
        snapshot_submit(native,source,target,False,'ready_to_execute')
        for fault in ('status','missing-status','bool-status','unknown','telemetry','spec-decision','spec-epoch','spec-source','extra-spec','wrong-service','refusal'):
            bad=deepcopy(native)
            if fault=='status':bad[0]['status']['code']=13
            if fault=='missing-status':bad[0].pop('status')
            if fault=='bool-status':bad[0]['status']['code']=False
            if fault=='unknown':bad[0]['method']='UnrecognizedRPC'
            if fault=='wrong-service':bad[0]['service']='SQL'
            if fault=='refusal':bad[2]['response']=None;bad[2]['status']['code']=3
            if fault=='telemetry':bad[0]['response']['success']=False
            if fault=='spec-decision':bad[1]['response']={'response':{'skip_execution':{}}}
            if fault=='spec-epoch':bad[1]['request']['tables'][0]['last_modified_epoch']=1
            if fault=='spec-source':bad[1]['request']['tables'][0]['name']='other'
            if fault=='extra-spec':bad.insert(1,bad[1])
            with self.subTest(fault=fault),self.assertRaises(Refused):snapshot_submit(bad,source,target,False,'ready_to_execute')
        for fault in ('foreign-response','foreign-request','unacknowledged','unknown-epoch','failed-clone','runtime'):
            bad=deepcopy(calls)
            if fault=='foreign-response':bad[1]['response']['request_id']='other'
            if fault=='foreign-request':bad[1]['request']['request_id']='other'
            if fault=='unacknowledged':bad[1]['response']['success']=False
            if fault=='unknown-epoch':bad[1]['request']['last_modified_epoch']=None
            if fault=='failed-clone':bad[1]['request']['failed_to_clone']=True
            if fault=='runtime':bad[1]['request']['execution_runtime_ms']=0
            with self.subTest(fault=fault),self.assertRaises(Refused):snapshot_submit(bad,source,target,False,'ready_to_execute')

    def test_reuse_requires_own_confirmed_target_and_unchanged_source_sql_and_history(self):
        from databricks_snapshots import assert_reuse_evidence, assert_same_history
        from copy import deepcopy
        source='cdo_demo_db.owned.inputs';target='cdo_demo_db.owned.consumer'
        previous={'sql':'select * from '+source,'semantic_extras':{'unique_key':'"id"'},
                  'table_namespace':'owned','execution_type':7,
                  'tables':[{'name':source,'last_modified_epoch':10},{'name':target,'last_modified_epoch':15}]}
        current=deepcopy(previous);current['tables'][1]['last_modified_epoch']=20
        assert_reuse_evidence(current,previous,source,target,20)
        for fault in ('target','source','sql','extras','namespace','unknown-confirm'):
            bad=deepcopy(current);epoch=20
            if fault=='target':bad['tables'][1]['last_modified_epoch']=21
            if fault=='source':bad['tables'][0]['last_modified_epoch']=11
            if fault=='sql':bad['sql']+=' where id=1'
            if fault=='extras':bad['semantic_extras']['unique_key']='"v"'
            if fault=='namespace':bad['table_namespace']='other'
            if fault=='unknown-confirm':epoch=None
            with self.subTest(fault=fault),self.assertRaises(Refused):assert_reuse_evidence(bad,previous,source,target,epoch)
        history={'columns':[{'name':'version','type_name':'LONG','type_text':'BIGINT','position':0}],
                 'rows':[['1'],['0']]}
        reordered=deepcopy(history);reordered['rows'].reverse()
        assert_same_history(history,reordered)
        for fault in ('extra','missing','changed','schema'):
            bad=deepcopy(history)
            if fault=='extra':bad['rows'].append(['2'])
            if fault=='missing':bad['rows'].pop()
            if fault=='changed':bad['rows'][0][0]='2'
            if fault=='schema':bad['columns'][0]['type_name']='STRING'
            with self.subTest(fault=fault),self.assertRaises(Refused):assert_same_history(history,bad)

    def test_reviewed_literal_ledger_refuses_disagreeing_dynamic_oracle(self):
        from databricks_snapshots import read_result_ledger, clearance_contract
        from pathlib import Path
        from unittest.mock import patch
        import hashlib
        path=Path(__file__).with_name('databricks_snapshot_results.json')
        grant=clearance_contract();grant['result_ledger_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
        for phase,run in grant['run_ids'].items():
            self.assertEqual(len(read_result_ledger(path,grant,phase,run)),12)
        execute=Ledger.execute
        def wrong_history(ledger,source,statement):
            source=tuple((r[0],'wrong-derived-business',r[2],r[3]) for r in source)
            return execute(ledger,source,statement)
        with patch.object(Ledger,'execute',wrong_history),self.assertRaises(Refused):
            read_result_ledger(path,grant,'hosted',grant['run_ids']['hosted'])

    def test_runner_rejects_missing_or_disagreeing_frozen_SCD_before_initialization(self):
        from databricks_snapshots import Snapshots, clearance_contract, operation_manifest
        from types import SimpleNamespace
        from pathlib import Path
        from copy import deepcopy
        from unittest.mock import patch
        import json, hashlib, tempfile
        original=json.loads(Path(__file__).with_name('databricks_snapshot_results.json').read_text())
        for phase in ('hosted','rust'):
            for fault in ('missing','digest','business','interval','hash','missing-cell','extra-cell','wrong-run'):
                with self.subTest(phase=phase,fault=fault),tempfile.TemporaryDirectory() as folder:
                    result=Path(folder)/'results.json';grant_path=Path(folder)/'grant.json'
                    value=deepcopy(original)
                    if fault=='business':value['cells']['upsert']['managed'][0][1]='wrong-old-business'
                    if fault=='interval':value['cells']['upsert']['managed'][0][7]=None
                    if fault=='hash':value['cells']['first']['independent_reference'][0][4]='wrong'
                    if fault=='missing-cell':del value['cells']['delete']
                    if fault=='extra-cell':value['cells']['extra']=value['cells']['first']
                    if fault=='wrong-run':value['run_ids'][phase]='20261114ffffff'
                    result.write_text(json.dumps(value))
                    grant=clearance_contract();grant.update(status='LIVE CLEARED #151 timestamp-ignore',authorized_phases=[phase],result_ledger_sha256=hashlib.sha256(result.read_bytes()).hexdigest())
                    if fault=='digest':result.write_text(result.read_text()+' ')
                    if fault=='missing':result.unlink()
                    operation=Path(folder)/'operation.json';operation.write_text(json.dumps(operation_manifest()))
                    grant.update(source_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('databricks_*.py')},operation_manifest_sha256=hashlib.sha256(json.dumps(operation_manifest(),sort_keys=True,separators=(',',':')).encode()).hexdigest(),operation_ledger_sha256=hashlib.sha256(operation.read_bytes()).hexdigest(),allocation={'receipt':str(operation),'receipt_sha256':hashlib.sha256(operation.read_bytes()).hexdigest()})
                    grant_path.write_text(json.dumps(grant))
                    args=SimpleNamespace(live_clearance=grant_path,run_id=grant['run_ids'][phase],rust_url='http://127.0.0.1:1' if phase=='rust' else None,result_ledger=result,operation_ledger=operation)
                    with patch('databricks_snapshots.Campaign.__init__') as initialize:
                        with self.assertRaises(Refused):Snapshots(args)
                        initialize.assert_not_called()

    def test_continuation_budget_skips_only_original_parse_and_first_managed(self):
        from databricks_snapshots_continuation import ContinuationGuard, SnapshotContinuation
        from databricks_snapshots import CELLS
        guard=ContinuationGuard()
        self.assertEqual(len(guard.expected),23)
        for label,command,managed in [('inventory',['parse'],False),('first',['snapshot','--select','consumer'],True)]:
            with self.assertRaises(Refused):guard.admit(label,command,managed)
        guard.admit('first_reference',['snapshot','--select','reference'],False)
        for label,_,_ in CELLS[1:]:
            guard.admit(label,['snapshot','--select','consumer'],True)
            guard.admit(label+'_reference',['snapshot','--select','reference'],False)
        with self.assertRaises(Refused):guard.admit('extra',['snapshot','--select','consumer'],True)
        runner=object.__new__(SnapshotContinuation);runner.bootstrap_complete=False
        with self.assertRaises(Refused):runner.prepare_initial()
        with self.assertRaises(Refused):runner.dbt('first_reference',['snapshot','--select','reference'],managed=False)
        with self.assertRaises(Refused):runner.source_change('upsert')
        runner.bootstrap_complete=True;runner.anchor_consumed=False;runner.guard=ContinuationGuard();runner.original_calls=['original']
        self.assertEqual(runner.managed_calls('first'),['original'])
        with self.assertRaises(Refused):runner.managed_calls('first')

    def test_continuation_requires_exact_three_objects_and_unchanged_physical_epochs(self):
        from databricks_snapshots_continuation import check_resume_inventory,history_epoch,continuation_contract
        from databricks_snapshots import SnapshotScope,clearance_contract
        from copy import deepcopy
        contract=clearance_contract();scope=SnapshotScope(contract['run_ids']['hosted'],contract)
        objects=[{'full_name':n,'table_type':'MANAGED'} for n in continuation_contract()['preexisting_names']]
        check_resume_inventory(scope,objects,objects)
        for bad in (objects[:-1],objects+[{'full_name':continuation_contract()['required_absent'],'table_type':'MANAGED'}]):
            with self.assertRaises(Refused):check_resume_inventory(scope,bad,objects)
        history={'columns':[{'name':'version','type_name':'LONG'},{'name':'timestamp','type_name':'TIMESTAMP'},{'name':'queryHistoryStatementId','type_name':'STRING'}],
                 'rows':[['1','2026-11-15T02:44:51.000Z','insert'],['0','2026-11-15T02:44:40.000Z','create']]}
        self.assertEqual(history_epoch(history,['create','insert'],1794710691000),1794710691000)
        for fault in ('query','version','extra','time','epoch','schema'):
            bad=deepcopy(history);epoch=1794710691000
            if fault=='query':bad['rows'][0][2]='other'
            if fault=='version':bad['rows'][0][0]='2'
            if fault=='extra':bad['rows'].append(bad['rows'][0])
            if fault=='time':bad['rows'][0][1]='2026-11-15T02:44:51Z'
            if fault=='epoch':epoch+=1000
            if fault=='schema':bad['columns'][1]['type_name']='STRING'
            with self.subTest(fault=fault),self.assertRaises(Refused):history_epoch(bad,['create','insert'],epoch)

    def test_whole_capture_requires_complete_chronological_byte_exact_sessions(self):
        from databricks_snapshots_continuation import combine_captures
        import tempfile,json
        from pathlib import Path
        with tempfile.TemporaryDirectory() as folder:
            def write(name,stamp,code=0):
                path=Path(folder)/name;path.write_text(json.dumps({'recorded_at':stamp,'status':{'code':code}})+'\n')
                Path(str(path)+'.capture.json').write_text(json.dumps({'complete':True,'failed':False,'started':1,'recorded':1}))
                return path
            first=write('first.jsonl','2026-11-15T02:44:55.000000001Z');second=write('second.jsonl','2026-11-15T03:00:00.000000001Z')
            output=Path(folder)/'whole.jsonl';receipt=combine_captures(first,second,output)
            self.assertEqual(output.read_bytes(),first.read_bytes()+second.read_bytes());self.assertEqual(receipt['records'],2)
            with self.assertRaises(Refused):combine_captures(second,first,Path(folder)/'backwards.jsonl')
            failed=write('failed.jsonl','2026-11-15T03:00:00.000000001Z',13)
            with self.assertRaises(Refused):combine_captures(first,failed,Path(folder)/'failed-whole.jsonl')

if __name__=='__main__':unittest.main()
