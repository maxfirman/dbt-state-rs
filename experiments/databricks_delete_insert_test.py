import unittest
from collections import Counter
from databricks_delete_insert import HISTORY, CELLS, replacement

class DeleteInsertOracle(unittest.TestCase):
    def test_null_safe_replacement_preserves_multiplicity(self):
        self.assertEqual(Counter(replacement(((None,'old'),(None,'old'),('1','keep')),((None,'new'),(None,'new')))),Counter(((None,'new'),(None,'new'),('1','keep'))))
    def test_absent_null_and_nonnull_keys_retain_all_old_rows(self):
        self.assertEqual(Counter(replacement(((None,'old'),(None,'old'),('2','keep'),('1','old')), (('1','new'),))),Counter(((None,'old'),(None,'old'),('2','keep'),('1','new'))))
    def test_literal_history_agrees_with_key_operation(self):
        previous=()
        for label,stage,expected in CELLS:
            case=HISTORY[stage]
            if label=='first' or expected=='record':actual=case.F
            elif expected=='skip_execution':actual=previous
            else:actual=replacement(previous,case.F)
            self.assertEqual(Counter(actual),Counter(case.R),label)
            previous=case.R
    def test_empty_membership_is_identity(self):
        self.assertEqual(replacement(((None,'a'),('1','b')),()),((None,'a'),('1','b')))
    def test_composite_membership_is_whole_tuple(self):
        self.assertEqual(Counter(replacement(((1,2,'keep'),(2,1,'keep'),(None,2,'old')),((1,1,'new'),(2,2,'new'),(None,2,'new')), key_columns=(0,1))),Counter(((1,2,'keep'),(2,1,'keep'),(1,1,'new'),(2,2,'new'),(None,2,'new'))))

class CampaignGuards(unittest.TestCase):
    def test_exact_native_key_replacement_refuses_legacy_and_foreign_writes(self):
        from databricks_delete_insert import assert_replace_on_sql
        from databricks_survey import Refused
        target='cdo_demo_db.state_20261114_append146.di_20261114a149aa_consumer'
        temporary='di_20261114a149aa_consumer__dbt_tmp'
        sql=f'create or replace temporary view {temporary} as select id,v,p,ts from owned_source; insert into table {target} as target replace on (target.id <=> temp.id) (select id,v,p,ts from {temporary}) as temp; alter table {target} cluster by none;'
        assert_replace_on_sql(sql,target,temporary)
        for bad in (sql.replace('<=>','='),sql.replace('target.id','target.p'),sql.replace(temporary,'other'),sql.replace(target,'protected'),sql+' DELETE FROM protected;',sql.replace('replace on','overwrite')):
            with self.subTest(sql=bad),self.assertRaises(Refused):assert_replace_on_sql(bad,target,temporary)
    def test_complete_rows_reject_fresh_substitution_duplicate_collapse_and_schema_drift(self):
        from databricks_delete_insert import SCHEMA,assert_typed
        from databricks_survey import Refused
        from copy import deepcopy
        case=HISTORY['remove_null']
        actual={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in SCHEMA], 'rows':[list(r) for r in case.R]}
        assert_typed(actual,case.R)
        for fault in ('F','source','duplicate','null','schema','order'):
            bad=deepcopy(actual)
            if fault=='F':bad['rows']=case.F
            if fault=='source':bad['rows']=case.source
            if fault=='duplicate':bad['rows'].pop(0)
            if fault=='null':bad['rows']=[r for r in bad['rows'] if r[0] is not None]
            if fault=='schema':bad['columns'][2]['type_text']='STRING'
            if fault=='order':bad['columns'].reverse()
            with self.subTest(fault=fault),self.assertRaises(Refused):assert_typed(bad,case.R)

class BoundaryControls(unittest.TestCase):
    def test_nullable_typed_select_reference_rejects_null_or_timestamp_coercion(self):
        import json
        from copy import deepcopy
        from databricks_delete_insert import SCHEMA,assert_reference,literal_reference
        from databricks_survey import Refused
        rows=HISTORY['first'].R
        direct={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in SCHEMA], 'rows':rows}
        shown=[dict(zip(('id','v','p','ts'),(None if r[0] is None else int(r[0]),*r[1:]))) for r in rows]
        sql=literal_reference(rows)
        assert_reference(json.dumps(shown),'',direct,rows,sql,sql+';',sql)
        for fault in ('null','bool','timestamp','duplicate','write'):
            bad=deepcopy(shown);query=sql+';'
            if fault=='null':bad[3]['id']=0
            if fault=='bool':bad[0]['id']=True
            if fault=='timestamp':bad[0]['ts']='2026-11-01T12:00:00Z'
            if fault=='duplicate':bad.pop(3)
            if fault=='write':query+=' DROP TABLE protected;'
            with self.subTest(fault=fault),self.assertRaises(Refused):assert_reference(json.dumps(bad),'',direct,rows,sql,query,sql)
    def test_scope_refuses_siblings_persistent_auxiliary_and_expanded_lease(self):
        from copy import deepcopy
        from databricks_delete_insert import DeleteInsertScope,clearance_contract
        from databricks_survey import Refused
        grant=clearance_contract();scope=DeleteInsertScope('20261114a149aa',grant)
        scope.check('cdo_demo_db.state_20261114_append146.di_20261114a149aa_inputs')
        for name in ('a_20261114a146aa_inputs','io_20261114a147aa_consumer','rw_20261114a148aa_inputs','di_20261114b149bb_consumer','di_20261114a149aa_consumer__other'):
            with self.subTest(name=name),self.assertRaises(Refused):scope.check('cdo_demo_db.state_20261114_append146.'+name)
        bad=deepcopy(grant);bad['max_extra_concurrent_persistent']=2
        with self.assertRaises(Refused):DeleteInsertScope('20261114a149aa',bad)
    def test_full_record_requires_acknowledged_positive_outcome(self):
        from copy import deepcopy
        from databricks_delete_insert import full_refresh_record
        from databricks_survey import Refused
        target='cdo_demo_db.owned.consumer'
        record={'input':{'enriched_sql':{'execution_type':1,'target_table':target}},'outcome':{'last_modified_epoch':123,'execution_runtime_ms':456}}
        calls=[{'method':'RecordExecutions','request':{'records':[record]},'response':{'records_stored':1}}]
        self.assertEqual(full_refresh_record(calls,target),record)
        for fault in ('type','epoch','runtime','ack','extra'):
            bad=deepcopy(calls)
            if fault=='type':bad[0]['request']['records'][0]['input']['enriched_sql']['execution_type']=0
            if fault=='epoch':bad[0]['request']['records'][0]['outcome']['last_modified_epoch']=None
            if fault=='runtime':bad[0]['request']['records'][0]['outcome']['execution_runtime_ms']=0
            if fault=='ack':bad[0]['response']['records_stored']=True
            if fault=='extra':bad.append({'method':'ConfirmExecution'})
            with self.subTest(fault=fault),self.assertRaises(Refused):full_refresh_record(bad,target)

class RefreshContractControls(unittest.TestCase):
    def test_native_refresh_kind_must_match_prior_hosted_observation(self):
        from databricks_delete_insert import full_refresh_record
        from databricks_survey import Refused
        target='cdo_demo_db.owned.consumer'
        record={'input':{'enriched_sql':{'execution_type':5,'target_table':target}},'outcome':{'last_modified_epoch':123,'execution_runtime_ms':456}}
        calls=[{'method':'RecordExecutions','request':{'records':[record]},'response':{'records_stored':1}}]
        self.assertEqual(full_refresh_record(calls,target,expected_kind=5),record)
        with self.assertRaises(Refused):full_refresh_record(calls,target,expected_kind=1)

class SubmitControls(unittest.TestCase):
    def test_public_submit_requires_exact_oneof_and_scoped_positive_evidence(self):
        from copy import deepcopy
        from databricks_delete_insert import delete_insert_submit
        from databricks_survey import Refused
        source='cdo_demo_db.owned.di_inputs';target='cdo_demo_db.owned.di_consumer'
        request={'dialect':'databricks','execution_type':5,'compare_unrendered_code':False,
          'table_namespace':'own','query_dependencies':{},'target_table':target,
          'semantic_extras':{'unique_key':'"id"','on_schema_change':'"ignore"','file_format':'"delta"'},
          'tables':[{'name':source,'last_modified_epoch':123}], 'clone_chain_depth_limit':0,
          'ignore_external_modifications':False,'freshness_tolerance_seconds':0,'stale_upstream_policy':0,
          'labels':{'dbt_node_name':'consumer'}}
        calls=[{'method':'SubmitEnrichedSQL','request':request,'response':{'response':{'ready_to_execute':{'request_id':'own'}}}},
          {'method':'ConfirmExecution','request':{'request_id':'own'},'response':{'success':True}}]
        delete_insert_submit(calls,source,target,False,'ready_to_execute')
        for fault in ('oneof','type','unknown','duplicate','target','predicate','depth','failed-confirm'):
            bad=deepcopy(calls)
            if fault=='oneof':bad[0]['response']['response']['skip_execution']={}
            if fault=='type':bad[0]['request']['execution_type']=3
            if fault=='unknown':bad[0]['request']['tables'][0]['last_modified_epoch']=None
            if fault=='duplicate':bad[0]['request']['tables']*=2
            if fault=='target':bad[0]['request']['target_table']='cdo_demo_db.protected.foreign'
            if fault=='predicate':bad[0]['request']['semantic_extras']['incremental_predicates']='["id>0"]'
            if fault=='depth':bad[0]['request']['clone_chain_depth_limit']=1
            if fault=='failed-confirm':bad[1]['response']['success']=False
            with self.subTest(fault=fault),self.assertRaises(Refused):delete_insert_submit(bad,source,target,False,'ready_to_execute')

class CodecControls(unittest.TestCase):
    def test_observed_whole_second_utc_codec_is_lossless_with_nullable_key(self):
        import json
        from databricks_delete_insert import SCHEMA,assert_reference,literal_reference
        rows=((None,'a','2026-11-01','2026-11-01T12:00:00.000Z'),)
        direct={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in SCHEMA],'rows':rows}
        sql=literal_reference(rows)
        shown=[{'id':None,'v':'a','p':'2026-11-01','ts':'2026-11-01T12:00:00Z'}]
        result=assert_reference(json.dumps(shown),'',direct,rows,sql,sql+';',sql)
        self.assertEqual(result['observed_show_whole_second_rows'],1)

class FootprintControls(unittest.TestCase):
    def test_native_safe_replacement_has_one_shared_persistent_peak(self):
        from databricks_delete_insert import native_footprint
        from databricks_survey import Refused
        target='cdo_demo_db.state_20261114_append146.di_20261114a149aa_consumer';temp='di_20261114a149aa_consumer__dbt_tmp'
        sql=f'create or replace temporary view {temp} as select id,v,p,ts from own_source; create table {target}__dbt_stg using delta as select id,v,p,ts from {temp}; alter table {target} rename to {target}__dbt_backup; alter table {target}__dbt_stg rename to {target}; drop table {target}__dbt_backup;'
        self.assertEqual(native_footprint(sql,target,temp,True)['peak_extra_persistent_beyond_target'],1)
        for bad in (sql.replace('__dbt_stg','__other'),sql.replace('__dbt_backup','__other'),sql+' CREATE TABLE '+target+'__dbt_stg USING DELTA AS SELECT 1;',sql.replace('alter table '+target+' rename to '+target+'__dbt_backup;','create table '+target+'__dbt_backup using delta as select 1;')):
            with self.subTest(sql=bad),self.assertRaises(Refused):native_footprint(bad,target,temp,True)

if __name__=='__main__':unittest.main()
