"""Offline faults at #145's complete typed cumulative Result boundary."""
from copy import deepcopy
import unittest
from databricks_merge_settings import HISTORY, SCHEMA, assert_typed, merge_model, lifecycle, assert_reference, literal_reference, assert_merge_sql, hosted_contract, CELLS, CONFIGS, SettingsScope, clearance_contract, assert_client_status
from databricks_survey import Refused


def actual(rows):
    return {'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in SCHEMA],
            'rows':[list(r) for r in rows]}


class SettingsOracle(unittest.TestCase):
    def test_actual_null_key_update_retains_one_null_and_deleted_rows(self):
        prior=HISTORY['first'];case=HISTORY['null_update']
        self.assertCountEqual(merge_model(prior.R,case.source),case.R)
        assert_typed(actual(case.R),case.R)
        wrong=actual(case.R);wrong['rows'].append([None,None,None])
        with self.assertRaisesRegex(Refused,'physical:rows'):assert_typed(wrong,case.R)

    def test_include_exclude_updates_do_not_restrict_insert_and_preserve_deleted(self):
        for prior,stage in [('null_update','include'),('include','exclude')]:
            case=HISTORY[stage]
            self.assertCountEqual(merge_model(HISTORY[prior].R,case.source,('amount',)),case.R)
            assert_typed(actual(case.R),case.R)
            for fault in ('copy-fresh','update-tag','restrict-insert','drop-deleted'):
                wrong=actual(case.R)
                if fault=='copy-fresh':wrong=actual(case.F)
                elif fault=='update-tag':wrong['rows'][0][2]='newer' if stage=='include' else 'newest'
                elif fault=='restrict-insert':wrong['rows'][-1][2]=None
                else:wrong['rows'].pop(1)
                with self.subTest(stage=stage,fault=fault),self.assertRaisesRegex(Refused,'physical:rows'):
                    assert_typed(wrong,case.R)

    def test_destination_predicate_is_on_qualification_and_preserves_exact_duplicate(self):
        case=HISTORY['predicate']
        self.assertCountEqual(merge_model(HISTORY['exclude'].R,case.source,('id','amount'),True),case.R)
        self.assertEqual(case.R.count(('1','13','changed')),2)
        for fault in ('on-as-where','lost-duplicate','null-equals','copy-fresh'):
            wrong=actual(case.R)
            if fault=='on-as-where':wrong=actual(HISTORY['exclude'].R+(('7','70','insert-seven'),))
            elif fault=='lost-duplicate':wrong['rows'].pop(-2)
            elif fault=='null-equals':wrong['rows'].append([None,'32','null-newest'])
            else:wrong=actual(case.F)
            with self.subTest(fault=fault),self.assertRaisesRegex(Refused,'physical:rows'):assert_typed(wrong,case.R)

    def test_complete_reference_rejects_lossy_null_duplicate_type_or_extra_sql(self):
        import json
        case=HISTORY['predicate'];sql=literal_reference(case.R)
        rows=[{'id':None if r[0] is None else int(r[0]),'amount':None if r[1] is None else int(r[1]),'tag':r[2]} for r in case.R]
        assert_reference(json.dumps(rows),'',actual(case.R),case.R,sql,sql+';',sql)
        for fault in ('duplicate','null','bool','schema','extra-sql','stderr'):
            wrong=deepcopy(rows);direct=actual(case.R);query=sql+';';stderr=''
            if fault=='duplicate':wrong.pop(-2)
            elif fault=='null':wrong[3]['id']=0
            elif fault=='bool':wrong[0]['id']=True
            elif fault=='schema':direct['columns'][1]['type_text']='BIGINT'
            elif fault=='extra-sql':query+=' DROP TABLE protected;'
            else:stderr='warning'
            with self.subTest(fault=fault),self.assertRaises(Refused):
                assert_reference(json.dumps(wrong),stderr,direct,case.R,sql,query,sql)

    def test_failure_cannot_publish_reusable_success(self):
        calls=[{'method':'SubmitEnrichedSQL','status':{'code':0},
          'request':{'labels':{'dbt_node_name':'consumer'},'freshness_tolerance_seconds':0,'stale_upstream_policy':0},
          'response':{'response':{'ready_to_execute':{'request_id':'owned'}}}},
          {'method':'ConfirmExecution','status':{'code':0},'request':{'request_id':'owned','last_modified_epoch':12,'execution_runtime_ms':30},'response':{'success':True}}]
        lifecycle(calls,'ready_to_execute')
        for fault in ('failed-confirm','wrong-confirm','error','record','skip-confirm'):
            wrong=deepcopy(calls)
            if fault=='failed-confirm':wrong[1]['response']['success']=False
            elif fault=='wrong-confirm':wrong[1]['request']['request_id']='foreign'
            elif fault=='error':wrong[0]['status']['code']=3
            elif fault=='record':wrong.append({'method':'RecordExecutions'})
            else:wrong[0]['response']['response']={'skip_execution':{}}
            with self.subTest(fault=fault),self.assertRaises(Refused):lifecycle(wrong)

    def test_exact_native_sql_rejects_where_equals_insert_exclusion_and_foreign_writes(self):
        target='cdo_demo_db.owned.ms_run_consumer';source='cdo_demo_db.owned.ms_run_inputs';tmp='ms_run_consumer__dbt_tmp'
        sql=(f'create or replace temporary view {tmp} as select id, amount, tag from {source}; '
             f'merge into {target} as DBT_INTERNAL_DEST using {tmp} as DBT_INTERNAL_SOURCE '
             'on (DBT_INTERNAL_DEST.id >= 2 OR DBT_INTERNAL_DEST.id IS NULL) and '
             'DBT_INTERNAL_SOURCE.id <=> DBT_INTERNAL_DEST.id '
             'when matched then update set id = DBT_INTERNAL_SOURCE.id, amount = DBT_INTERNAL_SOURCE.amount '
             'when not matched then insert *; '
             f'alter table {target} cluster by none;')
        assert_merge_sql(sql,target,source,tmp,'predicate')
        for fault in (sql.replace('<=>','='),sql.replace('on (','where ('),
                      sql.replace('insert *','insert (amount) values (DBT_INTERNAL_SOURCE.amount)'),
                      sql.replace(source,'cdo_demo_db.owned.protected'),sql+f'DROP TABLE {target};',
                      sql.replace('amount = DBT_INTERNAL_SOURCE.amount','tag = DBT_INTERNAL_SOURCE.tag')):
            with self.subTest(fault=fault),self.assertRaises(Refused):assert_merge_sql(fault,target,source,tmp,'predicate')

    def test_typed_schema_positions_and_properties_are_not_coerced(self):
        for field,value in [('type_text','BIGINT'),('position',True),('name','other')]:
            wrong=actual(HISTORY['first'].R);wrong['columns'][1][field]=value
            with self.subTest(field=field),self.assertRaisesRegex(Refused,'physical:schema'):assert_typed(wrong,HISTORY['first'].R)

    def test_config_decisions_require_whole_hosted_history_and_do_not_come_from_rust(self):
        # Synthetic protocol fault controls, never a claim about hosted policy.
        import json
        source='cdo_demo_db.owned.inputs';target='cdo_demo_db.owned.consumer';calls=[]
        epoch=10
        for label,stage,config,expected in CELLS:
            if label in ('null_update','include_delta','exclude_delta','predicate_delta'):epoch+=10
            choice='skip_execution' if expected=='DISCOVERY' else expected
            extras={'unique_key':'"id"','on_schema_change':'"ignore"','file_format':'"delta"'}
            extras.update({k:json.dumps(v) for k,v in CONFIGS[config].items()})
            request={'labels':{'dbt_node_name':'consumer'},'freshness_tolerance_seconds':0,'stale_upstream_policy':0,
              'dialect':'databricks','execution_type':3,'compare_unrendered_code':False,'table_namespace':'ns',
              'query_dependencies':[],'semantic_extras':extras,'clone_chain_depth_limit':0,'ignore_external_modifications':False,
              'target_table':target,'sql':'select id, amount, tag from '+source,
              'tables':[{'name':source,'last_modified_epoch':epoch}]+([] if label=='first' else [{'name':target,'last_modified_epoch':epoch}])}
            calls.append({'method':'SubmitEnrichedSQL','status':{'code':0},'request':request,
                          'response':{'response':{choice:{'request_id':label}}}})
            if choice=='ready_to_execute':calls.append({'method':'ConfirmExecution','status':{'code':0},
              'request':{'request_id':label,'last_modified_epoch':epoch,'execution_runtime_ms':12},'response':{'success':True}})
        contract=hosted_contract(calls,source,target)
        self.assertEqual(contract['cells'],12)
        self.assertEqual(contract['decisions']['config_include'],'skip_execution')
        for fault in ('partial','failed-confirm','config-source-changed','wire-setting-changed'):
            wrong=deepcopy(calls)
            if fault=='partial':wrong.pop()
            elif fault=='failed-confirm':wrong[1]['response']['success']=False
            elif fault=='config-source-changed':next(r for r in wrong if r['method']=='SubmitEnrichedSQL' and r['response']['response'].get('skip_execution',{}).get('request_id')=='config_include')['request']['tables'][0]['last_modified_epoch']=999
            else:next(r for r in wrong if r['method']=='SubmitEnrichedSQL' and r['request']['semantic_extras'].get('incremental_predicates'))['request']['semantic_extras']['incremental_predicates']='[]'
            with self.subTest(fault=fault),self.assertRaises(Refused):hosted_contract(wrong,source,target)

    def test_scope_never_writes_prior_graphs_reference_or_schema(self):
        proposal=clearance_contract()
        for run in proposal['run_ids'].values():
            scope=SettingsScope(run,proposal)
            self.assertEqual(len(scope.permanent),2)
            scope.relation('prod','inputs');scope.relation('prod','consumer')
            for name in ('inc_merge','producer','m_20261114a145aa_consumer','a_20261114a146aa_inputs'):
                with self.subTest(name=name),self.assertRaises(Refused):scope.check('cdo_demo_db.'+proposal['schema']+'.'+name)
            with self.assertRaises(Refused):scope.check('cdo_demo_db.oldschema.ms_'+run+'_consumer')

    def test_native_client_success_requires_actual_query_receipt_and_own_uid(self):
        good={'results':[{'unique_id':'model.owned.consumer','relation_name':'cdo_demo_db.owned.consumer',
          'status':'success','failures':None,'adapter_response':{'code':'SUCCESS','query_id':'native-query'}}]}
        assert_client_status(good,'model.owned.consumer','cdo_demo_db.owned.consumer','ready_to_execute')
        for fault in ('uid','target','query','failure','status'):
            wrong=deepcopy(good);row=wrong['results'][0]
            if fault=='uid':row['unique_id']='other'
            elif fault=='target':row['relation_name']='protected'
            elif fault=='query':row['adapter_response'].pop('query_id')
            elif fault=='failure':row['failures']=1
            else:row['status']='error'
            with self.subTest(fault=fault),self.assertRaises(Refused):assert_client_status(wrong,'model.owned.consumer','cdo_demo_db.owned.consumer','ready_to_execute')


if __name__=='__main__':unittest.main()
