"""Offline contract examples; warehouse semantics remain unverified."""
import unittest
from collections import Counter
import databricks_microbatch as m

class Windows(unittest.TestCase):
    def test_cumulative_windows_follow_literal_ledger(self):
        prior=()
        for name in m.STAGES:
            case=m.HISTORY[name]
            if name=='update':prior=prior+(m.target_null,)
            prior=m.apply_window(prior,case.source,*case.window,planned_target_null=True)
            self.assertEqual(Counter(prior),Counter(case.R),name)
            self.assertEqual(Counter(m.selected(case.source,*case.window)),Counter(case.F),name)
    def test_null_predicate_is_unknown_and_unproved_target_null_stops(self):
        null=(None,'null',None,None)
        self.assertIsNone(m.in_window(null,'2026-11-01','2026-11-03'))
        with self.assertRaisesRegex(m.Refused,'target NULL'):
            m.apply_window((null,),(),'2026-11-01','2026-11-03')
    def test_empty_revisit_deletes_window_and_retains_old_outside(self):
        case=m.HISTORY['empty']
        self.assertEqual(Counter(case.R),Counter((m.moved,m.edge,m.target_null)))
        self.assertEqual(case.F,())
    def test_duplicate_and_null_keys_are_bags(self):
        self.assertEqual(Counter(m.HISTORY['first'].R)[m.b],2)
        self.assertIn(m.null_id,m.HISTORY['first'].R)
    def test_window_bounds_include_start_exclude_end(self):
        self.assertTrue(m.in_window(('8','x',None,'2026-11-01T00:00:00.000Z'),'2026-11-01','2026-11-03'))
        self.assertFalse(m.in_window(('8','x',None,'2026-11-03T00:00:00.000Z'),'2026-11-01','2026-11-03'))

class TargetNullPlan(unittest.TestCase):
    def test_manual_null_row_is_retained_in_every_later_literal_R_not_F(self):
        for stage in ('update','shifted','overlap','empty'):
            case=m.HISTORY[stage]
            self.assertIn(m.target_null,case.R)
            self.assertNotIn(m.target_null,case.source)
            self.assertNotIn(m.target_null,case.F)
            actual={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in m.SCHEMA],
              'rows':tuple(row for row in case.R if row!=m.target_null)}
            with self.assertRaisesRegex(m.Refused,'physical:rows'):m.assert_typed(actual,case.R)

    def test_nullable_metadata_is_exact_and_fails_closed(self):
        good={'full_name':'cdo_demo_db.owned.consumer','table_type':'MANAGED','data_source_format':'DELTA',
          'columns':[{'name':'ts','type_name':'TIMESTAMP','nullable':True}]}
        m.require_nullable_target(good,'cdo_demo_db.owned.consumer')
        for value in (False,None,1):
            with self.subTest(value=value), self.assertRaises(m.Refused):
                m.require_nullable_target(dict(good,columns=[{'name':'ts','type_name':'TIMESTAMP','nullable':value}]),good['full_name'])
        with self.assertRaises(m.Refused):m.require_nullable_target(dict(good,columns=[]),good['full_name'])
        with self.assertRaises(m.Refused):m.require_nullable_target(good,'cdo_demo_db.foreign.consumer')

class NativeGuard(unittest.TestCase):
    def test_primary_two_batch_sql_and_foreign_mutations(self):
        # Literal native primary shape, independent of guard generation.
        raw="""create or replace table cdo_demo_db.ds_20261112a134ef_prod.inc_microbatch using delta as
        select * from (select * from cdo_demo_db.ds_20261112a134ef_raw.inputs where ts >= '2026-11-01T00:00:00+00:00' and ts < '2026-11-02T00:00:00+00:00');
        create or replace temporary view inc_microbatch__dbt_tmp_20261102 as select * from (select * from cdo_demo_db.ds_20261112a134ef_raw.inputs where ts >= '2026-11-02T00:00:00+00:00' and ts < '2026-11-03T00:00:00+00:00');
        INSERT INTO cdo_demo_db.ds_20261112a134ef_prod.inc_microbatch BY NAME REPLACE WHERE cast(ts as TIMESTAMP) >= '2026-11-02 00:00:00+00:00' and cast(ts as TIMESTAMP) < '2026-11-03 00:00:00+00:00' TABLE inc_microbatch__dbt_tmp_20261102;
        ALTER TABLE cdo_demo_db.ds_20261112a134ef_prod.inc_microbatch CLUSTER BY NONE;"""
        target='cdo_demo_db.ds_20261112a134ef_prod.inc_microbatch'
        source='cdo_demo_db.ds_20261112a134ef_raw.inputs'
        m.assert_batch_sql(raw,target,source,('2026-11-01','2026-11-03'),False)
        with self.assertRaises(m.Refused):
            m.assert_batch_sql(raw.replace('REPLACE WHERE','REPLACE ON'),target,source,('2026-11-01','2026-11-03'),False)
        with self.assertRaises(m.Refused):
            m.assert_batch_sql(raw+'; DELETE FROM '+target,target,source,('2026-11-01','2026-11-03'),False)

class Contract(unittest.TestCase):
    def test_exact_names_no_wildcards_or_unbounded_daily_temps(self):
        contract=m.clearance_contract()
        self.assertEqual(contract['max_final_permanent'],4)
        self.assertEqual(contract['max_extra_concurrent_persistent'],1)
        self.assertEqual(contract['limits']['managed_executed_daily_batches_per_backend'],9)
        self.assertEqual(contract['session_temporary_unqualified_names']['hosted'],[
          'mb_20261114a150aa_consumer__dbt_tmp_20261101','mb_20261114a150aa_consumer__dbt_tmp_20261102',
          'mb_20261114a150aa_consumer__dbt_tmp_20261103'])
    def test_wrong_result_mutations_detected(self):
        case=m.HISTORY['shifted']
        actual={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in m.SCHEMA], 'rows':case.R}
        m.assert_typed(actual,case.R)
        for wrong in (case.F,tuple(set(case.R)),case.R+(m.null_time,)):
            with self.assertRaises(m.Refused):m.assert_typed(dict(actual,rows=wrong),case.R)
    def test_source_null_exclusion_does_not_prove_target_null_retention(self):
        self.assertNotIn(m.null_time,m.HISTORY['first'].F)
        self.assertIn('target NULL retention',m.ledger()['unverified'])
        self.assertEqual(m.ledger()['manual_target_operations'][0]['row'],m.target_null)

class ReferenceCodec(unittest.TestCase):
    def test_evidenced_whole_second_encoding_retains_zero_fraction(self):
        import json
        rows=(m.a,m.null_id)
        direct={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in m.SCHEMA], 'rows':rows}
        objects=[dict(zip(('id','v','p','ts'),(None if row[0] is None else int(row[0]),row[1],row[2],row[3].replace('.000Z','Z')))) for row in rows]
        sql=m.literal_reference(rows)
        m.assert_reference(json.dumps(objects),'',direct,rows,sql,sql+';',sql)

class WireGuard(unittest.TestCase):
    def test_whole_command_window_identity_zero_lag_and_positive_epochs(self):
        import copy
        source='cdo_demo_db.state_20261114_append146.mb_20261114a150aa_inputs'
        target=source.replace('_inputs','_consumer')
        request={'execution_type':6,'dialect':'databricks','compare_unrendered_code':False,
          'default_catalog':'cdo_demo_db','default_schema':'state_20261114_append146',
          'table_namespace':'fresh','query_dependencies':[],'freshness_tolerance_seconds':0,
          'stale_upstream_policy':0,'clone_chain_depth_limit':0,'ignore_external_modifications':False,
          'sql':'select * from '+source,'target_table':target,'tables':[{'name':source,'last_modified_epoch':1}],
          'labels':{'dbt_node_name':'consumer'},'semantic_extras':{'unique_key':'"id"','file_format':'"delta"',
          'event_time':'"ts"','on_schema_change':'"ignore"','lookback':'1',
          '__microbatch_event_time_start':'2026-11-01T00:00:00+00:00','__microbatch_event_time_end':'2026-11-03T00:00:00+00:00'}}
        calls=[{'method':'SubmitEnrichedSQL','request':request,'response':{'response':{'ready_to_execute':{'request_id':'one'}}}},
          {'method':'ConfirmExecution','request':{'request_id':'one'},'response':{'success':True}}]
        m.microbatch_submit(calls,source,target,False,('2026-11-01','2026-11-03'),'ready_to_execute')
        for field,value in [('execution_type',1),('default_catalog','other'),('default_schema','other'),('freshness_tolerance_seconds',2700),('stale_upstream_policy',1),('sql','select * from old_source'),('tables',[{'name':source,'last_modified_epoch':None}])]:
            changed=copy.deepcopy(calls);changed[0]['request'][field]=value
            with self.assertRaises(m.Refused):m.microbatch_submit(changed,source,target,False,('2026-11-01','2026-11-03'),'ready_to_execute')
        with self.assertRaises(m.Refused):m.microbatch_submit(calls+calls,source,target,False,('2026-11-01','2026-11-03'),'ready_to_execute')


class UnchangedSkip(unittest.TestCase):
    def invoke_with_skip(self, response):
        import json
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import patch
        source='cdo_demo_db.state_20261114_append146.mb_20261114a150aa_inputs'
        target=source.replace('_inputs','_consumer')
        extras={'unique_key':'"id"','file_format':'"delta"','event_time':'"ts"',
          'on_schema_change':'"ignore"','lookback':'1',
          '__microbatch_event_time_start':'2026-11-01T00:00:00+00:00',
          '__microbatch_event_time_end':'2026-11-03T00:00:00+00:00'}
        request={'execution_type':6,'dialect':'databricks',
          'default_catalog':'cdo_demo_db','default_schema':'state_20261114_append146',
          'compare_unrendered_code':False,'table_namespace':'own','query_dependencies':[],
          'freshness_tolerance_seconds':0,'stale_upstream_policy':0,'clone_chain_depth_limit':0,
          'ignore_external_modifications':False,'sql':'select * from '+source,
          'target_table':target,'tables':[{'name':source,'last_modified_epoch':1},
          {'name':target,'last_modified_epoch':2}],'labels':{'dbt_node_name':'consumer'},
          'semantic_extras':extras}
        calls=[{'method':'SubmitEnrichedSQL','request':request,
          'response':{'response':{'skip_execution':response}}},
          {'method':'SubmitEnrichedSQLSpeculative'}]
        campaign=m.Microbatch.__new__(m.Microbatch)
        campaign.scope=SimpleNamespace(relation=lambda role,name:source if name=='inputs' else target)
        campaign.a=SimpleNamespace(run_id='20261114a150aa')
        campaign.work=Path('/unused-offline-microbatch');campaign.n=1
        campaign.previous_epochs={source:1,target:2};campaign.confirmed_epoch=2
        campaign.previous_extras=extras;campaign.last_runtime=9757
        campaign.dbt=lambda *args:calls
        campaign.save=lambda *args:None
        campaign.explain=lambda *args:None
        campaign.checked_results=lambda *args:None
        result={'results':[{'status':'reused','unique_id':'model.survey_20261114a150aa.consumer',
          'relation_name':target,'adapter_response':{},'failures':None,'batch_results':None}]}
        with patch.object(Path,'read_text',side_effect=[json.dumps(result),'']):
            campaign.invocation('first_repeat','first','skip_execution')

    def test_actual_nested_nonstale_shape_is_required(self):
        # Retained134 041/042 place this flag under explained_decision.
        self.invoke_with_skip({'execution_runtime_ms':9757,'explained_decision':{'is_stale':False}})
        for response in (
            {'execution_runtime_ms':9757,'explained_decision':{'is_stale':True}},
            {'execution_runtime_ms':9757,'explained_decision':{}},
            {'execution_runtime_ms':9757,'explained_decision':{'is_stale':None}},
            {'execution_runtime_ms':9757,'explained_decision':{'is_stale':0}},
            {'execution_runtime_ms':9757,'explained_decision':None},
            {'execution_runtime_ms':9757},
            {'execution_runtime_ms':9757,'is_stale':False,'explained_decision':{'is_stale':True}},
            {'execution_runtime_ms':9757,'is_stale':False,'explained_decision':{}},
        ):
            with self.subTest(response=response), self.assertRaisesRegex(m.Refused,'SKIP lacks own'):
                self.invoke_with_skip(response)

if __name__=='__main__':unittest.main()
