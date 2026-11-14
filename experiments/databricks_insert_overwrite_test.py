#!/usr/bin/env python3
"""Offline independent operation-history reference for #147."""
import unittest
from copy import deepcopy
from databricks_insert_overwrite import replace_partitions, assert_partition_result, literal_reference, SCHEMA


class PartitionHistory(unittest.TestCase):
    def test_replacement_removes_entire_represented_partition_and_preserves_duplicates(self):
        old=((1,'old','A'),(2,'delete','A'),(1,'other','B'),(9,None,None))
        fresh=((1,'changed','A'),(3,None,'A'),(3,None,'A'))
        expected=((1,'other','B'),(9,None,None),(1,'changed','A'),(3,None,'A'),(3,None,'A'))
        self.assertCountEqual(replace_partitions(old,fresh,(2,)),expected)
        self.assertEqual(old[0],(1,'old','A'))

    def test_null_partition_replaces_null_and_empty_selection_preserves_every_partition(self):
        old=((1,'old',None),(2,'A','A'),(3,'old',None))
        self.assertCountEqual(replace_partitions(old,((4,'new',None),),(2,)),
                              ((2,'A','A'),(4,'new',None)))
        self.assertEqual(replace_partitions(old,(),(2,)),old)

    def test_composite_partition_does_not_replace_a_partially_matching_tuple(self):
        old=((1,None,'A'),(2,None,'B'),(3,'x','A'))
        self.assertCountEqual(replace_partitions(old,((4,None,'A'),),(1,2)),
                              ((2,None,'B'),(3,'x','A'),(4,None,'A')))

    def test_full_typed_result_rejects_lost_duplicate_null_type_and_partition_metadata(self):
        rows=[['1',None,None,'2026-11-01T12:00:00.000Z']]*2
        actual={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in SCHEMA], 'rows':rows}
        props={'rows':[['Type','MANAGED'],['Provider','delta'],['# Partition Information','',''],['# col_name','data_type','comment'],['p','date',None],['','','']]}
        assert_partition_result(actual,props,rows)
        from databricks_survey import Refused
        for fault in ('duplicate','null','schema','position','partition','provider'):
            value=deepcopy(actual);properties=deepcopy(props)
            if fault=='duplicate':value['rows'].pop()
            if fault=='null':value['rows'][0][1]=''
            if fault=='schema':value['columns'][2]['type_text']='STRING'
            if fault=='position':value['columns'][1]['position']=0
            if fault=='partition':properties['rows'][4][0]='v'
            if fault=='provider':properties['rows'][1][1]='parquet'
            with self.subTest(fault=fault),self.assertRaises(Refused):
                assert_partition_result(value,properties,rows)

    def test_disabled_reference_preserves_dates_timestamps_duplicates_and_nulls(self):
        from databricks_insert_overwrite import assert_reference
        from databricks_survey import Refused
        import json
        rows=[['1',None,None,'2026-11-01T12:00:00.000Z']]*2
        direct={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in SCHEMA], 'rows':rows}
        shown=[{'id':1,'v':None,'p':None,'ts':'2026-11-01T12:00:00.000Z'}]*2
        sql=literal_reference(rows)
        assert_reference(json.dumps(shown),'',direct,rows,sql,sql+';',sql)
        for fault in ('duplicate','timestamp','date','type','extra-sql'):
            value=deepcopy(shown);query=sql+';'
            if fault=='duplicate':value.pop()
            if fault=='timestamp':value[0]['ts']='2026-11-01T12:00:00.001Z'
            if fault=='date':value[0]['p']='2026-11-01'
            if fault=='type':value[0]['id']=True
            if fault=='extra-sql':query+=' DROP TABLE protected;'
            with self.subTest(fault=fault), self.assertRaises(Refused):
                assert_reference(json.dumps(value),'',direct,rows,sql,query,sql)

    def test_scope_refuses_other_runs_old_objects_auxiliary_and_peak_expansion(self):
        from databricks_insert_overwrite import OverwriteScope, clearance_contract
        from databricks_survey import Refused
        grant=clearance_contract()
        scope=OverwriteScope('20261114a147aa',grant)
        scope.check('cdo_demo_db.state_20261114_append146.io_20261114a147aa_inputs')
        for name in ('a_20261114a146aa_inputs','io_20261114b147bb_consumer','inc_insert_overwrite','io_20261114a147aa_consumer__dbt_tmp'):
            with self.subTest(name=name),self.assertRaises(Refused):
                scope.check('cdo_demo_db.state_20261114_append146.'+name)
        for field in ('max_extra_concurrent_persistent','max_final_permanent'):
            bad=deepcopy(grant);bad[field]+=1
            with self.subTest(field=field),self.assertRaises(Refused):OverwriteScope('20261114a147aa',bad)

    def test_native_partition_path_requires_exact_null_safe_replace_on(self):
        from databricks_insert_overwrite import assert_replace_on_sql
        from databricks_survey import Refused
        target='cdo_demo_db.state_20261114_append146.io_20261114a147aa_consumer'
        temp='io_20261114a147aa_consumer__dbt_tmp'
        sql=f'insert into table {target} AS t replace on (t.p <=> s.p) (select id,v,p,ts from {temp}) AS s;'
        assert_replace_on_sql(sql,target,temp)
        for bad in (sql.replace('<=>','='),sql.replace('t.p','t.id'),sql.replace(temp,'other'),sql.replace('replace on','overwrite'),sql+' SET x=1;'):
            with self.subTest(sql=bad),self.assertRaises(Refused):assert_replace_on_sql(bad,target,temp)

    def test_refresh_requires_actual_full_record_and_acknowledged_outcome(self):
        from databricks_insert_overwrite import full_refresh_record
        from databricks_survey import Refused
        target='cdo_demo_db.owned.consumer'
        record={'input':{'enriched_sql':{'execution_type':1,'target_table':target}},
                'outcome':{'last_modified_epoch':100,'execution_runtime_ms':25}}
        calls=[{'method':'RecordExecutions','request':{'records':[record]},'response':{'records_stored':1}}]
        self.assertEqual(full_refresh_record(calls,target),record)
        for fault in ('unacked','bool-ack','duplicate','ordinary','unknown','runtime','submit'):
            value=deepcopy(calls)
            if fault=='unacked':value[0]['response']['records_stored']=0
            if fault=='bool-ack':value[0]['response']['records_stored']=True
            if fault=='duplicate':value[0]['request']['records'].append(deepcopy(record))
            if fault=='ordinary':value[0]['request']['records'][0]['input']['enriched_sql']['execution_type']=4
            if fault=='unknown':value[0]['request']['records'][0]['outcome']['last_modified_epoch']=None
            if fault=='runtime':value[0]['request']['records'][0]['outcome']['execution_runtime_ms']=False
            if fault=='submit':value.append({'method':'SubmitEnrichedSQL'})
            with self.subTest(fault=fault),self.assertRaises(Refused):full_refresh_record(value,target)

    def test_actual_wire_rejects_incomplete_evidence_other_strategy_and_undeclared_policy(self):
        from databricks_insert_overwrite import overwrite_submit
        from databricks_survey import Refused
        source='cdo_demo_db.owned.inputs';target='cdo_demo_db.owned.consumer'
        request={'dialect':'databricks','execution_type':4,'compare_unrendered_code':False,'table_namespace':'owned',
          'query_dependencies':[],'semantic_extras':{'unique_key':'"id"','partition_by':'["p"]','file_format':'"delta"','on_schema_change':'"ignore"'},
          'clone_chain_depth_limit':0,'ignore_external_modifications':False,
          'tables':[{'name':source,'last_modified_epoch':10},{'name':target,'last_modified_epoch':20}],
          'target_table':target,'labels':{'dbt_node_name':'consumer'},'freshness_tolerance_seconds':0,'stale_upstream_policy':0}
        row={'method':'SubmitEnrichedSQL','request':request,'response':{'response':{'skip_execution':{}}}}
        overwrite_submit([row],source,target,True,'skip_execution')
        for fault in ('missing','unknown','bool-epoch','merge','template','lag','confirm','record','partition','dialect','omitted-external-policy'):
            calls=[deepcopy(row)];r=calls[0]['request']
            if fault=='missing':r['tables'].pop(0)
            if fault=='unknown':r['tables'][0]['last_modified_epoch']=None
            if fault=='bool-epoch':r['tables'][0]['last_modified_epoch']=True
            if fault=='merge':r['execution_type']=3
            if fault=='template':r['compare_unrendered_code']=True
            if fault=='lag':r['freshness_tolerance_seconds']=100
            if fault=='confirm':calls.append({'method':'ConfirmExecution','request':{'request_id':'x'},'response':{'success':True}})
            if fault=='record':calls.append({'method':'RecordExecutions'})
            if fault=='partition':r['semantic_extras']['partition_by']='["id"]'
            if fault=='dialect':r['dialect']='snowflake'
            if fault=='omitted-external-policy':del r['ignore_external_modifications']
            with self.subTest(fault=fault),self.assertRaises(Refused):overwrite_submit(calls,source,target,True,'skip_execution')

    def test_observed_pinned_second_timestamp_codec_is_lossless_and_narrow(self):
        from databricks_insert_overwrite import assert_reference
        from databricks_survey import Refused
        import json
        expected=[['1','old','2026-11-01','2026-11-01T12:00:00.000Z']]*2
        direct={'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in SCHEMA], 'rows':expected}
        shown=[{'id':1,'v':'old','p':'2026-11-01','ts':'2026-11-01T12:00:00Z'}]*2
        sql=literal_reference(expected)
        assert_reference(json.dumps(shown),'',direct,expected,sql,sql+';',sql)
        for changed in ('2026-11-01T12:00:00.001Z','2026-11-01T12:00:00+00:00','2026-11-01 12:00:00','2026-11-01T12:00:00.000001Z'):
            wrong=deepcopy(shown);wrong[0]['ts']=changed
            with self.subTest(timestamp=changed),self.assertRaises(Refused):
                assert_reference(json.dumps(wrong),'',direct,expected,sql,sql+';',sql)

    def test_native_footprint_auditor_understands_table_keyword_and_rejects_other_target(self):
        from databricks_insert_overwrite import Overwrite,OverwriteScope,clearance_contract
        from databricks_survey import Refused
        from pathlib import Path
        import tempfile,json
        with tempfile.TemporaryDirectory() as directory:
            campaign=object.__new__(Overwrite)
            campaign.work=Path(directory);campaign.scope=OverwriteScope('20261114a147aa',clearance_contract())
            campaign.n=4;campaign.secrets=[]
            log=campaign.work/'logs/004_upsert/query_log.sql';log.parent.mkdir(parents=True)
            target=campaign.scope.relation('prod','consumer');temp=campaign.scope.temporary
            raw=f'create or replace temporary view `{temp}` as select 1; insert into table {target} AS t replace on (t.p <=> s.p) (select id,v,p,ts from `{temp}`) AS s;'
            log.write_text(raw);campaign.audit_native('upsert',True)
            receipt=json.loads((campaign.work/'materialization/upsert.json').read_text())
            self.assertEqual(receipt['peak_extra_persistent_beyond_target'],0)
            log.write_text(raw.replace('insert into table '+target,'insert into table cdo_demo_db.state_20261114_append146.a_20261114a146aa_consumer'))
            with self.assertRaises(Refused):campaign.audit_native('upsert',True)

    def test_pinned_client_warn_reused_and_refresh_statuses_are_distinct(self):
        from databricks_insert_overwrite import assert_client_status
        from databricks_survey import Refused
        uid='model.survey_20261114a147aa.consumer';target='cdo_demo_db.owned.consumer'
        for decision,status,refresh in [('ready_to_execute','warn',False),('skip_execution','reused',False),(None,'success',True)]:
            adapter={} if status=='reused' else {'code':'SUCCESS','query_id':'actual-native-query'}
            result={'results':[{'unique_id':uid,'relation_name':target,'status':status,'adapter_response':adapter,'failures':None}]}
            assert_client_status(result,uid,target,decision,refresh)
            wrong=deepcopy(result);wrong['results'][0]['status']='success' if status!='success' else 'warn'
            with self.subTest(status=status),self.assertRaises(Refused):assert_client_status(wrong,uid,target,decision,refresh)
        failed={'results':[{'unique_id':uid,'relation_name':target,'status':'warn','adapter_response':{'code':'ERROR'},'failures':None}]}
        with self.assertRaises(Refused):assert_client_status(failed,uid,target,'ready_to_execute',False)

    def test_literal_reference_is_typed_and_empty_reference_is_still_select_only(self):
        sql=literal_reference([['1',"a'b",None,'2026-11-01T12:00:00.000Z']])
        self.assertIn("'a''b'",sql)
        self.assertIn('cast(null as date)',sql)
        self.assertIn('cast(ts as timestamp)',sql)
        empty=literal_reference([])
        self.assertTrue(empty.startswith('select '))
        self.assertIn('where false',empty)
        self.assertNotIn(';',empty)


if __name__=='__main__':unittest.main()
