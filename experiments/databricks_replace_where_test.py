#!/usr/bin/env python3
"""Offline replacement history and complete-result sensitivity checks."""
from copy import deepcopy
from collections import Counter
import unittest
from databricks_replace_where import HISTORY, SCHEMA, assert_typed, replacement, assert_result, literal_reference, assert_reference, replace_submit, full_refresh_record, clearance_contract, ReplaceScope, assert_replace_sql
from databricks_survey import Refused


def result(rows):
    return {'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in SCHEMA],
            'rows':[list(r) for r in rows]}


class PredicateReplacementOracle(unittest.TestCase):
    def test_deleted_outside_source_row_remains_in_cumulative_result(self):
        case=HISTORY['outside']
        self.assertEqual(len(case.R),6)
        self.assertEqual(len(case.F),5)
        self.assertIn(('2','b-old','2026-11-02','2026-11-02T12:00:00.000Z'),case.R)
        self.assertFalse(any(r[0]=='2' for r in case.source))
        assert_typed(result(case.R),case.R)
        with self.assertRaisesRegex(Refused,'^physical:rows$'):
            assert_typed(result(case.F),case.R)

    def test_replacement_preserves_sql_unknown_and_duplicate_multiplicity(self):
        # A destination p=A comparison is UNKNOWN for NULL, so NULL is retained.
        predicate=lambda r: None if r[2] is None else r[2]=='2026-11-01'
        previous=HISTORY['first'].R
        actual=replacement(previous,(),predicate)
        self.assertEqual(Counter(map(tuple,actual)),Counter(map(tuple,(previous[2],previous[3],previous[4]))))
        self.assertEqual(replacement(HISTORY['empty'].R,(),lambda r:True),())

    def test_literal_oracle_rejects_independent_row_schema_and_property_faults(self):
        for stage,case in HISTORY.items():
            props={'rows':[['Type','MANAGED'],['Provider','delta']]}
            assert_result(result(case.R),props,case.R)
            if case.R:
                faults=[case.R[:-1],case.R+(case.R[0],)]
                if len(set(case.R))!=len(case.R):faults.append(tuple(set(case.R)))
                for rows in faults:
                    with self.subTest(stage=stage),self.assertRaisesRegex(Refused,'^physical:rows$'):
                        assert_typed(result(rows),case.R)
            for fault in ('order','type','position','clone'):
                actual=result(case.R);badprops=deepcopy(props)
                if fault=='order':actual['columns'].reverse()
                if fault=='type':actual['columns'][2]['type_name']='STRING'
                if fault=='position':actual['columns'][0]['position']=False
                if fault=='clone':badprops['rows'][0][1]='MANAGED_SHALLOW_CLONE'
                with self.subTest(stage=stage,fault=fault),self.assertRaisesRegex(Refused,'^physical:'):
                    assert_result(actual,badprops,case.R)
        with self.assertRaisesRegex(Refused,'^physical:rows$'):
            assert_typed(result(HISTORY['steady'].R),HISTORY['inside'].R)
        with self.assertRaisesRegex(Refused,'^physical:rows$'):
            assert_typed(result(HISTORY['empty'].R),HISTORY['predicate'].R)

    def test_empty_and_temporal_reference_are_exact_select_only(self):
        import json
        for stage,case in HISTORY.items():
            sql=literal_reference(case.R)
            stdout=json.dumps([dict(zip(('id','v','p','ts'),(int(r[0]),*r[1:]))) for r in case.R])
            assert_reference(stdout,'',result(case.R),case.R,sql,sql+';',sql)
            for fault in ('sql','rows','time','stderr'):
                bad=stdout;query=sql+';';stderr=''
                if fault=='sql':query+=' drop table evil;'
                if fault=='rows':bad='[{}]'
                if fault=='time':bad=stdout.replace('T12:00:00.123Z',' 12:00:00.123') if '.123Z' in stdout else '[{}]'
                if fault=='stderr':stderr='warning'
                with self.subTest(stage=stage,fault=fault),self.assertRaises(Refused):
                    assert_reference(bad,stderr,result(case.R),case.R,sql,query,sql)

    def test_wire_rejects_false_reuse_missing_epochs_and_wrong_predicate(self):
        request={'execution_type':11,'compare_unrendered_code':False,'table_namespace':'owned','query_dependencies':[],
          'semantic_extras':{'unique_key':'"id"','on_schema_change':'"ignore"','file_format':'"delta"','incremental_predicates':'["'+HISTORY['steady'].predicate+'"]'},
          'clone_chain_depth_limit':0,'ignore_external_modifications':False,'freshness_tolerance_seconds':0,'stale_upstream_policy':0,
          'tables':[{'name':'cdo_demo_db.owned.inputs','last_modified_epoch':10},{'name':'cdo_demo_db.owned.consumer','last_modified_epoch':20}],
          'target_table':'cdo_demo_db.owned.consumer','labels':{'dbt_node_name':'consumer'}}
        row={'method':'SubmitEnrichedSQL','request':request,'response':{'response':{'skip_execution':{}}}}
        replace_submit([row],'cdo_demo_db.owned.inputs','cdo_demo_db.owned.consumer',True,HISTORY['steady'].predicate,'skip_execution')
        for fault in ('missing','unknown','type','predicate','template','lag','confirm','false_reuse'):
            calls=[deepcopy(row)]
            if fault=='missing':calls[0]['request']['tables'].pop(0)
            if fault=='unknown':calls[0]['request']['tables'][0]['last_modified_epoch']=None
            if fault=='type':calls[0]['request']['execution_type']=2
            if fault=='predicate':calls[0]['request']['semantic_extras']['incremental_predicates']='[]'
            if fault=='template':calls[0]['request']['compare_unrendered_code']=True
            if fault=='lag':calls[0]['request']['freshness_tolerance_seconds']=1
            if fault=='confirm':calls.append({'method':'ConfirmExecution'})
            with self.subTest(fault=fault),self.assertRaises(Refused):
                replace_submit(calls,'cdo_demo_db.owned.inputs','cdo_demo_db.owned.consumer',True,HISTORY['steady'].predicate,'ready_to_execute' if fault=='false_reuse' else 'skip_execution')

    def test_scope_and_refresh_never_admit_prior_objects_or_failed_records(self):
        contract=clearance_contract();scope=ReplaceScope('20261114a148aa',contract)
        scope.relation('prod','inputs');scope.relation('prod','consumer')
        for n in contract['protected_permanent_names']+contract['permanent_exact_allowlists']['rust']:
            with self.assertRaises(Refused):scope.check(n)
        target=scope.relation('prod','consumer')
        calls=[{'method':'RecordExecutions','request':{'records':[{'input':{'enriched_sql':{'execution_type':1,'target_table':target}},'outcome':{'last_modified_epoch':1,'execution_runtime_ms':1}}]},'response':{'records_stored':1}}]
        full_refresh_record(calls,target)
        calls[0]['response']['records_stored']=0
        with self.assertRaises(Refused):full_refresh_record(calls,target)

    def test_native_sql_requires_destination_predicate_and_session_temp(self):
        target='cdo_demo_db.owned.consumer';temp='consumer__dbt_tmp';predicate=HISTORY['steady'].predicate
        sql='create or replace temporary view '+temp+' as select * from cdo_demo_db.owned.inputs; INSERT INTO '+target+' BY NAME REPLACE WHERE '+predicate+' TABLE '+temp+';'
        assert_replace_sql(sql,target,temp,predicate)
        for bad in (sql.replace('REPLACE WHERE '+predicate,'REPLACE WHERE p IS NULL'),sql.replace('BY NAME ',''),sql.replace('temporary view','table'),sql.replace(temp,'other'),sql+' DELETE FROM '+target+';'):
            with self.assertRaises(Refused):assert_replace_sql(bad,target,temp,predicate)

    def test_evidenced_whole_second_show_spelling_is_typed_zero_milliseconds(self):
        import json
        rows=(('1','a','2026-11-01','2026-11-01T12:00:00.000Z'),)
        sql=literal_reference(rows)
        stdout='[{"id":1,"v":"a","p":"2026-11-01","ts":"2026-11-01T12:00:00Z"}]'
        checks=assert_reference(stdout,'',result(rows),rows,sql,sql+';',sql)
        self.assertEqual(checks['observed_show_whole_second_rows'],1)
        for ts in ('2026-11-01 12:00:00','2026-11-01T12:00:00+00:00','2026-11-01T12:00:00.000000Z'):
            bad=json.dumps([{'id':1,'v':'a','p':'2026-11-01','ts':ts}])
            with self.assertRaises(Refused):assert_reference(bad,'',result(rows),rows,sql,sql+';',sql)


if __name__=='__main__':unittest.main()
