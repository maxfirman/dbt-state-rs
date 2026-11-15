#!/usr/bin/env python3
"""Offline probes for independent APPEND history and physical comparison."""
from copy import deepcopy
import unittest
from databricks_append import APPEND_HISTORY, assert_append_result, literal_reference, append_submit, AppendScope
from databricks_survey import Refused


def result(rows):
    return {'columns':[{'name':n,'type_text':t,'type_name':k,'position':i}
        for i,(n,t,k) in enumerate([('id','BIGINT','LONG'),('amount','INT','INT'),('tag','STRING','STRING')])],
        'rows':deepcopy(rows)}


PROPERTIES={'rows':[['Type','MANAGED'],['Provider','delta']]}


class AppendOracle(unittest.TestCase):
    def test_source_changes_append_without_updating_or_removing_previous_rows(self):
        case=APPEND_HISTORY['delete']
        self.assertEqual(len(case['R']),15)
        self.assertEqual(case['R'].count(['2','20','old']),2)
        self.assertEqual(case['R'].count(['1','10','same']),2)
        self.assertEqual(case['R'].count(['1','11','changed']),4)
        self.assertEqual(case['R'].count(['4','40','new']),4)
        assert_append_result(result(case['R']),PROPERTIES,case['R'])
        with self.assertRaisesRegex(Refused,'^physical:rows$'):
            assert_append_result(result(case['F']),PROPERTIES,case['R'])

    def test_repeat_rejects_deduplication_null_coercion_and_refresh_substitution(self):
        for stage in ('first','upsert','delete','projection','refresh','next_ordinary'):
            expected=APPEND_HISTORY[stage]['R']
            assert_append_result(result(expected),PROPERTIES,expected)
            for fault in ('deduplicated','null-string','schema','position','clone'):
                actual=result(expected);props=deepcopy(PROPERTIES)
                if fault=='deduplicated':actual['rows']=[list(r) for r in set(map(tuple,expected))]
                if fault=='null-string':actual['rows']=[['NULL' if c is None else c for c in r] for r in expected]
                if fault=='schema':actual['columns'][1]['type_text']='BIGINT'
                if fault=='position':actual['columns'][1]['position']=0
                if fault=='clone':props['rows'][0][1]='MANAGED_SHALLOW_CLONE'
                with self.subTest(stage=stage,fault=fault),self.assertRaisesRegex(Refused,'^physical:'):
                    assert_append_result(actual,props,expected)
        with self.assertRaisesRegex(Refused,'^physical:rows$'):
            assert_append_result(result(APPEND_HISTORY['refresh']['R']),PROPERTIES,APPEND_HISTORY['next_ordinary']['R'])

    def test_exact_allocation_scope_never_admits_prior_or_reference_objects(self):
        clearance={'schema':'state_20261114_append146',
          'exact_permanent_names':['a_20261114a146aa_inputs','a_20261114a146aa_consumer','a_20261114b146bb_inputs','a_20261114b146bb_consumer'],
          'auxiliary_allowlist':{'a_20261114a146aa_consumer':['a_20261114a146aa_consumer__dbt_stg','a_20261114a146aa_consumer__dbt_backup']},
          'session_only_unqualified_temp_names':['a_20261114a146aa_consumer__dbt_tmp']}
        scope=AppendScope('20261114a146aa',clearance)
        self.assertEqual(len(scope.permanent),2)
        scope.relation('prod','inputs');scope.relation('prod','consumer')
        for wrong in ('cdo_demo_db.ds_20261112a134ef_ref.a_20261114a146aa_inputs',
                      'cdo_demo_db.state_20261114_append146.a_20261114b146bb_inputs',
                      'cdo_demo_db.state_20261114_append146.a_20261114a146aa_consumer__dbt_tmp'):
            with self.subTest(name=wrong),self.assertRaises(Refused):scope.check(wrong)

    def test_actual_append_wire_requires_known_complete_evidence_and_positive_reuse(self):
        request={'execution_type':2,'compare_unrendered_code':False,'table_namespace':'owned',
          'query_dependencies':[],'semantic_extras':{'unique_key':'"id"','on_schema_change':'"ignore"'},
          'clone_chain_depth_limit':0,'ignore_external_modifications':False,
          'tables':[{'name':'cdo_demo_db.owned.inputs','last_modified_epoch':10},
                    {'name':'cdo_demo_db.owned.consumer','last_modified_epoch':20}],
          'target_table':'cdo_demo_db.owned.consumer','labels':{'dbt_node_name':'consumer'},
          'freshness_tolerance_seconds':0,'stale_upstream_policy':0}
        row={'method':'SubmitEnrichedSQL','request':request,'response':{'response':{'skip_execution':{}}}}
        append_submit([row],'cdo_demo_db.owned.inputs','cdo_demo_db.owned.consumer',True,'skip_execution')
        for fault in ('missing','unknown','merge','template','lag','confirm','record'):
            calls=[deepcopy(row)]
            if fault=='missing':calls[0]['request']['tables'].pop(0)
            if fault=='unknown':calls[0]['request']['tables'][0]['last_modified_epoch']=None
            if fault=='merge':calls[0]['request']['execution_type']=3
            if fault=='template':calls[0]['request']['compare_unrendered_code']=True
            if fault=='lag':calls[0]['request']['freshness_tolerance_seconds']=100
            if fault=='confirm':calls.append({'method':'ConfirmExecution','request':{'request_id':'x'},'response':{'success':True}})
            if fault=='record':calls.append({'method':'RecordExecutions'})
            with self.subTest(fault=fault),self.assertRaises(Refused):
                append_submit(calls,'cdo_demo_db.owned.inputs','cdo_demo_db.owned.consumer',True,'skip_execution')


if __name__=='__main__':unittest.main()
