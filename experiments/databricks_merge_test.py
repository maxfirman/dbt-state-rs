#!/usr/bin/env python3
"""Offline fault probes at the complete physical-result comparison boundary."""
from copy import deepcopy
import unittest
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from databricks_merge import SNAPSHOTS, Merge, MergeScope, assert_merge_result, literal_reference
from databricks_survey import Refused


def result(stage):
    return {'columns':[{'name':n,'type_text':t,'type_name':k,'position':i}
                       for i,(n,t,k) in enumerate([('id','BIGINT','LONG'),('amount','INT','INT'),('tag','STRING','STRING')])],
            'rows':deepcopy(SNAPSHOTS[stage])}


PROPERTIES = {'rows':[['Type','MANAGED'],['Provider','delta']]}


class MergeOracle(unittest.TestCase):
    def test_lease_allows_only_fresh_phase_pair_and_no_reference_writes(self):
        with TemporaryDirectory() as directory:
            evidence=Path(directory)/'owned.json'
            roles={'prod':'ds_20261112a134ef_ref','ref':'ds_20261112e134ef_ref'}
            evidence.write_text(json.dumps({'retained_schemas':list(roles.values())}))
            ledger={'catalog':'cdo_demo_db','roles':roles,'source_evidence':[{
                'path':str(evidence),'sha256':hashlib.sha256(evidence.read_bytes()).hexdigest()}]}
            scope=MergeScope('20261114abcdef',ledger)
            self.assertEqual(len(scope.permanent),2)
            scope.relation('prod','inputs');scope.relation('prod','consumer')
            for role,name in [('ref','inputs'),('ref','consumer'),('prod','inc_merge'),('prod','producer')]:
                with self.subTest(role=role,name=name),self.assertRaises(Refused):
                    scope.relation(role,name)

    def test_native_safer_replacement_counts_renames_without_touching_prior_objects(self):
        from types import SimpleNamespace
        with TemporaryDirectory() as directory:
            c=object.__new__(Merge);c.work=Path(directory);c.n=2
            target='cdo_demo_db.owned.m_run_consumer'
            c.scope=SimpleNamespace(temporary='m_run_consumer__dbt_tmp',
                auxiliary={target+'__dbt_stg',target+'__dbt_backup'},relation=lambda *_:target)
            c.save=lambda name,value:None
            p=c.work/'logs'/'002_refresh'/'query_log.sql';p.parent.mkdir(parents=True)
            native=(f'create or replace temporary view `m_run_consumer__dbt_tmp` as select 1;'
                    f'create table {target}__dbt_stg as select 1;'
                    f'alter table {target} rename to {target}__dbt_backup;'
                    f'alter table {target}__dbt_stg rename to {target};'
                    f'drop table {target}__dbt_backup;')
            p.write_text(native);c.audit_native('refresh',True)
            for wrong in (native.replace('temporary view','view'),
                          native.replace(target+'__dbt_stg','cdo_demo_db.owned.unapproved')):
                p.write_text(wrong)
                with self.assertRaisesRegex(Refused,'unapproved persistent native CREATE'):
                    c.audit_native('refresh',True)

    def test_deleted_source_row_survives_merge_but_not_refresh(self):
        assert_merge_result(result('delete'), PROPERTIES, SNAPSHOTS['delete'])
        for wrong_stage, expected_stage in [('refresh','delete'),('delete','refresh')]:
            with self.subTest(wrong_stage=wrong_stage), self.assertRaisesRegex(Refused,'^physical:rows$'):
                assert_merge_result(result(wrong_stage), PROPERTIES, SNAPSHOTS[expected_stage])

    def test_projection_preserves_deleted_row_prior_value(self):
        actual=result('projection')
        assert_merge_result(actual,PROPERTIES,SNAPSHOTS['projection'])
        actual['rows'][1][1]='120'
        with self.assertRaisesRegex(Refused,'^physical:rows$'):
            assert_merge_result(actual,PROPERTIES,SNAPSHOTS['projection'])

    def test_lost_duplicate_values_null_schema_and_clone_type_fail(self):
        for fault in ('lost-row','null','schema','clone','provider'):
            actual=result('first');props=deepcopy(PROPERTIES)
            if fault=='lost-row':actual['rows'].pop(2)
            if fault=='null':actual['rows'][-1][-1]=''
            if fault=='schema':actual['columns'][1]['type_text']='BIGINT'
            if fault=='clone':props['rows'][0][1]='MANAGED_SHALLOW_CLONE'
            if fault=='provider':props['rows'][1][1]='parquet'
            with self.subTest(fault=fault), self.assertRaisesRegex(Refused,'^physical:'):
                assert_merge_result(actual,props,SNAPSHOTS['first'])

    def test_literal_reference_contains_history_and_explicit_types(self):
        sql=literal_reference(SNAPSHOTS['delete'])
        self.assertIn("(2,20,'old')",sql)
        self.assertIn('cast(id as bigint)',sql)
        self.assertIn('cast(null as int)',sql)
        self.assertNotIn('FROM cdo_demo_db',sql)


if __name__=='__main__':unittest.main()
