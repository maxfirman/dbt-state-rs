#!/usr/bin/env python3
"""Two owned transport stages: before model SQL and after SQL/before Confirm.

No payload/epoch edits. One fresh source/consumer pair serves both stages in
one campaign lifetime. Outcomes after races are observed, not preassigned.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json
from pathlib import Path
import re

from databricks_freshness_race import RaceFreshness
from databricks_freshness import assert_rows,decisions
from databricks_readonly_reference import show_reference
from databricks_survey import Refused,same_result


class CombinedRace(RaceFreshness):
    entrypoint=Path(__file__)

    def source_reference(self,label,amount):
        direct=self.sql(label+'_transform',self.direct)
        rows=[['1',str(amount),self.tag],['1',str(amount),self.tag],['2',None,None]]
        assert_rows(direct,rows)
        show_reference(self,label,self.direct,[[1,amount,self.tag],[1,amount,self.tag],[2,None,None]],direct)
        self.save('source-reference/'+label+'.json',direct)
        return direct

    def require_absent_consumer(self,label):
        inventory=self.inventory();target=self.scope.relation('prod','consumer')
        self.save('gate/'+label+'_inventory.json',inventory)
        if any(r['full_name']==target for objects in inventory.values() for r in objects):
            raise Refused('pre-execution consumer already exists')

    def baseline(self):
        self.setup_project();self.observations=[]
        source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
        self.direct=f'SELECT id, amount, tag FROM {source}';self.tag='combined_'+self.query_identity
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'freshness','database':'cdo_demo_db',
                  'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.write_model('consumer',"materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}",
                         "select id, amount, tag from {{ source('freshness','inputs') }}")
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('initial_source',f"INSERT INTO {source} VALUES (1,10,'{self.tag}'),(1,10,'{self.tag}'),(2,NULL,NULL)")
        self.gate.response_armed=True
        pool=ThreadPoolExecutor(max_workers=1)
        future=pool.submit(self.dbt,'before_sql_first',['run','--select','consumer'])
        try:
            if not self.gate.response_arrived.wait(120):raise Refused('owned ready response did not reach gate')
            self.require_absent_consumer('before_sql_old')
            self.source_reference('before_sql_old',10);self.sample('before_sql_old')
            self.sql('before_sql_update',f'UPDATE {source} SET amount=17 WHERE id=1')
            self.source_reference('before_sql_new',17);self.sample('before_sql_new')
            self.require_absent_consumer('before_sql_new')
            self.event('before-sql-write-complete',source_amount=17,consumer_absent=True)
        finally:
            self.gate.response_release.set()
            try:calls=future.result(timeout=120)
            finally:pool.shutdown(wait=True)
        forwarded=json.loads((self.work/'gate-transport/response-forwarded.json').read_text())
        if forwarded['controller_released'] is not True:raise Refused('ready response gate timed out')
        submits=decisions(calls,'ready_to_execute');self.explain('before_sql_first',submits)
        rows17=[['1','17',self.tag],['1','17',self.tag],['2',None,None]]
        self.checked_results('before_sql_first',rows17,self.direct)
        # Retain positive model-DDL timing evidence rather than inferring the
        # execution point solely from a sleep or the transport hook's name.
        query=(self.work/'logs/001_before_sql_first/query_log.sql').read_text()
        create_times=[]
        for block in re.split(r'(?m)^-- created_at: ',query)[1:]:
            if re.search(r'(?im)^\s*create or replace table\s+',block):
                if target not in block.replace('`',''):raise Refused('unexpected model DDL target')
                create_times.append(datetime.fromisoformat(block.splitlines()[0]))
        released=datetime.fromisoformat(forwarded['at'])
        if len(create_times)!=1 or create_times[0]<released:raise Refused('model DDL not proven after response release')
        self.save('gate/before-sql-order.json',{'response_forwarded_at':forwarded['at'],
                  'model_ddl_started_at':create_times[0].isoformat(),'consumer_absent_during_update':True,
                  'limitation':'No claim about a mutation inside warehouse SQL execution.'})
        pre=self.invocation('before_sql_followup',17)
        self.invocation('before_sql_unchanged',17,'skip_execution')
        # Still the same owned campaign lifetime. Reset only this fresh source
        # to force the second stage; no previous campaign relation is touched.
        self.sql('prepare_confirm_source',f'UPDATE {source} SET amount=20 WHERE id=1')
        self.gate.armed=True
        pool=ThreadPoolExecutor(max_workers=1)
        future=pool.submit(self.dbt,'after_sql_build',['run','--select','consumer'])
        rows20=[['1','20',self.tag],['1','20',self.tag],['2',None,None]]
        try:
            if not self.gate.arrived.wait(120):raise Refused('owned Confirm did not reach gate')
            self.checked_results('after_sql_old',rows20,self.direct);self.sample('after_sql_old')
            self.sql('after_sql_update',f'UPDATE {source} SET amount=23 WHERE id=1')
            self.source_reference('after_sql_new',23);self.sample('after_sql_new')
            target_old=self.observe('after_sql_new_target',target);assert_rows(target_old,rows20)
            earlier=json.loads((self.work/'physical/after_sql_old.json').read_text())
            if not same_result(target_old,earlier):raise Refused('held target differs from earlier independent snapshot')
            self.event('after-sql-write-complete',source_amount=23,target_amount=20)
        finally:
            self.gate.release.set()
            try:calls=future.result(timeout=120)
            finally:pool.shutdown(wait=True)
        forwarded=json.loads((self.work/'gate-transport/forwarded.json').read_text())
        if forwarded['controller_released'] is not True:raise Refused('Confirm gate timed out')
        submits=decisions(calls,'ready_to_execute');self.explain('after_sql_build',submits)
        assert_rows(self.observe('after_sql_confirmed',target),rows20)
        post=self.invocation('after_sql_followup',23)
        self.invocation('after_sql_unchanged',23,'skip_execution')
        self.save('reachability.json',{'actual_write_before_model_sql':True,'actual_write_after_sql_before_confirm':True,
                  'response_and_confirm_bytes_unchanged':True,'within_sql_snapshot_interleaving':'unverified',
                  'before_sql_followup_decision':pre['observed'],'after_sql_followup_decision':post['observed'],
                  'required_unchanged_reuses':2,'pairs':1,'managed_invocations':6})
        return {'actual_managed_calls':6,'response_holds':1,'confirm_holds':1}


def main():
    import argparse,os
    from datetime import datetime,timezone
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease'):p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91');p.add_argument('--query-identity');p.add_argument('--rust-url')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+os.urandom(3).hex())
    return CombinedRace(p.parse_args()).run()

if __name__=='__main__':raise SystemExit(main())
