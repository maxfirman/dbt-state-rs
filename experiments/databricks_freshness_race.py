#!/usr/bin/env python3
"""One actual source update while the first owned dbt Confirm is held.

The consumer SQL has completed when Confirm reaches this gate. This proves a
write in the Submit→Confirm interval, not before query execution or stale reads.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from databricks_confirm_gate import CONFIRM, UnaryGate
from databricks_freshness_actual import ActualFreshness
from databricks_freshness import assert_rows, decisions
from databricks_readonly_reference import show_reference
from databricks_survey import Refused, same_result

SUBMIT='/com.fivetran.query_cache.SQL/SubmitEnrichedSQL'


class RaceFreshness(ActualFreshness):
    entrypoint=Path(__file__)

    def start(self):
        super().start()
        output=self.work/'generated-proto';output.mkdir()
        source=Path(__file__).resolve().parents[1]/'proto'
        files=sorted(source.rglob('*.proto'))
        subprocess.run(['protoc','-I',str(source),'--python_out',str(output),*[str(p) for p in files]],check=True,capture_output=True)
        self.save('gate/proto-source.json',{str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
        sys.path.insert(0,str(output))
        from query_cache_protobuf.query_cache.services.execution_service_pb2 import ConfirmExecutionRequest
        from query_cache_protobuf.query_cache.services.sql_service_pb2 import SubmitEnrichedSQLRequest, SubmitSQLResponse
        self.owned_ids=set()
        target=self.scope.relation('prod','consumer');upstream=self.scope.relation('prod','inputs')
        def ownership(method,request,response):
            if method==SUBMIT and response is not None:
                submit=SubmitEnrichedSQLRequest.FromString(request)
                sources=[r.name.replace('`','') for r in submit.tables if r.name!=submit.target_table]
                if submit.target_table.replace('`','')==target and sources==[upstream]:
                    result=SubmitSQLResponse.FromString(response)
                    if result.HasField('ready_to_execute'):
                        self.owned_ids.add(result.ready_to_execute.request_id)
                        (self.work/'gate'/'submit.request.pb').write_bytes(request)
                        (self.work/'gate'/'submit.response.pb').write_bytes(response)
                        return True
            if method==CONFIRM:
                return ConfirmExecutionRequest.FromString(request).request_id in self.owned_ids
            return False
        self.gate=UnaryGate(self.env['RUN_CACHE_API_URL'],self.work/'gate-transport',ownership,timeout=45)
        self.env['RUN_CACHE_API_URL']=self.gate.endpoint
        self.save('gate/ownership.json',{'target':target,'upstream':upstream,'gate_timeout_seconds':45,
                   'payload_changes':False,'only_confirm_for_observed_owned_submit':True})

    def stop(self):
        if hasattr(self,'gate'):self.gate.close()
        super().stop()

    def baseline(self):
        self.setup_project();self.observations=[]
        source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
        self.direct=f'SELECT id, amount, tag FROM {source}';self.tag='race_'+self.query_identity
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'freshness','database':'cdo_demo_db',
                  'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.write_model('consumer',"materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}",
                         "select id, amount, tag from {{ source('freshness','inputs') }}")
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('initial_source',f"INSERT INTO {source} VALUES (1,10,'{self.tag}'),(1,10,'{self.tag}'),(2,NULL,NULL)")
        old=[['1','10',self.tag],['1','10',self.tag],['2',None,None]]
        self.gate.armed=True
        pool=ThreadPoolExecutor(max_workers=1)
        future=pool.submit(self.dbt,'held_first',['run','--select','consumer'])
        try:
            if not self.gate.arrived.wait(120):raise Refused('owned Confirm did not reach gate')
            # Output already exists; preserve full independent reference before
            # changing the input. A later result is judged against this snapshot.
            self.checked_results('held_before_update',old,self.direct)
            self.sample('held_before_update')
            self.sql('held_source_update',f'UPDATE {source} SET amount=17 WHERE id=1')
            self.sample('held_after_update')
            direct=self.sql('held_after_update_transform',self.direct)
            new=[['1','17',self.tag],['1','17',self.tag],['2',None,None]]
            assert_rows(direct,new)
            show_reference(self,'held_after_update',self.direct,[[1,17,self.tag],[1,17,self.tag],[2,None,None]],direct)
            retained=self.observe('held_after_update_target',target);assert_rows(retained,old)
            snapshot=json.loads((self.work/'physical/held_before_update.json').read_text())
            if not same_result(retained,snapshot):raise Refused('held output differs from recorded reference')
            self.save('gate/write-completed.json',{'source_rows':direct,'target_rows':retained,
                       'timing':'mutation completed before forwarding original Confirm',
                       'limitation':'output SQL already completed; no claim of mutation before warehouse execution'})
        finally:
            self.gate.release.set()
            try:calls=future.result(timeout=120)
            finally:pool.shutdown(wait=True)
        forwarded=json.loads((self.work/'gate-transport/forwarded.json').read_text())
        if forwarded['controller_released'] is not True:raise Refused('gate timed out before controlled release')
        submits=decisions(calls,'ready_to_execute');self.explain('held_first',submits)
        confirmed=self.observe('after_held_confirm',target);assert_rows(confirmed,old)
        self.invocation('after_race',17,'ready_to_execute')
        self.invocation('after_race_unchanged',17,'skip_execution')
        self.save('reachability.json',{'actual_update_between_submit_and_confirm':True,
                  'before_warehouse_execution':False,'pre_execution_stale_read_interleaving':'not established',
                  'confirm_bytes_unchanged':True,'gate_count':1,'source_updates_while_held':1,
                  'required_builds':2,'required_reuses':1})
        return {'actual_managed_calls':3,'held_confirmations':1,'actual_source_updates':1}


def main():
    import argparse,os
    from datetime import datetime,timezone
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease'):p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91');p.add_argument('--query-identity');p.add_argument('--rust-url')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+os.urandom(3).hex())
    return RaceFreshness(p.parse_args()).run()

if __name__=='__main__':raise SystemExit(main())
