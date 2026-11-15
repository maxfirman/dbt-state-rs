#!/usr/bin/env python3
"""Opt-in #158 actual dbt failures/retries and public execution publication.

Capture hosted first; exact replay must pass before running --rust-url. Protocol
controls are explicitly crafted and do not establish client reachability. All
warehouse writes use the exact fresh relation allowlist in an ownership lease.
"""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

from databricks_baseline import Baseline, LeaseScope, query, assert_physical
from databricks_survey import Campaign, Refused, Scope, same_result


class LifecycleScope(LeaseScope):
    def __init__(self, run, ledger):
        super().__init__(run, ledger)
        self.relation_prefix = 'l_' + run + '_'
        self.allowed = {f'cdo_demo_db.{schema}.{self.relation_prefix}{name}'
                        for schema in self.schemas for name in ('recovered','recorded','delayed')}


class Lifecycle(Baseline):
    def __init__(self, args):
        if not args.schema_lease:
            raise Refused('lifecycle requires an explicit ownership lease')
        self.lease = json.loads(Path(args.schema_lease).read_text())
        self.query_identity = getattr(args, 'query_identity', None) or args.run_id
        Scope(self.query_identity)
        Campaign.__init__(self, args, LifecycleScope(args.run_id,self.lease))
        self.save('lifecycle-runner.py',Path(__file__).read_text())
        self.save('binary-digests.json',{name:hashlib.sha256(Path(getattr(args,name)).read_bytes()).hexdigest()
                                       for name in ('dbt','proxy','probe')})

    def rpc(self, operation, body, error=None):
        lo = len(self.wire())
        result = subprocess.run([self.a.probe,operation],input=json.dumps(body),
                    env=dict(self.env,PROBE_URL='http://'+self.env['RUN_CACHE_API_URL'],
                             PROBE_SESSION_ID=self.a.run_id,PROBE_INVOCATION=self.a.run_id),
                    capture_output=True,text=True,timeout=60)
        self.event('crafted-public-rpc',operation=operation,request=body,returncode=result.returncode,
                   stdout=result.stdout,stderr=result.stderr)
        calls=self.wire()[lo:]
        if len(calls)!=1:
            raise Refused('crafted RPC must be captured exactly once')
        if error:
            if result.returncode==0 or (calls[0]['status']['code'],calls[0]['status']['message'])!=error:
                raise Refused('unexpected complete RPC error: '+str(calls[0]))
            return None
        if result.returncode:
            raise Refused('RPC failed: '+self.clean(result.stderr))
        return json.loads(result.stdout)

    def check_relation(self,label,role,name):
        result=self.observe(label,self.scope.relation(role,name))
        props=json.loads((self.work/'physical'/f'{label}_properties.json').read_text())
        assert_physical(result,props,0,'table',self.query_identity)
        return result

    def required_submit(self,calls,expected):
        submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
        if len(submits)!=1 or expected not in submits[0]['response']['response']:
            raise Refused('expected exactly one '+expected)
        return submits

    def baseline(self):
        self.setup_project()
        (self.project/'models/sources.yml').unlink()
        self.save('operation-manifest.json',{'client':'2.0.11','backend':'rust' if self.a.rust_url else 'hosted',
            'actual_client':['failed execution','same failing SQL retry','changed SQL recovery','required unchanged reuse','write-only Record'],
            'crafted_public_grpc':['atomic rejected Record batch','valid Record','duplicate Record','pending submits','delayed confirm','duplicate confirm','unknown confirm'],
            'restart':'separate local production-router persistence test; no hosted restart claim',
            'relations':{role:{name:self.scope.relation(role,name) for name in ('recovered','recorded','delayed')}
                         for role in ('prod','ref')}})
        config="materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}"
        for label in ('failed_first','failed_retry'):
            self.write_model('recovered',config,"select cast(raise_error('owned_lifecycle_158_failure') as bigint) id")
            lo=len(self.wire())
            try:
                self.dbt(label,['run','--select','recovered'])
            except Refused as error:
                if ('client invocation exited 1;' not in str(error)
                        or '[USER_RAISED_EXCEPTION] owned_lifecycle_158_failure' not in str(error)):
                    raise
            else:
                raise Refused('expected actual warehouse execution failure')
            calls=self.wire()[lo:]
            submits=self.required_submit(calls,'ready_to_execute')
            if any(r['method']=='ConfirmExecution' for r in calls):
                raise Refused('failed execution unexpectedly confirmed')
            self.explain(label,submits)
        self.write_model('recovered',config,query(0,self.query_identity))
        confirmed=None
        for label,expected in [('recovery','ready_to_execute'),('recovery_unchanged','skip_execution')]:
            calls=self.dbt(label,['run','--select','recovered'])
            submits=self.required_submit(calls,expected)
            if expected=='ready_to_execute':
                confirms=[r for r in calls if r['method']=='ConfirmExecution']
                pending=submits[0]['response']['response'][expected]['request_id']
                if len(confirms)!=1 or confirms[0]['request']['request_id']!=pending or confirms[0]['response']['success'] is not True:
                    raise Refused('recovery lacks successful correlated confirmation')
                confirmed=confirms[0]['request']
            self.explain(label,submits)
            actual=self.check_relation(label,'prod','recovered')
            independent=self.sql('reference_'+label,query(0,self.query_identity).replace('select ','SELECT ',1))
            if not same_result(actual,independent):
                raise Refused('independent full rows/schema mismatch')
        # Independent State-disabled dbt project checks the warehouse execution
        # through a different Target relation, with no shared output artifacts.
        source=self.project
        reference=self.work/'reference-project'
        shutil.copytree(source,reference)
        self.project=reference
        if self.dbt('reference_recovery',['run','--select','recovered'],target='ref',managed=False):
            raise Refused('reference unexpectedly sent State RPCs')
        independent=self.check_relation('reference_recovery','ref','recovered')
        if not same_result(actual,independent):
            raise Refused('independent dbt full rows/schema mismatch')
        self.project=source
        # Client-reachable Record path, distinct from crafted rejection probes.
        self.write_model('recorded',config,query(0,self.query_identity))
        calls=self.dbt('write_only_record',['run','--select','recorded','--state-mode','write-only'])
        records=[r for r in calls if r['method']=='RecordExecutions']
        if len(records)!=1 or records[0]['response']['records_stored']!=1:
            raise Refused('write-only did not publish exactly one execution')
        self.check_relation('write_only_record','prod','recorded')
        calls=self.dbt('recorded_reuse',['run','--select','recorded'])
        self.explain('recorded_reuse',self.required_submit(calls,'skip_execution'))
        self.check_relation('recorded_reuse','prod','recorded')
        # Atomic rejection uses a fresh identity and a physically executed result.
        req=deepcopy(submits[0]['request'])
        req['target_table']='`'+self.scope.relation('prod','delayed').replace('.','`.`')+'`'
        uid=req['dbt_node_state']['node_unique_id']+'__delayed'
        req['dbt_node_state']['node_unique_id']=uid
        req['labels']=dict(req['labels'],dbt_node_unique_id=uid)
        req.update(tables=[],allow_clones=True,clone_chain_depth_limit=None,
                   table_namespace='lifecycle_'+self.a.run_id+'_delayed')
        self.sql('owned_delayed_execution','CREATE TABLE '+self.scope.relation('prod','delayed')+' USING DELTA AS '+query(0,self.query_identity))
        self.check_relation('delayed_before_publication','prod','delayed')
        # This epoch is crafted protocol evidence, not a claimed client metadata
        # observation. The actual-client cells above retain real Delta epochs.
        epoch=int(time.time()*1000)
        outcome={'last_modified_epoch':epoch,'table_type':'MANAGED','execution_results':None,'execution_runtime_ms':158}
        keys=('target_table','dialect','default_catalog','execution_type','sql','tables','query_dependencies','semantic_extras','labels','dbt_node_state','default_schema','table_namespace')
        record={'outcome':outcome,'input':{'enriched_sql':{**{k:req[k] for k in keys},
                                                    'from_speculative_submit':False}}}
        self.rpc('record',{'records':[record,{'outcome':None,'input':None}]},
                 error=(3,'Record at index 1 has no input set'))
        req['tables']=[{'name':req['target_table'],'last_modified_epoch':epoch}]
        lo=len(self.wire());first=self.rpc('submit',req)
        self.required_submit(self.wire()[lo:],'ready_to_execute')
        lo=len(self.wire());second=self.rpc('submit',req)
        self.required_submit(self.wire()[lo:],'ready_to_execute')
        confirm=dict(outcome,request_id=second['response']['ready_to_execute']['request_id'],
                     failed_to_clone=False,labels=req['labels'])
        self.rpc('confirm',confirm)
        for label,body in [('confirmed',None),('delayed',dict(confirm,
                request_id=first['response']['ready_to_execute']['request_id'],
                last_modified_epoch=epoch-1000,execution_runtime_ms=1)),
                ('unknown',dict(confirm,request_id='019f0000-0000-7000-8000-000000000158'))]:
            if body:
                self.rpc('confirm',body,error=(5,'Execution request with ID '+body['request_id']+' not found')
                         if label=='unknown' else None)
            lo=len(self.wire());response=self.rpc('submit',req)
            checks=self.required_submit(self.wire()[lo:],'skip_execution')
            if response['response']['skip_execution']['execution_runtime_ms']!=158:
                raise Refused('late confirmation replaced newer physical evidence')
            self.explain(label,checks)
        for label in ('record_valid','record_duplicate'):
            if self.rpc('record',{'records':[record]})!={'records_stored':1}:
                raise Refused('unexpected Record count')
            lo=len(self.wire());response=self.rpc('submit',req)
            checks=self.required_submit(self.wire()[lo:],'skip_execution')
            if response['response']['skip_execution']['execution_runtime_ms']!=158:
                raise Refused('wrong published runtime')
            self.explain(label,checks)
        # Duplicate confirmation must be harmless even when its response is an
        # error. Both exact and conflicting repeats preserve the first outcome.
        for label,body in [('same',confirmed),('changed',dict(confirmed,execution_runtime_ms=999,last_modified_epoch=epoch+1000))]:
            self.rpc('confirm',body,error=(13,'Database operation failed'))
            calls=self.dbt('after_duplicate_'+label,['run','--select','recovered'])
            self.explain('after_duplicate_'+label,self.required_submit(calls,'skip_execution'))
            self.check_relation('after_duplicate_'+label,'prod','recovered')
        self.save('checks/accepted.json',{'failed_attempts':2,'required_reuses':sum(r['method']=='SubmitEnrichedSQL' and 'skip_execution' in (r.get('response') or {}).get('response',{}) for r in self.wire()),
            'complete_typed_multiset_schema_and_properties':True,'reference':'independent dbt plus SELECT',
            'duplicate_status':13,'atomic_rejection_status':3})


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','probe','work-dir','schema-lease'):
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--rust-url')
    parser.add_argument('--query-identity', help='shared fresh literal identity for hosted/Rust comparison')
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks')
    parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+os.urandom(3).hex())
    args=parser.parse_args()
    if args.rust_url:
        import re
        if not re.fullmatch(r'http://127\.0\.0\.1:\d+',args.rust_url):
            parser.error('--rust-url must be an owned loopback endpoint')
    try:
        return Lifecycle(args).run()
    except Exception as error:
        print('FAILED:',str(error),file=sys.stderr)
        return 1


if __name__=='__main__':
    sys.exit(main())
