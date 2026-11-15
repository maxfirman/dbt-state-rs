#!/usr/bin/env python3
"""Warehouse-free hosted protocol scope/lifecycle characterization for #155.

Start from a pinned-client request, but classify every variation and synthetic
confirmation as crafted protocol evidence, never as real warehouse execution.
"""
from copy import deepcopy
import json
from pathlib import Path
import time

from databricks_deferral import Deferral
from databricks_survey import Refused


class DeferralProtocol(Deferral):
    def init_schemas(self):
        self.save('classification.json', {'evidence_class':'crafted-public-grpc',
                  'warehouse_sql_executed':False,'actual_client_reachable':False,
                  'synthetic_confirmations':True})
        self.save('protocol-runner.py',Path(__file__).read_text())

    def baseline(self):
        rows = [json.loads(line) for line in Path(self.a.template).read_text().splitlines()]
        base = next(r['request'] for r in rows if r['method']=='SubmitEnrichedSQL')
        for dialect in ('databricks','snowflake'):
            for use_id in (False, True):
                cell = dialect + ('_id' if use_id else '_named')
                uid = 'model.protocol_' + self.a.run_id + '.' + cell
                body = deepcopy(base)
                # Exact declared owned relation name, but no warehouse SQL runs.
                relation = self.scope.relation('prod','pending')
                quote = '`' if dialect=='databricks' else '"'
                body.update(dialect=dialect, target_table=quote+relation.replace('.',quote+'.'+quote)+quote,
                            sql='select 155 as id /* '+self.a.run_id+'_'+cell+' */', tables=[],
                            table_namespace='protocol_'+self.a.run_id+'_'+cell,allow_clones=False)
                project_id = 'protocol_id_'+self.a.run_id+'_'+cell if use_id else None
                body['dbt_node_state'].update(node_unique_id=uid, project_name='protocol_'+self.a.run_id+'_'+cell,
                     profile_name='protocol_profile', target_name='protocol_prod',project_id=project_id)
                body['labels']=dict(body['labels'],dbt_node_unique_id=uid,dbt_node_name=cell)
                submit=self.rpc('submit',body)['response']['ready_to_execute']
                request={key:body['dbt_node_state'][key] for key in ('profile_name','target_name','project_name','project_id')}
                request['node_unique_ids']=[uid]
                if self.rpc('resolve',request)['fqn_by_unique_id']:
                    raise Refused('pending synthetic execution published a relation')
                self.rpc('confirm',{'request_id':submit['request_id'],'last_modified_epoch':int(time.time()*1000),
                         'failed_to_clone':False,'table_type':'MANAGED' if dialect=='databricks' else 'TABLE',
                         'execution_results':None,'execution_runtime_ms':7,'labels':body['labels']})
                for label,updates in [('confirmed',{}),('other_profile',{'profile_name':'other'}),
                      ('other_name',{'project_name':'other'}),('no_id',{'project_id':None}),
                      ('empty_id',{'project_id':''}),('wrong_id',{'project_id':'wrong'}),
                      ('missing_profile',{'profile_name':''}),('missing_name',{'project_name':''})]:
                    req=dict(request,**updates)
                    result=self.rpc('resolve',req)
                    visible=(label in ('confirmed','no_id','empty_id') or use_id and label in ('other_name','missing_name'))
                    expected={uid:body['target_table']} if visible else {}
                    if result['fqn_by_unique_id'] != expected:
                        raise Refused('protocol scope mismatch: '+cell+'_'+label)
                    self.save('cells/'+cell+'_'+label+'.json',{'request':req,'response':result})
                # failed_to_clone reports a successful fallback build, not an
                # execution-failure flag. Observe its first outcome explicitly.
                body['sql'] += ' /* fallback */'
                body['table_namespace'] += '_fallback'
                submit=self.rpc('submit',body)['response']['ready_to_execute']
                self.rpc('confirm',{'request_id':submit['request_id'],'last_modified_epoch':int(time.time()*1000),
                         'failed_to_clone':True,'table_type':'MANAGED' if dialect=='databricks' else 'TABLE',
                         'execution_results':None,'execution_runtime_ms':9,'labels':body['labels']})
                if self.rpc('resolve',request)['fqn_by_unique_id'] != {uid:body['target_table']}:
                    raise Refused('fallback confirmation lost mapping')

        self.skip_scopes(base)

    def skip_scopes(self, base):
        for dialect in ('databricks','snowflake'):
            body=deepcopy(base)
            quote='`' if dialect=='databricks' else '"'
            relation=self.scope.relation('prod','pending')
            body.update(dialect=dialect,sql='select 155 as id /* skip_'+self.a.run_id+' */',
                        tables=[],allow_clones=False,
                        target_table=quote+relation.replace('.',quote+'.'+quote)+quote,
                        table_namespace='skip_scope_'+self.a.run_id+'_'+dialect)
            uid='model.skip_'+self.a.run_id+'.first'
            body['dbt_node_state'].update(node_unique_id=uid,profile_name='first_profile',
                  project_name='first_project',target_name='prod',project_id=None)
            body['labels']=dict(body['labels'],dbt_node_unique_id=uid,dbt_node_name='first')
            submit=self.rpc('submit',body)['response']['ready_to_execute']
            epoch=int(time.time()*1000)
            self.rpc('confirm',{'request_id':submit['request_id'],'last_modified_epoch':epoch,
                     'failed_to_clone':False,'table_type':'MANAGED' if dialect=='databricks' else 'TABLE',
                     'execution_results':None,'execution_runtime_ms':7,'labels':body['labels']})
            for label,changes in [('same',{}),('new_uid',{'node_unique_id':'model.skip_'+self.a.run_id+'.second'}),
                       ('new_scope',{'profile_name':'second_profile','project_name':'second_project','project_id':'second_id'})]:
                request=deepcopy(body)
                request['tables']=[{'name':body['target_table'],'last_modified_epoch':epoch}]
                request['dbt_node_state'].update(**changes)
                request['labels']['dbt_node_unique_id']=request['dbt_node_state']['node_unique_id']
                if 'skip_execution' not in self.rpc('submit',request)['response']:
                    raise Refused('required same-physical-history reuse missing')
                resolve={key:request['dbt_node_state'][key] for key in ('profile_name','project_name','project_id','target_name')}
                resolve['node_unique_ids']=[request['dbt_node_state']['node_unique_id']]
                result=self.rpc('resolve',resolve)
                expected={uid:body['target_table']} if label=='same' else {}
                if result['fqn_by_unique_id'] != expected:
                    raise Refused('SKIP changed confirmed logical deferral scope')
                self.save('skip_cells/'+dialect+'_'+label+'.json',{'request':resolve,'response':result})


def main():
    import argparse, os, sys
    from datetime import datetime, timezone
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dbt',required=True)
    parser.add_argument('--proxy',required=True)
    parser.add_argument('--probe',required=True)
    parser.add_argument('--template',required=True)
    parser.add_argument('--schema-lease',required=True)
    parser.add_argument('--work-dir',required=True)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks')
    parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+os.urandom(3).hex())
    args=parser.parse_args()
    args.rust_url=None
    args.query_identity=None
    try:
        return DeferralProtocol(args).run()
    except Exception as error:
        print('FAILED:',str(error),file=sys.stderr)
        return 1


if __name__=='__main__':
    raise SystemExit(main())
