#!/usr/bin/env python3
"""Opt-in bounded #142 producer/policy discovery; hosted before Rust replay.

Four fresh production-role objects, zero persistent reference objects, no schema
creation or cleanup. Decisions are observations until separately captured and
replayed; no hosted policy is inferred from this runner's local hypotheses.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

from databricks_baseline import check_inventory
from databricks_freshness_actual import ActualFreshness
from databricks_policy import PolicySnapshot, assert_policy_result
from databricks_readonly_reference import show_reference
from databricks_freshness import assert_rows
from databricks_survey import Refused


class Policy(ActualFreshness):
    entrypoint=Path(__file__)

    def __init__(self,args):
        super().__init__(args)
        self.scope.relation_prefix='p_'+args.run_id+'_'
        self.scope.allowed={f'cdo_demo_db.{self.scope.schema("prod")}.{self.scope.relation_prefix}{name}'
                            for name in ('inputs','producer_a','producer_b','consumer')}
        self.save('footprint.json',{'phase_max_objects':4,'hosted_and_rust_max_objects':8,
                  'roles':self.scope.roles,'allowed':sorted(self.scope.allowed),
                  'reference_objects':0,'previous_objects_writable':False})
        self.clearance=json.loads(Path(args.live_clearance).read_text())
        backend='rust' if args.rust_url else 'hosted'
        if (args.run_id!=self.clearance['run_ids'][backend]
                or set(self.clearance['exact_allowlists'][backend])!=self.scope.allowed
                or self.clearance['reference_objects']!=0 or self.clearance['max_new_objects']!=8):
            raise Refused('run names/footprint differ from exact live clearance')
        self.save('live-clearance.json',self.clearance)
        self.cells=[];self.snapshot=None;self.amounts={'a':10,'b':20};self.offsets={'a':0,'b':0}
        self.contract=json.loads(Path(args.contract).read_text()) if args.contract else {}

    def init_schemas(self):
        inventory=self.inventory()
        if not self.a.resume_from:check_inventory(self.scope,self.lease,inventory)
        else:
            prior=json.loads((Path(self.a.resume_from)/'lease-inventory-after.json').read_text())
            if any(sorted(inventory[k],key=lambda r:r['full_name'])!=sorted(prior[k],key=lambda r:r['full_name']) for k in prior):
                raise Refused('same-graph continuation inventory changed from certified stopped phase')
            present={r['full_name'] for rows in inventory.values() for r in rows}
            if not self.scope.allowed<=present:raise Refused('same-graph continuation lacks exact four owned relations')
        if len(inventory[self.scope.schema('prod')])+(0 if self.a.resume_from else 4)>100:
            raise Refused('four-object phase exceeds observed schema capacity')
        expected=dict(self.clearance['observed_counts'])
        if self.a.rust_url or self.a.resume_from:expected[self.scope.schema('prod')]+=4
        if {schema:len(rows) for schema,rows in inventory.items()}!=expected:
            raise Refused('fresh inventory counts differ from exact phase clearance')
        self.before_inventory=inventory;self.save('lease-inventory-before.json',inventory)

    def configure(self,policy):
        # Ordinary pinned-client YAML, with no user macros or modified client.
        state={'compare_unrendered_code':False,'pre_clone':'never',
               'lag_tolerance':f'{self.a.lag_minutes}m','require_fresh_data_from':policy}
        self.save('project/models/policy.yml',{'version':2,'models':[{'name':'consumer','config':{'state':state}}]})
        self.policy=policy

    def expected_rows(self):
        return [[1,self.amounts['a']+self.offsets['a'],'a_'+self.query_identity],
                [1,self.amounts['a']+self.offsets['a'],'a_'+self.query_identity],
                [2,None,'a_'+self.query_identity],
                [3,self.amounts['b']+self.offsets['b'],'b_'+self.query_identity],
                [4,None,'b_'+self.query_identity]]

    def reference_sql(self):
        source=self.scope.relation('prod','inputs')
        return ' UNION ALL '.join(
            f"SELECT cast(id as bigint) id, cast(amount+{self.offsets[b]} as int) amount, "
            f"cast('{b}_{self.query_identity}' as string) tag FROM {source} WHERE tag='{b}'"
            for b in ('a','b'))

    def producer(self,branch):
        self.write_model('producer_'+branch,
            "materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never','compare_unrendered_code':false}",
            f"select cast(id as bigint) id, cast(amount+{self.offsets[branch]} as int) amount, "
            f"cast('{branch}_{self.query_identity}' as string) tag "
            "from {{ source('policy','inputs') }} where tag='"+branch+"'")

    def publish(self,label,name):
        calls=self.dbt(label,['run','--select',name,'--state-mode','write-only'])
        records=[r for r in calls if r['method']=='RecordExecutions']
        if (len(records)!=1 or records[0]['response']!={'records_stored':1}
                or len(records[0]['request']['records'])!=1):
            raise Refused('expected exactly one successful actual write-only Record')
        record=records[0]['request']['records'][0]
        req=record['input']['enriched_sql']
        if req['execution_type']!=1 or record['outcome'].get('last_modified_epoch') is None:
            raise Refused('ordinary FULL Record requires known completion epoch')
        self.save('records/'+label+'.json',record)
        actual=self.observe(label+'_published',self.scope.relation('prod',name))
        expected=self.expected_rows()
        if name.startswith('producer_'):
            expected=[r for r in expected if r[2]==name[-1]+'_'+self.query_identity]
        assert_rows(actual,[[None if x is None else str(x) for x in r] for r in expected])
        props=json.loads((self.work/'physical'/f'{label}_published_properties.json').read_text())
        props={r[0]:r[1] for r in props['rows']}
        if props.get('Type')!='MANAGED' or props.get('Provider','').lower()!='delta':
            raise Refused('published physical properties mismatch')
        return req,record['outcome']['last_modified_epoch']

    def wire_policy(self,label,req):
        if (req['freshness_tolerance_seconds']!=self.a.lag_minutes*60
                or req['stale_upstream_policy']!=int(self.policy=='all')
                or req.get('compare_unrendered_code') or req['execution_type']!=1):
            raise Refused('actual pinned request did not emit intended ordinary FULL policy/lag/template settings')
        manifest=json.loads((self.work/'targets'/f'{self.n:03d}_{label}'/'manifest.json').read_text())
        nodes=[n for n in manifest['nodes'].values() if n.get('name')=='consumer']
        if len(nodes)!=1 or nodes[0]['config']['state']['pre_clone']!='never':
            raise Refused('manifest does not prove consumer pre_clone never')
        self.save('policy-proof/'+label+'.json',{'manifest_config':nodes[0]['config'],
                  'wire_policy':req['stale_upstream_policy'],'wire_seconds':req['freshness_tolerance_seconds'],
                  'wire_compare_unrendered_code':req.get('compare_unrendered_code')})

    def inputs(self,req):
        inputs={}
        for row in req['tables']:
            if row['name']==req['target_table']:continue
            if row.get('last_modified_epoch') is None or row['name'] in inputs:
                raise Refused('policy scope requires complete unique current upstream epochs')
            inputs[row['name']]=row['last_modified_epoch']
        if len(inputs)!=2:raise Refused('policy scope requires exactly two genuine upstreams')
        return inputs

    def physical(self,label,decision):
        fresh=self.expected_rows();sql=self.reference_sql()
        direct=self.sql(label+'_fresh_transform',sql)
        assert_rows(direct,[[None if x is None else str(x) for x in r] for r in fresh])
        show_reference(self,label,sql,fresh,direct)
        actual=self.observe(label,self.scope.relation('prod','consumer'))
        props=json.loads((self.work/'physical'/f'{label}_properties.json').read_text())
        checks=assert_policy_result(actual,props,self.snapshot,fresh,decision)
        self.save('checks/'+label+'.json',checks)
        self.save('fresh-references/'+label+'.json',{'rows':fresh,'sql':sql,'direct':direct})
        after=self.inventory()
        for schema,rows in self.before_inventory.items():
            if any(r not in after[schema] for r in rows):raise Refused('preexisting object changed')
            if any(r['full_name'] not in self.scope.allowed for r in after[schema] if r not in rows):
                raise Refused('unplanned physical object')

    def cell(self,label,expected=None):
        started=datetime.now(timezone.utc).isoformat()
        anchor=self.snapshot
        calls=self.dbt(label,['run','--select','consumer'])
        submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
        if len(submits)!=1:raise Refused('expected exactly one actual consumer Submit')
        row=submits[0];req=row['request'];decision,payload=next(iter(row['response']['response'].items()))
        self.wire_policy(label,req);inputs=self.inputs(req)
        response_ms=int(datetime.fromisoformat(row['recorded_at'].replace('Z','+00:00')).timestamp()*1000)
        age_ms=response_ms-anchor.build_epoch if anchor else None
        if label.endswith('_inside') and (age_ms is None or age_ms>=self.a.lag_minutes*60000-15000):
            raise Refused('inside cell lacks conservative observed proxy/build age margin')
        if decision=='skip_execution':
            own=[r for r in req['tables'] if r['name']==req['target_table']]
            if anchor is None or len(own)!=1 or own[0].get('last_modified_epoch')!=anchor.build_epoch:
                raise Refused('SKIP lacks unchanged known target completion evidence')
        if decision not in ('skip_execution','ready_to_execute'):raise Refused('unhandled observed decision')
        confirmed=[r for r in calls if r['method']=='ConfirmExecution' and r['request']['request_id']==payload.get('request_id')
                   and r['response'].get('success') is True]
        if decision=='ready_to_execute':
            if len(confirmed)!=1:raise Refused('BUILD lacks exactly one successful correlated Confirm')
            self.snapshot=PolicySnapshot(label,self.expected_rows(),inputs,confirmed[0]['request']['last_modified_epoch'])
            self.save('snapshots/'+label+'.json',self.snapshot.artifact())
        if self.snapshot is None:raise Refused('SKIP without saved independent BUILD snapshot')
        previous=self.cells[-1]['request'] if self.cells else req
        delta={k:{'before':previous.get(k),'after':req.get(k)} for k in set(previous)|set(req) if previous.get(k)!=req.get(k)}
        result={'label':label,'started_at':started,'request':req,'response':row['response'],
                'recorded_at':row['recorded_at'],'observed_response_age_ms':age_ms,
                'age_clock_basis':'proxy wall clock versus warehouse completion; service instant unobserved',
                'decision':decision,'request_delta':delta,
                'snapshot':self.snapshot.artifact(),'decision_anchor':anchor.artifact() if anchor else None,
                'local_age_at_start_ms':int(datetime.fromisoformat(started).timestamp()*1000)-anchor.build_epoch if anchor else None,
                'client_evidence':'actual-pinned-2.0.11',
                'policy':self.policy,'lag_seconds':self.a.lag_minutes*60}
        self.cells.append(result);self.save('policy-observations.json',self.cells)
        self.explain(label,submits);self.physical(label,decision)
        required=self.contract.get(label,expected)
        if required and required!=decision:raise Refused(f'{label}: expected {required}, got {decision}')
        return result

    def reset(self,label):
        req,epoch=self.publish(label,'consumer')
        self.snapshot=PolicySnapshot(label,self.expected_rows(),self.inputs(req),epoch)
        self.save('snapshots/'+label+'.json',self.snapshot.artifact())
        self.cell(label+'_unchanged','skip_execution')

    def mutate(self,label,branch,logic=False):
        if logic:
            self.offsets[branch]+=1;self.producer(branch)
        else:
            self.amounts[branch]+=7
            self.sql(label+'_raw_update',f"UPDATE {self.scope.relation('prod','inputs')} SET amount={self.amounts[branch]} WHERE tag='{branch}' AND amount IS NOT NULL")
        req,_=self.publish(label,'producer_'+branch)
        prior=self.producer_requests[branch]
        fields=('target_table','dialect','default_catalog','default_schema','execution_type','sql',
                'query_dependencies','semantic_extras','table_namespace','dbt_node_state')
        deltas={k:{'before':prior.get(k),'after':req.get(k)} for k in fields if prior.get(k)!=req.get(k)}
        if not logic and deltas:raise Refused('same-logic producer Record changed identity fields')
        if logic and prior['sql']==req['sql']:raise Refused('changed producer did not change rendered SQL')
        self.save('producer-audits/'+label+'.json',{'changed_rendered_logic':logic,'field_delta':deltas,
                  'complete_before':prior,'complete_after':req})
        self.producer_requests[branch]=req

    def outside(self,label):
        # Real local/warehouse age with a conservative margin; service timing
        # remains captured evidence, not an invented exact hosted equality.
        deadline=self.snapshot.build_epoch/1000+self.a.lag_minutes*60+15
        while time.time()<deadline:
            remaining=deadline-time.time();print(f'{label}: waiting {remaining:.1f}s for age margin',flush=True)
            time.sleep(min(30,remaining))
        self.save('age-evidence/'+label+'.json',{'build_epoch':self.snapshot.build_epoch,
                  'observed_local_ms':int(time.time()*1000),'tolerance_seconds':self.a.lag_minutes*60,
                  'margin_seconds':15,'kind':'elapsed-age-crossing-not-config-change'})
        return self.cell(label)

    def baseline(self):
        if self.a.resume_from:
            self.resume()
            return self.matrix()
        self.setup_project();source=self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'policy','database':'cdo_demo_db',
                  'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        for branch in ('a','b'):self.producer(branch)
        self.write_model('consumer',"materialized='table',file_format='delta'",
             "select cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag from {{ ref('producer_a') }} "
             "union all select cast(id as bigint), cast(amount as int), cast(tag as string) from {{ ref('producer_b') }}")
        self.configure('any')
        self.dbt('graph_parse',['parse'],managed=False)
        manifest=json.loads((self.work/'targets'/f'{self.n:03d}_graph_parse'/'manifest.json').read_text())
        models=[n for n in manifest['nodes'].values() if n.get('resource_type')=='model']
        if {n['name'] for n in models}!={'producer_a','producer_b','consumer'}:
            raise Refused('parsed graph must contain exactly the three planned FULL models')
        for node in models:
            if node['config']['materialized']!='table':raise Refused('only ordinary FULL table models permitted')
            self.scope.check('.'.join([node['database'],node['schema'],node['alias']]))
        self.save('graph-proof.json',{'models':[{k:n[k] for k in ('name','database','schema','alias','config','depends_on')} for n in models]})
        self.sql('create_raw',f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('initial_raw',f"INSERT INTO {source} VALUES (1,10,'a'),(1,10,'a'),(2,NULL,'a'),(3,20,'b'),(4,NULL,'b')")
        self.producer_requests={}
        calls=self.dbt('initial_producers',['run','--select','producer_a','producer_b'])
        submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
        if len(submits)!=2 or {r['request']['labels']['dbt_node_name'] for r in submits}!={'producer_a','producer_b'}:
            raise Refused('initial producer graph must submit exactly both producers')
        confirms={r['request']['request_id'] for r in calls if r['method']=='ConfirmExecution' and r['response'].get('success') is True}
        for row in submits:
            response=row['response']['response']
            if 'ready_to_execute' not in response or response['ready_to_execute']['request_id'] not in confirms:
                raise Refused('initial producer requires ordinary BUILD+Confirm')
            self.producer_requests[row['request']['labels']['dbt_node_name'][-1]]=row['request']
        self.explain('initial_producers',submits)
        self.cell('first','ready_to_execute');self.cell('first_unchanged','skip_execution')
        self.configure('all');self.cell('config_all')
        if self.a.config_only:return
        return self.matrix()

    def resume(self):
        import shutil
        base=Path(self.a.resume_from)
        summary=json.loads((base/'summary.json').read_text())
        if summary['success'] or summary['failure']!='actual pinned request did not emit intended ordinary FULL policy/lag/template settings':
            raise Refused('resume only admits documented stopped config discovery phase')
        configuration=json.loads((base/'configuration.json').read_text())
        if configuration['run_id']!=self.a.run_id or configuration['baseline']['query_identity']!=self.query_identity:
            raise Refused('resume changed graph/query identity')
        shutil.copytree(base/'project',self.project)
        original=json.loads((base/'snapshots/first.json').read_text())
        self.snapshot=PolicySnapshot(original['label'],original['rows'],original['upstream_epochs'],original['build_epoch'])
        initial=json.loads((base/'wire/002_initial_producers.json').read_text())
        self.producer_requests={r['request']['labels']['dbt_node_name'][-1]:r['request'] for r in initial if r['method']=='SubmitEnrichedSQL'}
        self.save('continuation.json',{'prior':str(base),'prior_capture':summary['capture'],
                  'reason':'legacy updates_on candidate parsed but emitted ANY; use primary documented require_fresh_data_from',
                  'primary_source':'https://docs.getdbt.com/reference/resource-configs/dbt-state-configs?version=2',
                  'fresh_graph':False,'extra_objects':0})
        failed=json.loads((base/'wire/005_config_all.json').read_text())
        self.explain('legacy_all_candidate',[r for r in failed if r['method']=='SubmitEnrichedSQL'])
        self.configure('all');self.cell('config_all_documented')

    def matrix(self):
        for policy in ('any','all'):
            self.configure(policy);self.reset(policy+'_partial_anchor')
            self.mutate(policy+'_a_same_logic','a');self.cell(policy+'_partial_inside')
            self.outside(policy+'_partial_outside')
            if policy=='all':
                # Earlier partial SKIPs must not reset the BUILD input anchors.
                self.mutate('all_b_after_partial_skips','b');self.cell('all_b_after_partial_skips')
            self.reset(policy+'_both_anchor')
            self.mutate(policy+'_a_both','a');self.mutate(policy+'_b_both','b')
            self.cell(policy+'_both_inside');self.outside(policy+'_both_outside')
            self.reset(policy+'_logic_inside_anchor')
            self.mutate(policy+'_changed_logic_inside','a',True);self.cell(policy+'_changed_logic_inside')
            self.reset(policy+'_logic_outside_anchor')
            self.outside(policy+'_logic_outside_unchanged')
            self.mutate(policy+'_changed_logic_outside','a',True);self.cell(policy+'_changed_logic_outside')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease','live-clearance'):p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91');p.add_argument('--query-identity');p.add_argument('--rust-url')
    p.add_argument('--resume-from',help='explicit same-graph continuation of a retained stopped config discovery')
    p.add_argument('--contract',help='captured per-label decisions, supplied only after hosted discovery')
    p.add_argument('--config-only',action='store_true',help='bounded first-build/unchanged YAML-wire reachability pilot')
    p.add_argument('--lag-minutes',type=int,default=3)
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+os.urandom(3).hex())
    args=p.parse_args()
    if not 1<=args.lag_minutes<=10:p.error('lag-minutes must be 1..10')
    return Policy(args).run()

if __name__=='__main__':raise SystemExit(main())
