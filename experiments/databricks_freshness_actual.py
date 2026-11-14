#!/usr/bin/env python3
"""Bounded actual-client equal/delayed evidence discovery with no-object references.

Hosted first, then exact replay, then Rust using a fresh pair. Each phase owns
only two exact relations in the lease's prod role; ref is inventory-only.
No cleanup or schema creation. Absence of an observed delay/equal-change is a
reachability limitation, never inferred metadata precision or policy evidence.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

from databricks_baseline import check_inventory
from databricks_freshness import Freshness, decisions
from databricks_readonly_reference import show_reference
from databricks_survey import Refused, same_result


class ActualFreshness(Freshness):
    entrypoint = Path(__file__)

    def __init__(self, args):
        import re
        if args.rust_url and not re.fullmatch(r'http://127\.0\.0\.1:\d+', args.rust_url):
            raise Refused('Rust endpoint must be an owned literal loopback URL')
        super().__init__(args)
        self.save('schema-lease.json',self.lease)
        self.scope.allowed = {f'cdo_demo_db.{self.scope.schema("prod")}.{self.scope.relation_prefix}{name}'
                              for name in ('inputs','consumer')}
        import hashlib
        self.save('source-provenance.json',{p.name:hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in Path(__file__).parent.glob('databricks_*.py')})
        self.save('footprint.json', {'phase_max_objects':2, 'hosted_and_rust_per_workload_max_objects':4, 'actual_plus_race_total_max_objects':10,
                  'roles':self.scope.roles, 'allowed':sorted(self.scope.allowed),
                  'reference_objects':0, 'previous_objects_writable':False})
        # Do not inherit arbitrary project-related adapter config into the empty
        # read-only project. Preserve only the evidenced connection fields.
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role, output in profile['survey']['outputs'].items():
            profile['survey']['outputs'][role]={k:v for k,v in output.items()
                if k in {'type','host','http_path','catalog','schema','token','threads'}}
        self.save('profiles/profiles.yml',profile)

    def init_schemas(self):
        inventory=self.inventory();check_inventory(self.scope,self.lease,inventory)
        if len(inventory[self.scope.schema('prod')])+2>100:
            raise Refused('two-object phase exceeds observed schema capacity')
        self.before_inventory=inventory
        self.save('lease-inventory-before.json',inventory)
        return None

    def checked_results(self,label,expected,sql):
        actual=self.observe(label,self.scope.relation('prod','consumer'))
        direct=self.sql(label+'_independent_transform',sql)
        from databricks_freshness import assert_rows
        assert_rows(actual,expected);assert_rows(direct,expected)
        properties=json.loads((self.work/'physical'/f'{label}_properties.json').read_text())
        props={r[0]:r[1] for r in properties['rows']}
        if props.get('Type')!='MANAGED' or props.get('Provider','').lower()!='delta':
            raise Refused('physical:properties')
        if not same_result(actual,direct):raise Refused('physical:reference')
        typed_expected=[[int(row[0]),None if row[1] is None else int(row[1]),row[2]] for row in expected]
        show_reference(self,label,sql,typed_expected,direct)
        after=self.inventory()
        for schema,rows in self.before_inventory.items():
            if any(r not in after[schema] for r in rows):raise Refused('preexisting object changed')
            added=[r for r in after[schema] if r not in rows]
            if any(r['full_name'] not in self.scope.allowed for r in added):
                raise Refused('unplanned physical object')
        self.save('checks/'+label+'.json',{'independent_typed_rows_schema':True,
                  'managed_delta':True,'readonly_dbt_reference':True,'expected':expected})

    def invocation(self,label,amount,expected=None):
        start=datetime.now(timezone.utc).isoformat();mono=time.monotonic()
        calls=self.dbt(label,['run','--select','consumer'])
        submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
        if len(submits)!=1:raise Refused('expected one actual submit')
        response=submits[0]['response']['response']
        observed='skip_execution' if 'skip_execution' in response else 'ready_to_execute'
        decisions(calls,expected or observed)
        self.explain(label,submits)
        self.sample(label+'_after_client')
        self.checked_results(label,[['1',str(amount),self.tag],['1',str(amount),self.tag],['2',None,None]],self.direct)
        entry={'label':label,'started_at':start,'elapsed':time.monotonic()-mono,
               'submit':submits[0],'observed':observed,'evidence_class':'actual-pinned-client'}
        self.observations.append(entry);self.save('actual-observations.json',self.observations)
        return entry

    def baseline(self):
        self.setup_project();self.observations=[]
        source=self.scope.relation('prod','inputs');self.direct=f'SELECT id, amount, tag FROM {source}'
        self.tag='actual_'+self.query_identity
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'freshness','database':'cdo_demo_db',
                  'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.write_model('consumer',"materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}",
                         "select id, amount, tag from {{ source('freshness','inputs') }}")
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('initial_source',f"INSERT INTO {source} VALUES (1,10,'{self.tag}'),(1,10,'{self.tag}'),(2,NULL,NULL)")
        self.invocation('first',10,'ready_to_execute');previous=self.invocation('unchanged',10,'skip_execution')
        equal=[]
        # Three mutation/actual-client attempts; deliberately do not delay or run
        # metadata queries between the update completion and the next dbt call.
        for amount in (11,12,13):
            self.sql('rapid_update_'+str(amount),f'UPDATE {source} SET amount={amount} WHERE id=1')
            current=self.invocation('rapid_'+str(amount),amount)
            old=[r for r in previous['submit']['request']['tables'] if r['name']!=previous['submit']['request']['target_table']]
            new=[r for r in current['submit']['request']['tables'] if r['name']!=current['submit']['request']['target_table']]
            equal.append({'amount':amount,'recorded_source':old,'current_source':new,
                          'equal_known_evidence':len(old)==len(new)==1 and old[0].get('last_modified_epoch') is not None and old==new})
            previous=current
        self.sql('delayed_update',f'UPDATE {source} SET amount=17 WHERE id=1')
        mutation_finished=time.monotonic();delay=[]
        for offset in (0,1,3):
            remaining=mutation_finished+offset-time.monotonic()
            if remaining>0:time.sleep(remaining)
            start=datetime.now(timezone.utc).isoformat();elapsed=time.monotonic()-mutation_finished
            self.sample('delayed_'+str(offset)+'_before_client')
            observation=self.invocation('delayed_'+str(offset),17,
                                        'ready_to_execute' if offset==0 else 'skip_execution')
            delay.append({'scheduled_offset_seconds':offset,'actual_start_elapsed_seconds':elapsed,
                          'started_at':start,'submit_tables':observation['submit']['request']['tables']})
        self.save('reachability.json',{'equal_change_attempts':equal,'delay_observations':delay,
                  'precision_conclusion':None,'delay_reachability_conclusion':'Inspect actual timestamps/epochs; scheduled offsets are not actual elapsed delays.',
                  'race':'Not attempted: current recording proxy lacks a reviewed one-shot forwarding gate; mandatory capability remains unverified.',
                  'omitted_source':'Known unsafe hosted SKIP remains whole-ticket blocker; no metadata edited in this campaign.'})
        return {'managed_invocations':8,'rapid_change_attempts':3,'delay_observations':3,'race_attempts':0}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease'):p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91');p.add_argument('--query-identity');p.add_argument('--rust-url')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+os.urandom(3).hex())
    return ActualFreshness(p.parse_args()).run()

if __name__=='__main__':raise SystemExit(main())
