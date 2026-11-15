#!/usr/bin/env python3
"""Bounded APPEND operation-history oracle and opt-in real-client campaign.

Offline expectations are worked literals. Hosted capture, exact public replay,
then fresh Rust physical acceptance are required; this is not whole #146.
"""
from databricks_freshness import assert_rows, decisions
from databricks_survey import Refused

# R is the complete accumulated target; F is the selected rows at this step.
# These literal snapshots are independent of State identity/freshness code and
# warehouse results. A configured unique_key does not change APPEND semantics.
APPEND_HISTORY = {
 'first': {
  'F': [['1','10','same'],['1','10','same'],['2','20','old'],['3',None,None]],
  'R': [['1','10','same'],['1','10','same'],['2','20','old'],['3',None,None]],
 },
 'upsert': {
  'F': [['1','11','changed'],['1','11','changed'],['2','20','old'],['3',None,None],['4','40','new'],['4','40','new']],
  'R': [['1','10','same'],['1','10','same'],['2','20','old'],['3',None,None],
        ['1','11','changed'],['1','11','changed'],['2','20','old'],['3',None,None],['4','40','new'],['4','40','new']],
 },
 'delete': {
  'F': [['1','11','changed'],['1','11','changed'],['3',None,None],['4','40','new'],['4','40','new']],
  'R': [['1','10','same'],['1','10','same'],['2','20','old'],['3',None,None],
        ['1','11','changed'],['1','11','changed'],['2','20','old'],['3',None,None],['4','40','new'],['4','40','new'],
        ['1','11','changed'],['1','11','changed'],['3',None,None],['4','40','new'],['4','40','new']],
 },
 'projection': {
  'F': [['1','111','changed'],['1','111','changed'],['3',None,None],['4','140','new'],['4','140','new']],
  'R': [['1','10','same'],['1','10','same'],['2','20','old'],['3',None,None],
        ['1','11','changed'],['1','11','changed'],['2','20','old'],['3',None,None],['4','40','new'],['4','40','new'],
        ['1','11','changed'],['1','11','changed'],['3',None,None],['4','40','new'],['4','40','new'],
        ['1','111','changed'],['1','111','changed'],['3',None,None],['4','140','new'],['4','140','new']],
 },
 'refresh': {
  'F': [['1','111','changed'],['1','111','changed'],['3',None,None],['4','140','new'],['4','140','new']],
  'R': [['1','111','changed'],['1','111','changed'],['3',None,None],['4','140','new'],['4','140','new']],
 },
 'next_ordinary': {
  'F': [['1','111','changed'],['1','111','changed'],['3',None,None],['4','140','new'],['4','140','new']],
  'R': [['1','111','changed'],['1','111','changed'],['3',None,None],['4','140','new'],['4','140','new'],
        ['1','111','changed'],['1','111','changed'],['3',None,None],['4','140','new'],['4','140','new']],
 },
}


def assert_append_result(actual, properties, expected):
    assert_rows(actual, expected)
    if [c.get("position") for c in actual["columns"]] != [0,1,2]:
        raise Refused("physical:schema-position")
    props={row[0]:row[1] for row in properties['rows']}
    if props.get('Type') != 'MANAGED' or props.get('Provider','').lower() != 'delta':
        raise Refused('physical:properties')


def literal_reference(rows):
    def cell(value,kind):
        if value is None:return 'cast(null as '+kind+')'
        if kind == 'string':return "'"+value.replace("'","''")+"'"
        return str(int(value))
    values=','.join('('+','.join(cell(v,t) for v,t in zip(row,('bigint','int','string')))+')' for row in rows)
    return ('select cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag '
            'from values '+values+' as declared(id,amount,tag)')


def append_submit(calls, source, target, target_exists, expected=None):
    submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
    if len(submits)!=1:raise Refused('expected one actual APPEND submit')
    request=submits[0]['request']
    if (request['execution_type']!=2 or request['compare_unrendered_code']
            or not request.get('table_namespace') or request['query_dependencies']
            or request['semantic_extras'].get('unique_key') != '"id"'
            or request['semantic_extras'].get('on_schema_change') != '"ignore"'
            or request.get('clone_chain_depth_limit') != 0
            or request.get('ignore_external_modifications')):
        raise Refused('outside bounded rendered APPEND scope')
    physical=lambda name:name.replace('`','').lower()
    wanted={source,target} if target_exists else {source}
    evidence=request['tables']
    if (len(evidence)!=len(wanted) or {physical(t['name']) for t in evidence}!=wanted
            or any(t.get('last_modified_epoch') is None for t in evidence)
            or physical(request['target_table'])!=target):
        raise Refused('APPEND needs exact complete known source/target epochs')
    response=submits[0]['response']['response']
    observed=next(iter(response))
    if observed not in ('skip_execution','ready_to_execute'):raise Refused('unexpected copy/defer response')
    decisions(calls,expected or observed)
    if observed=='skip_execution' and any(r['method']=='ConfirmExecution' for r in calls):
        raise Refused('reuse unexpectedly confirmed execution')
    if any(r['method'] in ('RecordExecutions','RegisterClone') for r in calls):
        raise Refused('ordinary APPEND unexpectedly recorded/copied')
    return submits,observed


# Reuse only the evidenced native-materialization footprint auditor and the
# established capture/Explain/reference plumbing; APPEND history remains separate.
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

from databricks_baseline import LeaseScope
from databricks_merge import Merge
from databricks_readonly_reference import show_reference
from databricks_survey import Campaign, Scope, check_manifest


class AppendScope(LeaseScope):
    profile_roles=('prod',)

    def __init__(self,run,clearance):
        Scope.__init__(self,run)
        self.roles={'prod':clearance['schema']}
        self.schemas=set(self.roles.values())
        self.relation_prefix='a_'+run+'_'
        self.permanent={f'cdo_demo_db.{self.schema("prod")}.{self.relation_prefix}{n}' for n in ('inputs','consumer')}
        self.auxiliary={f'cdo_demo_db.{self.schema("prod")}.{self.relation_prefix}consumer{s}' for s in ('__dbt_stg','__dbt_backup')}
        self.temporary=self.relation_prefix+'consumer__dbt_tmp'
        expected={f'cdo_demo_db.{self.schema("prod")}.{n}' for n in clearance['exact_permanent_names'] if n.startswith(self.relation_prefix)}
        auxiliary={f'cdo_demo_db.{self.schema("prod")}.{n}' for n in clearance['auxiliary_allowlist'].get(self.relation_prefix+'consumer',[])}
        if (self.permanent != expected or self.auxiliary != auxiliary
                or self.temporary not in clearance['session_only_unqualified_temp_names']):
            raise Refused('APPEND names differ from exact clearance')
        self.allowed=self.permanent|self.auxiliary


class Append(Merge):
    entrypoint=Path(__file__)

    def __init__(self,args):
        clearance=json.loads(Path(args.live_clearance).read_text())
        if (not clearance['status'].startswith('LIVE CLEARED')
                or clearance['catalog']!='cdo_demo_db'
                or clearance['max_final_permanent']!=4
                or clearance['max_extra_concurrent_persistent']!=1
                or clearance['limits']['managed_per_backend']<11):
            raise Refused('APPEND clearance outside reviewed bounded footprint')
        phase='rust' if args.rust_url else 'hosted'
        runs=[re.fullmatch(r'a_(\d{8}[a-f0-9]{6})_inputs',name).group(1)
              for name in clearance['exact_permanent_names'] if name.endswith('_inputs')]
        if args.run_id != runs[1 if phase=='rust' else 0]:raise Refused('wrong cleared backend run')
        if args.rust_url and not re.fullmatch(r'http://127\.0\.0\.1:\d+',args.rust_url):
            raise Refused('Rust endpoint must be owned literal loopback')
        allocation=clearance['allocation'];receipt=Path(allocation['receipt'])
        if hashlib.sha256(receipt.read_bytes()).hexdigest()!=allocation['receipt_sha256']:
            raise Refused('allocation evidence digest changed')
        self.clearance=clearance;self.phase=phase;self.lease=clearance
        self.query_identity=args.query_identity or args.run_id
        Scope(self.query_identity)
        Campaign.__init__(self,args,AppendScope(args.run_id,clearance))
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            profile['survey']['outputs'][role]={k:v for k,v in output.items() if k in {'type','host','http_path','catalog','schema','token','threads'}}
        self.save('profiles/profiles.yml',profile)
        self.save('live-clearance.json',clearance)
        self.save('source-provenance.json',{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('databricks_*.py')})
        self.save('campaign-entrypoint.py',self.entrypoint.read_text())
        self.save('operation-manifest.json',{'scope':'bounded independent #146 APPEND partial',
          'expected':APPEND_HISTORY,'managed_runs':11,'reference_objects':0,'whole_ticket_complete':False,
          'not_covered':['supported settings/schema evolution','strategy/node-kind transitions','generated lifecycle/restart'],
          'full_refresh_path':'discover actual pinned client wire, no assumed Record/FULL'})
        self.save('footprint.json',{'permanent':sorted(self.scope.permanent),'auxiliary':sorted(self.scope.auxiliary),
          'session_only_unqualified':self.scope.temporary,'max_concurrent_extra_persistent':1,'cleanup':False})

    def init_schemas(self):
        schema=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+self.scope.schema('prod'))
        allocation=self.clearance['allocation']
        if (schema.get('schema_id')!=allocation['schema_id'] or schema.get('owner')!=allocation['owner']
                or schema.get('created_at')!=allocation['created_at']):
            raise Refused('fresh allocation UUID/owner/created_at changed')
        inventory=self.inventory();objects=inventory[self.scope.schema('prod')]
        prior=set() if self.phase=='hosted' else {n.replace('b146bb','a146aa') for n in self.scope.permanent}
        if {r['full_name'] for r in objects}!=prior:raise Refused('allocation not exact expected empty/hosted-only inventory')
        self.before_inventory=inventory
        self.save('allocation-identity-before.json',schema);self.save('lease-inventory-before.json',inventory)

    def checked_results(self,label,stage):
        case=APPEND_HISTORY[stage]
        self.save('expected/'+label+'.json',{'stage':stage,**case,'basis':'independent literal cumulative operation history'})
        actual=self.observe(label,self.scope.relation('prod','consumer'))
        props=json.loads((self.work/'physical'/f'{label}_properties.json').read_text())
        assert_append_result(actual,props,case['R'])
        sql=literal_reference(case['R']);direct=self.sql(label+'_cumulative_R_literal',sql)
        assert_rows(direct,case['R'])
        typed=[[int(r[0]),None if r[1] is None else int(r[1]),r[2]] for r in case['R']]
        show_reference(self,label,sql,typed,direct)
        selected=self.sql(label+'_selected_F_literal',literal_reference(case['F']))
        assert_rows(selected,case['F'])
        amount='cast(amount+100 as int)' if stage in ('projection','refresh','next_ordinary') else 'amount'
        fresh=self.sql(label+'_selected_F_source',f'SELECT id,{amount} as amount,tag FROM '+self.scope.relation('prod','inputs'))
        assert_rows(fresh,case['F'])
        after=self.inventory()
        for schema,previous in self.before_inventory.items():
            if any(row not in after[schema] for row in previous):raise Refused('preexisting object changed')
            if any(row['full_name'] not in self.scope.permanent for row in after[schema] if row not in previous):
                raise Refused('unplanned final physical object')
        self.save('inventory/'+label+'.json',after)
        self.save('checks/'+label+'.json',{'complete_typed_R_and_F':True,'zero_RPC_disabled_R_reference':True,
          'managed_delta':True,'stage':stage,'R_count':len(case['R']),'F_count':len(case['F'])})

    def invocation(self,label,stage,expected=None,refresh=False):
        self.save('expected/'+label+'.json',{'stage':stage,**APPEND_HISTORY[stage]})
        calls=self.dbt(label,['run','--select','consumer']+(['--full-refresh'] if refresh else []))
        self.audit_native(label,label!='first')
        if refresh:
            if any(r['method']=='RegisterClone' for r in calls):raise Refused('refresh unexpectedly copied')
            self.save('full-refresh-wire.json',calls)
            submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
            if submits:self.explain(label,submits)
        else:
            source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
            submits,observed=append_submit(calls,source,target,label!='first',expected)
            request=submits[0]['request'];evidence={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in request['tables']}
            if label in ('upsert','delete') and evidence[source]<=self.previous_epochs[source]:
                raise Refused('changed source epoch failed to advance; #140 remains unresolved')
            if observed=='skip_execution':
                if request['sql']!=self.previous_sql:raise Refused('reuse control changed rendered SQL')
                runtime=submits[0]['response']['response'][observed]['execution_runtime_ms']
                if runtime!=self.last_runtime:raise Refused('SKIP runtime differs from own confirmed APPEND')
            else:
                confirms=[r for r in calls if r['method']=='ConfirmExecution' and r.get('response',{}).get('success')]
                if len(confirms)!=1:raise Refused('APPEND requires exactly one successful Confirm')
                self.last_runtime=confirms[0]['request']['execution_runtime_ms']
                if label!='first':
                    query=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
                    if not re.search(r'(?is)insert\s+into\b.*?\bselect\b',query):raise Refused('ordinary APPEND lacks actual INSERT SELECT')
            self.previous_epochs=evidence;self.previous_sql=request['sql']
            self.explain(label,submits)
            self.save('epochs/'+label+'.json',{'tables':request['tables'],'observed':observed})
        self.checked_results(label,stage)

    def baseline(self):
        self.setup_project();source=self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'append_history','database':'cdo_demo_db',
          'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        config="materialized='incremental',incremental_strategy='append',unique_key='id',file_format='delta',on_schema_change='ignore',state={'lag_tolerance':'0s','pre_clone':'never'}"
        def model(projected=False):
            self.write_model('consumer',config,"select id, "+('cast(amount+100 as int)' if projected else 'amount')+" as amount, tag from {{ source('append_history','inputs') }}")
        model();self.dbt('inventory',['parse'],managed=False)
        manifest=json.loads(next((self.work/'targets').glob('*_inventory/manifest.json')).read_text());check_manifest(self.scope,manifest)
        node=next(v for v in manifest['nodes'].values() if v['name']=='consumer')
        if (node.get('language')!='sql' or node['config']['incremental_strategy']!='append'
                or node['config'].get('pre-hook') or node['config'].get('post-hook')):
            raise Refused('manifest outside pinned SQL APPEND scope')
        for node in [*manifest.get('nodes',{}).values(),*manifest.get('sources',{}).values()]:
            self.scope.check('.'.join([node['database'],node['schema'],node.get('alias') or node['identifier']]))
        self.save('actual-default-configuration.json',{'node':node,'metadata':manifest['metadata'],'feature_overrides':False,'macros_overridden':False})
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('initial_source',f"INSERT INTO {source} VALUES (1,10,'same'),(1,10,'same'),(2,20,'old'),(3,NULL,NULL)")
        self.invocation('first','first','ready_to_execute');self.invocation('unchanged','first','skip_execution')
        self.sql('update_source',f"UPDATE {source} SET amount=11,tag='changed' WHERE id=1")
        self.sql('insert_source',f"INSERT INTO {source} VALUES (4,40,'new'),(4,40,'new')")
        self.invocation('upsert','upsert','ready_to_execute');self.invocation('upsert_repeat','upsert','skip_execution')
        self.sql('delete_source',f'DELETE FROM {source} WHERE id=2')
        self.invocation('delete','delete','ready_to_execute');self.invocation('delete_repeat','delete','skip_execution')
        model(True);self.invocation('projection','projection','ready_to_execute');self.invocation('projection_repeat','projection','skip_execution')
        self.invocation('full_refresh','refresh',refresh=True)
        self.invocation('next_ordinary','next_ordinary','ready_to_execute');self.invocation('next_repeat','next_ordinary','skip_execution')
        return {'managed_runs':11,'objects':2,'whole_ticket_complete':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','live-clearance'):parser.add_argument('--'+key,required=True)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--query-identity');parser.add_argument('--rust-url');parser.add_argument('--run-id',required=True)
    return Append(parser.parse_args()).run()


if __name__=='__main__':raise SystemExit(main())
