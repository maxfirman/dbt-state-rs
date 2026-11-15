#!/usr/bin/env python3
"""#153 ordinary-submit copy discovery: pinned native dbt, never RegisterClone.

Shares #152's physical bag, properties, independent reference and executed-DDL
checks. The exact plan must be cleared before any API or native command.
"""
import argparse
import json
from pathlib import Path
import re
import uuid

from databricks_baseline import Baseline, LeaseScope
from databricks_precopy import Precopy, SCHEMAS, SCHEMA_IDS, FIRST, CHANGED, digest
from databricks_snapshot_oracle import assert_source, source_projection_sql
from databricks_snapshots import literal_source
from databricks_survey import Campaign, Scope, Refused, check_manifest

OBJECTS={'hosted':'80615b233ac140e48993d85c06bfc823','rust':'038ae03354ba416883666b11c796a13d'}
RUNS={'hosted':'20261115e153aa','rust':'20261115f153bb'}
# Source refresh precedes overwrite, so no ordinary FULL rebuild of a clone.
CELLS=(('source','prod','first','ready_to_execute'),
       ('absent','dev','first','ready_to_clone'),
       ('repeat','dev','first','skip_execution'),
       ('source_changed','prod','changed','ready_to_execute'),
       ('existing','dev','changed','ready_to_clone'),
       ('existing_repeat','dev','changed','skip_execution'))


def plan():
    return {'ticket':153,'catalog':'cdo_demo_db','schemas':SCHEMAS,'schema_ids':SCHEMA_IDS,
        'run_ids':RUNS,'objects':OBJECTS,'cells':CELLS,
        'source':{'first':FIRST,'changed':CHANGED},
        'names':{phase:[f'cdo_demo_db.{SCHEMAS[role]}.p153_{oid}_{name}'
            for role,names in (('prod',('inputs','full','merge')),('dev',('full','merge')))
            for name in names] for phase,oid in OBJECTS.items()},
        'managed_per_backend':12,'compile_per_backend':2,'readonly_show_per_backend':12,
        'source_writes':['CREATE','INSERT','UPDATE id=1','INSERT id=3'],
        'permanent_per_backend':5,'temporary_peak':1,'new_schemas':0,'cleanup':False,
        'profile':{'prod':{'allow_clones':False,'defer_to_target':None},
                   'dev':{'allow_clones':True,'defer_to_target':'prod'}},
        'state':{'pre_clone':'never','lag_tolerance':'0s'},
        'scope':'FULL and MERGE; exact SELECT-star single current physical input; quiescent Delta',
        'expected_unproven':'Both absent/existing SQL-copy grants are hypotheses; stop and retain any different observation.',
        'excluded':['RegisterClone','chains','physical concurrent source changes',
                    'ordinary FULL rebuild of cloned relation','snapshots','general SQL/templates/dependencies']}


class SubmitScope(LeaseScope):
    profile_roles=('prod','dev')

    def __init__(self,run,lease,phase):
        Scope.__init__(self,run)
        if run!=RUNS[phase]:raise Refused('submit-copy:exact-run')
        for value in OBJECTS.values():
            if len(value)!=32 or uuid.UUID(value).hex!=value or uuid.UUID(value).version!=4:
                raise Refused('submit-copy:UUID4')
        owned=set()
        for evidence in lease['source_evidence']:
            if digest(evidence['path'])!=evidence['sha256']:raise Refused('submit-copy:ownership-digest')
            receipt=json.loads(Path(evidence['path']).read_text())
            for role,schema in SCHEMAS.items():
                if receipt.get(schema)=={'full_name':'cdo_demo_db.'+schema,'schema_id':SCHEMA_IDS[role],'owner':'max.firman@gmail.com'}:owned.add(schema)
        if not set(SCHEMAS.values())<=owned:raise Refused('submit-copy:owned-containers')
        self.roles=SCHEMAS;self.schemas=set(SCHEMAS.values())
        self.relation_prefix='p153_'+OBJECTS[phase]+'_'
        self.allowed=set(plan()['names'][phase])


class SubmitCopy(Precopy):
    def __init__(self,args):
        self.phase='rust' if args.rust_url else 'hosted'
        grant=json.loads(Path(args.live_clearance).read_text())
        self.lease=json.loads(Path(args.schema_lease).read_text())
        scope=SubmitScope(args.run_id,self.lease,self.phase)
        if (grant.get('status')!='LIVE CLEARED #153 ordinary submit-copy'
                or self.phase not in grant['authorized_phases']
                or json.loads(Path(args.operation_ledger).read_text())!=json.loads(json.dumps(plan()))
                or digest(args.operation_ledger)!=grant['operation_sha256']
                or grant['source_sha256']!={p:digest(Path(__file__).parent/p) for p in grant['source_sha256']}):
            raise Refused('submit-copy:exact-reviewed-plan-and-source')
        Campaign.__init__(self,args,scope)
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            for key in list(output):
                if key not in {'type','host','http_path','catalog','schema','token','threads'}:del output[key]
            output['threads']=1;output['allow_clones']=role=='dev'
            if role=='dev':output['defer_to_target']='prod'
        self.save('profiles/profiles.yml',profile)
        self.save('submit-copy-plan.json',plan());self.save('live-clearance.json',grant)
        self.save('submit-copy-runner.py',Path(__file__).read_text())
        self.save('binary-provenance.json',{k:{'path':str(Path(getattr(args,k)).resolve()),'sha256':digest(getattr(args,k))} for k in ('dbt','proxy','probe')})
        self.commands=[('compile_'+role,['compile'],role,False) for role in ('prod','dev')]
        for stage in ('first','changed'):
            for kind in ('full','merge'):
                self.commands.extend((kind+'_'+label,['run','--select',kind],role,True)
                    for label,role,revision,_ in CELLS if revision==stage)
        self.command_index=0;self.source_requests={};self.source_confirms={}
        self.prefix=None;self.completed=set();self.prefixes=[]

    def init_schemas(self):
        ownership={}
        for role,schema in SCHEMAS.items():
            metadata=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+schema)
            if metadata.get('schema_id')!=SCHEMA_IDS[role] or metadata.get('owner')!='max.firman@gmail.com':raise Refused('submit-copy:schema-owner')
            ownership[schema]={k:metadata[k] for k in ('full_name','schema_id','owner')}
        self.save('schema-ownership.json',ownership)
        self.before_inventory=self.inventory()
        for schema,rows in self.before_inventory.items():
            wanted={name for name in self.scope.allowed if name.split('.')[1]==schema}
            if len(rows)+len(wanted)>100:raise Refused('submit-copy:capacity')
            if any(r['full_name'] in wanted or r['name'].startswith(self.scope.relation_prefix) for r in rows):raise Refused('submit-copy:collision')
        self.save('lease-inventory-before.json',self.before_inventory)
        self.save('warehouse.json',self.sql('warehouse',"SELECT current_version() AS version,current_timezone() AS timezone"))

    def preflight(self):
        self.setup_project();(self.project/'models/sources.yml').unlink()
        source=self.scope.relation('prod','inputs')
        for kind in ('full','merge'):
            cfg="materialized='table'" if kind=='full' else "materialized='incremental',incremental_strategy='merge',unique_key='id'"
            self.save('project/models/'+kind+'.sql',"{{ config(alias='"+self.scope.relation_prefix+kind+"',"+cfg+",state={'lag_tolerance':'0s','pre_clone':'never'}) }}\nselect * from "+source+'\n')
        for role in ('prod','dev'):
            label='compile_'+role
            if self.dbt(label,['compile'],role,False):raise Refused('submit-copy:compile-State-RPC')
            manifest=json.loads((self.evidence(label)/'targets'/f'{self.n:03d}_{label}/manifest.json').read_text())
            check_manifest(self.scope,manifest)
            if {n['name'] for n in manifest['nodes'].values()}!={'full','merge'}:raise Refused('submit-copy:manifest-nodes')
            for n in manifest['nodes'].values():
                self.scope.check('.'.join((n['database'],n['schema'],n['alias'])))
                cfg=n['config']
                if (cfg.get('pre-hook') or cfg.get('post-hook') or n.get('depends_on',{}).get('nodes')
                        or cfg.get('state',{}).get('pre_clone')!='never'):raise Refused('submit-copy:manifest-contract')
            self.save('preflight/'+label+'.json',manifest)

    def invocation(self,kind,cell):
        label,role,stage,verdict=cell;label=kind+'_'+label
        calls=self.dbt(label,['run','--select',kind],role)
        if any(c['method']=='RegisterClone' for c in calls):raise Refused('submit-copy:unexpected-RegisterClone')
        if any(c.get('status',{}).get('code')!=0 for c in calls):raise Refused('submit-copy:RPC-status')
        submits=[c for c in calls if c['method']=='SubmitEnrichedSQL']
        if len(submits)!=1:raise Refused('submit-copy:one-ordinary-submit')
        row=submits[0];req=row['request'];response=row['response']['response']
        self.save('observed/'+label+'.json',{'request':req,'response':row['response']})
        if set(response)!={verdict}:raise Refused('submit-copy:observed-'+','.join(response)+'-expected-'+verdict)
        target=self.scope.relation(role,kind);source=self.scope.relation('prod',kind)
        if (req['execution_type']!=(1 if kind=='full' else 3) or req['compare_unrendered_code'] or req['query_dependencies']
                or req['target_table'].replace('`','')!=target or not req.get('table_namespace')
                or req['allow_clones']!=(role=='dev') or req['clone_chain_depth_limit']!=(1 if role=='dev' else 0)):
            raise Refused('submit-copy:actual-wire-contract')
        if re.sub(r'\s+',' ',req['sql'].replace('`','')).strip().lower()!='select * from '+self.scope.relation('prod','inputs'):
            raise Refused('submit-copy:actual-single-input')
        confirms={c['request']['request_id']:c for c in calls if c['method']=='ConfirmExecution' and c['response'].get('success') is True}
        ready=response[verdict];copy=verdict=='ready_to_clone'
        if verdict!='skip_execution' and ready['request_id'] not in confirms:raise Refused('submit-copy:missing-confirm')
        if copy:
            # SQL Submit has one ready_to_clone oneof; the duplicate v1 mirror
            # exists only on RegisterClone, which this campaign forbids.
            wanted='\n        CREATE OR REPLACE TABLE '+req['target_table']+' SHALLOW CLONE '+ready['clone_source']+'\n        '
            if (ready['clone_sqls']!=[wanted] or ready['clone_source'].replace('`','')!=source
                    or ready['clone_target']!=req['target_table']
                    or ready['clone_required_last_modified_epoch']!=self.source_confirms[kind]['last_modified_epoch']
                    or ready['execution_runtime_ms']!=self.source_confirms[kind]['execution_runtime_ms']):raise Refused('submit-copy:exact-grant-DDL-source-history')
            self.save('copies/'+label+'.json',{'request':req,'response':row['response'],'confirmation':confirms[ready['request_id']]})
        if role=='prod':
            self.source_requests[kind]=req;self.source_confirms[kind]=confirms[ready['request_id']]['request']
        elif req['dbt_node_state']['node_unique_id']!=self.source_requests[kind]['dbt_node_state']['node_unique_id']:
            raise Refused('submit-copy:fullUID')
        self.audit_native(label,target,source,copy)
        self.explain(label,submits)
        self.physical(label,kind,role,stage,role=='dev',copy)

    def baseline(self):
        self.preflight()
        source=self.scope.relation('prod','inputs')
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
        self.sql('initial_source','INSERT INTO '+source+' VALUES '+literal_source(FIRST))
        for stage in ('first','changed'):
            if stage=='changed':
                row=CHANGED[0]
                self.sql('change_source',f"UPDATE {source} SET v='{row[1]}',p=DATE '{row[2]}',ts=TIMESTAMP '{row[3]}' WHERE id=1")
                self.sql('insert_source','INSERT INTO '+source+' VALUES '+literal_source((CHANGED[2],)))
                assert_source(self.sql('changed_source',source_projection_sql(source)),CHANGED)
            for kind in ('full','merge'):
                for cell in CELLS:
                    if cell[2]==stage:self.invocation(kind,cell)
        if self.command_index!=len(self.commands):raise Refused('submit-copy:incomplete-command-budget')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',action='store_true')
    for key in ('dbt','proxy','probe','work-dir','schema-lease','live-clearance','operation-ledger','run-id'):parser.add_argument('--'+key)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--rust-url')
    args=parser.parse_args()
    if args.plan:print(json.dumps(plan(),indent=2));return 0
    return SubmitCopy(args).run()


if __name__=='__main__':raise SystemExit(main())
