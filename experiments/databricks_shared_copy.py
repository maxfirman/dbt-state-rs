#!/usr/bin/env python3
"""Focused #186 shared-analysis acceptance using the existing copy physical oracle.

Exact ledger/source clearance precedes all warehouse mutations. No continuation,
cleanup, schema creation, extra persistent reference or production clock override.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
from types import SimpleNamespace
import uuid

from databricks_baseline import Baseline, LeaseScope
from databricks_copy_recovery import direct_grant, guarded_metadata
from databricks_precopy import Precopy, SCHEMAS, SCHEMA_IDS, digest
from databricks_snapshot_oracle import (Ledger, NATIVE_SCHEMA, assert_source,
    check_schema, source_projection_sql)
from databricks_snapshots import literal_source
from databricks_submit_copy import SubmitCopy
from databricks_survey import Campaign, Scope, Refused, check_manifest

OBJECTS={'hosted':'22a7ea446d1d4b82a8156acd21c113f7','rust':'e2db6b9218cf47d789eebf1594ef42cf'}
RUNS={'hosted':'20261115a186aa','rust':'20261115b186bb'}
FIRST=(('1','one','2026-11-01','2026-11-01 12:00:00.123450'),
       ('2',None,'2026-11-02','2026-11-02 12:00:00.654321'))
CHANGED=(('1','changed','2026-11-01','2026-11-03 12:00:00.000001'),FIRST[1])
SOURCE_SCHEMA=(('id','INT','INT',0),)+NATIVE_SCHEMA[1:4]
FULL_SCHEMA=(('id','BIGINT','LONG',0),('n','BIGINT','LONG',1))
KINDS=('full','merge','check')
MODES=('copy','repair','refusal')
# stage, role, kinds, mode. Exactly fifteen managed commands per backend.
CELLS=(('source','prod',KINDS,'copy'),('copy','dev',KINDS,'copy'),
       ('repeat','dev',KINDS,'copy'),('changed','dev',('merge','check'),'repair'),
       ('changed_repeat','dev',('merge','check'),'repair'),
       ('source_modified','dev',('merge',),'refusal'),('refusal_repeat','dev',('merge',),'refusal'))


def queries(scope):
    a=scope.relation('prod','a');b=scope.relation('prod','b')
    return {
        'full':f'WITH ca AS (SELECT system.builtin.count(*) AS id FROM {a}), cb AS (SELECT system.builtin.count(b.id) AS n FROM {b} AS b(id)) SELECT ca.id AS id,cb.n AS n FROM ca LEFT JOIN cb ON ca.id = cb.n',
        'merge':f'WITH base AS (SELECT a.id AS id,a.v AS v,a.p AS p,a.ts AS ts FROM {a} AS a(id,v,p,ts) WHERE a.id IS NOT NULL), stats AS (SELECT system.builtin.count(b.id) AS n FROM {b} AS b(id)) SELECT base.id AS id,base.v AS v,base.p AS p,base.ts AS ts FROM base INNER JOIN stats ON stats.n > 0',
        'check':f'WITH base AS (SELECT a.id AS id,a.v AS v,a.p AS p,a.ts AS ts FROM {a} AS a(id,v,p,ts) WHERE a.id IS NOT NULL) SELECT base.id AS id,base.v AS v,base.p AS p,base.ts AS ts FROM base'}


def expected(kind,stage):
    if kind=='full':return (('2','2'),)
    rows=FIRST if stage in ('source','copy','repeat') else CHANGED
    if kind=='merge':return rows
    ledger=Ledger('check','ignore',source_updated_at=True).execute(FIRST,'source')
    if rows==CHANGED:ledger=ledger.execute(CHANGED,'changed')
    return ledger.rows({})


def sql_contract(phase):
    scope=SimpleNamespace(relation=lambda role,name:f'cdo_demo_db.{SCHEMAS[role]}.p186_{OBJECTS[phase]}_{name}')
    a=scope.relation('prod','a');b=scope.relation('prod','b')
    return {'models':queries(scope),'writes':[
        ('create_a',f'CREATE TABLE {a} (id INT,v STRING,p DATE,ts TIMESTAMP) USING DELTA'),
        ('insert_a','INSERT INTO '+a+' VALUES '+literal_source(FIRST)),
        ('create_b',f'CREATE TABLE {b} (id INT) USING DELTA'),
        ('insert_b','INSERT INTO '+b+' VALUES (7),(7),(NULL)'),
        ('change_a',f"UPDATE {a} SET v='changed',ts=TIMESTAMP '2026-11-03 12:00:00.000001' WHERE id=1"),
        ('sentinel',"UPDATE "+scope.relation('prod','merge')+" SET v='sentinel_wrong' WHERE id=1"),
        ('drop_dev_merge','DROP TABLE '+scope.relation('dev','merge'))]}


def plan():
    return {'ticket':186,'catalog':'cdo_demo_db','schemas':SCHEMAS,'schema_ids':SCHEMA_IDS,
        'run_ids':RUNS,'objects':OBJECTS,'sql_by_backend':{p:sql_contract(p) for p in OBJECTS},'cells':CELLS,'source':{'first':FIRST,'changed':CHANGED,'b':[['7'],['7'],[None]]},
        'names':{phase:[f'cdo_demo_db.{SCHEMAS[role]}.p186_{oid}_{name}'
            for role,names in (('prod',('a','b',*KINDS)),('dev',KINDS)) for name in names]
            for phase,oid in OBJECTS.items()},
        'managed_per_backend':15,'compile_per_backend':6,'readonly_show_per_backend':15,
        'input_create_insert':4,'guarded_updates':2,'guarded_drops':1,
        'permanent_per_backend':8,'temporary_peak':1,'new_schemas':0,'cleanup':False,
        'schemas_expected':{'source':SOURCE_SCHEMA,'full':FULL_SCHEMA,'check':SOURCE_SCHEMA+NATIVE_SCHEMA[4:]},
        'modes':MODES,'scope':'qualified COUNT CTE LEFT JOIN FULL; filtered CTE COUNT INNER JOIN MERGE; source-clock check snapshot; changed-source refusal',
        'excluded':['FULL ordinary replacement','chains','new snapshot modes','races','producer graph expansion','arbitrary SQL']}


class SharedScope(LeaseScope):
    profile_roles=('prod','dev')

    def __init__(self,run,lease,phase):
        Scope.__init__(self,run)
        if run!=RUNS[phase] or any(uuid.UUID(v).version!=4 or uuid.UUID(v).hex!=v for v in OBJECTS.values()):
            raise Refused('shared-copy:exact-UUID4-run')
        owned=set()
        for evidence in lease['source_evidence']:
            if digest(evidence['path'])!=evidence['sha256']:raise Refused('shared-copy:ownership-digest')
            receipt=json.loads(Path(evidence['path']).read_text())
            for role,schema in SCHEMAS.items():
                if receipt.get(schema)=={'full_name':'cdo_demo_db.'+schema,'schema_id':SCHEMA_IDS[role],'owner':'max.firman@gmail.com'}:owned.add(schema)
        if owned!=set(SCHEMAS.values()):raise Refused('shared-copy:owned-containers')
        self.roles=SCHEMAS;self.schemas=set(SCHEMAS.values())
        self.relation_prefix='p186_'+OBJECTS[phase]+'_';self.allowed=set(plan()['names'][phase])


class SharedCopy(Precopy):
    def __init__(self,args):
        self.phase='rust' if args.rust_url else 'hosted'
        grant=json.loads(Path(args.live_clearance).read_text())
        self.lease=json.loads(Path(args.schema_lease).read_text())
        scope=SharedScope(args.run_id,self.lease,self.phase)
        if (grant.get('status')!='LIVE CLEARED #186 shared-copy' or self.phase not in grant['authorized_phases']
                or json.loads(Path(args.operation_ledger).read_text())!=json.loads(json.dumps(plan()))
                or digest(args.operation_ledger)!=grant['operation_sha256']
                or grant['source_sha256']!={p:digest(Path(__file__).parent/p) for p in grant['source_sha256']}
                or not {'databricks_shared_copy.py','databricks_precopy.py','databricks_snapshot_oracle.py'}<=set(grant['source_sha256'])):
            raise Refused('shared-copy:exact-reviewed-plan-source')
        for key in ('dbt','proxy','probe'):
            if digest(getattr(args,key))!=grant['tools_sha256'][key]:raise Refused('shared-copy:tool-digest')
        Campaign.__init__(self,args,scope)
        self.save('shared-copy-plan.json',plan());self.save('live-clearance.json',grant)
        self.save('binary-provenance.json',{k:{'path':str(Path(getattr(args,k)).resolve()),'sha256':digest(getattr(args,k))} for k in ('dbt','proxy','probe')})
        self.commands=[('compile_'+mode+'_'+role,['compile'],role,False) for mode in MODES for role in ('prod','dev')]
        self.commands += [(kind+'_'+stage,['snapshot' if kind=='check' else 'run','--select',kind],role,True)
            for stage,role,kinds,mode in CELLS for kind in kinds]
        self.command_index=0;self.completed=set();self.prefixes=[];self.prefix=None
        self.confirmed={};self.source_requests={};self.shallow=set();self.created_ids={};self.expected={}
        self.mutations=0;self.write_index=0;self.namespace=None;self.sql_queries=queries(scope)
        self.save('sql-contract.json',self.sql_queries)
        self.save('source-attribution.json',{'commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parent,text=True).strip(),
            'dirty':subprocess.check_output(['git','status','--porcelain'],cwd=Path(__file__).parent,text=True),
            'source_sha256':grant['source_sha256']})

    init_schemas=SubmitCopy.init_schemas

    def sql(self,label,statement):
        if re.match(r'(?i)^\s*(CREATE|INSERT|UPDATE|DROP|ALTER|DELETE|MERGE)\b',statement):
            writes=sql_contract(self.phase)['writes']
            if self.write_index>=len(writes) or (label,statement)!=writes[self.write_index]:raise Refused('shared-copy:exact-ordered-SQL-write-budget')
            self.write_index+=1
        return Campaign.sql(self,label,statement)

    def configure(self,mode):
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            for key in list(output):
                if key not in {'type','host','http_path','catalog','schema','token','threads'}:del output[key]
            output['threads']=1;output['allow_clones']=role=='dev' and mode!='repair'
            if output['allow_clones']:output['defer_to_target']='prod'
        self.save('profiles/profiles.yml',profile)
        for kind,query in self.sql_queries.items():
            cfg={'full':"materialized='table'",'merge':"materialized='incremental',incremental_strategy='merge',unique_key='id'",
                 'check':"strategy='check',check_cols=['v'],unique_key='id',updated_at='ts',hard_deletes='ignore'"}[kind]
            state="{'lag_tolerance':'0s'"+(" ,'pre_clone':'never'" if kind=='full' or mode=='repair' else '')+'}'
            folder='snapshots' if kind=='check' else 'models'
            text="{{ config(alias='"+self.scope.relation_prefix+kind+"',"+cfg+",state="+state+") }}\n"+query+'\n'
            if kind=='check':text='{% snapshot check %}\n'+text+'{% endsnapshot %}\n'
            self.save('project/'+folder+'/'+kind+'.sql',text)

    def preflight(self):
        self.setup_project();(self.project/'models/sources.yml').unlink()
        for mode in MODES:
            self.configure(mode)
            for role in ('prod','dev'):
                label='compile_'+mode+'_'+role
                if self.dbt(label,['compile'],role,False):raise Refused('shared-copy:compile-State-RPC')
                manifest=json.loads((self.work/'targets'/f'{self.n:03d}_{label}/manifest.json').read_text())
                check_manifest(self.scope,manifest)
                if {n['name'] for n in manifest['nodes'].values()}!=set(KINDS):raise Refused('shared-copy:manifest-nodes')
                for n in manifest['nodes'].values():
                    self.scope.check('.'.join((n['database'],n['schema'],n['alias'])))
                    cfg=n['config'];wanted='never' if n['name']=='full' or mode=='repair' else None
                    if (cfg.get('pre-hook') or cfg.get('post-hook') or n.get('depends_on',{}).get('nodes')
                            or cfg.get('state',{}).get('pre_clone')!=wanted
                            or n['compiled_code'].strip()!=self.sql_queries[n['name']]):raise Refused('shared-copy:compiled-contract')
                    if n['name']=='check' and (cfg.get('updated_at')!='ts' or cfg.get('check_cols')!=['v'] or cfg.get('hard_deletes')!='ignore'):raise Refused('shared-copy:compiled-snapshot-config')
                self.save('preflight/'+label+'.json',manifest)

    def remember(self,label,target,rows):
        meta=self.api('get','/api/2.1/unity-catalog/tables/'+target)
        table_id=guarded_metadata(meta,target)
        if target in self.created_ids and self.created_ids[target]!=table_id:raise Refused('shared-copy:unexpected-replacement-ID')
        self.created_ids[target]=table_id;self.expected[target]=rows
        self.save('physical/'+label+'_created_ID.json',{'table_id':table_id,'full_metadata':meta})

    def mutate(self,label,target,operation):
        allowed=(('change_a',self.scope.relation('prod','a'),'update_a'),
                 ('sentinel',self.scope.relation('prod','merge'),'sentinel'),
                 ('drop_dev_merge',self.scope.relation('dev','merge'),'drop'))
        if self.mutations>=len(allowed) or (label,target,operation)!=allowed[self.mutations]:raise Refused('shared-copy:ordered-mutation-budget')
        meta=self.api('get','/api/2.1/unity-catalog/tables/'+target)
        table_id=guarded_metadata(meta,target)
        if self.created_ids.get(target)!=table_id:raise Refused('shared-copy:mutation-created-ID')
        before={'metadata':meta,'bag':self.observe(label+'_before',target),
                'properties':self.sql(label+'_properties','SHOW TBLPROPERTIES '+target),
                'history':self.sql(label+'_history','DESCRIBE HISTORY '+target)}
        assert_source(self.sql(label+'_lossless',source_projection_sql(target)),self.expected[target],SOURCE_SCHEMA)
        self.save('mutations/'+label+'_before.json',before)
        if guarded_metadata(self.api('get','/api/2.1/unity-catalog/tables/'+target),target)!=table_id:raise Refused('shared-copy:mutation-recheck-ID')
        self.mutations+=1
        if operation=='drop':
            self.sql(label,'DROP TABLE '+target);self.shallow.discard(target)
            del self.created_ids[target];del self.expected[target]
            inventory=self.inventory()
            if any(r['full_name']==target for rows in inventory.values() for r in rows):raise Refused('shared-copy:drop-still-present')
            self.save('mutations/'+label+'_after.json',{'absent':target,'inventory':inventory});return
        sql="SET v='changed',ts=TIMESTAMP '2026-11-03 12:00:00.000001' WHERE id=1" if operation=='update_a' else "SET v='sentinel_wrong' WHERE id=1"
        self.sql(label,'UPDATE '+target+' '+sql)
        rows=CHANGED if operation=='update_a' else (('1','sentinel_wrong',*FIRST[0][2:]),FIRST[1])
        assert_source(self.sql(label+'_after_lossless',source_projection_sql(target)),rows,SOURCE_SCHEMA)
        after_props=self.sql(label+'_after_properties','SHOW TBLPROPERTIES '+target)
        if dict(after_props['rows'])!=dict(before['properties']['rows']):raise Refused('shared-copy:update-properties')
        if guarded_metadata(self.api('get','/api/2.1/unity-catalog/tables/'+target),target)!=table_id:raise Refused('shared-copy:update-ID')
        self.expected[target]=rows
        self.save('mutations/'+label+'_after.json',{'rows':rows,'properties':after_props,'history':self.sql(label+'_after_history','DESCRIBE HISTORY '+target)})

    def invocation(self,kind,stage,role,mode):
        self.configure(mode);label=kind+'_'+stage
        target=self.scope.relation(role,kind);source=self.scope.relation('prod',kind)
        calls=self.dbt(label,['snapshot' if kind=='check' else 'run','--select',kind],role)
        if any(c.get('status',{}).get('code')!=0 for c in calls):raise Refused('shared-copy:RPC-status')
        submits=[c for c in calls if c['method']=='SubmitEnrichedSQL']
        if len(submits)!=1:raise Refused('shared-copy:one-submit')
        row=submits[0];req=row['request'];response=row['response']['response'];verdict=next(iter(response))
        norm=lambda s:re.sub(r'\s+',' ',s.replace('`','')).strip().lower()
        if (req['target_table'].replace('`','')!=target or req['execution_type']!={'full':1,'merge':3,'check':7}[kind]
                or req['compare_unrendered_code'] or req['query_dependencies'] or not req.get('table_namespace')
                or norm(req['sql'])!=norm(self.sql_queries[kind])):raise Refused('shared-copy:actual-SQL-scope-kind')
        allow=role=='dev' and mode!='repair'
        if req['allow_clones']!=allow or req['clone_chain_depth_limit']!=int(allow):raise Refused('shared-copy:actual-copy-policy')
        if self.namespace and req['table_namespace']!=self.namespace:raise Refused('shared-copy:namespace')
        inputs={self.scope.relation('prod','a')}
        if kind!='check':inputs.add(self.scope.relation('prod','b'))
        tables={t['name'].replace('`',''):t['last_modified_epoch'] for t in req['tables']}
        if (not inputs<=set(tables) or set(tables)-inputs-{target}
                or any(not tables[n] for n in inputs)):raise Refused('shared-copy:actual-positive-input-epochs')
        self.namespace=req['table_namespace']
        if role=='prod':self.source_requests[kind]=req
        elif req['dbt_node_state']['node_unique_id']!=self.source_requests[kind]['dbt_node_state']['node_unique_id']:raise Refused('shared-copy:UID')
        confirms={c['request']['request_id']:c['request'] for c in calls if c['method']=='ConfirmExecution' and c['response'].get('success') is True}
        direct=[c for c in calls if c['method']=='RegisterClone']
        grants=[g for c in direct if (g:=direct_grant(c['response']))]
        if verdict=='ready_to_clone':grants.append(response[verdict])
        for c in direct:
            cr=c['request']
            if (kind=='full' or cr['clone_source_table'].replace('`','')!=source or cr['target_table'].replace('`','')!=target
                    or cr['table_namespace']!=self.namespace or cr['labels']['dbt_node_unique_id']!=req['dbt_node_state']['node_unique_id']):raise Refused('shared-copy:direct-scope')
            epoch=cr['clone_source_last_modified_epoch']
            if not epoch or (epoch==self.confirmed[source]['last_modified_epoch'])==(stage=='source_modified'):raise Refused('shared-copy:source-epoch')
        for g in grants:
            if (g['clone_source'].replace('`','')!=source or g['clone_target'].replace('`','')!=target
                    or g['clone_sqls']!=['\n        CREATE OR REPLACE TABLE '+g['clone_target']+' SHALLOW CLONE '+g['clone_source']+'\n        ']):raise Refused('shared-copy:grant-DDL')
        if any(c['failed_to_clone'] for c in confirms.values()):raise Refused('shared-copy:unexpected-failed-copy')
        successful=[g for g in grants if g['request_id'] in confirms and not confirms[g['request_id']]['failed_to_clone']]
        expected_ids={g['request_id'] for g in successful}
        if verdict!='skip_execution':expected_ids.add(response[verdict]['request_id'])
        if set(confirms)!=expected_ids:raise Refused('shared-copy:extra-or-missing-confirm')
        if kind=='full' and grants:
            g=grants[0]
            if (g['clone_required_last_modified_epoch']!=self.confirmed[source]['last_modified_epoch']
                    or g['execution_runtime_ms']!=self.confirmed[source]['execution_runtime_ms']):raise Refused('shared-copy:automatic-source-runtime-epoch')
        if verdict!='skip_execution' and response[verdict]['request_id'] not in confirms:raise Refused('shared-copy:ordinary-confirm')
        if stage=='copy':
            if len(successful)!=1 or len(direct)!=int(kind!='full') or verdict!=('ready_to_clone' if kind=='full' else 'skip_execution'):raise Refused('shared-copy:copy-adoption')
        elif stage.endswith('repeat') or stage=='repeat':
            if verdict!='skip_execution' or grants or direct or response[verdict]['execution_runtime_ms']!=self.confirmed[target]['execution_runtime_ms']:raise Refused('shared-copy:own-repeat-runtime')
        elif stage=='source_modified':
            if len(direct)!=1 or verdict!='ready_to_execute' or (self.phase=='rust' and grants):raise Refused('shared-copy:modified-source-refusal')
        elif verdict!='ready_to_execute' or grants or direct:raise Refused('shared-copy:ordinary-build')
        if successful:self.shallow.add(target)
        repaired=bool(successful and direct and verdict=='ready_to_execute')
        self.save('observed/'+label+'.json',{'calls':calls,'verdict':verdict,'successful_copies':len(successful),'repair_after_copy':repaired})
        self.audit_native(label,target,source,bool(successful));self.explain(label,submits)
        schema=FULL_SCHEMA if kind=='full' else SOURCE_SCHEMA+NATIVE_SCHEMA[4:] if kind=='check' else SOURCE_SCHEMA
        rows=expected(kind,stage)
        self.physical(label,kind,role,'first',target in self.shallow,bool(successful),copy_repaired=repaired,expected_rows=rows,native_schema=schema)
        self.remember(label,target,rows)
        if verdict!='skip_execution' or successful:
            pending=response[verdict]['request_id'] if verdict!='skip_execution' else successful[0]['request_id']
            retained=dict(confirms[pending])
            if successful and retained['execution_runtime_ms'] is None:retained['execution_runtime_ms']=self.confirmed[source]['execution_runtime_ms']
            self.confirmed[target]=retained

    def baseline(self):
        self.preflight()
        for name,ddl,values in (('a','id INT,v STRING,p DATE,ts TIMESTAMP',literal_source(FIRST)),('b','id INT','(7),(7),(NULL)')):
            target=self.scope.relation('prod',name)
            self.sql('create_'+name,f'CREATE TABLE {target} ({ddl}) USING DELTA')
            self.sql('insert_'+name,'INSERT INTO '+target+' VALUES '+values)
            if name=='a':
                assert_source(self.sql('initial_a',source_projection_sql(target)),FIRST,SOURCE_SCHEMA)
                self.remember('initial_a',target,FIRST)
            else:
                raw=self.observe('initial_b',target);check_schema(raw,(('id','INT','INT',0),))
                if sorted(raw['rows'],key=repr)!=sorted([['7'],['7'],[None]],key=repr):raise Refused('shared-copy:B-bag')
        for stage,role,kinds,mode in CELLS:
            if stage=='changed':self.mutate('change_a',self.scope.relation('prod','a'),'update_a')
            if stage=='source_modified':
                self.mutate('sentinel',self.scope.relation('prod','merge'),'sentinel')
                self.mutate('drop_dev_merge',self.scope.relation('dev','merge'),'drop')
            for kind in kinds:self.invocation(kind,stage,role,mode)
        if self.command_index!=len(self.commands) or self.mutations!=3 or self.write_index!=7:raise Refused('shared-copy:complete-budget')


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--plan',action='store_true')
    for key in ('dbt','proxy','probe','work-dir','schema-lease','live-clearance','operation-ledger','run-id'):parser.add_argument('--'+key)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));parser.add_argument('--profile',default='databricks')
    parser.add_argument('--warehouse',default='47fd3fa312624d91');parser.add_argument('--rust-url')
    args=parser.parse_args()
    if args.plan:print(json.dumps(plan(),indent=2));return 0
    return SharedCopy(args).run()


if __name__=='__main__':raise SystemExit(main())
