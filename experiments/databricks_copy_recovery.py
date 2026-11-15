#!/usr/bin/env python3
"""#154 serialized copy/source recovery using the proven #152 physical oracle.

This never injects a transport response or modifies an older campaign object.
Every intentional result DROP is a named test operation with a current UUID,
owner and complete beforeimage, not experiment cleanup.
"""
import argparse
import json
from pathlib import Path
import re
import uuid

from databricks_baseline import Baseline, LeaseScope
from databricks_precopy import Precopy, SCHEMAS, SCHEMA_IDS, FIRST, digest
from databricks_snapshot_oracle import assert_source, source_projection_sql
from databricks_snapshots import literal_source
from databricks_survey import Campaign, Scope, Refused, check_manifest

OBJECTS={'hosted':'9b197ef74e7e45a3a608f681e63536d2','rust':'3124b157d6db478cadc66c2f5a338da9'}
RUNS={'hosted':'20261115a154aa','rust':'20261115b154bb'}
MODES=('normal','reverse','repair')


def mutations():
    result=[]
    for kind in ('full','merge'):
        result += [(kind+'_drop_original',kind,'prod','drop'),(kind+'_modify_target',kind,'dev','update')]
        if kind=='full':result += [(kind+'_drop_failed_target',kind,'dev','drop')]
        result += [(kind+'_modify_source',kind,'prod','update'),(kind+'_drop_before_modified',kind,'dev','drop'),
            (kind+'_drop_missing_source',kind,'prod','drop'),(kind+'_drop_before_missing',kind,'dev','drop')]
    return result


def guarded_metadata(meta,target):
    try:table_id=uuid.UUID(meta.get('table_id',''))
    except (ValueError,TypeError,AttributeError):raise Refused('recovery:mutation-owner-ID-provider')
    if (meta.get('full_name')!=target or meta.get('owner')!='max.firman@gmail.com'
            or str(table_id)!=meta['table_id'] or table_id.version!=4
            or meta.get('data_source_format')!='DELTA'
            or meta.get('table_type') not in ('MANAGED','MANAGED_SHALLOW_CLONE')):
        raise Refused('recovery:mutation-owner-ID-provider')
    return str(table_id)


def cells(kind):
    result=[('source','prod','normal'),('copy','dev','normal'),('repeat','dev','normal'),
        ('source_removed','dev','normal'),('chain','prod','reverse'),('chain_repeat','prod','reverse'),
        ('target_modified','dev','repair')]
    if kind=='full':result += [('target_retry','dev','normal')]
    return result+ [('target_repeat','dev','normal'),('source_modified','dev','normal'),
        ('modified_repeat','dev','normal'),('missing_source','dev','normal'),('missing_repeat','dev','normal')]


def plan():
    return {'ticket':154,'catalog':'cdo_demo_db','schemas':SCHEMAS,'schema_ids':SCHEMA_IDS,
        'run_ids':RUNS,'objects':OBJECTS,'source':FIRST,
        'names':{phase:[f'cdo_demo_db.{SCHEMAS[role]}.p154_{oid}_{name}'
            for role,names in (('prod',('inputs','full','merge')),('dev',('full','merge')))
            for name in names] for phase,oid in OBJECTS.items()},
        'cells':{kind:cells(kind) for kind in ('full','merge')},
        'managed_per_backend':25,'compile_per_backend':6,'readonly_show_max':25,
        'unique_permanent_per_backend':5,'temporary_peak':1,'new_schemas':0,'cleanup':False,
        'input_writes':['CREATE','INSERT'],
        'result_operations':mutations(),
        'scope':'automatic FULL and direct MERGE; exact single-input SELECT-star; quiescent managed Delta',
        'excluded':['snapshot recovery','physical concurrency','new positive chain policy','general SQL']}


class RecoveryScope(LeaseScope):
    profile_roles=('prod','dev')

    def __init__(self,run,lease,phase):
        Scope.__init__(self,run)
        if run!=RUNS[phase]:raise Refused('recovery:exact-run')
        for oid in OBJECTS.values():
            if uuid.UUID(oid).hex!=oid or uuid.UUID(oid).version!=4:raise Refused('recovery:UUID4')
        owned=set()
        for evidence in lease['source_evidence']:
            if digest(evidence['path'])!=evidence['sha256']:raise Refused('recovery:ownership-digest')
            receipt=json.loads(Path(evidence['path']).read_text())
            for role,schema in SCHEMAS.items():
                if receipt.get(schema)=={'full_name':'cdo_demo_db.'+schema,'schema_id':SCHEMA_IDS[role],'owner':'max.firman@gmail.com'}:owned.add(schema)
        if not set(SCHEMAS.values())<=owned:raise Refused('recovery:owned-containers')
        self.roles=SCHEMAS;self.schemas=set(SCHEMAS.values())
        self.relation_prefix='p154_'+OBJECTS[phase]+'_';self.allowed=set(plan()['names'][phase])


class CopyRecovery(Precopy):
    def __init__(self,args):
        self.phase='rust' if args.rust_url else 'hosted'
        self.lease=json.loads(Path(args.schema_lease).read_text())
        grant=json.loads(Path(args.live_clearance).read_text())
        scope=RecoveryScope(args.run_id,self.lease,self.phase)
        if (grant.get('status')!='LIVE CLEARED #154 copy recovery'
                or self.phase not in grant['authorized_phases']
                or json.loads(Path(args.operation_ledger).read_text())!=json.loads(json.dumps(plan()))
                or digest(args.operation_ledger)!=grant['operation_sha256']
                or grant['source_sha256']!={p:digest(Path(__file__).parent/p) for p in grant['source_sha256']}):
            raise Refused('recovery:exact-reviewed-plan-and-source')
        Campaign.__init__(self,args,scope)
        self.prefixes=[];self.completed=set();self.prefix=None
        self.save('recovery-plan.json',plan());self.save('live-clearance.json',grant)
        self.save('recovery-runner.py',Path(__file__).read_text())
        self.save('binary-provenance.json',{k:{'path':str(Path(getattr(args,k)).resolve()),'sha256':digest(getattr(args,k))} for k in ('dbt','proxy','probe')})
        self.commands=[('compile_'+mode+'_'+role,['compile'],role,False) for mode in MODES for role in ('prod','dev')]
        self.commands += [(kind+'_'+label,['run','--select',kind],role,True) for kind in ('full','merge') for label,role,_ in cells(kind)]
        self.command_index=0;self.drops=0;self.updates=0;self.mutation_index=0;self.shallow=set()
        self.confirmed={};self.uids={};self.namespace=None;self.created_ids={};self.expected={}

    def configure(self,mode):
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            for key in list(output):
                if key not in {'type','host','http_path','catalog','schema','token','threads'}:del output[key]
            output['threads']=1
            copy_role='prod' if mode=='reverse' else 'dev'
            output['allow_clones']=mode!='repair' and role==copy_role
            if output['allow_clones']:output['defer_to_target']='dev' if role=='prod' else 'prod'
        self.save('profiles/profiles.yml',profile)
        source=self.scope.relation('prod','inputs')
        for kind in ('full','merge'):
            cfg="materialized='table'" if kind=='full' else "materialized='incremental',incremental_strategy='merge',unique_key='id'"
            state="{'lag_tolerance':'0s'"+(",'pre_clone':'never'" if kind=='full' or mode=='repair' else '')+'}'
            self.save('project/models/'+kind+'.sql',"{{ config(alias='"+self.scope.relation_prefix+kind+"',"+cfg+",state="+state+") }}\nselect * from "+source+'\n')

    def init_schemas(self):
        # Identical owner/ID/paged-inventory/collision/capacity checks to #153.
        from databricks_submit_copy import SubmitCopy
        return SubmitCopy.init_schemas(self)

    def preflight(self):
        self.setup_project();(self.project/'models/sources.yml').unlink()
        for mode in MODES:
            self.configure(mode)
            for role in ('prod','dev'):
                label='compile_'+mode+'_'+role
                if self.dbt(label,['compile'],role,False):raise Refused('recovery:compile-State-RPC')
                manifest=json.loads((self.work/'targets'/f'{self.n:03d}_{label}/manifest.json').read_text())
                check_manifest(self.scope,manifest)
                if {n['name'] for n in manifest['nodes'].values()}!={'full','merge'}:raise Refused('recovery:manifest-nodes')
                for n in manifest['nodes'].values():
                    self.scope.check('.'.join((n['database'],n['schema'],n['alias'])))
                    cfg=n['config']
                    if cfg.get('pre-hook') or cfg.get('post-hook') or n.get('depends_on',{}).get('nodes'):raise Refused('recovery:manifest-hooks-dependencies')
                    wanted='never' if n['name']=='full' or mode=='repair' else None
                    if cfg.get('state',{}).get('pre_clone')!=wanted:raise Refused('recovery:manifest-copy-mode')
                self.save('preflight/'+label+'.json',manifest)

    def mutate(self,label,kind,role,operation):
        if (self.mutation_index>=len(mutations())
                or (label,kind,role,operation)!=mutations()[self.mutation_index]):raise Refused('recovery:ordered-mutation-budget')
        self.mutation_index+=1
        target=self.scope.relation(role,kind)
        meta=self.api('get','/api/2.1/unity-catalog/tables/'+target)
        table_id=guarded_metadata(meta,target)
        if self.created_ids.get(target)!=table_id:raise Refused('recovery:created-ID-mismatch')
        before={'metadata':meta,'properties':self.sql(label+'_properties','SHOW TBLPROPERTIES '+target),
            'bag':self.observe(label+'_before',target),'history':self.sql(label+'_history','DESCRIBE HISTORY '+target)}
        assert_source(self.sql(label+'_before_lossless',source_projection_sql(target)),self.expected[target])
        self.save('mutations/'+label+'_before.json',before)
        if guarded_metadata(self.api('get','/api/2.1/unity-catalog/tables/'+target),target)!=table_id:raise Refused('recovery:mutation-ID-changed')
        if operation=='drop':
            self.drops+=1
            if self.drops>9:raise Refused('recovery:drop-budget')
            self.sql(label,'DROP TABLE '+target)
            self.shallow.discard(target)
            del self.created_ids[target]
            del self.expected[target]
            inventory=self.inventory()
            if any(r['full_name']==target for rows in inventory.values() for r in rows):raise Refused('recovery:dropped-target-still-present')
            self.save('mutations/'+label+'_after.json',{'absent':target,'inventory':inventory})
        elif operation=='update':
            self.updates+=1
            if self.updates>4:raise Refused('recovery:update-budget')
            self.sql(label,"UPDATE "+target+" SET v='recovery_sentinel_wrong' WHERE id=1")
            expected=[list(r) for r in FIRST];expected[0][1]='recovery_sentinel_wrong'
            assert_source(self.sql(label+'_after_lossless',source_projection_sql(target)),expected)
            after_props=self.sql(label+'_after_properties','SHOW TBLPROPERTIES '+target)
            if dict(after_props['rows'])!=dict(before['properties']['rows']):raise Refused('recovery:mutation-property-change')
            if guarded_metadata(self.api('get','/api/2.1/unity-catalog/tables/'+target),target)!=table_id:raise Refused('recovery:updated-ID-changed')
            self.expected[target]=expected
            self.save('mutations/'+label+'_after.json',{'expected_complete_bag':expected,
                'properties':after_props,'history':self.sql(label+'_after_history','DESCRIBE HISTORY '+target)})
        else:raise Refused('recovery:mutation-operation')

    def invocation(self,kind,cell):
        label,role,mode=cell;label=kind+'_'+label
        self.configure(mode)
        target=self.scope.relation(role,kind)
        source=self.scope.relation('dev' if mode=='reverse' else 'prod',kind)
        try:calls=self.dbt(label,['run','--select',kind],role)
        except Refused:
            if label!='full_target_modified':raise
            events=[e for e in self.events if e['kind']=='dbt-result' and e['label']==label]
            if len(events)!=1 or events[0]['returncode']==0:raise
            log=(self.work/'dbt'/f'{self.n:03d}_{label}.log').read_text()
            if not re.search(r'(?i)(shallow.clone|managedshallowclone|managed_shallow_clone)',log):raise
            calls=json.loads((self.work/'wire'/f'{self.n:03d}_{label}.json').read_text())
            if any(c['method']=='ConfirmExecution' for c in calls):raise Refused('recovery:failed-replacement-confirmed')
            submits=[c for c in calls if c['method']=='SubmitEnrichedSQL']
            if len(submits)!=1 or set(submits[0]['response']['response'])!={'ready_to_execute'}:raise Refused('recovery:failed-replacement-must-BUILD')
            self.audit_native(label,target,source,False)
            self.explain(label,submits)
            expected=[list(r) for r in FIRST];expected[0][1]='recovery_sentinel_wrong'
            assert_source(self.sql(label+'_retained_corruption',source_projection_sql(target)),expected)
            before=json.loads((self.work/'mutations/full_modify_target_after.json').read_text())
            after_history=self.sql(label+'_after_failure_history','DESCRIBE HISTORY '+target)
            if after_history!=before['history']:raise Refused('recovery:failed-replacement-changed-history')
            if guarded_metadata(self.api('get','/api/2.1/unity-catalog/tables/'+target),target)!=self.created_ids[target]:
                raise Refused('recovery:failed-replacement-changed-ID')
            self.save('checks/'+label+'.json',{'native_replacement_failed':True,'no_Confirm':True,'error_log':f'dbt/{self.n:03d}_{label}.log'})
            return
        submits=[c for c in calls if c['method']=='SubmitEnrichedSQL']
        if len(submits)!=1:raise Refused('recovery:one-ordinary-submit')
        req=submits[0]['request'];response=submits[0]['response']['response'];verdict=next(iter(response))
        copy_role='prod' if mode=='reverse' else 'dev'
        allow=mode!='repair' and role==copy_role
        if (req['allow_clones']!=allow or req['clone_chain_depth_limit']!=int(allow)
                or not req.get('table_namespace') or req['compare_unrendered_code'] or req['query_dependencies']):
            raise Refused('recovery:actual-copy-policy-wire')
        uid=req['dbt_node_state']['node_unique_id']
        if kind in self.uids and uid!=self.uids[kind]:raise Refused('recovery:UID-continuity')
        self.uids[kind]=uid
        if self.namespace and req['table_namespace']!=self.namespace:raise Refused('recovery:namespace-continuity')
        self.namespace=req['table_namespace']
        confirms={c['request']['request_id']:c for c in calls if c['method']=='ConfirmExecution' and c['response'].get('success') is True}
        if verdict!='skip_execution' and response[verdict]['request_id'] not in confirms:raise Refused('recovery:unconfirmed-success')
        clones=[c for c in calls if c['method']=='RegisterClone']
        direct=[c['response']['ready_to_clone'] for c in clones if c['response'].get('ready_to_clone')]
        for c in clones:
            cr=c['request']
            if (kind!='merge' or cr['clone_source_table'].replace('`','')!=source
                    or cr['target_table'].replace('`','')!=target or cr['table_namespace']!=self.namespace
                    or cr['clone_chain_depth_limit']!=1 or cr['labels']['dbt_node_unique_id']!=uid):
                raise Refused('recovery:actual-direct-source-scope')
            if cell[0]!='source_modified' and cr['clone_source_last_modified_epoch']!=self.confirmed[source]['last_modified_epoch']:
                raise Refused('recovery:actual-direct-source-epoch')
            if cell[0]=='source_modified' and (not cr['clone_source_last_modified_epoch']
                    or cr['clone_source_last_modified_epoch']==self.confirmed[source]['last_modified_epoch']):
                raise Refused('recovery:physical-source-change-unobserved')
            if c['response'].get('ready_to_clone')!=c['response'].get('response',{}).get('ready_to_clone_v1') and c['response'].get('ready_to_clone'):
                raise Refused('recovery:direct-mirrors')
        ready=direct or ([response[verdict]] if verdict=='ready_to_clone' else [])
        for grant in ready:
            if (grant['clone_source'].replace('`','')!=source or grant['clone_target'].replace('`','')!=target
                    or grant['clone_sqls']!=['\n        CREATE OR REPLACE TABLE '+grant['clone_target']+' SHALLOW CLONE '+grant['clone_source']+'\n        ']):
                raise Refused('recovery:exact-grant-DDL')
            if not direct and (grant['clone_required_last_modified_epoch']!=self.confirmed[source]['last_modified_epoch']
                    or grant['execution_runtime_ms']!=self.confirmed[source]['execution_runtime_ms']):
                raise Refused('recovery:automatic-source-epoch-runtime')
        successful=[r for r in ready if r['request_id'] in confirms and not confirms[r['request_id']]['request']['failed_to_clone']]
        failed=[r for r in ready if r['request_id'] in confirms and confirms[r['request_id']]['request']['failed_to_clone']]
        if req['target_table'].replace('`','')!=target or req['execution_type']!=(1 if kind=='full' else 3):raise Refused('recovery:actual-scope')
        if req['sql'].replace('`','').strip().lower()!='select * from '+self.scope.relation('prod','inputs'):raise Refused('recovery:actual-input')
        if cell[0].endswith('repeat') or cell[0]=='source_removed':
            if verdict!='skip_execution' or ready:raise Refused('recovery:expected-own-SKIP')
            if response[verdict]['execution_runtime_ms']!=self.confirmed[target]['execution_runtime_ms']:
                raise Refused('recovery:own-runtime-history')
        if cell[0]=='copy' or (cell[0]=='target_retry' and source not in self.shallow):
            if len(successful)!=1:raise Refused('recovery:expected-successful-copy')
        if cell[0]=='target_retry' and source in self.shallow and not successful and verdict!='ready_to_execute':
            raise Refused('recovery:copy-source-retry-must-BUILD')
        if self.phase=='rust' and cell[0]=='chain' and (successful or verdict!='ready_to_execute'):raise Refused('recovery:conservative-chain-BUILD')
        if cell[0] in ('source_modified','missing_source') and successful:
            # A direct pre-copy may be followed by a separate ordinary MERGE
            # that repairs it. Automatic successful copy ends the invocation.
            if not direct or verdict!='ready_to_execute':raise Refused('recovery:invalid-source-copy-without-repair')
        self.save('observed/'+label+'.json',{'request':req,'calls':calls,'verdict':verdict,
            'successful_copies':len(successful),'failed_copy_fallbacks':len(failed)})
        self.audit_native(label,target,source,bool(successful),allow_failed_attempt=bool(failed))
        self.explain(label,submits)
        if successful:self.shallow.add(target)
        self.physical(label,kind,role,'first',target in self.shallow,bool(successful),'dev' if mode=='reverse' else 'prod')
        meta=self.api('get','/api/2.1/unity-catalog/tables/'+target)
        table_id=guarded_metadata(meta,target)
        if target in self.created_ids and self.created_ids[target]!=table_id:raise Refused('recovery:unexpected-ID-replacement')
        self.created_ids[target]=table_id
        self.expected[target]=FIRST
        self.save('physical/'+label+'_created_ID.json',{'target':target,'table_id':table_id,'full_metadata':meta})
        if verdict!='skip_execution' or successful:
            pending_id=response[verdict]['request_id'] if verdict!='skip_execution' else successful[0]['request_id']
            retained=dict(confirms[pending_id]['request'])
            if successful and retained['execution_runtime_ms'] is None:
                retained['execution_runtime_ms']=self.confirmed[source]['execution_runtime_ms']
            self.confirmed[target]=retained

    def baseline(self):
        self.preflight()
        source=self.scope.relation('prod','inputs')
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
        self.sql('initial_source','INSERT INTO '+source+' VALUES '+literal_source(FIRST))
        assert_source(self.sql('source_check',source_projection_sql(source)),FIRST)
        for kind in ('full','merge'):
            for cell in cells(kind):
                label=cell[0]
                if label=='source_removed':self.mutate(kind+'_drop_original',kind,'prod','drop')
                if label=='target_modified':self.mutate(kind+'_modify_target',kind,'dev','update')
                if label=='target_retry':self.mutate(kind+'_drop_failed_target',kind,'dev','drop')
                if label=='source_modified':
                    self.mutate(kind+'_modify_source',kind,'prod','update')
                    self.mutate(kind+'_drop_before_modified',kind,'dev','drop')
                if label=='missing_source':
                    self.mutate(kind+'_drop_missing_source',kind,'prod','drop')
                    self.mutate(kind+'_drop_before_missing',kind,'dev','drop')
                self.invocation(kind,cell)
        if (self.command_index!=len(self.commands) or self.drops!=9 or self.updates!=4
                or self.mutation_index!=len(mutations())):raise Refused('recovery:complete-operation-budget')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',action='store_true')
    for key in ('dbt','proxy','probe','work-dir','schema-lease','live-clearance','operation-ledger','run-id'):parser.add_argument('--'+key)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--rust-url')
    args=parser.parse_args()
    if args.plan:print(json.dumps(plan(),indent=2));return 0
    return CopyRecovery(args).run()


if __name__=='__main__':raise SystemExit(main())
