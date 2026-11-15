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


def direct_grant(response):
    ready=response.get('ready_to_clone')
    mirror=(response.get('response') or {}).get('ready_to_clone_v1')
    if ready!=mirror:raise Refused('recovery:direct-mirrors')
    return ready


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
        self.original_events=[];self.completed_mutations=set()
        if args.resume_prefix:
            from capture_integrity import verify_capture
            self.prefix=Path(args.resume_prefix);current=self.prefix
            while current:
                self.prefixes.insert(0,current)
                if len(self.prefixes)>3:raise Refused('recovery:too-many-prefixes')
                receipt=current/'retained-prefix.json'
                current=Path(json.loads(receipt.read_text())['capture']).parent.parent if receipt.exists() else None
            hashes=['72c0399dc1f64d9478c1be95e843188460dd366721d7ed09b9b7860bd68a0ea3',
                '0319ed4142271357a89f5458c7334725a41a53758f9b4fc96e62f539b1fd013f',
                '73493435fa42692ecf2bc3a0a54a81dd49aa3fb5c6ff9aab84fdbb80f517b232']
            for root,sha in zip(self.prefixes,hashes):
                captures=list((root/'golden').glob('*.jsonl'))
                if self.phase!='hosted' or len(captures)!=1 or digest(captures[0])!=sha:raise Refused('recovery:exact-original-hosted-prefix')
                verify_capture(captures[0])
                self.original_events+=json.loads((root/'operations.json').read_text())
            results=[e for e in self.original_events if e['kind']=='dbt-result']
            count={1:13,2:24,3:28}[len(self.prefixes)]
            if [(e['label'],e['returncode']) for e in results]!=[(c[0],int(i==12)) for i,c in enumerate(self.commands[:count])]:
                raise Refused('recovery:exact-prefix-command-results')
            self.completed={e['label'] for e in results}
            writes=[e['label'] for e in self.original_events if e['kind']=='sql' and e.get('state')=='SUCCEEDED'
                and re.match(r'(?i)^(CREATE|INSERT|UPDATE|DROP) ',e['statement'])]
            completed_count={1:2,2:8,3:11}[len(self.prefixes)]
            if writes!=['create_source','initial_source']+[m[0] for m in mutations()[:completed_count]]:
                raise Refused('recovery:exact-prefix-mutations')
            self.completed_mutations=set(writes[2:])
            self.save('retained-prefix.json',{'capture':str(captures[0]),'sha256':digest(captures[0]),
                'repeated_native_commands':0,'repeated_source_or_result_writes':0})

    def dbt(self,label,command,target='prod',managed=True):
        calls=super().dbt(label,command,target,managed)
        if label in self.completed and label=='full_target_modified':
            raise Refused('retained native replacement error; do not rerun')
        return calls

    def run(self):
        # Precopy.run supplies its old #152 output name; use the same exact
        # chronological concatenation without changing either capture.
        result=Baseline.run(self)
        if self.prefix:
            from databricks_snapshots_continuation import combine_captures
            old=next((self.prefixes[0]/'golden').glob('*.jsonl'))
            for index,root in enumerate(self.prefixes[1:],1):
                combined=self.work/f'combined-prefix-{index}.jsonl'
                combine_captures(old,next((root/'golden').glob('*.jsonl')),combined);old=combined
            combine_captures(old,
                next((self.work/'golden').glob('*.jsonl')),self.work/'databricks_copy_recovery_hosted_whole.jsonl')
        return result

    def mutation_evidence(self,label):
        roots=[root for root in self.prefixes if (root/'mutations'/f'{label}_before.json').exists()]
        if len(roots)!=1:raise Refused('recovery:exact-mutation-evidence')
        return roots[0]

    def created_metadata(self,label,target):
        retained=[json.loads(path.read_text())['full_metadata'] for root in self.prefixes
            if (path:=root/'physical'/f'{label}_created_ID.json').exists()]
        if retained:
            ids={guarded_metadata(meta,target) for meta in retained}
            if len(ids)!=1:raise Refused('recovery:inconsistent-retained-created-ID')
            return retained[-1]
        if label in self.completed and ({'merge_chain':2,'merge_source_modified':3}.get(label)!=len(self.prefixes)
                or not (self.work/'checks'/f'{label}.json').exists()):raise Refused('recovery:missing-retained-created-ID')
        return self.api('get','/api/2.1/unity-catalog/tables/'+target)

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
        if self.prefix:
            previous=json.loads((self.prefix/'lease-inventory-after.json').read_text())
            # The shared check's prefix mode requires every planned object.
            # This exact causal prefix has only input + FULL prod/dev so far.
            required={r['full_name'] for rows in previous.values() for r in rows if r['full_name'] in self.scope.allowed}
            planned=self.scope.allowed
            self.scope.allowed=required
            try:result=SubmitCopy.init_schemas(self)
            finally:self.scope.allowed=planned
            for schema,rows in self.before_inventory.items():
                if any(r not in rows for r in previous[schema]):raise Refused('recovery:prefix-object-preservation')
                missing=sum(name.split('.')[1]==schema and name not in required for name in planned)
                if len(rows)+missing>100:raise Refused('recovery:continuation-capacity')
            # Final preservation checks concern older graphs. The current
            # phase's named results may be removed by its exact test ledger.
            self.before_inventory={schema:[r for r in rows if r['full_name'] not in planned]
                for schema,rows in self.before_inventory.items()}
            return result
        return SubmitCopy.init_schemas(self)

    def preflight(self):
        self.setup_project();(self.project/'models/sources.yml').unlink()
        for mode in MODES:
            self.configure(mode)
            for role in ('prod','dev'):
                label='compile_'+mode+'_'+role
                if self.dbt(label,['compile'],role,False):raise Refused('recovery:compile-State-RPC')
                manifest=json.loads((self.evidence(label)/'targets'/f'{self.n:03d}_{label}/manifest.json').read_text())
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
        if label in self.completed_mutations:
            original=self.mutation_evidence(label)
            before=json.loads((original/'mutations'/f'{label}_before.json').read_text())
            if guarded_metadata(before['metadata'],target)!=self.created_ids[target]:raise Refused('recovery:retained-created-ID-mismatch')
            if operation=='drop':
                self.drops+=1;self.shallow.discard(target);del self.created_ids[target];del self.expected[target]
            else:
                self.updates+=1
                self.expected[target]=json.loads((original/'mutations'/f'{label}_after.json').read_text())['expected_complete_bag']
            self.save('mutations/'+label+'_retained.json',{'original':str(original/'mutations'/f'{label}_before.json'),'repeated_write':False})
            return
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
            events=[e for e in self.original_events+self.events if e['kind']=='dbt-result' and e['label']==label]
            if len(events)!=1 or events[0]['returncode']==0:raise
            log=(self.evidence(label)/'dbt'/f'{self.n:03d}_{label}.log').read_text()
            if ('[CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR]' not in log
                    or 'Database Error in model '+target.rsplit('.',1)[-1] not in log):raise
            calls=json.loads((self.evidence(label)/'wire'/f'{self.n:03d}_{label}.json').read_text())
            if any(c['method']=='ConfirmExecution' for c in calls):raise Refused('recovery:failed-replacement-confirmed')
            submits=[c for c in calls if c['method']=='SubmitEnrichedSQL']
            if len(submits)!=1 or set(submits[0]['response']['response'])!={'ready_to_execute'}:raise Refused('recovery:failed-replacement-must-BUILD')
            req=submits[0]['request']
            if (req['target_table'].replace('`','')!=target or req['execution_type']!=1
                    or req['allow_clones'] or req['clone_chain_depth_limit']!=0
                    or req['table_namespace']!=self.namespace or req['query_dependencies'] or req['compare_unrendered_code']
                    or req['dbt_node_state']['node_unique_id']!=self.uids[kind]):raise Refused('recovery:failed-replacement-active-scope')
            old_checks=[root/'checks'/f'{label}.json' for root in self.prefixes if (root/'checks'/f'{label}.json').exists()]
            if old_checks:
                check=json.loads(old_checks[0].read_text())
                if check.get('native_replacement_failed') is not True or check.get('no_Confirm') is not True:raise Refused('recovery:retained-failure-check')
                self.save('checks/'+label+'_retained.json',{'original_check':str(old_checks[0]),'repeated_failed_command':False})
                return
            self.audit_native(label,target,source,False)
            self.explain(label,submits)
            expected=[list(r) for r in FIRST];expected[0][1]='recovery_sentinel_wrong'
            assert_source(self.sql(label+'_retained_corruption',source_projection_sql(target)),expected)
            original=self.mutation_evidence('full_modify_target') if label in self.completed else self.work
            before=json.loads((original/'mutations/full_modify_target_after.json').read_text())
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
        direct=[ready for c in clones if (ready:=direct_grant(c['response']))]
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
        repaired=bool(direct and successful and verdict=='ready_to_execute')
        self.physical(label,kind,role,'first',target in self.shallow,bool(successful),'dev' if mode=='reverse' else 'prod',repaired)
        meta=self.created_metadata(label,target)
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
        if not self.prefix:
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
    parser.add_argument('--resume-prefix')
    args=parser.parse_args()
    if args.plan:print(json.dumps(plan(),indent=2));return 0
    return CopyRecovery(args).run()


if __name__=='__main__':raise SystemExit(main())
