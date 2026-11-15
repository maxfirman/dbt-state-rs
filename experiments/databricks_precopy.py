#!/usr/bin/env python3
"""#152 native State pre-copy acceptance, bounded to quiescent managed Delta.

No explicit dbt clone, adapter overrides, persistent references or inferred
provider evidence. Source and development runs share the actual native UID.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import uuid
from urllib.parse import quote

from databricks_baseline import Baseline, LeaseScope
from databricks_readonly_reference import show_reference, unique_object
from databricks_snapshot_oracle import (Ledger, NATIVE_SCHEMA, assert_source,
    check_schema, decode_projection, projection_sql, source_projection_sql)
from databricks_snapshots import literal_source
from databricks_survey import Campaign, Refused, Scope, check_manifest

SCHEMAS={'prod':'ds_20261112a134ef_prod','dev':'ds_20261112a134ef_ref_dbt_test__audit'}
SCHEMA_IDS={'prod':'59559b89-efa8-4ed7-b106-1394c2718cc3','dev':'dcabff91-9a47-4ff0-9f53-2b71c06f414a'}
OBJECTS={'hosted':'f806154a328b45f1bbbb05383375bc94','rust':'c62f21b7efe74c54a74ca9b1f8d8d4ca'}
RUNS={'hosted':'20261115c152aa','rust':'20261115d152bb'}
FIRST=(('1','one','2026-11-01','2026-11-01 12:00:00.123450'),
       ('2',None,'2026-11-02','2026-11-02 12:00:00.654321'))
CHANGED=(('1','changed','2026-11-03','2026-11-03 12:00:00.000001'),FIRST[1],
         ('3','one','2026-11-04','2026-11-04 12:00:00.100010'))
# label, target, input revision, pre_clone setting, expected copy, submit decision
CELLS=(('source','prod','first',None,False,'ready_to_execute'),
       ('absent','dev','first',None,True,'skip_execution'),
       ('repeat','dev','first',None,False,'skip_execution'),
       ('changed','dev','changed',None,False,'ready_to_execute'),
       ('changed_repeat','dev','changed',None,False,'skip_execution'),
       ('source_always','prod','changed','always',False,'ready_to_execute'),
       ('existing','dev','changed','always',True,'skip_execution'))


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def assert_clone_history(history,source,repaired=False):
    names=[c['name'] for c in history['columns']]
    rows=[dict(zip(names,r)) for r in history['rows']]
    if not repaired:
        if not rows or rows[0].get('operation')!='CLONE':raise Refused('precopy:physical-copy-history')
        return
    if ([(r.get('version'),r.get('operation')) for r in rows]!=[('2','CLUSTER BY'),('1','MERGE'),('0','CLONE')]
            or json.loads(rows[0]['operationParameters'])!={'oldClusteringColumns':'','newClusteringColumns':'','clusterByAuto':'false'}):
        raise Refused('precopy:physical-copy-repair-history')
    cloned=json.loads(rows[2]['operationParameters'])
    if cloned!={'source':source,'sourceVersion':'1','isShallow':'true'}:raise Refused('precopy:physical-copy-repair-source')


def plan():
    return {'ticket':152,'catalog':'cdo_demo_db','schemas':SCHEMAS,'schema_ids':SCHEMA_IDS,
        'run_ids':RUNS,'objects':OBJECTS,'cells':CELLS,'source':{'first':FIRST,'changed':CHANGED},
        'names':{phase:[f'cdo_demo_db.{SCHEMAS[role]}.p152_{oid}_{name}'
            for role,names in (('prod',('inputs','merge','check')),('dev',('merge','check')))
            for name in names] for phase,oid in OBJECTS.items()},
        'managed_per_backend':14,'compile_per_backend':4,'readonly_show_per_backend':14,
        'source_writes':['CREATE','INSERT','UPDATE id=1','INSERT id=3'],
        'permanent_per_backend':5,'temporary_peak':1,'new_schemas':0,'cleanup':False,
        'scope':'MERGE and check/ignore with updated_at=ts; exact SELECT-star single input',
        'excluded':['automatic submit copy','chains','physical concurrent source changes',
                    'FULL/seed replacement','new_record','general SQL/templates/dependencies']}


def expected(kind,stage):
    if kind in ('merge','full'):return FIRST if stage=='first' else CHANGED
    ledger=Ledger('check','ignore',source_updated_at=True).execute(FIRST,'first')
    if stage=='changed':ledger=ledger.execute(CHANGED,'changed')
    return ledger.rows({})


class CopyScope(LeaseScope):
    profile_roles=('prod','dev')

    def __init__(self,run,lease,phase):
        Scope.__init__(self,run)
        if (run!=RUNS[phase] or any(len(v)!=32 or uuid.UUID(v).hex!=v or uuid.UUID(v).version!=4 for v in OBJECTS.values())):
            raise Refused('precopy:exact-run')
        owned=set()
        for evidence in lease['source_evidence']:
            if digest(evidence['path'])!=evidence['sha256']:raise Refused('precopy:ownership-digest')
            receipt=json.loads(Path(evidence['path']).read_text())
            for role,schema in SCHEMAS.items():
                if receipt.get(schema)=={'full_name':'cdo_demo_db.'+schema,'schema_id':SCHEMA_IDS[role],'owner':'max.firman@gmail.com'}:
                    owned.add(schema)
        if not set(SCHEMAS.values())<=owned:raise Refused('precopy:owned-containers')
        self.roles=SCHEMAS;self.schemas=set(SCHEMAS.values())
        self.relation_prefix='p152_'+OBJECTS[phase]+'_'
        self.allowed=set(plan()['names'][phase])


def assert_show(stdout,stderr,direct,rows,sql,query_log,compiled):
    if stderr.strip():raise Refused('precopy:reference-stderr')
    objects=json.loads(stdout,object_pairs_hook=unique_object)
    names=[c['name'] for c in direct['columns']]
    if (not isinstance(objects,list) or any(not isinstance(o,dict) or list(o)!=names for o in objects)
            or Counter(tuple(o.values()) for o in objects)!=Counter(map(tuple,rows))
            or any(c['type_name']!='STRING' for c in direct['columns'])):
        raise Refused('precopy:reference-complete-rows-types')
    query='\n'.join(line for line in query_log.splitlines() if not line.startswith('--'))
    norm=lambda s:re.sub(r'\s+',' ',s).strip()
    if (not sql.startswith('SELECT ') or ';' in sql or norm(compiled)!=norm(sql)
            or query.count(';')!=1 or not query.rstrip().endswith(';') or norm(query.rstrip()[:-1])!=norm(sql)):
        raise Refused('precopy:reference-not-exact-select')
    return {'complete_string_multiset':True,'exact_select_only':True}


def assert_copy_properties(source,target,cluster_by_none=False):
    # New physical tables have distinct UC/row-tracking identifiers. These are
    # the only observed differences; preserve every other reported property.
    identities={'io.unitycatalog.tableId':'',
        'delta.rowTracking.materializedRowIdColumnName':'_row-id-col-',
        'delta.rowTracking.materializedRowCommitVersionColumnName':'_row-commit-version-col-'}
    if not source or set(source)!=set(target):raise Refused('precopy:property-keys')
    if cluster_by_none and (source.get('clusterByAuto'),target.get('clusterByAuto'))!=('true','false'):
        raise Refused('precopy:repair-clustering-property')
    for key,value in source.items():
        if key not in identities:
            if cluster_by_none and key=='clusterByAuto':continue
            if target[key]!=value:raise Refused('precopy:default-properties-not-preserved')
        else:
            prefix=identities[key]
            for identifier in (value,target[key]):
                if not identifier.startswith(prefix):raise Refused('precopy:physical-identifier-prefix')
                text=identifier[len(prefix):]
                try:parsed=uuid.UUID(text)
                except ValueError as error:raise Refused('precopy:physical-identifier-UUID') from error
                if str(parsed)!=text or parsed.version!=4:raise Refused('precopy:physical-identifier-UUID')
            if target[key]==value:raise Refused('precopy:physical-identifier-not-distinct')
    result={'preserved':[k for k in source if k not in identities and not (cluster_by_none and k=='clusterByAuto')],
            'distinct_physical_identifiers':[k for k in source if k in identities]}
    if cluster_by_none:result['explicit_property_transition']={'clusterByAuto':{'source':'true','target':'false','reason':'executed ALTER TABLE CLUSTER BY NONE'}}
    return result


class Precopy(Baseline):
    def __init__(self,args):
        self.phase='rust' if args.rust_url else 'hosted'
        grant=json.loads(Path(args.live_clearance).read_text())
        self.lease=json.loads(Path(args.schema_lease).read_text())
        self.scope=CopyScope(args.run_id,self.lease,self.phase)
        if (grant.get('status')!='LIVE CLEARED #152 physical pre-copy'
                or self.phase not in grant['authorized_phases']
                or json.loads(Path(args.operation_ledger).read_text())!=json.loads(json.dumps(plan()))
                or digest(args.operation_ledger)!=grant['operation_sha256']
                or grant['source_sha256']!={p:digest(Path(__file__).parent/p) for p in grant['source_sha256']}):
            raise Refused('precopy:exact-reviewed-plan-and-source')
        Campaign.__init__(self,args,self.scope)
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            for key in list(output):
                if key not in {'type','host','http_path','catalog','schema','token','threads','defer_to_target'}:del output[key]
            output['threads']=1
            if role=='dev':output['defer_to_target']='prod'
        self.save('profiles/profiles.yml',profile)
        self.save('precopy-plan.json',plan());self.save('live-clearance.json',grant)
        self.save('precopy-runner.py',Path(__file__).read_text())
        self.save('binary-provenance.json',{k:{'path':str(Path(getattr(args,k)).resolve()),'sha256':digest(getattr(args,k))} for k in ('dbt','proxy','probe')})
        self.commands=[(f'compile_{mode}_{role}',['compile'],role,False) for mode in ('default','always') for role in ('prod','dev')]
        for stage in ('initial','changed','always'):
            labels={'initial':('source','absent','repeat'),'changed':('changed','changed_repeat'),'always':('source_always','existing')}[stage]
            for kind in ('merge','check'):
                for label,role,_,_,_,_ in CELLS:
                    if label in labels:self.commands.append((kind+'_'+label,['run' if kind=='merge' else 'snapshot','--select',kind],role,True))
        self.command_index=0
        self.source_requests={};self.source_confirms={}
        self.prefix=Path(args.resume_prefix) if args.resume_prefix else None
        self.completed=set()
        self.prefixes=[]
        if self.prefix:
            from capture_integrity import verify_capture
            current=self.prefix
            while current:
                self.prefixes.insert(0,current)
                if len(self.prefixes)>2:raise Refused('precopy:too-many-prefixes')
                anchor=current/'retained-prefix.json'
                current=Path(json.loads(anchor.read_text())['capture']).parent.parent if anchor.exists() else None
            hashes=['09b538948b4b067c881415ed84064a92d2b5e611dbee6f4dc06621ea50ad7cbe',
                    '93a68c52a9abd45ab125c80b8eef6c446d105ac8924e3ca57c3e61992108e7a7']
            events=[]
            for root,sha in zip(self.prefixes,hashes):
                captures=list((root/'golden').glob('*.jsonl'))
                if self.phase!='hosted' or len(captures)!=1 or digest(captures[0])!=sha:raise Refused('precopy:exact-hosted-prefix-only')
                verify_capture(captures[0]);events+=json.loads((root/'operations.json').read_text())
            self.completed={e['label'] for e in events if e['kind']=='dbt-result' and e['returncode']==0}
            count=6 if len(self.prefixes)==1 else 11
            if self.completed!={c[0] for c in self.commands[:count]}:raise Refused('precopy:prefix-command-set')
            if len(self.prefixes)==2:
                writes={e['label'] for e in events if e['kind']=='sql' and e.get('state')=='SUCCEEDED'}
                if not {'create_source','initial_source','change_source','insert_source'}<=writes:raise Refused('precopy:missing-completed-source-writes')
            captures=list((self.prefix/'golden').glob('*.jsonl'))
            self.save('retained-prefix.json',{'capture':str(captures[0]),'sha256':digest(captures[0]),
                'completed_commands':sorted(self.completed),'repeated_successful_commands':0})

    def dbt(self,label,command,target='prod',managed=True):
        if self.command_index>=len(self.commands) or (label,command,target,managed)!=self.commands[self.command_index]:raise Refused('precopy:ordered-command-budget')
        self.command_index+=1
        if label in self.completed:
            self.n+=1
            paths=[path for root in self.prefixes for path in (root/'wire').glob('*_'+label+'.json')]
            if len(paths)!=1:raise Refused('precopy:missing-original-wire')
            return json.loads(paths[0].read_text())
        return Campaign.dbt(self,label,command,target,managed)

    def evidence(self,label):
        if label not in self.completed:return self.work
        roots=[root for root in self.prefixes if list((root/'wire').glob('*_'+label+'.json'))]
        if len(roots)!=1:raise Refused('precopy:ambiguous-original-evidence')
        return roots[0]

    def explain(self,label,submits):
        if label in self.completed and any((root/'wire'/f'{label}_explain.json').exists() for root in self.prefixes):return
        return Baseline.explain(self,label,submits)

    def run(self):
        result=Baseline.run(self)
        if self.prefix:
            from databricks_snapshots_continuation import combine_captures
            old=next((self.prefixes[0]/'golden').glob('*.jsonl'))
            for root in self.prefixes[1:]:
                intermediate=self.work/'combined-prefix.jsonl'
                combine_captures(old,next((root/'golden').glob('*.jsonl')),intermediate);old=intermediate
            combine_captures(old,next((self.work/'golden').glob('*.jsonl')),self.work/'databricks_precopy_hosted_whole.jsonl')
        return result

    def inventory(self):
        result={}
        for schema in sorted(self.scope.schemas):
            rows=[];token=None;seen=set()
            while True:
                endpoint='/api/2.1/unity-catalog/tables?catalog_name=cdo_demo_db&schema_name='+schema
                if token:endpoint+='&page_token='+quote(token,safe='')
                response=self.api('get',endpoint)
                rows.extend({k:r[k] for k in ('name','full_name','table_type')} for r in response.get('tables',[]))
                token=response.get('next_page_token')
                if not token:break
                if token in seen:raise Refused('precopy:pagination-loop')
                seen.add(token)
            result[schema]=rows
        return result

    def init_schemas(self):
        ownership={}
        for role,schema in SCHEMAS.items():
            metadata=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+schema)
            if metadata.get('schema_id')!=SCHEMA_IDS[role] or metadata.get('owner')!='max.firman@gmail.com':raise Refused('precopy:schema-owner')
            ownership[schema]={k:metadata[k] for k in ('full_name','schema_id','owner')}
        self.save('schema-ownership.json',ownership)
        self.before_inventory=self.inventory()
        for role,schema in SCHEMAS.items():
            existing={r['full_name'] for r in self.before_inventory[schema] if r['full_name'] in self.scope.allowed}
            wanted=({name for name in self.scope.allowed if name.split('.')[1]==schema}
                    if len(self.prefixes)==2 else
                    ({self.scope.relation('prod','inputs'),self.scope.relation(role,'merge')} if role=='prod' else {self.scope.relation(role,'merge')})) if self.prefix else set()
            if len(self.before_inventory[schema])+(3 if role=='prod' else 2)-len(wanted)>100:raise Refused('precopy:capacity')
            if existing!=wanted:raise Refused('precopy:collision')
        self.save('lease-inventory-before.json',self.before_inventory)
        self.save('warehouse.json',self.sql('warehouse',"SELECT current_version() AS version,current_timezone() AS timezone"))

    def write_projects(self,mode):
        if not self.project.exists():
            self.setup_project();(self.project/'models/sources.yml').unlink()
        state="{'lag_tolerance':'0s'"+(",'pre_clone':'always'" if mode=='always' else '')+'}'
        source=self.scope.relation('prod','inputs')
        self.save('project/models/merge.sql',"{{ config(alias='"+self.scope.relation_prefix+"merge',materialized='incremental',incremental_strategy='merge',unique_key='id',state="+state+") }}\nselect * from "+source+'\n')
        self.save('project/snapshots/check.sql',"{% snapshot check %}\n{{ config(alias='"+self.scope.relation_prefix+"check',unique_key='id',strategy='check',check_cols=['v'],updated_at='ts',hard_deletes='ignore',state="+state+") }}\nselect * from "+source+'\n{% endsnapshot %}\n')

    def preflight(self):
        for mode in ('default','always'):
            self.write_projects(mode)
            for role in ('prod','dev'):
                label=f'compile_{mode}_{role}'
                if self.dbt(label,['compile'],role,False):raise Refused('precopy:compile-State-RPC')
                manifest=json.loads((self.evidence(label)/'targets'/f'{self.n:03d}_{label}/manifest.json').read_text())
                check_manifest(self.scope,manifest)
                if {n['name'] for n in manifest['nodes'].values()}!={'merge','check'}:raise Refused('precopy:manifest-nodes')
                for n in manifest['nodes'].values():
                    self.scope.check('.'.join((n['database'],n['schema'],n['alias'])))
                    cfg=n['config']
                    if cfg.get('pre-hook') or cfg.get('post-hook') or n.get('depends_on',{}).get('nodes'):raise Refused('precopy:hooks-dependencies')
                    if cfg.get('state',{}).get('pre_clone')!=('always' if mode=='always' else None):raise Refused('precopy:manifest-preclone')
                    if n['name']=='check' and (cfg.get('updated_at')!='ts' or cfg.get('check_cols')!=['v'] or cfg.get('hard_deletes')!='ignore'):raise Refused('precopy:manifest-snapshot')
                self.save('preflight/'+label+'.json',manifest)

    def physical(self,label,kind,role,stage,copied,cloned_now,copy_source_role='prod',copy_repaired=False):
        completed_checks=[root/'checks'/f'{label}.json' for root in self.prefixes if (root/'checks'/f'{label}.json').exists()]
        if label in self.completed and completed_checks:
            self.save('checks/'+label+'_retained.json',{'original_check':str(completed_checks[0]),'repeated_reference':False})
            return
        target=self.scope.relation(role,kind)
        raw=self.observe(label,target)
        check_schema(raw,NATIVE_SCHEMA if kind=='check' else NATIVE_SCHEMA[:4])
        props=json.loads((self.work/'physical'/f'{label}_properties.json').read_text())
        values={r[0]:r[1] for r in props['rows']}
        if values.get('Type')!='MANAGED' or values.get('Provider','').lower()!='delta':raise Refused('precopy:physical-managed-delta')
        projection=self.sql(label+'_lossless',projection_sql(target) if kind=='check' else source_projection_sql(target))
        if kind=='check':actual=decode_projection(projection)
        else:
            assert_source(projection,expected(kind,stage));actual=[r[:4] for r in projection['rows']]
        rows=expected(kind,stage)
        if Counter(map(tuple,actual))!=Counter(map(tuple,rows)):raise Refused('precopy:complete-physical-bag')
        history=self.sql(label+'_history','DESCRIBE HISTORY '+target)
        self.save('physical/'+label+'_history.json',history)
        metadata=self.api('get','/api/2.1/unity-catalog/tables/'+target)
        self.save('physical/'+label+'_uc.json',{k:metadata.get(k) for k in ('full_name','table_id','table_type','data_source_format','properties','columns')})
        if metadata.get('data_source_format')!='DELTA' or metadata.get('table_type')!=('MANAGED_SHALLOW_CLONE' if copied else 'MANAGED'):
            raise Refused('precopy:independent-UC-provider-type')
        if cloned_now:
            source=self.scope.relation(copy_source_role,kind)
            assert_clone_history(history,source,copy_repaired)
            source_props=self.sql(label+'_source_properties','SHOW TBLPROPERTIES '+source)
            target_props=self.sql(label+'_target_properties','SHOW TBLPROPERTIES '+target)
            sp=dict(source_props['rows']);tp=dict(target_props['rows'])
            check=assert_copy_properties(sp,tp,copy_repaired)
            source_metadata=self.api('get','/api/2.1/unity-catalog/tables/'+source)
            for props,meta in ((sp,source_metadata),(tp,metadata)):
                if props.get('io.unitycatalog.tableId')!=meta.get('table_id'):raise Refused('precopy:independent-UC-table-ID')
                for key in check['distinct_physical_identifiers']:
                    if props[key]!=meta.get('properties',{}).get(key):raise Refused('precopy:independent-UC-physical-identifiers')
            self.save('copies/'+label+'_properties.json',{'source':sp,'target':tp,'check':check})
        names=[n for n,_,_,_ in (NATIVE_SCHEMA if kind=='check' else NATIVE_SCHEMA[:4])]
        def cell(v):return 'cast(null as string)' if v is None else "'"+v.replace("'","''")+"'"
        sql='SELECT '+','.join('cast('+n+' as string) AS '+n for n in names)+' FROM VALUES '+','.join('('+','.join(cell(v) for v in row)+')' for row in rows)+' AS declared('+','.join(names)+')'
        direct=self.sql(label+'_independent_literal',sql)
        if Counter(map(tuple,direct['rows']))!=Counter(map(tuple,rows)):raise Refused('precopy:literal-reference')
        show_reference(self,label,sql,rows,direct,validator=assert_show)
        self.save('checks/'+label+'.json',{'physical_complete_schema_bag':True,'independent_literal_and_disabled_native_show':True,'copied':copied})

    def invocation(self,kind,cell):
        label,role,stage,_,copy,verdict=cell;label=kind+'_'+label
        target=self.scope.relation(role,kind);source=self.scope.relation('prod',kind)
        calls=self.dbt(label,['run' if kind=='merge' else 'snapshot','--select',kind],role)
        if any(c.get('status',{}).get('code')!=0 for c in calls):raise Refused('precopy:RPC-status')
        clones=[c for c in calls if c['method']=='RegisterClone']
        if len(clones)!=int(copy):raise Refused('precopy:unexpected-native-copy-count')
        confirms={c['request']['request_id']:c for c in calls if c['method']=='ConfirmExecution' and c['response'].get('success') is True}
        if copy:
            row=clones[0];req=row['request'];ready=row['response'].get('ready_to_clone')
            if (not ready or ready!=row['response'].get('response',{}).get('ready_to_clone_v1')
                    or req['clone_source_table'].replace('`','')!=source or req['target_table'].replace('`','')!=target
                    or ready['request_id'] not in confirms):raise Refused('precopy:grant-mirrors-source-confirm')
            original=self.source_requests[kind]
            if (req['labels']['dbt_node_unique_id']!=original['dbt_node_state']['node_unique_id']
                    or req['table_namespace']!=original['table_namespace']
                    or req['clone_source_last_modified_epoch']!=self.source_confirms[kind]['last_modified_epoch']):
                raise Refused('precopy:source-UID-namespace-epoch')
            ddl=ready['clone_sqls']
            wanted='\n        CREATE OR REPLACE TABLE '+req['target_table']+' SHALLOW CLONE '+req['clone_source_table']+'\n        '
            if ddl!=[wanted]:raise Refused('precopy:exact-native-DDL')
            self.save('copies/'+label+'.json',{'request':req,'response':row['response'],'confirmation':confirms[ready['request_id']], 'ddl':ddl})
        submits=[c for c in calls if c['method']=='SubmitEnrichedSQL']
        if len(submits)!=1 or set(submits[0]['response']['response'])!={verdict}:raise Refused('precopy:submit-decision')
        req=submits[0]['request']
        if (req['execution_type']!=(3 if kind=='merge' else 7) or req['compare_unrendered_code'] or req['query_dependencies']
                or req['target_table'].replace('`','')!=target or not req.get('table_namespace')):raise Refused('precopy:submit-scope')
        normalized=re.sub(r'\s+',' ',req['sql'].replace('`','')).strip().lower()
        if normalized!='select * from '+self.scope.relation('prod','inputs'):raise Refused('precopy:actual-direct-single-input')
        if verdict=='ready_to_execute' and submits[0]['response']['response'][verdict]['request_id'] not in confirms:raise Refused('precopy:build-confirm')
        if role=='prod':
            self.source_requests[kind]=req
            self.source_confirms[kind]=confirms[submits[0]['response']['response'][verdict]['request_id']]['request']
        elif req['dbt_node_state']['node_unique_id']!=self.source_requests[kind]['dbt_node_state']['node_unique_id']:
            raise Refused('precopy:adoption-fullUID')
        self.audit_native(label,target,source,copy)
        self.explain(label,submits)
        self.physical(label,kind,role,stage,role=='dev',copy)

    def audit_native(self,label,target,source,copy,allow_failed_attempt=False):
        raw=(self.evidence(label)/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        self.save('materialization/'+label+'.sql',raw)
        code=re.sub(r'/\*.*?\*/|--[^\n]*','',raw,flags=re.S)
        relation=r'(`[^`]+`(?:\s*\.\s*`[^`]+`)*|[a-zA-Z_][a-zA-Z_0-9.]*)'
        normalize=lambda s:re.sub(r'\s+','',s).replace('`','').lower()
        temp=target.rsplit('.',1)[-1]+'__dbt_tmp'
        writes=[];clones=[]
        for sql in code.split(';'):
            sql=sql.strip()
            if not re.match(r'(?i)^(create|drop|alter|merge|insert|update|delete)\b',sql):continue
            create=re.match(r'(?is)^create\s+(?:or\s+replace\s+)?(temporary\s+view|table|view)\s+'+relation,sql)
            drop=re.match(r'(?is)^drop\s+(table|view)\s+(?:if\s+exists\s+)?'+relation,sql)
            change=re.match(r'(?is)^(?:merge\s+into|insert\s+into(?:\s+table)?|update|delete\s+from)\s+'+relation,sql)
            if create:
                kind,name=create.group(1).lower(),normalize(create.group(2))
                if name!=(temp if kind=='temporary view' else target) or kind=='view':raise Refused('precopy:unapproved-native-create')
                if re.search(r'(?i)\bshallow\s+clone\b',sql):
                    clone=re.fullmatch(r'(?is)create\s+or\s+replace\s+table\s+'+relation+r'\s+shallow\s+clone\s+'+relation,sql)
                    if not clone or tuple(map(normalize,clone.groups()))!=(target,source):raise Refused('precopy:executed-clone-DDL')
                    clones.append(sql)
            elif drop:
                if drop.group(1).lower()!='view' or normalize(drop.group(2))!=temp:raise Refused('precopy:unapproved-native-drop')
            elif re.match(r'(?i)^alter\b',sql):
                alter=re.fullmatch(r'(?is)alter\s+table\s+'+relation+r'\s+cluster\s+by\s+none',sql)
                if not alter or normalize(alter.group(1))!=target:raise Refused('precopy:unapproved-native-alter')
            elif not change or normalize(change.group(1))!=target:raise Refused('precopy:unapproved-native-write')
            writes.append(sql)
        if len(clones)!=int(copy) and not (allow_failed_attempt and not copy and len(clones)==1):raise Refused('precopy:granted-DDL-not-executed')
        self.save('materialization/'+label+'.json',{'writes':writes,'executed_clones':len(clones),'persistent_auxiliary':0,'temporary_allowlist':[temp]})

    def baseline(self):
        self.preflight();self.write_projects('default')
        source=self.scope.relation('prod','inputs')
        if not self.prefix:
            self.sql('create_source',f'CREATE TABLE {source} (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
            self.sql('initial_source','INSERT INTO '+source+' VALUES '+literal_source(FIRST))
        else:assert_source(self.sql('resume_source',source_projection_sql(source)),CHANGED if len(self.prefixes)==2 else FIRST)
        for stage in ('initial','changed','always'):
            if stage=='changed' and len(self.prefixes)<2:
                row=CHANGED[0]
                self.sql('change_source',f"UPDATE {source} SET v='{row[1]}',p=DATE '{row[2]}',ts=TIMESTAMP '{row[3]}' WHERE id=1")
                self.sql('insert_source','INSERT INTO '+source+' VALUES '+literal_source((CHANGED[2],)))
            if stage=='always':self.write_projects('always')
            labels={'initial':('source','absent','repeat'),'changed':('changed','changed_repeat'),'always':('source_always','existing')}[stage]
            for kind in ('merge','check'):
                for cell in CELLS:
                    if cell[0] in labels:self.invocation(kind,cell)
        if self.command_index!=len(self.commands):raise Refused('precopy:incomplete-command-budget')


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
    return Precopy(args).run()

if __name__=='__main__':raise SystemExit(main())
