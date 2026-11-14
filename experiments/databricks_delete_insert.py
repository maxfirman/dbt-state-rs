"""Bounded #149 native null-safe key replacement; offline until root clearance."""
from collections import Counter
from dataclasses import dataclass
from types import MappingProxyType

SCHEMA=(('id','BIGINT','LONG',0),('v','STRING','STRING',1),
        ('p','DATE','DATE',2),('ts','TIMESTAMP','TIMESTAMP',3))
PREDICATE="p = DATE '2026-11-01' OR p IS NULL"

@dataclass(frozen=True)
class Snapshot:
    source:tuple
    R:tuple
    F:tuple

# Literal independently worked bags: no warehouse, State, or prior output input.
a=('1','old-a','2026-11-01','2026-11-01T12:00:00.123Z')
b=('1','old-b','2026-11-01','2026-11-01T12:00:00.123Z')
c=('2','old-two','2026-11-01','2026-11-02T12:00:00.456Z')
n=(None,'old-null',None,None)
d=('3','old-three','2026-11-01','2026-11-03T12:00:00.789Z')
x=('1','new-one','2026-11-01','2026-11-01T12:00:00.123Z')
y=('4','new-four','2026-11-01','2026-11-04T12:00:00.012Z')
z=('2','new-two','2026-11-02','2026-11-02T12:00:00.456Z')
na=(None,'new-null',None,None)
HISTORY=MappingProxyType({
 'first':Snapshot((a,b,c,n,n,d),(a,b,c,n,n,d),(a,b,c,n,n,d)),
 'update':Snapshot((x,x,y,z,n,n),(x,x,y,c,n,n,d),(x,x,y,n,n)),
 'remove_null':Snapshot((x,x,y,z),(x,x,y,c,n,n,d),(x,x,y)),
 'restore_null':Snapshot((x,x,y,z,na),(x,x,y,c,na,d),(x,x,y,na)),
 'empty':Snapshot((z,),(x,x,y,c,na,d),()),
 'refresh':Snapshot((z,),(z,),(z,)),
 'next':Snapshot((z,),(z,),()),
})
CELLS=(('first','first','ready_to_execute'),('first_repeat','first','skip_execution'),
 ('update','update','ready_to_execute'),('update_repeat','update','skip_execution'),
 ('remove_null','remove_null','ready_to_execute'),('remove_null_repeat','remove_null','skip_execution'),
 ('restore_null','restore_null','ready_to_execute'),('restore_null_repeat','restore_null','skip_execution'),
 ('empty','empty','ready_to_execute'),('empty_repeat','empty','skip_execution'),
 ('full_refresh','refresh','record'),('next_ordinary','next','ready_to_execute'),('next_repeat','next','skip_execution'))

def replacement(previous,incoming,key_columns=(0,)):
    """Native <=> matches complete nullable tuples; incoming is a bag."""
    keys={tuple(row[i] for i in key_columns) for row in incoming}
    return tuple(row for row in previous if tuple(row[i] for i in key_columns) not in keys)+tuple(incoming)

def ledger():
    return {'version':1,'ticket':149,'schema':SCHEMA,'predicate':PREDICATE,'keys':['id'],
            'cells':CELLS,'history':{k:{'source':v.source,'R':v.R,'F':v.F} for k,v in HISTORY.items()},
            'basis':'independent immutable literal multiset key-replacement history; NULL-safe <=>'}

def clearance_contract():
    schema='cdo_demo_db.state_20261114_append146'
    runs={'hosted':'20261114a149aa','rust':'20261114b149bb'}
    return {'ticket':149,'catalog':'cdo_demo_db','schema':schema.split('.')[1],
      'schema_id':'6f14be2c-a920-4309-8240-caf8e8df7888','run_ids':runs,
      'max_final_permanent':4,'max_extra_concurrent_persistent':1,
      'permanent_exact_allowlists':{k:[f'{schema}.di_{v}_{n}' for n in ('inputs','consumer')] for k,v in runs.items()},
      'session_temporary_unqualified_names':{k:f'di_{v}_consumer__dbt_tmp' for k,v in runs.items()},
      'persistent_auxiliary_exact_spellings':{k:[f'{schema}.di_{v}_consumer{suffix}' for suffix in ('__dbt_stg','__dbt_backup')] for k,v in runs.items()},
      'limits':{'managed_per_backend':13,'disabled_select_only_references_per_backend':13,'parse_per_backend':1},
      'protected_permanent_names':[f'{schema}.{prefix}_20261114{r}_{name}'
        for prefix,suffix in [('a','146'),('io','147'),('rw','148')]
        for r in ('a'+suffix+'aa','b'+suffix+'bb') for name in ('inputs','consumer')]}

from databricks_survey import Refused
def assert_typed(actual,expected):
    schema=tuple(tuple(c.get(k) for k in ('name','type_text','type_name','position')) for c in actual['columns'])
    if schema!=SCHEMA or any(type(c.get('position')) is not int for c in actual['columns']):
        raise Refused('physical:schema')
    if Counter(map(tuple,actual['rows']))!=Counter(map(tuple,expected)):
        raise Refused('physical:rows')


def assert_result(actual,properties,expected):
    assert_typed(actual,expected)
    props={r[0]:r[1] for r in properties['rows']}
    if props.get('Type')!='MANAGED' or props.get('Provider','').lower()!='delta':
        raise Refused('physical:properties')


def literal_reference(rows):
    """Independent read-only literal relation, including a fully typed empty set."""
    types=('bigint','string','date','timestamp')
    def cell(value,kind):
        if value is None:return 'cast(null as '+kind+')'
        if kind=='bigint':return str(int(value))
        return "cast('"+value.replace("'","''")+"' as "+kind+')'
    projection='select cast(id as bigint) id, cast(v as string) v, cast(p as date) p, cast(ts as timestamp) ts '
    if not rows:
        return projection+'from (select cast(null as bigint) id, cast(null as string) v, cast(null as date) p, cast(null as timestamp) ts) declared where false'
    return projection+'from values '+','.join('('+','.join(cell(v,t) for v,t in zip(r,types))+')' for r in rows)+' as declared(id,v,p,ts)'


import json
import re
from databricks_readonly_reference import unique_object


def assert_reference(stdout,stderr,direct,expected,sql,query_log,compiled):
    """Exact bounded DATE/TIMESTAMP JSON codec; uncharacterized forms stop."""
    if stderr.strip():raise Refused('reference:stderr')
    try:rows=json.loads(stdout,object_pairs_hook=unique_object)
    except (TypeError,ValueError) as error:raise Refused('reference:json') from error
    if not isinstance(rows,list):raise Refused('reference:json-array')
    actual=[]
    for row in rows:
        if not isinstance(row,dict) or list(row)!=['id','v','p','ts']:raise Refused('reference:columns')
        a,v,p,ts=row.values()
        if ((a is not None and (type(a) is not int or abs(a)>2**53-1))
                or (v is not None and type(v) is not str)
                or (p is not None and (type(p) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',p)))
                or (ts is not None and (type(ts) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z',ts)))):
            raise Refused('reference:uncharacterized-typed-codec')
        actual.append((None if a is None else str(a),v,p,ts))
    if Counter(actual)!=Counter(map(tuple,expected)):raise Refused('reference:rows')
    assert_typed(direct,expected)
    query='\n'.join(line for line in query_log.splitlines() if not line.startswith('--'))
    normalize=lambda s:re.sub(r'\s+',' ',s).strip()
    if (not re.match(r'(?i)^select\s',sql) or ';' in sql
            or normalize(compiled)!=normalize(sql) or query.count(';')!=1
            or not query.rstrip().endswith(';') or normalize(query.rstrip()[:-1])!=normalize(sql)):
        raise Refused('reference:query-sql')
    return {'complete_show_rows':len(rows),'typed_schema':True,'exact_select_only':True,
            'duplicate_null_multiset':True,'exact_temporal_codec':True}



from databricks_baseline import LeaseScope
from databricks_freshness import decisions
from databricks_survey import Scope
class DeleteInsertScope(LeaseScope):
    profile_roles=('prod',)
    def __init__(self,run,clearance):
        proposal=clearance_contract()
        if any(clearance.get(k)!=v for k,v in proposal.items()):raise Refused('delete+insert exact footprint differs')
        if run not in proposal['run_ids'].values():raise Refused('uncleared backend run')
        Scope.__init__(self,run)
        phase=next(k for k,v in proposal['run_ids'].items() if v==run)
        self.roles={'prod':proposal['schema']};self.schemas=set(self.roles.values())
        self.relation_prefix='di_'+run+'_'
        self.permanent=set(proposal['permanent_exact_allowlists'][phase])
        self.auxiliary=set(proposal['persistent_auxiliary_exact_spellings'][phase])
        self.allowed=self.permanent|self.auxiliary
        self.temporary=proposal['session_temporary_unqualified_names'][phase]


def delete_insert_submit(calls,source,target,target_exists,expected):
    submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
    if len(submits)!=1:raise Refused('expected one actual delete+insert submit')
    request=submits[0]['request'];extras=request['semantic_extras']
    if (request.get('dialect')!='databricks' or request['execution_type']!=5 or request['compare_unrendered_code']
            or not request.get('table_namespace') or request['query_dependencies']
            or extras.get('unique_key')!='"id"' or extras.get('on_schema_change')!='"ignore"'
            or extras.get('file_format')!='"delta"'
            or extras.get('incremental_predicates') is not None
            or request.get('clone_chain_depth_limit')!=0 or request.get('ignore_external_modifications')):
        raise Refused('outside bounded rendered delete+insert scope')
    normalize=lambda name:name.replace('`','').lower()
    evidence=request['tables'];wanted={source,target} if target_exists else {source}
    if (len(evidence)!=len(wanted) or {normalize(t['name']) for t in evidence}!=wanted
            or any(type(t.get('last_modified_epoch')) is not int or t['last_modified_epoch']<=0 for t in evidence)
            or normalize(request['target_table'])!=target):raise Refused('delete+insert needs exact known positive epochs')
    decisions(calls,expected)
    if any(r['method'] in ('RecordExecutions','RegisterClone') for r in calls):raise Refused('unexpected ordinary Record/copy')
    if expected=='skip_execution' and any(r['method']=='ConfirmExecution' for r in calls):raise Refused('reuse unexpectedly confirmed')
    return submits


def full_refresh_record(calls,target):
    if any(r['method'] in ('SubmitEnrichedSQL','ConfirmExecution','RegisterClone') for r in calls):
        raise Refused('refresh route differs from FULL Record path')
    records=[r for r in calls if r['method']=='RecordExecutions']
    if (len(records)!=1 or len(records[0]['request']['records'])!=1
            or records[0]['response']!={'records_stored':1}
            or type(records[0]['response']['records_stored']) is not int):raise Refused('refresh lacks acknowledged record')
    record=records[0]['request']['records'][0];request=record['input']['enriched_sql'];outcome=record['outcome']
    if (request['execution_type']!=1 or request['target_table'].replace('`','').lower()!=target
            or type(outcome.get('last_modified_epoch')) is not int or outcome['last_modified_epoch']<=0
            or type(outcome.get('execution_runtime_ms')) is not int or outcome['execution_runtime_ms']<=0):
        raise Refused('refresh record lacks FULL known positive epoch/runtime')
    return record


import argparse
import hashlib
import inspect
from pathlib import Path
from databricks_merge import Merge
from databricks_readonly_reference import show_reference
from databricks_survey import Campaign, check_manifest


def native_writes(raw):
    text=re.sub(r'/\*.*?\*/','',raw,flags=re.S)
    text='\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))
    return [re.sub(r'\s+',' ',q.replace('`','')).strip().lower() for q in text.split(';')
            if re.match(r'(?i)^\s*(create|drop|alter|merge|insert|update|delete|set)\b',q)]


def assert_replace_on_sql(raw,target,temporary):
    writes=native_writes(raw)
    expected=(r'insert into table '+re.escape(target)+r' as target replace on '
      r'\(\s*target\.id\s*<=>\s*temp\.id\s*\) '
      r'\(\s*select id,\s*v,\s*p,\s*ts from '+re.escape(temporary)+r'\s*\) as temp')
    inserts=[q for q in writes if re.fullmatch(expected,q)]
    temps=[q for q in writes if q.startswith('create or replace temporary view '+temporary+' as select ')]
    alters=[q for q in writes if q=='alter table '+target+' cluster by none']
    if len(inserts)!=1 or len(temps)!=1 or len(alters)>1 or len(writes)!=len(inserts)+len(temps)+len(alters):
        raise Refused('native SQL differs from exact null-safe key replacement footprint')


def native_footprint(raw,target,temporary,target_existed):
    """Bound native V1/V2 lifetimes; staging and backup share one reserve."""
    writes=native_writes(raw);active={target} if target_existed else set()
    peak=len(active);staging=target+'__dbt_stg';backup=target+'__dbt_backup'
    relation=r'([a-z0-9_.]+)'
    for sql in writes:
        create=re.fullmatch(r'create (?:or replace )?(temporary view|table) '+relation+r' (.+)',sql)
        rename=re.fullmatch(r'alter table '+relation+r' rename to '+relation,sql)
        drop=re.fullmatch(r'drop (table|view) (?:if exists )?'+relation,sql)
        if create:
            kind,name,body=create.groups()
            if kind=='temporary view':
                if name!=temporary or not body.startswith('as select '):raise Refused('foreign session temporary view')
            else:
                if name not in (target,staging) or not re.fullmatch(r'using delta as select .+',body):raise Refused('foreign persistent CREATE')
                active.add(name)
        elif rename:
            old,new=rename.groups()
            if (old,new) not in ((target,backup),(staging,target)) or old not in active or new in active:raise Refused('unapproved safe replacement rename')
            active.remove(old);active.add(new)
        elif drop:
            kind,name=drop.groups()
            if kind=='view' and name==temporary:pass
            elif kind=='table' and name==backup and backup in active:active.remove(backup)
            else:raise Refused('unapproved persistent DROP')
        elif sql=='alter table '+target+' cluster by none':pass
        elif re.match(r'insert into table '+re.escape(target)+r' as target replace on ',sql):
            if target not in active:raise Refused('replacement target absent')
        else:raise Refused('unreviewed native mutation')
        peak=max(peak,len(active))
        if peak>2:raise Refused('persistent peak exceeds target plus one shared auxiliary')
    if active!={target}:raise Refused('native auxiliary lifetime not closed')
    return {'writes':writes,'peak_consumer_lifetime_objects':peak,
      'peak_extra_persistent_beyond_target':max(0,peak-1),'session_temporary':temporary}


def assert_client_status(results,uid,target,expected):
    rows=results.get('results',[])
    if len(rows)!=1:raise Refused('client result omitted or added model')
    row=rows[0];status='reused' if expected=='skip_execution' else 'success'
    if (row.get('unique_id')!=uid or row.get('relation_name','').replace('`','').lower()!=target
            or row.get('status')!=status or row.get('failures') is not None):
        raise Refused('client status/UID/target differs from pinned DELETE_INSERT evidence')
    adapter=row.get('adapter_response')
    if status=='reused':
        if adapter!={}:raise Refused('reused client reports native execution')
    elif not isinstance(adapter,dict) or adapter.get('code')!='SUCCESS' or not adapter.get('query_id'):
        raise Refused('executed client lacks successful query response')
    return {'status':status,'uid':uid,'adapter_response':adapter}


class DeleteInsert(Merge):
    entrypoint=Path(__file__)
    def __init__(self,args):
        clearance=json.loads(Path(args.live_clearance).read_text());proposal=clearance_contract()
        self.phase='rust' if args.rust_url else 'hosted'
        if (not clearance.get('status','').startswith('LIVE CLEARED')
                or self.phase not in clearance.get('authorized_phases',[])
                or args.run_id!=proposal['run_ids'][self.phase]):raise Refused('backend lacks separate exact root clearance')
        scope=DeleteInsertScope(args.run_id,clearance)
        if args.rust_url and not re.fullmatch(r'http://127\.0\.0\.1:\d+',args.rust_url):raise Refused('Rust endpoint must be owned loopback')
        # #147 introduces this shared validator seam; consume only after canonical merge.
        if 'validator' not in inspect.signature(show_reference).parameters:
            raise Refused('canonical shared reference validator seam not yet available')
        raw=Path(args.operation_ledger).read_bytes()
        if (hashlib.sha256(raw).hexdigest()!=clearance['operation_ledger_sha256']
                or json.loads(raw)!=json.loads(json.dumps(ledger()))):raise Refused('immutable independent ledger differs')
        allocation=clearance['allocation']
        if hashlib.sha256(Path(allocation['receipt']).read_bytes()).hexdigest()!=allocation['receipt_sha256']:
            raise Refused('allocation receipt digest differs')
        self.clearance=clearance;self.lease=clearance;self.query_identity=args.query_identity or args.run_id
        Scope(self.query_identity)
        Campaign.__init__(self,args,scope)
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            profile['survey']['outputs'][role]={k:v for k,v in output.items() if k in {'type','host','http_path','catalog','schema','token','threads'}}
        self.save('profiles/profiles.yml',profile)
        self.save('live-clearance.json',clearance);self.save('operation-manifest.json',raw.decode())
        self.save('source-provenance.json',{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('*.py')})
        self.save('campaign-entrypoint.py',self.entrypoint.read_text())
        self.save('binary-provenance.json',{name:{'path':str(Path(getattr(args,name)).resolve()),
                  'sha256':hashlib.sha256(Path(getattr(args,name)).read_bytes()).hexdigest()} for name in ('dbt','proxy','probe')})
        self.save('footprint.json',{'permanent':sorted(scope.permanent),'auxiliary':sorted(scope.auxiliary),
                  'session_temporary':scope.temporary,'max_extra_persistent':1,'reference_objects':0,'cleanup':False})

    def init_schemas(self):
        allocation=self.clearance['allocation']
        schema=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+self.scope.schema('prod'))
        if any(schema.get(k)!=allocation[k] for k in ('schema_id','owner','created_at')):raise Refused('allocation UUID/owner/creation differs')
        inventory=self.inventory();actual=inventory[self.scope.schema('prod')]
        prior=set(self.clearance['protected_permanent_names'])
        if self.phase=='rust':prior.update(self.clearance['permanent_exact_allowlists']['hosted'])
        if {r['full_name'] for r in actual}!=prior:raise Refused('allocation inventory differs from exact protected prior set')
        if len(actual)+3>100:raise Refused('insufficient permanent plus one transient headroom')
        self.before_inventory=inventory
        self.save('allocation-identity-before.json',schema);self.save('lease-inventory-before.json',inventory)

    def audit_native(self,label,target_existed):
        raw=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        target=self.scope.relation('prod','consumer')
        expected=next(c[2] for c in CELLS if c[0]==label)
        writes=native_writes(raw)
        if expected=='skip_execution':
            if writes:raise Refused('SKIP emitted native writes')
            footprint={'writes':[],'peak_extra_persistent_beyond_target':0,'session_temporary':self.scope.temporary}
        else:
            footprint=native_footprint(raw,target,self.scope.temporary,target_existed)
            if (label=='first' or expected=='record') and (not any(q.startswith(('create table ','create or replace table ')) for q in writes) or any(q.startswith('insert ') for q in writes)):raise Refused('CTAS route lacks exact native table creation')
            if label!='first' and expected!='record':assert_replace_on_sql(raw,target,self.scope.temporary)
        self.save('materialization/'+label+'.sql',raw)
        self.save('materialization/'+label+'.json',footprint)

    def checked_results(self,label,stage):
        case=HISTORY[stage];source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
        source_rows=self.observe(label+'_source',source)
        source_props=json.loads((self.work/'physical'/f'{label}_source_properties.json').read_text())
        assert_result(source_rows,source_props,case.source)
        self.save('history/'+label+'_source.json',self.sql(label+'_source_history',f'DESCRIBE HISTORY {source}'))
        actual=self.observe(label,target)
        props=json.loads((self.work/'physical'/f'{label}_properties.json').read_text());assert_result(actual,props,case.R)
        history=self.sql(label+'_history',f'DESCRIBE HISTORY {target}')
        if not history['rows']:raise Refused('missing complete Delta history')
        version_col=next((i for i,c in enumerate(history['columns']) if c['name']=='version'),None)
        if version_col is None:raise Refused('missing Delta version column')
        version=max(int(r[version_col]) for r in history['rows'])
        expected=next(c[2] for c in CELLS if c[0]==label)
        if hasattr(self,'last_version'):
            if expected=='skip_execution' and version!=self.last_version:raise Refused('SKIP changed physical Delta version')
            if expected!='skip_execution' and version<self.last_version:raise Refused('Delta version regressed')
        self.last_version=version
        self.save('history/'+label+'.json',history)
        sql=literal_reference(case.R);direct=self.sql(label+'_cumulative_R_literal',sql);assert_typed(direct,case.R)
        show_reference(self,label,sql,case.R,direct,validator=assert_reference)
        selected=self.sql(label+'_selected_F_literal',literal_reference(case.F));assert_typed(selected,case.F)
        filter_sql='' if stage=='refresh' else ' WHERE '+PREDICATE
        fresh=self.sql(label+'_selected_F_source',f'SELECT id,v,p,ts FROM {source}'+filter_sql);assert_typed(fresh,case.F)
        after=self.inventory()
        for schema,previous in self.before_inventory.items():
            if any(r not in after[schema] for r in previous):raise Refused('preexisting inventory changed')
            if any(r['full_name'] not in self.scope.permanent for r in after[schema] if r not in previous):
                raise Refused('unplanned final physical object')
        self.save('inventory/'+label+'.json',after)
        self.save('checks/'+label+'.json',{'complete_typed_source_R_F':True,'managed_delta_history':True,
                  'zero_RPC_disabled_SELECT_reference':True,'stage':stage,'R_count':len(case.R),'F_count':len(case.F)})

    def invocation(self,label,stage,expected):
        case=HISTORY[stage]
        self.save('expected/'+label+'.json',{'stage':stage,'source':case.source,'R':case.R,'F':case.F,'keys':['id']})
        calls=self.dbt(label,['run','--select','consumer']+(['--full-refresh'] if expected=='record' else []))
        results=json.loads((self.work/'targets'/f'{self.n:03d}_{label}'/'run_results.json').read_text())
        status=assert_client_status(results,'model.survey_'+self.a.run_id+'.consumer',self.scope.relation('prod','consumer'),expected)
        self.save('client-status/'+label+'.json',status)
        self.audit_native(label,label!='first')
        target=self.scope.relation('prod','consumer');source=self.scope.relation('prod','inputs')
        query=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        if expected=='record':
            record=full_refresh_record(calls,target);self.last_runtime=record['outcome']['execution_runtime_ms']
            self.confirmed_epoch=record['outcome']['last_modified_epoch']
            self.save('full-refresh-wire.json',calls)
        else:
            submits=delete_insert_submit(calls,source,target,label!='first',expected)
            request=submits[0]['request'];evidence={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in request['tables']}
            if hasattr(self,'previous_sql') and request['sql']!=self.previous_sql:raise Refused('fixed first/steady SQL changed')
            if hasattr(self,'namespace') and request['table_namespace']!=self.namespace:raise Refused('connection namespace changed')
            self.namespace=request['table_namespace']
            if label in ('update','remove_null','restore_null','empty') and evidence[source]<=self.previous_epochs[source]:
                raise Refused('changed source epoch did not advance; #140 remains incomplete')
            if expected=='skip_execution':
                if request['sql']!=self.previous_sql or request['semantic_extras']!=self.previous_extras:raise Refused('reuse changed SQL/config')
                if evidence[source]!=self.previous_epochs[source] or evidence[target]!=self.confirmed_epoch:
                    raise Refused('unchanged positive reuse differs from own confirmed/source epochs')
                response=submits[0]['response']['response'][expected]
                if response.get('is_stale'):raise Refused('positive reuse is stale')
                if response['execution_runtime_ms']!=self.last_runtime:raise Refused('SKIP runtime differs from own confirmed replacement')
            else:
                confirms=[r for r in calls if r['method']=='ConfirmExecution' and r.get('response',{}).get('success')]
                if len(confirms)!=1:raise Refused('replacement lacks exactly one accepted Confirm')
                self.last_runtime=confirms[0]['request']['execution_runtime_ms']
                self.confirmed_epoch=confirms[0]['request']['last_modified_epoch']
                if type(self.confirmed_epoch) is not int or self.confirmed_epoch<=0:raise Refused('missing confirmed target epoch')
                if type(self.last_runtime) is not int or self.last_runtime<=0:raise Refused('missing positive runtime')
                if label!='first':assert_replace_on_sql(query,target,self.scope.temporary)
            self.previous_epochs=evidence;self.previous_sql=request['sql'];self.previous_extras=request['semantic_extras']
            self.explain(label,submits);self.save('epochs/'+label+'.json',{'tables':request['tables'],'expected':expected})
        self.checked_results(label,stage)

    def model(self):
        config="materialized='incremental',incremental_strategy='delete+insert',unique_key='id',file_format='delta',on_schema_change='ignore',state={'lag_tolerance':'0s','pre_clone':'never'}"
        self.write_model('consumer',config,"select id,v,p,ts from {{ source('delete_insert_history','inputs') }} {% if not flags.FULL_REFRESH %} where "+PREDICATE+" {% endif %}")

    def baseline(self):
        self.setup_project();source=self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'delete_insert_history','database':'cdo_demo_db',
          'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.model()
        if self.dbt('inventory',['parse'],managed=False):raise Refused('State-disabled parse emitted RPCs')
        manifest=json.loads(next((self.work/'targets').glob('*_inventory/manifest.json')).read_text());check_manifest(self.scope,manifest)
        node=next(v for v in manifest['nodes'].values() if v['name']=='consumer')
        if (node.get('language')!='sql' or node['config']['incremental_strategy']!='delete+insert'
                or node['config'].get('file_format')!='delta' or node['config'].get('pre-hook') or node['config'].get('post-hook')):
            raise Refused('manifest outside pinned native SQL managed Delta scope')
        for n in [*manifest['nodes'].values(),*manifest['sources'].values()]:
            self.scope.check('.'.join([n['database'],n['schema'],n.get('alias') or n['identifier']]))
        self.save('actual-default-configuration.json',{'node':node,'metadata':manifest['metadata'],'feature_overrides':False,'macro_overrides':False})
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
        self.sql('initial_source',f'INSERT INTO {source} '+literal_reference(HISTORY['first'].source))
        for label,stage,expected in CELLS:
            if label=='update':
                self.sql('replace_source',f'INSERT OVERWRITE TABLE {source} '+literal_reference(HISTORY['update'].source))
            elif label=='remove_null':self.sql('remove_source_null',f'DELETE FROM {source} WHERE id IS NULL')
            elif label=='restore_null':self.sql('restore_source_null',f'INSERT INTO {source} '+literal_reference((na,)))
            elif label=='empty':self.sql('empty_selected_source',f'DELETE FROM {source} WHERE '+PREDICATE)
            self.invocation(label,stage,expected)
        return {'managed_runs':13,'reference_objects':0,'whole_ticket_complete':False,'dependencies_138_140_complete':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','probe','work-dir','live-clearance','operation-ledger','run-id'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--query-identity');parser.add_argument('--rust-url')
    return DeleteInsert(parser.parse_args()).run()


if __name__=='__main__':raise SystemExit(main())
