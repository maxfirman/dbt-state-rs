#!/usr/bin/env python3
"""Bounded #150 offline operation model. No live authorization is implied."""
from collections import Counter
from dataclasses import dataclass
from datetime import datetime,timezone
from types import MappingProxyType
from databricks_survey import Refused

SCHEMA=(('id','BIGINT','LONG',0),('v','STRING','STRING',1),('p','DATE','DATE',2),('ts','TIMESTAMP','TIMESTAMP',3))
a=('1','old','2026-11-01','2026-11-01T00:00:00.000Z')
a_new=('1','updated','2026-11-01','2026-11-01T00:00:00.000Z')
b=('2','duplicate','2026-11-02','2026-11-02T12:00:00.123Z')
null_id=(None,'nullable-key','2026-11-02','2026-11-02T23:59:59.999Z')
null_time=('3','excluded-time',None,None)
edge=('4','end-boundary','2026-11-03','2026-11-03T00:00:00.000Z')
moved=('1','updated','2026-11-03','2026-11-03T12:00:00.456Z')
@dataclass(frozen=True)
class Snapshot:
    source:tuple
    window:tuple
    R:tuple
    F:tuple
# Literal bags independently worked before the operation implementation.
HISTORY=MappingProxyType({
 'first':Snapshot((a,b,b,null_id,null_time,edge),('2026-11-01','2026-11-03'),(a,b,b,null_id),(a,b,b,null_id)),
 'update':Snapshot((a_new,b,b,null_id,null_time,edge),('2026-11-01','2026-11-03'),(a_new,b,b,null_id),(a_new,b,b,null_id)),
 'shifted':Snapshot((moved,b,b,null_id,null_time,edge),('2026-11-02','2026-11-04'),(a_new,b,b,null_id,edge,moved),(b,b,null_id,edge,moved)),
 'overlap':Snapshot((moved,null_id,null_time,edge),('2026-11-01','2026-11-03'),(moved,edge,null_id),(null_id,)),
 'empty':Snapshot((moved,null_time,edge),('2026-11-02','2026-11-03'),(moved,edge),()),
})
STAGES=tuple(HISTORY)
CELLS=tuple((label,stage,kind) for stage in STAGES for label,kind in
            ((stage,'ready_to_execute'),(stage+'_repeat','skip_execution')))

def utc(value):
    value=datetime.fromisoformat(value.replace('Z','+00:00'))
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)

def in_window(row,start,end):
    """SQL >= AND < returns UNKNOWN for a NULL event time."""
    if row[3] is None:return None
    return utc(start)<=utc(row[3])<utc(end)

def selected(source,start,end):
    return tuple(row for row in source if in_window(row,start,end) is True)

def apply_window(previous,source,start,end):
    """Proposed non-NULL target operation; NULL retention requires live evidence."""
    if utc(start)>=utc(end):raise Refused('empty or reversed bounds')
    if any(row[3] is None for row in previous):raise Refused('target NULL retention unproved')
    return tuple(row for row in previous if in_window(row,start,end) is False)+selected(source,start,end)

def ledger():
    return {'version':1,'ticket':150,'schema':SCHEMA,'cells':CELLS,
      'history':{k:{'source':v.source,'window':v.window,'R':v.R,'F':v.F} for k,v in HISTORY.items()},
      'basis':'independent literal cumulative bags, command-ordered bounded replacement; NULL target unproved',
      'full_refresh':'DISCOVERY ONLY: no assumed Record execution type or batch behavior',
      'unverified':['target NULL retention','implicit moving clock/lookback','settings/predicates','concurrency','partial failure','full refresh']}

def clearance_contract():
    namespace='cdo_demo_db.state_20261114_append146'
    runs={'hosted':'20261114a150aa','rust':'20261114b150bb'}
    return {'ticket':150,'catalog':'cdo_demo_db','schema':namespace.split('.')[1],
      'schema_id':'6f14be2c-a920-4309-8240-caf8e8df7888','run_ids':runs,
      'max_final_permanent':4,'max_extra_concurrent_persistent':1,
      'permanent_exact_allowlists':{k:[f'{namespace}.mb_{v}_{n}' for n in ('inputs','consumer')] for k,v in runs.items()},
      'persistent_auxiliary_exact_spellings':{k:[f'{namespace}.mb_{v}_consumer{s}' for s in ('__dbt_stg','__dbt_backup')] for k,v in runs.items()},
      'session_temporary_unqualified_names':{k:[f'mb_{v}_consumer__dbt_tmp_2026110{day}' for day in (1,2,3)] for k,v in runs.items()},
      'limits':{'managed_per_backend':10,'disabled_select_only_references_per_backend':10,'parse_per_backend':1,
        'managed_executed_daily_batches_per_backend':9,'ordinary_submit_per_backend':10,
        'speculative_submit_per_backend':10,'confirm_per_backend':5,'record_per_backend':0,
        'managed_dbt_commands_per_backend':10,'reference_dbt_commands_per_backend':10},
      'requires_prior_windows_complete':[148,149],
      'protected_permanent_names':[f'{namespace}.{prefix}_20261114{r}_{name}'
        for prefix,suffix in [('a','146'),('io','147'),('rw','148'),('di','149')]
        for r in ('a'+suffix+'aa','b'+suffix+'bb') for name in ('inputs','consumer')]}

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
        # #147 primary preflight proved this exact lossless zero-fraction spelling.
        if type(ts) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',ts):
            ts=ts[:-1]+'.000Z' 
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



from datetime import timedelta

def days(window):
    start,end=map(utc,window)
    if start>=end or any(x.time().isoformat()!='00:00:00' for x in (start,end)):
        raise Refused('only bounded daily midnight windows')
    result=[]
    while start<end:
        next_day=start+timedelta(days=1)
        result.append((start.date().isoformat(),next_day.date().isoformat()))
        start=next_day
    return result

def assert_batch_sql(raw,target,source,window,target_existed):
    writes=native_writes(raw)
    wanted=[]
    for index,(start,end) in enumerate(days(window)):
        projection=("select * from (select * from "+source+" where ts >= '"+start+
                    "t00:00:00+00:00' and ts < '"+end+"t00:00:00+00:00')")
        if not target_existed and index==0:
            wanted.append('create or replace table '+target+' using delta as '+projection)
        else:
            temp=target.split('.')[-1]+'__dbt_tmp_'+start.replace('-','')
            wanted.append('create or replace temporary view '+temp+' as '+projection)
            wanted.append("insert into "+target+" by name replace where cast(ts as timestamp) >= '"+
              start+" 00:00:00+00:00' and cast(ts as timestamp) < '"+end+" 00:00:00+00:00' table "+temp)
    # Primary creation emits a final policy-neutral cluster statement.
    if writes and writes[-1]=='alter table '+target+' cluster by none':writes=writes[:-1]
    if writes!=wanted:raise Refused('native SQL differs from bounded daily microbatch footprint')
    return {'executed_daily_batches':len(days(window)),'writes':writes,
            'peak_extra_persistent_beyond_target':0}
from databricks_baseline import LeaseScope
from databricks_freshness import decisions
from databricks_survey import Scope
class MicrobatchScope(LeaseScope):
    profile_roles=('prod',)
    def __init__(self,run,clearance):
        proposal=clearance_contract()
        if any(clearance.get(k)!=v for k,v in proposal.items()):raise Refused('microbatch exact footprint differs')
        if run not in proposal['run_ids'].values():raise Refused('uncleared backend run')
        Scope.__init__(self,run)
        phase=next(k for k,v in proposal['run_ids'].items() if v==run)
        self.roles={'prod':proposal['schema']};self.schemas=set(self.roles.values())
        self.relation_prefix='mb_'+run+'_'
        self.permanent=set(proposal['permanent_exact_allowlists'][phase])
        self.auxiliary=set(proposal['persistent_auxiliary_exact_spellings'][phase])
        self.allowed=self.permanent|self.auxiliary
        self.temporary=proposal['session_temporary_unqualified_names'][phase]


class Microbatch(Merge):
    entrypoint=Path(__file__)
    def __init__(self,args):
        clearance=json.loads(Path(args.live_clearance).read_text());proposal=clearance_contract()
        self.phase='rust' if args.rust_url else 'hosted'
        if (not clearance.get('status','').startswith('LIVE CLEARED')
                or self.phase not in clearance.get('authorized_phases',[])
                or args.run_id!=proposal['run_ids'][self.phase]):raise Refused('backend lacks separate exact root clearance')
        scope=MicrobatchScope(args.run_id,clearance)
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
        filter_sql=" WHERE ts >= '"+case.window[0]+"T00:00:00+00:00' AND ts < '"+case.window[1]+"T00:00:00+00:00'"
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
        case=HISTORY[stage];source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
        self.save('expected/'+label+'.json',{'stage':stage,'window':case.window,'R':case.R,'F':case.F})
        calls=self.dbt(label,['run','--select','consumer','--event-time-start',case.window[0],'--event-time-end',case.window[1]])
        results=json.loads((self.work/'targets'/f'{self.n:03d}_{label}'/'run_results.json').read_text())
        row=results.get('results',[])
        if len(row)!=1 or row[0].get('status')!=('reused' if expected=='skip_execution' else 'success') or row[0].get('unique_id')!='model.survey_'+self.a.run_id+'.consumer' or row[0].get('relation_name','').replace('`','').lower()!=target:
            raise Refused('unexpected microbatch native status/UID/target')
        adapter=row[0].get('adapter_response')
        if row[0].get('failures') is not None or row[0].get('batch_results') is not None:
            raise Refused('client failures or uncharacterized per-batch response')
        if expected=='skip_execution':
            if adapter!={}:raise Refused('reused client reports execution')
        elif not isinstance(adapter,dict) or adapter.get('code')!='SUCCESS' or not adapter.get('query_id'):
            raise Refused('executed client lacks native success query id')
        self.save('client-status/'+label+'.json',results)
        raw=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        if expected=='skip_execution':
            if native_writes(raw):raise Refused('SKIP wrote target')
            footprint={'executed_daily_batches':0,'writes':[]}
        else:footprint=assert_batch_sql(raw,target,source,case.window,label!='first')
        self.save('materialization/'+label+'.json',footprint);self.save('materialization/'+label+'.sql',raw)
        submits=microbatch_submit(calls,source,target,label!='first',case.window,expected)
        speculative=[r for r in calls if r['method']=='SubmitEnrichedSQLSpeculative']
        if len(speculative)!=1:raise Refused('one whole-command speculative Submit required')
        self.save('command-counts/'+label+'.json',{'managed_cli_commands':1,'ordinary_submit':1,'speculative_submit':1,
                  'confirmed_commands':int(expected=='ready_to_execute'),'executed_daily_batches':footprint['executed_daily_batches']})
        request=submits[0]['request'];epochs={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in request['tables']}
        if hasattr(self,'previous_sql') and request['sql']!=self.previous_sql:raise Refused('unwindowed State SQL changed')
        if hasattr(self,'namespace') and request['table_namespace']!=self.namespace:raise Refused('namespace changed')
        self.namespace=request['table_namespace']
        if label in STAGES[1:] and epochs[source]<=self.previous_epochs[source]:raise Refused('changed source epoch failed to advance')
        if expected=='skip_execution':
            response=submits[0]['response']['response'][expected]
            explained=response.get('explained_decision')
            if (epochs[source]!=self.previous_epochs[source] or epochs[target]!=self.confirmed_epoch
                    or request['semantic_extras']!=self.previous_extras
                    or not isinstance(explained,dict) or explained.get('is_stale') is not False
                    or response['execution_runtime_ms']!=self.last_runtime):raise Refused('SKIP lacks own exact unchanged evidence/runtime')
        else:
            confirms=[r for r in calls if r['method']=='ConfirmExecution' and r.get('response',{}).get('success')]
            if len(confirms)!=1:raise Refused('whole-command Confirm missing or duplicated')
            self.last_runtime=confirms[0]['request']['execution_runtime_ms'];self.confirmed_epoch=confirms[0]['request']['last_modified_epoch']
            if any(type(v) is not int or v<=0 for v in (self.last_runtime,self.confirmed_epoch)):raise Refused('Confirm lacks own positive runtime/epoch')
        self.previous_epochs=epochs;self.previous_sql=request['sql'];self.previous_extras=request['semantic_extras']
        self.explain(label,submits);self.checked_results(label,stage)

    def baseline(self):
        timezone_result=self.sql('session_timezone', 'SELECT current_timezone() AS session_timezone')
        if timezone_result.get('rows')!=[['Etc/UTC']]:raise Refused('session timezone differs from pinned UTC primary')
        self.save('session-timezone.json',timezone_result)
        self.setup_project();source=self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'microbatch_history','database':'cdo_demo_db','schema':self.scope.schema('prod'),
          'tables':[{'name':'inputs','identifier':source.split('.')[-1],'config':{'event_time':'ts'}}]}]})
        self.write_model('consumer',"materialized='incremental',incremental_strategy='microbatch',unique_key='id',event_time='ts',begin='2026-11-01',batch_size='day',file_format='delta',on_schema_change='ignore',state={'lag_tolerance':'0s','pre_clone':'never'}","select * from {{ source('microbatch_history','inputs') }}")
        if self.dbt('inventory',['parse'],managed=False):raise Refused('disabled parse emitted RPC')
        manifest=json.loads(next((self.work/'targets').glob('*_inventory/manifest.json')).read_text());check_manifest(self.scope,manifest)
        node=next(n for n in manifest['nodes'].values() if n['name']=='consumer');config=node['config']
        if any(config.get(k)!=v for k,v in {'incremental_strategy':'microbatch','event_time':'ts','batch_size':'day','lookback':1,'file_format':'delta'}.items()) or config.get('pre-hook') or config.get('post-hook'):
            raise Refused('unreviewed native microbatch configuration')
        self.save('actual-default-configuration.json',{'node':node,'metadata':manifest['metadata']})
        for n in [*manifest['nodes'].values(),*manifest['sources'].values()]:self.scope.check('.'.join([n['database'],n['schema'],n.get('alias') or n['identifier']]))
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
        self.sql('initial_source',f'INSERT INTO {source} '+literal_reference(HISTORY['first'].source))
        for label,stage,expected in CELLS:
            if label=='update':self.sql('source_update',f"UPDATE {source} SET v='updated' WHERE id=1")
            elif label=='shifted':self.sql('source_timestamp_move',f"UPDATE {source} SET p=DATE '2026-11-03',ts=TIMESTAMP '2026-11-03 12:00:00.456' WHERE id=1")
            elif label=='overlap':self.sql('source_delete_duplicates',f'DELETE FROM {source} WHERE id=2')
            elif label=='empty':self.sql('source_empty_day',f'DELETE FROM {source} WHERE id IS NULL')
            self.invocation(label,stage,expected)
        return {'managed_runs':10,'daily_batches':9,'reference_runs':10,'whole_ticket_complete':False,'dependencies_138_140_complete':False}

from databricks_freshness import decisions

def microbatch_submit(calls,source,target,target_exists,window,expected):
    submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
    if len(submits)!=1:raise Refused('one whole-command Submit required, not per-batch')
    request=submits[0]['request'];extras=request['semantic_extras']
    wanted_extras={'unique_key':'"id"','file_format':'"delta"','event_time':'"ts"','on_schema_change':'"ignore"','lookback':'1',
      '__microbatch_event_time_start':window[0]+'T00:00:00+00:00','__microbatch_event_time_end':window[1]+'T00:00:00+00:00'}
    normalize=lambda s:s.replace('`','').lower().strip()
    if (request.get('execution_type')!=6 or request.get('dialect')!='databricks' or extras!=wanted_extras
            or request.get('default_catalog')!=source.split('.')[0]
            or request.get('default_schema')!=source.split('.')[1]
            or request.get('compare_unrendered_code') or not request.get('table_namespace') or request.get('query_dependencies')
            or request.get('freshness_tolerance_seconds')!=0 or request.get('clone_chain_depth_limit')!=0
            or request.get('ignore_external_modifications') or normalize(request['sql'])!='select * from '+source
            or normalize(request['target_table'])!=target):raise Refused('outside bounded zero-lag whole-command microbatch scope')
    evidence=request['tables'];wanted={source,target} if target_exists else {source}
    if len(evidence)!=len(wanted) or {normalize(t['name']) for t in evidence}!=wanted or any(type(t.get('last_modified_epoch')) is not int or t['last_modified_epoch']<=0 for t in evidence):
        raise Refused('microbatch requires exact current positive evidence')
    decisions(calls,expected)
    if any(r['method'] in ('RecordExecutions','RegisterClone') for r in calls):raise Refused('unexpected ordinary Record/copy')
    if expected=='skip_execution' and any(r['method']=='ConfirmExecution' for r in calls):raise Refused('SKIP Confirm')
    return submits

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','probe','work-dir','live-clearance','operation-ledger','run-id'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--query-identity');parser.add_argument('--rust-url')
    return Microbatch(parser.parse_args()).run()
if __name__=='__main__':raise SystemExit(main())
