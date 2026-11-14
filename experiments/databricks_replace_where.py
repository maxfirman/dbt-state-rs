#!/usr/bin/env python3
"""Bounded #148 predicate replacement: independent R/F ledger and opt-in driver.

The literal ledger precedes execution. Fresh SELECT F is not cumulative R.
No live action is authorized by importing or running the offline oracle.
"""
from collections import Counter
from dataclasses import dataclass
from types import MappingProxyType
from databricks_survey import Refused

SCHEMA=(('id','BIGINT','LONG',0),('v','STRING','STRING',1),
        ('p','DATE','DATE',2),('ts','TIMESTAMP','TIMESTAMP',3))
P1="p = DATE '2026-11-01' OR p IS NULL"
P2="p IN (DATE '2026-11-01', DATE '2026-11-02') OR p IS NULL"

@dataclass(frozen=True)
class Snapshot:
    source:tuple
    R:tuple
    F:tuple
    predicate:str=P1

# Worked literal snapshots, not output of replacement(), warehouse or State.
_INITIAL=(('1','a','2026-11-01','2026-11-01T12:00:00.000Z'),
          ('1','a','2026-11-01','2026-11-01T12:00:00.000Z'),
          ('2','b-old','2026-11-02','2026-11-02T12:00:00.000Z'),
          ('3',None,None,None),('3',None,None,None))
_SELECTED=(('1','a','2026-11-01','2026-11-01T12:00:00.000Z'),
           ('1','a','2026-11-01','2026-11-01T12:00:00.000Z'),
           ('3',None,None,None),('3',None,None,None))
_INSIDE=(('1','changed','2026-11-01','2026-11-01T12:00:00.000Z'),
         ('1','changed','2026-11-01','2026-11-01T12:00:00.000Z'),
         ('2','b-old','2026-11-02','2026-11-02T12:00:00.000Z'),
         ('3',None,None,None),('3',None,None,None),('3',None,None,None))
_INSIDE_F=(('1','changed','2026-11-01','2026-11-01T12:00:00.000Z'),
           ('1','changed','2026-11-01','2026-11-01T12:00:00.000Z'),
           ('3',None,None,None),('3',None,None,None),('3',None,None,None))
_REFRESH=(('4','fresh','2026-11-01','2026-11-03T01:02:03.000Z'),
          ('4','fresh','2026-11-01','2026-11-03T01:02:03.000Z'),
          ('5','b-new','2026-11-02','2026-11-04T01:02:03.000Z'),('6','null-new',None,None))
_NEXT=(('4','fresh','2026-11-01','2026-11-03T01:02:03.000Z'),
       ('4','fresh','2026-11-01','2026-11-03T01:02:03.000Z'),('6','null-new',None,None))
HISTORY=MappingProxyType({
 'first':Snapshot(_INITIAL,_INITIAL,_INITIAL),
 'steady':Snapshot(_INITIAL,_INITIAL,_SELECTED),
 'inside':Snapshot(_INSIDE,_INSIDE,_INSIDE_F),
 'outside':Snapshot(_INSIDE_F,_INSIDE,_INSIDE_F),
 'empty':Snapshot((),(('2','b-old','2026-11-02','2026-11-02T12:00:00.000Z'),),()),
 'predicate':Snapshot((),(),(),P2),
 'refresh':Snapshot(_REFRESH,_REFRESH,_REFRESH,P2),
 'next_ordinary':Snapshot(_REFRESH,_NEXT,_NEXT,P2),
})
CELLS=(('first','first','ready_to_execute'),('steady','steady','ready_to_execute'),
 ('steady_repeat','steady','skip_execution'),('inside','inside','ready_to_execute'),
 ('inside_repeat','inside','skip_execution'),('outside','outside','ready_to_execute'),
 ('outside_repeat','outside','skip_execution'),('empty','empty','ready_to_execute'),
 ('predicate','predicate','ready_to_execute'),('predicate_repeat','predicate','skip_execution'),
 ('full_refresh','refresh','record'),('next_ordinary','next_ordinary','ready_to_execute'),
 ('next_repeat','next_ordinary','skip_execution'))


def replacement(previous, incoming, predicate):
    """Independent multiset operation; SQL UNKNOWN belongs to retained rows."""
    return tuple(row for row in previous if predicate(row) is not True)+tuple(incoming)


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
    actual=[];whole_second_rows=0
    for row in rows:
        if not isinstance(row,dict) or list(row)!=['id','v','p','ts']:raise Refused('reference:columns')
        a,v,p,ts=row.values()
        # Retained pinned147 show/API proof: exact UTC whole seconds denote
        # exactly zero milliseconds. No offsets, space forms or other precision.
        if type(ts) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',ts):
            ts=ts[:-1]+'.000Z';whole_second_rows+=1
        if (type(a) is not int or abs(a)>2**53-1
                or (v is not None and type(v) is not str)
                or (p is not None and (type(p) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',p)))
                or (ts is not None and (type(ts) is not str or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z',ts)))):
            raise Refused('reference:uncharacterized-typed-codec')
        actual.append((str(a),v,p,ts))
    if Counter(actual)!=Counter(map(tuple,expected)):raise Refused('reference:rows')
    assert_typed(direct,expected)
    query='\n'.join(line for line in query_log.splitlines() if not line.startswith('--'))
    normalize=lambda s:re.sub(r'\s+',' ',s).strip()
    if (not re.match(r'(?i)^select\s',sql) or ';' in sql
            or normalize(compiled)!=normalize(sql) or query.count(';')!=1
            or not query.rstrip().endswith(';') or normalize(query.rstrip()[:-1])!=normalize(sql)):
        raise Refused('reference:query-sql')
    return {'complete_show_rows':len(rows),'typed_schema':True,'exact_select_only':True,
            'duplicate_null_multiset':True,'exact_temporal_codec':True,'observed_show_whole_second_rows':whole_second_rows}


from databricks_baseline import LeaseScope
from databricks_freshness import decisions
from databricks_survey import Scope


def clearance_contract():
    """Exact offline proposal. Only a separate root clearance grants execution."""
    schema='cdo_demo_db.state_20261114_append146'
    runs={'hosted':'20261114a148aa','rust':'20261114b148bb'}
    return {'ticket':148,'catalog':'cdo_demo_db','schema':schema.split('.')[1],
      'schema_id':'6f14be2c-a920-4309-8240-caf8e8df7888','run_ids':runs,
      'max_final_permanent':4,'max_extra_concurrent_persistent':1,
      'container_counts':{'hosted_before':8,'rust_before':10,'final_permanent':12,'persistent_peak':13},
      'permanent_exact_allowlists':{k:[f'{schema}.rw_{v}_{n}' for n in ('inputs','consumer')] for k,v in runs.items()},
      'persistent_auxiliary_exact_spellings':{k:[f'{schema}.rw_{v}_consumer{s}' for s in ('__dbt_stg','__dbt_backup')] for k,v in runs.items()},
      'session_temporary_unqualified_names':{k:f'rw_{v}_consumer__dbt_tmp' for k,v in runs.items()},
      'limits':{'managed_per_backend':13,'disabled_select_only_references_per_backend':13,'parse_per_backend':1},
      'protected_permanent_names':[f'{schema}.a_20261114{r}_{n}' for r in ('a146aa','b146bb') for n in ('inputs','consumer')]+[f'{schema}.io_20261114{r}_{n}' for r in ('a147aa','b147bb') for n in ('inputs','consumer')]}


class ReplaceScope(LeaseScope):
    profile_roles=('prod',)
    def __init__(self,run,clearance):
        proposal=clearance_contract()
        if any(clearance.get(k)!=v for k,v in proposal.items()):raise Refused('replace_where exact footprint differs')
        if run not in proposal['run_ids'].values():raise Refused('uncleared backend run')
        Scope.__init__(self,run)
        phase=next(k for k,v in proposal['run_ids'].items() if v==run)
        self.roles={'prod':proposal['schema']};self.schemas=set(self.roles.values())
        self.relation_prefix='rw_'+run+'_'
        self.permanent=set(proposal['permanent_exact_allowlists'][phase])
        self.auxiliary=set(proposal['persistent_auxiliary_exact_spellings'][phase])
        self.allowed=self.permanent|self.auxiliary
        self.temporary=proposal['session_temporary_unqualified_names'][phase]


def replace_submit(calls,source,target,target_exists,predicate,expected):
    submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
    if len(submits)!=1:raise Refused('expected one actual replace_where submit')
    request=submits[0]['request'];extras=request['semantic_extras']
    if (request['execution_type']!=11 or request['compare_unrendered_code']
            or not request.get('table_namespace') or request['query_dependencies']
            or extras.get('unique_key')!='"id"' or extras.get('on_schema_change')!='"ignore"'
            or extras.get('file_format')!='"delta"'
            or extras.get('incremental_predicates')!=json.dumps([predicate])
            or request.get('clone_chain_depth_limit')!=0 or request.get('ignore_external_modifications')):
        raise Refused('outside bounded rendered replace_where scope')
    normalize=lambda name:name.replace('`','').lower()
    evidence=request['tables'];wanted={source,target} if target_exists else {source}
    if (len(evidence)!=len(wanted) or {normalize(t['name']) for t in evidence}!=wanted
            or any(type(t.get('last_modified_epoch')) is not int or t['last_modified_epoch']<=0 for t in evidence)
            or normalize(request['target_table'])!=target):raise Refused('replace_where needs exact known positive epochs')
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


def ledger():
    return {'version':2,'ticket':148,'cells':CELLS,
            'history':{k:{'source':v.source,'R':v.R,'F':v.F,'predicate':v.predicate} for k,v in HISTORY.items()},
            'schema':SCHEMA,'basis':'independent immutable literal multiset operation history'}


def assert_replace_sql(raw,target,temporary,predicate):
    text=re.sub(r'/\*.*?\*/','',raw,flags=re.S)
    text='\n'.join(l for l in text.splitlines() if not l.lstrip().startswith('--'))
    normalize=lambda s:re.sub(r'\s+',' ',s.replace('`','')).strip().lower()
    text=normalize(text)
    expected='insert into '+target+' by name replace where '+predicate+' table '+temporary
    if normalize(expected) not in text:raise Refused('native SQL lacks exact BY NAME REPLACE WHERE predicate/temp path')
    if not re.search(r'create or replace temporary view '+re.escape(temporary)+r'\s+as\b',text):
        raise Refused('native SQL lacks exact session-local temporary view')
    writes=[s.strip() for s in text.split(';') if re.match(r'^(create|drop|alter|merge|insert|update|delete)\b',s.strip())]
    temp_prefix='create or replace temporary view '+temporary+' as '
    if (sum(s.startswith(temp_prefix) for s in writes)!=1
            or sum(s==normalize(expected) for s in writes)!=1
            or any(not (s.startswith(temp_prefix) or s==normalize(expected)
                        or s=='alter table '+target+' cluster by none') for s in writes)):
        raise Refused('native SQL contains unreviewed additional writes')


class ReplaceWhere(Merge):
    entrypoint=Path(__file__)
    def __init__(self,args):
        clearance=json.loads(Path(args.live_clearance).read_text());proposal=clearance_contract()
        self.phase='rust' if args.rust_url else 'hosted'
        if (not clearance.get('status','').startswith('LIVE CLEARED')
                or self.phase not in clearance.get('authorized_phases',[])
                or args.run_id!=proposal['run_ids'][self.phase]):raise Refused('backend lacks separate exact root clearance')
        scope=ReplaceScope(args.run_id,clearance)
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
            if expected!='skip_execution' and version<=self.last_version:raise Refused('successful replacement did not advance Delta version')
        self.last_version=version
        self.save('history/'+label+'.json',history)
        sql=literal_reference(case.R);direct=self.sql(label+'_cumulative_R_literal',sql);assert_typed(direct,case.R)
        show_reference(self,label,sql,case.R,direct,validator=assert_reference)
        selected=self.sql(label+'_selected_F_literal',literal_reference(case.F));assert_typed(selected,case.F)
        filter_sql='' if stage in ('first','refresh') else ' WHERE '+P1
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
        self.save('expected/'+label+'.json',{'stage':stage,'source':case.source,'R':case.R,'F':case.F,'predicate':case.predicate})
        calls=self.dbt(label,['run','--select','consumer']+(['--full-refresh'] if expected=='record' else []))
        self.audit_native(label,label!='first')
        target=self.scope.relation('prod','consumer');source=self.scope.relation('prod','inputs')
        query=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        if expected=='record':
            record=full_refresh_record(calls,target);self.last_runtime=record['outcome']['execution_runtime_ms']
            self.confirmed_epoch=record['outcome']['last_modified_epoch']
            self.save('full-refresh-wire.json',calls)
        else:
            submits=replace_submit(calls,source,target,label!='first',case.predicate,expected)
            request=submits[0]['request'];evidence={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in request['tables']}
            if label in ('inside','outside','empty') and evidence[source]<=self.previous_epochs[source]:
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
                if label!='first':assert_replace_sql(query,target,self.scope.temporary,case.predicate)
                if label=='predicate' and request['sql']!=self.previous_sql:raise Refused('predicate-only isolation changed rendered SQL')
            self.previous_epochs=evidence;self.previous_sql=request['sql'];self.previous_extras=request['semantic_extras']
            self.explain(label,submits);self.save('epochs/'+label+'.json',{'tables':request['tables'],'expected':expected})
        self.checked_results(label,stage)

    def model(self,predicate=P1):
        config="materialized='incremental',incremental_strategy='replace_where',unique_key='id',file_format='delta',on_schema_change='ignore',state={'lag_tolerance':'0s','pre_clone':'never'},incremental_predicates="+repr([predicate])
        self.write_model('consumer',config,"select id,v,p,ts from {{ source('replace_history','inputs') }} {% if is_incremental() %} where "+P1+" {% endif %}")

    def baseline(self):
        self.setup_project();source=self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'replace_history','database':'cdo_demo_db',
                  'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.model()
        if self.dbt('inventory',['parse'],managed=False):raise Refused('State-disabled parse emitted unexpected RPCs')
        manifest=json.loads(next((self.work/'targets').glob('*_inventory/manifest.json')).read_text());check_manifest(self.scope,manifest)
        node=next(v for v in manifest['nodes'].values() if v['name']=='consumer')
        if (node.get('language')!='sql' or node['config']['incremental_strategy']!='replace_where'
                or node['config'].get('file_format')!='delta' or node['config'].get('pre-hook') or node['config'].get('post-hook')):
            raise Refused('manifest outside pinned native SQL managed Delta scope')
        for n in [*manifest['nodes'].values(),*manifest['sources'].values()]:
            self.scope.check('.'.join([n['database'],n['schema'],n.get('alias') or n['identifier']]))
        self.save('actual-default-configuration.json',{'node':node,'metadata':manifest['metadata'],'feature_overrides':False,'macro_overrides':False})
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
        self.sql('initial_source',f'INSERT INTO {source} '+literal_reference(HISTORY['first'].source))
        for label,stage,expected in CELLS:
            if label=='inside':
                self.sql('inside_update',f"UPDATE {source} SET v='changed' WHERE p=DATE '2026-11-01'")
                self.sql('inside_duplicate',f'INSERT INTO {source} VALUES (3,NULL,NULL,NULL)')
            elif label=='outside':
                self.sql('outside_update',f"UPDATE {source} SET v='b-changed' WHERE p=DATE '2026-11-02'")
                self.sql('outside_delete',f"DELETE FROM {source} WHERE p=DATE '2026-11-02'")
            elif label=='empty':self.sql('empty_selected',f'DELETE FROM {source} WHERE '+P1)
            elif label=='predicate':self.model(P2)
            elif label=='full_refresh':self.sql('refill_source',f'INSERT INTO {source} '+literal_reference(HISTORY['refresh'].source))
            self.invocation(label,stage,expected)
        return {'managed_runs':13,'reference_objects':0,'whole_ticket_complete':False,'dependencies_138_140_complete':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','probe','work-dir','live-clearance','operation-ledger','run-id'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--query-identity');parser.add_argument('--rust-url')
    return ReplaceWhere(parser.parse_args()).run()


if __name__=='__main__':raise SystemExit(main())
