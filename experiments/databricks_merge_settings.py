"""Bounded #145 MERGE settings characterization; no live run without root clearance.

Config-only policy is discovered from hosted whole histories. No production
changes or schema-evolution assumptions are made by this preparation.
"""
from collections import Counter
from dataclasses import dataclass
from types import MappingProxyType
from databricks_survey import Refused

SCHEMA=(('id','BIGINT','LONG',0),('amount','INT','INT',1),('tag','STRING','STRING',2))
PREDICATE='(DBT_INTERNAL_DEST.id >= 2 OR DBT_INTERNAL_DEST.id IS NULL)'

@dataclass(frozen=True)
class Snapshot:
    source:tuple
    R:tuple
    F:tuple

# These complete bags are independently worked literals, never observed output.
HISTORY=MappingProxyType({
 'first':Snapshot((('1','10','same'),('2','20','old'),('3','10','same'),(None,None,None)),
                  (('1','10','same'),('2','20','old'),('3','10','same'),(None,None,None)),
                  (('1','10','same'),('2','20','old'),('3','10','same'),(None,None,None))),
 'null_update':Snapshot((('1','11','changed'),('2','20','old'),('3','10','same'),(None,'30','null-v'),('4','40','new')),
                       (('1','11','changed'),('2','20','old'),('3','10','same'),(None,'30','null-v'),('4','40','new')),
                       (('1','11','changed'),('2','20','old'),('3','10','same'),(None,'30','null-v'),('4','40','new'))),
 'include':Snapshot((('1','12','newer'),('3','10','same'),(None,'31','null-new'),('4','40','new'),('5','50','insert-five')),
                    (('1','12','changed'),('2','20','old'),('3','10','same'),(None,'31','null-v'),('4','40','new'),('5','50','insert-five')),
                    (('1','12','newer'),('3','10','same'),(None,'31','null-new'),('4','40','new'),('5','50','insert-five'))),
 'exclude':Snapshot((('1','13','newest'),('3','10','same'),(None,'32','null-newest'),('4','40','new'),('5','50','insert-five'),('6','60','insert-six')),
                    (('1','13','changed'),('2','20','old'),('3','10','same'),(None,'32','null-v'),('4','40','new'),('5','50','insert-five'),('6','60','insert-six')),
                    (('1','13','newest'),('3','10','same'),(None,'32','null-newest'),('4','40','new'),('5','50','insert-five'),('6','60','insert-six'))),
 'predicate':Snapshot((('1','13','changed'),('3','10','same'),(None,'32','null-newest'),('4','40','new'),('5','50','insert-five'),('6','60','insert-six'),('7','70','insert-seven')),
                    (('1','13','changed'),('2','20','old'),('3','10','same'),(None,'32','null-v'),('4','40','new'),('5','50','insert-five'),('6','60','insert-six'),('1','13','changed'),('7','70','insert-seven')),
                    (('1','13','changed'),('3','10','same'),(None,'32','null-newest'),('4','40','new'),('5','50','insert-five'),('6','60','insert-six'),('7','70','insert-seven'))),
})


def merge_model(previous,incoming,update_columns=('id','amount','tag'),exclude_first=False):
    """Independent bag operation model for unique source keys and known ON subset.

    Predicate-excluded target key 1 cannot match. All remaining nullable keys
    match with <=>. This does not characterize duplicated-target update rules.
    """
    if len({r[0] for r in incoming})!=len(incoming):raise Refused('uncharacterized duplicate source keys')
    if len({r[0] for r in previous})!=len(previous):raise Refused('uncharacterized duplicate target update')
    rows=list(previous)
    for row in incoming:
        index=next((i for i,old in enumerate(previous)
                    if old[0]==row[0] and not (exclude_first and old[0]=='1')),None)
        if index is None:rows.append(row)
        else:rows[index]=tuple(row[i] if name in update_columns else previous[index][i]
                               for i,name in enumerate(('id','amount','tag')))
    return tuple(rows)


def assert_typed(actual,expected):
    schema=tuple(tuple(c.get(k) for k in ('name','type_text','type_name','position')) for c in actual['columns'])
    if schema!=SCHEMA or any(type(c.get('position')) is not int for c in actual['columns']):raise Refused('physical:schema')
    if Counter(map(tuple,actual['rows']))!=Counter(map(tuple,expected)):raise Refused('physical:rows')

# Config is in schema YAML, so model raw body and rendered SELECT stay fixed.
CONFIGS=MappingProxyType({
 'default':{}, 'include':{'merge_update_columns':['amount']},
 'exclude':{'merge_exclude_columns':['tag']},
 'predicate':{'merge_exclude_columns':['tag'],'incremental_predicates':[PREDICATE]},
})
CELLS=(('first','first','default','ready_to_execute'),
 ('first_repeat','first','default','skip_execution'),
 ('null_update','null_update','default','ready_to_execute'),
 ('null_repeat','null_update','default','skip_execution'),
 ('config_include','null_update','include','DISCOVERY'),
 ('include_delta','include','include','ready_to_execute'),
 ('include_repeat','include','include','skip_execution'),
 ('config_exclude','include','exclude','DISCOVERY'),
 ('exclude_delta','exclude','exclude','ready_to_execute'),
 ('exclude_repeat','exclude','exclude','skip_execution'),
 ('predicate_delta','predicate','predicate','ready_to_execute'),
 ('predicate_repeat','predicate','predicate','skip_execution'))


def ledger():
    return {'version':1,'ticket':145,'scope':'NULL keys / update columns / destination ON predicate',
      'schema':SCHEMA,'configs':dict(CONFIGS),'cells':CELLS,
      'history':{k:{'source':v.source,'R':v.R,'F':v.F} for k,v in HISTORY.items()},
      'basis':'independent literal complete typed bags, no State or warehouse result input',
      'config_only_policy':'DISCOVERY hosted; Rust locked from whole certified and strictly replayed hosted trace',
      'duplicate_target_updates':'uncharacterized; predicate exclusion is last',
      'deferred':['schema add/fail recovery','drop/type capability','configured refresh','ambiguous source keys',
                  'restart/upgrade','generated lifecycle','native dependencies #138/#140']}


import hashlib
import json
from pathlib import Path
import re


def clearance_contract():
    schema='cdo_demo_db.state_20261114_append146'
    runs={'hosted':'20261114a145cc','rust':'20261114b145dd'}
    return {'ticket':145,'phase':'settings','catalog':'cdo_demo_db','schema':schema.split('.')[1],
      'schema_id':'6f14be2c-a920-4309-8240-caf8e8df7888','run_ids':runs,
      'max_final_permanent':4,'max_extra_concurrent_persistent':1,
      'permanent_exact_allowlists':{k:[f'{schema}.ms_{v}_{n}' for n in ('inputs','consumer')] for k,v in runs.items()},
      'session_temporary_unqualified_names':{k:f'ms_{v}_consumer__dbt_tmp' for k,v in runs.items()},
      'persistent_auxiliary_exact_spellings':{k:[f'{schema}.ms_{v}_consumer{s}' for s in ('__dbt_stg','__dbt_backup')] for k,v in runs.items()},
      'limits':{'managed_per_backend':12,'disabled_select_only_references_per_backend':12,'parse_per_backend':1},
      'cleanup':False,'schema_allocation':False}


def literal_reference(rows):
    def cell(v,t):
        if v is None:return 'cast(null as '+t+')'
        return str(int(v)) if t!='string' else "'"+v.replace("'","''")+"'"
    return ('select cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag from values '+
            ','.join('('+','.join(cell(v,t) for v,t in zip(r,('bigint','int','string')))+')' for r in rows)+
            ' as declared(id,amount,tag)')


def assert_result(actual,properties,expected):
    assert_typed(actual,expected)
    props={r[0]:r[1] for r in properties['rows']}
    if props.get('Type')!='MANAGED' or props.get('Provider','').lower()!='delta':raise Refused('physical:properties')


from databricks_readonly_reference import unique_object

def assert_reference(stdout,stderr,direct,expected,sql,query_log,compiled):
    if stderr.strip():raise Refused('reference:stderr')
    try:rows=json.loads(stdout,object_pairs_hook=unique_object)
    except (TypeError,ValueError) as error:raise Refused('reference:json') from error
    if not isinstance(rows,list):raise Refused('reference:array')
    converted=[]
    for row in rows:
        if not isinstance(row,dict) or list(row)!=['id','amount','tag']:raise Refused('reference:columns')
        key,amount,tag=row.values()
        if ((key is not None and (type(key) is not int or abs(key)>2**53-1))
                or (amount is not None and (type(amount) is not int or not -2**31<=amount<2**31))
                or (tag is not None and type(tag) is not str)):raise Refused('reference:types')
        converted.append((None if key is None else str(key),None if amount is None else str(amount),tag))
    if Counter(converted)!=Counter(map(tuple,expected)):raise Refused('reference:rows')
    assert_typed(direct,expected)
    normalize=lambda s:re.sub(r'\s+',' ',s).strip()
    query='\n'.join(line for line in query_log.splitlines() if not line.startswith('--'))
    if (not re.match(r'(?i)^select\s',sql) or ';' in sql or normalize(compiled)!=normalize(sql)
            or query.count(';')!=1 or not query.rstrip().endswith(';')
            or normalize(query.rstrip()[:-1])!=normalize(sql)):raise Refused('reference:query-sql')
    return {'complete_show_rows':len(rows),'typed_schema':True,'exact_select_only':True,'duplicate_null_multiset':True}


from databricks_freshness import decisions

def lifecycle(calls,expected=None):
    submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
    if len(submits)!=1 or any(r.get('status',{}).get('code',0)!=0 for r in calls):raise Refused('one successful ordinary submit required')
    response=submits[0].get('response',{}).get('response',{})
    if len(response)!=1 or next(iter(response)) not in ('ready_to_execute','skip_execution'):raise Refused('unexpected ordinary response')
    observed=next(iter(response))
    if expected is not None and observed!=expected:raise Refused('decision differs from locked expectation')
    decisions(calls,observed)
    if any(r['method'] in ('RecordExecutions','RegisterClone') for r in calls):raise Refused('ordinary settings copied/recorded')
    confirms=[r for r in calls if r['method']=='ConfirmExecution']
    if observed=='skip_execution' and confirms:raise Refused('reuse unexpectedly confirmed')
    if observed=='ready_to_execute':
        if (len(confirms)!=1 or confirms[0].get('response')!={'success':True}
                or confirms[0]['request'].get('request_id')!=response[observed]['request_id']
                or any(type(confirms[0]['request'].get(k)) is not int or confirms[0]['request'][k]<=0
                       for k in ('last_modified_epoch','execution_runtime_ms'))):raise Refused('missing exact accepted positive Confirm')
    return submits,observed


def settings_submit(calls,source,target,exists,config,expected=None):
    submits,observed=lifecycle(calls,expected);r=submits[0]['request'];extras=r['semantic_extras']
    if (r.get('dialect')!='databricks' or r['execution_type']!=3 or r['compare_unrendered_code']
            or not r.get('table_namespace') or r['query_dependencies']
            or extras.get('unique_key')!='"id"' or extras.get('on_schema_change')!='"ignore"'
            or extras.get('file_format')!='"delta"' or r.get('clone_chain_depth_limit')!=0
            or r.get('ignore_external_modifications')):raise Refused('outside native rendered MERGE scope')
    for k in ('merge_update_columns','merge_exclude_columns','incremental_predicates'):
        value=extras.get(k)
        if k in CONFIGS[config]:
            try:decoded=json.loads(value)
            except (TypeError,ValueError) as error:raise Refused('settings semantic_extras missing') from error
            if decoded!=CONFIGS[config][k]:raise Refused('settings semantic_extras differ')
        elif value is not None:raise Refused('unexpected settings semantic_extras')
    norm=lambda s:s.replace('`','').lower()
    wanted={source,target} if exists else {source}
    if (norm(r['target_table'])!=target or len(r['tables'])!=len(wanted)
            or {norm(t['name']) for t in r['tables']}!=wanted
            or any(type(t.get('last_modified_epoch')) is not int or t['last_modified_epoch']<=0 for t in r['tables'])):
        raise Refused('settings require complete known positive exact source/target epochs')
    return submits,observed


def native_writes(raw):
    text=re.sub(r'/\*.*?\*/','',raw,flags=re.S)
    text='\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))
    return [re.sub(r'\s+',' ',q.replace('`','')).strip().lower() for q in text.split(';')
            if re.match(r'(?i)^\s*(create|drop|alter|merge|insert|update|delete|set)\b',q)]


def assert_merge_sql(raw,target,source,temporary,config):
    writes=native_writes(raw)
    temp='create or replace temporary view '+temporary+' as select id, amount, tag from '+source
    on='dbt_internal_source.id <=> dbt_internal_dest.id'
    if config=='predicate':on=PREDICATE.lower()+' and '+on
    if config=='default':update='*'
    elif config=='include':update='amount = dbt_internal_source.amount'
    else:update='id = dbt_internal_source.id, amount = dbt_internal_source.amount'
    merge=('merge into '+target+' as dbt_internal_dest using '+temporary+' as dbt_internal_source on '+on+
           ' when matched then update set '+update+' when not matched then insert *')
    allowed=[temp,merge,'alter table '+target+' cluster by none']
    if writes.count(temp)!=1 or writes.count(merge)!=1 or any(w not in allowed for w in writes) or writes.count(allowed[2])>1:
        raise Refused('native MERGE settings/ON/insert footprint differs from pinned macro')
    return {'writes':writes,'peak_extra_persistent_beyond_target':0,'session_temporary':temporary}


def hosted_contract(calls,source,target):
    """Derive a native decision lock solely from a complete prior hosted history."""
    starts=[i for i,r in enumerate(calls) if r['method']=='SubmitEnrichedSQL']
    if len(starts)!=len(CELLS):raise Refused('whole hosted settings history required')
    if any(r.get('status',{}).get('code',0)!=0 for r in calls):raise Refused('hosted history contains failed RPC')
    result={};previous=None
    for cell,lo,hi in zip(CELLS,starts,starts[1:]+[len(calls)]):
        label,_,config,expected=cell
        submits,observed=settings_submit(calls[lo:hi],source,target,label!='first',config,
                                         None if expected=='DISCOVERY' else expected)
        r=submits[0]['request']
        if previous:
            if r['sql']!=previous['sql']:raise Refused('hosted raw compiled SELECT changed')
            if label.startswith('config_') and r['tables']!=previous['tables']:
                # Input target epoch may differ from the previous pre-build submit.
                prev_source=next(t for t in previous['tables'] if t['name'].replace('`','').lower()==source)
                cur_source=next(t for t in r['tables'] if t['name'].replace('`','').lower()==source)
                if cur_source!=prev_source:raise Refused('hosted config-only source evidence changed')
        result[label]=observed;previous=r
    return {'decisions':result,'derivation':'complete prior hosted trace only','cells':len(CELLS)}


from databricks_baseline import LeaseScope
from databricks_merge import Merge
from databricks_readonly_reference import show_reference
from databricks_survey import Scope, Campaign, check_manifest

class SettingsScope(LeaseScope):
    profile_roles=('prod',)
    def __init__(self,run,clearance):
        proposal=clearance_contract()
        if any(clearance.get(k)!=v for k,v in proposal.items()):raise Refused('settings exact footprint differs')
        if run not in proposal['run_ids'].values():raise Refused('uncleared settings run')
        Scope.__init__(self,run)
        phase=next(k for k,v in proposal['run_ids'].items() if v==run)
        self.roles={'prod':proposal['schema']};self.schemas=set(self.roles.values())
        self.relation_prefix='ms_'+run+'_'
        self.permanent=set(proposal['permanent_exact_allowlists'][phase])
        self.auxiliary=set(proposal['persistent_auxiliary_exact_spellings'][phase])
        self.allowed=self.permanent|self.auxiliary
        self.temporary=proposal['session_temporary_unqualified_names'][phase]


def pinned_json(record):
    path=Path(record['path']);raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=record['sha256']:raise Refused('frozen evidence hash differs')
    return json.loads(raw)


def assert_client_status(results,uid,target,expected):
    rows=results.get('results',[])
    if len(rows)!=1:raise Refused('client omitted or added model')
    row=rows[0];status='reused' if expected=='skip_execution' else 'success'
    if (row.get('unique_id')!=uid or row.get('relation_name','').replace('`','').lower()!=target
            or row.get('status')!=status or row.get('failures') is not None):raise Refused('client status/UID/target mismatch')
    adapter=row.get('adapter_response')
    if status=='reused':
        if adapter!={}:raise Refused('reused client reports native execution')
    elif not isinstance(adapter,dict) or adapter.get('code')!='SUCCESS' or not adapter.get('query_id'):
        raise Refused('client lacks successful native query receipt')
    return {'status':status,'uid':uid,'adapter_response':adapter}


class Settings(Merge):
    entrypoint=Path(__file__)
    def __init__(self,args):
        clearance=json.loads(Path(args.live_clearance).read_text());self.phase='rust' if args.rust_url else 'hosted'
        if (not clearance.get('status','').startswith('LIVE CLEARED')
                or self.phase not in clearance.get('authorized_phases',[])
                or args.run_id!=clearance_contract()['run_ids'][self.phase]):raise Refused('backend lacks exact root clearance')
        scope=SettingsScope(args.run_id,clearance)
        if args.rust_url and not re.fullmatch(r'http://127\.0\.0\.1:\d+',args.rust_url):raise Refused('Rust endpoint must be owned loopback')
        raw=Path(args.operation_ledger).read_bytes()
        if (hashlib.sha256(raw).hexdigest()!=clearance['operation_ledger_sha256']
                or json.loads(raw)!=json.loads(json.dumps(ledger()))):raise Refused('frozen independent ledger differs')
        self.decision_lock={}
        if self.phase=='rust':
            from capture_integrity import verify_capture
            evidence=clearance['hosted_capture'];path=Path(evidence['path'])
            if hashlib.sha256(path.read_bytes()).hexdigest()!=evidence['sha256']:raise Refused('whole hosted capture hash differs')
            replay=pinned_json(clearance['hosted_replay_receipt'])
            if (replay.get('strict_replay_passed') is not True or replay.get('capture_sha256')!=evidence['sha256']
                    or replay.get('physical_campaign_passed') is not True or replay.get('complete_cells')!=len(CELLS)
                    or replay.get('operation_ledger_sha256')!=clearance['operation_ledger_sha256']):
                raise Refused('whole hosted strict replay receipt required before native run')
            names=clearance_contract()['permanent_exact_allowlists']['hosted']
            self.decision_lock=hosted_contract(verify_capture(path),names[0],names[1])['decisions']
        pinned_json(clearance['allocation_receipt'])
        self.clearance=clearance;self.lease=clearance;self.query_identity=args.query_identity or args.run_id
        Scope(self.query_identity)
        Campaign.__init__(self,args,scope)
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            profile['survey']['outputs'][role]={k:v for k,v in output.items() if k in {'type','host','http_path','catalog','schema','token','threads'}}
        self.save('profiles/profiles.yml',profile)
        self.save('live-clearance.json',clearance);self.save('operation-manifest.json',raw.decode())
        self.save('locked-prior-hosted-decisions.json',self.decision_lock)
        self.save('source-provenance.json',{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('*.py')})
        self.save('campaign-entrypoint.py',self.entrypoint.read_text())
        self.save('binary-provenance.json',{name:{'path':str(Path(getattr(args,name)).resolve()),
          'sha256':hashlib.sha256(Path(getattr(args,name)).read_bytes()).hexdigest()} for name in ('dbt','proxy','probe')})
        self.save('footprint.json',{'permanent':sorted(scope.permanent),'auxiliary':sorted(scope.auxiliary),
          'session_temporary':scope.temporary,'max_extra_persistent':1,'reference_objects':0,'cleanup':False})

    def init_schemas(self):
        allocation=self.clearance['allocation']
        schema=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+self.scope.schema('prod'))
        if any(schema.get(k)!=allocation[k] for k in ('schema_id','owner','created_at')):raise Refused('allocation identity differs')
        inventory=self.inventory();actual=inventory[self.scope.schema('prod')]
        prior=set(self.clearance['protected_permanent_names'])
        if self.phase=='rust':prior.update(self.clearance['permanent_exact_allowlists']['hosted'])
        if {r['full_name'] for r in actual}!=prior:raise Refused('exact protected prior inventory differs')
        if any(r['full_name'] in self.scope.allowed for r in actual):raise Refused('fresh settings name already exists')
        if len(actual)+3>100:raise Refused('insufficient permanent plus one transient capacity')
        self.before_inventory=inventory
        self.save('allocation-identity-before.json',schema);self.save('lease-inventory-before.json',inventory)

    def configure(self,config):
        model={'name':'consumer','config':dict(CONFIGS[config])}
        document={'version':2,'models':[model]}
        self.save('project/models/settings.yml',document)
        self.save('config-variants/'+config+'.json',document)

    def checked_results(self,label,stage,observed):
        case=HISTORY[stage];source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
        for suffix,relation,expected in (('_source',source,case.source),('',target,case.R)):
            actual=self.observe(label+suffix,relation)
            properties=json.loads((self.work/'physical'/f'{label}{suffix}_properties.json').read_text())
            assert_result(actual,properties,expected)
            history=self.sql(label+suffix+'_history',f'DESCRIBE HISTORY {relation}')
            self.save('history/'+label+suffix+'.json',history)
            if not suffix:
                if not history['rows']:raise Refused('missing full target Delta history')
                if hasattr(self,'previous_history') and observed=='skip_execution' and history!=self.previous_history:
                    raise Refused('SKIP changed complete physical Delta history')
                self.previous_history=history
        sql=literal_reference(case.R);direct=self.sql(label+'_literal_R',sql);assert_typed(direct,case.R)
        show_reference(self,label,sql,case.R,direct,validator=assert_reference)
        fresh=self.sql(label+'_fresh_F',f'SELECT id,amount,tag FROM {source}');assert_typed(fresh,case.F)
        assert_typed(self.sql(label+'_literal_F',literal_reference(case.F)),case.F)
        after=self.inventory()
        for schema,previous in self.before_inventory.items():
            if any(r not in after[schema] for r in previous):raise Refused('protected inventory changed')
            if any(r['full_name'] not in self.scope.permanent for r in after[schema] if r not in previous):raise Refused('unplanned final object')
        self.save('inventory/'+label+'.json',after)
        self.save('checks/'+label+'.json',{'complete_typed_source_R_F':True,'managed_delta_full_history':True,
          'zero_RPC_disabled_SELECT_reference':True,'R_count':len(case.R),'F_count':len(case.F),'observed_decision':observed})

    def invocation(self,label,stage,config,expected):
        case=HISTORY[stage]
        self.save('expected/'+label+'.json',{'source':case.source,'R':case.R,'F':case.F,'config':dict(CONFIGS[config]),'decision':expected})
        raw=(self.project/'models/consumer.sql').read_bytes()
        if hasattr(self,'raw_body') and self.raw_body!=raw:raise Refused('config-only model raw body changed')
        self.raw_body=raw
        calls=self.dbt(label,['run','--select','consumer'])
        source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
        lock=self.decision_lock[label] if self.phase=='rust' else (None if expected=='DISCOVERY' else expected)
        submits,observed=settings_submit(calls,source,target,label!='first',config,lock)
        request=submits[0]['request'];evidence={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in request['tables']}
        results=json.loads((self.work/'targets'/f'{self.n:03d}_{label}'/'run_results.json').read_text())
        status=assert_client_status(results,'model.survey_'+self.a.run_id+'.consumer',target,observed)
        self.save('client-status/'+label+'.json',status)
        if label!='first' and evidence[target]!=self.confirmed_epoch:raise Refused('target differs from own latest confirmed epoch')
        if hasattr(self,'previous_sql') and request['sql']!=self.previous_sql:raise Refused('config-only compiled SELECT changed')
        if hasattr(self,'namespace') and request['table_namespace']!=self.namespace:raise Refused('connection namespace changed')
        self.namespace=request['table_namespace']
        if label in ('null_update','include_delta','exclude_delta','predicate_delta') and evidence[source]<=self.previous_epochs[source]:
            raise Refused('source mutation did not advance actual epoch; #140 remains incomplete')
        if label.startswith('config_') and evidence[source]!=self.previous_epochs[source]:raise Refused('config-only source epoch changed')
        if observed=='skip_execution':
            response=submits[0]['response']['response'][observed]
            if (evidence[source]!=self.previous_epochs[source] or evidence[target]!=self.confirmed_epoch
                    or response.get('is_stale') or response['execution_runtime_ms']!=self.last_runtime):raise Refused('reuse not anchored to own latest confirmed build')
        else:
            confirm=next(r['request'] for r in calls if r['method']=='ConfirmExecution')
            self.confirmed_epoch=confirm['last_modified_epoch'];self.last_runtime=confirm['execution_runtime_ms']
        query=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        if observed=='skip_execution':
            if native_writes(query):raise Refused('SKIP emitted native writes')
            footprint={'writes':[],'peak_extra_persistent_beyond_target':0}
        elif label=='first':
            # Canonical lifetime audit covers only exact own target/staging/backup.
            Merge.audit_native(self,label,False)
            footprint=json.loads((self.work/'materialization'/f'{label}.json').read_text())
        else:footprint=assert_merge_sql(query,target,source,self.scope.temporary,config)
        self.save('materialization/'+label+'.sql',query);self.save('materialization/'+label+'.json',footprint)
        self.previous_epochs=evidence;self.previous_sql=request['sql']
        self.explain(label,submits)
        self.save('epochs/'+label+'.json',{'tables':request['tables'],'semantic_extras':request['semantic_extras'],
          'observed':observed,'prior_hosted_lock':lock,'discovery':expected=='DISCOVERY'})
        self.checked_results(label,stage,observed)

    def baseline(self):
        self.setup_project();source=self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'merge_settings','database':'cdo_demo_db',
          'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.write_model('consumer',"materialized='incremental',incremental_strategy='merge',unique_key='id',file_format='delta',on_schema_change='ignore',state={'lag_tolerance':'0s','pre_clone':'never'}",
          "select id, amount, tag from {{ source('merge_settings','inputs') }}")
        self.configure('default')
        if self.dbt('inventory',['parse'],managed=False):raise Refused('disabled parse emitted State RPCs')
        manifest=json.loads(next((self.work/'targets').glob('*_inventory/manifest.json')).read_text());check_manifest(self.scope,manifest)
        node=next(v for v in manifest['nodes'].values() if v['name']=='consumer')
        if (node.get('language')!='sql' or node['config'].get('incremental_strategy')!='merge'
                or node['config'].get('file_format')!='delta' or node['config'].get('pre-hook') or node['config'].get('post-hook')):
            raise Refused('manifest outside exact native SQL Delta MERGE')
        for n in [*manifest['nodes'].values(),*manifest['sources'].values()]:
            self.scope.check('.'.join([n['database'],n['schema'],n.get('alias') or n['identifier']]))
        self.save('actual-default-configuration.json',{'node':node,'metadata':manifest['metadata'],'macro_overrides':False})
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT,amount INT,tag STRING) USING DELTA')
        self.sql('initial_source',f'INSERT INTO {source} '+literal_reference(HISTORY['first'].source))
        previous='default'
        for label,stage,config,expected in CELLS:
            if config!=previous:self.configure(config);previous=config
            if label in ('null_update','include_delta','exclude_delta','predicate_delta'):
                self.sql('source_'+label,f'INSERT OVERWRITE TABLE {source} '+literal_reference(HISTORY[stage].source))
            self.invocation(label,stage,config,expected)
        return {'managed_runs':12,'reference_objects':0,'whole_ticket_complete':False,'schema_evolution_complete':False,'dependencies_138_140_complete':False}


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','probe','work-dir','live-clearance','operation-ledger','run-id'):parser.add_argument('--'+name,required=True)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--query-identity');parser.add_argument('--rust-url')
    return Settings(parser.parse_args()).run()


if __name__=='__main__':raise SystemExit(main())
