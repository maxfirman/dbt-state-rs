#!/usr/bin/env python3
"""Offline-prepared #145 schema-add/failure/recovery runner. No hosted claim.

Only a separate exact root grant permits warehouse use. Unknown client status,
RPC engagement or native SQL stops discovery; never manufacture hosted fixtures.
"""
from collections import Counter
from dataclasses import dataclass
from types import MappingProxyType
import hashlib
import json
from pathlib import Path
import re
from databricks_survey import Refused

C3=(('id','BIGINT','LONG',0),('amount','INT','INT',1),('tag','STRING','STRING',2))
C4=C3+(('note','STRING','STRING',3),)
B0=(('1','10','one'),('2','20','retained'))
B1=(('1','11','one-new','n1'),('3','30','three','n3'))
R1=(('1','11','one-new','n1'),('2','20','retained',None),('3','30','three','n3'))
B2=(('3','31','three-new',None),('4','40','four','n4'))
R2=(('1','11','one-new','n1'),('2','20','retained',None),('3','31','three-new',None),('4','40','four','n4'))

@dataclass(frozen=True)
class Bag:
    schema:tuple
    rows:tuple

@dataclass(frozen=True)
class Cell:
    source:Bag
    R:Bag
    F:Bag
    config:str
    body:int
    outcome:str

CELLS=MappingProxyType({
 'first':Cell(Bag(C3,B0),Bag(C3,B0),Bag(C3,B0),'fail',0,'build'),
 'first_repeat':Cell(Bag(C3,B0),Bag(C3,B0),Bag(C3,B0),'fail',0,'reuse'),
 'schema_fail':Cell(Bag(C4,B1),Bag(C3,B0),Bag(C4,B1),'fail',1,'expected_schema_failure'),
 'schema_fail_retry':Cell(Bag(C4,B1),Bag(C3,B0),Bag(C4,B1),'fail',1,'expected_schema_failure'),
 'recover_append':Cell(Bag(C4,B1),Bag(C4,R1),Bag(C4,B1),'append_new_columns',1,'build'),
 'recover_repeat':Cell(Bag(C4,B1),Bag(C4,R1),Bag(C4,B1),'append_new_columns',1,'reuse'),
 'next_delta':Cell(Bag(C4,B2),Bag(C4,R2),Bag(C4,B2),'append_new_columns',1,'build'),
 'next_repeat':Cell(Bag(C4,B2),Bag(C4,R2),Bag(C4,B2),'append_new_columns',1,'reuse'),
})


def assert_typed(actual,bag):
    schema=tuple(tuple(c.get(k) for k in ('name','type_text','type_name','position')) for c in actual['columns'])
    if schema!=bag.schema or any(type(c.get('position')) is not int for c in actual['columns']):raise Refused('physical:schema')
    for col in actual['columns']:
        # UC metadata may omit nullability; if exposed it must be complete and true.
        for key in ('nullable','is_nullable'):
            if any(key in c for c in actual['columns']) and (key not in col or col[key] is not True):
                raise Refused('physical:nullability')
    if Counter(map(tuple,actual['rows']))!=Counter(bag.rows):raise Refused('physical:rows')


def validate_checkpoint(label,source,result,fresh):
    cell=CELLS[label]
    for actual,bag in ((source,cell.source),(result,cell.R),(fresh,cell.F)):assert_typed(actual,bag)


def clearance_contract():
    schema='cdo_demo_db.state_20261114_append146'
    runs={'hosted':'20261115a145ee','rust':'20261115b145ff'}
    return {'ticket':145,'phase':'schema-recovery','catalog':'cdo_demo_db','schema':schema.split('.')[1],
      'schema_id':'6f14be2c-a920-4309-8240-caf8e8df7888','run_ids':runs,
      'max_final_permanent':4,'max_extra_concurrent_persistent':1,
      'permanent_exact_allowlists':{k:[f'{schema}.me_{v}_{n}' for n in ('inputs','consumer')] for k,v in runs.items()},
      'session_temporary_unqualified_names':{k:f'me_{v}_consumer__dbt_tmp' for k,v in runs.items()},
      'persistent_auxiliary_exact_spellings':{k:[f'{schema}.me_{v}_consumer{s}' for s in ('__dbt_stg','__dbt_backup')] for k,v in runs.items()},
      'limits':{'managed_per_backend':8,'disabled_select_only_references_per_backend':8,'parse_per_backend':1,
                'rpcs_per_backend':80,'ordinary_submits_per_backend':8},
      'cleanup':False,'schema_allocation':False}


def source_operations(source):
    return {'first':[
      f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA',
      f"INSERT INTO {source} VALUES (1,10,'one'),(2,20,'retained')"],
      'schema_fail':[f'ALTER TABLE {source} ADD COLUMNS (note STRING)',
      f"INSERT OVERWRITE TABLE {source} VALUES (1,11,'one-new','n1'),(3,30,'three','n3')"],
      'next_delta':[f"INSERT OVERWRITE TABLE {source} VALUES (3,31,'three-new',CAST(NULL AS STRING)),(4,40,'four','n4')"]}


def ledger():
    def bag(v):return {'schema':v.schema,'rows':v.rows,'all_columns_nullable':True}
    return json.loads(json.dumps({'version':1,'ticket':145,'phase':'schema-recovery',
      'run_ids':clearance_contract()['run_ids'],
      'source_operations':source_operations('S'),
      'basis':'independent literal source/R/F bags; synthetic offline validation is not hosted evidence',
      'cells':{label:{'source':bag(c.source),'R':bag(c.R),'F':bag(c.F),'config':c.config,'body':c.body,
                      'outcome':c.outcome,'hosted_status_decision_SQL':'UNKNOWN'} for label,c in CELLS.items()}}))


def read_ledger(path,grant,phase,run):
    """Mandatory complete external literals, validated BEFORE credentials/proxy/DDL."""
    try:
        raw=Path(path).read_bytes()
        document=json.loads(raw,object_pairs_hook=unique_object)
        if (hashlib.sha256(raw).hexdigest()!=grant.get('operation_ledger_sha256')
                or document!=ledger() or document['run_ids']!=grant['run_ids']
                or document['run_ids'][phase]!=run):raise Refused('complete external source/R/F/schema ledger differs')
        return document['cells']
    except (OSError,ValueError,KeyError,TypeError) as error:
        raise Refused('missing or malformed complete external source/R/F/schema ledger') from error


def literal_reference(bag):
    def cell(v,t):
        if v is None:return 'CAST(NULL AS '+t+')'
        return str(int(v)) if t!='STRING' else "'"+v.replace("'","''")+"'"
    names=','.join(c[0] for c in bag.schema)
    return ('select '+', '.join('cast('+c[0]+' as '+c[1]+') '+c[0] for c in bag.schema)+
      ' from values '+','.join('('+','.join(cell(v,c[1]) for v,c in zip(row,bag.schema))+')' for row in bag.rows)+
      ' as declared('+names+')')


from databricks_readonly_reference import unique_object

def assert_reference(stdout,stderr,direct,expected,sql,query_log,compiled):
    if stderr.strip():raise Refused('reference:stderr')
    try:rows=json.loads(stdout,object_pairs_hook=unique_object)
    except (ValueError,TypeError) as error:raise Refused('reference:json') from error
    if not isinstance(rows,list):raise Refused('reference:array')
    converted=[]
    for row in rows:
        if not isinstance(row,dict) or list(row)!=[c[0] for c in expected.schema]:raise Refused('reference:columns')
        out=[]
        for value,column in zip(row.values(),expected.schema):
            if column[1]=='STRING':
                if value is not None and type(value) is not str:raise Refused('reference:type')
                out.append(value)
            else:
                bound=2**53-1 if column[1]=='BIGINT' else 2**31-1
                if type(value) is not int or abs(value)>bound:raise Refused('reference:type')
                out.append(str(value))
        converted.append(tuple(out))
    if Counter(converted)!=Counter(expected.rows):raise Refused('reference:rows')
    assert_typed(direct,expected)
    normalize=lambda s:re.sub(r'\s+',' ',s).strip()
    query='\n'.join(line for line in query_log.splitlines() if not line.startswith('--'))
    if (not re.match(r'(?i)^select\s',sql) or ';' in sql or normalize(compiled)!=normalize(sql)
            or query.count(';')!=1 or not query.rstrip().endswith(';')
            or normalize(query.rstrip()[:-1])!=normalize(sql)):raise Refused('reference:query-sql')
    return {'complete_show_rows':len(rows),'typed_schema':True,'exact_select_only':True,'multiplicity':True}


from databricks_merge_settings import lifecycle, pinned_json, assert_client_status


def validate_success(calls,outcome):
    return lifecycle(calls,{'build':'ready_to_execute','reuse':'skip_execution'}[outcome])


def validate_failure(error,calls,contract):
    if error is None:raise Refused('expected schema failure unexpectedly succeeded')
    if any(r['method'] in ('ConfirmExecution','RecordExecutions','RegisterClone') for r in calls):
        raise Refused('failed execution attempted publication; retain whole status and stop')
    if not contract:raise Refused('UNKNOWN schema failure exit/diagnostic: discovery STOP')
    code=contract.get('exit_code');diagnostic=contract.get('required_diagnostic')
    if (type(code) is not int or code<=0 or code==124 or not isinstance(diagnostic,str) or not diagnostic
            or not all(word in diagnostic.lower() for word in ('schema','source','target','note'))
            or not any(word in diagnostic.lower() for word in ('sync','mismatch','differ'))
            or not str(error).startswith(f'client invocation exited {code};') or diagnostic not in str(error)):
        raise Refused('non-schema client failure: discovery STOP')


def unchanged_target(before,after):
    same_rows=(before['rows']['columns']==after['rows']['columns']
               and Counter(map(tuple,before['rows']['rows']))==Counter(map(tuple,after['rows']['rows'])))
    if not same_rows or any(before[k]!=after[k] for k in ('properties','history')):
        raise Refused('failed/reused target complete rows/schema/properties/history changed')


def statements(raw):
    text=re.sub(r'/\*.*?\*/','',raw,flags=re.S)
    text='\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))
    return [re.sub(r'\s+',' ',q.replace('`','')).strip().lower() for q in text.split(';') if q.strip()]


def native_writes(raw):
    # Only read statements are excluded. Unknown verbs must fail, not disappear.
    return [q for q in statements(raw) if not re.match(r'^(select|show|describe|explain)\b',q)]


def assert_failure_sql(raw,source,target,temporary):
    allowed='create or replace temporary view '+temporary+' as select id, amount, tag, note from '+source
    writes=native_writes(raw)
    if writes not in ([],[allowed]):raise Refused('failed execution unknown/target native write: STOP')
    return {'writes':writes,'target_writes':0}


from databricks_baseline import LeaseScope
from databricks_merge import Merge
from databricks_readonly_reference import show_reference
from databricks_survey import Scope, Campaign, check_manifest

class SchemaScope(LeaseScope):
    profile_roles=('prod',)
    def __init__(self,run,grant):
        contract=clearance_contract()
        if any(grant.get(k)!=v for k,v in contract.items()):raise Refused('schema-recovery exact footprint differs')
        Scope.__init__(self,run)
        if run not in contract['run_ids'].values():raise Refused('uncleared schema-recovery run')
        phase=next(k for k,v in contract['run_ids'].items() if v==run)
        self.roles={'prod':contract['schema']};self.schemas=set(self.roles.values())
        self.relation_prefix='me_'+run+'_'
        self.permanent=set(contract['permanent_exact_allowlists'][phase])
        self.auxiliary=set(contract['persistent_auxiliary_exact_spellings'][phase])
        self.allowed=self.permanent|self.auxiliary
        self.temporary=contract['session_temporary_unqualified_names'][phase]


def validate_request(calls,source,target,exists,cell):
    submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
    if len(submits)!=1:raise Refused('UNKNOWN failure/client State engagement: expected one ordinary submit; STOP')
    request=submits[0]['request'];extras=request.get('semantic_extras',{})
    if (request.get('dialect')!='databricks' or request.get('execution_type')!=3
            or request.get('compare_unrendered_code') is not False or not request.get('table_namespace')
            or request.get('query_dependencies')!=[] or request.get('clone_chain_depth_limit')!=0
            or request.get('ignore_external_modifications') is not False
            or extras.get('unique_key')!='"id"' or extras.get('file_format')!='"delta"'
            or extras.get('on_schema_change')!=json.dumps(cell.config)
            or request.get('freshness_tolerance_seconds')!=0 or request.get('stale_upstream_policy')!=0
            or any(k in extras for k in ('merge_update_columns','merge_exclude_columns','incremental_predicates'))):
        raise Refused('outside exact schema-recovery rendered MERGE request')
    norm=lambda s:s.replace('`','').lower()
    wanted={source,target} if exists else {source}
    tables=request.get('tables',[])
    if (norm(request.get('target_table',''))!=target or len(tables)!=len(wanted)
            or {norm(t['name']) for t in tables}!=wanted
            or any(type(t.get('last_modified_epoch')) is not int or t['last_modified_epoch']<=0 for t in tables)):
        raise Refused('complete exact positive source/target epoch evidence required')
    return submits,{norm(t['name']):t['last_modified_epoch'] for t in tables}


class SchemaRecovery(Merge):
    entrypoint=Path(__file__)
    def __init__(self,args):
        grant=json.loads(Path(args.live_clearance).read_text())
        self.phase='rust' if args.rust_url else 'hosted'
        # This must precede Campaign.__init__, which loads credentials.
        self.frozen=read_ledger(args.operation_ledger,grant,self.phase,args.run_id)
        if (not grant.get('status','').startswith('LIVE CLEARED') or grant.get('authorized_phases')!=[self.phase]
                or args.run_id!=clearance_contract()['run_ids'][self.phase]):raise Refused('separate backend root grant required')
        scope=SchemaScope(args.run_id,grant)
        if args.rust_url and not re.fullmatch(r'http://127\.0\.0\.1:\d+',args.rust_url):raise Refused('Rust endpoint must be owned loopback')
        pinned_json(grant['allocation_receipt'])
        self.trace_lock={}
        if self.phase=='rust':
            # Future native preparation must bind a certified WHOLE hosted trace,
            # including both failures/statuses/Explain, to its strict replay.
            from capture_integrity import verify_capture
            evidence=grant['hosted_capture'];path=Path(evidence['path'])
            if hashlib.sha256(path.read_bytes()).hexdigest()!=evidence['sha256']:raise Refused('hosted whole capture digest differs')
            calls=verify_capture(path);receipt=pinned_json(grant['hosted_replay_receipt'])
            if (receipt.get('strict_replay_passed') is not True or receipt.get('physical_campaign_passed') is not True
                    or receipt.get('capture_sha256')!=evidence['sha256'] or receipt.get('complete_cells')!=8
                    or receipt.get('operation_ledger_sha256')!=grant['operation_ledger_sha256']):
                raise Refused('whole hosted schema-recovery strict replay required')
            # No transport contract is characterized yet. Refuse native use until
            # actual whole-history derivation is implemented from hosted evidence.
            raise Refused(f'UNKNOWN hosted schema-recovery contract ({len(calls)} complete calls): native STOP')
        self.clearance=grant;self.lease=grant;self.query_identity=args.query_identity or args.run_id
        Scope(self.query_identity)
        Campaign.__init__(self,args,scope)
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            profile['survey']['outputs'][role]={k:v for k,v in output.items() if k in {'type','host','http_path','catalog','schema','token','threads'}}
        self.save('profiles/profiles.yml',profile)
        self.save('live-clearance.json',grant);self.save('complete-independent-ledger.json',Path(args.operation_ledger).read_text())
        self.save('campaign-entrypoint.py',self.entrypoint.read_text());self.save('footprint.json',clearance_contract())
        self.save('binary-provenance.json',{name:{'path':str(Path(getattr(args,name)).resolve()),
          'sha256':hashlib.sha256(Path(getattr(args,name)).read_bytes()).hexdigest()} for name in ('dbt','proxy','probe')})

    def init_schemas(self):
        allocation=self.clearance['allocation']
        schema=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+self.scope.schema('prod'))
        if any(schema.get(k)!=allocation[k] for k in ('schema_id','owner','created_at')):raise Refused('allocation identity differs')
        inventory=self.inventory();actual=inventory[self.scope.schema('prod')]
        if {r['full_name'] for r in actual}!=set(self.clearance['protected_permanent_names']):raise Refused('exact protected prior inventory differs')
        if any(r['full_name'] in self.scope.allowed for r in actual):raise Refused('fresh schema-recovery name already exists')
        if len(actual)+3>100:raise Refused('insufficient final plus contingency capacity')
        self.before_inventory=inventory
        self.save('allocation-identity-before.json',schema);self.save('lease-inventory-before.json',inventory)

    def configure(self,cell):
        self.save('project/models/schema-change.yml',{'version':2,'models':[{'name':'consumer','config':{'on_schema_change':cell.config}}]})
        columns='id, amount, tag'+(', note' if cell.body else '')
        self.write_model('consumer',"materialized='incremental',incremental_strategy='merge',unique_key='id',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}",
                         'select '+columns+" from {{ source('merge_schema','inputs') }}")

    def checkpoint(self,label):
        cell=CELLS[label];source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
        observed={}
        for suffix,relation,name in (('_source',source,'source'),('',target,'R')):
            bag=Bag(tuple(map(tuple,self.frozen[label][name]['schema'])),tuple(map(tuple,self.frozen[label][name]['rows'])))
            actual=self.observe(label+suffix,relation);assert_typed(actual,bag)
            props=json.loads((self.work/'physical'/f'{label}{suffix}_properties.json').read_text())
            properties={r[0]:r[1] for r in props['rows']}
            if properties.get('Type')!='MANAGED' or properties.get('Provider','').lower()!='delta':raise Refused('physical:properties')
            history=self.sql(label+suffix+'_history',f'DESCRIBE HISTORY {relation}')
            if not history['rows']:raise Refused('complete history missing')
            observed[name]={'rows':actual,'properties':props,'history':history}
            self.save('history/'+label+suffix+'.json',history)
        if cell.outcome in ('reuse','expected_schema_failure') and hasattr(self,'target_checkpoint'):
            unchanged_target(self.target_checkpoint,observed['R'])
        self.target_checkpoint=observed['R']
        R=Bag(tuple(map(tuple,self.frozen[label]['R']['schema'])),tuple(map(tuple,self.frozen[label]['R']['rows'])))
        F=Bag(tuple(map(tuple,self.frozen[label]['F']['schema'])),tuple(map(tuple,self.frozen[label]['F']['rows'])))
        sql=literal_reference(R);direct=self.sql(label+'_literal_R',sql);assert_typed(direct,R)
        show_reference(self,label,sql,R,direct,validator=assert_reference)
        fresh=self.sql(label+'_fresh_F','SELECT '+','.join(c[0] for c in F.schema)+' FROM '+source);assert_typed(fresh,F)
        assert_typed(self.sql(label+'_literal_F',literal_reference(F)),F)
        after=self.inventory()
        for schema,previous in self.before_inventory.items():
            if any(r not in after[schema] for r in previous):raise Refused('protected inventory changed')
            if any(r['full_name'] not in self.scope.permanent for r in after[schema] if r not in previous):raise Refused('unplanned final object')
        self.save('inventory/'+label+'.json',after)
        self.save('checks/'+label+'.json',{'complete_external_source_R_F_schema':True,'zero_RPC_reference':True})

    def invocation(self,label):
        cell=CELLS[label];source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
        raw=(self.project/'models/consumer.sql').read_bytes()
        if cell.body==1 and hasattr(self,'body_v1') and raw!=self.body_v1:raise Refused('v1 raw body changed')
        if cell.body==1:self.body_v1=raw
        lo=len(self.wire());error=None
        try:
            self.dbt(label,['run','--select','consumer'])
        except Refused as caught:
            # Only the two explicit failure cells may inspect a nonzero result.
            # Everything else preserves Campaign.dbt's strict success contract.
            if cell.outcome!='expected_schema_failure':raise
            error=caught
        calls=self.wire()[lo:]
        path=self.work/'targets'/f'{self.n:03d}_{label}'
        self.save('discovery/'+label+'.json',{'client_error':str(error) if error else None,'calls':calls,
          'artifacts':{name:(path/name).exists() for name in ('run_results.json','manifest.json')},
          'hosted_policy':'UNKNOWN; no approximate fixture or inferred completion'})
        query=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        self.save('materialization/'+label+'.sql',query)
        # Check the physical failure interval even when engagement/status is new.
        if cell.outcome=='expected_schema_failure':
            self.checkpoint(label)
            assert_failure_sql(query,source,target,self.scope.temporary)
            submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
            if len(submits)==1 and any((submits[0].get('response') or {}).get('response',{}).values()):self.explain(label,submits)
            validate_failure(error,calls,self.clearance.get('schema_failure_contract'))
        else:
            submits,observed=validate_success(calls,cell.outcome)
            assert_client_status(json.loads((path/'run_results.json').read_text()),'model.survey_'+self.a.run_id+'.consumer',target,observed)
        submits,epochs=validate_request(calls,source,target,label!='first',cell)
        request=submits[0]['request']
        if request.get('labels',{}).get('dbt_node_unique_id')!='model.survey_'+self.a.run_id+'.consumer':raise Refused('model UID differs')
        if any(r.get('status',{}).get('code',0)!=0 for r in calls):raise Refused('UNKNOWN RPC error/status: discovery STOP')
        if label!='first' and epochs[target]!=self.confirmed_epoch:raise Refused('failed submit/recovery lost last successful own target anchor')
        if hasattr(self,'previous_request'):
            previous=self.previous_request
            if (any(request.get(k)!=previous.get(k) for k in ('table_namespace','default_catalog','default_schema','dialect'))
                    or request.get('labels',{}).get('dbt_node_unique_id')!=previous.get('labels',{}).get('dbt_node_unique_id')):
                raise Refused('namespace/UID changed')
            if cell.body==1 and label!='schema_fail' and request['sql']!=previous['sql']:raise Refused('compiled v1 SELECT changed')
            if label in ('schema_fail','next_delta'):
                if epochs[source]<=self.previous_epochs[source]:raise Refused('genuine source mutation lacks positive epoch advance')
            elif epochs[source]!=self.previous_epochs[source]:raise Refused('unmutated source epoch changed')
            if label=='recover_append':
                left=dict(previous);right=dict(request)
                # Only schema config changes; labels and generated request IDs
                # may vary. The full actual requests remain retained for replay.
                if {k:v for k,v in left['semantic_extras'].items() if k!='on_schema_change'}!={k:v for k,v in right['semantic_extras'].items() if k!='on_schema_change'}:
                    raise Refused('recovery changed additional semantic config')
        if cell.outcome=='build':
            confirm=next(r['request'] for r in calls if r['method']=='ConfirmExecution')
            self.confirmed_epoch=confirm['last_modified_epoch'];self.last_runtime=confirm['execution_runtime_ms']
            if label=='first':Merge.audit_native(self,label,False)
            else:
                # Actual hosted SQL must be inspected before this path is locked.
                # No permissive guessed ADD/MERGE grammar is accepted offline.
                self.save('discovery/'+label+'-native-writes.json',native_writes(query))
                self.explain(label,submits);self.checkpoint(label)
                raise Refused('UNKNOWN recovery native SQL: retain diagnostic and STOP before another invocation')
        elif cell.outcome=='reuse':
            response=submits[0]['response']['response']['skip_execution']
            if response.get('is_stale') is not False or response.get('execution_runtime_ms')!=self.last_runtime:
                raise Refused('reuse lacks exact cached runtime/freshness')
            if native_writes(query):raise Refused('reuse emitted native writes')
        self.previous_request=request;self.previous_epochs=epochs
        if cell.outcome!='expected_schema_failure':
            self.explain(label,submits);self.checkpoint(label)
        self.check_budget()

    def check_budget(self):
        calls=self.wire()
        if len(calls)>80 or sum(r['method']=='SubmitEnrichedSQL' for r in calls)>8:
            raise Refused('schema-recovery whole traffic ceiling exceeded; STOP')

    def baseline(self):
        self.setup_project();source=self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'merge_schema','database':'cdo_demo_db',
          'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.configure(CELLS['first'])
        if self.dbt('inventory',['parse'],managed=False):raise Refused('disabled parse emitted State RPCs')
        manifest=json.loads(next((self.work/'targets').glob('*_inventory/manifest.json')).read_text());check_manifest(self.scope,manifest)
        nodes=[n for n in manifest['nodes'].values() if n.get('resource_type')=='model']
        if (len(nodes)!=1 or nodes[0]['name']!='consumer' or nodes[0].get('language')!='sql'
                or nodes[0]['config'].get('incremental_strategy')!='merge' or nodes[0]['config'].get('file_format')!='delta'
                or nodes[0]['config'].get('on_schema_change')!='fail'
                or nodes[0]['config'].get('pre-hook') or nodes[0]['config'].get('post-hook')):
            raise Refused('manifest outside native schema-recovery scope')
        self.save('actual-default-configuration.json',nodes[0])
        operations=source_operations(source)
        for label,cell in CELLS.items():
            self.check_budget();self.configure(cell)
            if label in ('schema_fail','next_delta'):
                target=self.scope.relation('prod','consumer')
                actual=self.observe('pre_'+label,target)
                properties=json.loads((self.work/'physical'/f'pre_{label}_properties.json').read_text())
                history=self.sql('pre_'+label+'_target_history',f'DESCRIBE HISTORY {target}')
                baseline={'rows':actual,'properties':properties,'history':history}
                unchanged_target(self.target_checkpoint,baseline)
                self.save('pre-mutation/'+label+'.json',baseline)
                source_rows=self.observe('pre_'+label+'_source',source)
                assert_typed(source_rows,CELLS['first'].source if label=='schema_fail' else CELLS['recover_repeat'].source)
                before=self.sql('pre_'+label+'_source_history',f'DESCRIBE HISTORY {source}')
                self.save('pre-mutation/'+label+'-source-history.json',before)
            for index,sql in enumerate(operations.get(label,[])):self.sql(f'source_{label}_{index}',sql)
            self.invocation(label)
        return {'managed_runs':8,'whole_ticket_complete':False,'hosted_policy':'UNKNOWN until whole certified replay'}


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','probe','work-dir','live-clearance','operation-ledger','run-id'):parser.add_argument('--'+name)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--query-identity');parser.add_argument('--rust-url');parser.add_argument('--proposal',action='store_true')
    args=parser.parse_args()
    if args.proposal:
        print(json.dumps({'contract':clearance_contract(),'ledger':ledger(),'live_authority':False},indent=2));return 0
    if any(getattr(args,k.replace('-','_')) is None for k in ('dbt','proxy','probe','work-dir','live-clearance','operation-ledger','run-id')):
        parser.error('every pinned argument and exact backend root grant is required')
    return SchemaRecovery(args).run()


if __name__=='__main__':raise SystemExit(main())
