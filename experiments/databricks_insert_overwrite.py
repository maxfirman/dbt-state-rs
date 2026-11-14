#!/usr/bin/env python3
"""Bounded #147 native REPLACE ON campaign; explicit live clearance required.

Reference targets are derived from immutable operations, never actual results.
No live action is authorized merely by invoking this module's offline helpers.
"""


def replace_partitions(previous, selected, partition_columns):
    """Independent multiset reference: replace every represented partition.

Python tuple equality provides NULL-safe equality here (None equals None).
The row sequence deliberately retains duplicate rows and exact scalar types.
"""
    keys={tuple(row[i] for i in partition_columns) for row in selected}
    return tuple(row for row in previous if tuple(row[i] for i in partition_columns) not in keys)+tuple(selected)

from collections import Counter
from databricks_survey import Refused

SCHEMA=(('id','BIGINT','LONG',0),('v','STRING','STRING',1),
        ('p','DATE','DATE',2),('ts','TIMESTAMP','TIMESTAMP',3))


def assert_typed(actual, expected):
    schema=tuple(tuple(c.get(k) for k in ('name','type_text','type_name','position')) for c in actual['columns'])
    if schema!=SCHEMA or any(type(c.get('position')) is not int for c in actual['columns']):
        raise Refused('physical:schema')
    if Counter(map(tuple,actual['rows']))!=Counter(map(tuple,expected)):
        raise Refused('physical:rows')


def assert_partition_result(actual, properties, expected):
    assert_typed(actual,expected)
    props={r[0]:r[1] for r in properties['rows']}
    if props.get('Type')!='MANAGED' or props.get('Provider','').lower()!='delta':
        raise Refused('physical:properties')
    rows=properties['rows']
    try:start=next(i for i,r in enumerate(rows) if r[0]=='# Partition Information')+2
    except StopIteration:raise Refused('physical:partition-metadata')
    partition=[]
    for row in rows[start:]:
        if row[0]=='':break
        partition.append(row[:2])
    if partition!=[['p','date']]:raise Refused('physical:partition-metadata')


def literal_reference(rows):
    def cell(value,kind):
        if value is None:return 'cast(null as '+kind+')'
        if kind=='bigint':return str(int(value))
        return "cast('"+value.replace("'","''")+"' as "+kind+')'
    columns=('bigint','string','date','timestamp')
    projection='select cast(id as bigint) id, cast(v as string) v, cast(p as date) p, cast(ts as timestamp) ts '
    if not rows:
        return projection+'from (select cast(null as bigint) id, cast(null as string) v, cast(null as date) p, cast(null as timestamp) ts) declared where false'
    values=','.join('('+','.join(cell(v,t) for v,t in zip(row,columns))+')' for row in rows)
    return projection+'from values '+values+' as declared(id,v,p,ts)'


def assert_reference(stdout, stderr, direct, expected, sql, query_log, compiled):
    """Bounded show codec: exact date and evidenced UTC second/millisecond strings.

    Unknown client encodings stop discovery instead of coercing away precision.
    Typed SQL results remain independently checked, including empty results.
    """
    import json
    import re
    from databricks_readonly_reference import unique_object
    if stderr.strip():raise Refused('reference:stderr')
    try:objects=json.loads(stdout,object_pairs_hook=unique_object)
    except (ValueError,TypeError) as error:raise Refused('reference:ambiguous-json') from error
    if not isinstance(objects,list):raise Refused('reference:json-array')
    rows=[];whole_second_rows=0
    for obj in objects:
        if not isinstance(obj,dict) or list(obj)!=['id','v','p','ts']:
            raise Refused('reference:json-columns')
        row=list(obj.values())
        # Pinned2.0.11 preflight proves show omits the zero fractional part.
        # Admit only that exact UTC spelling, preserving every nonzero fraction.
        if type(row[3]) is str and re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',row[3]):
            row[3]=row[3][:-1]+'.000Z';whole_second_rows+=1
        if (type(row[0]) is not int or abs(row[0])>2**53-1
                or any(v is not None and type(v) is not str for v in row[1:])
                or (row[2] is not None and not re.fullmatch(r'\d{4}-\d{2}-\d{2}',row[2]))
                or (row[3] is not None and not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z',row[3]))):
            raise Refused('reference:json-types-or-uncharacterized-temporal-codec')
        rows.append([str(row[0]),*row[1:]])
    if Counter(map(tuple,rows))!=Counter(map(tuple,expected)):raise Refused('reference:json-rows')
    assert_typed(direct,expected)
    query='\n'.join(line for line in query_log.splitlines() if not line.startswith('--'))
    normalize=lambda text:re.sub(r'\s+',' ',text).strip()
    if (not re.match(r'(?i)^select\s',sql) or ';' in sql
            or normalize(compiled)!=normalize(sql)
            or not query.rstrip().endswith(';') or query.count(';')!=1
            or normalize(query.rstrip()[:-1])!=normalize(sql)):
        raise Refused('reference:query-sql')
    return {'complete_show_rows':len(rows),'typed_schema':True,'exact_select_only':True,
            'duplicate_null_multiset':True,'exact_date_utc_millisecond_timestamp':True,'observed_show_whole_second_rows':whole_second_rows}


import argparse
import hashlib
import json
from pathlib import Path
import re

from databricks_baseline import LeaseScope
from databricks_freshness import decisions
from databricks_merge import Merge
from databricks_readonly_reference import show_reference
from databricks_survey import Campaign, Scope, check_manifest


def clearance_contract():
    """Offline exact footprint proposal; this object grants no live authority."""
    runs={'hosted':'20261114a147aa','rust':'20261114b147bb'}
    return {'ticket':147,'catalog':'cdo_demo_db','schema':'state_20261114_append146',
      'schema_id':'6f14be2c-a920-4309-8240-caf8e8df7888','run_ids':runs,
      'max_final_permanent':4,'max_extra_concurrent_persistent':1,
      'permanent_exact_allowlists':{phase:[f'cdo_demo_db.state_20261114_append146.io_{run}_{n}' for n in ('inputs','consumer')] for phase,run in runs.items()},
      'persistent_auxiliary_exact_spellings':{phase:[f'cdo_demo_db.state_20261114_append146.io_{run}_consumer{s}' for s in ('__dbt_stg','__dbt_backup')] for phase,run in runs.items()},
      'session_temporary_unqualified_names':{phase:f'io_{run}_consumer__dbt_tmp' for phase,run in runs.items()},
      'limits':{'managed_per_backend':13,'disabled_select_only_references_per_backend':13,'parse_per_backend':1},
      'protected_permanent_names':[f'cdo_demo_db.state_20261114_append146.a_20261114{run}_{n}' for run in ('a146aa','b146bb') for n in ('inputs','consumer')]}


class OverwriteScope(LeaseScope):
    profile_roles=('prod',)

    def __init__(self,run,clearance):
        proposal=clearance_contract()
        if any(clearance.get(k)!=v for k,v in proposal.items()):
            raise Refused('insert-overwrite clearance differs from exact reviewed footprint')
        if run not in proposal['run_ids'].values():raise Refused('uncleared backend run')
        Scope.__init__(self,run)
        phase=next(k for k,v in proposal['run_ids'].items() if v==run)
        self.roles={'prod':proposal['schema']};self.schemas=set(self.roles.values())
        self.relation_prefix='io_'+run+'_'
        self.permanent=set(proposal['permanent_exact_allowlists'][phase])
        self.auxiliary=set(proposal['persistent_auxiliary_exact_spellings'][phase])
        self.allowed=self.permanent|self.auxiliary
        self.temporary=proposal['session_temporary_unqualified_names'][phase]


def assert_replace_on_sql(raw,target,temporary):
    text=re.sub(r'/\*.*?\*/','',raw,flags=re.S)
    text='\n'.join(l for l in text.splitlines() if not l.lstrip().startswith('--'))
    text=text.replace('`','').lower()
    if re.search(r'\b(?:merge|overwrite|set)\b',text):raise Refused('unexpected legacy/alternate native strategy')
    inserts=[q.strip() for q in text.split(';') if re.match(r'(?i)^\s*insert\b',q)]
    pattern=(r'insert\s+into\s+table\s+'+re.escape(target)+r'\s+as\s+t\s+replace\s+on\s*'
      r'\(\s*t\.p\s*<=>\s*s\.p\s*\)\s*\(\s*select\s+id\s*,\s*v\s*,\s*p\s*,\s*ts\s+from\s+'
      +re.escape(temporary)+r'\s*\)\s+as\s+s')
    if len(inserts)!=1 or not re.fullmatch(pattern,inserts[0],flags=re.S):
        raise Refused('actual native SQL lacks exact null-safe partition REPLACE ON')


# Worked before execution; these are full multisets, including exact duplicates.
# All ordinary and refresh queries use the SAME fixed A-or-NULL predicate.
T0='2026-11-01T12:00:00.000Z'
T1='2026-11-01T13:00:00.000Z'
T2='2026-11-01T14:00:00.000Z'
A='2026-11-01'
B='2026-11-02'
HISTORY={
 'first':{'F':(('1','old',A,T0),('1','old',A,T0),('2','delete',A,T1),('9',None,None,T2)),
          'R':(('1','old',A,T0),('1','old',A,T0),('2','delete',A,T1),('9',None,None,T2))},
 'upsert':{'F':(('1','changed',A,T0),('1','changed',A,T0),('3',None,A,T1),('3',None,A,T1),('9','null-new',None,T2)),
           'R':(('1','changed',A,T0),('1','changed',A,T0),('3',None,A,T1),('3',None,A,T1),('9','null-new',None,T2))},
 'remove_null':{'F':(('1','changed',A,T0),('1','changed',A,T0),('3',None,A,T1),('3',None,A,T1)),
                'R':(('1','changed',A,T0),('1','changed',A,T0),('3',None,A,T1),('3',None,A,T1),('9','null-new',None,T2))},
 'empty':{'F':(),
          'R':(('1','changed',A,T0),('1','changed',A,T0),('3',None,A,T1),('3',None,A,T1),('9','null-new',None,T2))},
 'restore':{'F':(('4','restored',A,T0),('8',None,None,T2)),
            'R':(('4','restored',A,T0),('8',None,None,T2))},
 'refresh':{'F':(('4','restored',A,T0),('8',None,None,T2)),
            'R':(('4','restored',A,T0),('8',None,None,T2))},
}
CELLS=(('first','first','ready_to_execute'),('unchanged','first','skip_execution'),
       ('upsert','upsert','ready_to_execute'),('upsert_repeat','upsert','skip_execution'),
       ('remove_null','remove_null','ready_to_execute'),('remove_null_repeat','remove_null','skip_execution'),
       ('empty','empty','ready_to_execute'),('empty_repeat','empty','skip_execution'),
       ('restore','restore','ready_to_execute'),('restore_repeat','restore','skip_execution'),
       ('full_refresh','refresh',None),('next_ordinary','refresh','ready_to_execute'),
       ('next_repeat','refresh','skip_execution'))


def overwrite_submit(calls,source,target,target_exists,expected):
    submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
    if len(submits)!=1:raise Refused('expected one ordinary insert-overwrite submit')
    request=submits[0]['request']
    extras=request['semantic_extras']
    if (request.get('dialect')!='databricks' or request['execution_type']!=4 or request['compare_unrendered_code']
            or not request.get('table_namespace') or request['query_dependencies']
            or extras.get('unique_key')!='"id"' or extras.get('partition_by')!='["p"]'
            or extras.get('file_format')!='"delta"' or extras.get('on_schema_change')!='"ignore"'
            or request.get('clone_chain_depth_limit')!=0 or request.get('ignore_external_modifications') is not False):
        raise Refused('outside bounded rendered partition-replacement scope')
    physical=lambda value:value.replace('`','').lower()
    wanted={source,target} if target_exists else {source}
    evidence=request['tables']
    if (len(evidence)!=len(wanted) or {physical(t['name']) for t in evidence}!=wanted
            or any(type(t.get('last_modified_epoch')) is not int or t['last_modified_epoch']<=0 for t in evidence)
            or physical(request['target_table'])!=target):
        raise Refused('insert-overwrite needs exact known source/target epochs')
    response=submits[0]['response']['response']
    if set(response)!={expected}:raise Refused('unexpected bounded insert-overwrite decision')
    decisions(calls,expected)
    if any(r['method'] in ('RecordExecutions','RegisterClone') for r in calls):
        raise Refused('ordinary replacement unexpectedly recorded/copied')
    if expected=='skip_execution' and any(r['method']=='ConfirmExecution' for r in calls):
        raise Refused('reuse unexpectedly confirmed')
    return submits


def full_refresh_record(calls,target):
    if any(r['method'] in ('SubmitEnrichedSQL','ConfirmExecution','RegisterClone') for r in calls):
        raise Refused('refresh route differs from reviewed FULL Record path')
    records=[r for r in calls if r['method']=='RecordExecutions']
    if (len(records)!=1 or len(records[0]['request']['records'])!=1
            or records[0]['response']!={'records_stored':1}
            or type(records[0]['response']['records_stored']) is not int):
        raise Refused('refresh lacks one acknowledged record')
    record=records[0]['request']['records'][0]
    request=record['input']['enriched_sql'];outcome=record['outcome']
    if (request['execution_type']!=1 or request['target_table'].replace('`','').lower()!=target
            or type(outcome.get('last_modified_epoch')) is not int or outcome['last_modified_epoch']<=0
            or type(outcome.get('execution_runtime_ms')) is not int or outcome['execution_runtime_ms']<=0):
        raise Refused('refresh record lacks FULL type/known positive epoch/runtime')
    return record


def assert_client_status(results,uid,target,decision,refresh):
    rows=results.get('results',[])
    expected='success' if refresh else ('reused' if decision=='skip_execution' else 'warn')
    if len(rows)!=1:raise Refused('client result omitted or added model')
    row=rows[0]
    if (row.get('unique_id')!=uid or row.get('relation_name','').replace('`','').lower()!=target
            or row.get('status')!=expected or row.get('failures') is not None):
        raise Refused('client status/UID/target differs from pinned hosted observation')
    adapter=row.get('adapter_response')
    if expected=='reused':
        if adapter!={}:raise Refused('reused client unexpectedly reports native adapter execution')
    elif not isinstance(adapter,dict) or adapter.get('code')!='SUCCESS' or not adapter.get('query_id'):
        raise Refused('executed client lacks successful native query response')
    return {'status':expected,'unique_id':uid,'adapter_response':adapter,
            'ordinary_warning':'pinned adapter SQL warehouse DBR17.1+ warning; actual native REPLACE ON verified' if expected=='warn' else None}


class Overwrite(Merge):
    entrypoint=Path(__file__)

    def __init__(self,args):
        grant=json.loads(Path(args.live_clearance).read_text())
        scope=OverwriteScope(args.run_id,grant)
        phase='rust' if args.rust_url else 'hosted'
        if (grant.get('status')!='LIVE CLEARED #147 bounded partition replacement'
                or phase not in grant.get('authorized_phases',[])
                or grant['run_ids'][phase]!=args.run_id):
            raise Refused('no exact insert-overwrite live authorization for backend')
        if args.rust_url and not re.fullmatch(r'http://127\.0\.0\.1:\d+',args.rust_url):
            raise Refused('Rust endpoint must be owned literal loopback')
        receipt=Path(grant['allocation']['receipt'])
        if hashlib.sha256(receipt.read_bytes()).hexdigest()!=grant['allocation']['receipt_sha256']:
            raise Refused('allocation receipt digest changed')
        if grant['allocation']['schema_id']!=grant['schema_id']:raise Refused('allocation UUID mismatch')
        self.clearance=grant;self.phase=phase;self.lease=grant
        self.query_identity=args.query_identity or args.run_id;Scope(self.query_identity)
        Campaign.__init__(self,args,scope)
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            profile['survey']['outputs'][role]={k:v for k,v in output.items() if k in {'type','host','http_path','catalog','schema','token','threads'}}
        self.save('profiles/profiles.yml',profile)
        self.save('live-clearance.json',grant)
        self.save('source-provenance.json',{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob('databricks_*.py')})
        self.save('campaign-entrypoint.py',self.entrypoint.read_text())
        self.save('operation-manifest.json',{'ticket':147,'whole_ticket_complete':False,'expected':HISTORY,'cells':CELLS,
          'native_path':'observe pinned serverless REPLACE ON, no legacy overwrite override',
          'reference_objects':0,'native_dependencies_blocked':[138,140],
          'not_covered':['schema evolution','multiple partition keys','unpartitioned/cluster settings','strategy transitions','generated/restart lifecycle']})
        self.save('footprint.json',clearance_contract())

    def init_schemas(self):
        schema=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+self.scope.schema('prod'))
        allocation=self.clearance['allocation']
        if (schema.get('schema_id')!=self.clearance['schema_id'] or schema.get('owner')!=allocation['owner']
                or schema.get('created_at')!=allocation['created_at']):raise Refused('container identity changed')
        inventory=self.inventory();objects=inventory[self.scope.schema('prod')]
        expected=self.clearance['preexisting_objects'][self.phase]
        if sorted(objects,key=lambda r:r['full_name'])!=sorted(expected,key=lambda r:r['full_name']):
            raise Refused('container inventory changed or new names occupied')
        prior=set(self.clearance['protected_permanent_names'])
        if self.phase=='rust':prior.update(self.clearance['permanent_exact_allowlists']['hosted'])
        if {r['full_name'] for r in objects}!=prior:raise Refused('unexpected preexisting names')
        if any(n in prior for n in self.scope.allowed):raise Refused('phase names already exist')
        self.before_inventory=inventory
        self.save('allocation-identity-before.json',schema);self.save('lease-inventory-before.json',inventory)

    def selected_sql(self):
        return "SELECT id,v,p,ts FROM "+self.scope.relation('prod','inputs')+" WHERE p=DATE '2026-11-01' OR p IS NULL"

    def checked_results(self,label,stage):
        case=HISTORY[stage]
        actual=self.observe(label,self.scope.relation('prod','consumer'))
        props=json.loads((self.work/'physical'/f'{label}_properties.json').read_text())
        assert_partition_result(actual,props,case['R'])
        reference=literal_reference(case['R']);direct=self.sql(label+'_cumulative_R_literal',reference)
        assert_typed(direct,case['R'])
        show_reference(self,label,reference,case['R'],direct,validator=assert_reference)
        selected=self.sql(label+'_selected_F_literal',literal_reference(case['F']))
        assert_typed(selected,case['F'])
        fresh=self.sql(label+'_selected_F_source',self.selected_sql());assert_typed(fresh,case['F'])
        source=self.scope.relation('prod','inputs')
        source_rows=self.sql(label+'_complete_source','SELECT id,v,p,ts FROM '+source)
        assert_typed(source_rows,tuple(case['F'])+(('1','excluded-B',B,T0),))
        source_history=self.sql(label+'_full_source_history','DESCRIBE HISTORY '+source)
        self.save('physical/'+label+'_source_history.json',source_history)
        history=self.sql(label+'_full_delta_history','DESCRIBE HISTORY '+self.scope.relation('prod','consumer'))
        self.save('physical/'+label+'_history.json',history)
        after=self.inventory()
        for schema,previous in self.before_inventory.items():
            if any(row not in after[schema] for row in previous):raise Refused('protected inventory changed')
            if any(row['full_name'] not in self.scope.permanent for row in after[schema] if row not in previous):
                raise Refused('unplanned final object')
        self.save('inventory/'+label+'.json',after)
        self.save('checks/'+label+'.json',{'complete_typed_R_and_F':True,'zero_RPC_disabled_R_reference':True,
          'managed_delta_partition_p':True,'full_delta_history':True,'stage':stage,'R_count':len(case['R']),'F_count':len(case['F'])})

    def invocation(self,label,stage,expected=None):
        refresh=label=='full_refresh'
        self.save('expected/'+label+'.json',{'stage':stage,**HISTORY[stage],'basis':'immutable worked operation multiset'})
        calls=self.dbt(label,['run','--select','consumer']+(['--full-refresh'] if refresh else []))
        results=json.loads((self.work/'targets'/f'{self.n:03d}_{label}'/'run_results.json').read_text())
        status=assert_client_status(results,'model.survey_'+self.a.run_id+'.consumer',self.scope.relation('prod','consumer'),expected,refresh)
        self.save('client-status/'+label+'.json',status)
        self.audit_native(label,label!='first')
        if refresh:
            self.save('full-refresh-wire.json',calls)
            record=full_refresh_record(calls,self.scope.relation('prod','consumer'))
            request=record['input']['enriched_sql']
            if request['sql']!=self.previous_sql or request['table_namespace']!=self.namespace:
                raise Refused('refresh unexpectedly changed fixed SQL or namespace')
            self.save('refresh-record.json',record)
        else:
            source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
            submits=overwrite_submit(calls,source,target,label!='first',expected)
            request=submits[0]['request'];evidence={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in request['tables']}
            if label in ('upsert','remove_null','empty','restore') and evidence[source]<=self.previous_epochs[source]:
                raise Refused('changed source epoch failed to advance; #140 remains blocked')
            if hasattr(self,'namespace') and request['table_namespace']!=self.namespace:
                raise Refused('connection namespace changed within bounded history')
            self.namespace=request['table_namespace']
            if hasattr(self,'previous_sql') and request['sql']!=self.previous_sql:
                raise Refused('fixed first/steady SQL unexpectedly changed')
            if expected=='skip_execution':
                runtime=submits[0]['response']['response'][expected]['execution_runtime_ms']
                if runtime!=self.last_runtime:raise Refused('SKIP runtime differs from own confirmed replacement')
            else:
                confirms=[r for r in calls if r['method']=='ConfirmExecution' and r.get('response',{}).get('success')]
                if len(confirms)!=1:raise Refused('build lacks one successful Confirm')
                self.last_runtime=confirms[0]['request']['execution_runtime_ms']
                if label!='first':
                    raw=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
                    assert_replace_on_sql(raw,target,self.scope.temporary)
            self.previous_epochs=evidence;self.previous_sql=request['sql']
            self.explain(label,submits)
            self.save('epochs/'+label+'.json',{'tables':request['tables'],'observed':expected})
        self.checked_results(label,stage)

    def baseline(self):
        self.setup_project();source=self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'overwrite_history','database':'cdo_demo_db',
          'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        config="materialized='incremental',incremental_strategy='insert_overwrite',partition_by=['p'],unique_key='id',file_format='delta',on_schema_change='ignore',state={'lag_tolerance':'0s','pre_clone':'never'}"
        self.write_model('consumer',config,"select id,v,p,ts from {{ source('overwrite_history','inputs') }} where p=DATE '2026-11-01' OR p IS NULL")
        self.dbt('inventory',['parse'],managed=False)
        manifest=json.loads(next((self.work/'targets').glob('*_inventory/manifest.json')).read_text());check_manifest(self.scope,manifest)
        node=next(v for v in manifest['nodes'].values() if v['name']=='consumer');cfg=node['config']
        if (node.get('language')!='sql' or cfg['materialized']!='incremental' or cfg['incremental_strategy']!='insert_overwrite'
                or cfg.get('partition_by')!=['p'] or cfg.get('unique_key')!='id' or cfg.get('file_format')!='delta'
                or cfg.get('on_schema_change')!='ignore' or cfg.get('full_refresh') is not None
                or cfg.get('pre-hook') or cfg.get('post-hook')):
            raise Refused('manifest outside bounded pinned partition SQL scope')
        for item in [*manifest.get('nodes',{}).values(),*manifest.get('sources',{}).values()]:
            self.scope.check('.'.join([item['database'],item['schema'],item.get('alias') or item['identifier']]))
        self.save('actual-default-configuration.json',{'node':node,'metadata':manifest['metadata'],'feature_overrides':False,'macros_overridden':False})
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
        initial=HISTORY['first']['F']+(('1','excluded-B',B,T0),)
        self.sql('initial_source',f'INSERT INTO {source} '+literal_reference(initial))
        for label,stage,expected in CELLS:
            if label=='upsert':
                self.sql('update_source',f"UPDATE {source} SET v='changed' WHERE id=1 AND p=DATE '2026-11-01'")
                self.sql('delete_represented_row',f'DELETE FROM {source} WHERE id=2')
                self.sql('insert_duplicates',f'INSERT INTO {source} '+literal_reference((('3',None,A,T1),('3',None,A,T1))))
                self.sql('replace_null_partition',f"UPDATE {source} SET v='null-new' WHERE p IS NULL")
            elif label=='remove_null':self.sql('remove_null_partition',f'DELETE FROM {source} WHERE p IS NULL')
            elif label=='empty':self.sql('empty_selected_partition',f"DELETE FROM {source} WHERE p=DATE '2026-11-01'")
            elif label=='restore':self.sql('restore_selected',f'INSERT INTO {source} '+literal_reference(HISTORY['restore']['F']))
            self.invocation(label,stage,expected)
        return {'managed_runs':13,'objects':2,'whole_ticket_complete':False,'native_dependencies_blocked':[138,140]}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','live-clearance'):parser.add_argument('--'+key,required=True)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--query-identity');parser.add_argument('--rust-url');parser.add_argument('--run-id',required=True)
    return Overwrite(parser.parse_args()).run()


if __name__=='__main__':raise SystemExit(main())
