#!/usr/bin/env python3
"""Prepared #151 timestamp-ignore native campaign, held for exact root clearance.

Offline plan/oracles do not read credentials or initialize the campaign. No
new_record, generated warehouse clock, cleanup, schema allocation or transition
is authorized here. Four independent source/target pairs remain after both
backends. Each State-disabled snapshot is checked against its own full ledger.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re

from databricks_baseline import Baseline, LeaseScope
from databricks_freshness import decisions
from databricks_snapshot_oracle import (Ledger, NATIVE_SCHEMA, assert_history,
    check_schema, decode_projection, projection_sql, assert_source, source_projection_sql)
from databricks_survey import Campaign, Scope, Refused, check_manifest

T0='2026-11-01 12:00:00.000000'
T1='2026-11-02 12:00:00.000000'
T2='2026-11-03 12:00:00.000000'
T3='2026-11-03 13:00:00.000000'
T4='2026-11-04 12:00:00.000000'
SOURCE={
 'first':(('1','one','2026-11-01',T0),('2','two','2026-11-02',T1)),
 'upsert':(('1','changed','2026-11-01',T2),('2','two','2026-11-02',T1),('3','three','2026-11-01',T3)),
 'delete':(('1','changed','2026-11-01',T2),('3','three','2026-11-01',T3)),
 'reinsert':(('1','changed','2026-11-01',T2),('2','returned','2026-11-02',T4),('3','three','2026-11-01',T3)),
 'same_timestamp':(('1','equal_time_new_value','2026-11-01',T2),('2','returned','2026-11-02',T4),('3','three','2026-11-01',T3)),
}
CELLS=(('first','first','ready_to_execute'),('unchanged','first','skip_execution'),
       ('upsert','upsert','ready_to_execute'),('upsert_repeat','upsert','skip_execution'),
       ('delete','delete','ready_to_execute'),('delete_repeat','delete','skip_execution'),
       ('reinsert','reinsert','ready_to_execute'),('reinsert_repeat','reinsert','skip_execution'),
       ('same_timestamp','same_timestamp','ready_to_execute'),('same_timestamp_repeat','same_timestamp','skip_execution'),
       ('sql_change','same_timestamp','ready_to_execute'),('sql_change_repeat','same_timestamp','skip_execution'))


def clearance_contract():
    runs={'hosted':'20261114a151aa','rust':'20261114b151bb'}
    schema='state_20261114_snapshot151'
    return {'ticket':151,'catalog':'cdo_demo_db','schema':schema,'run_ids':runs,
      'strategy':'timestamp','hard_deletes':'ignore','max_final_permanent':8,
      'max_phase_permanent':4,'max_extra_concurrent_persistent':0,
      'max_concurrent_session_temporary_views':1,
      'permanent_exact_allowlists':{phase:[f'cdo_demo_db.{schema}.sn_{run}_{name}'
          for name in ('inputs','consumer','ref_inputs','reference')] for phase,run in runs.items()},
      'session_temporary_unqualified_names':{phase:[f'sn_{run}_{name}__dbt_tmp'
          for name in ('consumer','reference')] for phase,run in runs.items()},
      'limits':{'managed_per_backend':12,'disabled_native_snapshots_per_backend':12,'parse_per_backend':1},
      'cleanup':False,'schema_allocation':False,'generated_clock_modes':False}


class SnapshotScope(LeaseScope):
    profile_roles=('prod',)

    def __init__(self,run,grant):
        contract=clearance_contract()
        if any(grant.get(k)!=v for k,v in contract.items()):raise Refused('snapshot footprint differs from frozen proposal')
        if run not in contract['run_ids'].values():raise Refused('uncleared snapshot run')
        Scope.__init__(self,run)
        phase=next(p for p,r in contract['run_ids'].items() if r==run)
        self.roles={'prod':contract['schema']};self.schemas=set(self.roles.values())
        self.relation_prefix='sn_'+run+'_'
        self.permanent=set(contract['permanent_exact_allowlists'][phase]);self.allowed=self.permanent
        self.auxiliary=set();self.temporaries=set(contract['session_temporary_unqualified_names'][phase])


class CallGuard:
    def __init__(self):
        self.expected=[('inventory',['parse'],False)]
        for label,_,_ in CELLS:
            self.expected.extend(((label,['snapshot','--select','consumer'],True),
                                  (label+'_reference',['snapshot','--select','reference'],False)))
        self.index=0

    def admit(self,label,command,managed):
        if self.index>=len(self.expected) or (label,command,managed)!=self.expected[self.index]:
            raise Refused('snapshot invocation differs from frozen ordered budget')
        self.index+=1


def snapshot_submit(calls,source,target,target_exists,expected,last_runtime=None):
    submits=[c for c in calls if c['method']=='SubmitEnrichedSQL']
    if len(submits)!=1:raise Refused('one ordinary snapshot submit required')
    row=submits[0];request=row['request']
    physical=lambda n:n.replace('`','').lower()
    wanted={source,target} if target_exists else {source}
    tables=request.get('tables',[])
    if (request.get('dialect')!='databricks' or request.get('execution_type')!=7
            or request.get('compare_unrendered_code') is not False
            or request.get('tolerate_nondeterminism') is not True
            or request.get('ignore_external_modifications') is not False
            or not request.get('table_namespace') or request.get('query_dependencies')!=[]
            or request.get('semantic_extras')!={'unique_key':'"id"'}
            or physical(request.get('target_table',''))!=target
            or len(tables)!=len(wanted) or {physical(t['name']) for t in tables}!=wanted
            or any(type(t.get('last_modified_epoch')) is not int or t['last_modified_epoch']<=0 for t in tables)):
        raise Refused('snapshot lacks bounded strategy/complete known physical evidence')
    if set(row.get('response',{}).get('response',{}))!={expected}:raise Refused('unexpected snapshot decision')
    decisions(calls,expected)
    allowed={'ValidateClientVersion','SubmitEnrichedSQL','ConfirmExecution'}
    if any(c['method'] not in allowed for c in calls):raise Refused('unexpected snapshot RPC')
    confirms=[c for c in calls if c['method']=='ConfirmExecution']
    if expected=='skip_execution':
        runtime=row['response']['response'][expected].get('execution_runtime_ms')
        if (confirms or type(runtime) is not int or runtime<=0 or runtime!=last_runtime):
            raise Refused('snapshot SKIP lacks its own positive confirmed runtime')
    else:
        if len(confirms)!=1:raise Refused('snapshot execution lacks one Confirm')
        confirm=confirms[0];outcome=confirm['request']
        pending=row['response']['response'][expected]['request_id']
        if (outcome.get('request_id')!=pending or confirm.get('response')!={'success':True,'request_id':pending}
                or confirm.get('response',{}).get('success') is not True
                or outcome.get('failed_to_clone') is not False
                or type(outcome.get('last_modified_epoch')) is not int or outcome['last_modified_epoch']<=0
                or type(outcome.get('execution_runtime_ms')) is not int or outcome['execution_runtime_ms']<=0):
            raise Refused('snapshot Confirm lacks causal successful known epoch/runtime')
    return row


def assert_reuse_evidence(current,previous,source,target,confirmed_epoch):
    def epochs(request):
        tables=request.get('tables',[])
        if len({t['name'].replace('`','').lower() for t in tables})!=len(tables):
            raise Refused('duplicate physical reuse evidence')
        return {t['name'].replace('`','').lower():t.get('last_modified_epoch') for t in tables}
    now=epochs(current);before=epochs(previous)
    if (type(confirmed_epoch) is not int or confirmed_epoch<=0
            or type(now.get(target)) is not int or now.get(target)!=confirmed_epoch
            or type(now.get(source)) is not int or now.get(source)!=before.get(source)
            or now.get(source,0)<=0
            or any(current.get(k)!=previous.get(k) for k in
                   ('sql','semantic_extras','table_namespace','execution_type'))):
        raise Refused('SKIP lacks own confirmed target/unchanged source/query/context evidence')


def assert_same_history(previous,current):
    if (previous.get('columns')!=current.get('columns')
            or Counter(map(tuple,previous.get('rows',[])))!=Counter(map(tuple,current.get('rows',[])))):
        raise Refused('snapshot SKIP changed complete Delta history/version')


def assert_status(results,uid,target,reused):
    rows=results.get('results',[])
    if len(rows)!=1:raise Refused('snapshot client result omitted/added node')
    row=rows[0];adapter=row.get('adapter_response')
    if (row.get('unique_id')!=uid or row.get('relation_name','').replace('`','').lower()!=target
            or row.get('status')!=('reused' if reused else 'success') or row.get('failures') is not None):
        raise Refused('snapshot client UID/relation/status differs from pinned native result')
    if reused:
        if adapter!={}:raise Refused('reused snapshot unexpectedly reports warehouse execution')
    elif not isinstance(adapter,dict) or adapter.get('code')!='SUCCESS' or not adapter.get('query_id'):
        raise Refused('snapshot client lacks native successful query identity')
    return row


def operation_manifest():
    return {'version':1,'ticket':151,'scope':'timestamp-ignore only; prepared offline, no live grant',
      'source_rows':SOURCE,'cells':CELLS,'source_write_labels_per_pair':[
          'create','initial','update','insert','delete','reinsert','equal_time'],
      'sql_change':"select id,concat(v,'_sql') as v,p,ts from exact owned source",
      'sql_change_effect':'no new versions at unchanged source timestamp',
      'independent_native_reference':True,'complete_scd_multiset':True,
      'clock_evidence':'exact source timestamps only; no warehouse generated-clock modes enabled',
      'blockers':['new_record NULL deleted business columns with State disabled',138,140],
      'not_covered':['check/invalidate native precision/bounds','config changes','node-kind transitions',
          'failure/retry','generated lifecycle/restart','NULL/duplicate keys','NULL timestamps',
          'schema evolution','full refresh','copy']}


def assert_native_sql(raw,target,temporary,first,skip=False):
    text=re.sub(r'/\*.*?\*/','',raw,flags=re.S)
    text='\n'.join(l for l in text.splitlines() if not l.lstrip().startswith('--'))
    normalize=lambda x:re.sub(r'\s+','',x).replace('`','').lower()
    relation=r'(`[^`]+`(?:\s*\.\s*`[^`]+`)*|[a-zA-Z_][a-zA-Z_0-9.]*)'
    writes=[]
    for statement in text.split(';'):
        statement=statement.strip()
        if not statement:continue
        if re.match(r'(?i)^(show|describe|select)\b',statement):continue
        if skip:raise Refused('native snapshot SKIP issued non-read SQL')
        create=re.match(r'(?is)^create\s+(?:or\s+replace\s+)?(temporary\s+view|table)\s+'+relation,statement)
        merge=re.match(r'(?is)^merge\s+into\s+'+relation,statement)
        drop=re.fullmatch(r'(?is)drop\s+view\s+(?:if\s+exists\s+)?'+relation,statement)
        if create:
            kind,name=create.group(1).lower(),normalize(create.group(2))
            if first and kind=='table' and name==target:writes.append('create_target')
            elif not first and kind=='temporary view' and name==temporary:writes.append('create_temporary')
            else:raise Refused('snapshot native CREATE outside exact observed lifetime')
        elif merge and not first and normalize(merge.group(1))==target:writes.append('merge_target')
        elif drop and not first and normalize(drop.group(1))==temporary:writes.append('drop_temporary')
        else:raise Refused('unexpected snapshot native statement/target')
    expected=[] if skip else (['create_target'] if first else ['create_temporary','merge_target','drop_temporary'])
    if writes!=expected:raise Refused('snapshot native lifetime differs from retained CTAS/MERGE path')
    return {'writes':writes,'persistent_extra':0,'temporary_peak':0 if first or skip else 1}


def literal_source(rows):
    def quote(v):return "'"+v.replace("'","''")+"'"
    return ','.join('('+','.join((r[0],'NULL' if r[1] is None else quote(r[1]),
        'DATE '+quote(r[2]),'TIMESTAMP '+quote(r[3])))+')' for r in rows)


class Snapshots(Baseline):
    entrypoint=Path(__file__)

    def __init__(self,args):
        grant=json.loads(Path(args.live_clearance).read_text())
        self.scope=SnapshotScope(args.run_id,grant)
        self.phase='rust' if args.rust_url else 'hosted'
        if (grant.get('status')!='LIVE CLEARED #151 timestamp-ignore'
                or self.phase not in grant.get('authorized_phases',[])
                or grant['run_ids'][self.phase]!=args.run_id):raise Refused('no exact snapshot backend authorization')
        if args.rust_url and not re.fullmatch(r'http://127\.0\.0\.1:\d+',args.rust_url):
            raise Refused('Rust URL must be owned literal loopback')
        sources={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in self.entrypoint.parent.glob('databricks_*.py')}
        if grant.get('source_sha256')!=sources:
            raise Refused('snapshot helper/source set differs from reviewed freeze')
        manifest=json.dumps(operation_manifest(),sort_keys=True,separators=(',',':')).encode()
        if grant.get('operation_manifest_sha256')!=hashlib.sha256(manifest).hexdigest():
            raise Refused('snapshot operations differ from reviewed ledger')
        receipt=Path(grant['allocation']['receipt'])
        if hashlib.sha256(receipt.read_bytes()).hexdigest()!=grant['allocation']['receipt_sha256']:
            raise Refused('snapshot allocation evidence changed')
        self.lease=grant;self.clearance=grant;self.guard=CallGuard()
        Campaign.__init__(self,args,self.scope)
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for role,output in profile['survey']['outputs'].items():
            profile['survey']['outputs'][role]={k:v for k,v in output.items() if k in {'type','host','http_path','catalog','schema','token','threads'}}
        self.save('profiles/profiles.yml',profile)
        self.save('live-clearance.json',grant)
        self.save('operation-manifest.json',operation_manifest());self.save('footprint.json',clearance_contract())
        self.save('source-provenance.json',{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in self.entrypoint.parent.glob('databricks_*.py')})
        self.save('campaign-entrypoint.py',self.entrypoint.read_text())

    def dbt(self,label,command,target='prod',managed=True):
        self.guard.admit(label,command,managed)
        calls=Campaign.dbt(self,label,command,target,managed)
        if not managed and calls:raise Refused('State-disabled reference/parse emitted RPC')
        return calls

    def init_schemas(self):
        allocation=self.clearance['allocation']
        schema=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+self.scope.schema('prod'))
        if any(schema.get(k)!=allocation[k] for k in ('schema_id','owner','created_at')):
            raise Refused('snapshot container identity changed')
        inventory=self.inventory();objects=inventory[self.scope.schema('prod')]
        expected=self.clearance['preexisting_objects'][self.phase]
        if sorted(objects,key=lambda x:x['full_name'])!=sorted(expected,key=lambda x:x['full_name']):
            raise Refused('snapshot preexisting inventory changed')
        if any(x['full_name'] in self.scope.allowed for x in objects):raise Refused('snapshot names already occupied')
        if (type(self.clearance['schema_capacity_limit']) is not int
                or not 0<self.clearance['schema_capacity_limit']<=100
                or len(objects)+4>self.clearance['schema_capacity_limit']):raise Refused('snapshot four permanent slots unavailable')
        self.before_inventory=inventory;self.save('lease-inventory-before.json',inventory)
        self.save('allocation-identity-before.json',schema)
        # Independently known literals validate the proposed six-digit projection
        # before any object write. This is a native preflight, not offline proof.
        preflight=self.sql('lossless_temporal_preflight',"SELECT current_timezone() AS timezone,date_format(TIMESTAMP '2026-11-12 01:50:34.797584','yyyy-MM-dd HH:mm:ss.SSSSSS') AS stamp,unix_micros(TIMESTAMP '2026-11-12 01:50:34.797584') AS epoch")
        from databricks_snapshot_oracle import micros
        if preflight['rows']!=[['Etc/UTC','2026-11-12 01:50:34.797584',str(micros('2026-11-12 01:50:34.797584'))]]:
            raise Refused('native timezone/lossless projection not yet characterized')
        self.save('lossless-codec-preflight.json',preflight)

    def write_snapshots(self,projected=False):
        for name,source_name in (('consumer','inputs'),('reference','ref_inputs')):
            source=self.scope.relation('prod',source_name)
            v="concat(v,'_sql') as v" if projected else 'v'
            # Preserve actual retained direct physical SQL dependency form.
            self.save('project/snapshots/'+name+'.sql','{% snapshot '+name+' %}\n'+
                "{{ config(alias='"+self.scope.relation_prefix+name+"',unique_key='id',strategy='timestamp',updated_at='ts',hard_deletes='ignore',state={'lag_tolerance':'0s'}) }}\n"+
                'select id,'+v+',p,ts from '+source+'\n{% endsnapshot %}\n')

    def check_physical(self,label,name,ledger):
        target=self.scope.relation('prod',name)
        raw=self.observe(label,target);check_schema(raw,NATIVE_SCHEMA)
        properties=json.loads((self.work/'physical'/f'{label}_properties.json').read_text())
        props={r[0]:r[1] for r in properties['rows']}
        if props.get('Type')!='MANAGED' or props.get('Provider','').lower()!='delta':raise Refused('physical:snapshot-properties')
        projection=self.sql(label+'_lossless',projection_sql(target))
        rows=decode_projection(projection);result=assert_history(rows,ledger,{})
        self.save('physical/'+label+'_lossless.json',projection)
        self.save('checks/'+label+'.json',result)
        history=self.sql(label+'_full_history','DESCRIBE HISTORY '+target)
        self.save('physical/'+label+'_history.json',history)
        return rows,history

    def source_change(self,label):
        for name in ('inputs','ref_inputs'):
            source=self.scope.relation('prod',name)
            if label=='upsert':
                self.sql(label+'_'+name+'_update',f"UPDATE {source} SET v='changed',ts=TIMESTAMP '{T2}' WHERE id=1")
                self.sql(label+'_'+name+'_insert',f'INSERT INTO {source} VALUES '+literal_source((SOURCE['upsert'][2],)))
            elif label=='delete':self.sql(label+'_'+name,f'DELETE FROM {source} WHERE id=2')
            elif label=='reinsert':self.sql(label+'_'+name,f'INSERT INTO {source} VALUES '+literal_source((SOURCE['reinsert'][1],)))
            elif label=='same_timestamp':self.sql(label+'_'+name,f"UPDATE {source} SET v='equal_time_new_value' WHERE id=1")

    def baseline(self):
        self.setup_project();self.save('project/models/sources.yml',{'version':2,'sources':[]})
        self.write_snapshots()
        self.dbt('inventory',['parse'],managed=False)
        manifest=json.loads(next((self.work/'targets').glob('*_inventory/manifest.json')).read_text())
        check_manifest(self.scope,manifest)
        nodes=list(manifest['nodes'].values())
        if len(nodes)!=2:raise Refused('snapshot manifest has extra/missing nodes')
        for node in nodes:
            cfg=node['config'];name=node['name']
            if (name not in ('consumer','reference') or node['resource_type']!='snapshot'
                    or cfg.get('strategy')!='timestamp' or cfg.get('hard_deletes')!='ignore'
                    or cfg.get('updated_at')!='ts' or cfg.get('unique_key')!='id'
                    or cfg.get('pre-hook') or cfg.get('post-hook') or cfg.get('check_cols')
                    or node.get('depends_on',{}).get('nodes')):
                raise Refused('snapshot manifest outside direct-source timestamp-ignore scope')
            self.scope.check('.'.join((node['database'],node['schema'],node['alias'])))
        self.save('actual-manifest-scope.json',nodes)
        for name in ('inputs','ref_inputs'):
            source=self.scope.relation('prod',name)
            self.sql('create_'+name,f'CREATE TABLE {source} (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
            self.sql('initial_'+name,f'INSERT INTO {source} VALUES '+literal_source(SOURCE['first']))
        managed=Ledger('timestamp','ignore');reference=Ledger('timestamp','ignore');previous_epochs=None;last_runtime=None;latest_confirmed_epoch=None;previous_request=None;previous_history=None
        for label,stage,expected in CELLS:
            self.source_change(label)
            if label=='sql_change':self.write_snapshots(True)
            projected=label in ('sql_change','sql_change_repeat')
            source_rows=tuple((r[0],r[1]+'_sql',r[2],r[3]) for r in SOURCE[stage]) if projected else SOURCE[stage]
            # Freeze expected histories before native execution. Reference always
            # executes; managed changes only for captured execution decisions.
            if expected=='ready_to_execute':managed=managed.execute(source_rows,label)
            reference=reference.execute(source_rows,label+'_reference')
            self.save('expected/'+label+'.json',{'managed':managed.rows({}),'reference':reference.rows({}),'source':SOURCE[stage]})
            calls=self.dbt(label,['snapshot','--select','consumer'])
            index=self.n
            status=json.loads((self.work/'targets'/f'{index:03d}_{label}'/'run_results.json').read_text())
            self.save('client-status/'+label+'.json',assert_status(status,'snapshot.survey_'+self.a.run_id+'.consumer',self.scope.relation('prod','consumer'),expected=='skip_execution'))
            row=snapshot_submit(calls,self.scope.relation('prod','inputs'),self.scope.relation('prod','consumer'),label!='first',expected,last_runtime)
            request=row['request']
            if request.get('labels',{}).get('dbt_node_unique_id')!='snapshot.survey_'+self.a.run_id+'.consumer':
                raise Refused('snapshot wire UID differs from exact native node')
            if hasattr(self,'namespace') and request['table_namespace']!=self.namespace:
                raise Refused('snapshot connection namespace changed')
            self.namespace=request['table_namespace']
            sql_expected='select id,'+("concat(v,'_sql') as v" if projected else 'v')+',p,ts from '+self.scope.relation('prod','inputs')
            normalize=lambda text:re.sub(r'\s+',' ',text).strip()
            if normalize(request['sql'])!=normalize(sql_expected):raise Refused('snapshot query differs from frozen source/projection SQL')
            epochs={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in row['request']['tables']}
            source=self.scope.relation('prod','inputs')
            if label in SOURCE and label!='first' and epochs[source]<=previous_epochs[source]:
                raise Refused('snapshot source epoch did not advance; #140 remains required')
            if expected=='ready_to_execute':
                confirm=next(c for c in calls if c['method']=='ConfirmExecution')['request']
                last_runtime=confirm['execution_runtime_ms'];latest_confirmed_epoch=confirm['last_modified_epoch']
                self.save('confirmed/'+label+'.json',{'request_id':confirm['request_id'],
                    'last_modified_epoch':latest_confirmed_epoch,'execution_runtime_ms':last_runtime})
            else:
                assert_reuse_evidence(request,previous_request,source,self.scope.relation('prod','consumer'),latest_confirmed_epoch)
            previous_request=request
            previous_epochs=epochs
            query=(self.work/'logs'/f'{index:03d}_{label}'/'query_log.sql').read_text()
            native=assert_native_sql(query,self.scope.relation('prod','consumer'),self.scope.relation_prefix+'consumer__dbt_tmp',label=='first',expected=='skip_execution')
            self.save('materialization/'+label+'.json',native)
            self.explain(label+'_tree',[row])
            managed_rows,history=self.check_physical(label,'consumer',managed)
            if expected=='skip_execution':assert_same_history(previous_history,history)
            previous_history=history
            self.dbt(label+'_reference',['snapshot','--select','reference'],managed=False)
            status=json.loads((self.work/'targets'/f'{self.n:03d}_{label}_reference'/'run_results.json').read_text())
            self.save('client-status/'+label+'_reference.json',assert_status(status,'snapshot.survey_'+self.a.run_id+'.reference',self.scope.relation('prod','reference'),False))
            query=(self.work/'logs'/f'{self.n:03d}_{label}_reference'/'query_log.sql').read_text()
            self.save('materialization/'+label+'_reference.json',assert_native_sql(query,self.scope.relation('prod','reference'),self.scope.relation_prefix+'reference__dbt_tmp',label=='first'))
            reference_rows,_=self.check_physical(label+'_reference','reference',reference)
            if Counter(managed_rows)!=Counter(reference_rows):raise Refused('managed/disabled complete timestamp histories disagree')
            after=self.inventory()
            if any(r not in after[s] for s,prior in self.before_inventory.items() for r in prior):raise Refused('protected inventory changed')
            for s,objects in after.items():
                if any(r not in self.before_inventory[s] and r['full_name'] not in self.scope.permanent for r in objects):raise Refused('snapshot unplanned persistent object')
            for source_name in ('inputs','ref_inputs'):
                source=self.scope.relation('prod',source_name)
                complete=self.sql(label+'_'+source_name+'_lossless',source_projection_sql(source))
                assert_source(complete,SOURCE[stage])
                self.save('physical/'+label+'_'+source_name+'_complete.json',complete)
                self.save('physical/'+label+'_'+source_name+'_history.json',self.sql(label+'_'+source_name+'_history','DESCRIBE HISTORY '+source))
            self.save('inventory/'+label+'.json',after)
        if self.guard.index!=25:raise Refused('snapshot call budget incomplete')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',action='store_true')
    for key in ('dbt','proxy','probe','work-dir','live-clearance','run-id'):parser.add_argument('--'+key)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--rust-url')
    args=parser.parse_args()
    if args.plan:
        print(json.dumps({'footprint':clearance_contract(),'operations':operation_manifest()},indent=2));return 0
    if any(getattr(args,k.replace('-','_')) is None for k in ('dbt','proxy','probe','work-dir','live-clearance','run-id')):
        parser.error('live execution requires every explicit pinned argument and clearance')
    return Snapshots(args).run()


if __name__=='__main__':raise SystemExit(main())
