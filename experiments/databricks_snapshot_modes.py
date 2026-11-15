#!/usr/bin/env python3
"""#151 source-clock check/ignore and precision/valid-SCD FULL→snapshot supplement.

Every expected row derives from a frozen source operation ledger. Native disabled
references have their own histories; no target row supplies an expected clock.
Only exact fresh UUID objects in an existing evidenced owned container are used.
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
from databricks_snapshot_oracle import (Ledger, NATIVE_SCHEMA, assert_history,
    assert_source, check_schema, decode_projection, projection_sql, source_projection_sql)
from databricks_snapshots import (assert_native_sql, assert_reuse_evidence,
    assert_same_history, assert_status, literal_source, snapshot_submit)
from databricks_survey import Campaign, Refused, Scope, check_manifest

SCHEMA='ds_20261112a134ef_prod'
SCHEMA_ID='59559b89-efa8-4ed7-b106-1394c2718cc3'
OBJECTS={'hosted':'c948f761eba4495ea9d3592b46916c4d','rust':'dd2962a732824adba62a5f1539154aca'}
RUNS={'hosted':'20261115a151cc','rust':'20261115b151dd'}
T0='2026-11-01 12:00:00.123450'
T1='2026-11-02 12:00:00.654321'
T2='2026-11-03 12:00:00.000001'
T3='2026-11-04 12:00:00.100010'
T4='2026-11-05 12:00:00.123456'
T5='2026-11-06 12:00:00.654320'
T6='2026-11-05 12:00:00.123457'
A=('1','one','2026-11-01',T0)
B=('2','two','2026-11-02',T1)
SOURCE={'first':(A,B)}
SOURCE['noncheck']=(('1','one','2026-11-03',T2),B)
SOURCE['null']=(('1',None,'2026-11-03',T3),B)
SOURCE['restore']=(('1','restored','2026-11-03',T4),B)
SOURCE['delete']=(SOURCE['restore'][0],)
SOURCE['reinsert']=(SOURCE['restore'][0],('2','returned','2026-11-02',T5))
SOURCE['older']=(('1','older','2026-11-03','2026-11-01 12:00:00.123449'),SOURCE['reinsert'][1])
SOURCE['equal']=(('1','equal','2026-11-03',T4),SOURCE['reinsert'][1])
SOURCE['advance']=(('1','advanced','2026-11-03',T6),SOURCE['reinsert'][1])
# label, kind, source stage, executes, mutates source
CELLS=(('full','full','first',True,False),
 ('timestamp_adopt','timestamp','first',True,False),('timestamp_repeat','timestamp','first',False,False),
 ('check_first','check','first',True,False),('check_repeat','check','first',False,False),
 ('noncheck','check','noncheck',True,True),('null','check','null',True,True),
 ('null_repeat','check','null',False,False),('restore','check','restore',True,True),
 ('delete','check','delete',True,True),('reinsert','check','reinsert',True,True),
 ('reinsert_repeat','check','reinsert',False,False),
 ('timestamp_catchup','timestamp','reinsert',True,False),
 ('older','timestamp','older',True,True),('equal','timestamp','equal',True,True),
 ('advance','timestamp','advance',True,True),('advance_repeat','timestamp','advance',False,False))


def plan():
    return {'ticket':151,'run_ids':RUNS,'object_ids':OBJECTS,'catalog':'cdo_demo_db','schema':SCHEMA,
      'strategy_scope':'timestamp + check(updated_at=ts,check_cols=[v]); hard_deletes=ignore',
      'names':{phase:[f'cdo_demo_db.{SCHEMA}.s151_{oid}_{name}' for name in ('inputs','check','check_ref','timestamp','timestamp_ref')] for phase,oid in OBJECTS.items()},
      'source':SOURCE,'cells':CELLS,'managed':17,'disabled_native':12,'disabled_compile':2,
      'permanent_per_backend':5,'new_schemas':0,'extra_persistent':0,'temporary_peak':1,
      'source_mutations':['CREATE','INSERT','noncheck','null','restore','delete','reinsert','older','equal','advance'],
      'retained':True,'full_transition':'native FULL creates valid complete SCD schema; native SNAPSHOT adopts it',
      'excluded':['generated-clock check without updated_at','invalidate clock fidelity','new_record NULL business-column client defect',
                  'NULL/duplicate keys','NULL updated_at','schema evolution','copy']}


def histories():
    ledgers={kind:Ledger(kind,'ignore',source_updated_at=True) for kind in ('check','timestamp')}
    result={}
    for label,kind,stage,execute,_ in CELLS:
        key='timestamp' if kind=='full' else kind
        if execute:ledgers[key]=ledgers[key].execute(SOURCE[stage],label)
        result[label]=ledgers[key].rows({})
    return result


def assert_source_clock_sql(query):
    code=re.sub(r'/\*.*?\*/|--[^\n]*','',query,flags=re.S).lower()
    writes=[statement for statement in code.split(';') if statement.strip().startswith(('create ','merge '))]
    if any(re.search(r'current_timestamp\s*\(',statement) for statement in writes):
        raise Refused('snapshot-modes:unexpected-generated-check-clock')
    compact=re.sub(r'\s+',' ', '\n'.join(writes))
    if 'ts as dbt_updated_at' not in compact or 'ts as dbt_valid_from' not in compact:
        raise Refused('snapshot-modes:missing-explicit-source-clock-DML')


def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class ModeScope(LeaseScope):
    profile_roles=('prod',)

    def __init__(self,run,lease,phase):
        Scope.__init__(self,run)
        if run!=RUNS[phase] or uuid.UUID(OBJECTS[phase]).hex!=OBJECTS[phase]:raise Refused('snapshot-modes:exact-run')
        owned=set()
        for evidence in lease['source_evidence']:
            if digest(evidence['path'])!=evidence['sha256']:raise Refused('snapshot-modes:ownership-digest')
            owned.update(json.loads(Path(evidence['path']).read_text())['retained_schemas'])
        if SCHEMA not in owned:raise Refused('snapshot-modes:owned-container')
        self.roles={'prod':SCHEMA};self.schemas={SCHEMA};self.relation_prefix='s151_'+OBJECTS[phase]+'_'
        self.allowed={f'cdo_demo_db.{SCHEMA}.{self.relation_prefix}{name}' for name in ('inputs','check','check_ref','timestamp','timestamp_ref')}


class SnapshotModes(Baseline):
    def __init__(self,args):
        grant=json.loads(Path(args.live_clearance).read_text())
        self.phase='rust' if args.rust_url else 'hosted'
        frozen=json.loads(json.dumps({'plan':plan(),'histories':histories()}))
        if (grant.get('status')!='LIVE CLEARED #151 source-clock modes' or self.phase not in grant['authorized_phases']
                or json.loads(Path(args.operation_ledger).read_text())!=frozen
                or digest(args.operation_ledger)!=grant['operation_sha256']
                or grant['source_sha256']!={p:digest(Path(__file__).parent/p) for p in grant['source_sha256']}):
            raise Refused('snapshot-modes:exact-reviewed-plan-and-sources')
        self.lease=json.loads(Path(args.schema_lease).read_text())
        self.scope=ModeScope(args.run_id,self.lease,self.phase)
        self.grant=grant;self.frozen=frozen;self.call_index=0
        Campaign.__init__(self,args,self.scope)
        # Prevent inherited hooks/profile options from adding any side effects.
        import yaml
        profile=yaml.safe_load((self.profiles/'profiles.yml').read_text())
        for output in profile['survey']['outputs'].values():
            for key in list(output):
                if key not in {'type','host','http_path','catalog','schema','token','threads'}:del output[key]
        self.save('profiles/profiles.yml',profile)
        self.save('source-clock-plan.json',frozen)
        self.save('live-clearance.json',grant)
        self.save('runner-source.py',Path(__file__).read_text())
        self.save('binary-provenance.json',{key:{'path':str(Path(getattr(args,key)).resolve()),'sha256':digest(getattr(args,key))} for key in ('dbt','proxy','probe')})
        self.runtime={};self.previous={};self.confirmed={};self.history={}
        self.prefix=Path(args.resume_prefix) if args.resume_prefix else None
        self.completed=set()
        self.prefixes=[]
        if self.prefix:
            from capture_integrity import verify_capture
            current=self.prefix
            while current:
                self.prefixes.insert(0,current)
                anchor=current/'retained-prefix.json'
                current=Path(json.loads(anchor.read_text())['capture']).parent.parent if anchor.exists() else None
            expected_prefixes=['e071de6f07c62e562d5664a38e699c40da762533d5f0c67a0009c92716d16561',
                'a80bf6d04bda26fd35943c893dd6f5cb67faa551994f089185898682dc5f1be6']
            if self.phase!='hosted' or len(self.prefixes)>2:raise Refused('snapshot-modes:exact-hosted-prefix-only')
            for root,expected in zip(self.prefixes,expected_prefixes):
                captures=list((root/'golden').glob('*.jsonl'))
                if len(captures)!=1 or digest(captures[0])!=expected:raise Refused('snapshot-modes:changed-prefix')
                verify_capture(captures[0])
                events=json.loads((root/'operations.json').read_text())
                self.completed.update(e['label'] for e in events if e['kind']=='dbt-result' and e['returncode']==0)
            self.save('retained-prefix.json',{'capture':str(next((self.prefix/'golden').glob('*.jsonl'))),
                'sha256':digest(next((self.prefix/'golden').glob('*.jsonl'))),'completed_commands':sorted(self.completed),'repeated_commands':0})
        self.expected_calls=[('snapshots_preflight',['compile'],False),('full_preflight',['compile'],False)]
        for label,kind,_,execute,_ in CELLS:
            key='timestamp' if kind=='full' else kind
            command='run' if kind=='full' else 'snapshot'
            self.expected_calls.append((label,[command,'--select',key],True))
            if execute:self.expected_calls.append((label+'_reference',[command,'--select',key+'_ref'],False))

    def dbt(self,label,command,target='prod',managed=True):
        if self.call_index>=len(self.expected_calls) or (label,command,managed)!=self.expected_calls[self.call_index]:
            raise Refused('snapshot-modes:exact-ordered-command-budget')
        self.call_index+=1
        if label in self.completed:
            self.n+=1
            paths=[path for root in self.prefixes for path in (root/'wire').glob('*_'+label+'.json')]
            if len(paths)!=1:raise Refused('snapshot-modes:missing-original-command-wire')
            return json.loads(paths[0].read_text())
        return Campaign.dbt(self,label,command,target,managed)

    def evidence(self,label):
        if label not in self.completed:return self.work
        roots=[root for root in self.prefixes if list((root/'wire').glob('*_'+label+'.json'))]
        if len(roots)!=1:raise Refused('snapshot-modes:ambiguous-prefix-artifact')
        return roots[0]

    def run(self):
        result=Baseline.run(self)
        if self.prefix:
            from databricks_snapshots_continuation import combine_captures
            old=next((self.prefixes[0]/'golden').glob('*.jsonl'))
            for index,root in enumerate(self.prefixes[1:]):
                new=next((root/'golden').glob('*.jsonl'))
                output=self.work/f'combined-prefix-{index}.jsonl'
                combine_captures(old,new,output);old=output
            new=next((self.work/'golden').glob('*.jsonl'))
            combine_captures(old,new,self.work/'databricks_snapshot_modes_hosted_whole.jsonl')
        return result

    def inventory(self):
        rows=[];token=None;seen=set()
        while True:
            endpoint='/api/2.1/unity-catalog/tables?catalog_name=cdo_demo_db&schema_name='+SCHEMA
            if token:endpoint+='&page_token='+quote(token,safe='')
            response=self.api('get',endpoint)
            rows.extend({k:row[k] for k in ('name','full_name','table_type')} for row in response.get('tables',[]))
            token=response.get('next_page_token')
            if not token:break
            if token in seen:raise Refused('snapshot-modes:pagination-loop')
            seen.add(token)
        return {SCHEMA:rows}

    def init_schemas(self):
        metadata=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+SCHEMA)
        if metadata.get('schema_id')!=SCHEMA_ID or metadata.get('owner')!='max.firman@gmail.com':raise Refused('snapshot-modes:schema-owner')
        self.save('schema-ownership.json',{k:metadata[k] for k in ('full_name','schema_id','owner')})
        self.before_inventory=self.inventory()
        rows=self.before_inventory[SCHEMA]
        existing={row['full_name'] for row in rows if row['full_name'] in self.scope.allowed}
        wanted={self.scope.relation('prod',name) for name in (('inputs','timestamp','timestamp_ref','check') if len(self.prefixes)==2 else ('inputs','timestamp','timestamp_ref'))} if self.prefix else set()
        if len(rows)+(5-len(wanted))>100 or existing!=wanted:raise Refused('snapshot-modes:capacity-or-collision')
        self.save('lease-inventory-before.json',self.before_inventory)
        codec=self.sql('precision_preflight',"SELECT current_timezone() AS timezone,date_format(TIMESTAMP '2026-11-01 12:00:00.123450','yyyy-MM-dd HH:mm:ss.SSSSSS') AS stamp,unix_micros(TIMESTAMP '2026-11-01 12:00:00.123450') AS epoch,cast(TIMESTAMP '2026-11-01 12:00:00.123450' AS STRING) AS hash_stamp")
        from databricks_snapshot_oracle import micros
        if codec['rows']!=[['Etc/UTC',T0,str(micros(T0)),'2026-11-01 12:00:00.12345']]:raise Refused('snapshot-modes:native-lossless-codec')
        self.save('precision-preflight.json',codec)

    def write_projects(self):
        self.setup_project();(self.project/'models/sources.yml').unlink()
        for kind in ('check','timestamp'):
            for suffix in ('','_ref'):
                name=kind+suffix
                extra=",check_cols=['v']" if kind=='check' else ''
                self.save('project/snapshots/'+name+'.sql','{% snapshot '+name+' %}\n'+
                    "{{ config(alias='"+self.scope.relation_prefix+name+"',unique_key='id',strategy='"+kind+"',updated_at='ts',hard_deletes='ignore'"+extra+",state={'lag_tolerance':'0s'}) }}\n"+
                    'select id,v,p,ts from '+self.scope.relation('prod','inputs')+'\n{% endsnapshot %}\n')
        self.snapshot_project=self.project
        self.full_project=self.work/'full-project'
        self.save('full-project/dbt_project.yml',{'name':'full_'+self.a.run_id,'version':'1.0','profile':'survey'})
        for name in ('timestamp','timestamp_ref'):
            self.save('full-project/models/'+name+'.sql',"{{ config(materialized='table',alias='"+self.scope.relation_prefix+name+"',file_format='delta',state={'lag_tolerance':'0s'}) }}\n"+
                "select id,v,p,ts,md5(coalesce(cast(id as string),'') || '|' || coalesce(cast(ts as string),'')) as dbt_scd_id,ts as dbt_updated_at,ts as dbt_valid_from,cast(null as timestamp) as dbt_valid_to from "+self.scope.relation('prod','inputs'))

    def compile_preflight(self):
        for label,project in (('snapshots',self.snapshot_project),('full',self.full_project)):
            self.project=project
            calls=self.dbt(label+'_preflight',['compile'],managed=False)
            if calls:raise Refused('snapshot-modes:disabled-compile-RPC')
            manifest=json.loads((self.evidence(label+'_preflight')/'targets'/f'{self.n:03d}_{label}_preflight/manifest.json').read_text())
            check_manifest(self.scope,manifest)
            wanted={label for label in ('timestamp','timestamp_ref')} if label=='full' else {'check','check_ref','timestamp','timestamp_ref'}
            if {n['name'] for n in manifest['nodes'].values()}!=wanted:raise Refused('snapshot-modes:manifest-nodes')
            for node in manifest['nodes'].values():
                cfg=node['config'];self.scope.check('.'.join((node['database'],node['schema'],node['alias'])))
                if cfg.get('pre-hook') or cfg.get('post-hook') or node.get('depends_on',{}).get('nodes'):raise Refused('snapshot-modes:unexpected-hook-dependency')
                if label=='snapshots' and (cfg.get('updated_at')!='ts' or cfg.get('hard_deletes')!='ignore' or cfg.get('unique_key')!='id'
                    or cfg.get('strategy')!=node['name'].split('_')[0] or (cfg.get('check_cols')!=['v'] if node['name'].startswith('check') else bool(cfg.get('check_cols')))):
                    raise Refused('snapshot-modes:manifest-config')
            self.save(label+'-preflight-manifest.json',manifest)
        self.project=self.snapshot_project

    def change(self,stage):
        source=self.scope.relation('prod','inputs')
        row=SOURCE[stage][0]
        if stage=='delete':sql=f'DELETE FROM {source} WHERE id=2'
        elif stage=='reinsert':sql=f'INSERT INTO {source} VALUES '+literal_source((SOURCE[stage][1],))
        else:
            value='NULL' if row[1] is None else "'"+row[1]+"'"
            sql=f"UPDATE {source} SET v={value},p=DATE '{row[2]}',ts=TIMESTAMP '{row[3]}' WHERE id=1"
        self.sql(stage+'_source',sql)

    def physical(self,label,name,expected):
        target=self.scope.relation('prod',name)
        raw=self.observe(label,target);check_schema(raw,NATIVE_SCHEMA)
        props=json.loads((self.work/'physical'/f'{label}_properties.json').read_text())
        values={r[0]:r[1] for r in props['rows']}
        if values.get('Type')!='MANAGED' or values.get('Provider','').lower()!='delta':raise Refused('snapshot-modes:managed-delta')
        projection=self.sql(label+'_lossless',projection_sql(target));actual=decode_projection(projection)
        if Counter(actual)!=Counter(map(tuple,expected)):raise Refused('snapshot-modes:complete-frozen-SCD-history')
        self.save('physical/'+label+'_lossless.json',projection)
        history=self.sql(label+'_history','DESCRIBE HISTORY '+target)
        self.save('physical/'+label+'_history.json',history)
        return history

    def baseline(self):
        self.write_projects();self.compile_preflight()
        source=self.scope.relation('prod','inputs')
        if not self.prefix:
            self.sql('create_source',f'CREATE TABLE {source} (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
            self.sql('initial_source',f'INSERT INTO {source} VALUES '+literal_source(SOURCE['first']))
        else:
            assert_source(self.sql('resume_source',source_projection_sql(source)),SOURCE['first'])
            self.physical('resume_timestamp','timestamp',self.frozen['histories']['timestamp_adopt'])
            self.physical('resume_reference','timestamp_ref',self.frozen['histories']['full'])
        for label,kind,stage,execute,mutate in CELLS:
            if mutate:self.change(stage)
            key='timestamp' if kind=='full' else kind
            self.project=self.full_project if kind=='full' else self.snapshot_project
            target=self.scope.relation('prod',key)
            calls=self.dbt(label,['run' if kind=='full' else 'snapshot','--select',key])
            expected='ready_to_execute' if execute else 'skip_execution'
            if kind=='full':
                submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
                if len(submits)!=1 or submits[0]['request']['execution_type']!=1:raise Refused('snapshot-modes:real-FULL-required')
                row=submits[0]
                if set(row['response']['response'])!={expected}:raise Refused('snapshot-modes:FULL-decision')
                confirms=[r for r in calls if r['method']=='ConfirmExecution']
                if len(confirms)!=1 or confirms[0]['response'].get('success') is not True:raise Refused('snapshot-modes:FULL-outcome')
            else:
                row=snapshot_submit(calls,source,target,label!='check_first',expected,self.runtime.get(key),node_name=key)
            request=row['request']
            if request.get('freshness_tolerance_seconds')!=0:raise Refused('snapshot-modes:nonzero-lag')
            uid=('model.full_' if kind=='full' else 'snapshot.survey_')+self.a.run_id+'.'+key
            status=json.loads((self.evidence(label)/'targets'/f'{self.n:03d}_{label}/run_results.json').read_text())
            assert_status(status,uid,target,not execute)
            if execute:
                confirm=next(r for r in calls if r['method']=='ConfirmExecution')['request']
                self.runtime[key]=confirm['execution_runtime_ms'];self.confirmed[key]=confirm['last_modified_epoch']
            else:assert_reuse_evidence(request,self.previous[key],source,target,self.confirmed[key])
            self.previous[key]=request
            query=(self.evidence(label)/'logs'/f'{self.n:03d}_{label}/query_log.sql').read_text()
            assert_native_sql(query,target,self.scope.relation_prefix+key+'__dbt_tmp',kind=='full' or label=='check_first',not execute)
            if kind=='check' and execute:
                # Exact source-clock contract: all ordinary versions hash id/ts,
                # and generated clocks cannot substitute for configured ts.
                assert_source_clock_sql(query)
            if not any((root/'wire'/(label+'_tree_explain.json')).exists() for root in self.prefixes):self.explain(label+'_tree',[row])
            history=self.physical(label,key,self.frozen['histories'][label])
            if not execute:assert_same_history(self.history[key],history)
            self.history[key]=history
            if execute:
                ref=key+'_ref';ref_label=label+'_reference'
                disabled=self.dbt(ref_label,['run' if kind=='full' else 'snapshot','--select',ref],managed=False)
                if disabled:raise Refused('snapshot-modes:disabled-reference-RPC')
                status=json.loads((self.evidence(ref_label)/'targets'/f'{self.n:03d}_{ref_label}/run_results.json').read_text())
                assert_status(status,('model.full_' if kind=='full' else 'snapshot.survey_')+self.a.run_id+'.'+ref,self.scope.relation('prod',ref),False)
                query=(self.evidence(ref_label)/'logs'/f'{self.n:03d}_{ref_label}/query_log.sql').read_text()
                assert_native_sql(query,self.scope.relation('prod',ref),self.scope.relation_prefix+ref+'__dbt_tmp',kind=='full' or label=='check_first')
            self.physical(label+'_reference_check',key+'_ref',self.frozen['histories'][label])
            assert_source(self.sql(label+'_source_lossless',source_projection_sql(source)),SOURCE[stage])
            after=self.inventory()
            if any(row not in after[SCHEMA] for row in self.before_inventory[SCHEMA]) or any(row not in self.before_inventory[SCHEMA] and row['full_name'] not in self.scope.allowed for row in after[SCHEMA]):raise Refused('snapshot-modes:unplanned-inventory')
            self.save('inventory/'+label+'.json',after)
        if self.call_index!=len(self.expected_calls):raise Refused('snapshot-modes:incomplete-command-budget')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan',action='store_true')
    for key in ('dbt','proxy','probe','work-dir','schema-lease','live-clearance','operation-ledger','run-id'):parser.add_argument('--'+key)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--rust-url')
    parser.add_argument('--resume-prefix')
    args=parser.parse_args()
    if args.plan:print(json.dumps({'plan':plan(),'histories':histories()},indent=2));return 0
    return SnapshotModes(args).run()

if __name__=='__main__':raise SystemExit(main())
