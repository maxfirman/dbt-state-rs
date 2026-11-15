#!/usr/bin/env python3
"""Bounded #150 real-clock lookback and predicate campaign, hosted first."""
import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import uuid

from databricks_freshness import decisions
from databricks_merge_recovery import Recovery
from databricks_microbatch import (assert_reference, assert_result, assert_typed,
                                  days, in_window, literal_reference, native_writes)
from databricks_readonly_reference import reference_canary, show_reference
from databricks_survey import Refused, check_manifest


def plan(today):
    """Literal operation ledger, anchored before any source or target exists."""
    d=lambda offset:(today+timedelta(days=offset)).isoformat()
    row=lambda i,v,offset:(str(i),v,d(offset),d(offset)+'T12:00:00.123Z')
    old=row(10,'outside-old',-3);older=row(20,'lookback-old',-2)
    one=row(1,'one-old',0);dup=row(3,'duplicate-old',0)
    future=row(4,'exclusive-boundary',1);future=(*future[:3],d(1)+'T00:00:00.000Z')
    null=('5','null-time',None,None)
    A=(old,older,one,dup,dup,future,null)
    old_new=row(10,'outside-new',-3);older_new=row(20,'lookback-new',-2)
    one_new=row(1,'one-new',0);dup_new=row(3,'duplicate-new',0)
    B=(old_new,older_new,one_new,dup_new,dup_new,future,null)
    one_excluded=row(1,'excluded-current',0);pred=row(3,'predicate-new',0)
    C=(old_new,older_new,one_excluded,pred,pred,future,null)
    fixed=(d(-3),d(1));current=(d(0),d(1));back=(d(-2),d(1))
    specs=[('first',A,fixed,0,None,None,(old,older,one,dup,dup),(old,older,one,dup,dup)),
           ('late_zero',B,current,0,None,None,(old,older,one_new,dup_new,dup_new),(one_new,dup_new,dup_new)),
           ('lookback_two',B,back,2,None,None,(old,older_new,one_new,dup_new,dup_new),(older_new,one_new,dup_new,dup_new)),
           ('predicate',C,current,0,3,3,(old,older_new,one_new,pred,pred),(pred,pred)),
           ('widen',C,current,0,1,1,(old,older_new,one_excluded,pred,pred),(one_excluded,pred,pred))]
    return [dict(label=label+('_repeat' if repeat else ''),source=source,window=window,
                 lookback=lookback,predicate=predicate,filter=filter_,R=R,F=F,repeat=repeat,
                 explicit=label=='first')
            for label,source,window,lookback,predicate,filter_,R,F in specs for repeat in (False,True)]


def replace_window(previous,source,window,predicate):
    eligible=lambda row:in_window(row,*window) is True and (predicate is None or row[0] is not None and int(row[0])>=predicate)
    return tuple(row for row in previous if not eligible(row))+tuple(row for row in source if eligible(row))


def failure_sql(raw,target,source,window):
    """Exact pinned-client limitation; neither generic transport nor SQL errors qualify."""
    observed=native_writes(raw);expected=[];predicates=['id >= 0']
    for i,(start,end) in enumerate(days(window)):
        projection=("select * from (select * from "+source+" where ts >= '"+start+
                    "t00:00:00+00:00' and ts < '"+end+"t00:00:00+00:00')")
        if i==0:expected.append('create or replace table '+target+' using delta as '+projection)
        else:
            temp=target.split('.')[-1]+'__dbt_tmp_'+start.replace('-','')
            expected.append('create or replace temporary view '+temp+' as '+projection)
            predicates.extend(["cast(ts as timestamp) >= '"+start+" 00:00:00+00:00'",
                               "cast(ts as timestamp) < '"+end+" 00:00:00+00:00'"])
            expected.append('insert into '+target+' by name replace where '+' and '.join(predicates)+' table '+temp)
            if i==1:expected.append('alter table '+target+' cluster by none')
    if observed!=expected:raise Refused('moving-windows:uncharacterized-failed-native-footprint')
    return observed


class MovingWindows(Recovery):
    entrypoint=Path(__file__)

    def __init__(self,args):
        super().__init__(args)
        self.today=date.fromisoformat(args.utc_day)
        self.cases=plan(self.today)
        self.scope.relation_prefix='mw150_'+args.object_id+'_'
        self.target=f'cdo_demo_db.{args.schema}.{self.scope.relation_prefix}consumer'
        self.source=self.target.removesuffix('consumer')+'inputs'
        self.scope.permanent={self.target,self.source}
        self.scope.auxiliary={self.target+s for s in ('__dbt_stg','__dbt_backup')}
        self.scope.allowed=self.scope.permanent|self.scope.auxiliary
        self.scope.temporary=[self.target.split('.')[-1]+'__dbt_tmp_'+start.replace('-','')
                              for start,_ in days(self.cases[0]['window'])]
        self.save('footprint.json',{'permanent':sorted(self.scope.permanent),'auxiliary':sorted(self.scope.auxiliary),
                  'session_only':self.scope.temporary,'max_extra_persistent':1,'reference_objects':0,'cleanup':False})
        self.save('operation-manifest.json',{'cells':self.cases,'owned':sorted(self.scope.permanent),
                  'auxiliary':sorted(self.scope.auxiliary),'temporary':self.scope.temporary,
                  'session_kind':'public-history-hydrated continuation' if args.native_prefix else 'native client campaign',
                  'managed_runs':6 if args.native_prefix else 11,
                  'successful_daily_batches':5 if args.native_prefix else 10,
                  'attempted_failed_batches':0 if args.native_prefix else 4,
                  'references':21 if args.native_prefix else 33,
                  'preflight_compiles':0 if args.native_prefix else 4 if args.failed_prefix else 5,
                  'utc_day':args.utc_day,'max_extra_persistent':1,'cleanup':False})
        self.save('tools.json',{name:{'path':str(Path(getattr(args,name)).resolve()),
                  'sha256':hashlib.sha256(Path(getattr(args,name)).read_bytes()).hexdigest()}
                  for name in ('dbt','proxy','probe')})

    def guard_clock(self):
        now=datetime.now(timezone.utc)
        if now.date()!=self.today or now.hour==23:raise Refused('moving-windows:UTC day changed or midnight too near')
        return now.isoformat()

    def init_schemas(self):
        if self.a.native_prefix:
            from capture_integrity import verify_capture
            prior=Path(self.a.native_prefix)
            summary=json.loads((prior/'summary.json').read_text())
            captures=list((prior/'golden').glob('*.jsonl'))
            previous=json.loads((prior/'operation-manifest.json').read_text())
            if (not self.a.rust_url or summary['success'] is not False
                    or summary['failure']!='moving-windows:whole-command-submit'
                    or set(previous['owned'])!=self.scope.permanent or len(captures)!=1):
                raise Refused('moving-windows:invalid-native-prefix')
            self.native_entries=verify_capture(captures[0])
            if len(self.native_entries)!=33:raise Refused('moving-windows:unexpected-native-prefix-length')
            self.before_inventory=self.inventory()
            if self.before_inventory!=json.loads((prior/'lease-inventory-after.json').read_text()):
                raise Refused('moving-windows:native-prefix-inventory-changed')
            self.save('lease-inventory-before.json',self.before_inventory)
            return
        if not self.a.failed_prefix:return super().init_schemas()
        from capture_integrity import verify_capture
        prior=Path(self.a.failed_prefix)
        summary=json.loads((prior/'summary.json').read_text())
        manifest=json.loads((prior/'operation-manifest.json').read_text())
        captures=list((prior/'golden').glob('*.jsonl'))
        if (summary['success'] is not False or set(manifest['owned'])!=self.scope.permanent or len(captures)!=1
                or 'Table does not support overwrite by expression' not in summary['failure']):
            raise Refused('moving-windows:invalid-failed-prefix')
        entries=verify_capture(captures[0])
        if len(entries)!=6 or any(r['method'] in ('ConfirmExecution','RecordExecutions','RegisterClone') for r in entries):
            raise Refused('moving-windows:failed-prefix-published')
        self.before_inventory=self.inventory()
        if self.before_inventory!=json.loads((prior/'lease-inventory-after.json').read_text()):raise Refused('moving-windows:prefix-inventory-changed')
        self.save('lease-inventory-before.json',self.before_inventory)
        self.save('failed-prefix.json',{'path':str(prior),'capture_sha256':hashlib.sha256(captures[0].read_bytes()).hexdigest()})
        self.failed_submits=[r for r in entries if r['method']=='SubmitEnrichedSQL']
        failure_sql((prior/'logs/005_first/query_log.sql').read_text(),self.target,self.source,self.cases[0]['window'])

    def model(self,c):
        self.write_model('consumer',"materialized='incremental',incremental_strategy='microbatch',unique_key='id',"
            "event_time='ts',begin='"+self.cases[0]['window'][0]+"',batch_size='day',lookback="+str(c['lookback'])+
            (",incremental_predicates="+repr(['id >= '+str(c['predicate'])]) if c['predicate'] is not None else '')+",file_format='delta',on_schema_change='ignore',"
            "state={'lag_tolerance':'0s','pre_clone':'never'}",
            "select * from {{ source('moving','inputs') }}"+(' where id >= '+str(c['filter']) if c['filter'] is not None else ''))

    def preflight(self):
        configurations=[self.cases[index] for index in (0,4,6,8)]
        if not self.a.failed_prefix:configurations.append(dict(self.cases[0],label='failed_predicate',predicate=0))
        for c in configurations:
            self.model(c);label='preflight_'+c['label']
            with reference_canary(self,label) as endpoint:
                previous=self.env['RUN_CACHE_API_URL'];self.env['RUN_CACHE_API_URL']=endpoint
                try:self.dbt(label,['compile','--select','consumer'],managed=False)
                finally:self.env['RUN_CACHE_API_URL']=previous
            folder=self.work/'targets'/f'{self.n:03d}_{label}'
            m=json.loads((folder/'manifest.json').read_text());check_manifest(self.scope,m)
            node=next(v for v in m['nodes'].values() if v['name']=='consumer');config=node['config']
            wanted={'materialized':'incremental','incremental_strategy':'microbatch','event_time':'ts',
                    'batch_size':'day','lookback':c['lookback'],'unique_key':'id','file_format':'delta',
                    'on_schema_change':'ignore','incremental_predicates':None if c['predicate'] is None else ['id >= '+str(c['predicate'])]}
            if (m['metadata']['dbt_version']!='2.0.11' or any(config.get(k)!=v for k,v in wanted.items())
                    or config.get('pre-hook') or config.get('post-hook') or config['state']['pre_clone']!='never'):
                raise Refused('moving-windows:preflight-config')
            compiled=list((folder/'compiled').rglob('consumer.sql'))
            expected='select * from '+self.source+(' where id >= '+str(c['filter']) if c['filter'] is not None else '')
            if len(compiled)!=1 or re.sub(r'\s+',' ',compiled[0].read_text().replace('`','')).strip().lower()!=expected:
                raise Refused('moving-windows:preflight-SQL')
            macros={k:v['macro_sql'] for k,v in m['macros'].items() if k in {
                    'macro.dbt_databricks.databricks__get_incremental_microbatch_sql',
                    'macro.dbt_databricks.get_replace_where_sql'}}
            if len(macros)!=2 or 'incremental_predicates.append' not in next(v for k,v in macros.items() if 'microbatch' in k):
                raise Refused('moving-windows:preflight-macros')
            self.save('native-macros/'+label+'.json',macros)

    def checkpoint(self,c):
        label=c['label'];self.model(c)
        args=['run','--select','consumer']
        if c['explicit']:args+=['--event-time-start',c['window'][0],'--event-time-end',c['window'][1]]
        before=self.guard_clock();calls=self.dbt(label,args);after=self.guard_clock()
        submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
        if len(submits)!=1 or len([r for r in calls if r['method']=='SubmitEnrichedSQLSpeculative'])>1:
            raise Refused('moving-windows:whole-command-submit')
        req=submits[0]['request'];extras=req['semantic_extras']
        public_sql='select * from '+self.source+(' where id >= '+str(c['filter']) if c['filter'] is not None else '')
        wanted={'unique_key':'"id"','file_format':'"delta"','event_time':'"ts"','on_schema_change':'"ignore"',
                'lookback':str(c['lookback']),
                '__microbatch_event_time_start':c['window'][0]+'T00:00:00+00:00',
                '__microbatch_event_time_end':c['window'][1]+'T00:00:00+00:00'}
        if c['predicate'] is not None:wanted['incremental_predicates']=json.dumps(['id >= '+str(c['predicate'])])
        tables={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in req['tables']}
        names={self.source,self.target} # failed native prefix has already created the target
        if (req['execution_type']!=6 or req['dialect']!='databricks' or extras!=wanted
                or not req.get('table_namespace') or req.get('clone_chain_depth_limit')!=0
                or req.get('ignore_external_modifications') or req.get('query_dependencies')
                or req.get('compare_unrendered_code') or req['freshness_tolerance_seconds']!=0
                or req['default_catalog']!='cdo_demo_db' or req['default_schema']!=self.scope.schema('prod')
                or re.sub(r'\s+',' ',req['sql'].replace('`','')).strip().lower()!=public_sql
                or req['target_table'].replace('`','').lower()!=self.target
                or len(req['tables'])!=len(names) or set(tables)!=names
                or any(type(v) is not int or v<=0 for v in tables.values())):
            raise Refused('moving-windows:public-request-scope-and-planned-bounds')
        if hasattr(self,'namespace') and req['table_namespace']!=self.namespace:raise Refused('moving-windows:namespace-changed')
        self.namespace=req['table_namespace']
        if hasattr(self,'last_tables'):
            if tables[self.target]!=self.confirmed_epoch:raise Refused('moving-windows:unexpected-target-epoch')
            prior=self.last_tables[self.source]
            if (tables[self.source]<=prior if label in ('late_zero','predicate') else tables[self.source]!=prior):
                raise Refused('moving-windows:source-epoch-did-not-follow-operations')
        if any(r['method'] in ('RecordExecutions','RegisterClone') or r['status']['code']!=0 for r in calls):
            raise Refused('moving-windows:unexpected-RPC')
        expected='skip_execution' if c['repeat'] else 'ready_to_execute';decisions(calls,expected)
        confirms=[r for r in calls if r['method']=='ConfirmExecution']
        if c['repeat']:
            response=submits[0]['response']['response'][expected]
            if (confirms or tables!=self.last_tables or extras!=self.last_extras or req['sql']!=self.last_sql
                    or response['execution_runtime_ms']!=self.last_runtime
                    or response['explained_decision']['is_stale'] is not False):
                raise Refused('moving-windows:own-unchanged-reuse')
        else:
            if len(confirms)!=1 or confirms[0]['response'].get('success') is not True:raise Refused('moving-windows:one-confirm')
            self.last_runtime=confirms[0]['request']['execution_runtime_ms']
            self.confirmed_epoch=confirms[0]['request']['last_modified_epoch']
            if any(type(v) is not int or v<=0 for v in (self.last_runtime,self.confirmed_epoch)):raise Refused('moving-windows:positive-confirm')
        self.last_tables=dict(tables,**{self.target:self.confirmed_epoch})
        self.last_extras=extras;self.last_sql=req['sql']
        self.explain(label,submits)
        self.save('clock-and-bounds/'+label+'.json',{'observed_UTC_before':before,'observed_UTC_after':after,
                  'explicit_cli_bounds':c['explicit'],'planned_window':c['window'],'actual_semantic_extras':extras})
        result=json.loads((self.work/'targets'/f'{self.n:03d}_{label}'/'run_results.json').read_text())['results']
        if (len(result)!=1 or result[0]['status']!=('reused' if c['repeat'] else 'success')
                or result[0]['relation_name'].replace('`','').lower()!=self.target
                or result[0]['unique_id']!='model.survey_'+self.a.run_id+'.consumer'
                or result[0]['failures'] is not None or result[0].get('batch_results') is not None):
            raise Refused('moving-windows:native-client-status')
        adapter=result[0]['adapter_response']
        if (adapter!={} if c['repeat'] else not isinstance(adapter,dict) or adapter.get('code')!='SUCCESS' or not adapter.get('query_id')):
            raise Refused('moving-windows:native-adapter-response')
        raw=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        writes=native_writes(raw)
        if c['repeat'] and writes:raise Refused('moving-windows:SKIP-writes')
        if not c['repeat']:
            for start,end in days(c['window']):
                exact=("insert into "+self.target+" by name replace where "+
                       ('id >= '+str(c['predicate'])+' and ' if c['predicate'] is not None else '')+
                       "cast(ts as timestamp) >= '"+start+" 00:00:00+00:00' and cast(ts as timestamp) < '"+
                       end+" 00:00:00+00:00' table "+self.target.split('.')[-1]+'__dbt_tmp_'+start.replace('-',''))
                if exact not in writes:raise Refused('moving-windows:native-predicate-and-window')
                temp=self.target.split('.')[-1]+'__dbt_tmp_'+start.replace('-','')
                projection=("select * from (select * from "+self.source+" where ts >= '"+start+
                            "t00:00:00+00:00' and ts < '"+end+"t00:00:00+00:00')"+
                            (' where id >= '+str(c['filter']) if c['filter'] is not None else ''))
                if 'create or replace temporary view '+temp+' as '+projection not in writes:
                    raise Refused('moving-windows:native-source-filter')
            if len(writes)!=2*len(days(c['window'])):
                raise Refused('moving-windows:native-write-count')
            for q in writes:
                if not (any(q.startswith('create or replace temporary view '+temp+' as ') for temp in self.scope.temporary)
                        or q.startswith('insert into '+self.target+' by name replace where ')):
                    raise Refused('moving-windows:native-write-outside-footprint')
        self.save('materialization/'+label+'.json',{'writes':writes,'daily_batches':0 if c['repeat'] else len(days(c['window']))})
        self.physical(c)

    def physical(self,c):
        label=c['label']
        for name,relation,rows in [('source',self.source,c['source']),('R',self.target,c['R'])]:
            observed=self.observe(label+'_'+name,relation)
            props=json.loads((self.work/'physical'/f'{label}_{name}_properties.json').read_text());assert_result(observed,props,rows)
        history=self.sql(label+'_target_history','DESCRIBE HISTORY '+self.target)
        def version(h):
            index=next(i for i,col in enumerate(h['columns']) if col['name']=='version')
            return max(int(row[index]) for row in h['rows'])
        if c['repeat'] and history!=self.last_history:raise Refused('moving-windows:SKIP-changed-history')
        if not c['repeat'] and hasattr(self,'last_history') and version(history)<=version(self.last_history):
            raise Refused('moving-windows:BUILD-did-not-advance-history')
        self.last_history=history;self.save('history/'+label+'_target.json',history)
        self.save('history/'+label+'_source.json',self.sql(label+'_source_history','DESCRIBE HISTORY '+self.source))
        sql='SELECT id,v,p,ts FROM '+self.source+" WHERE ts >= '"+c['window'][0]+"' AND ts < '"+c['window'][1]+"'"+(' AND id >= '+str(c['predicate']) if c['predicate'] is not None else '')
        assert_typed(self.sql(label+'_fresh_selected',sql),c['F'])
        for name,rows in [('source',c['source']),('R',c['R']),('F',c['F'])]:
            sql=literal_reference(rows);direct=self.sql(label+'_'+name+'_literal',sql);assert_typed(direct,rows)
            show_reference(self,label+'_'+name,sql,rows,direct,validator=assert_reference)
        after=self.inventory()
        for schema,rows in self.before_inventory.items():
            if any(r not in after[schema] for r in rows) or any(r['full_name'] not in self.scope.permanent for r in after[schema] if r not in rows):
                raise Refused('moving-windows:prior-inventory-changed')
        self.save('checks/'+label+'.json',{'source_R_F_complete_typed':True,'native_SQL_checked':True,
                  'zero_RPC_literal_references':3,'case':c})

    def baseline(self):
        self.guard_clock();self.setup_project()
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'moving','database':'cdo_demo_db',
                  'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':self.source.split('.')[-1],
                  'config':{'event_time':'ts'}}]}]})
        if self.a.native_prefix:
            self.hydrate_native_prefix()
            for c in self.cases[4:]:
                if c['label']=='predicate':self.sql('predicate_source','INSERT OVERWRITE TABLE '+self.source+' '+literal_reference(c['source']))
                self.checkpoint(c)
            return {'native_managed_commands':6,'read_only_references':21,'public_hydration_calls':7,
                    'original_native_prefix_retained_separately':True,'objects':2}
        self.preflight()
        if self.sql('timezone','SELECT current_timezone() AS session_timezone')['rows']!=[['Etc/UTC']]:raise Refused('moving-windows:timezone')
        if not self.a.failed_prefix:
            self.sql('create_source','CREATE TABLE '+self.source+' (id BIGINT,v STRING,p DATE,ts TIMESTAMP) USING DELTA')
            self.sql('initial_source','INSERT INTO '+self.source+' '+literal_reference(self.cases[0]['source']))
            self.failed_first()
        self.explain('partial_failure',self.failed_submits)
        first=self.cases[0]
        self.physical(dict(first,label='partial_failure',R=first['R'][:2]))
        for c in self.cases:
            if c['label'] in ('late_zero','predicate'):
                self.sql(c['label']+'_source','INSERT OVERWRITE TABLE '+self.source+' '+literal_reference(c['source']))
            self.checkpoint(c)
        return {'managed':11,'successful_daily_batches':10,'failed_attempted_batches':4,'references':33,'objects':2,'real_UTC_day':self.a.utc_day,
                'capability_blocker':'configured predicates accumulate contradictory bounds across multiple batches in pinned dbt2.0.11'}

    def hydrate_native_prefix(self):
        """Restore only genuine captured public writes, with real current metadata."""
        prior=Path(self.a.native_prefix);ids={};decisions_={};receipt=[]
        env=dict(self.env,PROBE_URL='http://'+self.env['RUN_CACHE_API_URL'])
        env.pop('PROBE_SUBMITTED_SHIFT_MS',None)
        for index,row in enumerate(self.native_entries):
            method=row['method']
            if method not in ('SubmitEnrichedSQL','ConfirmExecution'):continue
            body=json.loads(json.dumps(row['request']))
            if method=='SubmitEnrichedSQL':
                if (body['target_table'].replace('`','').lower()!=self.target
                        or body.get('execution_type')!=6
                        or {t['name'].replace('`','').lower() for t in body['tables']}-{self.source,self.target}
                        or any(k=='x-organization-id' and v!='local' for k,v in row.get('metadata',[]))):
                    raise Refused('moving-windows:hydration-outside-original-owned-scope')
            else:body['request_id']=ids[body['request_id']]
            result=subprocess.run([self.a.probe,'submit' if method=='SubmitEnrichedSQL' else 'confirm'],
                    input=json.dumps(body),env=env,text=True,capture_output=True,timeout=60)
            if result.returncode:raise Refused('moving-windows:public-hydration-failed')
            response=json.loads(result.stdout)
            if method=='SubmitEnrichedSQL':
                old=row['response']['response'];new=response['response']
                if old.keys()!=new.keys():raise Refused('moving-windows:hydration-decision-changed')
                kind=next(iter(old));old_,new_=old[kind],new[kind]
                if old_['explained_decision']!=new_['explained_decision']:raise Refused('moving-windows:hydration-reason-changed')
                decisions_[old_['execution_decision_id']]=new_['execution_decision_id']
                if kind=='ready_to_execute':ids[old_['request_id']]=new_['request_id']
                elif kind=='skip_execution' and old_['execution_runtime_ms']!=new_['execution_runtime_ms']:
                    raise Refused('moving-windows:hydration-runtime-changed')
            else:
                expected=dict(row['response'],request_id=body['request_id'])
                if response!=expected:raise Refused('moving-windows:hydration-confirm-changed')
                self.last_runtime=body['execution_runtime_ms'];self.confirmed_epoch=body['last_modified_epoch']
            receipt.append({'original_entry':index,'method':method,'actual_request':body,'actual_response':response})
        if len(receipt)!=7:raise Refused('moving-windows:hydration-write-count')
        self.save('public-history-hydration.json',{'basis':'genuine original public requests; only Confirm request IDs remapped; current System clock, no warehouse command',
                  'original_prefix':str(prior),'request_ids':ids,'decision_ids':decisions_,'calls':receipt})
        calls=json.loads((prior/'wire/010_late_zero_repeat.json').read_text())
        submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
        if len(submits)!=1 or any(r['method'] in ('ConfirmExecution','RecordExecutions','RegisterClone','SubmitEnrichedSQLSpeculative') for r in calls):
            raise Refused('moving-windows:saved-repeat-shape')
        req=submits[0]['request'];response=submits[0]['response']['response']['skip_execution']
        tables={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in req['tables']}
        result=json.loads((prior/'targets/010_late_zero_repeat/run_results.json').read_text())['results']
        if (response['execution_runtime_ms']!=self.last_runtime or response['explained_decision']['is_stale'] is not False
                or tables[self.target]!=self.confirmed_epoch or len(result)!=1 or result[0]['status']!='reused'
                or result[0]['adapter_response']!={} or result[0]['failures'] is not None
                or native_writes((prior/'logs/010_late_zero_repeat/query_log.sql').read_text())):
            raise Refused('moving-windows:saved-repeat-not-own-safe-SKIP')
        self.namespace=req['table_namespace'];self.last_tables=tables;self.last_extras=req['semantic_extras'];self.last_sql=req['sql']
        self.last_history=json.loads((prior/'history/late_zero_target.json').read_text())
        mapped=json.loads(json.dumps(submits));mapped[0]['response']['response']['skip_execution']['execution_decision_id']=decisions_[response['execution_decision_id']]
        self.explain('late_zero_repeat_completed_after_hydration',mapped)
        self.physical(self.cases[3])

    def failed_first(self):
        c=dict(self.cases[0],predicate=0);self.model(c)
        try:self.dbt('predicate_multibatch_failure',['run','--select','consumer','--event-time-start',c['window'][0],'--event-time-end',c['window'][1]])
        except Refused:
            log=(self.work/'dbt'/f'{self.n:03d}_predicate_multibatch_failure.log').read_text()
            if self.events[-1]['returncode']!=1 or 'Table does not support overwrite by expression' not in log:raise
            calls=json.loads((self.work/'wire'/f'{self.n:03d}_predicate_multibatch_failure.json').read_text())
            if any(r['method'] in ('ConfirmExecution','RecordExecutions','RegisterClone') for r in calls):raise Refused('moving-windows:failure-published')
            self.failed_submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
            if len(self.failed_submits)!=1:raise Refused('moving-windows:failed-submit-count')
            failure_sql((self.work/'logs'/f'{self.n:03d}_predicate_multibatch_failure'/'query_log.sql').read_text(),
                        self.target,self.source,c['window'])
        else:raise Refused('moving-windows:uncharacterized-multibatch-predicate-success')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease','schema','object-id','utc-day'):p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91');p.add_argument('--query-identity');p.add_argument('--rust-url')
    p.add_argument('--failed-prefix',help='same UUID original failed native attempt, retained whole')
    p.add_argument('--native-prefix',help='exact same graph native33 prefix; public-history hydration only, then remaining six managed commands')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+uuid.uuid4().hex[:6])
    return MovingWindows(p.parse_args()).run()

if __name__=='__main__':raise SystemExit(main())
