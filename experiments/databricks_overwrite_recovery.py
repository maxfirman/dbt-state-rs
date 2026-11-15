#!/usr/bin/env python3
"""Bounded #147 composite partitions and schema failure/recovery, hosted first."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import uuid

from databricks_freshness import decisions
from databricks_merge_recovery import Recovery
from databricks_readonly_reference import reference_canary, show_reference, unique_object
from databricks_survey import Refused

C4=[('id','BIGINT','LONG',0),('amount','INT','INT',1),('p','STRING','STRING',2),('q','STRING','STRING',3)]
C5=C4+[('note','STRING','STRING',4)]
# Worked operation ledger. Equal first keys with distinct second keys must not
# share replacement scope. NULL is a genuine component; duplicate rows survive.
A=[['1','10','A','x'],['1','10','A','x'],['2','20','A','y'],['3','30',None,'x'],['4','40','A',None],['5','50',None,None]]
B=[['1','11','A','x','n1'],['1','11','A','x','n1'],['6','60',None,'x','n6'],['7','70','A',None,'n7']]
RB=[['2','20','A','y',None],['5','50',None,None,None],*B]
C=[['8','80','A','y','n8'],['9','90',None,None,'n9']]
RC=[*B,*C]
CELLS=[('first',A,A,False),('first_repeat',A,A,False),
       ('schema_fail',B,A,True),('schema_retry',B,A,True),
       ('recover',B,RB,False),('recover_repeat',B,RB,False),
       ('delta',C,RC,False),('delta_repeat',C,RC,False),
       ('empty',[],RC,False),('empty_repeat',[],RC,False)]


def assert_bag(actual,expected,schema=None):
    schema=schema or (C4 if expected and len(expected[0])==4 else C5)
    observed=[tuple(c.get(k) for k in ('name','type_text','type_name','position')) for c in actual['columns']]
    if (observed!=schema or any(type(c.get('position')) is not int for c in actual['columns'])
            or Counter(map(tuple,actual['rows']))!=Counter(map(tuple,expected))):
        raise Refused('overwrite-recovery:complete-typed-bag')


def literal(rows,schema=None):
    schema=schema or (C4 if rows and len(rows[0])==4 else C5)
    def value(v,kind):
        return 'CAST(NULL AS '+kind+')' if v is None else (str(int(v)) if kind!='STRING' else "'"+v.replace("'","''")+"'")
    projection='SELECT '+', '.join('cast('+n+' as '+t+') '+n for n,t,_,_ in schema)
    if not rows:
        return projection+' FROM (SELECT '+', '.join('cast(null as '+t+') '+n for n,t,_,_ in schema)+') declared WHERE false'
    return projection+' FROM VALUES '+','.join('('+','.join(value(v,c[1]) for v,c in zip(r,schema))+')' for r in rows)+' AS declared('+','.join(c[0] for c in schema)+')'


def reference(stdout,stderr,direct,expected,sql,query_log,compiled):
    if stderr.strip():raise Refused('overwrite-recovery:reference-stderr')
    objects=json.loads(stdout,object_pairs_hook=unique_object)
    names=[c['name'] for c in direct['columns']]
    if (not isinstance(objects,list) or any(not isinstance(o,dict) or list(o)!=names for o in objects)
            or Counter(tuple(o.values()) for o in objects)!=Counter(map(tuple,expected))
            or any(type(o['id']) is not int or type(o['amount']) is not int
                   or any(v is not None and type(v) is not str for k,v in o.items() if k not in ('id','amount')) for o in objects)):
        raise Refused('overwrite-recovery:reference-rows-types')
    assert_bag(direct,[[None if v is None else str(v) for v in r] for r in expected],C4 if len(names)==4 else C5)
    query='\n'.join(l for l in query_log.splitlines() if not l.startswith('--'))
    normalize=lambda s:re.sub(r'\s+',' ',s).strip()
    if (not re.match(r'(?i)^select\s',sql) or ';' in sql or normalize(compiled)!=normalize(sql)
            or query.count(';')!=1 or not query.rstrip().endswith(';') or normalize(query.rstrip()[:-1])!=normalize(sql)):
        raise Refused('overwrite-recovery:reference-select-only')
    return {'complete_typed_multiset':True,'exact_select_only':True}


class OverwriteRecovery(Recovery):
    entrypoint=Path(__file__)

    def __init__(self,args):
        super().__init__(args)
        self.scope.relation_prefix='or147_'+args.object_id+'_'
        self.target=f'cdo_demo_db.{args.schema}.{self.scope.relation_prefix}consumer'
        self.source=self.target.removesuffix('consumer')+'inputs'
        self.scope.permanent={self.target,self.source}
        self.scope.auxiliary={self.target+s for s in ('__dbt_stg','__dbt_backup')}
        self.scope.allowed=self.scope.permanent|self.scope.auxiliary
        self.scope.temporary=self.scope.relation_prefix+'consumer__dbt_tmp'
        self.save('footprint.json',{'permanent':sorted(self.scope.permanent),'auxiliary':sorted(self.scope.auxiliary),
                  'session_only':self.scope.temporary,'max_extra_persistent':1,'reference_objects':0,'cleanup':False})
        self.save('operation-manifest.json',{'cells':CELLS,'owned':sorted(self.scope.permanent),
                  'auxiliary':sorted(self.scope.auxiliary),'temporary':self.scope.temporary,
                  'managed_runs':10,'references':20,'preflight_compiles':3,'reference_objects':0,
                  'max_extra_persistent':1,'partition_by':['p','q'],'source_mutations':5,'cleanup':False})

    def model(self,note=False,policy='fail'):
        self.note,self.policy=note,policy
        config=("materialized='incremental',incremental_strategy='insert_overwrite',partition_by=['p','q'],unique_key='id',"
                "file_format='delta',on_schema_change='"+policy+"',state={'lag_tolerance':'0s','pre_clone':'never'}")
        self.write_model('consumer',config,"select id, amount, p, q"+(', note' if note else '')+" from {{ source('replacement','inputs') }}")

    def preflight(self):
        for i,(note,policy) in enumerate([(False,'fail'),(True,'fail'),(True,'append_new_columns')]):
            self.model(note,policy);label='preflight_'+str(i)
            with reference_canary(self,label) as endpoint:
                previous=self.env['RUN_CACHE_API_URL'];self.env['RUN_CACHE_API_URL']=endpoint
                try:self.dbt(label,['compile','--select','consumer'],managed=False)
                finally:self.env['RUN_CACHE_API_URL']=previous
            folder=self.work/'targets'/f'{self.n:03d}_{label}'
            manifest=json.loads((folder/'manifest.json').read_text())
            node=next(v for v in manifest['nodes'].values() if v['name']=='consumer');c=node['config']
            if (manifest['metadata']['dbt_version']!='2.0.11' or c['materialized']!='incremental'
                    or c['incremental_strategy']!='insert_overwrite' or c['partition_by']!=['p','q']
                    or c['unique_key']!='id' or c['file_format']!='delta' or c['on_schema_change']!=policy
                    or c.get('pre-hook') or c.get('post-hook') or c['state']['pre_clone']!='never'):
                raise Refused('overwrite-recovery:preflight-config')
            compiled=list((folder/'compiled').rglob('consumer.sql'))
            expected='select id, amount, p, q'+(', note' if note else '')+' from '+self.source
            if len(compiled)!=1 or re.sub(r'\s+',' ',compiled[0].read_text().replace('`','')).strip().lower()!=expected:
                raise Refused('overwrite-recovery:preflight-SQL')
            macros={k:v['macro_sql'] for k,v in manifest['macros'].items() if k in {
                'macro.dbt_databricks.get_insert_overwrite_sql','macro.dbt_databricks.get_insert_replace_on_sql'}}
            if len(macros)!=2 or "replace_conditions | join(' AND ')" not in macros['macro.dbt_databricks.get_insert_replace_on_sql']:
                raise Refused('overwrite-recovery:uncharacterized-native-macros')
            self.save('native-macros/'+label+'.json',macros)

    def checkpoint(self,label,source_rows,target_rows,failure):
        before=self.sql(label+'_before_history','DESCRIBE HISTORY '+self.target) if failure else None
        try:calls=self.dbt(label,['run','--select','consumer'])
        except Refused:
            if not failure:raise
            result=self.events[-1];log=(self.work/'dbt'/f'{self.n:03d}_{label}.log').read_text()
            if (result['kind']!='dbt-result' or result['returncode']!=1
                    or '[JinjaError (dbt1501)]' not in log
                    or 'The source and target schemas on this incremental model are out of sync!' not in log
                    or "Source columns not in target: [{'name': 'note', 'dtype': 'STRING'" not in log):
                raise Refused('overwrite-recovery:unexpected-failure')
            calls=json.loads((self.work/'wire'/f'{self.n:03d}_{label}.json').read_text())
        else:
            if failure:raise Refused('overwrite-recovery:missing-schema-failure')
        if any(c['status']['code']!=0 for c in calls):raise Refused('overwrite-recovery:RPC-status')
        submits=[c for c in calls if c['method']=='SubmitEnrichedSQL']
        if len(submits)!=1:raise Refused('overwrite-recovery:one-submit')
        req=submits[0]['request'];tables=req['tables']
        wanted={self.source} if label=='first' else {self.source,self.target}
        if (req['execution_type']!=4 or req['dialect']!='databricks' or not req.get('table_namespace')
                or req['target_table'].replace('`','').lower()!=self.target
                or len(tables)!=len(wanted) or {t['name'].replace('`','').lower() for t in tables}!=wanted
                or any(type(t.get('last_modified_epoch')) is not int or t['last_modified_epoch']<=0 for t in tables)
                or req['semantic_extras'].get('on_schema_change')!=json.dumps(self.policy)
                or req['semantic_extras'].get('partition_by')!='["p","q"]'
                or req['semantic_extras'].get('unique_key')!='"id"'
                or req.get('clone_chain_depth_limit')!=0 or req.get('ignore_external_modifications')
                or req.get('query_dependencies') or req.get('compare_unrendered_code')):
            raise Refused('overwrite-recovery:submit-scope-settings')
        if hasattr(self,'namespace') and req['table_namespace']!=self.namespace:raise Refused('overwrite-recovery:namespace-changed')
        self.namespace=req['table_namespace'];repeat=label.endswith('repeat')
        expected='skip_execution' if repeat else 'ready_to_execute'
        if any(c['method'] in ('RecordExecutions','RegisterClone') for c in calls):raise Refused('overwrite-recovery:record-or-copy')
        confirms=[c for c in calls if c['method']=='ConfirmExecution']
        if failure:
            if expected not in submits[0]['response']['response'] or confirms:raise Refused('overwrite-recovery:failure-published')
        else:
            decisions(calls,expected)
            if repeat:
                if confirms or req['sql']!=self.last_sql or submits[0]['response']['response'][expected]['execution_runtime_ms']!=self.last_runtime:
                    raise Refused('overwrite-recovery:reuse-not-own-unchanged-result')
            else:
                if len(confirms)!=1 or not confirms[0].get('response',{}).get('success'):raise Refused('overwrite-recovery:one-confirm')
                self.last_runtime=confirms[0]['request']['execution_runtime_ms'];self.last_sql=req['sql']
        self.audit_native(label,label!='first')
        native=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        if not failure and not repeat and label!='first':
            normalized=re.sub(r'\s+',' ',native.replace('`','')).lower()
            exact='replace on (t.p <=> s.p and t.q <=> s.q)'
            if exact not in normalized or not re.search(r'insert\s+into\s+table\s+'+re.escape(self.target)+r'\s+as t',normalized):
                raise Refused('overwrite-recovery:native-composite-REPLACE-ON')
        if label=='recover' and not re.search(r'alter\s+table\b.*?add\s+columns?',native,re.I|re.S):raise Refused('overwrite-recovery:native-ADD')
        self.explain(label,submits)
        history=self.sql(label+'_history','DESCRIBE HISTORY '+self.target)
        if failure and history!=before:raise Refused('overwrite-recovery:failure-changed-history')
        if repeat and history!=self.last_history:raise Refused('overwrite-recovery:skip-changed-history')
        self.last_history=history
        self.save('epochs/'+label+'.json',{'tables':tables,'semantic_extras':req['semantic_extras'],'response':submits[0]['response']})
        for name,rows in [('source',source_rows),('target',target_rows)]:
            relation=self.source if name=='source' else self.target
            schema=C5 if (self.note if name=='source' else len(target_rows[0])==5) else C4
            actual=self.observe(label+'_'+name,relation);assert_bag(actual,rows,schema)
            props=json.loads((self.work/'physical'/f'{label}_{name}_properties.json').read_text())
            properties={r[0]:r[1] for r in props['rows']}
            if properties.get('Type')!='MANAGED' or properties.get('Provider','').lower()!='delta':raise Refused('overwrite-recovery:managed-Delta')
            if name=='target':
                rs=props['rows'];start=next(i for i,r in enumerate(rs) if r[0]=='# Partition Information')+2
                partition=[]
                for row in rs[start:]:
                    if row[0]=='':break
                    partition.append(row[:2])
                if partition!=[['p','string'],['q','string']]:raise Refused('overwrite-recovery:physical-partition-keys')
            sql=literal(rows,schema);direct=self.sql(label+'_'+name+'_reference',sql);assert_bag(direct,rows,schema)
            typed=[[int(r[0]),int(r[1]),*r[2:]] for r in rows]
            show_reference(self,label+'_'+name,sql,typed,direct,reference)
        after=self.inventory()
        for schema,rows in self.before_inventory.items():
            if any(r not in after[schema] for r in rows) or any(r['full_name'] not in self.scope.permanent for r in after[schema] if r not in rows):raise Refused('overwrite-recovery:inventory-changed')
        self.save('checks/'+label+'.json',{'source':source_rows,'cumulative_target':target_rows,'schema_failure':failure,
                  'complete_typed_disabled_references':True,'composite_partition_metadata':True})

    def baseline(self):
        self.setup_project()
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'replacement','database':'cdo_demo_db',
                  'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':self.source.split('.')[-1]}]}]})
        self.preflight();self.model()
        self.sql('create_source',f'CREATE TABLE {self.source} (id BIGINT,amount INT,p STRING,q STRING) USING DELTA')
        self.sql('initial_source','INSERT INTO '+self.source+' '+literal(A))
        for label,source,target,failure in CELLS:
            if label=='schema_fail':
                self.model(True)
                self.sql('add_note','ALTER TABLE '+self.source+' ADD COLUMNS (note STRING)')
                self.sql('schema_source','INSERT OVERWRITE TABLE '+self.source+' '+literal(B))
            if label=='recover':self.model(True,'append_new_columns')
            if label=='delta':self.sql('delta_source','INSERT OVERWRITE TABLE '+self.source+' '+literal(C))
            if label=='empty':self.sql('empty_source','DELETE FROM '+self.source)
            self.checkpoint(label,source,target,failure)
        return {'managed':10,'objects':2,'schema_failures':2,'partition_keys':['p','q']}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease','schema','object-id'):p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91');p.add_argument('--query-identity');p.add_argument('--rust-url')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+uuid.uuid4().hex[:6])
    return OverwriteRecovery(p.parse_args()).run()

if __name__=='__main__':raise SystemExit(main())
