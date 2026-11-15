#!/usr/bin/env python3
"""Bounded #148 native predicate constraint and schema failure/recovery."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import uuid

from databricks_freshness import decisions
from databricks_merge_recovery import Recovery
from databricks_readonly_reference import reference_canary, show_reference
from databricks_survey import Refused

from databricks_overwrite_recovery import C4, C5, assert_bag, literal, reference

P1="p = 'A' OR p IS NULL"
P2="p IN ('A','B') OR p IS NULL"
# Independent worked bags: outside B survives schema recovery, then the widened
# predicate replaces it. Duplicate rows and genuine NULL values remain visible.
A=[['1','10','A','x'],['1','10','A','x'],['2','20','B','outside'],['3','30',None,'x'],['4','40','A',None]]
B=[['1','11','A','x','n1'],['1','11','A','x','n1'],['6','60',None,None,'n6']]
RB=[['2','20','B','outside',None],*B]
C=[['7','70','B','new','n7'],['8','80','A',None,None]]
CELLS=[('first',A,A,None),('first_repeat',A,A,None),
       ('schema_fail',B,A,'schema'),('schema_retry',B,A,'schema'),
       ('recover',B,RB,None),('recover_repeat',B,RB,None),
       ('constraint_fail',C,RB,'constraint'),('constraint_retry',C,RB,'constraint'),
       ('predicate_recover',C,C,None),('predicate_recover_repeat',C,C,None)]


def failure_matches(kind,log):
    if kind=='schema':
        return ('[JinjaError (dbt1501)]' in log
                and 'The source and target schemas on this incremental model are out of sync!' in log
                and "Source columns not in target: [{'name': 'note', 'dtype': 'STRING'" in log)
    return (kind=='constraint'
            and '[DELTA_REPLACE_WHERE_MISMATCH.INVARIANT_VIOLATION]' in log
            and '[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES]' in log
            and "constraint '((p = 'A') OR (p IS NULL))'" in log
            and '- p : B.' in log)


class PredicateRecovery(Recovery):
    entrypoint=Path(__file__)

    def __init__(self,args):
        super().__init__(args)
        self.scope.relation_prefix='pr148_'+args.object_id+'_'
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
                  'managed_runs':10,'references':20,'preflight_compiles':4,'reference_objects':0,
                  'max_extra_persistent':1,'predicates':[P1,P2],'source_mutations':4,'cleanup':False})

    def model(self,note=False,policy='fail',predicate=P1):
        self.note,self.policy,self.predicate=note,policy,predicate
        config=("materialized='incremental',incremental_strategy='replace_where',unique_key='id',"
                "file_format='delta',on_schema_change='"+policy+"',state={'lag_tolerance':'0s','pre_clone':'never'},incremental_predicates="+repr([predicate]))
        self.write_model('consumer',config,"select id, amount, p, q"+(', note' if note else '')+" from {{ source('replacement','inputs') }}")

    def preflight(self):
        for i,(note,policy,predicate) in enumerate([(False,'fail',P1),(True,'fail',P1),(True,'append_new_columns',P1),(True,'append_new_columns',P2)]):
            self.model(note,policy,predicate);label='preflight_'+str(i)
            with reference_canary(self,label) as endpoint:
                previous=self.env['RUN_CACHE_API_URL'];self.env['RUN_CACHE_API_URL']=endpoint
                try:self.dbt(label,['compile','--select','consumer'],managed=False)
                finally:self.env['RUN_CACHE_API_URL']=previous
            folder=self.work/'targets'/f'{self.n:03d}_{label}'
            manifest=json.loads((folder/'manifest.json').read_text())
            node=next(v for v in manifest['nodes'].values() if v['name']=='consumer');c=node['config']
            if (manifest['metadata']['dbt_version']!='2.0.11' or c['materialized']!='incremental'
                    or c['incremental_strategy']!='replace_where' or c['incremental_predicates']!=[predicate]
                    or c['unique_key']!='id' or c['file_format']!='delta' or c['on_schema_change']!=policy
                    or c.get('pre-hook') or c.get('post-hook') or c['state']['pre_clone']!='never'):
                raise Refused('predicate-recovery:preflight-config')
            compiled=list((folder/'compiled').rglob('consumer.sql'))
            expected='select id, amount, p, q'+(', note' if note else '')+' from '+self.source
            if len(compiled)!=1 or re.sub(r'\s+',' ',compiled[0].read_text().replace('`','')).strip().lower()!=expected:
                raise Refused('predicate-recovery:preflight-SQL')
            macro=manifest['macros']['macro.dbt_databricks.get_replace_where_sql']['macro_sql']
            if 'replace where' not in macro.lower():raise Refused('predicate-recovery:uncharacterized-native-macro')
            self.save('native-macros/'+label+'.json',macro)

    def checkpoint(self,label,source_rows,target_rows,failure):
        before=self.sql(label+'_before_history','DESCRIBE HISTORY '+self.target) if failure else None
        try:calls=self.dbt(label,['run','--select','consumer'])
        except Refused:
            if not failure:raise
            result=self.events[-1];log=(self.work/'dbt'/f'{self.n:03d}_{label}.log').read_text()
            if (result['kind']!='dbt-result' or result['returncode']!=1
                    or not failure_matches(failure,log)):
                raise Refused('predicate-recovery:unexpected-failure')
            calls=json.loads((self.work/'wire'/f'{self.n:03d}_{label}.json').read_text())
        else:
            if failure:raise Refused('predicate-recovery:missing-schema-failure')
        if any(c['status']['code']!=0 for c in calls):raise Refused('predicate-recovery:RPC-status')
        submits=[c for c in calls if c['method']=='SubmitEnrichedSQL']
        if len(submits)!=1:raise Refused('predicate-recovery:one-submit')
        req=submits[0]['request'];tables=req['tables']
        wanted={self.source} if label=='first' else {self.source,self.target}
        if (req['execution_type']!=11 or req['dialect']!='databricks' or not req.get('table_namespace')
                or req['target_table'].replace('`','').lower()!=self.target
                or len(tables)!=len(wanted) or {t['name'].replace('`','').lower() for t in tables}!=wanted
                or any(type(t.get('last_modified_epoch')) is not int or t['last_modified_epoch']<=0 for t in tables)
                or req['semantic_extras'].get('on_schema_change')!=json.dumps(self.policy)
                or req['semantic_extras'].get('incremental_predicates')!=json.dumps([self.predicate])
                or req['semantic_extras'].get('unique_key')!='"id"'
                or req.get('clone_chain_depth_limit')!=0 or req.get('ignore_external_modifications')
                or req.get('query_dependencies') or req.get('compare_unrendered_code')):
            raise Refused('predicate-recovery:submit-scope-settings')
        if hasattr(self,'namespace') and req['table_namespace']!=self.namespace:raise Refused('predicate-recovery:namespace-changed')
        self.namespace=req['table_namespace'];repeat=label.endswith('repeat')
        expected='skip_execution' if repeat else 'ready_to_execute'
        if any(c['method'] in ('RecordExecutions','RegisterClone') for c in calls):raise Refused('predicate-recovery:record-or-copy')
        confirms=[c for c in calls if c['method']=='ConfirmExecution']
        if failure:
            if expected not in submits[0]['response']['response'] or confirms:raise Refused('predicate-recovery:failure-published')
        else:
            decisions(calls,expected)
            if repeat:
                if confirms or req['sql']!=self.last_sql or submits[0]['response']['response'][expected]['execution_runtime_ms']!=self.last_runtime:
                    raise Refused('predicate-recovery:reuse-not-own-unchanged-result')
            else:
                if len(confirms)!=1 or not confirms[0].get('response',{}).get('success'):raise Refused('predicate-recovery:one-confirm')
                self.last_runtime=confirms[0]['request']['execution_runtime_ms'];self.last_sql=req['sql']
        result=json.loads((self.work/'targets'/f'{self.n:03d}_{label}'/'run_results.json').read_text())['results']
        if (len(result)!=1 or result[0]['status']!=('error' if failure else 'reused' if repeat else 'success')
                or result[0]['relation_name'].replace('`','').lower()!=self.target):
            raise Refused('predicate-recovery:client-status')
        self.audit_native(label,label!='first')
        native=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        if not failure and not repeat and label!='first':
            normalized=re.sub(r'\s+',' ',native.replace('`','')).lower()
            exact='insert into '+self.target+' by name replace where '+self.predicate.lower()+' table '+self.scope.temporary
            if exact not in normalized:raise Refused('predicate-recovery:native-REPLACE-WHERE')
        if label=='recover' and not re.search(r'alter\s+table\b.*?add\s+columns?',native,re.I|re.S):raise Refused('predicate-recovery:native-ADD')
        self.explain(label,submits)
        history=self.sql(label+'_history','DESCRIBE HISTORY '+self.target)
        if failure and history!=before:raise Refused('predicate-recovery:failure-changed-history')
        if repeat and history!=self.last_history:raise Refused('predicate-recovery:skip-changed-history')
        self.last_history=history
        self.save('history/'+label+'_target.json',history)
        self.save('history/'+label+'_source.json',self.sql(label+'_source_history','DESCRIBE HISTORY '+self.source))
        self.save('epochs/'+label+'.json',{'tables':tables,'semantic_extras':req['semantic_extras'],'response':submits[0]['response']})
        for name,rows in [('source',source_rows),('target',target_rows)]:
            relation=self.source if name=='source' else self.target
            schema=C5 if (self.note if name=='source' else len(target_rows[0])==5) else C4
            actual=self.observe(label+'_'+name,relation);assert_bag(actual,rows,schema)
            props=json.loads((self.work/'physical'/f'{label}_{name}_properties.json').read_text())
            properties={r[0]:r[1] for r in props['rows']}
            if properties.get('Type')!='MANAGED' or properties.get('Provider','').lower()!='delta':raise Refused('predicate-recovery:managed-Delta')
            sql=literal(rows,schema);direct=self.sql(label+'_'+name+'_reference',sql);assert_bag(direct,rows,schema)
            typed=[[int(r[0]),int(r[1]),*r[2:]] for r in rows]
            show_reference(self,label+'_'+name,sql,typed,direct,reference)
        after=self.inventory()
        for schema,rows in self.before_inventory.items():
            if any(r not in after[schema] for r in rows) or any(r['full_name'] not in self.scope.permanent for r in after[schema] if r not in rows):raise Refused('predicate-recovery:inventory-changed')
        self.save('checks/'+label+'.json',{'source':source_rows,'cumulative_target':target_rows,'native_failure':failure,
                  'complete_typed_disabled_references':True,'predicate':self.predicate})

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
            if label=='constraint_fail':self.sql('constraint_source','INSERT OVERWRITE TABLE '+self.source+' '+literal(C))
            if label=='predicate_recover':self.model(True,'append_new_columns',P2)
            self.checkpoint(label,source,target,failure)
        return {'managed':10,'objects':2,'schema_failures':2,'constraint_failures':2}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease','schema','object-id'):p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91');p.add_argument('--query-identity');p.add_argument('--rust-url')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+uuid.uuid4().hex[:6])
    return PredicateRecovery(p.parse_args()).run()

if __name__=='__main__':raise SystemExit(main())
