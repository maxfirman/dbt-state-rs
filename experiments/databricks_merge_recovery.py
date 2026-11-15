#!/usr/bin/env python3
"""Bounded native #145 schema failure/recovery and destination-predicate trace."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import uuid

from databricks_baseline import check_inventory
from databricks_freshness import decisions
from databricks_freshness_actual import ActualFreshness
from databricks_merge import Merge
from databricks_readonly_reference import assert_reference, reference_canary, show_reference, unique_object
from databricks_survey import Refused

C3 = [('id','BIGINT','LONG',0),('amount','INT','INT',1),('tag','STRING','STRING',2)]
C4 = C3 + [('note','STRING','STRING',3)]
B0 = [['1','10','one'],['2','20','retained']]
B1 = [['1','11','one-new','n1'],['3','30','three','n3']]
R1 = [B1[0],['2','20','retained',None],B1[1]]
B2 = [['3','31','three-new',None],['4','40','four','n4']]
R2 = [R1[0],R1[1],B2[0],B2[1]]
B3 = [['3','32','three-p','p3'],['5','50','five','p5']]
R3 = [R1[0],R1[1],B3[0],B2[1],B3[1]]
B4 = [['1','12','one-p','p1'],*B3]
R4 = [*R3,B4[0]]
CELLS = [('first',B0,B0,False),('first_repeat',B0,B0,False),
         ('schema_fail',B1,B0,True),('schema_retry',B1,B0,True),
         ('recover',B1,R1,False),('recover_repeat',B1,R1,False),
         ('delta',B2,R2,False),('delta_repeat',B2,R2,False),
         ('predicate',B3,R3,False),('predicate_repeat',B3,R3,False),
         ('predicate_effect',B4,R4,False),('predicate_effect_repeat',B4,R4,False)]
PREDICATE = 'DBT_INTERNAL_DEST.id >= 2'


def assert_bag(actual, expected):
    schema = C3 if len(expected[0]) == 3 else C4
    observed = [tuple(c.get(k) for k in ('name','type_text','type_name','position')) for c in actual['columns']]
    if observed != schema or Counter(map(tuple,actual['rows'])) != Counter(map(tuple,expected)):
        raise Refused('recovery:complete-typed-bag')


def literal(rows):
    schema = C3 if len(rows[0]) == 3 else C4
    def value(v, kind):
        return 'CAST(NULL AS '+kind+')' if v is None else (str(int(v)) if kind != 'STRING' else "'"+v.replace("'","''")+"'")
    return ('SELECT '+', '.join('cast('+n+' as '+t+') '+n for n,t,_,_ in schema)+
            ' FROM VALUES '+','.join('('+','.join(value(v,c[1]) for v,c in zip(row,schema))+')' for row in rows)+
            ' AS declared('+','.join(c[0] for c in schema)+')')


def reference(stdout, stderr, direct, expected, sql, query_log, compiled):
    # Reuse the established SELECT-only/query-log/schema guard for the first
    # three columns; independently validate the complete fourth column as well.
    objects = json.loads(stdout,object_pairs_hook=unique_object)
    names = ['id','amount','tag'] + (['note'] if len(expected[0]) == 4 else [])
    if (not isinstance(objects,list) or any(list(o) != names for o in objects)
            or Counter(tuple(o.values()) for o in objects) != Counter(map(tuple,expected))
            or any(o.get('note') is not None and type(o['note']) is not str for o in objects)):
        raise Refused('recovery:reference-complete-rows')
    assert_bag(direct,[[None if v is None else str(v) for v in r] for r in expected])
    narrowed = dict(direct,columns=direct['columns'][:3],rows=[r[:3] for r in direct['rows']])
    return assert_reference(json.dumps([{k:o[k] for k in names[:3]} for o in objects]),stderr,
                            narrowed,[r[:3] for r in expected],sql,query_log,compiled)


class Recovery(ActualFreshness):
    entrypoint = Path(__file__)
    audit_native = Merge.audit_native

    def __init__(self,args):
        super().__init__(args)
        owned = set()
        for evidence in self.lease['source_evidence']:
            owned.update(json.loads(Path(evidence['path']).read_text())['retained_schemas'])
        if (args.schema not in owned or not re.fullmatch(r'ds_\d{8}[a-f0-9]{6}_(prod|ref)',args.schema)
                or args.schema == self.scope.schema('ref') or uuid.UUID(args.object_id).hex != args.object_id):
            raise Refused('recovery:owned-schema-fresh-UUID')
        self.lease['roles']['prod'] = args.schema
        # Validate original lease first; the override is local to this runner.
        self.scope.roles['prod'] = args.schema
        self.scope.schemas = set(self.scope.roles.values())
        self.scope.relation_prefix = 'mr145_'+args.object_id+'_'
        target = f'cdo_demo_db.{args.schema}.{self.scope.relation_prefix}consumer'
        self.scope.permanent = {target,target.removesuffix('consumer')+'inputs'}
        self.scope.auxiliary = {target+s for s in ('__dbt_stg','__dbt_backup')}
        self.scope.allowed = self.scope.permanent | self.scope.auxiliary
        self.scope.temporary = self.scope.relation_prefix+'consumer__dbt_tmp'
        self.lease.setdefault('preexisting_objects',{}).setdefault(args.schema,[])
        import yaml
        profile = yaml.safe_load((self.profiles/'profiles.yml').read_text())
        profile['survey']['outputs']['prod']['schema'] = args.schema
        self.save('profiles/profiles.yml',profile)
        self.save('schema-lease.json',self.lease)
        self.save('operation-manifest.json',{'cells':CELLS,'predicate':PREDICATE,
                  'owned':sorted(self.scope.permanent),'auxiliary':sorted(self.scope.auxiliary),
                  'temporary':self.scope.temporary,'managed_runs':12,'references':24,
                  'keys':'unique nonNULL; no duplicate-key or NULL-key claim','cleanup':False})

    def inventory(self):
        from urllib.parse import quote
        inventory = {}
        for schema in sorted(self.scope.schemas):
            rows, token, seen = [],None,set()
            while True:
                endpoint = '/api/2.1/unity-catalog/tables?catalog_name=cdo_demo_db&schema_name='+schema
                if token: endpoint += '&page_token='+quote(token,safe='')
                response = self.api('get',endpoint)
                rows.extend({k:r[k] for k in ('name','full_name','table_type')} for r in response.get('tables',[]))
                token = response.get('next_page_token')
                if not token: break
                if token in seen: raise Refused('recovery:inventory-loop')
                seen.add(token)
            inventory[schema] = rows
        return inventory

    def init_schemas(self):
        inventory = self.inventory()
        if getattr(self.a,'continue_from',None):
            from capture_integrity import verify_capture
            prior = Path(self.a.continue_from)
            summary = json.loads((prior/'summary.json').read_text())
            plan = json.loads((prior/'operation-manifest.json').read_text())
            if (summary['success'] is not True or set(plan['owned']) != self.scope.permanent
                    or not (prior/'checks/predicate_repeat.json').exists()):
                raise Refused('recovery:continuation-prior-incomplete-or-other-graph')
            for capture in summary['capture']: verify_capture(prior/capture['path'])
            previous = json.loads((prior/'lease-inventory-after.json').read_text())
            if inventory != previous: raise Refused('recovery:continuation-inventory-changed')
            self.save('continuation-prior.json',{'path':str(prior.resolve()),'summary':summary})
        else:
            check_inventory(self.scope,self.lease,inventory)
        if len(inventory[self.scope.schema('prod')])+3 > 100: raise Refused('recovery:capacity')
        self.before_inventory = inventory
        self.save('lease-inventory-before.json',inventory)

    def model(self, note=False, policy='fail', predicate=False):
        self.note,self.policy,self.predicate = note,policy,predicate
        config = ("materialized='incremental',incremental_strategy='merge',unique_key='id',file_format='delta',"
                  "on_schema_change='"+policy+"',state={'lag_tolerance':'0s','pre_clone':'never'}")
        if predicate: config += ',incremental_predicates='+repr([PREDICATE])
        self.write_model('consumer',config,"select id, amount, tag"+(', note' if note else '')+" from {{ source('recovery','inputs') }}")

    def preflight(self):
        with reference_canary(self,'preflight') as endpoint:
            previous = self.env['RUN_CACHE_API_URL']
            self.env['RUN_CACHE_API_URL'] = endpoint
            try: self.dbt('preflight',['compile','--select','consumer'],managed=False)
            finally: self.env['RUN_CACHE_API_URL'] = previous
        node = next(iter(json.loads((self.work/'targets'/f'{self.n:03d}_preflight'/'manifest.json').read_text())['nodes'].values()))
        c = node['config']
        if (c['materialized']!='incremental' or c['incremental_strategy']!='merge'
                or c['unique_key']!='id' or c['on_schema_change']!='fail' or c['file_format']!='delta'
                or c.get('pre-hook') or c.get('post-hook')): raise Refused('recovery:preflight-config')
        compiled = list((self.work/'targets'/f'{self.n:03d}_preflight'/'compiled').rglob('consumer.sql'))
        if len(compiled)!=1 or re.sub(r'\s+',' ',compiled[0].read_text()).strip().lower() != ('select id, amount, tag from '+self.source).lower():
            # Native compiler may independently quote each fully qualified part.
            if len(compiled)!=1 or re.sub(r'\s+',' ',compiled[0].read_text().replace('`','')).strip().lower() != ('select id, amount, tag from '+self.source).lower():
                raise Refused('recovery:preflight-SQL')

    def checkpoint(self,label,source_rows,target_rows,failure):
        before = self.sql(label+'_before_history',f'DESCRIBE HISTORY {self.target} LIMIT 1') if failure else None
        try: calls = self.dbt(label,['run','--select','consumer'])
        except Refused:
            if not failure: raise
            result = self.events[-1]
            log = (self.work/'dbt'/f'{self.n:03d}_{label}.log').read_text()
            if (result['kind']!='dbt-result' or result['returncode'] != 1
                    or '[JinjaError (dbt1501)]' not in log
                    or 'The source and target schemas on this incremental model are out of sync!' not in log
                    or "Source columns not in target: [{'name': 'note', 'dtype': 'STRING'" not in log):
                raise Refused('recovery:unexpected-failure')
            calls = json.loads((self.work/'wire'/f'{self.n:03d}_{label}.json').read_text())
        else:
            if failure: raise Refused('recovery:schema-failure-missing')
        if any(c['status']['code'] != 0 for c in calls): raise Refused('recovery:non-OK-RPC')
        submits = [c for c in calls if c['method']=='SubmitEnrichedSQL']
        if len(submits)!=1: raise Refused('recovery:ordinary-submit')
        req = submits[0]['request']
        wanted = {self.source} if label=='first' else {self.source,self.target}
        if (req['execution_type']!=3 or not req.get('table_namespace') or {t['name'].replace('`','').lower() for t in req['tables']}!=wanted
                or len(req['tables']) != len(wanted) or any(t['last_modified_epoch'] is None for t in req['tables'])
                or req['semantic_extras'].get('on_schema_change')!=json.dumps(self.policy)
                or req['semantic_extras'].get('unique_key')!='"id"'
                or req.get('clone_chain_depth_limit')!=0 or req.get('ignore_external_modifications')
                or req.get('query_dependencies') or req.get('compare_unrendered_code')):
            raise Refused('recovery:submit-evidence')
        expected = 'skip_execution' if label.endswith('repeat') else 'ready_to_execute'
        if any(c['method'] in ('RecordExecutions','RegisterClone') for c in calls): raise Refused('recovery:unexpected-record-or-copy')
        if expected == 'skip_execution' and any(c['method']=='ConfirmExecution' for c in calls): raise Refused('recovery:skip-confirmed')
        if failure:
            if (expected not in submits[0]['response']['response'] or any(c['method'] in ('ConfirmExecution','RecordExecutions','RegisterClone') for c in calls)):
                raise Refused('recovery:failure-published')
            if self.sql(label+'_after_history',f'DESCRIBE HISTORY {self.target} LIMIT 1') != before:
                raise Refused('recovery:failure-changed-history')
        else: decisions(calls,expected)
        self.audit_native(label,label!='first')
        native = (self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        if not failure and expected == 'ready_to_execute' and label != 'first':
            if not re.search(r'(?is)merge\s+into\b.*?<=>',native): raise Refused('recovery:native-MERGE')
            if self.predicate and PREDICATE.lower() not in native.lower(): raise Refused('recovery:native-predicate')
        if label == 'recover' and not re.search(r'(?is)alter\s+table\b.*?add\s+(?:columns?\s*)?\(',native):
            raise Refused('recovery:native-ADD')
        self.explain(label,submits)
        for name,rows in [('source',source_rows),('target',target_rows)]:
            relation = self.source if name=='source' else self.target
            actual = self.observe(label+'_'+name,relation)
            assert_bag(actual,rows)
            props = json.loads((self.work/'physical'/f'{label}_{name}_properties.json').read_text())
            properties = {r[0]:r[1] for r in props['rows']}
            if properties.get('Type')!='MANAGED' or properties.get('Provider','').lower()!='delta': raise Refused('recovery:managed-Delta')
            sql = literal(rows)
            direct = self.sql(label+'_'+name+'_reference',sql)
            assert_bag(direct,rows)
            expected_rows = [[int(r[0]),None if r[1] is None else int(r[1]),*r[2:]] for r in rows]
            show_reference(self,label+'_'+name,sql,expected_rows,direct,reference)
        after = self.inventory()
        for schema,rows in self.before_inventory.items():
            if any(r not in after[schema] for r in rows) or any(r['full_name'] not in self.scope.permanent for r in after[schema] if r not in rows):
                raise Refused('recovery:inventory-changed')
        self.save('checks/'+label+'.json',{'source':source_rows,'cumulative_target':target_rows,
                  'failed_publication_absent':failure,'complete_typed_rows_and_disabled_references':True})

    def baseline(self):
        self.setup_project()
        self.source,self.target = [self.scope.relation('prod',n) for n in ('inputs','consumer')]
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'recovery','database':'cdo_demo_db',
                  'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':self.source.split('.')[-1]}]}]})
        if getattr(self.a,'continue_from',None):
            assert_bag(self.observe('continuation_source',self.source),B3)
            assert_bag(self.observe('continuation_target',self.target),R3)
            self.model(True,'append_new_columns',True)
            self.sql('predicate_effect_source',f"INSERT OVERWRITE TABLE {self.source} VALUES (1,12,'one-p','p1'),(3,32,'three-p','p3'),(5,50,'five','p5')")
            for label,source_rows,target_rows,failure in CELLS[-2:]:
                self.checkpoint(label,source_rows,target_rows,failure)
            return {'continuation_managed':2,'objects':2}
        self.model()
        self.preflight()
        self.sql('create_source',f'CREATE TABLE {self.source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('initial_source',f"INSERT INTO {self.source} VALUES (1,10,'one'),(2,20,'retained')")
        for label,source_rows,target_rows,failure in CELLS:
            if label=='schema_fail':
                self.model(True)
                self.sql('add_note',f'ALTER TABLE {self.source} ADD COLUMNS (note STRING)')
                self.sql('replace_source',f"INSERT OVERWRITE TABLE {self.source} VALUES (1,11,'one-new','n1'),(3,30,'three','n3')")
            if label=='recover': self.model(True,'append_new_columns')
            if label=='delta': self.sql('delta_source',f"INSERT OVERWRITE TABLE {self.source} VALUES (3,31,'three-new',CAST(NULL AS STRING)),(4,40,'four','n4')")
            if label=='predicate':
                self.model(True,'append_new_columns',True)
                self.sql('predicate_source',f"INSERT OVERWRITE TABLE {self.source} VALUES (3,32,'three-p','p3'),(5,50,'five','p5')")
            if label=='predicate_effect':
                self.sql('predicate_effect_source',f"INSERT OVERWRITE TABLE {self.source} VALUES (1,12,'one-p','p1'),(3,32,'three-p','p3'),(5,50,'five','p5')")
            self.checkpoint(label,source_rows,target_rows,failure)
        return {'managed':12,'objects':2,'schema_failures':2}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease','schema','object-id'): p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    p.add_argument('--profile',default='databricks');p.add_argument('--warehouse',default='47fd3fa312624d91')
    p.add_argument('--query-identity');p.add_argument('--rust-url')
    p.add_argument('--continue-from',help='complete same-owned-graph initial ten-cell campaign; only final predicate effect pair')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+uuid.uuid4().hex[:6])
    return Recovery(p.parse_args()).run()


if __name__=='__main__': raise SystemExit(main())
