#!/usr/bin/env python3
"""One bounded APPEND/settings/node-kind graph; hosted first, then public replay/Rust."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import uuid

from databricks_freshness import decisions
from databricks_merge_recovery import Recovery, assert_bag, literal, reference
from databricks_readonly_reference import reference_canary, show_reference
from databricks_survey import Refused

# Literal cumulative operation ledger, independently checked by fault probes.
# MERGE occurs before APPEND introduces repeated keys. Table/view reset the bag.
A = [['1','10','old'],['2','20','retained']]
M = [['1','11','new'],['3','30','three']]
RM = [M[0],A[1],M[1]]
RA = RM+M
N = [['1','11','new','n1'],['3','30','three','n3']]
RN = [r+[None] for r in RA]+N
V = [['4','40','four','n4']]
STAGES = [('append',A,A),('merge',M,RM),('append',M,RA),('schema',N,RN),
          ('table',N,N),('view',V,V),('append',V,V)]
assert_transition_result = assert_bag


class Transitions(Recovery):
    entrypoint = Path(__file__)

    def __init__(self,args):
        super().__init__(args)
        self.scope.relation_prefix = 'at146_'+args.object_id+'_'
        target = f'cdo_demo_db.{args.schema}.{self.scope.relation_prefix}consumer'
        self.scope.permanent = {target,target.removesuffix('consumer')+'inputs'}
        self.scope.auxiliary = {target+s for s in ('__dbt_stg','__dbt_backup')}
        self.scope.allowed = self.scope.permanent | self.scope.auxiliary
        self.scope.temporary = self.scope.relation_prefix+'consumer__dbt_tmp'
        self.save('footprint.json',{'permanent':sorted(self.scope.permanent),'auxiliary':sorted(self.scope.auxiliary),
                  'session_only':self.scope.temporary,'max_extra_persistent':1,'reference_objects':0,'cleanup':False})
        self.save('operation-manifest.json',{'stages':STAGES,'managed_runs':14,'references':28,
                  'preflight_compiles':7,'owned':sorted(self.scope.permanent),
                  'auxiliary':sorted(self.scope.auxiliary),'temporary':self.scope.temporary,
                  'keys':'MERGE unique nonNULL only; no MERGE after duplicate APPEND',
                  'source_writes':6,'cleanup':False})

    def init_schemas(self):
        if not self.a.continue_from: return super().init_schemas()
        from capture_integrity import verify_capture
        prior = Path(self.a.continue_from)
        summary = json.loads((prior/'summary.json').read_text())
        plan = json.loads((prior/'operation-manifest.json').read_text())
        if (summary['success'] is not False or summary['failure']!='unapproved persistent native CREATE'
                or set(plan['owned'])!=self.scope.permanent
                or not (prior/'checks/4_table_repeat.json').exists()):
            raise Refused('transitions:continuation-not-completed-view-guard')
        for capture in summary['capture']: verify_capture(prior/capture['path'])
        current = self.inventory()
        if current != json.loads((prior/'lease-inventory-after.json').read_text()):
            raise Refused('transitions:continuation-inventory-changed')
        self.before_inventory = current
        # Our own consumer changes VIEW→TABLE; protect every other original row.
        self.before_inventory = {s:[r for r in rows if r['full_name'] not in self.scope.permanent]
                                 for s,rows in current.items()}
        self.save('lease-inventory-before.json',current)
        self.save('continuation-prior.json',{'path':str(prior.resolve()),'summary':summary})

    def model(self,kind,note):
        self.kind,self.note = kind,note
        config = "materialized='"+('incremental' if kind in ('append','merge','schema') else kind)+"'"
        if kind in ('append','merge','schema'):
            config += ",incremental_strategy='"+('merge' if kind=='merge' else 'append')+"',unique_key='id',on_schema_change='"+('append_new_columns' if kind=='schema' else 'ignore')+"'"
        config += ",file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}"
        self.write_model('consumer',config,"select id, amount, tag"+(', note' if note else '')+" from {{ source('transitions','inputs') }}")

    def preflight(self):
        # Compile every actual config before any warehouse mutation. No hooks,
        # custom macros, copied results or unvalidated SQL reach native DDL.
        for i,(kind,_,_) in enumerate(STAGES):
            self.model(kind,i>=3)
            label = 'preflight_'+str(i)
            with reference_canary(self,label) as endpoint:
                previous = self.env['RUN_CACHE_API_URL']
                self.env['RUN_CACHE_API_URL'] = endpoint
                try: self.dbt(label,['compile','--select','consumer'],managed=False)
                finally: self.env['RUN_CACHE_API_URL'] = previous
            folder = self.work/'targets'/f'{self.n:03d}_{label}'
            node = next(v for v in json.loads((folder/'manifest.json').read_text())['nodes'].values() if v['name']=='consumer')
            c = node['config']
            materialized = 'incremental' if kind in ('append','merge','schema') else kind
            if (c['materialized']!=materialized or c.get('pre-hook') or c.get('post-hook')
                    or c['file_format']!='delta' or c['state']['pre_clone']!='never'):
                raise Refused('transitions:preflight-config')
            if materialized=='incremental' and (c['incremental_strategy']!=('merge' if kind=='merge' else 'append')
                    or c['unique_key']!='id' or c['on_schema_change']!=('append_new_columns' if kind=='schema' else 'ignore')):
                raise Refused('transitions:preflight-incremental-config')
            compiled = list((folder/'compiled').rglob('consumer.sql'))
            expected = 'select id, amount, tag'+(', note' if i>=3 else '')+' from '+self.source
            if len(compiled)!=1 or re.sub(r'\s+',' ',compiled[0].read_text().replace('`','')).strip().lower()!=expected:
                raise Refused('transitions:preflight-SQL')

    def checkpoint(self,label,source_rows,target_rows,repeat,calls=None,native_log=None):
        if calls is None: calls = self.dbt(label,['run','--select','consumer'])
        if any(c['status']['code']!=0 for c in calls): raise Refused('transitions:RPC-status')
        submits = [c for c in calls if c['method']=='SubmitEnrichedSQL']
        if len(submits)!=1: raise Refused('transitions:one-submit')
        req = submits[0]['request']
        expected_type = {'append':2,'schema':2,'merge':3,'table':1,'view':10}[self.kind]
        if (req['execution_type']!=expected_type or not req.get('table_namespace')
                or req.get('clone_chain_depth_limit')!=0 or req.get('ignore_external_modifications')
                or req.get('query_dependencies') or req.get('compare_unrendered_code')
                or req['target_table'].replace('`','').lower()!=self.target):
            raise Refused('transitions:submit-scope')
        tables = req['tables']
        names = {t['name'].replace('`','').lower() for t in tables}
        if (len(names)!=len(tables) or not names.issubset({self.source,self.target})
                or any(t['last_modified_epoch'] is None for t in tables)):
            raise Refused('transitions:physical-evidence')
        if self.kind!='view' and self.source not in names: raise Refused('transitions:missing-source')
        if self.kind in ('append','merge','schema') and (
                req['semantic_extras'].get('unique_key')!='"id"' or
                req['semantic_extras'].get('on_schema_change')!=json.dumps('append_new_columns' if self.kind=='schema' else 'ignore')):
            raise Refused('transitions:semantic-settings')
        decisions(calls,'skip_execution' if repeat else 'ready_to_execute')
        if any(c['method'] in ('RecordExecutions','RegisterClone') for c in calls): raise Refused('transitions:unexpected-publication')
        if repeat and any(c['method']=='ConfirmExecution' for c in calls): raise Refused('transitions:skip-confirmed')
        if repeat:
            reuse = submits[0]['response']['response']['skip_execution']
            if reuse['execution_runtime_ms']!=self.last_runtime or req['sql']!=self.last_sql:
                raise Refused('transitions:reuse-not-own-unchanged-result')
        else:
            confirms = [c for c in calls if c['method']=='ConfirmExecution' and c.get('response',{}).get('success')]
            if len(confirms)!=1: raise Refused('transitions:one-successful-confirm')
            self.last_runtime = confirms[0]['request']['execution_runtime_ms']
            self.last_sql = req['sql']
        self.native_target_kind = 'view' if self.kind=='view' else 'table'
        if native_log is not None: self.native_log_path = native_log
        try: self.audit_native(label,label!='0_append')
        finally:
            if hasattr(self,'native_log_path'): del self.native_log_path
        sql = (native_log or self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
        if not repeat:
            operation = {'merge':r'merge\s+into','schema':r'insert\s+into'}
            if self.kind in operation and not re.search(operation[self.kind],sql,re.I): raise Refused('transitions:native-operation')
            if label=='2_append' and not re.search(r'insert\s+into',sql,re.I): raise Refused('transitions:append-operation')
            if self.kind=='schema' and not re.search(r'alter\s+table\b.*?add\s+columns?',sql,re.I|re.S): raise Refused('transitions:schema-add')
        self.explain(label,submits)
        if self.kind!='view':
            history = self.sql(label+'_history',f'DESCRIBE HISTORY {self.target} LIMIT 1')
            if repeat and history != self.last_history: raise Refused('transitions:skip-changed-history')
            self.last_history = history
        self.save('epochs/'+label+'.json',{'tables':tables,'execution_type':req['execution_type'],
                  'semantic_extras':req['semantic_extras'],'response':submits[0]['response']})
        for name,rows in [('source',source_rows),('target',target_rows)]:
            relation = self.source if name=='source' else self.target
            actual = self.observe(label+'_'+name,relation)
            assert_transition_result(actual,rows)
            props = json.loads((self.work/'physical'/f'{label}_{name}_properties.json').read_text())
            properties = {r[0]:r[1] for r in props['rows']}
            if name=='target' and self.kind=='view':
                if properties.get('Type')!='VIEW': raise Refused('transitions:view-property')
            elif properties.get('Type')!='MANAGED' or properties.get('Provider','').lower()!='delta':
                raise Refused('transitions:managed-Delta')
            sql = literal(rows)
            direct = self.sql(label+'_'+name+'_reference',sql)
            assert_transition_result(direct,rows)
            typed = [[int(r[0]),int(r[1]),*r[2:]] for r in rows]
            show_reference(self,label+'_'+name,sql,typed,direct,reference)
        after = self.inventory()
        for schema,rows in self.before_inventory.items():
            if any(r not in after[schema] for r in rows) or any(r['full_name'] not in self.scope.permanent for r in after[schema] if r not in rows):
                raise Refused('transitions:inventory-changed')
        self.save('checks/'+label+'.json',{'source':source_rows,'cumulative_target':target_rows,
                  'kind':self.kind,'required_unchanged_skip':repeat,'complete_typed_disabled_references':True})

    def baseline(self):
        self.setup_project()
        self.source,self.target = [self.scope.relation('prod',n) for n in ('inputs','consumer')]
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'transitions','database':'cdo_demo_db',
                  'schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':self.source.split('.')[-1]}]}]})
        if self.a.continue_from:
            prior = Path(self.a.continue_from)
            self.model('view',True)
            calls = json.loads((prior/'wire/018_5_view.json').read_text())
            self.checkpoint('5_view',V,V,False,calls,prior/'logs/018_5_view/query_log.sql')
            self.checkpoint('5_view_repeat',V,V,True)
            self.model('append',True)
            self.checkpoint('6_append',V,V,False)
            self.checkpoint('6_append_repeat',V,V,True)
            return {'continuation_managed':3,'recovered_view_guard':True,'objects':2}
        self.preflight()
        self.sql('create_source',f'CREATE TABLE {self.source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('initial_source',f"INSERT INTO {self.source} VALUES (1,10,'old'),(2,20,'retained')")
        for i,(kind,source,target) in enumerate(STAGES):
            if i==1: self.sql('merge_source',f"INSERT OVERWRITE TABLE {self.source} VALUES (1,11,'new'),(3,30,'three')")
            if i==3:
                self.sql('add_note',f'ALTER TABLE {self.source} ADD COLUMNS (note STRING)')
                self.sql('note_source',f"INSERT OVERWRITE TABLE {self.source} VALUES (1,11,'new','n1'),(3,30,'three','n3')")
            if i==5: self.sql('view_source',f"INSERT OVERWRITE TABLE {self.source} VALUES (4,40,'four','n4')")
            self.model(kind,i>=3)
            label = str(i)+'_'+kind
            self.checkpoint(label,source,target,False)
            self.checkpoint(label+'_repeat',source,target,True)
        return {'managed':14,'objects':2,'all_boundaries_build_then_skip':True}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease','schema','object-id'): p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    p.add_argument('--profile',default='databricks');p.add_argument('--warehouse',default='47fd3fa312624d91')
    p.add_argument('--query-identity');p.add_argument('--rust-url')
    p.add_argument('--continue-from',help='same graph after completed view rejected only by native staging-view guard')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+uuid.uuid4().hex[:6])
    return Transitions(p.parse_args()).run()


if __name__=='__main__': raise SystemExit(main())
