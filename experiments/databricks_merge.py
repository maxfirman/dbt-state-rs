#!/usr/bin/env python3
"""Prepared bounded #145 MERGE discovery; opt-in, fresh two-name phase only.

Hosted first, complete certified history, exact replay, then current Rust with a
fresh pair. This trace is not whole #145: predicates, schema evolution, generated
lifecycle controls and restart persistence remain required follow-up work.
Expected rows are literal operation-history snapshots, never fresh source rows.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re

from databricks_baseline import LeaseScope, check_inventory
from databricks_freshness import assert_rows, decisions
from databricks_freshness_actual import ActualFreshness
from databricks_readonly_reference import show_reference
from databricks_survey import Refused, check_manifest

# Worked independently from the declared writes: default MERGE has no source-
# missing deletion action. In projection, removed key 2 retains its OLD amount.
# Different keys 1/3 deliberately hold duplicate non-key values; key 4 has NULLs.
SNAPSHOTS = {
    'first': [['1','10','same'],['2','20','old'],['3','10','same'],['4',None,None]],
    'upsert': [['1','11','changed'],['2','20','old'],['3','10','same'],['4',None,None],['5','50','new']],
    'delete': [['1','11','changed'],['2','20','old'],['3','10','same'],['4',None,None],['5','50','new']],
    'projection': [['1','111','changed'],['2','20','old'],['3','110','same'],['4',None,None],['5','150','new']],
    'refresh': [['1','111','changed'],['3','110','same'],['4',None,None],['5','150','new']],
}


def assert_merge_result(actual, properties, expected):
    assert_rows(actual, expected)
    props = {row[0]:row[1] for row in properties['rows']}
    if props.get('Type') != 'MANAGED' or props.get('Provider','').lower() != 'delta':
        raise Refused('physical:properties')


def literal_reference(rows):
    def cell(value, kind):
        if value is None:return 'cast(null as ' + kind + ')'
        if kind == 'string':return "'" + value.replace("'","''") + "'"
        return str(int(value))
    values=','.join('('+','.join(cell(v,t) for v,t in zip(row,('bigint','int','string')))+')' for row in rows)
    return ('select cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag '
            'from values '+values+' as declared(id,amount,tag)')


class MergeScope(LeaseScope):
    def __init__(self, run, ledger):
        super().__init__(run,ledger)
        self.relation_prefix='m_'+run+'_'
        self.permanent={f'cdo_demo_db.{self.schema("prod")}.{self.relation_prefix}{name}'
                        for name in ('inputs','consumer')}
        self.auxiliary={f'cdo_demo_db.{self.schema("prod")}.{self.relation_prefix}consumer{suffix}'
                        for suffix in ('__dbt_stg','__dbt_backup')}
        self.allowed=self.permanent | self.auxiliary
        self.temporary=self.relation_prefix+'consumer__dbt_tmp'


def merge_submit(calls, source, target, target_exists, expected=None):
    submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
    if len(submits)!=1:raise Refused('expected one actual MERGE submit')
    request=submits[0]['request']
    if (request['execution_type']!=3 or request['compare_unrendered_code']
            or not request.get('table_namespace') or request['query_dependencies']
            or request['semantic_extras'].get('unique_key') != '"id"'
            or request['semantic_extras'].get('on_schema_change') != '"ignore"'
            or request.get('clone_chain_depth_limit') != 0
            or request.get('ignore_external_modifications')):
        raise Refused('outside bounded rendered MERGE scope')
    physical=lambda name:name.replace('`','').lower()
    wanted={source,target} if target_exists else {source}
    evidence=request['tables']
    if (len(evidence)!=len(wanted) or {physical(t['name']) for t in evidence}!=wanted
            or any(t.get('last_modified_epoch') is None for t in evidence)
            or physical(request['target_table'])!=target):
        raise Refused('MERGE needs exact complete known source/target epochs')
    response=submits[0]['response']['response']
    observed=next(iter(response))
    if observed not in ('skip_execution','ready_to_execute'):raise Refused('unexpected copy/defer response')
    decisions(calls,expected or observed)
    if observed=='skip_execution' and any(r['method']=='ConfirmExecution' for r in calls):
        raise Refused('reuse unexpectedly confirmed execution')
    if any(r['method'] in ('RecordExecutions','RegisterClone') for r in calls):
        raise Refused('ordinary MERGE unexpectedly recorded/copied')
    return submits,observed


class Merge(ActualFreshness):
    entrypoint=Path(__file__)

    def __init__(self,args):
        clearance=json.loads(Path(args.live_clearance).read_text())
        phase='rust' if args.rust_url else 'hosted'
        if (args.run_id != clearance['run_ids'][phase]
                or Path(args.schema_lease).resolve() != Path(clearance['lease_path']).resolve()):
            raise Refused('run/lease differs from exact live clearance')
        super().__init__(args)
        self.clearance=clearance
        self.save('live-clearance.json',clearance)
        self.scope=MergeScope(args.run_id,self.lease)
        if (self.scope.permanent != set(clearance['permanent_exact_allowlists'][phase])
                or self.scope.auxiliary != set(clearance['persistent_auxiliary_exact_spellings'][phase])
                or self.scope.temporary != clearance['session_temporary_unqualified_names'][phase]):
            raise Refused('scope differs from clearance')
        self.save('footprint.json',{'phase_max_objects':2,'persistent_transient_peak':1,'hosted_and_rust_total_names':4,
                  'allowed':sorted(self.scope.allowed),'reference_objects':0,
                  'session_temporary_view':self.scope.relation_prefix+'consumer__dbt_tmp',
                  'temporary_name_requires_final_reservation':True,
                  'cleanup':False,'prior_objects_writable':False,'pre_clone':'never'})
        self.save('operation-manifest.json',{'version':1,'scope':'bounded partial #145 discovery',
                  'expected_snapshots':SNAPSHOTS,'invocations_per_service':11,
                  'readonly_reference_invocations_per_service':11,
                  'full_refresh_wire_path':'discover; no assumed FULL or Record path',
                  'not_covered':['predicates','schema evolution','NULL unique-key actual merge',
                                 'ambiguous duplicate keys','generated lifecycle','restart persistence'],
                  'macro_evidence':'pinned 2.0.11 databricks__get_merge_sql uses <=>; verify actual emitted SQL'})

    def init_schemas(self):
        inventory=self.inventory()
        check_inventory(self.scope,self.lease,inventory)
        if len(inventory[self.scope.schema('prod')])+3>100:
            raise Refused('phase needs two permanent names plus one native transient slot')
        self.before_inventory=inventory
        self.save('lease-inventory-before.json',inventory)
        self.save('headroom.json',{'counts':{k:len(v) for k,v in inventory.items()},
                  'permanent':sorted(self.scope.permanent),'auxiliary':sorted(self.scope.auxiliary),
                  'session_temporary':self.scope.temporary,'persistent_peak_with_phase':len(inventory[self.scope.schema('prod')])+3})

    def audit_native(self,label,target_existed):
        path=self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql'
        raw=path.read_text()
        self.save('materialization/'+label+'.sql',raw)
        text=re.sub(r'/\*.*?\*/','',raw,flags=re.S)
        text='\n'.join(l for l in text.splitlines() if not l.lstrip().startswith('--'))
        target=self.scope.relation('prod','consumer')
        active={target} if target_existed else set()
        peak=len(active);writes=[]
        relation=r'(`[^`]+`(?:\s*\.\s*`[^`]+`)*|[a-zA-Z_][a-zA-Z_0-9.]*)'
        normalize=lambda value:re.sub(r'\s+','',value).replace('`','').lower()
        for sql in text.split(';'):
            sql=sql.strip()
            if not re.match(r'(?i)^(create|drop|alter|merge|insert|update|delete)\b',sql):continue
            create=re.match(r'(?is)^create\s+(?:or\s+replace\s+)?(temporary\s+view|table|view)\s+'+relation,sql)
            rename=re.match(r'(?is)^alter\s+table\s+'+relation+r'\s+rename\s+to\s+'+relation,sql)
            drop=re.match(r'(?is)^drop\s+(table|view)\s+(?:if\s+exists\s+)?'+relation,sql)
            if create:
                kind,name=create.group(1).lower(),normalize(create.group(2))
                if kind=='temporary view':
                    if name!=self.scope.temporary or '.' in name:raise Refused('unapproved session temporary DDL')
                else:
                    if kind!='table' or name not in ({target}|self.scope.auxiliary) or name.endswith('__dbt_backup'):
                        raise Refused('unapproved persistent native CREATE')
                    active.add(name)
            elif rename:
                old,new=map(normalize,rename.groups())
                backup=target+'__dbt_backup';staging=target+'__dbt_stg'
                if (old,new) not in {(target,backup),(staging,target)} or old not in active:
                    raise Refused('unapproved native rename')
                active.remove(old);active.add(new)
            elif drop:
                kind,name=drop.group(1),normalize(drop.group(2))
                if name==self.scope.temporary and kind.lower()=='view':pass
                elif name in ({target}|self.scope.auxiliary):active.discard(name)
                else:raise Refused('unapproved native DROP')
            else:
                change=re.match(r'(?is)^(?:merge\s+into|insert\s+into|alter\s+table|update|delete\s+from)\s+'+relation,sql)
                if not change or normalize(change.group(1)) not in ({target}|self.scope.auxiliary):
                    raise Refused('unapproved native write')
            peak=max(peak,len(active))
            if peak>2:raise Refused('native persistent peak exceeds target plus one transient')
            writes.append({'sql':sql,'active_consumer_lifetime_names':sorted(active)})
        if active != {target}:raise Refused('native materialization left unexpected consumer lifetime names')
        self.save('materialization/'+label+'.json',{'writes':writes,'peak_consumer_lifetime_objects':peak,
                  'peak_extra_persistent_beyond_target':max(0,peak-1),'session_temporary':self.scope.temporary})

    def checked_results(self,label,stage):
        expected=SNAPSHOTS[stage]
        self.save('expected/'+label+'.json',{'stage':stage,'rows':expected,
                  'basis':'literal operation history; source deletion retains prior target row'})
        actual=self.observe(label,self.scope.relation('prod','consumer'))
        props=json.loads((self.work/'physical'/f'{label}_properties.json').read_text())
        assert_merge_result(actual,props,expected)
        reference=literal_reference(expected)
        direct=self.sql(label+'_literal_reference',reference)
        assert_rows(direct,expected)
        typed=[[int(r[0]),None if r[1] is None else int(r[1]),r[2]] for r in expected]
        show_reference(self,label,reference,typed,direct)
        after=self.inventory()
        for schema,previous in self.before_inventory.items():
            if any(row not in after[schema] for row in previous):raise Refused('preexisting object changed')
            if any(row['full_name'] not in self.scope.allowed for row in after[schema] if row not in previous):
                raise Refused('unplanned physical object')
        self.save('inventory/'+label+'.json',after)
        self.save('checks/'+label+'.json',{'complete_history_rows_schema_managed_delta':True,
                  'state_disabled_literal_reference':True,'stage':stage})

    def invocation(self,label,stage,expected=None,refresh=False):
        # Persist reference BEFORE execution, so later mutations cannot redefine it.
        self.save('expected/'+label+'.json',{'stage':stage,'rows':SNAPSHOTS[stage]})
        command=['run','--select','consumer']+(['--full-refresh'] if refresh else [])
        calls=self.dbt(label,command)
        self.audit_native(label,label!='first')
        if refresh:
            if any(r['method']=='RegisterClone' for r in calls):raise Refused('full refresh requested copy')
            # Preserve the actual full-refresh route, type and acknowledgements;
            # no Snowflake-derived assertion on presence of Submit/Record/FULL.
            self.save('full-refresh-wire.json',calls)
            submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
            if submits:self.explain(label,submits)
        else:
            source=self.scope.relation('prod','inputs');target=self.scope.relation('prod','consumer')
            submits,observed=merge_submit(calls,source,target,label!='first',expected)
            evidence={t['name'].replace('`','').lower():t['last_modified_epoch'] for t in submits[0]['request']['tables']}
            if label in ('upsert','delete') and evidence[source]<=self.previous_epochs[source]:
                raise Refused('changed source epoch did not advance; #140 reachability unresolved')
            self.previous_epochs=evidence
            self.explain(label,submits)
            self.save('epochs/'+label+'.json',{'tables':submits[0]['request']['tables'],'observed':observed})
            if observed=='ready_to_execute' and label!='first':
                query_log=(self.work/'logs'/f'{self.n:03d}_{label}'/'query_log.sql').read_text()
                if not re.search(r'(?is)merge\s+into\b.*?<=>',query_log):
                    raise Refused('actual incremental SQL did not evidence null-safe MERGE')
                self.save('materialization/'+label+'.sql',query_log)
        self.checked_results(label,stage)

    def baseline(self):
        self.setup_project()
        source=self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml',{'version':2,'sources':[{'name':'merge_history',
                  'database':'cdo_demo_db','schema':self.scope.schema('prod'),
                  'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        config="materialized='incremental',incremental_strategy='merge',unique_key='id',file_format='delta',on_schema_change='ignore',state={'lag_tolerance':'0s','pre_clone':'never'}"
        def model(projected=False):
            amount='cast(amount + 100 as int)' if projected else 'amount'
            self.write_model('consumer',config,"select id, "+amount+" as amount, tag from {{ source('merge_history','inputs') }}")
        model()
        self.dbt('inventory',['parse'],managed=False)
        manifest=json.loads(next((self.work/'targets').glob('*_inventory/manifest.json')).read_text())
        check_manifest(self.scope,manifest)
        node=next(v for v in manifest['nodes'].values() if v['name']=='consumer')
        if (node.get('language')!='sql' or node['config']['materialized']!='incremental'
                or node['config']['incremental_strategy']!='merge'
                or node['config'].get('file_format')!='delta'
                or node['config'].get('pre-hook') or node['config'].get('post-hook')):
            raise Refused('manifest outside pinned SQL managed-Delta MERGE scope')
        self.save('actual-default-configuration.json',{'node':node,'metadata':manifest['metadata'],
                  'client_behavior_overrides':False,'macros_overridden':False})
        for node in [*manifest.get('nodes',{}).values(),*manifest.get('sources',{}).values()]:
            self.scope.check('.'.join([node['database'],node['schema'],node.get('alias') or node['identifier']]))
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('initial_source',f"INSERT INTO {source} VALUES (1,10,'same'),(2,20,'old'),(3,10,'same'),(4,NULL,NULL)")
        self.invocation('first','first','ready_to_execute')
        self.invocation('unchanged','first','skip_execution')
        self.sql('update_source',f"UPDATE {source} SET amount=11,tag='changed' WHERE id=1")
        self.sql('insert_source',f"INSERT INTO {source} VALUES (5,50,'new')")
        self.invocation('upsert','upsert','ready_to_execute')
        self.invocation('upsert_repeat','upsert','skip_execution')
        self.sql('delete_source',f'DELETE FROM {source} WHERE id=2')
        self.invocation('delete','delete')
        self.invocation('delete_repeat','delete','skip_execution')
        model(True)
        self.invocation('projection','projection')
        self.invocation('projection_repeat','projection','skip_execution')
        self.invocation('full_refresh','refresh',refresh=True)
        self.invocation('next_ordinary','refresh')
        self.invocation('next_repeat','refresh','skip_execution')
        return {'managed_runs':11,'objects':2,'whole_ticket_complete':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease','live-clearance'):parser.add_argument('--'+key,required=True)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks');parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--query-identity');parser.add_argument('--rust-url')
    parser.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+os.urandom(3).hex())
    return Merge(parser.parse_args()).run()


if __name__=='__main__':raise SystemExit(main())
