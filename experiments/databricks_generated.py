#!/usr/bin/env python3
"""Opt-in physical replay of a bounded, exported #161 generated safety plan."""
import argparse
from datetime import datetime, timezone
import json
import re
from pathlib import Path
import uuid

from databricks_freshness import decisions
from databricks_freshness_actual import ActualFreshness
from databricks_survey import Refused


def operation(step):
    op = step['op']
    return ('Observe', None) if op == 'Observe' else next(iter(op.items()))


def validate_plan(plan):
    if (set(plan) != {'version','seed','literal','operations'} or plan['version'] != 1
            or type(plan['seed']) is not int or not 0 <= plan['seed'] < 2**64
            or type(plan['literal']) is not bool):
        raise Refused('generated:plan-header')
    steps = plan['operations']
    if not isinstance(steps, list) or not 2 <= len(steps) <= 44:
        raise Refused('generated:plan-bound')
    for i, step in enumerate(steps):
        if (not isinstance(step, dict) or set(step) != {'op','required','depends_on'}
                or step['depends_on'] != (i-1 if i else None)):
            raise Refused('generated:dependency')
        op = step['op']
        if op != 'Observe':
            if not isinstance(op, dict) or len(op) != 1:
                raise Refused('generated:operation')
            name, value = next(iter(op.items()))
            if name == 'Insert':
                valid = (not plan['literal'] and isinstance(value, list) and len(value) == 2
                         and all(type(v) is int for v in value)
                         and 3 <= value[0] <= 100 and -1000 <= value[1] <= 1000)
            else:
                valid = (type(value) is int and -1000 <= value <= 1000
                         and name in (('Literal',) if plan['literal'] else ('Delete','Update','K')))
            if not valid:
                raise Refused('generated:operation')
        expected = 'SKIP' if i and op == 'Observe' else 'BUILD'
        if step['required'] != expected or (i < 2 and op != 'Observe'):
            raise Refused('generated:decision-contract')
    return plan


class Generated(ActualFreshness):
    entrypoint = Path(__file__)

    def __init__(self, args):
        self.plan = validate_plan(json.loads(Path(args.plan).read_text()))
        if args.observations is not None:
            if not 2 <= args.observations <= len(self.plan['operations']):
                raise Refused('generated:observation-bound')
        object_id = uuid.UUID(args.object_id).hex
        if object_id != args.object_id:
            raise Refused('generated:object-id UUID')
        super().__init__(args)
        if args.schema:
            # LeaseScope has already verified every source evidence digest.
            owned = set()
            for evidence in self.lease['source_evidence']:
                owned.update(json.loads(Path(evidence['path']).read_text())['retained_schemas'])
            if (args.schema not in owned or not re.fullmatch(r'ds_\d{8}[a-f0-9]{6}_(prod|ref)',args.schema)
                    or args.schema == self.scope.schema('ref')):
                raise Refused('generated:schema is not a distinct evidenced owned container')
            self.scope.roles['prod'] = args.schema
            self.scope.schemas = set(self.scope.roles.values())
            self.lease['roles'] = dict(self.scope.roles)
            self.lease.setdefault('preexisting_objects',{}).setdefault(args.schema,[])
            import yaml
            profile = yaml.safe_load((self.profiles/'profiles.yml').read_text())
            profile['survey']['outputs']['prod']['schema'] = args.schema
            self.save('profiles/profiles.yml',profile)
            configuration = json.loads((self.work/'configuration.json').read_text())
            configuration['outputs'] = profile['survey']['outputs']
            configuration['retained_schemas'] = sorted(self.scope.schemas)
            self.save('configuration.json',configuration)
            self.save('schema-lease.json',self.lease)
        self.scope.relation_prefix = 'g161_' + object_id + '_'
        self.scope.allowed = {f'cdo_demo_db.{self.scope.schema("prod")}.{self.scope.relation_prefix}{name}'
                              for name in ('inputs','consumer')}
        self.save('footprint.json', {'phase_max_objects':2,'reference_objects':0,
                  'allowed':sorted(self.scope.allowed),'cleanup':'retain; no drops'})
        self.save('plan.json', self.plan)

    def baseline(self):
        self.setup_project()
        source = self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml', {'version':2,'sources':[{'name':'freshness',
            'database':'cdo_demo_db','schema':self.scope.schema('prod'),
            'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.sql('create_source',f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('initial_source',f"INSERT INTO {source} VALUES (1,10,'one'),(1,10,'one'),(2,NULL,NULL)")
        rows = [[1,10,'one'],[1,10,'one'],[2,None,None]]
        value = 10 if self.plan['literal'] else 0
        steps = self.plan['operations'][:self.a.observations]
        for index, step in enumerate(steps):
            label = f'generated_{index:02d}'
            name, operand = operation(step)
            self.event('generated-operation', index=index, operation=step)
            if name == 'Insert':
                key, amount = operand
                if any(r[0] == key for r in rows): raise Refused('generated:duplicate-insert')
                self.sql(label+'_insert',f"INSERT INTO {source} VALUES ({key},{amount},'inserted')")
                rows.append([key,amount,'inserted'])
            elif name == 'Update':
                self.sql(label+'_update',f'UPDATE {source} SET amount={operand} WHERE id=1')
                rows = [[i,operand if i == 1 else a,t] for i,a,t in rows]
            elif name == 'Delete':
                if not any(r[0] == operand for r in rows): raise Refused('generated:missing-delete')
                self.sql(label+'_delete',f'DELETE FROM {source} WHERE id={operand}')
                rows = [r for r in rows if r[0] != operand]
            elif name in ('K','Literal'):
                value = operand
            if self.plan['literal']:
                query = (f"select cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag "
                         f"from values (1,{value},'one'),(1,{value},'one'),(2,cast(null as int),cast(null as string)) as declared(id,amount,tag)")
                expected = [[1,value,'one'],[1,value,'one'],[2,None,None]]
            else:
                query = f'SELECT id, cast(amount+({value}) as int) amount, tag FROM {source}'
                # Independent symbolic ledger: never reads the target or State.
                expected = [[i,None if a is None else a+value,t] for i,a,t in rows]
            self.write_model('consumer',"materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}",query)
            calls = self.dbt(label,['run','--select','consumer'])
            # Check actual typed R against F before enforcing the decision, so
            # a stale SKIP is a result failure rather than a policy blessing.
            cells = [[None if cell is None else str(cell) for cell in row] for row in expected]
            self.checked_results(label,cells,query)
            submits = decisions(calls,'skip_execution' if step['required']=='SKIP' else 'ready_to_execute')
            if any(c['status']['code'] != 0 for c in calls):
                raise Refused('generated:non-OK-RPC')
            self.explain(label,submits)
            self.event('generated-verified',index=index,required=step['required'],expected=expected)
        return {'seed':self.plan['seed'],'observations':len(steps)}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease','plan'):
        p.add_argument('--'+key,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91')
    p.add_argument('--query-identity')
    p.add_argument('--schema',help='existing container in verified lease source evidence; never creates schemas')
    p.add_argument('--rust-url')
    p.add_argument('--observations',type=int,help='bounded prefix for a small live pilot')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+uuid.uuid4().hex[:6])
    p.add_argument('--object-id',default=uuid.uuid4().hex,help='fresh UUID hex; exact two-object allowlist')
    return Generated(p.parse_args()).run()


if __name__ == '__main__':
    raise SystemExit(main())
