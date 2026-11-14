#!/usr/bin/env python3
"""Opt-in #137 actual pinned-dbt SQL equivalence discovery/acceptance."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

from databricks_baseline import Baseline, LeaseScope, query
from databricks_survey import Refused, same_result


class SqlScope(LeaseScope):
    def __init__(self, run, ledger):
        super().__init__(run, ledger)
        self.relation_prefix = 'q_' + run + '_'
        self.allowed = {f'cdo_demo_db.{schema}.{self.relation_prefix}{name}'
                        for schema in self.schemas for name in ('input', 'result')}


def cells(source):
    base = 'select id, amount, tag from ' + source
    return [
        ('first', base, 'original'),
        ('unchanged', base, 'original'),
        ('spacing', 'select\n id,  amount,\n tag\nfrom   ' + source, 'original'),
        ('keyword_case', 'SELECT id, amount, tag FROM ' + source, 'original'),
        ('leading_comment', '-- leading comment\n' + base, 'original'),
        ('inline_comment', 'select id, /* inline */ amount, tag from ' + source, 'original'),
        ('trailing_comment', base + ' -- trailing comment', 'original'),
        ('table_alias', 'select x.id, x.amount, x.tag from ' + source + ' as x', 'original'),
        ('renamed_table_alias', 'select y.id, y.amount, y.tag from ' + source + ' y', 'original'),
        ('same_output_alias', 'select id as id, amount as amount, tag as tag from ' + source, 'original'),
        ('renamed_output', 'select id as renamed_id, amount, tag from ' + source, 'renamed'),
        ('return_original', base, 'original'),
        ('numeric_literal', 'select id, cast(10 as int) amount, tag from ' + source, 'ten'),
        ('numeric_spelling', 'select id, cast(010 as int) amount, tag from ' + source, 'ten'),
        ('numeric_changed', 'select id, cast(17 as int) amount, tag from ' + source, 'seventeen'),
        ('string_literal', "select id, amount, cast('-- /* one */' as string) tag from " + source, 'string_one'),
        ('string_comment', "select id, /* real */ amount, cast('-- /* one */' as string) tag from " + source, 'string_one'),
        ('string_changed', "select id, amount, cast('-- /* two */' as string) tag from " + source, 'string_two'),
        ('null_literal', 'select id, cast(null as int) amount, tag from ' + source, 'null'),
        ('null_case', 'SELECT id, CAST(NULL AS INT) amount, tag FROM ' + source, 'null'),
        ('cte_first', 'with a as (' + base + ') select id, amount, tag from a', 'original'),
        ('cte_rename', 'with b as (' + base + ') select id, amount, tag from b', 'original'),
        ('cte_pair', 'with a as (' + base + '), b as (select id, cast(17 as int) amount, tag from ' + source + ') select id, amount, tag from a', 'original'),
        ('cte_swapped', 'with a as (select id, cast(17 as int) amount, tag from ' + source + '), b as (' + base + ') select id, amount, tag from a', 'seventeen'),
        ('cte_shadow', 'with a as (' + base + ') select id, amount, tag from (with a as (select id, cast(17 as int) amount, tag from ' + source + ') select id, amount, tag from a) nested', 'seventeen'),
        ('cte_unshadow', 'with a as (' + base + ') select id, amount, tag from (with b as (select id, cast(17 as int) amount, tag from ' + source + ') select id, amount, tag from a) nested', 'original'),
    ]


def cte_cells(source):
    base = 'with stable as (select id, amount, tag from ' + source + ') select id, amount, tag from stable'
    return [('cte_control_first',base,'original'),
            ('cte_control_unchanged',base,'original'),
            ('cte_control_comment',base.replace('select id, amount, tag from stable','select id, /* ordinary */ amount, tag from stable'),'original'),
            ('cte_control_format',base.replace('select id, amount, tag from stable','SELECT id,  amount, tag FROM stable'),'original')]


def assert_result(actual, shape, identity):
    names = ['renamed_id' if shape == 'renamed' else 'id', 'amount', 'tag']
    schema = [(c.get('name'), c.get('type_text'), c.get('type_name')) for c in actual['columns']]
    if schema != list(zip(names, ['BIGINT','INT','STRING'], ['LONG','INT','STRING'])):
        raise Refused('physical:schema:' + shape)
    amounts = {'ten': ['10','10','10'], 'seventeen': ['17','17','17'], 'null': [None,None,None]}.get(shape, ['10','10',None])
    tags = ['one_' + identity, 'one_' + identity, None]
    if shape.startswith('string_'):
        tags = ['-- /* ' + ('one' if shape == 'string_one' else 'two') + ' */'] * 3
    expected = list(zip(['1','1','2'], amounts, tags))
    if Counter(map(tuple, actual['rows'])) != Counter(expected):
        raise Refused('physical:rows:' + shape)


class SqlEquivalence(Baseline):
    def __init__(self, args):
        super().__init__(args)
        if not self.lease:
            raise Refused('SQL campaign requires explicit schema lease')
        self.scope = SqlScope(args.run_id, self.lease)
        for name in ('proxy','probe'):
            owned = self.work/'bin'/name
            owned.parent.mkdir(exist_ok=True)
            shutil.copy2(getattr(args,name),owned)
            setattr(args,name,str(owned))
        self.save('sql-equivalence-runner.py', Path(__file__).read_text())
        self.save('sql-scope.json', {'allowed':sorted(self.scope.allowed)})
        self.contract = json.loads(Path(args.contract).read_text()) if args.contract else None

    def baseline(self):
        self.setup_project()
        (self.project/'models/sources.yml').unlink()
        base = self.project
        reference = self.work/'reference-project'
        shutil.copytree(base, reference)
        source = self.scope.relation('prod','input')
        self.sql('create_input', 'CREATE TABLE ' + source + ' USING DELTA AS ' + query(0,self.query_identity))
        matrix = cte_cells(source) if self.a.only_cte else cells(source)
        self.save('operation-manifest.json', {'version':1,'kind':'actual-pinned-dbt','query_identity':self.query_identity,
                  'cells':matrix,'relations':sorted(self.scope.allowed),'first_explain_only':True,'lag_tolerance':'0s',
                  'runner_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
        results=[]
        for label, sql, shape in matrix:
            self.project=base
            self.write_model('result', "materialized='table', file_format='delta', state={'lag_tolerance':'0s','pre_clone':'never'}",sql)
            calls=self.dbt(label,['run'])
            submits=[r for r in calls if r['method']=='SubmitEnrichedSQL']
            if len(submits)!=1:
                raise Refused('must submit exactly one model')
            response=submits[0]['response']['response']
            decision=next(iter(response))
            if decision=='ready_to_execute':
                confirms={r['request']['request_id'] for r in calls if r['method']=='ConfirmExecution' and (r.get('response') or {}).get('success') is True}
                if response[decision]['request_id'] not in confirms:
                    raise Refused('build lacks successful confirmation')
            elif decision!='skip_execution':
                raise Refused('unexpected copy/result')
            if self.contract and decision!=self.contract[label]:
                raise Refused(label+': required '+self.contract[label]+', got '+decision)
            self.explain(label,submits)
            actual=self.observe(label,self.scope.relation('prod','result'))
            assert_result(actual,shape,self.query_identity)
            properties=json.loads((self.work/'physical'/(label+'_properties.json')).read_text())
            props={r[0]:r[1] for r in properties['rows']}
            if props.get('Type')!='MANAGED' or props.get('Provider','').lower()!='delta':
                raise Refused('physical:properties')
            # Independent State-disabled execution uses the exact workload and
            # literal oracle; never derive the reference from a State decision.
            self.project=reference
            self.write_model('result',"materialized='table', file_format='delta'",sql)
            if self.dbt('reference_'+label,['run'],target='ref',managed=False):
                raise Refused('reference unexpectedly sent State RPCs')
            independent=self.observe('reference_'+label,self.scope.relation('ref','result'))
            assert_result(independent,shape,self.query_identity)
            if not same_result(actual,independent):
                raise Refused('reference:rows-or-schema')
            self.project=base
            results.append({'step':label,'decision':decision,'shape':shape,'request':submits[0]['request'],'response':submits[0]['response']})
            self.save('sql-results.json',results)
        return {'cells':len(results)}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','probe','work-dir','schema-lease'):
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--query-identity')
    parser.add_argument('--only-cte',action='store_true',help='fresh supplemental CTE formatting/unchanged characterization')
    parser.add_argument('--contract',help='literal decisions from earlier hosted discovery; required for Rust acceptance')
    parser.add_argument('--rust-url')
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    parser.add_argument('--profile',default='databricks')
    parser.add_argument('--warehouse',default='47fd3fa312624d91')
    parser.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+os.urandom(3).hex())
    args=parser.parse_args()
    if args.rust_url:
        import re
        if not args.contract or not re.fullmatch(r'http://127\.0\.0\.1:[1-9]\d*',args.rust_url):
            parser.error('Rust acceptance requires literal loopback HTTP and hosted contract')
    try:
        return SqlEquivalence(args).run()
    except Exception as error:
        print('FAILED:',str(error),file=sys.stderr)
        return 1

if __name__=='__main__':
    raise SystemExit(main())
