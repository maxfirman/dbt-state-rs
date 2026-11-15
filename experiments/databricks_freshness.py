#!/usr/bin/env python3
"""Opt-in #140 actual-client freshness campaign. Hosted first, Rust second."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import hashlib
import os
from pathlib import Path
import shutil
import sys

from databricks_baseline import Baseline, LeaseScope
from databricks_survey import Refused, same_result


class FreshnessScope(LeaseScope):
    def __init__(self, run, ledger):
        super().__init__(run, ledger)
        self.relation_prefix = 'f_' + run + '_'
        self.allowed = {f'cdo_demo_db.{schema}.{self.relation_prefix}{name}'
                        for schema in self.schemas
                        for name in ('inputs', 'consumer', 'producer', 'recorded_consumer')}


def assert_rows(actual, expected):
    schema = [(c['name'], c['type_text'], c['type_name']) for c in actual['columns']]
    if schema != [('id', 'BIGINT', 'LONG'), ('amount', 'INT', 'INT'), ('tag', 'STRING', 'STRING')]:
        raise Refused('physical:schema')
    if Counter(map(tuple, actual['rows'])) != Counter(map(tuple, expected)):
        raise Refused('physical:rows')


def decisions(calls, expected, names=('consumer',)):
    submits = [r for r in calls if r['method'] == 'SubmitEnrichedSQL']
    if Counter(r['request']['labels']['dbt_node_name'] for r in submits) != Counter(names):
        raise Refused('unexpected submitted models')
    confirms = {r['request']['request_id'] for r in calls if r['method'] == 'ConfirmExecution'
                and (r.get('response') or {}).get('success') is True}
    for row in submits:
        response = row['response']['response']
        if expected not in response:
            raise Refused('expected ' + expected + ' for ' + row['request']['labels']['dbt_node_name'])
        if expected == 'ready_to_execute' and response[expected]['request_id'] not in confirms:
            raise Refused('build lacks successful confirmation')
        if (row['request']['freshness_tolerance_seconds'] != 0
                or row['request']['stale_upstream_policy'] != 0):
            raise Refused('freshness isolation requires zero lag and ANY')
    return submits


class Freshness(Baseline):
    entrypoint = Path(__file__)
    def __init__(self, args):
        if not args.schema_lease:
            raise Refused('freshness campaign requires explicit schema ownership lease')
        super().__init__(args)
        self.scope = FreshnessScope(args.run_id, self.lease)
        self.save('freshness-runner.py', Path(__file__).read_text())
        self.save('campaign-entrypoint.py', self.entrypoint.read_text())
        configuration = json.loads((self.work / 'configuration.json').read_text())
        configuration['freshness'] = {
            'entrypoint':self.entrypoint.name,
            'entrypoint_sha256':hashlib.sha256(self.entrypoint.read_bytes()).hexdigest(),
            'evidence_class':'actual-pinned-client' if self.entrypoint.name != 'databricks_freshness_diagnostic.py'
                             else 'mixed-actual-client-and-crafted-public-grpc',
            'sql_timing':'started_at/completed_at UTC for every SQL statement',
        }
        self.save('configuration.json', configuration)

    def sql(self, label, statement):
        started = datetime.now(timezone.utc).isoformat()
        try:
            return super().sql(label, statement)
        finally:
            self.event('sql-timing', label=label, started_at=started,
                       completed_at=datetime.now(timezone.utc).isoformat())

    def sample(self, label):
        source = self.scope.relation('prod', 'inputs')
        history = self.sql(label + '_history', f'DESCRIBE HISTORY {source} LIMIT 1')
        catalog = self.sql(label + '_catalog', "SELECT table_catalog,table_schema,table_name,table_type,created,last_altered,data_source_format FROM cdo_demo_db.information_schema.tables WHERE table_schema='" + self.scope.schema('prod') + "' AND table_name='" + source.split('.')[-1] + "'")
        sample = {'observed_at':datetime.now(timezone.utc).isoformat(), 'history':history, 'catalog':catalog}
        self.save('metadata/' + label + '.json', sample)
        return sample

    def checked_results(self, label, expected, sql):
        actual = self.observe(label, self.scope.relation('prod', getattr(self, 'output_name', 'consumer')))
        assert_rows(actual, expected)
        properties = json.loads((self.work / 'physical' / (label + '_properties.json')).read_text())
        props = {r[0]:r[1] for r in properties['rows']}
        if props.get('Type') != 'MANAGED' or props.get('Provider', '').lower() != 'delta':
            raise Refused('physical:properties')
        direct = self.sql(label + '_independent_select', sql)
        assert_rows(direct, expected)
        base = self.project
        self.project = self.work / 'reference-project'
        try:
            if self.dbt('reference_' + label, ['run', '--select', getattr(self, 'output_name', 'consumer')], target='ref', managed=False):
                raise Refused('State-disabled reference sent RPCs')
            reference = self.observe('reference_' + label, self.scope.relation('ref', getattr(self, 'output_name', 'consumer')))
        finally:
            self.project = base
        assert_rows(reference, expected)
        if not same_result(actual, reference) or not same_result(actual, direct):
            raise Refused('physical:reference')
        self.save('checks/' + label + '.json', {'complete_typed_multiset_schema_properties':True,
                  'independent_dbt_and_select':True, 'expected_rows':expected})

    def baseline(self):
        self.setup_project()
        source = self.scope.relation('prod', 'inputs')
        self.save('project/models/sources.yml', {'version':2, 'sources':[{'name':'freshness',
                  'database':'cdo_demo_db', 'schema':self.scope.schema('prod'),
                  'tables':[{'name':'inputs', 'identifier':source.split('.')[-1]}]}]})
        self.sql('create_inputs', f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        tag = 'run_' + self.query_identity
        self.sql('initial_inputs', f"INSERT INTO {source} VALUES (1,10,'{tag}'),(1,10,'{tag}'),(2,NULL,NULL)")
        model_sql = "select id, amount, tag from {{ source('freshness', 'inputs') }}"
        self.write_model('consumer', "materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}", model_sql)
        shutil.copytree(self.project, self.work / 'reference-project')
        direct_sql = f'SELECT id, amount, tag FROM {source}'
        rows = [['1','10',tag],['1','10',tag],['2',None,None]]
        steps = [('first', None, rows), ('unchanged', None, rows),
                 ('insert', f"INSERT INTO {source} VALUES (3,30,'inserted')", rows + [['3','30','inserted']]),
                 ('update', f"UPDATE {source} SET amount=17 WHERE id=1", [['1','17',tag],['1','17',tag],['2',None,None],['3','30','inserted']]),
                 ('delete', f'DELETE FROM {source} WHERE id=3', [['1','17',tag],['1','17',tag],['2',None,None]]),
                 ('no_op', f'UPDATE {source} SET amount=amount WHERE id=1', None),
                 ('add_column', f'ALTER TABLE {source} ADD COLUMN extra STRING', None),
                 ('property', f"ALTER TABLE {source} SET TBLPROPERTIES ('freshness.note'='measured')", None),
                 ('comment', f"COMMENT ON TABLE {source} IS 'freshness metadata measurement'", None)]
        self.save('operation-manifest.json', {'version':1,'evidence_class':'actual-pinned-client',
                  'lag_tolerance_seconds':0, 'steps':[{'id':n,'sql':s,'expected_rows':r} for n,s,r in steps],
                  'relations':sorted(self.scope.allowed)})
        for label, mutation, expected_rows in steps:
            if expected_rows is not None:
                rows = expected_rows
            before = self.sample(label + '_before')
            if mutation:
                self.sql(label, mutation)
            self.sample(label + '_immediate')
            calls = self.dbt(label, ['run', '--select', 'consumer'])
            expected = 'skip_execution' if label == 'unchanged' else 'ready_to_execute'
            submits = decisions(calls, expected)
            self.explain(label, submits)
            self.checked_results(label, rows, direct_sql)
            # Bounded stable observations include client evidence after the metadata
            # reads, not a retry that stops merely because information_schema changed.
            self.sample(label + '_after_build')
            repeat = self.dbt(label + '_unchanged', ['run', '--select', 'consumer'])
            repeat_submits = decisions(repeat, 'skip_execution')
            self.explain(label + '_unchanged', repeat_submits)
            self.checked_results(label + '_unchanged', rows, direct_sql)
            self.sample(label + '_final')
            self.save('epochs/' + label + '.json', {'before':before,
                      'submit_tables':submits[0]['request']['tables'],
                      'unchanged_tables':repeat_submits[0]['request']['tables'],
                      'evidence_class':'actual-pinned-client'})
        return {'steps':len(steps)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--rust-url')
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--query-identity')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    args = parser.parse_args()
    if args.rust_url:
        import re
        if not re.fullmatch(r'http://127\.0\.0\.1:\d+', args.rust_url):
            parser.error('--rust-url must be an owned loopback endpoint')
    try:
        return Freshness(args).run()
    except Exception as error:
        print('FAILED:', str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
