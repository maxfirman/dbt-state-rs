#!/usr/bin/env python3
"""Opt-in pinned-client managed-seed histories; hosted capture precedes Rust replay.

No cross-target seed copy is requested or enabled. All expected CSV rows/schema
are declared here independently from captured State identities and physical reads.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

from databricks_baseline import Baseline, LeaseScope
from databricks_survey import Refused, check_manifest, same_result


class SeedScope(LeaseScope):
    def __init__(self, run, ledger):
        super().__init__(run, ledger)
        self.relation_prefix = 's_' + run + '_'
        self.allowed = {f'cdo_demo_db.{schema}.{self.relation_prefix}{name}'
                        for schema in self.schemas for name in ('records', 'downstream')}


def expected_rows(revision, identity):
    amount = '10' if revision == 0 else '17'
    return [['1', amount, 'one_' + identity], ['1', amount, 'one_' + identity], ['2', None, None]]


def assert_seed(actual, properties, revision, identity, amount_type='INT'):
    wanted = [('id', 'BIGINT', 'LONG'), ('amount', amount_type, 'INT' if amount_type == 'INT' else 'LONG'),
              ('tag', 'STRING', 'STRING')]
    if [(c.get('name'), c.get('type_text'), c.get('type_name')) for c in actual['columns']] != wanted:
        raise Refused('physical:schema')
    if Counter(map(tuple, actual['rows'])) != Counter(map(tuple, expected_rows(revision, identity))):
        raise Refused('physical:rows')
    props = {row[0]: row[1] for row in properties['rows']}
    if props.get('Type') != 'MANAGED':
        raise Refused('physical:relation-kind')
    if props.get('Provider', '').lower() != 'delta':
        raise Refused('physical:provider')


class Seeds(Baseline):
    def __init__(self, args):
        super().__init__(args)
        if not self.lease:
            raise Refused('seed campaign requires the explicit schema lease')
        self.scope = SeedScope(args.run_id, self.lease)
        for name in ('proxy', 'probe'):
            owned = self.work / 'bin' / name
            owned.parent.mkdir(exist_ok=True)
            shutil.copy2(getattr(args, name), owned)
            setattr(args, name, str(owned))
        self.save('seed-runner.py', Path(__file__).read_text())
        self.save('seed-scope.json', {'allowed': sorted(self.scope.allowed)})
        configuration = json.loads((self.work / 'configuration.json').read_text())
        configuration['seeds'] = {'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                                 'physical_allowlist': sorted(self.scope.allowed),
                                 'binaries': {name: {'path': getattr(args, name), 'sha256': hashlib.sha256(Path(getattr(args, name)).read_bytes()).hexdigest()}
                                              for name in ('proxy', 'probe')},
                                 'copy': 'not exercised; known copied-seed rebuild blocker remains separate'}
        self.save('configuration.json', configuration)

    def configure(self, revision, amount_type, delimiter, quote_columns):
        # Input and expected outputs are explicit, not derived from State hashes.
        amount = 10 if revision == 0 else 17
        rows = [('id', 'amount', 'tag'), (1, amount, 'one_' + self.query_identity),
                (1, amount, 'one_' + self.query_identity), (2, '', '')]
        relative = self.project.relative_to(self.work)
        self.save(str(relative / 'seeds/records.csv'), '\n'.join(delimiter.join(map(str, row)) for row in rows) + '\n')
        self.save(str(relative / 'seeds/properties.yml'), {'version': 2, 'seeds': [
            {'name': 'records', 'config': {'alias': self.scope.relation_prefix + 'records',
             'column_types': {'id': 'bigint', 'amount': amount_type.lower(), 'tag': 'string'},
             'delimiter': delimiter, 'quote_columns': quote_columns}}]})
        self.write_model('downstream', "materialized='table', file_format='delta', state={'lag_tolerance':'0s','pre_clone':'never'}",
                         "select id, amount, tag from {{ ref('records') }}")

    def check_relation(self, label, role, name, revision, amount_type):
        actual = self.observe(label, self.scope.relation(role, name))
        props = json.loads((self.work / 'physical' / (label + '_properties.json')).read_text())
        assert_seed(actual, props, revision, self.query_identity, amount_type)
        return actual

    def actual(self, label, command, method, required):
        calls = self.dbt(label, command)
        submits = [r for r in calls if r['method'] == method]
        if len(submits) != 1:
            raise Refused('expected one ' + method)
        response = submits[0]['response']['response']
        decision = next(iter(response))
        if decision not in ('ready_to_execute', 'skip_execution'):
            raise Refused('ordinary managed seed campaign unexpectedly proposed copy or deferred')
        if decision != required:
            raise Refused(label + ': expected ' + required + ', got ' + decision)
        confirms = [r for r in calls if r['method'] == 'ConfirmExecution'
                    and r['request']['request_id'] == response[decision].get('request_id')
                    and (r.get('response') or {}).get('success') is True]
        if decision == 'ready_to_execute' and len(confirms) != 1:
            raise Refused('build lacks complete successful confirmation')
        self.explain(label, submits)
        return {'decision': decision, 'submit': submits[0], 'confirmations': confirms}

    def baseline(self):
        self.setup_project()
        (self.project / 'models/sources.yml').unlink()
        base = self.project
        reference = self.work / 'reference-project'
        shutil.copytree(base, reference)
        steps = [('first', 0, 'INT', ',', False, 'ready_to_execute'),
                 ('unchanged', 0, 'INT', ',', False, 'skip_execution'),
                 ('contents', 1, 'INT', ',', False, 'ready_to_execute'),
                 ('contents_unchanged', 1, 'INT', ',', False, 'skip_execution'),
                 ('column_type', 1, 'BIGINT', ',', False, 'ready_to_execute'),
                 ('column_type_unchanged', 1, 'BIGINT', ',', False, 'skip_execution'),
                 ('delimiter', 1, 'BIGINT', '|', False, 'ready_to_execute'),
                 ('delimiter_unchanged', 1, 'BIGINT', '|', False, 'skip_execution'),
                 ('quoting', 1, 'BIGINT', '|', True, 'ready_to_execute'),
                 ('quoting_unchanged', 1, 'BIGINT', '|', True, 'skip_execution')]
        self.save('operation-manifest.json', {'version': 1, 'steps': steps, 'required_schema_types': ['BIGINT', 'INT/BIGINT', 'STRING'],
                                             'required_properties': {'Type': 'MANAGED', 'Provider': 'delta'},
                                             'relations': sorted(self.scope.allowed)})
        results = []
        for label, revision, amount_type, delimiter, quote_columns, required in steps:
            for project in (base, reference):
                self.project = project
                self.configure(revision, amount_type, delimiter, quote_columns)
            self.project = base
            if label == 'first':
                self.dbt('inventory', ['parse'], managed=False)
                manifest = json.loads(next((self.work / 'targets').glob('*_inventory/manifest.json')).read_text())
                check_manifest(self.scope, manifest)
                for node in manifest.get('nodes', {}).values():
                    if node.get('resource_type') in ('seed', 'model'):
                        self.scope.check('.'.join([node['database'], node['schema'], node['alias']]))
            seed = self.actual(label + '_seed', ['seed', '--select', 'records'], 'SubmitValues', required)
            actual_seed = self.check_relation(label + '_seed', 'prod', 'records', revision, amount_type)
            # Downstream SQL remains unchanged; its actual request/Explain binds
            # observed seed epochs and producer identity to this physical result.
            downstream = self.actual(label + '_downstream', ['run', '--select', 'downstream'], 'SubmitEnrichedSQL',
                                     'skip_execution' if 'unchanged' in label else 'ready_to_execute')
            actual_downstream = self.check_relation(label + '_downstream', 'prod', 'downstream', revision, amount_type)
            self.project = reference
            for command in (['seed', '--select', 'records'], ['run', '--select', 'downstream']):
                if self.dbt('reference_' + label + '_' + command[0], command, target='ref', managed=False):
                    raise Refused('State-disabled reference sent State RPCs')
            reference_seed = self.check_relation('reference_' + label + '_seed', 'ref', 'records', revision, amount_type)
            reference_downstream = self.check_relation('reference_' + label + '_downstream', 'ref', 'downstream', revision, amount_type)
            if not same_result(actual_seed, reference_seed) or not same_result(actual_downstream, reference_downstream):
                raise Refused('reference:rows-or-schema')
            results.append({'label': label, 'revision': revision, 'amount_type': amount_type,
                            'delimiter': delimiter, 'quote_columns': quote_columns, 'seed': seed, 'downstream': downstream,
                            'complete_typed_multiset_schema_properties': True, 'independent_state_disabled_dbt': True})
            self.save('seed-results.json', results)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('dbt', 'proxy', 'probe', 'work-dir', 'schema-lease'):
        parser.add_argument('--' + name, required=True)
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
        return Seeds(args).run()
    except Exception as error:
        print('FAILED:', str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
