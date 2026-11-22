#!/usr/bin/env python3
"""Capture #213's prioritized Volatile evaluation cells using dbt 2.0.11."""
import hashlib
import json
from collections import Counter
from pathlib import Path

from databricks_survey import Refused, check_manifest
from databricks_volatile import Volatile, VolatileScope, argument_parser, run_campaign


FUNCTIONS = {
    'rand_seeded': 'rand(42)',
    'random_alias': 'random()',
    'uuid_function': 'uuid()',
    'current_ts': 'current_timestamp()',
    'now_alias': 'now()',
    'today': 'current_date()',
    'rand_off': 'rand()',
    'ts_off': 'current_timestamp()',
    'deterministic_on': '1',
}
STRATEGIES = {'inc_append': 'append', 'inc_merge': 'merge', 'inc_delete': 'delete+insert'}
EXTRA_FORMS = {
    'rand_zero_seed': 'rand(0)',
    'rand_negative_seed': 'rand(-7)',
    'bare_date': 'current_date',
    'bare_timestamp': 'current_timestamp',
}
REUSE_CONTROLS = {'rand_seeded', 'today', 'deterministic_on'}
TABLE_CHAIN = ('input_row', 'with_input', 'strict_consumer')
NAMES = (*FUNCTIONS, *STRATEGIES, *TABLE_CHAIN)
CONSUMERS = ('strict_consumer', 'tolerant_consumer')


class PriorityScope(VolatileScope):
    object_prefix = 'p213_'
    names = (*NAMES, *EXTRA_FORMS, 'tolerant_consumer')
    auxiliary_suffixes = tuple(name + '__dbt_tmp' for name in STRATEGIES)


class Priority(Volatile):
    scope_type = PriorityScope

    def __init__(self, args):
        super().__init__(args)
        if args.batch == 'producer':
            self.names = (*TABLE_CHAIN, 'tolerant_consumer')
        elif args.batch == 'reuse-controls':
            self.names = NAMES
        elif args.batch == 'checked-reuse':
            self.names = ('rand_seeded', 'deterministic_on')
        elif args.batch == 'function-builds':
            self.names = ('rand_seeded', 'today', *EXTRA_FORMS)
        else:
            self.names = tuple(name for name in NAMES if name not in REUSE_CONTROLS)
        self.save('batch.json', {'batch': args.batch, 'models': self.names})
        self.save('priority-runner.py', Path(__file__).read_text())
        provenance = self.work / 'runner-provenance.json'
        self.save(provenance.name, {
            **json.loads(provenance.read_text()),
            Path(__file__).name: hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'record-proxy': hashlib.sha256(Path(args.proxy).read_bytes()).hexdigest(),
        })

    def write_models(self):
        self.setup_project()
        (self.project / 'models/sources.yml').unlink()
        nonce = self.scope.relation_prefix
        forms = FUNCTIONS | EXTRA_FORMS
        for name in (name for name in self.names if name in forms):
            function = forms[name]
            evaluation = 'false' if name.endswith('_off') else 'true'
            self.model(name, f"select 1 as id, {function} as value, '{nonce}{name}' as run_id",
                       "{'evaluate_volatile_sql': " + evaluation + ", 'pre_clone': 'never'}")
        self.model('input_row', f"select 1 as id, 1 as revision, '{nonce}input_row' as run_id",
                   "{'pre_clone': 'never'}")
        self.model('with_input', "select id, revision, rand() as value, run_id "
                   "from {{ ref('input_row') }}",
                   "{'evaluate_volatile_sql': true, 'pre_clone': 'never'}")
        for name, lag in zip(CONSUMERS, ('0s', '3600s')):
            if name in self.names:
                self.model(name, "select * from {{ ref('with_input') }}",
                           "{'lag_tolerance': '" + lag + "', 'pre_clone': 'never'}")
        for name in (name for name in self.names if name in STRATEGIES):
            self.write_model(
                name,
                f"materialized='incremental', incremental_strategy='{STRATEGIES[name]}', unique_key='id', "
                f"file_format='delta', alias='{nonce}{name}', "
                "state={'evaluate_volatile_sql': true, 'pre_clone': 'never'}",
                f"select 1 as id, rand() as value, '{nonce}{name}' as run_id",
            )
        self.dbt('inventory', ['parse'], managed=False)
        manifest = json.loads(next((self.work / 'targets').glob('*_inventory/manifest.json')).read_text())
        check_manifest(self.scope, manifest)
        for node in manifest['nodes'].values():
            self.scope.check('.'.join(node[key] for key in ('database', 'schema', 'alias')))

    def phase(self, label, names, previous):
        calls = self.dbt(label, ['run', '--select', *names])
        target_path = self.work / 'targets' / f'{self.n:03d}_{label}'
        submits = {row['request']['labels']['dbt_node_name']: row for row in calls
                   if row['method'] == 'SubmitEnrichedSQL'}
        if submits.keys() != set(names) or sum(row['method'] == 'SubmitEnrichedSQL' for row in calls) != len(names):
            raise Refused(f'{label}: unexpected ordinary submit inventory')
        confirms = {row['request']['request_id'] for row in calls
                    if row['method'] == 'ConfirmExecution' and row['response'].get('success')}
        self.explain(label + '_explain', target_path, submits.values())
        physical = {}
        decisions = {}
        for name in names:
            row = submits[name]
            response = row['response']['response']
            built = response.get('ready_to_execute')
            if built and built['request_id'] not in confirms:
                raise Refused(f'{label}: {name} lacks successful confirmation')
            if not built and ('skip_execution' not in response or name not in previous):
                raise Refused(f'{label}: {name} unexpectedly reused')
            relation = self.scope.relation('prod', name)
            actual = self.observe(label + '_' + name, relation)
            properties = json.loads((self.work / 'physical' / f'{label}_{name}_properties.json').read_text())
            props = {r[0]: r[1] for r in properties['rows']}
            if props.get('Type') != 'MANAGED' or props.get('Provider', '').lower() != 'delta':
                raise Refused(f'{label}: {name} is not a managed Delta table')

            tag = 'input_row' if name in ('with_input', *CONSUMERS) else name
            expected_rows = 1 + (len(previous['inc_append']['rows'])
                                 if name == 'inc_append' and built and name in previous else 0)
            if (len(actual['rows']) != expected_rows and built
                    or any(row[0] != '1' or row[-1] != self.scope.relation_prefix + tag
                           or any(value is None for value in row) for row in actual['rows'])):
                raise Refused(f'{label}: unexpected complete physical rows for {name}')
            if not built and Counter(map(tuple, actual['rows'])) != Counter(map(tuple, previous[name]['rows'])):
                raise Refused(f'{label}: {name} changed on reuse')
            if name in CONSUMERS and built and actual['rows'] != physical['with_input']['rows']:
                raise Refused(f'{label}: consumer differs from its upstream build')
            physical[name] = actual
            decisions[name] = row['response']
        self.save('checks/' + label + '.json', {'warehouse_values': physical, 'decisions': decisions})
        return physical

    def baseline(self):
        self.write_models()
        first = self.phase('first', self.names, {})
        if self.a.batch == 'function-builds':
            return
        rerun = self.phase('rerun', self.names, first)
        if self.a.batch == 'checked-reuse':
            return
        source = self.scope.relation('prod', 'input_row')
        self.sql('change_input', f'UPDATE {source} SET revision = 2 WHERE id = 1')
        actual = self.observe('changed_input', source)
        if actual['rows'] != [['1', '2', self.scope.relation_prefix + 'input_row']]:
            raise Refused('input revision update did not take effect')
        dependents = ('with_input', *(name for name in CONSUMERS if name in self.names))
        changed = self.phase('upstream_changed', ('with_input',) if self.a.batch == 'response' else dependents,
                             rerun)
        if self.a.batch == 'producer':
            self.phase('after_transition', dependents, changed)
        if self.a.batch != 'response':
            return
        target = self.scope.relation('prod', 'with_input')
        self.sql('modify_target', f'UPDATE {target} SET value = -1 WHERE id = 1')
        if self.observe('modified_target', target)['rows'][0][-2] != '-1.0':
            raise Refused('external target modification did not take effect')
        modified = self.phase('target_modified', ('with_input',), changed)
        self.sql('drop_target', f'DROP TABLE {target}')
        self.phase('target_dropped', ('with_input',), modified)


if __name__ == '__main__':
    parser = argument_parser(__doc__)
    parser.add_argument('--batch', choices=('response', 'reuse-controls', 'checked-reuse', 'function-builds', 'producer'),
                        default='response')
    raise SystemExit(run_campaign(parser, Priority))
