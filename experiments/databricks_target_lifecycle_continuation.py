#!/usr/bin/env python3
"""Explicit same-lifetime continuation of a successful fresh #141 pair.

The prerequisite history is mandatory and must be concatenated for replay. This
entrypoint does not allocate names, retry a failure or admit earlier campaigns.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

from databricks_freshness import assert_rows, decisions
from databricks_readonly_reference import show_reference
from databricks_survey import Refused
from databricks_target_lifecycle import TargetLifecycle


class ContinueLifecycle(TargetLifecycle):
    entrypoint = Path(__file__)

    def __init__(self, args):
        self.prior = Path(args.prerequisite).resolve()
        summary = json.loads((self.prior / 'summary.json').read_text())
        config = json.loads((self.prior / 'configuration.json').read_text())
        if not summary['success'] or config['run_id'] != args.run_id:
            raise Refused('continuation requires successful exact phase lifetime')
        if config['baseline']['backend'] != ('rust' if args.rust_url else 'hosted'):
            raise Refused('continuation backend must match prerequisite')
        from capture_integrity import verify_capture
        for capture in summary['capture']:
            path = self.prior / capture['path']
            rows = verify_capture(path)
            if (capture['complete'] is not True or len(rows) != capture['rows']
                    or hashlib.sha256(path.read_bytes()).hexdigest() != capture['sha256']):
                raise Refused('prerequisite capture certification changed')
        if not summary['capture']:
            raise Refused('prerequisite has no complete causal capture')
        args.query_identity = config['baseline']['query_identity']
        super().__init__(args)
        prior_allow = json.loads((self.prior / 'footprint.json').read_text())['allowed']
        if sorted(self.scope.allowed) != prior_allow:
            raise Refused('continuation names differ from prerequisite')
        self.save('prerequisite.json', {'path': str(self.prior),
                  'summary_sha256': hashlib.sha256((self.prior / 'summary.json').read_bytes()).hexdigest(),
                  'captures': summary['capture'], 'same_owned_lifetime': True})

    def init_schemas(self):
        current = self.inventory()
        prior_after = json.loads((self.prior / 'lease-inventory-after.json').read_text())
        if {k: sorted(v, key=repr) for k, v in current.items()} != {k: sorted(v, key=repr) for k, v in prior_after.items()}:
            raise Refused('continuation inventory changed since prerequisite')
        self.before_inventory = json.loads((self.prior / 'lease-inventory-before.json').read_text())
        self.save('lease-inventory-before.json', current)
        self.save('lifetime-inventory-before.json', self.before_inventory)
        # Exact physical identity proof against final successful checkpoint.
        for name in ('inputs', 'consumer'):
            relation = self.scope.relation('prod', name)
            previous = json.loads((self.prior / 'metadata' /
                      ('logic_a_return_unchanged_' + ('source' if name == 'inputs' else 'target') + '_identity.json')).read_text())
            now = self.api('get', '/api/2.1/unity-catalog/tables/' + relation)
            self.save('metadata/admitted_' + name + '.json', now)
            if now['table_id'] != previous['table_id'] or now['table_type'] != previous['table_type']:
                raise Refused('owned physical identity changed before continuation')

    def policy(self, ignore):
        # Ordinary project YAML; preserve model source bytes while toggling flag.
        import yaml
        path = self.project / 'dbt_project.yml'
        project = yaml.safe_load(path.read_text())
        project['models'] = {project['name']: {'+state': {'ignore_external_modifications': ignore}}}
        self.save('project/dbt_project.yml', project)

    def retained_checkpoint(self, label):
        calls = self.dbt(label, ['run', '--select', 'consumer'])
        submits = decisions(calls, 'skip_execution')
        if submits[0]['request']['ignore_external_modifications'] is not True:
            raise Refused('ordinary YAML did not emit ignore flag')
        self.explain(label, submits)
        # Explicit policy snapshot: source/model still 10, external target 999.
        fresh = [['1', '10', self.tag], ['1', '10', self.tag], ['2', None, None]]
        retained = [['1', '999', self.tag], ['1', '999', self.tag], ['2', None, None]]
        target = self.observe(label, self.target)
        direct = self.sql(label + '_fresh_transform', self.direct)
        assert_rows(target, retained)
        props = {r[0]: r[1] for r in json.loads((self.work / 'physical' / (label + '_properties.json')).read_text())['rows']}
        if props.get('Type') != 'MANAGED' or props.get('Provider', '').lower() != 'delta':
            raise Refused('physical:retained-properties')
        assert_rows(direct, fresh)
        show_reference(self, label, self.direct, [[1, 10, self.tag], [1, 10, self.tag], [2, None, None]], direct)
        self.metadata(label + '_target', self.target)
        self.save('checks/' + label + '.json', {'ignore_external_modifications': True,
                  'target_expected': retained, 'fresh_model_expected': fresh,
                  'policy': 'explicit ignore permits retained external target snapshot'})

    def baseline(self):
        shutil.copytree(self.prior / 'project', self.project)
        self.source = self.scope.relation('prod', 'inputs')
        self.target = self.scope.relation('prod', 'consumer')
        self.tag = 'lifecycle_' + self.query_identity
        self.source_amount, self.observations = 10, []
        self.revision, self.kind = 0, 'table'
        self.direct = f'SELECT id, cast(amount+0 as int) amount, tag FROM {self.source}'
        # Reproducible bounded generated sequence, with literal operation oracles.
        import random
        seed = 141
        candidates = [
            ('update', f'UPDATE {self.target} SET amount=999 WHERE id=1'),
            ('delete', f'DELETE FROM {self.target} WHERE id=1'),
            ('insert', f"INSERT INTO {self.target} VALUES (3,999,'generated')"),
        ]
        trace = random.Random(seed).sample(candidates, len(candidates))
        self.save('generated-trace.json', {'seed': seed, 'operations': trace})
        for number, (kind, statement) in enumerate(trace):
            label = f'generated_{number}_{kind}'
            self.sql(label, statement)
            corrupt = self.observe(label + '_corrupt', self.target)
            try:
                assert_rows(corrupt, [['1', '10', self.tag], ['1', '10', self.tag], ['2', None, None]])
            except Refused as error:
                if str(error) != 'physical:rows':
                    raise
                self.save('checks/' + label + '_mutation.json', {'rejected': 'physical:rows'})
            else:
                raise Refused('oracle accepted deliberate external corruption')
            self.checkpoint(label)
            self.checkpoint(label + '_unchanged', 'skip_execution')
        self.policy(True)
        self.checkpoint('ignore_enabled_unchanged', 'skip_execution')
        last = self.observations[-1]['submit']['request']
        if last['ignore_external_modifications'] is not True:
            raise Refused('ordinary YAML did not emit ignore flag')
        self.sql('ignore_external_update', f'UPDATE {self.target} SET amount=999 WHERE id=1')
        self.retained_checkpoint('ignore_external_retained')
        self.retained_checkpoint('ignore_external_retained_again')
        # Existence remains separate from modification policy.
        self.sql('ignore_drop', f'DROP TABLE {self.target}')
        self.checkpoint('ignore_absent_target', 'ready_to_execute')
        self.checkpoint('ignore_recreated_unchanged', 'skip_execution')
        self.policy(False)
        # External replacement by a view with deliberately different contents.
        self.sql('target_to_view_drop', f'DROP TABLE {self.target}')
        self.sql('target_to_view', f"CREATE VIEW {self.target} AS SELECT cast(9 as bigint) id, cast(999 as int) amount, cast('external_view' as string) tag")
        self.metadata('external_target_view', self.target)
        self.checkpoint('table_over_external_view')
        self.checkpoint('table_over_external_view_unchanged', 'skip_execution')
        # Recreate and replace upstream using the exact still-owned source name.
        self.sql('source_drop', f'DROP TABLE {self.source}')
        missing_start = len(self.wire())
        try:
            self.dbt('missing_source_preprocessing', ['run', '--select', 'consumer'])
        except Refused as error:
            log = (self.work / 'dbt' / f'{self.n:03d}_missing_source_preprocessing.log').read_text()
            missing_calls = self.wire()[missing_start:]
            if ('client invocation exited' not in str(error) or 'TABLE_OR_VIEW_NOT_FOUND' not in log
                    or any(r['method'] == 'SubmitEnrichedSQL' for r in missing_calls)):
                raise
            self.save('reachability/missing_source.json', {'client_failure': str(error),
                      'ordinary_submits': 0, 'warehouse_error': 'TABLE_OR_VIEW_NOT_FOUND',
                      'policy_conclusion': None, 'classification': 'client failure before ordinary Submit'})
        else:
            raise Refused('expected actual missing-source client failure not observed')
        self.source_amount = 17
        self.sql('source_recreate', f"CREATE TABLE {self.source} USING DELTA AS SELECT cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag FROM VALUES (1,17,'{self.tag}'),(1,17,'{self.tag}'),(2,NULL,NULL) t(id,amount,tag)")
        self.checkpoint('source_recreated')
        self.checkpoint('source_recreated_unchanged', 'skip_execution')
        self.source_amount = 23
        self.sql('source_replace', f"CREATE OR REPLACE TABLE {self.source} USING DELTA AS SELECT cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag FROM VALUES (1,23,'{self.tag}'),(1,23,'{self.tag}'),(2,NULL,NULL) t(id,amount,tag)")
        self.checkpoint('source_replaced')
        self.checkpoint('source_replaced_unchanged', 'skip_execution')
        self.sql('source_to_view_drop', f'DROP TABLE {self.source}')
        for label, amount in [('source_to_view', 31), ('source_view_replace', 37)]:
            self.source_amount = amount
            self.sql(label, f"CREATE OR REPLACE VIEW {self.source} AS SELECT cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag FROM VALUES (1,{amount},'{self.tag}'),(1,{amount},'{self.tag}'),(2,NULL,NULL) t(id,amount,tag)")
            self.metadata(label + '_external', self.source)
            self.checkpoint(label)
            self.checkpoint(label + '_unchanged', 'skip_execution')
        self.save('reachability.json', {'same_owned_names': True, 'no_extra_objects': True,
                  'native141': 'incomplete; blocked140; missing/equal/unknown/delayed not inferred'})
        return {'steps': len(self.observations)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('dbt', 'proxy', 'probe', 'work-dir', 'schema-lease', 'prerequisite', 'run-id'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--query-identity')
    parser.add_argument('--rust-url')
    return ContinueLifecycle(parser.parse_args()).run()


if __name__ == '__main__':
    raise SystemExit(main())
