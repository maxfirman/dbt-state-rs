#!/usr/bin/env python3
"""Bounded #141 owned target lifecycle; hosted first, fresh Rust pair second.

Only the two names admitted before launch are writable. Dropping/recreating these
new names is a lifecycle operation, never cleanup of earlier campaigns. References
create no objects. No missing-epoch or full-ticket policy claim follows.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

from databricks_freshness import assert_rows, decisions
from databricks_freshness_actual import ActualFreshness
from databricks_readonly_reference import show_reference
from databricks_survey import Refused, same_result


class TargetLifecycle(ActualFreshness):
    entrypoint = Path(__file__)

    def __init__(self, args):
        super().__init__(args)
        self.save('footprint.json', {'phase_max_objects': 2, 'campaign_max_objects': 4,
                  'roles': self.scope.roles, 'allowed': sorted(self.scope.allowed),
                  'reference_objects': 0, 'previous_objects_writable': False,
                  'drop_scope': 'Only this phase fresh target/source; lifecycle, no cleanup'})

    def metadata(self, label, relation):
        name = relation.rsplit('.', 1)[1]
        catalog = self.api('get', '/api/2.1/unity-catalog/tables/' + relation)
        self.save('metadata/' + label + '_identity.json', catalog)
        if catalog['table_type'] == 'MANAGED':
            self.save('metadata/' + label + '_history.json',
                      self.sql(label + '_history', f'DESCRIBE HISTORY {relation} LIMIT 1'))
        return catalog

    def model(self, revision=0, kind='table', ignore=False):
        self.revision, self.kind = revision, kind
        self.direct = f'SELECT id, cast(amount+{revision} as int) amount, tag FROM {self.source}'
        self.write_model('consumer', "materialized='" + kind + "',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'" +
                         (",'ignore_external_modifications':true" if ignore else '') + '}',
                         "select id, cast(amount+" + str(revision) + " as int) amount, tag from {{ source('freshness','inputs') }}")

    def checkpoint(self, label, expected=None):
        calls = self.dbt(label, ['run', '--select', 'consumer'])
        submits = [r for r in calls if r['method'] == 'SubmitEnrichedSQL']
        if len(submits) != 1:
            raise Refused('expected one ordinary submit')
        response = submits[0]['response']['response']
        observed = 'skip_execution' if 'skip_execution' in response else 'ready_to_execute'
        decisions(calls, expected or observed)
        self.explain(label, submits)
        # The literal operation ledger is independent of State responses.
        expected_rows = [['1', str(self.source_amount + self.revision), self.tag],
                         ['1', str(self.source_amount + self.revision), self.tag], ['2', None, None]]
        actual = self.observe(label, self.target)
        direct = self.sql(label + '_transform', self.direct)
        assert_rows(actual, expected_rows)
        assert_rows(direct, expected_rows)
        if not same_result(actual, direct):
            raise Refused('physical:reference')
        props = {r[0]: r[1] for r in json.loads((self.work / 'physical' / (label + '_properties.json')).read_text())['rows']}
        if props.get('Type') != ('VIEW' if self.kind == 'view' else 'MANAGED'):
            raise Refused('physical:kind')
        if self.kind == 'table' and props.get('Provider', '').lower() != 'delta':
            raise Refused('physical:provider')
        show_reference(self, label, self.direct,
                       [[1, self.source_amount + self.revision, self.tag],
                        [1, self.source_amount + self.revision, self.tag], [2, None, None]], direct)
        self.metadata(label + '_target', self.target)
        self.metadata(label + '_source', self.source)
        after = self.inventory()
        for schema, rows in self.before_inventory.items():
            if any(r not in after[schema] for r in rows):
                raise Refused('preexisting object changed')
            if any(r['full_name'] not in self.scope.allowed for r in after[schema] if r not in rows):
                raise Refused('unplanned physical object')
        self.observations.append({'label': label, 'observed': observed,
                                  'submit': submits[0], 'expected_rows': expected_rows})
        self.save('actual-observations.json', self.observations)
        self.save('checks/' + label + '.json', {'full_typed_rows_schema': True,
                  'properties': True, 'independent_show': True, 'observed': observed})

    def baseline(self):
        self.setup_project()
        self.source = self.scope.relation('prod', 'inputs')
        self.target = self.scope.relation('prod', 'consumer')
        self.tag = 'lifecycle_' + self.query_identity
        self.source_amount = 10
        self.observations = []
        self.save('project/models/sources.yml', {'version': 2, 'sources': [{'name': 'freshness',
                  'database': 'cdo_demo_db', 'schema': self.scope.schema('prod'),
                  'tables': [{'name': 'inputs', 'identifier': self.source.rsplit('.', 1)[1]}]}]})
        self.model()
        self.sql('create_source', f'CREATE TABLE {self.source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        self.sql('insert_source', f"INSERT INTO {self.source} VALUES (1,10,'{self.tag}'),(1,10,'{self.tag}'),(2,NULL,NULL)")
        self.checkpoint('first', 'ready_to_execute')
        self.checkpoint('unchanged', 'skip_execution')
        operations = [
            ('drop', f'DROP TABLE {self.target}'),
            ('update', f'UPDATE {self.target} SET amount=999 WHERE id=1'),
            ('insert', f"INSERT INTO {self.target} VALUES (3,999,'external')"),
            ('delete', f'DELETE FROM {self.target} WHERE id=1'),
            ('replace', f"CREATE OR REPLACE TABLE {self.target} USING DELTA AS SELECT cast(9 as bigint) id, cast(999 as int) amount, cast('external' as string) tag"),
            ('add_column', f'ALTER TABLE {self.target} ADD COLUMN extra STRING'),
            ('property', f"ALTER TABLE {self.target} SET TBLPROPERTIES ('lifecycle.note'='observed')"),
            ('comment', f"COMMENT ON TABLE {self.target} IS 'owned lifecycle observation'"),
        ]
        self.save('operation-manifest.json', {'operations': operations, 'allowed': sorted(self.scope.allowed),
                  'reference_objects': 0, 'history': 'one continuous serial history per phase'})
        for label, statement in operations:
            self.metadata(label + '_before', self.target)
            self.sql(label, statement)
            if label != 'drop':
                self.metadata(label + '_external', self.target)
            self.checkpoint(label)
            self.checkpoint(label + '_unchanged', 'skip_execution')
        # External same-name recreation is separate from the absent-target step.
        self.sql('recreate_drop', f'DROP TABLE {self.target}')
        self.sql('recreate', f"CREATE TABLE {self.target} USING DELTA AS SELECT cast(9 as bigint) id, cast(999 as int) amount, cast('recreated' as string) tag")
        self.metadata('recreated_external', self.target)
        self.checkpoint('recreated')
        self.checkpoint('recreated_unchanged', 'skip_execution')
        # Same physical name, A -> B -> A: no older matching result resurrection.
        self.model(1)
        self.checkpoint('logic_b', 'ready_to_execute')
        self.checkpoint('logic_b_unchanged', 'skip_execution')
        self.model(0)
        self.checkpoint('logic_a_return', 'ready_to_execute')
        self.checkpoint('logic_a_return_unchanged', 'skip_execution')
        self.save('reachability.json', {'native_141': 'incomplete: still blocked by140',
                  'view_transitions': 'not yet exercised', 'ignore_external_modifications': 'not yet reached',
                  'unknown_equal_delayed_metadata': 'not assumed', 'managed_calls': len(self.observations)})
        return {'managed_calls': len(self.observations)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('dbt', 'proxy', 'probe', 'work-dir', 'schema-lease'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--query-identity')
    parser.add_argument('--rust-url')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    return TargetLifecycle(parser.parse_args()).run()


if __name__ == '__main__':
    raise SystemExit(main())
