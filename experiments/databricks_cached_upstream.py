#!/usr/bin/env python3
"""Six ordinary cached data-test outcomes over one fresh owned Delta source.

Hosted first, complete public replay, then real dbt against Rust. No audit writes,
clone, schema creation, or cleanup. Every observation has a State-disabled dbt
Reference execution plus independently declared complete typed rows/aggregate.
"""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import uuid

from databricks_baseline import LeaseScope
from databricks_cached_tests import CachedTests, client_sql, main
from databricks_survey import Campaign, Refused


class UpstreamScope(LeaseScope):
    profile_roles = ('prod',)

    def __init__(self, args, lease):
        super().__init__(args.run_id, lease)  # Validate original ownership digests.
        owned = set()
        for evidence in lease['source_evidence']:
            owned.update(json.loads(Path(evidence['path']).read_text())['retained_schemas'])
        if (args.schema not in owned
                or not re.fullmatch(r'ds_\d{8}[a-f0-9]{6}_(prod|ref)', args.schema)
                or uuid.UUID(args.object_id).hex != args.object_id):
            raise Refused('upstream:owned-container-and-fresh-UUID')
        self.roles = {'prod': args.schema}
        self.schemas = {args.schema}
        self.relation_prefix = 't143_' + args.object_id + '_'
        self.allowed = {f'cdo_demo_db.{args.schema}.{self.relation_prefix}inputs'}


class UpstreamTests(CachedTests):
    reuse_binaries = True

    @staticmethod
    def configure_parser(parser):
        parser.add_argument('--schema', required=True)
        parser.add_argument('--object-id', required=True)

    def build_scope(self, args):
        return UpstreamScope(args, self.lease)

    def __init__(self, args):
        super().__init__(args)
        self.source = self.scope.relation('prod', 'inputs')
        self.quoted_source = '.'.join('`' + part + '`' for part in self.source.split('.'))
        self.source_uid = 'source.cached_' + args.run_id + '.cached.inputs'
        (self.work / 'no-object-scope.json').unlink()
        self.save('upstream-runner.py', Path(__file__).read_text())
        self.save('operation-manifest.json', {'owned': sorted(self.scope.allowed),
                  'phase_max_objects': 1, 'mutations': ['CREATE TABLE', 'INSERT', 'UPDATE fail', 'UPDATE recover'],
                  'cleanup': 'retain; no drops', 'plan': self.plan(),
                  'reference': 'State-disabled dbt test and complete typed SQL API result',
                  'initial_rows': [[1, 2], [1, 2], [2, None]],
                  'failed_rows': [[1, -2], [1, -2]], 'recovered_failures': 0})

    @staticmethod
    def plan():
        return [('pass_first', 0, 'ready_to_execute'), ('pass_repeat', 0, 'skip_execution'),
                ('data_fail', 2, 'ready_to_execute'), ('fail_repeat', 2, 'skip_execution'),
                ('data_recover', 0, 'ready_to_execute'), ('recover_repeat', 0, 'skip_execution')]

    def inventory(self):
        # Follow every page; protect the entire current inventory, not just the
        # lease's older floor. Only the exact fresh UUID name may be added.
        rows, token, seen = [], None, set()
        while True:
            from urllib.parse import quote
            endpoint = '/api/2.1/unity-catalog/tables?catalog_name=cdo_demo_db&schema_name=' + self.scope.schema('prod')
            if token:
                endpoint += '&page_token=' + quote(token, safe='')
            response = self.api('get', endpoint)
            rows.extend({'name': row['name'], 'full_name': row['full_name'], 'table_type': row['table_type']}
                        for row in response.get('tables', []))
            next_token = response.get('next_page_token')
            if not next_token:
                break
            if next_token in seen:
                raise Refused('upstream:inventory-pagination-loop')
            seen.add(next_token)
            token = next_token
        return {self.scope.schema('prod'): rows}

    def setup_campaign(self):
        rows = self.before_inventory[self.scope.schema('prod')]
        if len(rows) + 1 > 100 or any(row['name'].startswith(self.scope.relation_prefix) for row in rows):
            raise Refused('upstream:collision-or-capacity')
        self.write_test(0)
        from databricks_readonly_reference import reference_canary
        with reference_canary(self, 'preflight') as endpoint:
            result = self.command('preflight', 'compile', False, endpoint)
        if result.returncode:
            raise Refused('upstream:preflight-compile-failed-before-DDL')
        self.manifest(result, 0)
        self.sql('create_source', f'CREATE TABLE {self.source} (id INT, v INT) USING DELTA')
        self.sql('initial_source', f'INSERT INTO {self.source} VALUES (1,2),(1,2),(2,NULL)')
        props = self.sql('source_properties', f'DESCRIBE TABLE EXTENDED {self.source}')
        properties = {row[0]: row[1] for row in props['rows']}
        if properties.get('Type') != 'MANAGED' or properties.get('Provider', '').lower() != 'delta':
            raise Refused('upstream:managed-delta')
        self.save('source-properties.json', props)

    def before_cell(self, label):
        if label in ('data_fail', 'data_recover'):
            value = -2 if label == 'data_fail' else 2
            self.sql(label, f'UPDATE {self.source} SET v={value} WHERE id=1')

    def rendered_query(self, failures):
        return f'SELECT id, v FROM {self.quoted_source} WHERE v < 0'

    def compiled_sql(self, sql):
        return sql.strip()

    def write_test(self, failures):
        self.save('project/dbt_project.yml', {'name': 'cached_' + self.a.run_id,
                  'version': '1.0', 'config-version': 2, 'profile': 'survey'})
        self.save('project/models/sources.yml', {'version': 2, 'sources': [{'name': 'cached',
                  'database': 'cdo_demo_db', 'schema': self.scope.schema('prod'),
                  'tables': [{'name': 'inputs', 'identifier': self.source.split('.')[-1]}]}]})
        self.save('project/tests/literal_outcome.sql',
                  "{{ config(store_failures=false, state={'compare_unrendered_code':false}, "
                  "severity='ERROR', fail_calc='count(*)', error_if='> 0', warn_if='> 0') }}\n"
                  "SELECT id, v FROM {{ source('cached','inputs') }} WHERE v < 0")

    def manifest_inputs(self, manifest, node):
        sources = manifest.get('sources', {})
        return (set(sources) == {self.source_uid} and node['depends_on']['nodes'] == [self.source_uid]
                and sources[self.source_uid]['database'] == 'cdo_demo_db'
                and sources[self.source_uid]['schema'] == self.scope.schema('prod')
                and sources[self.source_uid]['identifier'] == self.source.split('.')[-1])

    def request_inputs(self, req, speculative=False):
        tables = req.get('tables', [])
        # A speculative request may deliberately omit physical metadata and stay
        # undecided. Ordinary requests must carry the real source and known epoch.
        return (not req.get('query_dependencies') and len(tables) == 1
                and tables[0]['name'].replace('`', '') == self.source
                and (speculative or tables[0].get('last_modified_epoch') is not None)
                and req.get('freshness_tolerance_seconds') == 0)

    def assert_rows(self, actual, failures):
        schema = [(c.get('name'), c.get('type_text'), c.get('type_name'), c.get('position')) for c in actual['columns']]
        expected = [] if failures == 0 else [['1', '-2'], ['1', '-2']]
        if schema != [('id', 'INT', 'INT', 0), ('v', 'INT', 'INT', 1)] or Counter(map(tuple, actual['rows'])) != Counter(map(tuple, expected)):
            raise Refused('upstream:complete-typed-rows')

    def sql(self, label, statement):
        allowed = {
            f'CREATE TABLE {self.source} (id INT, v INT) USING DELTA',
            f'INSERT INTO {self.source} VALUES (1,2),(1,2),(2,NULL)',
            f'UPDATE {self.source} SET v=-2 WHERE id=1',
            f'UPDATE {self.source} SET v=2 WHERE id=1',
            f'DESCRIBE TABLE EXTENDED {self.source}', self.rendered_query(0),
            f'SELECT count(*) AS failures, count(*) > 0 AS should_error, count(*) > 0 AS should_warn FROM ({self.rendered_query(0)}) AS independently_declared',
        }
        if statement not in allowed:
            raise Refused('upstream:SQL-outside-exact-operation-plan')
        return Campaign.sql(self, label, statement)

    def check_client_sql(self, statement):
        normalized = re.sub(r'\s+', ' ', statement).strip()
        source_metadata = ("SELECT last_altered, (table_type = 'VIEW' OR table_type = 'MATERIALIZED_VIEW') AS is_view "
                           "FROM `cdo_demo_db`.INFORMATION_SCHEMA.TABLES "
                           f"WHERE table_schema = '{self.scope.schema('prod')}' AND table_name = '{self.source.split('.')[-1]}'")
        aggregate = ("select count(*) as failures, count(*) > 0 as should_warn, "
                     "count(*) > 0 as should_error from ( " + self.rendered_query(0) + " ) dbt_internal_test")
        if normalized not in (self.rendered_query(0), aggregate, source_metadata,
                              f'DESCRIBE HISTORY {self.quoted_source} LIMIT 1',
                              f'DESCRIBE EXTENDED {self.quoted_source}'):
            client_sql(statement, self.audit_schema)

    def query_logs(self, label):
        logs = [p / 'query_log.sql' for p in (self.work / 'logs').iterdir()
                if p.name.split('_', 1)[-1] == label and (p / 'query_log.sql').is_file()]
        if len(logs) != 1:
            raise Refused('upstream:missing-query-log')
        text = re.sub(r'/\*.*?\*/', '', logs[0].read_text(), flags=re.S)
        text = '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))
        for statement in (s.strip() for s in text.split(';') if s.strip()):
            self.check_client_sql(statement)

    def command(self, label, verb, managed, endpoint=None):
        result = super().command(label, verb, managed, endpoint)
        for row in result.calls:
            if (row['method'] == 'SubmitEnrichedSQLSpeculative'
                    and row['response']['response'] != {'undecided': {}}):
                raise Refused('upstream:unknown-epoch-speculation-must-stay-undecided')
        return result

    def check_build_explain(self, submit, messages):
        body = submit['response']['response']
        if 'ready_to_execute' not in body:
            return
        epoch = submit['request']['tables'][0]['last_modified_epoch']
        absolute = datetime.fromtimestamp(epoch / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        try:
            lines = messages[0]['explain_lines']
            text = lines[0]['children'][0]['children'][0]['children'][0]['text']
        except (KeyError, IndexError, TypeError):
            raise Refused('upstream:complete-BUILD-Explain') from None
        if not re.fullmatch(r'last updated: (?:now|.+ ago), ' + re.escape(absolute) + r' UTC', text):
            raise Refused('upstream:BUILD-Explain-relative-and-absolute-time')
        def line(text, children=()):
            return {'text': text, 'marker': None, 'badge': None, 'children': list(children)}
        expected = [line('data freshness analysis', [line('upstream dependencies',
                    [line(self.quoted_source, [line(text)])])])]
        if len(messages) != 1 or lines != expected:
            raise Refused('upstream:complete-BUILD-Explain')

    def explain(self, label, submits):
        super().explain(label, submits)
        rows = json.loads((self.work / 'wire' / (label + '_explain.json')).read_text())
        messages = next(row['response']['messages'] for row in rows if row['method'] == 'GetExplainMessages')
        self.check_build_explain(submits[0], messages)

    def verify_inventory(self, after):
        for schema, rows in self.before_inventory.items():
            if any(row not in after[schema] for row in rows):
                raise Refused('upstream:preexisting-inventory-changed')
            added = [row for row in after[schema] if row not in rows]
            if {row['full_name'] for row in added} != self.scope.allowed:
                raise Refused('upstream:unexpected-added-objects')


if __name__ == '__main__':
    raise SystemExit(main(UpstreamTests))
