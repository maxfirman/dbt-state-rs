#!/usr/bin/env python3
"""Bounded #144 genuine-source audit table/view histories; retain every object."""
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import uuid
from urllib.parse import quote

from databricks_audit import audit_client_sql
from databricks_baseline import LeaseScope
from databricks_cached_tests import CachedTests, CommandResult, main
from capture_integrity import verify_capture
from databricks_cached_upstream import UpstreamTests
from databricks_readonly_reference import show_reference, unique_object
from databricks_survey import Campaign, Refused

BASE = 'ds_20261112a134ef_ref'
AUDIT = BASE + '_dbt_test__audit'
SOURCE = 'ds_20261112a134ef_prod'
ALLOCATION = '7f365e7ca3bd4282f30491f102b68353a11edbc8ffd25e81e8dc85decaefb41f'


class AuditScope(LeaseScope):
    profile_roles = ('prod',)

    def __init__(self, args, lease):
        super().__init__(args.run_id, lease)
        owned = set()
        for evidence in lease['source_evidence']:
            owned.update(json.loads(Path(evidence['path']).read_text())['retained_schemas'])
        receipt = Path(args.audit_receipt)
        if (uuid.UUID(args.object_id).hex != args.object_id
                or not {BASE, SOURCE} <= owned
                or hashlib.sha256(receipt.read_bytes()).hexdigest() != ALLOCATION):
            raise Refused('audit:original-ownership-and-fresh-UUID')
        self.roles = {'prod': BASE}
        self.schemas = {BASE, SOURCE, AUDIT}
        self.relation_prefix = 'a144_' + args.object_id + '_'
        self.source = f'cdo_demo_db.{SOURCE}.{self.relation_prefix}inputs'
        self.allowed = {self.source} | {f'cdo_demo_db.{AUDIT}.{self.relation_prefix}{kind}' for kind in ('table', 'view')}


def reference_rows(stdout, stderr, direct, expected, sql, query_log, compiled):
    if stderr.strip():
        raise Refused('audit:reference-stderr')
    objects = json.loads(stdout, object_pairs_hook=unique_object)
    if not isinstance(objects, list) or any(list(row) != ['id', 'v'] or any(type(v) is not int for v in row.values()) for row in objects):
        raise Refused('audit:reference-types')
    if Counter(tuple(row.values()) for row in objects) != Counter(map(tuple, expected)):
        raise Refused('audit:reference-full-bag')
    normalize = lambda s: re.sub(r'\s+', ' ', s).strip()
    actual = '\n'.join(line for line in query_log.splitlines() if not line.startswith('--')).strip()
    if actual.count(';') != 1 or normalize(actual.removesuffix(';')) != normalize(sql) or normalize(compiled) != normalize(sql):
        raise Refused('audit:reference-exact-select')
    return {'full_bag': True, 'exact_select': True}


class AuditUpstream(UpstreamTests):
    @staticmethod
    def configure_parser(parser):
        parser.add_argument('--object-id', required=True)
        parser.add_argument('--audit-receipt', required=True)
        parser.add_argument('--resume-prefix')

    def build_scope(self, args):
        return AuditScope(args, self.lease)

    def __init__(self, args):
        self.kind = 'table'
        CachedTests.__init__(self, args)
        self.source = self.scope.source
        self.quoted_source = '.'.join('`' + part + '`' for part in self.source.split('.'))
        self.source_uid = 'source.cached_' + args.run_id + '.cached.inputs'
        self.runtimes = {}
        self.prefix = Path(args.resume_prefix) if args.resume_prefix else None
        if self.prefix:
            captures = list((self.prefix / 'golden').glob('*.jsonl'))
            if (len(captures) != 1 or hashlib.sha256(captures[0].read_bytes()).hexdigest() != '6269fe92090d81aa3ee10a4b73c3595459ed7fb3e05a9993cf8f5a0d358942ee'
                    or args.object_id != 'd4e2a030ec5f4b67902870349cb1fbff' or args.rust_url):
                raise Refused('audit:exact-hosted-prefix-required')
            entries = verify_capture(captures[0])
            for kind in ('table', 'view'):
                uid = 'test.cached_' + args.run_id + '_' + kind + '.literal_outcome'
                submits = [r for r in entries if r['method'] == 'SubmitEnrichedSQL' and r['request']['dbt_node_state']['node_unique_id'] == uid and 'ready_to_execute' in r['response']['response']]
                last = submits[-1]['response']['response']['ready_to_execute']['request_id']
                confirm = next(r for r in entries if r['method'] == 'ConfirmExecution' and r['request']['request_id'] == last)
                self.runtimes[kind] = confirm['request']['execution_runtime_ms']
            self.failed_source = True
            steps = type(self).plan()[4:]
            self.plan = lambda: steps
            self.save('retained-prefix.json', {'capture': str(captures[0]), 'sha256': hashlib.sha256(captures[0].read_bytes()).hexdigest(), 'retained_table_fail_command': True})
        (self.work / 'no-object-scope.json').unlink()
        self.save('audit-upstream-runner.py', Path(__file__).read_text())
        self.save('operation-manifest.json', {'owned': sorted(self.scope.allowed), 'managed': 12,
                  'compile': 14, 'readonly_references': 12, 'max_persistent': 3,
                  'source_mutations': ['CREATE', 'INSERT', 'UPDATE fail', 'UPDATE recover'],
                  'audit': 'native CREATE OR REPLACE only; exact aliases; table/view',
                  'cleanup': 'retain every object', 'plan': self.plan()})

    @staticmethod
    def plan():
        return [(kind + '_' + label, failures, decision)
                for label, failures, decision in UpstreamTests.plan() for kind in ('table', 'view')]

    @property
    def relation(self):
        return f'cdo_demo_db.{AUDIT}.{self.scope.relation_prefix}{self.kind}'

    def inventory(self):
        result = {}
        for schema in sorted(self.scope.schemas):
            rows, token, seen = [], None, set()
            while True:
                endpoint = '/api/2.1/unity-catalog/tables?catalog_name=cdo_demo_db&schema_name=' + schema
                if token:
                    endpoint += '&page_token=' + quote(token, safe='')
                response = self.api('get', endpoint)
                rows.extend({k: row[k] for k in ('name', 'full_name', 'table_type')} for row in response.get('tables', []))
                token = response.get('next_page_token')
                if not token:
                    break
                if token in seen:
                    raise Refused('audit:pagination-loop')
                seen.add(token)
            result[schema] = rows
        return result

    def audit_absent(self):
        schemas = {SOURCE: '59559b89-efa8-4ed7-b106-1394c2718cc3',
                   BASE: '0260bd48-0090-4c65-9b7f-aab522f3b6f3',
                   AUDIT: 'dcabff91-9a47-4ff0-9f53-2b71c06f414a'}
        evidence = {}
        for schema, schema_id in schemas.items():
            metadata = self.api('get', '/api/2.1/unity-catalog/schemas/cdo_demo_db.' + schema)
            if metadata.get('schema_id') != schema_id or metadata.get('owner') != 'max.firman@gmail.com':
                raise Refused('audit:owned-schema-identity')
            evidence[schema] = {k: metadata[k] for k in ('full_name', 'schema_id', 'owner')}
        self.save('schema-ownership-preflight.json', evidence)
        for schema, rows in self.before_inventory.items():
            additions = 2 if schema == AUDIT else 1 if schema == SOURCE else 0
            if (not self.prefix and additions and len(rows) + additions > 100) or (not self.prefix and any(row['full_name'] in self.scope.allowed for row in rows)):
                raise Refused('audit:collision-or-capacity')

    def setup_campaign(self):
        if self.prefix:
            if {row['full_name'] for rows in self.before_inventory.values() for row in rows if row['full_name'] in self.scope.allowed} != self.scope.allowed:
                raise Refused('audit:resume-three-existing-owned-objects')
            return
        from databricks_readonly_reference import reference_canary
        for kind in ('table', 'view'):
            self.kind = kind
            self.write_test(0)
            with reference_canary(self, kind + '_preflight') as endpoint:
                result = self.command(kind + '_preflight', 'compile', False, endpoint)
            if result.returncode:
                raise Refused('audit:preflight-before-DDL')
            self.manifest(result, 0)
        self.sql('create_source', f'CREATE TABLE {self.source} (id INT, v INT) USING DELTA')
        self.sql('initial_source', f'INSERT INTO {self.source} VALUES (1,2),(1,2),(2,NULL)')
        self.source_properties('initial')

    def before_cell(self, label):
        kind, stage = label.split('_', 1)
        self.kind = kind
        if kind == 'table' and not (self.prefix and label == 'table_data_fail'):
            UpstreamTests.before_cell(self, stage)
        if kind in self.runtimes:
            self.recorded_runtime = self.runtimes[kind]

    def write_test(self, failures):
        project = 'cached_' + self.a.run_id + '_' + self.kind
        self.uid = 'test.' + project + '.literal_outcome'
        self.source_uid = 'source.' + project + '.cached.inputs'
        self.save('project/dbt_project.yml', {'name': project, 'version': '1.0', 'config-version': 2, 'profile': 'survey'})
        self.save('project/models/sources.yml', {'version': 2, 'sources': [{'name': 'cached',
                  'database': 'cdo_demo_db', 'schema': SOURCE,
                  'tables': [{'name': 'inputs', 'identifier': self.source.split('.')[-1]}]}]})
        self.body = ("{{ config(store_failures=true, store_failures_as='" + self.kind + "', alias='"
                     + self.relation.split('.')[-1] + "', state={'compare_unrendered_code':false}, "
                     "severity='ERROR', fail_calc='count(*)', error_if='> 0', warn_if='> 0') }}\n"
                     "SELECT id, v FROM {{ source('cached','inputs') }} WHERE v < 0")
        self.save('project/tests/literal_outcome.sql', self.body)

    def manifest_inputs(self, manifest, node):
        sources = manifest.get('sources', {})
        return (set(sources) == {self.source_uid} and node['depends_on']['nodes'] == [self.source_uid]
                and sources[self.source_uid]['database'] == 'cdo_demo_db'
                and sources[self.source_uid]['schema'] == SOURCE
                and sources[self.source_uid]['identifier'] == self.source.split('.')[-1])

    def manifest(self, result, failures):
        manifest = json.loads((result.target / 'manifest.json').read_text())
        if set(manifest.get('nodes', {})) != {self.uid} or manifest.get('unit_tests'):
            raise Refused('audit:selected-singular-test')
        node = manifest['nodes'][self.uid]
        config = node['config']
        expected = dict(self.test_config(), store_failures=True, store_failures_as=self.kind,
                        materialized='test', alias=self.relation.split('.')[-1])
        if (not self.manifest_inputs(manifest, node) or node.get('raw_code') != self.body
                or node.get('resource_type') != 'test' or node.get('database') != 'cdo_demo_db'
                or node.get('schema') != AUDIT or node.get('alias') != expected['alias']
                or any(config.get(k) != v for k, v in expected.items())
                or config.get('state', {}).get('compare_unrendered_code') is not False
                or any(config.get(k) for k in ('pre-hook', 'post-hook', 'sql_header'))):
            raise Refused('audit:manifest-config-source-alias')
        compiled = node.get('compiled_code')
        if compiled is None:
            compiled = next(result.target.glob('compiled/**/literal_outcome.sql')).read_text()
        if compiled.strip() != self.rendered_query(failures):
            raise Refused('audit:compiled-exact-source')

    def semantic_extras(self):
        return dict(super().semantic_extras(), store_failures='true', store_failures_as=json.dumps(self.kind))

    def request_inputs(self, req, speculative=False):
        tables = req.get('tables', [])
        names = [row['name'].replace('`', '') for row in tables]
        if (req.get('query_dependencies') or names.count(self.source) != 1 or len(names) != len(set(names))
                or set(names) - {self.source, self.relation} or req.get('default_catalog') != 'cdo_demo_db'
                or req.get('default_schema') != AUDIT or req.get('freshness_tolerance_seconds') != 0):
            return False
        source = tables[names.index(self.source)]
        return speculative or (type(source.get('last_modified_epoch')) is int and source['last_modified_epoch'] > 0)

    def reference(self, label, failures):
        if self.prefix and label == 'reference_table_data_fail':
            retained = json.loads((self.prefix / 'readonly-results' / (label + '.json')).read_text())
            if retained != {'full_bag': True, 'exact_select': True, 'zero_reference_rpcs': True}:
                raise Refused('audit:retained-readonly-reference')
            return
        direct = self.sql(label + '_direct', self.rendered_query(failures))
        self.assert_rows(direct, failures)
        show_reference(self, label, self.rendered_query(failures), [] if failures == 0 else [[1, -2], [1, -2]], direct, reference_rows)

    def source_properties(self, label):
        source_rows = Campaign.sql(self, label + '_source_rows', f'SELECT * FROM {self.source}')
        value = '-2' if getattr(self, 'failed_source', False) else '2'
        expected = [['1', value], ['1', value], ['2', None]]
        schema = [(c['name'], c['type_text'], c['type_name'], c['position']) for c in source_rows['columns']]
        if schema != [('id', 'INT', 'INT', 0), ('v', 'INT', 'INT', 1)] or Counter(map(tuple, source_rows['rows'])) != Counter(map(tuple, expected)):
            raise Refused('audit:genuine-source-preserved')
        props = self.sql(label + '_source_properties', f'DESCRIBE TABLE EXTENDED {self.source}')
        properties = {row[0]: row[1] for row in props['rows']}
        if properties.get('Type') != 'MANAGED' or properties.get('Provider', '').lower() != 'delta':
            raise Refused('audit:source-managed-delta')

    def sql(self, label, statement):
        extras = {f'SELECT * FROM {self.relation}', f'DESCRIBE TABLE EXTENDED {self.relation}',
                  f'DESCRIBE HISTORY {self.relation}', f'SHOW CREATE TABLE {self.relation}'}
        if statement in extras:
            return Campaign.sql(self, label, statement)
        return UpstreamTests.sql(self, label, statement)

    def check_client_sql(self, statement):
        sql = re.sub(r'\s+', ' ', statement).strip().removesuffix(';')
        # Only one exact output and one exact source; schema creation is forbidden.
        quoted = '.'.join('`' + p + '`' for p in self.relation.split('.'))
        aggregate = 'select count(*) as failures, count(*) > 0 as should_warn, count(*) > 0 as should_error from ( select * from ' + quoted + ' ) dbt_internal_test'
        ddl = {f'create or replace {self.kind} {quoted} as {self.rendered_query(0)}',
               f'create or replace {self.kind} {quoted} as ( {self.rendered_query(0)} )',
               f'create or replace table {quoted} using delta as {self.rendered_query(0)}'}
        if sql.lower() in {s.lower() for s in ddl | {aggregate, f'drop {self.kind} if exists {quoted}'}}:
            return
        for relation in (self.source, self.relation):
            catalog, schema, name = relation.split('.')
            q = '.'.join('`' + p + '`' for p in relation.split('.'))
            metadata = ("SELECT last_altered, (table_type = 'VIEW' OR table_type = 'MATERIALIZED_VIEW') AS is_view "
                        "FROM `cdo_demo_db`.INFORMATION_SCHEMA.TABLES "
                        f"WHERE table_schema = '{schema}' AND table_name = '{name}'")
            if sql in {metadata, f'DESCRIBE HISTORY {q} LIMIT 1', f'DESCRIBE EXTENDED {q}'}:
                return
        if ('information_schema' in sql.lower() or sql == 'SHOW SCHEMAS IN `cdo_demo_db`'):
            audit_client_sql(sql, self.relation)
            return
        if sql == self.rendered_query(0):
            return
        raise Refused('audit:native-SQL-outside-exact-source-and-output:' + sql)

    def retained_result(self, label):
        paths = list((self.prefix / 'targets').glob('*_' + label))
        if len(paths) != 1:
            raise Refused('audit:exact-retained-command')
        prefix = paths[0].name
        events = json.loads((self.prefix / 'operations.json').read_text())
        results = [r for r in events if r['kind'] == 'dbt-result' and r['label'] == label]
        if len(results) != 1:
            raise Refused('audit:retained-command-event')
        calls = json.loads((self.prefix / 'wire' / (prefix + '.json')).read_text())
        output = (self.prefix / 'dbt' / (prefix + '.log')).read_text()
        return CommandResult(results[0]['returncode'], output, paths[0], calls)

    def command(self, label, verb, managed, endpoint=None):
        if self.prefix and label in ('table_data_fail_compile', 'table_data_fail'):
            return self.retained_result(label)
        return super().command(label, verb, managed, endpoint)

    def query_logs(self, label):
        if self.prefix and label == 'table_data_fail':
            original = self.work
            try:
                self.work = self.prefix
                return super().query_logs(label)
            finally:
                self.work = original
        return super().query_logs(label)

    def check_build_explain(self, submit, messages):
        response = submit['response']['response']
        body = next(iter(response.values()))
        if (len(messages) != 1 or messages[0]['execution_decision_id'] != body['execution_decision_id']
                or messages[0]['decision'] != body['explained_decision']['decision']
                or messages[0]['decision_description'] != body['explained_decision']['decision_description']):
            raise Refused('audit:complete-Explain-decision')
        if 'ready_to_execute' in response:
            return super().check_build_explain(submit, messages)
        def line(text, children=(), marker=None, badge=None):
            return dict(text=text, marker=marker, badge=badge, children=list(children))
        try:
            updated = messages[0]['explain_lines'][1]['children'][0]['children'][0]['children'][1]['text']
        except (KeyError, IndexError, TypeError):
            raise Refused('audit:complete-SKIP-Explain') from None
        epoch = next(t['last_modified_epoch'] for t in submit['request']['tables'] if t['name'].replace('`', '') == self.source)
        absolute = datetime.fromtimestamp(epoch / 1000, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        if not re.fullmatch(r'last updated: (?:now|.+ ago), ' + re.escape(absolute) + r' UTC', updated):
            raise Refused('audit:SKIP-Explain-relative-absolute-time')
        expected = [line('query analysis', [line('the test query has not changed', marker=1, badge=5),
                    line('upstream model queries have not changed', marker=1)]),
                    line('data freshness analysis', [line('upstream dependencies',
                    [line(self.quoted_source, [line('no updates since test last executed'), line(updated)], badge=1)]),
                    line('upstream data is up to date', marker=1)])]
        if messages[0]['explain_lines'] != expected:
            raise Refused('audit:complete-SKIP-Explain')

    def cell(self, label, failures, decision):
        self.active_label = label
        if label == 'table_data_fail':
            self.failed_source = True
        elif label == 'table_data_recover':
            self.failed_source = False
        super().cell(label, failures, decision)
        self.runtimes[self.kind] = self.recorded_runtime
        actual = self.sql(label + '_audit_rows', f'SELECT * FROM {self.relation}')
        self.assert_rows(actual, failures)  # View rows are live, independent of cached outcome.
        props = self.sql(label + '_audit_properties', f'DESCRIBE TABLE EXTENDED {self.relation}')
        metadata = self.api('get', '/api/2.1/unity-catalog/tables/' + self.relation)
        properties = {row[0]: row[1] for row in props['rows']}
        expected_type = 'MANAGED' if self.kind == 'table' else 'VIEW'
        if (properties.get('Type') != expected_type or metadata.get('table_type') != expected_type
                or metadata.get('full_name') != self.relation
                or (self.kind == 'table' and (properties.get('Provider', '').lower() != 'delta' or metadata.get('data_source_format') != 'DELTA'))):
            raise Refused('audit:physical-kind-schema-provider')
        history = self.sql(label + '_audit_definition', f'DESCRIBE HISTORY {self.relation}' if self.kind == 'table' else f'SHOW CREATE TABLE {self.relation}')
        self.check_audit_definition(metadata, history)
        self.save('physical/' + label + '.json', {'rows': actual, 'properties': props, 'metadata': metadata, 'history_or_definition': history})
        self.source_properties(label)

    def check_audit_definition(self, metadata, history):
        if self.kind == 'view':
            definition = re.sub(r'\s+', ' ', metadata.get('view_definition', '')).strip()
            if definition.startswith('(') and definition.endswith(')'):
                definition = definition[1:-1].strip()
            if definition != self.rendered_query(0):
                raise Refused('audit:exact-live-view-definition')
            dependencies = metadata.get('view_dependencies', {}).get('dependencies', [])
            if len(dependencies) != 1 or dependencies[0].get('table', {}).get('table_full_name') != self.source:
                raise Refused('audit:live-view-genuine-source')
            if not history['rows'] or self.source.split('.')[-1] not in history['rows'][0][0]:
                raise Refused('audit:SHOW-CREATE-live-source')
        else:
            names = [column['name'] for column in history['columns']]
            if not {'version', 'timestamp', 'operation'} <= set(names) or not history['rows']:
                raise Refused('audit:complete-Delta-history')
            if any(row[names.index('version')] is None or row[names.index('timestamp')] is None
                   or row[names.index('operation')] is None for row in history['rows']):
                raise Refused('audit:complete-Delta-history-fields')

    def verify_inventory(self, after):
        added = set()
        for schema, rows in self.before_inventory.items():
            if any(row not in after[schema] for row in rows):
                raise Refused('audit:earlier-inventory-changed')
            added.update(row['full_name'] for row in after[schema] if row not in rows)
        existing = {row['full_name'] for rows in self.before_inventory.values() for row in rows if row['full_name'] in self.scope.allowed}
        if added != self.scope.allowed - existing:
            raise Refused('audit:exact-three-new-objects-required')


if __name__ == '__main__':
    raise SystemExit(main(AuditUpstream))
