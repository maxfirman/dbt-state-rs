#!/usr/bin/env python3
"""Opt-in bounded #144 source-less native audit characterization, not support.

Requires a coordinator's exact-name live clearance. Hosted first, strict public
router replay next, independently fresh Rust last. No Snowflake policy inference.
"""
import hashlib
import json
from pathlib import Path
import re
import shutil
import os
import subprocess
import time
import sys

from capture_integrity import verify_capture
from databricks_baseline import Baseline
from databricks_cached_tests import (CachedTests, assert_literal_rows, assert_payload,
                                     main, query, select_only, CommandResult)
from databricks_readonly_reference import reference_canary
from databricks_survey import Campaign, Refused, same_result

SCHEMA = 'ds_20261112a134ef_ref_dbt_test__audit'
BASE = 'ds_20261112a134ef_ref'
NAMES = {'20261114a144aa': ('sf_20261114a144aa', 'sf_20261114a144ab'),
         '20261114b144bb': ('sf_20261114b144bb', 'sf_20261114b144bc')}


def default_name_authorization(grant, receipt, receipt_sha256, run, rust):
    expected = {'cdo_demo_db.' + SCHEMA + '.' + n for n in NAMES['20261114a144aa']}
    if (run != '20261114a144aa' or rust
            or grant.get('status') != 'CLEARED exact144default-name hosted four-cell control'
            or grant.get('schema_id') != 'dcabff91-9a47-4ff0-9f53-2b71c06f414a'
            or sorted(grant.get('exact_permanent_objects', [])) != sorted(expected)
            or grant.get('schema') != 'cdo_demo_db.' + SCHEMA
            or grant.get('managed_project') != 'audit_default_20261114a144ad'
            or grant.get('reference_project') != 'audit_default_ref_20261114a144ad'
            or grant.get('body_nonces') != ['20261114a144adtablea', '20261114a144adtableb']
            or grant.get('limits') != {'managed': 4, 'compile': 4, 'disabled_references': 4,
                                       'max_permanent': 2, 'extra_persistent': 0}
            or grant.get('retirement_receipt_sha256') != receipt_sha256
            or receipt.get('status') != 'RETIRED exact144aaab owned outputs for default-name control'
            or sorted(receipt.get('exact_objects', [])) != sorted(expected)
            or receipt.get('metadata_absent') is not True
            or receipt.get('protected_inventory_preserved') is not True):
        raise Refused('guard:exact-default-name-retirement-and-hosted-clearance-required')


def rust_error_authorization(grant, run, contract_sha256):
    mode = grant.get('mode')
    counts = {'alias5': 5, 'default3': 3}
    fixture_hashes = {'alias5': '6a32d653285fbaf225d2f08f41cde8097eb52527a4fecae523c9abd01d3dca92',
                      'default3': '19f2c1c39fa5e15a8c3ef7951c9a103174dc8703208d6c8531df6e022ea2a4e1'}
    expected = {'cdo_demo_db.' + SCHEMA + '.' + n for n in NAMES['20261114b144bb']}
    if (mode not in counts or run != '20261114b144bb'
            or grant.get('status') != 'CLEARED exact144Rust expected-error ' + mode
            or grant.get('hosted_replay_success') is not True
            or grant.get('hosted_causal_fixture_sha256') != fixture_hashes[mode]
            or grant.get('schema_id') != 'dcabff91-9a47-4ff0-9f53-2b71c06f414a'
            or sorted(grant.get('exact_permanent_objects', [])) != sorted(expected)
            or grant.get('contract_sha256') != contract_sha256
            or grant.get('limits') != {'managed': counts[mode], 'compile': counts[mode],
                                       'disabled_references': counts[mode], 'max_permanent': 2,
                                       'extra_persistent': 0}):
        raise Refused('guard:separate-exact-Rust-expected-error-clearance-required')
    return mode


def assert_fallback_calls(calls):
    """Only the captured final-cell ordinary error can explain native fallback."""
    expected_error = {'code': 3, 'message': 'Failed to process SQL', 'details': [],
                      'metadata': {'ascii': [], 'binary': []}}
    requests = [r for r in calls if r['method'] in ('SubmitEnrichedSQLSpeculative', 'SubmitEnrichedSQL')]
    errors = [r for r in calls if (r.get('status') or {}).get('code') != 0]
    if (len(requests) != 2 or [r['method'] for r in requests] != ['SubmitEnrichedSQLSpeculative', 'SubmitEnrichedSQL']
            or requests[0].get('response') != {'response': {'undecided': {}}}
            or len(errors) != 1 or errors[0] is not requests[1]
            or errors[0].get('status') != expected_error or errors[0].get('response') is not None
            or any(r['method'] in ('ConfirmExecution', 'RecordExecutions', 'SubmitValues', 'RegisterClone') for r in calls)):
        raise Refused('wire:exact-expected-ordinary-error-without-published-result')
    speculative, ordinary = [r['request'] for r in requests]
    changed = {k for k in speculative.keys() | ordinary.keys() if speculative.get(k) != ordinary.get(k)}
    tables = ordinary.get('tables', [])
    if (changed != {'tables'} or speculative.get('tables') != [] or len(tables) != 1
            or type(tables[0].get('last_modified_epoch')) is not int
            or tables[0]['last_modified_epoch'] <= 0):
        raise Refused('wire:expected-error-only-adds-known-audit-evidence')
    return requests[1]


def assert_managed_audit_tables(tables, relation):
    """Owned reference outputs never count as the managed node's audit role."""
    for table in tables:
        parts = table['name'].split('.')
        canonical = []
        for part in parts:
            if part.startswith('`') or part.endswith('`'):
                if not (part.startswith('`') and part.endswith('`') and part.count('`') == 2):
                    raise Refused('wire:unknown-audit-name-quoting')
                part = part[1:-1]
            if not re.fullmatch(r'[a-z_][a-z0-9_]*', part):
                raise Refused('wire:unknown-audit-name-component')
            canonical.append(part)
        if len(canonical) != 3 or '.'.join(canonical) != relation:
            raise Refused('wire:reported-table-is-not-exact-managed-audit')


def assert_audit(rows, properties, metadata, relation, kind, failures):
    assert_literal_rows(rows, failures)
    props = dict(properties['rows'])
    expected = 'MANAGED' if kind == 'table' else 'VIEW'
    if (props.get('Type') != expected or metadata.get('full_name') != relation
            or metadata.get('table_type') != expected
            or (kind == 'table' and (props.get('Provider', '').lower() != 'delta'
                                    or metadata.get('data_source_format') != 'DELTA'))):
        raise Refused('oracle:audit-identity-kind-provider')


def audit_client_sql(statement, relation):
    """Native test output SQL and exact scoped primary metadata queries only."""
    sql = re.sub(r'\s+', ' ', statement).strip().removesuffix(';')
    names = re.findall(r'`?([a-z_][a-z0-9_]*)`?\s*\.\s*`?([a-z_][a-z0-9_]*)`?\s*\.\s*`?([a-z_][a-z0-9_]*)`?', sql, flags=re.I)
    if re.search(r'(?i)\b(insert|update|delete|merge|copy|alter)\b', sql):
        raise Refused('guard:unexpected-native-mutation')
    for catalog, schema, name in names:
        if schema.lower() == 'information_schema':
            if (not re.match(r'(?i)^select\b', sql) or catalog not in ('system', 'cdo_demo_db')
                    or name.lower() not in ('tables', 'row_filters')
                    or re.search(r'(?i)\b(union|join)\b', sql)
                    or re.search(r'(?i)\b(or|union|join)\b', re.split(r'(?i)\bwhere\b', sql)[-1])):
                raise Refused('guard:unexpected-metadata-statement')
            where = re.split(r'(?i)\bwhere\b', sql)
            if len(where) != 2:
                raise Refused('guard:missing-metadata-predicates')
            predicates = {p.strip().lower() for p in re.split(r'(?i)\s+and\s+', where[1])}
            expected = {"table_schema = '" + SCHEMA + "'"}
            if catalog == 'system' or name.lower() == 'row_filters':
                expected.add("table_catalog = 'cdo_demo_db'")
            if name.lower() == 'row_filters' or catalog != 'system':
                expected.add("table_name = '" + relation.rsplit('.', 1)[-1] + "'")
            if predicates != expected or (catalog == 'system' and name.lower() != 'tables'):
                raise Refused('guard:metadata-catalog-schema-table-predicates')
        elif '.'.join((catalog, schema, name)) != relation:
            raise Refused('guard:outside-active-audit-output')
    if re.match(r'(?i)^show schemas in `cdo_demo_db`$', sql):
        return
    if re.match(r'(?i)^create schema if not exists ', sql):
        if sql not in [f'CREATE SCHEMA IF NOT EXISTS `cdo_demo_db`.`{schema}`' for schema in (BASE, SCHEMA)]:
            raise Refused('guard:unexpected-native-schema')
        return
    if not names:
        select_only(sql)
    if not re.match(r'(?i)^(select|describe history|create (or replace )?(table|view)|drop (table|view))\b', sql):
        raise Refused('guard:unexpected-native-statement')


class AuditScope:
    profile_roles = ('prod',)

    def __init__(self, run, clearance):
        if (run not in NAMES or clearance.get('allocation', {}).get('schema') != 'cdo_demo_db.' + SCHEMA
                or clearance.get('allocation', {}).get('schema_id') != 'dcabff91-9a47-4ff0-9f53-2b71c06f414a'
                or clearance.get('status') != 'LIVE CLEARED bounded #144 source-less audit partial'):
            raise Refused('guard:exact-live-clearance-required')
        self.allowed = {'cdo_demo_db.' + SCHEMA + '.' + n for n in NAMES[run]}
        if not self.allowed <= {'cdo_demo_db.' + SCHEMA + '.' + n for n in clearance.get('exact_permanent_objects', [])}:
            raise Refused('guard:exact-objects-not-granted')
        self.schemas = {BASE, 'ds_20261112e134ef_ref', SCHEMA}

    def schema(self, role):
        if role != 'prod':
            raise Refused('guard:unexpected-profile-role')
        return BASE

    def check(self, value):
        if value.replace('`', '') not in self.allowed:
            raise Refused('guard:outside-owned-audit-output:' + value)
        return value


class AuditTests(Campaign):
    inventory = Baseline.inventory
    explain = Baseline.explain
    outcome = CachedTests.outcome

    @staticmethod
    def configure_parser(parser):
        parser.add_argument('--live-clearance', required=True)
        parser.add_argument('--continuation-clearance')
        parser.add_argument('--default-name-clearance', help='separate hosted-only four-cell grant and hashed retirement receipt')
        parser.add_argument('--rust-replay-clearance', help='separate coordinator clearance after complete green hosted replay')

    def __init__(self, args):
        self.expected_error_mode, self.rust_grant = None, None
        if args.rust_url:
            if not args.rust_replay_clearance or not args.contract:
                raise Refused('guard:Rust-held-until-exact-expected-error-clearance')
            self.rust_grant = json.loads(Path(args.rust_replay_clearance).read_text())
            self.expected_error_mode = rust_error_authorization(self.rust_grant, args.run_id,
                                        hashlib.sha256(Path(args.contract).read_bytes()).hexdigest())
            if args.default_name_clearance:
                raise Refused('guard:hosted-default-grant-cannot-authorize-Rust')
            process = json.loads((Path(args.work_dir).parent / 'running-server-process.json').read_text())
            if (args.rust_url != 'http://' + process['address']
                    or hashlib.sha256(Path(process['executable']).read_bytes()).hexdigest()
                       != self.rust_grant.get('server_executable_sha256')):
                raise Refused('guard:actual-Rust-process-does-not-match-frozen-clearance')
            if self.expected_error_mode == 'default3':
                receipt_bytes = Path(self.rust_grant['retirement_receipt']).read_bytes()
                retired = json.loads(receipt_bytes)
                expected = {'cdo_demo_db.' + SCHEMA + '.' + n for n in NAMES['20261114b144bb']}
                if (hashlib.sha256(receipt_bytes).hexdigest() != self.rust_grant['retirement_receipt_sha256']
                        or retired.get('status') != 'RETIRED exact144bbbc owned outputs for default-name control'
                        or sorted(retired.get('exact_objects', [])) != sorted(expected)
                        or retired.get('metadata_absent') is not True
                        or retired.get('protected_inventory_preserved') is not True):
                    raise Refused('guard:exact-Rust-default-retirement-required')
        self.default_mode = bool(args.default_name_clearance) or self.expected_error_mode == 'default3'
        self.default_grant = None
        if args.default_name_clearance:
            self.default_grant = json.loads(Path(args.default_name_clearance).read_text())
            receipt_bytes = Path(self.default_grant['retirement_receipt']).read_bytes()
            default_name_authorization(self.default_grant, json.loads(receipt_bytes),
                                       hashlib.sha256(receipt_bytes).hexdigest(), args.run_id, bool(args.rust_url))
            if args.continuation_clearance:
                raise Refused('guard:default-name-is-fresh-history-not-continuation')
        clearance = json.loads(Path(args.live_clearance).read_text())
        scope = AuditScope(args.run_id, clearance)  # Before profiles/client/API access.
        if args.query_identity:
            raise Refused('guard:each-backend-requires-fresh-body-and-rendered-nonce')
        super().__init__(args, scope)
        self.clearance = clearance
        self.continuation = json.loads(Path(args.continuation_clearance).read_text()) if args.continuation_clearance else None
        self.prior = None
        if self.continuation:
            if (args.run_id != '20261114a144aa' or self.continuation.get('status') != 'CLEARED exact144hosted continuation after harmless scoped metadata guard stop'):
                raise Refused('guard:unexpected-continuation')
            self.prior = Path(self.continuation['original_failed_campaign'])
            if hashlib.sha256((self.prior / 'summary.json').read_bytes()).hexdigest() != self.continuation['original_summary_sha256']:
                raise Refused('guard:original-failure-changed')
            for label in ('table_fail_first_compile', 'table_fail_first_reference'):
                directory = self.prior / 'readonly-canaries' / label
                if json.loads((directory / 'certification.json').read_text()) != {'complete': True, 'rpcs': 0}:
                    raise Refused('guard:prior-disabled-reference-canary')
                captures = list((directory / 'golden').glob('*.jsonl'))
                if len(captures) != 1 or verify_capture(captures[0]):
                    raise Refused('guard:prior-reference-sent-rpc')
            self.save('continuation-clearance.json', self.continuation)
            self.save('prior-artifact-hashes.json', {str(p.relative_to(self.prior)): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in self.prior.rglob('*') if p.is_file()})
        if self.default_grant is not None:
            self.save('default-name-clearance.json', self.default_grant)
            self.save('retirement-receipt.json', json.loads(receipt_bytes))
        if self.rust_grant is not None:
            self.save('Rust-expected-error-clearance.json', self.rust_grant)
            if self.expected_error_mode == 'default3':
                self.save('retirement-receipt.json', json.loads(receipt_bytes))
        self.cells, self.recorded = [], {}
        self.fallback_mode = False
        self.last_test_path = None
        self.contract = json.loads(Path(args.contract).read_text()) if args.contract else None
        self.active, self.kind, self.failures = 'fail_first', 'table', 3
        self.reference_mode = False
        for name in ('dbt', 'proxy', 'probe'):
            owned = self.work / 'bin' / name
            owned.parent.mkdir(exist_ok=True)
            shutil.copy2(getattr(args, name), owned)
            setattr(args, name, str(owned))
        self.save('audit-runner.py', Path(__file__).read_text())
        self.save('audit-live-clearance.json', clearance)
        self.save('audit-provenance.json', {'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  'dbt_sha256': hashlib.sha256(Path(args.dbt).read_bytes()).hexdigest(),
                  'proxy_sha256': hashlib.sha256(Path(args.proxy).read_bytes()).hexdigest(),
                  'probe_sha256': hashlib.sha256(Path(args.probe).read_bytes()).hexdigest()})
        self.save('operation-manifest.json', {'steps': self.plan(), 'objects': sorted(scope.allowed),
                  'max_permanent_objects_this_phase': 2, 'expected_auxiliary_objects': [],
                  'rows_R': [[1, 2], [1, 2], [2, None]], 'R': [4, True, True], 'F_pass': [0, False, False]})

    def command(self, label, verb, managed, endpoint=None):
        self.n += 1
        target = self.work / 'targets' / f'{self.n:03d}_{label}'
        logs = self.work / 'logs' / f'{self.n:03d}_{label}'
        argv = [self.a.dbt, verb, '--select', self.node_name, '--target', 'prod',
                '--profiles-dir', str(self.profiles), '--profile', 'survey',
                '--target-path', str(target), '--log-path', str(logs),
                '--no-version-check', '--skip-semantic-manifest-validation',
                '--manage-state' if managed else '--no-manage-state', *self.command_variables()]
        env = dict(self.env)
        if endpoint:
            env['RUN_CACHE_API_URL'] = endpoint
        lo = len(self.wire())
        self.event('dbt', label=label, argv=argv, managed=managed)
        print('dbt:', label, flush=True)
        process = subprocess.Popen(argv, cwd=self.project, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        proc = Path('/proc') / str(process.pid)
        self.save(f'process/{self.n:03d}_{label}.json', {
            'pid': process.pid, 'argv': argv, 'exe': os.readlink(proc / 'exe'),
            'actual_cmdline': (proc / 'cmdline').read_bytes().decode().split('\0'),
            'sha256': hashlib.sha256(Path(self.a.dbt).read_bytes()).hexdigest()})
        try:
            stdout, stderr = process.communicate(timeout=180)
            code = process.returncode
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            code = 124
        output = self.clean(stdout + stderr)
        self.save(f'dbt/{self.n:03d}_{label}.log', output)
        for path in logs.rglob('*'):
            if path.is_file():
                try:
                    path.write_text(self.clean(path.read_text()))
                except UnicodeError:
                    pass
        time.sleep(.1)
        calls = self.wire()[lo:]
        self.save(f'wire/{self.n:03d}_{label}.json', calls)
        self.event('dbt-result', label=label, returncode=code, calls=len(calls))
        if verb == 'test':
            self.query_logs(logs, label)
        return CommandResult(code, output, target, calls)

    def query_logs(self, logs, label):
        path = logs / 'query_log.sql'
        if not path.is_file():
            raise Refused('guard:missing-native-query-log')
        sql = re.sub(r'/\*.*?\*/', '', path.read_text(), flags=re.S)
        sql = '\n'.join(line for line in sql.splitlines() if not line.lstrip().startswith('--'))
        for statement in sql.split(';'):
            if statement.strip():
                audit_client_sql(statement, self.relation)
        self.save('native-query-guards/' + label + '.json', {'exact_phase_outputs_only': True, 'sql': sql})

    def plan(self):
        if self.expected_error_mode:
            prefix = [('table', 'fail_first', 3), ('table', 'fail_repeat', 3)]
            if self.expected_error_mode == 'alias5':
                prefix += [('table', 'render_pass', 0), ('table', 'render_pass_repeat', 0)]
            return prefix + [('table', 'body_pass_first', 0)]
        if self.default_mode:
            return [('table', label, rows) for label, rows in
                    [('fail_first', 3), ('fail_repeat', 3), ('body_pass_first', 0), ('body_pass_repeat', 0)]]
        return [(kind, label, failures) for kind in ('table', 'view') for label, failures in
                [('fail_first', 3), ('fail_repeat', 3), ('render_pass', 0),
                 ('render_pass_repeat', 0), ('body_pass_first', 0), ('body_pass_repeat', 0)]]

    @property
    def node_name(self):
        return self.alias if self.default_mode else 'literal_outcome'

    @property
    def alias(self):
        return NAMES[self.a.run_id][int(self.reference_mode)]

    @property
    def relation(self):
        return self.scope.check('cdo_demo_db.' + SCHEMA + '.' + self.alias)

    def test_config(self):
        return {'severity': 'ERROR', 'error_if': '> 0', 'warn_if': '> 0',
                'fail_calc': 'coalesce(sum(v), cast(0 as bigint))'}

    def expected_result(self, rows):
        return (4, True, True) if rows == 3 else (0, False, False)

    def expected_cached_result(self, rows):
        return self.expected_result(rows if self.fallback_mode else self.cached_rows)

    def rendered_query(self, rows):
        nonce = (self.default_identity if self.default_mode else self.a.run_id) + self.kind + ('b' if self.active.startswith('body_pass') else 'a')
        return query(nonce, rows)

    def command_variables(self):
        return ['--vars', json.dumps({'rows_enabled': self.failures == 3})]

    @property
    def default_identity(self):
        return '20261114b144bd' if self.expected_error_mode == 'default3' else '20261114a144ad'

    def write_test(self):
        if self.default_mode:
            project = ('audit_default_ref_' if self.reference_mode else 'audit_default_') + self.default_identity
        else:
            project = ('audit_ref_' if self.reference_mode else 'audit_') + self.a.run_id
        self.uid = 'test.' + project + '.' + self.node_name
        self.save('project/dbt_project.yml', {'name': project, 'version': '1.0', 'config-version': 2, 'profile': 'survey'})
        body = self.rendered_query(3).replace('WHERE 1 = 1', "WHERE {{ 1 if var('rows_enabled') else 0 }} = 1")
        alias_config = '' if self.default_mode else ", alias='" + self.alias + "'"
        self.body = ("{{ config(store_failures=true, store_failures_as='" + self.kind + "'" + alias_config
                     + ", state={'compare_unrendered_code':true}, severity='ERROR', "
                     "fail_calc='coalesce(sum(v), cast(0 as bigint))', error_if='> 0', warn_if='> 0') }}\n" + body)
        test_path = 'tests/' + self.node_name + '.sql'
        if self.last_test_path is not None and self.last_test_path != test_path:
            (self.project / self.last_test_path).unlink()
        self.save('project/' + test_path, self.body)
        self.last_test_path = test_path
        if sorted(p.relative_to(self.project).as_posix() for p in self.project.rglob('*') if p.is_file()) != ['dbt_project.yml', test_path]:
            raise Refused('guard:extra-project-inputs')

    def manifest(self, result):
        manifest = json.loads((result.target / 'manifest.json').read_text())
        if set(manifest.get('nodes', {})) != {self.uid} or manifest.get('sources') or manifest.get('unit_tests'):
            raise Refused('guard:selected-source-less-singular-test')
        node = manifest['nodes'][self.uid]
        config = node['config']
        if (node.get('resource_type') != 'test' or node.get('depends_on', {}).get('nodes')
                or config.get('store_failures') is not True or config.get('store_failures_as') != self.kind
                or config.get('state', {}).get('compare_unrendered_code') is not True
                or config.get('materialized') != 'test' or node.get('database') != 'cdo_demo_db'
                or node.get('schema') != SCHEMA or node.get('alias') != self.alias
                or node.get('name') != self.node_name
                or (self.default_mode and config.get('alias') is not None)
                or node.get('raw_code') != self.body
                or any(config.get(k) != v for k, v in self.test_config().items())
                or any(config.get(k) for k in ('pre-hook', 'post-hook', 'sql_header'))):
            raise Refused('guard:native-audit-manifest')
        compiled = node.get('compiled_code')
        if compiled is None:
            compiled = next(result.target.glob('compiled/**/' + self.node_name + '.sql')).read_text()
        if select_only(compiled) != self.rendered_query(self.failures):
            raise Refused('guard:compiled-source-less-query')

    def sql(self, label, statement):
        # Driver can read only; every mutating operation comes from native dbt.
        if not statement.startswith(('SELECT ', 'DESCRIBE ')):
            raise Refused('guard:no-driver-ddl-dml')
        return super().sql(label, statement)

    def aggregate(self, label, sql, rows):
        calc = self.test_config()['fail_calc']
        actual = self.sql(label, f'SELECT {calc} AS failures, {calc} > 0 AS should_error, {calc} > 0 AS should_warn FROM (' + sql + ') AS independently_declared')
        expected = self.expected_result(rows)
        schema = [(c.get('name'), c.get('type_text'), c.get('type_name'), c.get('position')) for c in actual['columns']]
        if (schema != [('failures', 'BIGINT', 'LONG', 0), ('should_error', 'BOOLEAN', 'BOOLEAN', 1), ('should_warn', 'BOOLEAN', 'BOOLEAN', 2)]
                or actual['rows'] != [[str(expected[0]), str(expected[1]).lower(), str(expected[2]).lower()]]):
            raise Refused('oracle:independent-aggregate')
        return actual

    def physical(self, label, rows):
        relation = self.relation
        actual = self.sql(label + '_rows', 'SELECT * FROM ' + relation)
        props = self.sql(label + '_properties', 'DESCRIBE TABLE EXTENDED ' + relation)
        metadata = self.api('get', '/api/2.1/unity-catalog/tables/' + relation)
        self.save('physical/' + label + '.json', {'rows': actual, 'properties': props, 'metadata': metadata})
        # DESCRIBE rows may contain a third comment column and section markers.
        property_pairs = {'rows': [r[:2] for r in props['rows'] if len(r) >= 2]}
        assert_audit(actual, property_pairs, metadata, relation, self.kind, rows)
        aggregate = self.aggregate(label + '_aggregate', 'SELECT * FROM ' + relation, rows)
        return {'rows': actual, 'properties': props, 'metadata': metadata, 'aggregate': aggregate}

    def inspect_calls(self, result):
        expected_fallback = self.expected_error_mode is not None and self.active == 'body_pass_first'
        fallback = assert_fallback_calls(result.calls) if expected_fallback else None
        if not expected_fallback and any((r.get('status') or {}).get('code') != 0 for r in result.calls):
            raise Refused('wire:unexpected-grpc-status')
        requests = [r for r in result.calls if r['method'] in ('SubmitEnrichedSQL', 'SubmitEnrichedSQLSpeculative')]
        selected = [r for r in requests if r['method'] == 'SubmitEnrichedSQL'] or [r for r in requests if 'skip_execution' in r['response']['response']]
        if len(selected) != 1:
            raise Refused('wire:exact-one-test-decision')
        for row in requests:
            req = row['request']
            extras = {k: json.dumps(v) for k, v in self.test_config().items()}
            extras.update(store_failures='true', store_failures_as=json.dumps(self.kind))
            if (req.get('execution_type') != 8 or req.get('target_table') is not None
                    or req.get('compare_unrendered_code') is not True
                    or req.get('default_catalog') != 'cdo_demo_db' or req.get('default_schema') != SCHEMA
                    or req.get('semantic_extras') != extras
                    or req.get('dbt_node_state', {}).get('node_unique_id') != self.uid
                    or req.get('labels', {}).get('dbt_node_unique_id') != self.uid
                    or not req.get('table_namespace')):
                raise Refused('wire:actual-audit-identity')
            # Preserve reported audit evidence verbatim; do not classify it as an input/output policy.
            for table in req.get('tables', []):
                self.scope.check(table['name'])
            assert_managed_audit_tables(req.get('tables', []), self.relation)
        row = selected[0]
        if fallback is not None:
            if self.kind not in self.recorded:
                raise Refused('wire:expected-fallback-requires-confirmed-R')
            self.cached_rows = self.recorded[self.kind]['rows']
            return fallback, 'invalid_argument_fallback', None
        response = row['response']['response']
        if len(response) != 1 or next(iter(response)) not in ('ready_to_execute', 'skip_execution'):
            raise Refused('wire:unexpected-audit-decision')
        decision = next(iter(response))
        body = response[decision]
        if decision == 'ready_to_execute':
            confirms = [r for r in result.calls if r['method'] == 'ConfirmExecution' and r['request']['request_id'] == body['request_id'] and r['response'].get('success') is True]
            if len(confirms) != 1:
                raise Refused('wire:successful-computation-confirm-required')
            cached = confirms[0]['request']['execution_results']
            self.recorded[self.kind] = {'rows': self.failures, 'runtime': confirms[0]['request']['execution_runtime_ms']}
        else:
            if self.kind not in self.recorded:
                raise Refused('wire:unknown-hosted-baseline')
            cached = body['execution_results']
            if body.get('execution_runtime_ms') != self.recorded[self.kind]['runtime']:
                raise Refused('wire:cached-runtime-echo')
        self.cached_rows = self.recorded[self.kind]['rows']
        assert_payload(cached, *self.expected_result(self.cached_rows))
        if self.active == 'fail_first' and decision != 'ready_to_execute':
            raise Refused('wire:fresh-fullUID-body-rendered-first-must-build')
        if self.default_mode and self.active == 'body_pass_first' and decision != 'ready_to_execute':
            raise Refused('wire:default-name-body-change-must-build')
        if self.active.endswith('repeat') and decision != 'skip_execution':
            raise Refused('wire:positive-unchanged-reuse-required')
        return row, decision, cached

    def cell(self, kind, label, rows):
        self.kind, self.active, self.failures = kind, label, rows
        self.fallback_mode = False
        label = kind + '_' + label
        self.reference_mode = False
        self.write_test()
        resume_first = self.prior is not None and label == 'table_fail_first'
        if resume_first:
            compiled = CommandResult(0, 'retained original compile', self.prior / 'targets/001_table_fail_first_compile', [])
        else:
            with reference_canary(self, label + '_compile') as endpoint:
                compiled = self.command(label + '_compile', 'compile', False, endpoint)
        if compiled.returncode:
            raise Refused('guard:compile-failed')
        self.manifest(compiled)
        self.reference_mode = True
        self.write_test()
        if resume_first:
            events = json.loads((self.prior / 'operations.json').read_text())
            results = [e for e in events if e['kind'] == 'dbt-result' and e['label'] == 'table_fail_first_reference']
            if len(results) != 1 or results[0]['calls'] != 0:
                raise Refused('guard:prior-reference-command-result')
            reference = CommandResult(results[0]['returncode'], 'retained original reference', self.prior / 'targets/002_table_fail_first_reference', [])
            self.query_logs(self.prior / 'logs/002_table_fail_first_reference', label + '_prior_reference')
        else:
            with reference_canary(self, label + '_reference') as endpoint:
                reference = self.command(label + '_reference', 'test', False, endpoint)
        self.manifest(reference)
        reference_outcome = self.outcome(reference, rows, False)
        fresh = self.physical(label + '_reference', rows)
        direct = self.sql(label + '_fresh_F', self.rendered_query(rows))
        assert_literal_rows(direct, rows)
        self.aggregate(label + '_fresh_F_aggregate', self.rendered_query(rows), rows)
        if not same_result(fresh['rows'], direct):
            raise Refused('oracle:independent-disabled-reference')
        self.reference_mode = False
        self.write_test()
        result = self.command(label, 'test', True)
        self.manifest(result)
        row, decision, cached = self.inspect_calls(result)
        self.fallback_mode = decision == 'invalid_argument_fallback'
        outcome = self.outcome(result, rows, True)
        if self.fallback_mode and (outcome['run_result']['status'] != 'pass' or result.returncode != 0
                or any(marker not in result.output for marker in ('dbt1410', 'Failed to process SQL', 'executing normally'))):
            raise Refused('oracle:expected-error-must-have-successful-native-fallback')
        physical_rows = rows if self.fallback_mode else self.cached_rows
        audit = self.physical(label + '_managed', physical_rows)
        if decision == 'ready_to_execute':
            self.save('recorded-R/' + label + '.json', audit)
        if not self.fallback_mode:
            self.explain(label, [row])
        cell = {'label': label, 'decision': decision, 'request': row['request'], 'response': row['response'],
                'cached': cached, 'recorded_R_rows': self.cached_rows, 'fresh_F_rows': rows,
                'physical_audit_rows': physical_rows, 'ordinary_status': 3 if self.fallback_mode else 0,
                'reference_outcome': reference_outcome, **outcome}
        self.cells.append(cell)
        self.save('audit-results.json', self.cells)
        if self.contract is not None:
            expected = self.contract[label]
            if any(cell[k] != expected[k] for k in ('decision', 'cached', 'recorded_R_rows', 'fresh_F_rows', 'physical_audit_rows', 'ordinary_status', 'raw_returncode')) or cell['run_result']['status'] != expected['status']:
                raise Refused('oracle:observed-hosted-contract')

    def run(self):
        success, failure, certified = False, None, []
        before = None
        try:
            metadata = self.api('get', '/api/2.1/unity-catalog/schemas/cdo_demo_db.' + SCHEMA)
            if metadata.get('schema_id') != self.clearance['allocation']['schema_id'] or metadata.get('owner') != self.clearance['allocation']['owner']:
                raise Refused('guard:audit-schema-identity')
            before = self.inventory()
            self.save('inventory-before.json', before)
            if self.a.run_id == '20261114a144aa' and before[SCHEMA] and self.prior is None:
                raise Refused('guard:audit-schema-not-empty')
            if self.prior:
                existing = self.continuation['existing_owned_reference']
                if {r['full_name'] for r in before[SCHEMA]} != {existing}:
                    raise Refused('guard:continuation-exact-reference-only')
                reference_metadata = self.api('get', '/api/2.1/unity-catalog/tables/' + existing)
                if (reference_metadata.get('table_id') != self.continuation['existing_owned_reference_table_id']
                        or reference_metadata.get('owner') != self.clearance['allocation']['owner']):
                    raise Refused('guard:continuation-reference-id-owner')
            if self.prior is None and any(r['full_name'] in self.scope.allowed for rows in before.values() for r in rows):
                raise Refused('guard:audit-output-not-fresh')
            self.start()
            for step in self.plan():
                self.cell(*step)
            success = True
        except Exception as error:
            failure = self.clean(str(error))
        finally:
            self.stop()
            for path in sorted((self.work / 'golden').glob('*.jsonl')):
                try:
                    entries = verify_capture(path)
                    certified.append({'path': str(path), 'complete': True, 'rows': len(entries), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
                except Exception as error:
                    certified.append({'path': str(path), 'complete': False, 'error': str(error)})
            if before is not None:
                try:
                    after = self.inventory()
                    self.save('inventory-after.json', after)
                    for schema in before:
                        prior = sorted([r for r in before[schema] if r['full_name'] not in self.scope.allowed], key=lambda r: r['full_name'])
                        retained = sorted([r for r in after[schema] if r['full_name'] not in self.scope.allowed], key=lambda r: r['full_name'])
                        if prior != retained:
                            raise Refused('guard:protected-inventory-changed')
                    if success and {r['full_name'] for r in after[SCHEMA] if r['full_name'] in self.scope.allowed} != self.scope.allowed:
                        raise Refused('guard:missing-exact-audit-outputs')
                except Exception as error:
                    failure = (failure + '; ' if failure else '') + self.clean(str(error))
            success = success and failure is None and bool(certified) and all(r['complete'] for r in certified)
            self.save('summary.json', {'success': success, 'failure': failure, 'cells': len(self.cells), 'invocations': self.n, 'capture': certified})
        print(json.dumps(json.loads((self.work / 'summary.json').read_text())), flush=True)
        return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main(AuditTests))
