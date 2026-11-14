#!/usr/bin/env python3
"""Bounded #143 source-less data-test capture. No warehouse mutations allowed.

The only workload is one selected singular test using literal INT rows. This is
an independent partial investigation; #143 remains blocked by #138 UID isolation.
"""
import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

from capture_integrity import verify_capture
from databricks_baseline import Baseline, LeaseScope
from databricks_readonly_reference import reference_canary
from databricks_survey import Campaign, Refused, complete_result


def payload(failures, should_error=None, should_warn=None):
    return {'fields': {'failures': {'kind': {'int_value': failures}},
                       'should_error': {'kind': {'bool_value': failures > 0 if should_error is None else should_error}},
                       'should_warn': {'kind': {'bool_value': failures > 0 if should_warn is None else should_warn}}}}


def assert_payload(actual, failures, should_error=None, should_warn=None):
    # Python equality alone equates bool and int. Compare JSON types as well.
    if json.dumps(actual, sort_keys=True) != json.dumps(payload(failures, should_error, should_warn), sort_keys=True):
        raise Refused('oracle:complete-typed-test-payload')


def assert_literal_rows(actual, failures):
    columns = [tuple(c.get(k) for k in ('name', 'type_text', 'type_name', 'position'))
               for c in actual['columns']]
    rows = [] if failures == 0 else [['1', '2'], ['1', '2'], ['2', None]]
    if (columns != [('id', 'INT', 'INT', 0), ('v', 'INT', 'INT', 1)]
            or any(type(c.get('position')) is not int for c in actual['columns'])
            or Counter(map(tuple, actual['rows'])) != Counter(map(tuple, rows))):
        raise Refused('oracle:literal-rows-schema-duplicate-null')


def select_only(sql):
    # This deliberately tiny grammar admits generated literal subqueries only.
    # SQL comments are stripped before checking so metadata cannot hide DDL.
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.S)
    sql = '\n'.join(line for line in sql.splitlines() if not line.lstrip().startswith('--')).strip()
    sql = sql.removesuffix(';').strip()
    if (not re.match(r'(?i)^select\s', sql) or ';' in sql or '`' in sql
            or re.search(r'(?i)\b(create|drop|alter|insert|update|delete|merge|describe|show|copy|use|call)\b', sql)
            or re.search(r'(?i)\b(from|join)\s+(?!\()[a-z_`]', sql)):
        raise Refused('guard:non-literal-select')
    return sql


def query(nonce, failures):
    # Neither the expected multiset nor the aggregate oracle is read from State.
    return ("SELECT id, v FROM (SELECT cast(1 as int) AS id, cast(2 as int) AS v "
            "UNION ALL SELECT cast(1 as int), cast(2 as int) "
            "UNION ALL SELECT cast(2 as int), cast(NULL as int)) AS literal_rows "
            f"WHERE {1 if failures else 0} = 1 AND '{nonce}' = '{nonce}'")


def client_sql(statement, audit_schema):
    """Admit the pinned client's exact audit metadata probes, never its data."""
    sql = re.sub(r'\s+', ' ', statement).strip().removesuffix(';')
    history = f'DESCRIBE HISTORY `cdo_demo_db`.`{audit_schema}`.`literal_outcome` LIMIT 1'
    metadata = ("SELECT last_altered, (table_type = 'VIEW' OR table_type = 'MATERIALIZED_VIEW') AS is_view "
                "FROM `cdo_demo_db`.INFORMATION_SCHEMA.TABLES "
                f"WHERE table_schema = '{audit_schema}' AND table_name = 'literal_outcome'")
    if sql in (history, metadata):
        return 'read-only-audit-metadata'
    select_only(statement)
    return 'literal-select'


class NoObjectScope(LeaseScope):
    def __init__(self, run, ledger):
        super().__init__(run, ledger)
        self.allowed = set()

    def check(self, value):
        raise Refused('guard:no-warehouse-relations-authorized')


@dataclass
class CommandResult:
    returncode: int
    output: str
    target: Path
    calls: list


class CachedTests(Campaign):
    inventory = Baseline.inventory
    explain = Baseline.explain

    def __init__(self, args):
        if args.query_identity and not re.fullmatch(r'\d{8}[a-f0-9]{6}', args.query_identity):
            raise Refused('query identity must use the run-ID alphabet')
        self.lease = json.loads(Path(args.schema_lease).read_text())
        super().__init__(args, NoObjectScope(args.run_id, self.lease))
        self.uid = 'test.cached_' + args.run_id + '.literal_outcome'
        self.audit_schema = self.scope.schema('prod') + '_dbt_test__audit'
        self.cells = []
        self.before_inventory = None
        self.contract = json.loads(Path(args.contract).read_text()) if args.contract else None
        for name in ('proxy', 'probe'):
            owned = self.work / 'bin' / name
            owned.parent.mkdir(exist_ok=True)
            shutil.copy2(getattr(args, name), owned)
            setattr(args, name, str(owned))
        self.save('cached-tests-runner.py', Path(__file__).read_text())
        self.save('cached-tests-provenance.json', {
            'entrypoint_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'proxy_sha256': hashlib.sha256(Path(args.proxy).read_bytes()).hexdigest(),
            'probe_sha256': hashlib.sha256(Path(args.probe).read_bytes()).hexdigest(),
            'backend': 'rust' if args.rust_url else 'hosted', 'contract': self.contract})
        self.save('no-object-scope.json', {'allowed': [], 'uid': self.uid,
                  'inventory_schemas': sorted(self.scope.schemas)})
        self.save('operation-manifest.json', {'steps': [
            {'label': label, 'failures': failures, 'decision': decision}
            for label, failures, decision in self.plan()],
            'literal_rows': [[1, 2], [1, 2], [2, None]],
            'schema': [['id', 'INT'], ['v', 'INT']],
            'oracle': {'pass': [0, False, False], 'fail': [3, True, True]},
            'config': {'store_failures': False, 'compare_unrendered_code': False,
                       'severity': 'ERROR', 'error_if': '> 0', 'warn_if': '> 0', 'fail_calc': 'count(*)'}})

    @staticmethod
    def plan():
        return [('pass_first', 0, 'ready_to_execute'), ('pass_repeat', 0, 'skip_execution'),
                ('fail_first', 3, 'ready_to_execute'), ('fail_repeat', 3, 'skip_execution')]

    def test_config(self):
        return {'severity': 'ERROR', 'error_if': '> 0', 'warn_if': '> 0', 'fail_calc': 'count(*)'}

    def expected_result(self, rows):
        return rows, bool(rows), bool(rows)

    def command_variables(self):
        return []

    def sql(self, label, statement):
        select_only(statement)
        return super().sql(label, statement)

    def write_test(self, failures):
        self.save('project/dbt_project.yml', {'name': 'cached_' + self.a.run_id,
                  'version': '1.0', 'config-version': 2, 'profile': 'survey'})
        self.save('project/tests/literal_outcome.sql',
                  "{{ config(store_failures=false, state={'compare_unrendered_code':false}, "
                  "severity='ERROR', fail_calc='count(*)', error_if='> 0', warn_if='> 0') }}\n"
                  + query(self.a.query_identity or self.a.run_id, failures))
        paths = sorted(p.relative_to(self.project).as_posix() for p in self.project.rglob('*') if p.is_file())
        if paths != ['dbt_project.yml', 'tests/literal_outcome.sql']:
            raise Refused('guard:project-has-extra-inputs')

    def command(self, label, verb, managed, endpoint=None):
        self.n += 1
        target = self.work / 'targets' / f'{self.n:03d}_{label}'
        logs = self.work / 'logs' / f'{self.n:03d}_{label}'
        argv = [self.a.dbt, verb, '--select', 'literal_outcome', '--target', 'prod',
                '--profiles-dir', str(self.profiles), '--profile', 'survey',
                '--target-path', str(target), '--log-path', str(logs),
                '--no-version-check', '--skip-semantic-manifest-validation',
                '--manage-state' if managed else '--no-manage-state']
        argv.extend(self.command_variables())
        env = dict(self.env)
        if endpoint:
            env['RUN_CACHE_API_URL'] = endpoint
        lo = len(self.wire())
        self.event('dbt', label=label, argv=argv, managed=managed)
        print('dbt:', label, flush=True)
        try:
            result = subprocess.run(argv, cwd=self.project, env=env, capture_output=True,
                                    text=True, timeout=180)
            output, code = result.stdout + result.stderr, result.returncode
        except subprocess.TimeoutExpired as error:
            output = ''.join(v.decode(errors='replace') if isinstance(v, bytes) else v
                             for v in (error.stdout or '', error.stderr or ''))
            code = 124
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
        return CommandResult(code, output, target, calls)

    def manifest(self, result, failures):
        manifest = json.loads((result.target / 'manifest.json').read_text())
        nodes = manifest.get('nodes', {})
        if (set(nodes) != {self.uid} or manifest.get('sources')
                or manifest.get('unit_tests')):
            raise Refused('guard:selected-source-less-singular-test')
        node = nodes[self.uid]
        config = node['config']
        if (node['resource_type'] != 'test' or node['depends_on']['nodes']
                or config.get('store_failures') is not False
                or config.get('store_failures_as') != 'ephemeral'
                or config.get('materialized') != 'test'
                or node.get('database') != 'cdo_demo_db'
                or node.get('schema') != self.audit_schema
                or node.get('alias') != 'literal_outcome'
                or config.get('pre-hook') or config.get('post-hook') or config.get('sql_header')):
            raise Refused('guard:test-config-or-dependencies')
        compiled = node.get('compiled_code')
        if compiled is None:
            compiled = next(result.target.glob('compiled/**/literal_outcome.sql')).read_text()
        if select_only(compiled) != query(self.a.query_identity or self.a.run_id, failures):
            raise Refused('guard:compiled-literal-sql')
        return node

    def query_logs(self, label):
        logs = [p / 'query_log.sql' for p in (self.work / 'logs').iterdir()
                if p.name.split('_', 1)[-1] == label and (p / 'query_log.sql').is_file()]
        if len(logs) != 1:
            raise Refused('guard:missing-query-log')
        text = re.sub(r'/\*.*?\*/', '', logs[0].read_text(), flags=re.S)
        text = '\n'.join(line for line in text.splitlines() if not line.lstrip().startswith('--'))
        statements = [s.strip() for s in text.split(';') if s.strip()]
        for statement in statements:
            client_sql(statement, self.audit_schema)
        return statements

    def audit_absent(self):
        # An exact metadata-only read before invoking dbt. An existing audit
        # object is never read as test input, replaced, or cleaned up.
        statement = ("SELECT table_name FROM cdo_demo_db.information_schema.tables "
                     f"WHERE table_schema='{self.audit_schema}' AND table_name='literal_outcome'")
        self.event('audit-metadata-guard', statement=statement)
        response = self.api('post', '/api/2.0/sql/statements', {
            'warehouse_id': self.a.warehouse, 'statement': statement,
            'wait_timeout': '30s', 'disposition': 'INLINE', 'format': 'JSON_ARRAY'})
        deadline = time.monotonic() + 120
        while response.get('status', {}).get('state') in ('PENDING', 'RUNNING'):
            if time.monotonic() > deadline:
                raise Refused('guard:audit-metadata-timeout')
            time.sleep(1)
            response = self.api('get', '/api/2.0/sql/statements/' + response['statement_id'])
        self.save('audit-absence-guard.json', response)
        result = complete_result(response, require_rows=True)
        if result['rows']:
            raise Refused('guard:selected-audit-relation-already-exists')

    def outcome(self, result, failures, managed):
        runs = json.loads((result.target / 'run_results.json').read_text())['results']
        if len(runs) != 1 or runs[0]['unique_id'] != self.uid:
            raise Refused('oracle:exact-selected-test-result')
        run = runs[0]
        status = run['status']
        failures, should_error, should_warn = self.expected_result(failures)
        severity = self.test_config()['severity']
        classification = 'fail' if should_error and severity == 'ERROR' else 'warn' if should_warn else 'pass'
        # Raw numerical exits are retained and learned; only this complete logical
        # result can explain a nonzero. Capture certification is still mandatory.
        if run.get('failures') != failures or type(run.get('failures')) is not int:
            raise Refused('oracle:client-failures')
        expected_exit = 1 if classification == 'fail' else 0
        if (status not in (classification, 'reused') or result.returncode != expected_exit):
            raise Refused('oracle:logical-class-status-exit')
        if not managed and (result.calls or status == 'reused'):
            raise Refused('oracle:disabled-state-reference')
        return {'raw_returncode': result.returncode, 'run_result': run}

    def reference(self, label, failures):
        with reference_canary(self, label) as endpoint:
            result = self.command(label, 'test', False, endpoint)
        self.manifest(result, failures)
        self.query_logs(label)
        outcome = self.outcome(result, failures, False)
        self.save('references/' + label + '.json', outcome)

    def cell(self, label, failures, decision):
        self.write_test(failures)
        with reference_canary(self, label + '_compile') as endpoint:
            compiled = self.command(label + '_compile', 'compile', False, endpoint)
        if compiled.returncode:
            raise Refused('guard:compile-failed')
        self.manifest(compiled, failures)
        # Reference first enforces zero-object reachability before any hosted call.
        self.reference('reference_' + label, failures)
        direct = self.sql('rows_' + label, query(self.a.query_identity or self.a.run_id, failures))
        assert_literal_rows(direct, failures)
        config = self.test_config()
        expected_failures, expected_error, expected_warn = self.expected_result(failures)
        calc = config['fail_calc']
        aggregate = self.sql('aggregate_' + label,
                 f"SELECT {calc} AS failures, {calc} {config['error_if']} AS should_error, {calc} {config['warn_if']} AS should_warn FROM ("
                 + query(self.a.query_identity or self.a.run_id, failures) + ') AS independently_declared')
        aggregate_schema = [tuple(c.get(k) for k in ('name', 'type_text', 'type_name', 'position'))
                            for c in aggregate['columns']]
        if (aggregate_schema != [('failures', 'BIGINT', 'LONG', 0),
                                 ('should_error', 'BOOLEAN', 'BOOLEAN', 1),
                                 ('should_warn', 'BOOLEAN', 'BOOLEAN', 2)]
                or aggregate['rows'] != [[str(expected_failures), str(expected_error).lower(), str(expected_warn).lower()]]):
            raise Refused('oracle:independent-aggregate')
        result = self.command(label, 'test', True)
        self.manifest(result, failures)
        self.query_logs(label)
        if any((row.get('status') or {}).get('code') != 0 for row in result.calls):
            raise Refused('wire:unexpected-grpc-status')
        submits = [r for r in result.calls if r['method'] == 'SubmitEnrichedSQL']
        if not submits:
            submits = [r for r in result.calls if r['method'] == 'SubmitEnrichedSQLSpeculative'
                       and 'skip_execution' in (r.get('response') or {}).get('response', {})]
        if len(submits) != 1:
            raise Refused('wire:one-test-decision')
        for row in result.calls:
            if row['method'] not in ('SubmitEnrichedSQL', 'SubmitEnrichedSQLSpeculative'):
                continue
            req = row['request']
            if (req.get('execution_type') != 8 or 'target_table' not in req or req['target_table'] is not None
                    or req.get('tables') or req.get('query_dependencies')
                    or req.get('compare_unrendered_code') is not False
                    or (req.get('dbt_node_state') or {}).get('node_unique_id') != self.uid):
                raise Refused('wire:scope-target-deps-type-mode')
            extras = {key: json.dumps(value) for key, value in self.test_config().items()}
            extras.update(store_failures='false', store_failures_as='"ephemeral"')
            if req.get('semantic_extras') != extras:
                raise Refused('wire:actual-test-config')
            namespace = req.get('table_namespace')
            if not namespace or (hasattr(self, 'namespace') and namespace != self.namespace):
                raise Refused('wire:stable-connection-namespace')
            self.namespace = namespace
            if req.get('labels', {}).get('dbt_node_unique_id') != self.uid:
                raise Refused('wire:full-uid-label')
        row = submits[0]
        response = row['response']['response']
        if set(response) != {decision}:
            raise Refused('wire:expected-positive-build-or-reuse')
        body = response[decision]
        if decision == 'ready_to_execute':
            confirms = [r for r in result.calls if r['method'] == 'ConfirmExecution'
                        and r['request']['request_id'] == body['request_id']
                        and (r.get('response') or {}).get('success') is True]
            if len(confirms) != 1:
                raise Refused('wire:missing-successful-computation-confirm')
            cached = confirms[0]['request']['execution_results']
            self.recorded_runtime = confirms[0]['request']['execution_runtime_ms']
            if type(self.recorded_runtime) is not int or self.recorded_runtime < 0:
                raise Refused('wire:complete-confirmed-runtime')
        else:
            cached = body['execution_results']
            if (type(body['execution_runtime_ms']) is not int
                    or body['execution_runtime_ms'] != self.recorded_runtime):
                raise Refused('wire:cached-runtime-echo')
        assert_payload(cached, expected_failures, expected_error, expected_warn)
        outcome = self.outcome(result, failures, True)
        if self.contract is not None:
            expected = self.contract[label]
            if (outcome['raw_returncode'] != expected['raw_returncode']
                    or outcome['run_result']['status'] != expected['status']
                    or decision != expected['decision']):
                raise Refused('oracle:hosted-client-outcome-contract')
        self.explain(label, submits)
        self.cells.append({'label': label, 'decision': decision, 'cached': cached,
                           'response': response, 'request': row['request'], **outcome})
        self.save('cached-test-results.json', self.cells)

    def run(self):
        success, failure, certified = False, None, []
        try:
            self.before_inventory = self.inventory()
            self.save('lease-inventory-before.json', self.before_inventory)
            self.audit_absent()
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
                    rows = verify_capture(path)
                    certified.append({'path': str(path), 'complete': True, 'rows': len(rows),
                                      'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
                except Exception as error:
                    certified.append({'path': str(path), 'complete': False, 'error': str(error)})
            if self.before_inventory is not None:
                try:
                    after = self.inventory()
                    self.save('lease-inventory-after.json', after)
                    canonical = lambda inventory: {s: sorted(rows, key=lambda r: r['full_name']) for s, rows in inventory.items()}
                    if canonical(after) != canonical(self.before_inventory):
                        raise Refused('guard:exact-inventory-changed')
                except Exception as error:
                    failure = (failure + '; ' if failure else '') + self.clean(str(error))
            self.save('summary.json', {'success': success and failure is None and bool(certified)
                      and all(c['complete'] for c in certified), 'failure': failure,
                      'cells': len(self.cells), 'invocations': self.n, 'capture': certified})
        print(json.dumps(json.loads((self.work / 'summary.json').read_text())), flush=True)
        return 0 if json.loads((self.work / 'summary.json').read_text())['success'] else 1


def main(runner_class=CachedTests):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('dbt', 'proxy', 'probe', 'work-dir', 'schema-lease'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--rust-url')
    parser.add_argument('--contract', help='observed hosted exit/status/decision contract; required for Rust')
    parser.add_argument('--query-identity')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    if hasattr(runner_class, 'configure_parser'):
        runner_class.configure_parser(parser)
    args = parser.parse_args()
    if args.rust_url and not re.fullmatch(r'http://127\.0\.0\.1:\d+', args.rust_url):
        parser.error('--rust-url must be an owned loopback endpoint')
    if args.rust_url and not args.contract:
        parser.error('--rust-url requires the observed hosted contract')
    try:
        return runner_class(args).run()
    except Exception as error:
        print('FAILED:', error, file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
