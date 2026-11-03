"""Real DuckDB + public Python gRPC trace executor; see duckdb-generated.md."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import uuid

import duckdb
import grpc
from dbt_state.grpc.client import QueryCacheGrpcClient
from dbt_state.grpc.interceptors import OrgIdInterceptor
from query_cache_common.models.services.sql_service_models import (
    SubmitEnrichedSQLRequest, ReadyToExecuteResponse, SkipExecutionResponse)
from query_cache_common.models.services.clone_service_models import ReadyToCloneResponse
from query_cache_common.models.services.execution_service_models import (
    ConfirmExecutionRequest, SQLExecution, ExecutionOutcome, ExecutionRecord,
    RecordExecutionsRequest)
from query_cache_common.models.shared_models import (
    TableModifiedInfo, ModelExecutionType, DbtNodeState)
from dbt_state_duckdb.evidence import EvidenceStore
from dbt_state_duckdb.recovery import Invocation, reset

from oracle import OracleViolation, expected, inputs, snapshot, assert_snapshot
from traces import generate, validate, minimize


class Server:
    def __init__(self, directory, org):
        self.directory, self.org = directory, org
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.address = '127.0.0.1:' + str(sock.getsockname()[1])
        self.process = self.client = None

    def start(self):
        self.log = (self.directory / 'server.log').open('a')
        self.process = subprocess.Popen([os.environ['DBT_STATE_SERVER_BIN']],
            env=dict(os.environ, DBT_STATE_LISTEN=self.address,
                     DBT_STATE_REQUIRE_ORG_HEADER='true'), stdout=self.log, stderr=self.log)
        # Preserve production client's interceptors and add organization scoping
        # on its real insecure transport (org_id= only affects secure channels).
        original = grpc.insecure_channel
        try:
            grpc.insecure_channel = lambda *a, **kw: grpc.intercept_channel(
                original(*a, **kw), OrgIdInterceptor(self.org))
            self.client = QueryCacheGrpcClient(self.address, timeout=10, max_attempts=1)
        finally:
            grpc.insecure_channel = original
        try:
            grpc.channel_ready_future(self.client.channel).result(timeout=15)
        except BaseException:
            self.stop()
            raise RuntimeError('server failed readiness: ' + (self.directory / 'server.log').read_text())

    def stop(self):
        if self.client:
            self.client.close()
            self.client = None
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
            self.process = None
            self.log.close()

    def restart(self):
        self.stop()
        self.start()


class Runner:
    def __init__(self, directory, trace, fault=None):
        directory.mkdir(parents=True, exist_ok=False)
        self.directory, self.trace, self.fault = directory, trace, fault
        self.path = directory / 'warehouse.duckdb'
        self.server = Server(directory, 'generated-' + uuid.uuid4().hex)
        self.submits, self.executions, self.confirmed = {}, {}, {}
        self.revision = 0
        self.counts = {}
        self.observations = []
        self.generation = 0

    def relation(self, env):
        return f'"warehouse"."{env}"."result"'

    def sql(self, k):
        return f'SELECT id, amount + {k} AS amount FROM "warehouse"."raw"."input"'

    def query(self, statement, params=None):
        with duckdb.connect(str(self.path)) as db:
            return db.execute(statement, params).fetchall()

    def request(self, env, k, clones=False):
        invocation = Invocation(self.path)
        namespace = invocation.namespace
        invocation.finish(True)
        evidence = EvidenceStore(self.path)
        target = self.relation(env)
        raw = '"warehouse"."raw"."input"'
        return SubmitEnrichedSQLRequest(
            target_table=target, dialect='duckdb', default_catalog='warehouse',
            default_schema=env, execution_type=ModelExecutionType.FULL,
            sql=self.sql(k), ignore_external_modifications=False,
            tables=[TableModifiedInfo(name=n, last_modified_epoch=evidence.observe(n))
                    for n in [raw, target]],
            tolerate_nondeterminism=True, allow_clones=clones,
            semantic_extras={'duckdb_copy_contract': 'rows-schema-v1'},
            table_namespace=namespace,
            dbt_node_state=DbtNodeState(node_unique_id='model.generated.result',
                target_name=env, project_name='generated', resource_type='model',
                node_hash='rendered-' + str(k), profile_name='generated'))

    def current(self, env):
        return snapshot(self.path, self.relation(env))

    def check(self, env, allowed, context):
        assert_snapshot(self.current(env), allowed, context)

    def publish(self, execution):
        self.confirmed[execution['env']] = dict(execution, generation=self.generation)

    def perform(self, op):
        kind = op['op']
        self.counts['op:' + kind] = self.counts.get('op:' + kind, 0) + 1
        if kind == 'reset':
            reset(self.path)
            self.generation += 1
            self.confirmed.clear()
        elif kind == 'restart':
            self.server.restart()
        elif kind == 'mutate_input':
            self.revision = op['revision']
            self.query('DELETE FROM raw.input')
            with duckdb.connect(str(self.path)) as db:
                db.executemany('INSERT INTO raw.input VALUES (?, ?)', inputs(self.revision))
        elif kind == 'drop':
            self.query('DROP TABLE IF EXISTS ' + self.relation(op['env']))
            self.confirmed.pop(op['env'], None)
        elif kind in ['mutate_target', 'mutate_schema']:
            if kind == 'mutate_target':
                self.query('UPDATE ' + self.relation(op['env']) + ' SET amount=999')
            else:
                self.query('ALTER TABLE ' + self.relation(op['env']) + ' ALTER amount TYPE BIGINT')
            self.confirmed.pop(op['env'], None)
        elif kind == 'submit':
            self.submit(op)
        elif kind in ['execute', 'copy']:
            self.execute(op)
        elif kind in ['confirm', 'record']:
            execution = self.executions[op['execute']]
            if execution.get('skipped'):
                return
            self.check(execution['env'], [execution['expected']], kind)
            outcome = dict(last_modified_epoch=execution['epoch'], table_type='DUCKDB CTAS TABLE')
            if kind == 'confirm':
                response = self.server.client.confirm_execution(ConfirmExecutionRequest(
                    request_id=execution['response'].request_id, **outcome))
            else:
                response = self.server.client.record_executions(RecordExecutionsRequest(records=[
                    ExecutionRecord(outcome=ExecutionOutcome(**outcome),
                        enriched_sql=SQLExecution.from_submit_sql_request(execution['request']))]))
            if kind == 'confirm':
                assert response.success, response
            else:
                assert response.records_stored == 1, response
            self.publish(execution)
        else:
            raise ValueError('unsupported operation: ' + kind)

    def submit(self, op):
        env, k = op['env'], op['k']
        req = self.request(env, k, op.get('clones', False))
        # The permitted source set and snapshots exist BEFORE observing response.
        # Staleness is explicitly allowed only by an annotated trace operation.
        allowed_sources = {self.relation(e): h for e, h in self.confirmed.items()
            if e != env and h['k'] == k and not h.get('copied')
            and h['generation'] == self.generation
            and (op.get('stale', False) or h['revision'] == self.revision)}
        skip_expected = expected(self.revision, k)
        if op.get('allowed_recorded_result'):
            history = self.confirmed[env]
            assert history['k'] == k and history['generation'] == self.generation
            skip_expected = history['expected']
        response = self.server.client.submit_sql(req)
        wire = response.to_dict()
        decision = ('skip' if isinstance(response, SkipExecutionResponse) else
                    'copy' if isinstance(response, ReadyToCloneResponse) else
                    'build' if isinstance(response, ReadyToExecuteResponse) else 'unknown')
        # Fault injection is at the response/warehouse boundary, after a real
        # production RPC. It neither changes the oracle nor manufactures rows.
        if self.fault == 'wrong-skip' and op.get('mutant'):
            decision = 'skip'
        record = dict(request=req, response=response, env=env, k=k,
                      revision=self.revision, expected=expected(self.revision, k),
                      decision=decision, allowed_sources=allowed_sources)
        self.submits[op['id']] = record
        self.observations.append(dict(id=op['id'], request=req.to_dict(), response=wire,
            decision=decision, allowed_skip_result=skip_expected,
            allowed_sources={n: h['expected'] for n, h in allowed_sources.items()}))
        self.counts['decision:' + decision] = self.counts.get('decision:' + decision, 0) + 1
        if decision == 'skip':
            self.check(env, [skip_expected], 'wrong-skip' if self.fault else 'skip')
        elif decision == 'copy':
            if response.clone_source not in allowed_sources:
                raise OracleViolation('copy:source-scope')
            source = allowed_sources[response.clone_source]
            assert_snapshot(snapshot(self.path, response.clone_source), [source['expected']], 'copy-source')
            record['expected'] = source['expected']
            record['copied'] = True
        if op.get('expect') and decision != op['expect']:
            raise OracleViolation('eligible-reuse:' + op['expect'] + '-got-' + decision)
        if decision == 'unknown':
            raise OracleViolation('unsupported-response')

    def execute(self, op):
        if 'submit' in op:
            execution = dict(self.submits[op['submit']])
            if execution['decision'] == 'skip':
                execution['skipped'] = True
                self.executions[op['id']] = execution
                return
        else:
            env, k = op['env'], op['k']
            execution = dict(env=env, k=k, revision=self.revision,
                expected=expected(self.revision, k), request=self.request(env, k))
        env = execution['env']
        if execution.get('decision') == 'copy':
            response = execution['response']
            source, target = response.clone_source, response.clone_target
            # Follow the returned CTAS plan; independently correlate its target
            # and source before executing it. Only this conservative subset is generated.
            assert target == self.relation(env)
            assert response.clone_sqls == [f'CREATE OR REPLACE TABLE {target} AS SELECT * FROM {source}']
            assert EvidenceStore(self.path).observe(source) == response.clone_required_last_modified_epoch
            if self.fault == 'wrong-source-copy' and op.get('mutant'):
                # Actual wrong-source CTAS execution, using a real existing decoy.
                # Injection follows source validation, like an executor bug.
                statement = f'CREATE OR REPLACE TABLE {target} AS SELECT * FROM warehouse.raw.decoy'
            else:
                statement = response.clone_sqls[0]
            self.counts['ctas'] = self.counts.get('ctas', 0) + 1
        else:
            statement = 'CREATE OR REPLACE TABLE ' + self.relation(env) + ' AS ' + self.sql(execution['k'])
        self.query(statement)
        self.observations.append(dict(id=op['id'], statement=statement,
            expected=execution['expected'], actual=self.current(env)))
        self.check(env, [execution['expected']],
                   'wrong-source-copy' if self.fault == 'wrong-source-copy' else 'execute')
        execution['epoch'] = EvidenceStore(self.path).observe(self.relation(env), completed_write=True)
        self.executions[op['id']] = execution

    def run(self):
        validate(self.trace)
        (self.directory / 'trace.json').write_text(json.dumps(self.trace, indent=2))
        with duckdb.connect(str(self.path)) as db:
            for name in ['raw', 'prod', 'dev']:
                db.execute('CREATE SCHEMA ' + name)
            db.execute('CREATE TABLE raw.input(id INTEGER, amount INTEGER)')
            db.executemany('INSERT INTO raw.input VALUES (?, ?)', inputs(0))
            db.execute('CREATE TABLE raw.decoy AS SELECT 99::INTEGER AS id, 999::INTEGER AS amount')
        invocation = Invocation(self.path)
        invocation.finish(True)
        try:
            self.server.start()
            for index, op in enumerate(self.trace['operations']):
                self.index = index
                self.perform(op)
                self.save()
        except BaseException as exc:
            self.save()
            (self.directory / 'failure.json').write_text(json.dumps(dict(
                index=getattr(self, 'index', -1), fingerprint=str(exc), type=type(exc).__name__), indent=2))
            raise
        finally:
            self.server.stop()
        return self.counts

    def save(self):
        (self.directory / 'observations.json').write_text(json.dumps(self.observations, indent=2, default=str))


def mutant_trace(fault):
    from traces import Trace
    t = Trace()
    t.add('reset')
    t.run('prod', 1, 'build', clones=True)
    if fault == 'wrong-skip':
        t.add('submit', env='prod', k=2, clones=False, stale=False, mutant=True)
    else:
        s = t.add('submit', env='dev', k=1, expect='copy', clones=True, stale=False)
        t.add('execute', submit=s, mutant=True)
    return dict(version=1, seed='mutation-' + fault, fault=fault, operations=t.operations)


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--seeds', default='0,1,2,3')
    parser.add_argument('--steps', type=int, default=4, help='random episodes in addition to fixed spine')
    parser.add_argument('--replay', type=Path)
    parser.add_argument('--shrink', action='store_true')
    parser.add_argument('--mutation-checks', action='store_true')
    args = parser.parse_args()
    if args.steps < 0:
        parser.error('--steps must be nonnegative')
    args.artifacts.mkdir(parents=True, exist_ok=True)
    traces = ([json.loads(args.replay.read_text())] if args.replay else
              [generate(int(seed), args.steps) for seed in args.seeds.split(',')])
    started = time.monotonic()
    for number, trace in enumerate(traces):
        directory = args.artifacts / ('trace-' + str(number) + '-' + uuid.uuid4().hex[:8])
        try:
            counts = Runner(directory, trace, fault=trace.get('fault')).run()
            print(json.dumps(dict(seed=trace['seed'], counts=counts)), flush=True)
        except OracleViolation as failure:
            print('Replay: --replay ' + str(directory / 'trace.json'), flush=True)
            if args.shrink:
                fingerprint = str(failure)
                def reproduces(candidate):
                    attempt = args.artifacts / ('shrink-' + uuid.uuid4().hex)
                    try:
                        Runner(attempt, candidate, fault=candidate.get('fault')).run()
                    except OracleViolation as exc:
                        return str(exc) == fingerprint
                    return False
                minimized = minimize(trace, reproduces)
                (directory / 'minimized.json').write_text(json.dumps(minimized, indent=2))
            raise
    if args.mutation_checks:
        for fault in ['wrong-skip', 'wrong-source-copy']:
            directory = args.artifacts / (fault + '-' + uuid.uuid4().hex[:8])
            try:
                Runner(directory, mutant_trace(fault), fault=fault).run()
            except OracleViolation as exc:
                assert str(exc) == fault + ':rows', str(exc)
                print('mutation caught: ' + str(exc), flush=True)
            else:
                raise AssertionError('oracle did not detect ' + fault)
    print(f'generated elapsed_seconds={time.monotonic() - started:.2f}', flush=True)


if __name__ == '__main__':
    main()
