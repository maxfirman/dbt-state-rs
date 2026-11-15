#!/usr/bin/env python3
"""Capture pinned dbt auxiliary RPCs without executing warehouse SQL.

Requires an existing Databricks profile and the same internal credential setup
as databricks_survey.py. An empty project compiles no nodes. Retains successful
and deliberately failing compile invocations, and certifies the proxy log.
"""
import argparse
import configparser
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time

from capture_integrity import verify_capture

DIGEST = 'f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dbt', required=True)
    parser.add_argument('--proxy', required=True)
    parser.add_argument('--profiles-dir', required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--work-dir', required=True)
    parser.add_argument('--local-upstream', help='literal-loopback HTTP Rust server; opt-in proxy mode')
    args = parser.parse_args()
    dbt, proxy = str(Path(args.dbt).resolve()), str(Path(args.proxy).resolve())
    if hashlib.sha256(Path(dbt).read_bytes()).hexdigest() != DIGEST:
        raise ValueError('expected pinned dbt 2.0.11 binary')
    if not args.profile.replace('_', '').isalnum():
        raise ValueError('profile must be a simple identifier')
    work = Path(args.work_dir).resolve()
    work.mkdir(parents=True, exist_ok=True)
    if any(work.iterdir()):
        raise ValueError('work directory must be empty')
    runner = Path(__file__).read_bytes()
    (work / 'runner.py').write_bytes(runner)
    project = work / 'project'
    (project / 'models').mkdir(parents=True)
    (project / 'dbt_project.yml').write_text(
        f'name: auxiliary_157\nversion: 1.0.0\nprofile: {args.profile}\n')
    env = dict(os.environ)
    config = configparser.ConfigParser(interpolation=None)
    config.read(Path.home() / '.databrickscfg')
    token = config['DEFAULT']['token']
    env['DBT_ENV_SECRET_DATABRICKS_TOKEN'] = token
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env.update(DBT_ENGINE_MANAGE_STATE='true', RUN_CACHE_API_URL=f'127.0.0.1:{port}',
               RUN_CACHE_API_SECURE='false')
    proxy_env = dict(env, PROXY_LISTEN=f'127.0.0.1:{port}', GOLDEN_DIR=str(work / 'golden'), RUST_LOG='warn')
    if args.local_upstream:
        proxy_env['PROXY_LOCAL_UPSTREAM'] = args.local_upstream
    else:
        proxy_env['PROXY_UPSTREAM'] = 'https://api.state.dbt.com:443'
        proxy_env.pop('PROXY_LOCAL_UPSTREAM', None)
    operations = []
    with (work / 'proxy.log').open('w') as log:
        process = subprocess.Popen([proxy], env=proxy_env, stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(200):
                if process.poll() is not None:
                    raise RuntimeError('owned recording proxy exited before startup')
                with socket.socket() as sock:
                    if sock.connect_ex(('127.0.0.1', port)) == 0:
                        break
                time.sleep(.1)
            else:
                raise RuntimeError('owned recording proxy did not start')
            for label, selection in [('success', '__auxiliary_no_nodes__'),
                                     ('failure', 'auxiliary_fail')]:
                if label == 'failure':
                    (project / 'models' / 'auxiliary_fail.sql').write_text(
                        "{% if execute %}{{ exceptions.raise_compiler_error('expected auxiliary failure') }}{% endif %} select 1 as id\n")
                command = [dbt, 'compile', '--select', selection, '--project-dir', str(project),
                           '--profiles-dir', str(Path(args.profiles_dir).resolve()),
                           '--manage-state', '--no-version-check']
                operation = {'label': label, 'command': command}
                operations.append(operation)
                (work / 'operations.json').write_text(json.dumps(operations, indent=2))
                result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=90)
                operation['returncode'] = result.returncode
                (work / f'{label}.log').write_text((result.stdout + result.stderr).replace(token, '[REDACTED]'))
                (work / 'operations.json').write_text(json.dumps(operations, indent=2))
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                process.wait(timeout=30)
    captures = list((work / 'golden').glob('*.jsonl'))
    if len(captures) != 1:
        raise RuntimeError('expected exactly one recording session')
    entries = verify_capture(captures[0])
    if operations[0]['returncode'] != 0 or operations[1]['returncode'] == 0:
        raise RuntimeError('expected successful empty compile and compile-time failure')
    if any(entry['service'] not in ('ClientValidation', 'ClientTelemetry') for entry in entries):
        raise RuntimeError('auxiliary-only invocation unexpectedly reached another service')
    ends = [event['request']['session_end'] for entry in entries if entry['method'] == 'SubmitTelemetryBatch'
            for event in entry['request']['events'] if 'session_end' in event['request']]
    if len(ends) != 2 or any(end.get('session_duration') is None for end in ends):
        raise RuntimeError('both real client session durations must be captured')
    if [end['result'] for end in ends] != [0, 1]:
        raise RuntimeError('expected success and failure client-result telemetry')
    if any(entry['status']['code'] != 0 for entry in entries):
        raise RuntimeError('auxiliary RPC unexpectedly rejected by upstream')
    for entry in entries:
        expected = {'is_supported': True} if entry['method'] == 'ValidateClientVersion' else {'success': True}
        if entry['response'] != expected:
            raise RuntimeError('unexpected auxiliary response fields')
    query_log = project / 'logs' / 'query_log.sql'
    if not query_log.exists() or query_log.read_text().strip():
        raise RuntimeError('expected retained empty warehouse query log')
    report = {'client_sha256': DIGEST, 'runner_sha256': hashlib.sha256(runner).hexdigest(),
              'upstream': args.local_upstream or 'hosted',
              'warehouse_statements_executed': 0, 'rpc_count': len(entries),
              'capture_sha256': hashlib.sha256(captures[0].read_bytes()).hexdigest(),
              'session_results': [end['result'] for end in ends]}
    (work / 'summary.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report))


if __name__ == '__main__':
    main()
