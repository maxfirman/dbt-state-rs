"""Freeze actual retained seed requests using a pinned historical local server.

No warehouse/client/auth calls. Creates and removes only its fresh local schema.
The resulting local responses are observations, not hosted expectations.
"""
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time
import urllib.parse
import uuid

REPO = Path(__file__).resolve().parents[1]
OLD = Path('/home/maxfirman/.local/state/dbt-state-rs/databricks-138-20261114/pre138-bin')
SOURCE = '302156014b182df2f545a4c9bc3d128926bdf11c'
PINS = {'dbt-state-server': '52afbea7916f0ff5bd32fb77fd78f229811ff4d4cfeb19cb6ce7ee5d4dc7ab15',
        'state-probe': 'e44baeebe13e1cb09a7d3d31873f04a4e199f63359b101cd788c707f3bb083d3'}
CAPTURE = REPO / 'golden/fixtures/databricks_managed_seeds.jsonl'
CAPTURE_SHA = '3f8d4f47748353efc7d44ac24e5254b399d3a6026247da135b33561d5a9ae33d'
BASE = 'postgres://dbtstate:dbtstate@127.0.0.1:55441/dbtstate'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    assert sha(CAPTURE) == CAPTURE_SHA
    for name, digest in PINS.items():
        assert sha(OLD / name) == digest
    schema = 'legacy160_freeze_' + uuid.uuid4().hex
    out = Path('/tmp/dbt-state-132-context') / schema
    out.mkdir()
    def sql(query):
        return subprocess.check_output(['psql', BASE, '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', query], text=True).strip()
    sql('CREATE SCHEMA ' + schema)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    dsn = BASE + '?' + urllib.parse.urlencode({'options[search_path]': schema})
    env = dict(os.environ, DATABASE_URL=dsn, DBT_STATE_LISTEN=f'127.0.0.1:{port}', RUST_LOG='warn')
    operations, episodes, remaps = [], [], {}
    manifest = {'source_commit': SOURCE, 'binary_sha256': PINS, 'capture': str(CAPTURE.relative_to(REPO)),
                'capture_sha256': CAPTURE_SHA, 'runner_sha256': sha(Path(__file__)),
                'schema': schema, 'database_url': dsn, 'server_command': [str(OLD / 'dbt-state-server')],
                'probe_command': [str(OLD / 'state-probe'), '<operation>'],
                'probe_url': f'http://127.0.0.1:{port}',
                'scope': 'Local historical-server observations from retained hosted request bodies; no hosted persisted rows or hosted expectation claim.',
                'row_export_sql': 'SELECT row_to_json(e) FROM <owned-schema>.executions e WHERE request_id=<local-id>',
                'request_changes': 'Only ConfirmExecution.request_id remapped; all other request fields unchanged. Transport metadata is not replayed; org defaults to local.',
                'id_policy': 'Exported id and generated columns retained in fixture. Import allocates fresh surrogate id; all semantic fields and transport UUIDs preserved.',
                'build_provenance': json.loads((OLD / 'provenance.json').read_text()),
                'build_command': 'Retained executable reused; no historical rebuild in this run.'}
    wire = [json.loads(s) for s in CAPTURE.read_text().splitlines()]
    with (out / 'server.log').open('w') as log:
        server = subprocess.Popen([str(OLD / 'dbt-state-server')], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            for _ in range(100):
                assert server.poll() is None
                with socket.socket() as sock:
                    if sock.connect_ex(('127.0.0.1', port)) == 0:
                        break
                time.sleep(.1)
            else:
                raise RuntimeError('old server startup failed')
            # Prove the old executable migrated only our schema before any RPC.
            assert sql("SELECT count(*) FROM " + schema + ".executions") == '0'
            def rpc(op, request, index):
                result = subprocess.run([str(OLD / 'state-probe'), op], input=json.dumps(request),
                    env=dict(os.environ, PROBE_URL=f'http://127.0.0.1:{port}'), text=True, capture_output=True, check=True)
                response = json.loads(result.stdout)
                operations.append({'capture_entry_index': index, 'method': op, 'request': request, 'response': response})
                return response
            def row(rid):
                uuid.UUID(rid)
                return json.loads(sql("SELECT row_to_json(e) FROM " + schema + ".executions e WHERE request_id='" + rid + "'"))
            for index, entry in enumerate(wire):
                req = entry['request']
                if entry['method'] == 'SubmitValues':
                    response = rpc('values', req, index)
                    ready = response['response'].get('ready_to_execute')
                    if ready:
                        hosted_id = entry['response']['response']['ready_to_execute']['request_id']
                        rid = ready['request_id']
                        remaps[hosted_id] = rid
                        episodes.append({'submit_entry_index': index, 'request': req, 'pending_row': row(rid), 'hosted_request_id': hosted_id})
                    else:
                        assert 'skip_execution' in response['response']
                        episodes[-1]['unchanged_request'] = req
                        episodes[-1]['unchanged_entry_index'] = index
                elif entry['method'] == 'ConfirmExecution' and req['request_id'] in remaps:
                    original = req['request_id']
                    local = dict(req, request_id=remaps[original])
                    assert rpc('confirm', local, index)['success']
                    episode = next(e for e in episodes if e['hosted_request_id'] == original)
                    episode.update(confirm_entry_index=index, outcome=local, row=row(local['request_id']))
            assert len(episodes) == 5 and all('unchanged_request' in e and 'row' in e for e in episodes)
            fixture = {'provenance': manifest, 'request_id_remapping': remaps, 'operations': operations, 'episodes': episodes}
            data = json.dumps(fixture, indent=2) + '\n'
            (out / 'fixture.json').write_text(data)
            (REPO / 'golden/upgrade/databricks_seed_v2_3021560.json').write_text(data)
            print(json.dumps({'artifact_directory': str(out), 'fixture_sha256': sha(out / 'fixture.json'), 'episodes': len(episodes)}))
        finally:
            server.terminate()
            server.wait(30)
            sql('DROP SCHEMA ' + schema + ' CASCADE')


if __name__ == '__main__':
    main()
