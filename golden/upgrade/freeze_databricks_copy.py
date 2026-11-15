"""Offline historical-server export; never connects to a Warehouse."""
import argparse, hashlib, json, os, socket, subprocess, time, urllib.parse, uuid
from pathlib import Path
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--historical-source', type=Path, required=True)
parser.add_argument('--bin-dir', type=Path, required=True)
args = parser.parse_args()
repo = Path(__file__).resolve().parents[2]
bin_dir = args.bin_dir
commit = subprocess.check_output(['git', '-C', str(args.historical_source), 'rev-parse', 'HEAD'], text=True).strip()
assert commit == '45e94c3e33a6fccfbaf79efb1a143b794f0ddb33'
assert not subprocess.check_output(['git', '-C', str(args.historical_source), 'status', '--porcelain'], text=True)
base = 'postgres://dbtstate:dbtstate@localhost:55441/dbtstate'
schema = 'upgrade160_' + uuid.uuid4().hex
wire_path = repo / 'crates/harness/tests/fixtures/databricks_precopy_native.jsonl'
wire = [json.loads(line) for line in wire_path.read_text().splitlines()]
def sql(query):
    return subprocess.check_output(['psql', base, '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', query], text=True).strip()
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
sql('CREATE SCHEMA ' + schema)
with socket.socket() as sock:
    sock.bind(('127.0.0.1', 0))
    port = sock.getsockname()[1]
env = dict(os.environ, DATABASE_URL=base + '?' + urllib.parse.urlencode({'options[search_path]': schema}), DBT_STATE_LISTEN=f'127.0.0.1:{port}', RUST_LOG='warn')
log = open('/tmp/freeze160-server.log', 'w')
server = subprocess.Popen([str(bin_dir / 'dbt-state-server')], env=env, stdout=log, stderr=subprocess.STDOUT)
try:
    for _ in range(100):
        assert server.poll() is None
        with socket.socket() as sock:
            if sock.connect_ex(('127.0.0.1', port)) == 0:
                break
        time.sleep(.1)
    else:
        raise RuntimeError('startup failed')
    def rpc(op, request):
        return json.loads(subprocess.check_output([str(bin_dir / 'state-probe'), op], input=json.dumps(request), text=True, env=dict(os.environ, PROBE_URL=f'http://127.0.0.1:{port}')))
    def row(request_id):
        return json.loads(sql("SELECT row_to_json(e) FROM " + schema + ".executions e WHERE request_id='" + request_id + "'"))
    episodes = []
    for offset in [0, 8]:
        rows = wire[offset:offset+8]
        source_id = rpc('submit', rows[0]['request'])['response']['ready_to_execute']['request_id']
        outcome = dict(rows[1]['request'], request_id=source_id)
        assert rpc('confirm', outcome)['success']
        source = row(source_id)
        copy_id = rpc('clone', rows[2]['request'])['ready_to_clone']['request_id']
        pending = row(copy_id)
        outcome = dict(rows[3]['request'], request_id=copy_id)
        assert rpc('confirm', outcome)['success']
        confirmed = row(copy_id)
        responses = [rpc('submit', r['request']) for r in rows[4:7]]
        assert ['skip_execution' in r['response'] for r in responses] == [True, True, False]
        episodes.append(dict(native_offset=offset, source_row=source, pending_copy=pending, confirmed_copy=confirmed, copy_outcome=outcome, historical_verdicts=['skip_execution', 'skip_execution', 'ready_to_execute']))
    fixture = dict(provenance=dict(kind='historical-local-server export; no new Warehouse execution or hosted persisted rows', source_commit='45e94c3e33a6fccfbaf79efb1a143b794f0ddb33', build_command='CARGO_TARGET_DIR=/home/maxfirman/.cache/dbt-state-152-target CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=3 cargo build -p dbt-state-server --bin dbt-state-server -p dbt-state-harness --bin state-probe', toolchain='rustc 1.98.0', artifacts={name: digest(bin_dir/name) for name in ['dbt-state-server', 'state-probe']}, freeze_runner_sha256=digest(Path(__file__)), native_fixture_sha256=digest(wire_path), schema=schema), episodes=episodes)
    (repo/'golden/upgrade/databricks_copy_v1_45e94c3.json').write_text(json.dumps(fixture, indent=2) + '\n')
    print('Exported native MERGE and check-snapshot complete source/pending/confirmed history')
finally:
    server.terminate()
    server.wait(30)
    log.close()
    sql('DROP SCHEMA ' + schema + ' CASCADE')
