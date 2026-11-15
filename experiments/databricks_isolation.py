#!/usr/bin/env python3
"""Opt-in #159 concurrent pinned-dbt campaigns with exact run-owned cleanup.

Run hosted first, replay the resulting complete captures, then use --rust-url.
Two independent projects share leased containers, never relations or processes.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys

from capture_integrity import verify_capture
from databricks_baseline import Baseline, LeaseScope, assert_physical, query
from databricks_deferral import require_decision
from databricks_survey import Campaign, Refused, Scope, same_result


def cleanup_plan(owned, requested):
    """Generate destructive statements only for exact run-created relations."""
    requested = set(requested)
    if not requested <= set(owned):
        raise Refused('cleanup relation is outside this run')
    if any(not re.fullmatch(r'cdo_demo_db\.[a-z0-9_]+\.[a-z0-9_]+', name) for name in requested):
        raise Refused('cleanup relation is outside this run')
    return ['DROP TABLE ' + relation for relation in sorted(requested)]


class IsolationScope(LeaseScope):
    def __init__(self, run, ledger):
        super().__init__(run, ledger)
        self.relation_prefix = 'i_' + run + '_'
        self.allowed = {f'cdo_demo_db.{schema}.{self.relation_prefix}literal_table' for schema in self.schemas}


class Isolation(Baseline):
    def __init__(self, args):
        self.lease = json.loads(Path(args.schema_lease).read_text())
        self.query_identity = args.query_identity or args.run_id
        Scope(self.query_identity)
        Campaign.__init__(self, args, IsolationScope(args.run_id, self.lease))
        self.created = set()
        binaries = self.work / 'bin'
        binaries.mkdir()
        for name in ('proxy', 'probe'):
            source = Path(getattr(args, name))
            destination = binaries / name
            shutil.copy2(source, destination)
            setattr(args, name, str(destination))
        self.save('isolation-runner.py', Path(__file__).read_text())
        self.save('operation-manifest.json', {
            'version': 1, 'backend': 'rust' if args.rust_url else 'hosted',
            'rust_url': args.rust_url, 'query_identity': self.query_identity,
            'binary_sha256': {name: hashlib.sha256(Path(getattr(args, name)).read_bytes()).hexdigest()
                              for name in ('proxy', 'probe')},
            'classification': 'actual-pinned-client-concurrent-campaign',
            'relations': sorted(self.scope.allowed),
            'steps': ['concurrent first build', 'required unchanged reuse',
                      'State-disabled reference', 'independent SELECT',
                      'reject peer cleanup', 'clean only first campaign',
                      'check peer complete result and required reuse', 'clean second campaign'],
            'properties': {'Type': 'MANAGED', 'Provider': 'delta'}})

    def physical_table(self, label, role):
        actual = self.observe(label, self.scope.relation(role, 'literal_table'))
        properties = json.loads((self.work / 'physical' / (label + '_properties.json')).read_text())
        assert_physical(actual, properties, 0, 'table', self.query_identity)
        return actual

    def prepare(self):
        self.init_schemas()
        self.setup_project()
        (self.project / 'models/sources.yml').unlink()
        self.write_model('literal_table', "materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}",
                         query(0, self.query_identity))
        self.start()

    def phase(self):
        for label, decision in [('first', 'ready_to_execute'), ('unchanged', 'skip_execution')]:
            calls = self.dbt(label, ['run', '--select', 'literal_table'])
            self.explain(label, require_decision(calls, 'literal_table', decision))
            self.created.add(self.scope.relation('prod', 'literal_table'))
            self.physical_table(label, 'prod')
        # A separate project and State-disabled invocation establish the reference.
        reference = self.work / 'reference-project'
        shutil.copytree(self.project, reference)
        self.project = reference
        if self.dbt('reference', ['run', '--select', 'literal_table'], target='ref', managed=False):
            raise Refused('State-disabled reference sent State RPCs')
        self.created.add(self.scope.relation('ref', 'literal_table'))
        actual = self.physical_table('reference', 'ref')
        independent = self.sql('independent_select', query(0, self.query_identity).replace('select ', 'SELECT ', 1))
        if not same_result(actual, independent):
            raise Refused('independent reference differs')
        self.project = self.work / 'project'
        if not same_result(self.physical_table('after_reference', 'prod'), actual):
            raise Refused('State result differs from independent reference')

    def cleanup(self):
        before = self.inventory()
        self.save('cleanup/inventory-before.json', before)
        for statement in cleanup_plan(self.created, self.created):
            self.sql('cleanup_owned', statement)
        after = self.inventory()
        self.save('cleanup/inventory-after.json', after)
        for schema, rows in before.items():
            for row in rows:
                if row['full_name'] not in self.created and row not in after[schema]:
                    raise Refused('cleanup altered a peer or preexisting object')
            if any(row['full_name'] in self.created for row in after[schema]):
                raise Refused('owned cleanup left a relation')
        self.save('checks/cleanup.json', {'removed': sorted(self.created), 'peer_and_preexisting_objects_preserved': True})


def run(args):
    if not re.fullmatch(r'\d{8}[a-f0-9]{5}', args.run_id):
        raise Refused('campaign run id must be YYYYMMDD plus five hex digits; child a/b completes each run id')
    if args.query_identity and not re.fullmatch(r'\d{8}[a-f0-9]{5}', args.query_identity):
        raise Refused('query identity must be YYYYMMDD plus five hex digits')
    root = Path(args.work_dir).resolve()
    if root.exists() and any(root.iterdir()):
        raise Refused('work directory must be empty')
    campaigns = []
    error = None
    try:
        for suffix in ('a', 'b'):
            options = deepcopy(args)
            options.run_id += suffix
            options.query_identity = (args.query_identity or args.run_id) + suffix
            options.work_dir = str(root / suffix)
            campaigns.append(Isolation(options))
        for campaign in campaigns:
            campaign.prepare()
        with ThreadPoolExecutor(max_workers=2) as workers:
            futures = [workers.submit(campaign.phase) for campaign in campaigns]
            # Await both even when one fails; never abandon a still-writing peer.
            failures = []
            for future in futures:
                try:
                    future.result()
                except Exception as failure:
                    failures.append(str(failure))
            if failures:
                raise Refused('; '.join(failures))
        first, peer = campaigns
        for owner, other in ((first, peer), (peer, first)):
            try:
                cleanup_plan(owner.created, other.created)
            except Refused as failure:
                if str(failure) != 'cleanup relation is outside this run':
                    raise
                owner.save('checks/peer-cleanup-rejected.json', {'failure': str(failure), 'warehouse_statements': 0})
            else:
                raise Refused('peer cleanup was accepted')
        first.cleanup()
        # This physically executable check catches a broad schema cleanup even
        # when its SQL command returned success. Require SKIP so rebuilding cannot mask it.
        peer.physical_table('after_peer_cleanup', 'prod')
        peer.physical_table('reference_after_peer_cleanup', 'ref')
        calls = peer.dbt('reuse_after_peer_cleanup', ['run', '--select', 'literal_table'])
        peer.explain('reuse_after_peer_cleanup', require_decision(calls, 'literal_table', 'skip_execution'))
        peer.physical_table('reused_after_peer_cleanup', 'prod')
        peer.cleanup()
    except Exception as failure:
        error = str(failure)
    finally:
        summaries = []
        for campaign in campaigns:
            campaign.stop()
            certified = []
            try:
                for path in sorted((campaign.work / 'golden').glob('*.jsonl')):
                    rows = verify_capture(path)
                    certified.append({'path': str(path.relative_to(campaign.work)), 'rows': len(rows),
                                      'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                                      'methods': dict(Counter(row['method'] for row in rows))})
                if not certified:
                    raise Refused('missing certified capture')
                after = campaign.inventory()
                campaign.save('lease-inventory-after.json', after)
                if hasattr(campaign, 'before_inventory') and any(
                        row not in after[schema] for schema, rows in campaign.before_inventory.items() for row in rows):
                    raise Refused('preexisting object disappeared')
            except Exception as failure:
                error = (error + '; ' if error else '') + str(failure)
            campaign.save('capture-certification.json', certified)
            summaries.append((campaign, certified))
        for campaign, _ in summaries:
            if error:
                error = campaign.clean(error)
        for campaign, certified in summaries:
            campaign.save('summary.json', {'success': error is None, 'failure': error,
                                         'invocations': campaign.n, 'capture': certified})
        root.mkdir(parents=True, exist_ok=True)
        (root / 'summary.json').write_text(json.dumps({'success': error is None, 'failure': error}, indent=2))
    if error:
        raise Refused(error)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('dbt', 'proxy', 'probe', 'schema-lease', 'work-dir'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex()[:5],
                        help='YYYYMMDD plus five hex digits; independent children append a/b')
    parser.add_argument('--query-identity')
    parser.add_argument('--rust-url')
    args = parser.parse_args()
    try:
        run(args)
    except Exception as error:
        print('FAILED:', str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
