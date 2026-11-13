#!/usr/bin/env python3
"""Opt-in #155 scoped deferral: hosted capture, exact replay, then --rust-url."""
import argparse
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys

from databricks_baseline import Baseline, LeaseScope, query, assert_physical
from databricks_survey import Campaign, Refused, Scope, same_result


class DeferralScope(LeaseScope):
    profile_roles = ('prod', 'dev', 'ref')

    def __init__(self, run, ledger):
        super().__init__(run, ledger)
        self.relation_prefix = 'd_' + run + '_'
        self.allowed = {f'cdo_demo_db.{schema}.{self.relation_prefix}{name}'
                        for schema in self.schemas
                        for name in ('upstream', 'downstream', 'pending', 'failed', 'reference', 'decoy')}

    def schema(self, role):
        return super().schema('ref' if role == 'dev' else role)


def require_decision(calls, name, expected):
    submits = [r for r in calls if r['method'] == 'SubmitEnrichedSQL'
               and r['request']['labels']['dbt_node_name'] == name]
    if len(submits) != 1 or expected not in submits[0]['response']['response']:
        raise Refused('missing required ' + expected + ': ' + name)
    if expected == 'ready_to_execute':
        request_id = submits[0]['response']['response'][expected]['request_id']
        if not any(r['method'] == 'ConfirmExecution' and r['request']['request_id'] == request_id
                   and r['response']['success'] for r in calls):
            raise Refused('missing successful confirmation: ' + name)
    return submits


class Deferral(Baseline):
    def __init__(self, args):
        if not args.schema_lease:
            raise Refused('deferral requires explicit schema ownership lease')
        self.lease = json.loads(Path(args.schema_lease).read_text())
        self.query_identity = args.query_identity or args.run_id
        Scope(self.query_identity)
        Campaign.__init__(self, args, DeferralScope(args.run_id, self.lease))
        binaries = self.work / 'bin'
        binaries.mkdir()
        for name in ('proxy','probe'):
            source = Path(getattr(args,name))
            destination = binaries / name
            shutil.copy2(source,destination)
            setattr(args,name,str(destination))
        self.save('deferral-runner.py', Path(__file__).read_text())
        config = json.loads((self.work / 'configuration.json').read_text())
        config['deferral'] = {'backend':'rust' if args.rust_url else 'hosted',
                              'rust_url':args.rust_url, 'query_identity':self.query_identity,
                              'binary_sha256':{name:hashlib.sha256(Path(getattr(args,name)).read_bytes()).hexdigest() for name in ('proxy','probe')},
                              'entrypoint_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
        self.save('configuration.json', config)

    def rpc(self, operation, body):
        env = dict(self.env, PROBE_URL='http://' + self.env['RUN_CACHE_API_URL'],
                   PROBE_INVOCATION=self.a.run_id, PROBE_SESSION_ID=self.a.run_id)
        result = subprocess.run([self.a.probe, operation], input=json.dumps(body), env=env,
                                text=True, capture_output=True, timeout=60)
        self.event('crafted-public-rpc', operation=operation, request=body,
                   returncode=result.returncode, stdout=result.stdout, stderr=result.stderr)
        if result.returncode:
            raise Refused('RPC failed: ' + self.clean(result.stderr))
        return json.loads(result.stdout)

    def check_relation(self, label, role, name, revision):
        actual = self.observe(label, self.scope.relation(role, name))
        properties = json.loads((self.work / 'physical' / (label + '_properties.json')).read_text())
        assert_physical(actual, properties, revision, 'table', self.query_identity)
        return actual

    def baseline(self):
        self.setup_project()
        (self.project / 'models/sources.yml').unlink()
        self.save('operation-manifest.json', {'version':1,
                  'client':'2.0.11', 'backend':'rust' if self.a.rust_url else 'hosted',
                  'query_identity':self.query_identity,
                  'classification':{'dbt':'actual-pinned-client','scope_mutations':'crafted-public-grpc'},
                  'steps':['confirmed upstream','required upstream reuse','deferred downstream build',
                           'required downstream reuse','scoped mappings','pending and failed visibility', 'failed replacement retains prior mapping'],
                  'relations':{role:{name:self.scope.relation(role,name) for name in ('upstream','downstream','decoy','pending','failed')}
                               for role in ('prod','dev')},
                  'reference':'independent literal SELECT plus declared duplicate/null typed rows',
                  'properties':{'Type':'MANAGED','Provider':'delta'}})
        config = "materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}"
        self.write_model('upstream', config, query(0, self.query_identity))
        self.write_model('downstream', config, "select id, amount, tag from {{ ref('upstream') }}")
        # A separate owned decoy makes a wrong returned mapping executable
        # SQL whose complete typed rows disagree with the independent oracle.
        decoy = self.scope.relation('dev', 'decoy')
        self.sql('create_decoy', f'CREATE TABLE {decoy} USING DELTA AS ' + query(1, self.query_identity))
        self.check_relation('decoy_before', 'dev', 'decoy', 1)
        calls = self.dbt('record_upstream', ['run', '--select', 'upstream'])
        submits = require_decision(calls, 'upstream', 'ready_to_execute')
        self.explain('record_upstream', submits)
        upstream = self.check_relation('recorded_upstream', 'prod', 'upstream', 0)
        calls = self.dbt('reuse_upstream', ['run', '--select', 'upstream'])
        self.explain('reuse_upstream', require_decision(calls, 'upstream', 'skip_execution'))
        uid = submits[0]['request']['dbt_node_state']['node_unique_id']
        expected_fqn = submits[0]['request']['target_table']
        for label, expected in [('deferred_first','ready_to_execute'), ('deferred_reuse','skip_execution')]:
            calls = self.dbt(label, ['run', '--select', 'downstream'], target='dev')
            resolutions = [r for r in calls if r['method'] == 'ResolveDeferredRelations']
            if len(resolutions) != 1 or resolutions[0]['response']['fqn_by_unique_id'].get(uid) != expected_fqn:
                raise Refused('actual deferred read lacks exact recorded upstream FQN')
            if expected_fqn != '`' + self.scope.relation('prod','upstream').replace('.', '`.`') + '`':
                raise Refused('expected fully backtick-qualified Databricks FQN')
            downstream = require_decision(calls, 'downstream', expected)
            if expected_fqn not in downstream[0]['request']['sql']:
                raise Refused('downstream SQL does not read the resolved production FQN')
            self.explain(label, downstream)
            actual = self.check_relation(label, 'dev', 'downstream', 0)
            independent = self.sql(label + '_reference', query(0, self.query_identity))
            if not same_result(actual, independent) or not same_result(actual, upstream):
                raise Refused('physical:independent-reference')
            self.check_relation(label + '_decoy', 'dev', 'decoy', 1)
            self.save('checks/' + label + '.json', {'exact_fqn':expected_fqn,
                      'required_decision':expected, 'full_typed_multiset_schema_properties':True,
                      'independent_literal_reference':True,'decoy_preserved':True})
        # Scope variations are deliberately crafted public requests. They do
        # not claim that arbitrary org headers authenticate to hosted State.
        request = deepcopy(resolutions[0]['request'])
        missing = 'model.survey_' + self.a.run_id + '.missing'
        request['node_unique_ids'] = [uid, missing]
        result = self.rpc('resolve', request)
        if result['fqn_by_unique_id'] != {uid:expected_fqn}:
            raise Refused('populated plus missing UID resolution mismatch')
        for label, updates in [('wrong_project',{'project_id':'missing_' + self.a.run_id}),
                               ('wrong_target',{'target_name':'missing_' + self.a.run_id}),
                               ('empty_target',{'target_name':''})]:
            body = dict(request, **updates)
            response = self.rpc('resolve', body)
            if response['fqn_by_unique_id']:
                raise Refused('scope leaked: ' + label)
        # Profile/project names are descriptive wire inputs; characterize
        # whether the stable project ID remains the authoritative scope.
        for label, updates in [('other_profile',{'profile_name':'other_profile'}),
                               ('other_project_name',{'project_name':'other_project'}),
                               ('empty_project_id',{'project_id':''}),
                               ('explicit_project_id',{'project_id':'155_' + self.a.run_id})]:
            body = dict(request, **updates)
            response = self.rpc('resolve', body)
            expected = {uid:expected_fqn} if label == 'empty_project_id' else {}
            if response['fqn_by_unique_id'] != expected:
                raise Refused('scope mismatch: ' + label)
            self.save('checks/' + label + '.json', {'request':body,'response':response,
                      'classification':'crafted-public-grpc'})
        # Submit-time visibility is characterized separately from confirmation.
        pending = deepcopy(submits[0]['request'])
        pending_uid = 'model.survey_' + self.a.run_id + '.pending'
        pending['table_namespace'] = pending['table_namespace'] + '_pending'
        pending['target_table'] = '`' + self.scope.relation('prod','pending').replace('.', '`.`') + '`'
        pending['dbt_node_state']['node_unique_id'] = pending_uid
        pending['labels'] = dict(pending['labels'], dbt_node_unique_id=pending_uid, dbt_node_name='pending')
        pending_response = self.rpc('submit', pending)
        if 'ready_to_execute' not in pending_response['response']:
            raise Refused('pending fresh identity did not execute')
        body = dict(request, node_unique_ids=[pending_uid])
        if self.rpc('resolve',body)['fqn_by_unique_id']:
            raise Refused('pending submit unexpectedly published a mapping')
        self.write_model('failed', config,
            "select cast(raise_error('owned_deferral_failure') as bigint) id, cast(0 as int) amount, cast(null as string) tag")
        lo = len(self.wire())
        try:
            self.dbt('failed_execution', ['run', '--select', 'failed'])
        except Refused as error:
            if ('client invocation exited 1;' not in str(error)
                    or '[USER_RAISED_EXCEPTION] owned_deferral_failure' not in str(error)):
                raise
        else:
            raise Refused('expected actual warehouse execution failure')
        calls = self.wire()[lo:]
        failed = [r for r in calls if r['method']=='SubmitEnrichedSQL']
        if len(failed) != 1 or any(r['method']=='ConfirmExecution' for r in calls):
            raise Refused('failed execution was not submitted unconfirmed')
        failed_uid = failed[0]['request']['dbt_node_state']['node_unique_id']
        body = dict(request, node_unique_ids=[failed_uid])
        if self.rpc('resolve',body)['fqn_by_unique_id']:
            raise Refused('failed execution unexpectedly published a mapping')
        # The same UID's next attempt writes another owned target and fails.
        # Deferral must retain the prior successful physical mapping.
        replacement = self.scope.relation('prod','pending').split('.')[-1]
        self.save('project/models/upstream.sql',
                  "{{ config(materialized='table', file_format='delta', alias='" + replacement +
                  "', state={'lag_tolerance':'0s','pre_clone':'never'}) }}\n" +
                  "select cast(raise_error('owned_deferral_replacement_failure') as bigint) id, "
                  "cast(0 as int) amount, cast(null as string) tag\n")
        lo = len(self.wire())
        try:
            self.dbt('failed_replacement', ['run','--select','upstream'])
        except Refused as error:
            if ('client invocation exited 1;' not in str(error)
                    or '[USER_RAISED_EXCEPTION] owned_deferral_replacement_failure' not in str(error)):
                raise
        else:
            raise Refused('expected replacement warehouse failure')
        calls = self.wire()[lo:]
        replacements = [r for r in calls if r['method']=='SubmitEnrichedSQL']
        if (len(replacements)!=1 or any(r['method']=='ConfirmExecution' for r in calls)
                or replacements[0]['request']['target_table']==expected_fqn):
            raise Refused('failed replacement did not reach a distinct target')
        if self.rpc('resolve',dict(request,node_unique_ids=[uid]))['fqn_by_unique_id'] != {uid:expected_fqn}:
            raise Refused('failed replacement displaced prior confirmed mapping')
        self.check_relation('prior_success_after_failed_replacement','prod','upstream',0)
        # Exercise the independent check against a real wrong-source relation.
        try:
            self.check_relation('wrong_source_mutation', 'dev', 'decoy', 0)
        except Refused as error:
            if str(error) != 'physical:rows':
                raise
            self.save('checks/wrong_source_mutation.json', {'observed_failure':str(error)})
        else:
            raise Refused('wrong-source decoy escaped physical checker')
        self.save('checks/scopes.json', {'populated_and_missing':True,
                  'wrong_project_target_profile_and_name_empty':True,
                  'missing_and_empty_project_id_use_named_scope':True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dbt', required=True)
    parser.add_argument('--proxy', required=True)
    parser.add_argument('--probe', required=True)
    parser.add_argument('--rust-url')
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--work-dir', required=True)
    parser.add_argument('--schema-lease', required=True)
    parser.add_argument('--query-identity')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    args = parser.parse_args()
    if args.rust_url:
        import re
        if not re.fullmatch(r'http://127\.0\.0\.1:\d+', args.rust_url):
            parser.error('--rust-url must be an owned loopback endpoint')
    try:
        return Deferral(args).run()
    except Exception as error:
        print('FAILED:', str(error), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
