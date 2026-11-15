#!/usr/bin/env python3
"""Opt-in #136 identifier/resolution characterization with independent results."""
import argparse
import copy
import hashlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
import subprocess
import time

from databricks_baseline import Baseline, LeaseScope, assert_physical, query
from databricks_survey import Refused, check_manifest, complete_result, same_result


class IdentityScope(LeaseScope):
    """Exact owned names only; no prior relation or schema may be mutated."""
    def __init__(self, run, ledger):
        super().__init__(run, ledger)
        self.relation_prefix = 'i_' + run + '_'
        self.allowed = {f'cdo_demo_db.{schema}.{self.relation_prefix}{name}'
                        for schema in self.schemas for name in ('input', 'result', 'input_view')}


class Identity(Baseline):
    def __init__(self, args):
        super().__init__(args)
        if not self.lease:
            raise Refused('identifier campaign requires the explicit schema lease')
        self.scope = IdentityScope(args.run_id, self.lease)
        # Shared Cargo outputs may be rebuilt by another campaign. Every
        # launched binary is retained under this campaign's exclusive directory.
        for name in ('proxy','probe'):
            owned=self.work/'bin'/name
            owned.parent.mkdir(exist_ok=True)
            shutil.copy2(getattr(args,name),owned)
            setattr(args,name,str(owned))
        self.save('identity-runner.py', Path(__file__).read_text())
        self.save('identity-scope.json', {'allowed': sorted(self.scope.allowed)})
        configuration=json.loads((self.work/'configuration.json').read_text())
        configuration['identity']={'entrypoint_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                                   'physical_allowlist':sorted(self.scope.allowed),
                                   'actual_client_steps':22,'crafted_boundary_submits':22,
                                   'owned_binaries':{name:str(getattr(args,name)) for name in ('proxy','probe')}}
        self.save('configuration.json',configuration)

    def baseline(self):
        self.setup_project()
        (self.project / 'models' / 'sources.yml').unlink()
        base = self.project
        reference = self.work / 'reference-project'
        shutil.copytree(base, reference)
        for role, revision in [('prod', 0), ('ref', 1)]:
            self.sql('create_' + role + '_input', 'CREATE TABLE ' + self.scope.relation(role, 'input')
                     + ' USING DELTA AS ' + query(revision, self.query_identity))
            self.sql('create_' + role + '_view', 'CREATE VIEW ' + self.scope.relation(role,'input_view')
                     + ' AS SELECT id, amount, tag FROM ' + self.scope.relation(role,'input'))
        source = self.scope.relation('prod', 'input')
        catalog, schema, name = source.split('.')
        collision = self.scope.relation('ref', 'input')
        steps = [('full', source, 0), ('schema_qualified', schema + '.' + name, 0),
                 ('unqualified', name, 0), ('backticks', '.'.join('`'+p+'`' for p in source.split('.')), 0),
                 ('catalog_case', '.'.join([catalog.upper(), schema, name]), 0),
                 ('schema_case', '.'.join([catalog, schema.upper(), name]), 0),
                 ('relation_case', '.'.join([catalog, schema, name.upper()]), 0),
                 ('quoted_case', '.'.join('`'+p.upper()+'`' for p in source.split('.')), 0),
                 ('collision', collision, 1), ('return_original', source, 0),
                 ('view_source', self.scope.relation('prod','input_view'), 0)]
        self.save('operation-manifest.json', {'version': 1, 'kind':'actual-pinned-dbt',
                  'query_identity': self.query_identity, 'steps': steps,
                  'relations': sorted(self.scope.allowed), 'lag_tolerance':'0s'})
        results = []
        for label, spelling, revision in steps:
            sql = 'select id, amount, tag from ' + spelling
            self.project = base
            self.write_model('result', "materialized='table', file_format='delta', state={'lag_tolerance':'0s','pre_clone':'never'}", sql)
            if label == 'full':
                self.dbt('inventory', ['parse'], managed=False)
                manifest = json.loads(next((self.work / 'targets').glob('*_inventory/manifest.json')).read_text())
                check_manifest(self.scope, manifest)
                for node in manifest.get('nodes', {}).values():
                    self.scope.check('.'.join([node['database'], node['schema'], node['alias']]))
            for suffix in ('', '_unchanged'):
                calls = self.dbt(label + suffix, ['run'])
                submits = [r for r in calls if r['method'] == 'SubmitEnrichedSQL']
                if len(submits) != 1:
                    raise Refused('identifier slice must submit exactly one model')
                response = submits[0]['response']['response']
                decision = next(iter(response))
                confirms = {r['request']['request_id'] for r in calls if r['method']=='ConfirmExecution'
                            and (r.get('response') or {}).get('success') is True}
                if decision == 'ready_to_execute' and response[decision]['request_id'] not in confirms:
                    raise Refused('build lacks successful confirmation')
                if suffix and decision != 'skip_execution':
                    raise Refused('unchanged identifier must reuse')
                required = 'ready_to_execute' if label in ('full','collision','return_original','view_source') and not suffix else 'skip_execution'
                if decision != required:
                    raise Refused('hosted identifier contract: '+label+suffix+' expected '+required+', got '+decision)
                self.explain(label + suffix, submits)
                actual = self.observe(label + suffix, self.scope.relation('prod','result'))
                properties = json.loads((self.work/'physical'/(label+suffix+'_properties.json')).read_text())
                assert_physical(actual, properties, revision, 'table', self.query_identity)
                # State-disabled dbt and SQL execute the declared source independently.
                self.project = reference
                independent_sql = 'select id, amount, tag from ' + (collision if revision else source)
                self.write_model('result', "materialized='table', file_format='delta'", independent_sql)
                if self.dbt('reference_' + label + suffix, ['run'], target='ref', managed=False):
                    raise Refused('State-disabled reference sent RPCs')
                expected = self.observe('reference_' + label + suffix, self.scope.relation('ref','result'))
                direct = self.sql('reference_select_' + label + suffix, independent_sql.replace('select ','SELECT ',1))
                if not same_result(actual, expected) or not same_result(actual, direct):
                    raise Refused('reference:rows-or-schema')
                self.project = base
                results.append({'step':label+suffix, 'decision':decision,'revision':revision,
                                'request':submits[0]['request'], 'response':submits[0]['response']})
                self.save('identity-results.json', results)
                if label=='unqualified' and suffix:
                    self.boundary(submits[0]['request'], 'unqualified_')
        self.boundary(results[-1]['request'])
        return {'steps':len(results)}

    def probe(self, label, request):
        env = dict(self.env, PROBE_URL='http://' + self.env['RUN_CACHE_API_URL'])
        lo = len(self.wire())
        result = subprocess.run([self.a.probe, 'submit'], input=json.dumps(request), env=env,
                                text=True, capture_output=True, timeout=60)
        self.save('boundary/' + label + '.json', {'kind':'crafted-from-pinned-request',
                  'request':request, 'returncode':result.returncode,
                  'stdout':result.stdout,'stderr':result.stderr})
        time.sleep(.1)
        calls = self.wire()[lo:]
        submits = [row for row in calls if row['method']=='SubmitEnrichedSQL']
        if result.returncode or len(submits)!=1:
            raise Refused('boundary submit failed: ' + label)
        decision = next(iter(submits[0]['response']['response']))
        required = 'ready_to_execute' if label in ('unqualified_root_schema_other','unqualified_root_catalog_other') else 'skip_execution'
        if decision != required:
            raise Refused('hosted boundary contract: '+label+' expected '+required+', got '+decision)
        self.explain('boundary_' + label, submits)
        return submits[0]

    def context_read(self, label, role, revision):
        relation=self.scope.relation(role,'input')
        name=relation.split('.')[-1]
        event=self.event('sql-context-read',label=label,statement='SELECT id, amount, tag FROM '+name,
                         catalog='cdo_demo_db',schema=self.scope.schema(role))
        response=self.api('post','/api/2.0/sql/statements', {
            'warehouse_id':self.a.warehouse,'statement':event['statement'],
            'catalog':'cdo_demo_db','schema':self.scope.schema(role),
            'wait_timeout':'30s','disposition':'INLINE','format':'JSON_ARRAY'})
        deadline=time.monotonic()+600
        while response.get('status',{}).get('state') in ('PENDING','RUNNING'):
            if time.monotonic()>deadline:
                raise Refused('context read timed out')
            time.sleep(1)
            response=self.api('get','/api/2.0/sql/statements/'+response['statement_id'])
        self.save('context/'+label+'.json',response)
        actual=complete_result(response,require_rows=True)
        props=self.sql('context_'+label+'_properties','DESCRIBE TABLE EXTENDED '+relation)
        assert_physical(actual,props,revision,'table',self.query_identity)
        event['state']='SUCCEEDED'
        self.save('operations.json',self.events)

    def boundary(self, baseline, prefix="qualified_"):
        # Every crafted call derives from a real, confirmed pinned-client history.
        # Build proposals remain pending, so they never publish invented epochs.
        # Independently execute identical unqualified SQL under each owned context.
        for role, revision in [('prod',0),('ref',1)]:
            self.context_read(prefix+role, role, revision)
        variants = [
            ('root_schema_other', {'default_schema':self.scope.schema('ref')}),
            ('root_schema_case', {'default_schema':self.scope.schema('prod').upper()}),
            ('root_catalog_case', {'default_catalog':'CDO_DEMO_DB'}),
            ('root_catalog_other', {'default_catalog':'workspace'}),
            ('root_schema_absent', {'default_schema':None}),
            ('target_unquoted', {'target_table':self.scope.relation('prod','result')}),
            ('target_case', {'target_table':baseline['target_table'].upper()}),
        ]
        observations=[]
        for label, fields in variants:
            request=copy.deepcopy(baseline)
            request.update(fields)
            row=self.probe(prefix+label, request)
            observations.append({'step':label,'decision':next(iter(row['response']['response'])),
                                 'request':request,'response':row['response']})
            self.save('boundary-'+prefix+'results.json',observations)
        # Metadata alias probes change only one physical name; record actual
        # statuses/decisions rather than assuming Warehouse resolution is State scope.
        for label, which in [('input_unquoted','input'),('own_unquoted','result')]:
            request=copy.deepcopy(baseline)
            original='`'+'`.`'.join(self.scope.relation('prod',which).split('.'))+'`'
            matching=[t for t in request['tables'] if t['name']==original]
            if len(matching)!=1:
                raise Refused('missing actual metadata name for boundary '+label)
            matching[0]['name']=self.scope.relation('prod',which)
            row=self.probe(prefix+label,request)
            observations.append({'step':label,'decision':next(iter(row['response']['response'])),
                                 'request':request,'response':row['response']})
            self.save('boundary-'+prefix+'results.json',observations)
        if baseline['query_dependencies']:
            for field, value in [('default_catalog','cdo_demo_db'),('default_schema',self.scope.schema('ref'))]:
                request=copy.deepcopy(baseline)
                request['query_dependencies'][0][field]=value
                label='dependency_'+field
                row=self.probe(prefix+label,request)
                observations.append({'step':label,'decision':next(iter(row['response']['response'])),
                                     'request':request,'response':row['response']})
                self.save('boundary-'+prefix+'results.json',observations)
        row=self.probe(prefix+'unchanged_control',baseline)
        if 'skip_execution' not in row['response']['response']:
            raise Refused('pending boundary calls changed confirmed history')
        observations.append({'step':'unchanged_control','decision':'skip_execution',
                             'request':baseline,'response':row['response']})
        self.save('boundary-'+prefix+'results.json',observations)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dbt',required=True)
    p.add_argument('--proxy',required=True)
    p.add_argument('--probe',required=True)
    p.add_argument('--work-dir',required=True)
    p.add_argument('--schema-lease',required=True)
    p.add_argument('--query-identity')
    p.add_argument('--rust-url')
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
    p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91')
    p.add_argument('--run-id',default=datetime.now(timezone.utc).strftime('%Y%m%d')+os.urandom(3).hex())
    args=p.parse_args()
    if args.rust_url:
        import re
        if not re.fullmatch(r'http://127\.0\.0\.1:[1-9]\d*',args.rust_url):
            p.error('Rust endpoint must be owned literal loopback HTTP')
    try:
        return Identity(args).run()
    except Exception as error:
        print('FAILED:',str(error),file=sys.stderr)
        return 1

if __name__=='__main__':
    sys.exit(main())
