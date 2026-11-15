#!/usr/bin/env python3
"""#140 crafted public-gRPC epoch boundaries; no client/warehouse reachability claim.

Template is an actual client's complete request, but epochs, namespace, names and
history are deliberately synthetic. This runner executes no warehouse SQL.
"""
import argparse
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

from databricks_survey import Campaign, Refused
from capture_integrity import verify_capture


class Boundaries(Campaign):
    def rpc(self, operation, body):
        env = dict(self.env, PROBE_URL='http://' + self.env['RUN_CACHE_API_URL'],
                   PROBE_INVOCATION=self.a.run_id, PROBE_SESSION_ID=self.a.run_id)
        result = subprocess.run([self.a.probe, operation], input=json.dumps(body), env=env,
                                text=True, capture_output=True, timeout=60)
        self.event('crafted-rpc', operation=operation, request=body, returncode=result.returncode,
                   stdout=result.stdout, stderr=result.stderr)
        if result.returncode:
            raise Refused('crafted RPC failed: ' + self.clean(result.stderr))
        return json.loads(result.stdout)

    def run(self):
        self.save('boundary-runner.py', Path(__file__).read_text())
        self.save('classification.json', {'evidence_class':'crafted-public-grpc',
                  'actual_client_reachable':False,'warehouse_sql_executed':False})
        template = json.loads(Path(self.a.template).read_text())
        base = next(r['request'] for r in template if r['method'] == 'SubmitEnrichedSQL')
        now = int(time.time() * 1000) - 60000
        cells = [('equal',now,now), ('older',now,now-1), ('plus_one_ms',now,now+1),
                 ('missing_current',now,None), ('missing_recorded',None,now),
                 ('both_missing',None,None), ('missing_input',now,'absent'),
                 ('duplicate_input',now,'duplicate'), ('delayed_old_then_new',now,'delayed')]
        self.save('operation-manifest.json', {'version':1,'cells':cells,
                  'comparison_anchor':'recorded upstream epoch, not later consumer confirmation'})
        try:
            self.start()
            for name, recorded, current in cells:
                request = deepcopy(base)
                target = f'`cdo_demo_db`.`{self.scope.schema("prod")}`.`boundary_{self.a.run_id}_{name}`'
                source = f'`cdo_demo_db`.`{self.scope.schema("prod")}`.`source_{self.a.run_id}`'
                request.update(target_table=target, table_namespace='crafted_' + self.a.run_id + '_' + name,
                               sql=f"select id, '{name}' as boundary_cell from {source}", tables=[{'name':source,'last_modified_epoch':recorded}],
                               freshness_tolerance_seconds=0, allow_clones=False,
                               default_schema=self.scope.schema('prod'))
                uid = 'model.boundary_' + self.a.run_id + '.' + name
                request['labels'] = {'dbt_node_name':name,'dbt_node_unique_id':uid,'dbt_node_fqn':uid}
                request['dbt_node_state'].update(node_unique_id=uid,project_name='boundary_' + self.a.run_id,
                                               node_hash=hashlib.md5(uid.encode()).hexdigest())
                first = self.rpc('submit', request)['response']
                if 'ready_to_execute' not in first:
                    raise Refused('crafted fresh target did not build')
                confirm_epoch = now + 10000
                self.rpc('confirm', {'request_id':first['ready_to_execute']['request_id'],
                         'last_modified_epoch':confirm_epoch,'failed_to_clone':False,'table_type':'MANAGED',
                         'execution_results':None,'execution_runtime_ms':17,'labels':request['labels']})
                own = {'name':target,'last_modified_epoch':confirm_epoch}
                if current == 'absent': inputs = []
                elif current == 'duplicate': inputs = [{'name':source,'last_modified_epoch':now}]*2
                else: inputs = [{'name':source,'last_modified_epoch':now if current == 'delayed' else current}]
                request['tables'] = [own] + inputs
                result = self.rpc('submit', request)['response']
                payload = next(iter(result.values()))
                self.rpc('explain', {'execution_decision_ids':[payload['execution_decision_id']]})
                self.save('cells/' + name + '.json', {'evidence_class':'crafted-public-grpc',
                          'recorded_epoch':recorded,'submitted_epoch':current,'response':result})
                if current == 'delayed':
                    request['tables'][1]['last_modified_epoch'] = now + 1
                    later = self.rpc('submit', request)['response']
                    self.rpc('explain', {'execution_decision_ids':[next(iter(later.values()))['execution_decision_id']]})
                    self.save('cells/delayed_later_new.json', {'evidence_class':'crafted-public-grpc','response':later})
        finally:
            self.stop()
            certifications = []
            for path in sorted((self.work / 'golden').glob('*.jsonl')):
                rows = verify_capture(path)
                certifications.append({'path':str(path),'rows':len(rows),
                                       'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
            self.save('capture-certification.json', certifications)
        return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','run-id','template'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--rust-url')
    try:
        return Boundaries(parser.parse_args()).run()
    except Exception as error:
        print('FAILED:',str(error),file=sys.stderr)
        return 1


if __name__ == '__main__': sys.exit(main())
