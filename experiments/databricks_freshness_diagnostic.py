#!/usr/bin/env python3
"""#140 mixed evidence: actual builds, then deliberately omitted source metadata.

The wrong reuse probe is crafted, even though its stale result is independently
observed in the real warehouse. It does not prove that dbt emits that request.
"""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import sys

from databricks_freshness import Freshness, assert_rows, decisions
from databricks_freshness_boundaries import Boundaries
from databricks_survey import Refused


class Diagnostic(Freshness):
    entrypoint = Path(__file__)
    rpc = Boundaries.rpc

    def baseline(self):
        self.setup_project()
        source = self.scope.relation('prod', 'inputs')
        target = self.scope.relation('prod', 'consumer')
        self.save('project/models/sources.yml', {'version':2,'sources':[{'name':'freshness',
                  'database':'cdo_demo_db','schema':self.scope.schema('prod'),
                  'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.sql('create_inputs', f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        tag = 'diagnostic_' + self.query_identity
        self.sql('initial_inputs', f"INSERT INTO {source} VALUES (1,10,'{tag}'),(1,10,'{tag}'),(2,NULL,NULL)")
        self.write_model('consumer', "materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}",
                         "select id, amount, tag from {{ source('freshness', 'inputs') }}")
        shutil.copytree(self.project, self.work / 'reference-project')
        calls = self.dbt('actual_first', ['run'])
        submits = decisions(calls, 'ready_to_execute')
        self.explain('actual_first', submits)
        old = [['1','10',tag],['1','10',tag],['2',None,None]]
        fresh = [['1','17',tag],['1','17',tag],['2',None,None]]
        self.checked_results('actual_first', old, f'SELECT id, amount, tag FROM {source}')
        self.sql('changed_source', f'UPDATE {source} SET amount=17 WHERE id=1')
        self.sample('changed_source')
        # Preserve real first-build identity and output epoch; deliberately
        # omit the source entry while SQL still reads that source.
        request = deepcopy(submits[0]['request'])
        confirm = next(r['request'] for r in calls if r['method']=='ConfirmExecution')
        request['tables'] = [{'name':request['target_table'],
                              'last_modified_epoch':confirm['last_modified_epoch']}]
        result = self.rpc('submit', request)['response']
        self.rpc('explain', {'execution_decision_ids':[next(iter(result.values()))['execution_decision_id']]})
        actual = self.observe('crafted_omitted_source_actual', target)
        independent = self.sql('crafted_omitted_source_reference', f'SELECT id, amount, tag FROM {source}')
        assert_rows(actual, old)
        assert_rows(independent, fresh)
        failure = None
        try:
            assert_rows(actual, fresh)
        except Refused as error:
            if str(error) != 'physical:rows': raise
            failure = str(error)
        if not failure:
            raise Refused('wrong-result diagnostic failed to detect stale target')
        self.save('checks/omitted-source.json', {'evidence_class':'crafted-public-grpc-over-real-warehouse',
                  'actual_client_reachable':False,'response':result,'actual_rows':actual,
                  'independent_current_rows':independent,'oracle_failure':failure})
        # The actual client receives complete current evidence and must repair.
        repaired = decisions(self.dbt('actual_repair', ['run']), 'ready_to_execute')
        self.explain('actual_repair', repaired)
        self.checked_results('actual_repair', fresh, f'SELECT id, amount, tag FROM {source}')
        # Seek close-spaced updates; retain both complete source snapshots and
        # SQL start/completion intervals. Failure to observe equal epochs is
        # explicitly not proof that they are impossible.
        attempts = []
        for value in (18,19):
            self.sql('close_write_' + str(value), f'UPDATE {source} SET amount={value} WHERE id=1')
            physical = self.observe('close_write_' + str(value), source)
            assert_rows(physical, [['1',str(value),tag],['1',str(value),tag],['2',None,None]])
            attempts.append(self.sample('close_write_' + str(value)))
        self.save('precision-attempt.json', {'evidence_class':'actual-warehouse',
                  'samples':attempts,'equal_wire_epoch_change_established':False,
                  'limitation':'No intervening client submit; history timestamps alone do not prove wire reachability.'})
        latest = decisions(self.dbt('actual_close_write', ['run']), 'ready_to_execute')
        self.explain('actual_close_write', latest)
        self.checked_results('actual_close_write', [['1','19',tag],['1','19',tag],['2',None,None]],
                             f'SELECT id, amount, tag FROM {source}')
        self.sql('missing_source', f'DROP TABLE {source}')
        before = len(self.wire())
        try:
            self.dbt('actual_missing_source', ['run'])
        except Refused as error:
            message = str(error)
            if 'TABLE_OR_VIEW_NOT_FOUND' not in message or source.split('.')[-1] not in message:
                raise
            calls = self.wire()[before:]
            if any(row['method'] == 'SubmitEnrichedSQL' for row in calls):
                raise Refused('missing source reached ordinary Submit; classify its actual metadata separately')
            self.save('checks/missing-source.json', {'evidence_class':'actual-pinned-client',
                      'failure':message,'ordinary_submits':sum(r['method']=='SubmitEnrichedSQL' for r in calls),
                      'limitation':'Absent relation failure is not a client-emitted None epoch.'})
        else:
            raise Refused('missing source unexpectedly succeeded; investigate actual-client safety')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('dbt','proxy','probe','work-dir','schema-lease'):
        parser.add_argument('--' + key, required=True)
    parser.add_argument('--rust-url')
    parser.add_argument('--profiles', default=str(Path.home() / '.dbt/profiles.yml'))
    parser.add_argument('--profile', default='databricks')
    parser.add_argument('--warehouse', default='47fd3fa312624d91')
    parser.add_argument('--query-identity')
    parser.add_argument('--run-id', default=datetime.now(timezone.utc).strftime('%Y%m%d') + os.urandom(3).hex())
    try:
        return Diagnostic(parser.parse_args()).run()
    except Exception as error:
        print('FAILED:',str(error),file=sys.stderr)
        return 1


if __name__ == '__main__': sys.exit(main())
