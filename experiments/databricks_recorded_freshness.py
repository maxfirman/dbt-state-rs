#!/usr/bin/env python3
"""Actual pinned-client same-logic RecordExecutions freshness, zero lag."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import sys
from databricks_freshness import Freshness, decisions
from databricks_survey import Refused


class Recorded(Freshness):
    entrypoint = Path(__file__)
    def baseline(self):
        self.output_name = 'recorded_consumer'
        self.setup_project()
        source = self.scope.relation('prod', 'inputs')
        self.save('project/models/sources.yml', {'version':2, 'sources':[{'name':'freshness',
                  'database':'cdo_demo_db','schema':self.scope.schema('prod'),
                  'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.sql('create_inputs', f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        tag = 'recorded_' + self.query_identity
        self.sql('initial_inputs', f"INSERT INTO {source} VALUES (1,10,'{tag}'),(1,10,'{tag}'),(2,NULL,NULL)")
        config = "materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}"
        self.write_model('producer', config, "select id, amount, tag from {{ source('freshness', 'inputs') }}")
        self.write_model('recorded_consumer', config, "select id, amount, tag from {{ ref('producer') }}")
        shutil.copytree(self.project, self.work / 'reference-project')
        rows = [['1','10',tag],['1','10',tag],['2',None,None]]
        initial = decisions(self.dbt('first', ['run']), 'ready_to_execute', ('producer','recorded_consumer'))
        self.explain('first', initial)
        # Reference builds the separate producer, then its own consumer.
        base = self.project
        self.project = self.work / 'reference-project'
        self.dbt('reference_producer', ['run','--select','producer'], target='ref', managed=False)
        self.project = base
        self.checked_results('first', rows, f'SELECT id, amount, tag FROM {source}')
        unchanged = decisions(self.dbt('unchanged', ['run']), 'skip_execution', ('producer','recorded_consumer'))
        self.explain('unchanged', unchanged)
        self.sql('changed_source', f'UPDATE {source} SET amount=17 WHERE id=1')
        self.sample('after_source_change')
        recorded = self.dbt('same_logic_write_only', ['run','--select','producer','--state-mode','write-only'])
        records = [row for row in recorded if row['method'] == 'RecordExecutions']
        self.save('recorded-producer.json', {'calls':recorded,'records':records})
        if not records:
            raise Refused('write-only mode did not emit actual RecordExecutions; cannot claim recorded-rebuild cell')
        if len(records) != 1 or records[0]['response'].get('records_stored') != 1:
            raise Refused('producer rebuild lacks one successful actual recording')
        published = records[0]['request']['records']
        if len(published) != 1:
            raise Refused('write-only phase recorded unexpected models')
        evidence = published[0]['input']['enriched_sql']
        prior = next(row['request'] for row in initial
                     if row['request']['labels']['dbt_node_name'] == 'producer')
        fields = ('target_table','dialect','default_catalog','default_schema','execution_type',
                  'sql','query_dependencies','semantic_extras','table_namespace')
        if evidence['execution_type'] != 1 or any(evidence[key] != prior[key] for key in fields):
            raise Refused('recorded producer changed logic/context/configuration or execution type')
        self.save('checks/same-logic-record.json', {'fields_equal':list(fields),
                  'actual_record_accepted':True,'execution_type':1})
        consumer = decisions(self.dbt('consumer_after_record', ['run','--select','recorded_consumer']), 'ready_to_execute', ('recorded_consumer',))
        self.explain('consumer_after_record', consumer)
        rows = [['1','17',tag],['1','17',tag],['2',None,None]]
        self.project = self.work / 'reference-project'
        self.dbt('reference_producer_changed', ['run','--select','producer'], target='ref', managed=False)
        self.project = base
        self.checked_results('consumer_after_record', rows, f'SELECT id, amount, tag FROM {source}')
        repeat = decisions(self.dbt('after_record_unchanged', ['run']), 'skip_execution', ('producer','recorded_consumer'))
        self.explain('after_record_unchanged', repeat)
        self.checked_results('after_record_unchanged', rows, f'SELECT id, amount, tag FROM {source}')
        self.save('checks/classification.json', {'evidence_class':'actual-pinned-client',
                  'same_logic_recorded_rebuild':True,'lag_tolerance_seconds':0})


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
        return Recorded(parser.parse_args()).run()
    except Exception as error:
        print('FAILED:', str(error), file=sys.stderr)
        return 1


if __name__ == '__main__': sys.exit(main())
