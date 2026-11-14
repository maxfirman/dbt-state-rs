"""Bounded #140 follow-up: actual repeated source references and crafted identical evidence."""
from argparse import ArgumentParser
from copy import deepcopy
from pathlib import Path
import json
import shutil
from databricks_freshness import Freshness, decisions
from databricks_freshness_boundaries import Boundaries
from databricks_survey import Refused

class Followup(Freshness):
    entrypoint = Path(__file__)
    rpc = Boundaries.rpc
    def __init__(self, args):
        super().__init__(args)
        configuration = json.loads((self.work / "configuration.json").read_text())
        configuration["freshness"]["evidence_class"] = "mixed-actual-client-and-crafted-public-grpc"
        self.save("configuration.json", configuration)
    def baseline(self):
        self.setup_project()
        source = self.scope.relation('prod','inputs')
        self.save('project/models/sources.yml', {'version':2,'sources':[{'name':'freshness','database':'cdo_demo_db','schema':self.scope.schema('prod'),'tables':[{'name':'inputs','identifier':source.split('.')[-1]}]}]})
        self.sql('create_inputs',f'CREATE TABLE {source} (id BIGINT, amount INT, tag STRING) USING DELTA')
        tag='followup_'+self.query_identity
        self.sql('initial_inputs',f"INSERT INTO {source} VALUES (1,10,'{tag}'),(1,10,'{tag}'),(2,NULL,NULL)")
        self.write_model('consumer',"materialized='table',file_format='delta',state={'lag_tolerance':'0s','pre_clone':'never'}", "select a.id, a.amount, a.tag from {{ source('freshness','inputs') }} a join (select distinct id from {{ source('freshness','inputs') }}) b on a.id=b.id")
        shutil.copytree(self.project,self.work/'reference-project')
        self.save('classification.json', {'actual_client':'Repeated SQL source references; actual emitted table entries retained without editing.', 'crafted':'Append an exact copy of the complete single known source TableModifiedInfo after genuine client builds.', 'scope':'No claim that dbt emits duplicate metadata; no missing/conflicting duplicate normalization.'})
        for label, expected, amount in [('first','ready_to_execute',10),('unchanged','skip_execution',10),('changed','ready_to_execute',17),('changed_unchanged','skip_execution',17)]:
            if label=='changed':
                self.sql('update_source',f'UPDATE {source} SET amount=17 WHERE id=1')
                self.sample('changed')
            calls=self.dbt(label,['run'])
            submits=decisions(calls,expected)
            self.explain(label,submits)
            rows=[['1',str(amount),tag],['1',str(amount),tag],['2',None,None]]
            direct=f'SELECT id, amount, tag FROM {source}'
            self.checked_results(label, rows, direct)
            if expected=='skip_execution':
                request=deepcopy(submits[0]['request'])
                inputs=[t for t in request['tables'] if t['name']!=request['target_table']]
                if len(inputs)!=1 or inputs[0].get('last_modified_epoch') is None:
                    raise Refused('actual client did not emit one unique known source; investigate before crafting')
                request['tables'].append(deepcopy(inputs[0]))
                result=self.rpc('submit',request)['response']
                self.rpc('explain',{'execution_decision_ids':[next(iter(result.values()))['execution_decision_id']]})
                self.save('crafted/'+label+'.json',{'evidence_class':'crafted-public-grpc-over-real-warehouse','actual_client_reachable':False,'request':request,'response':result})
                if 'skip_execution' not in result:
                    raise Refused('identical known duplicate did not reuse')
                self.checked_results('crafted_'+label,rows,direct)
        self.save('reachability.json',{'repeated_source_sql':True,'actual_client_duplicate_metadata_observed':False,'limitation':'This query is one actual-client control; it does not prove duplicates are unreachable in all queries.'})
        return {'actual_required_builds':2,'actual_required_reuses':2,'crafted_required_reuses':2}

p=ArgumentParser()
for key in ('dbt','proxy','probe','work-dir','schema-lease','run-id'):p.add_argument('--'+key,required=True)
p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'))
p.add_argument('--profile',default='databricks')
p.add_argument('--warehouse',default='47fd3fa312624d91')
p.add_argument('--query-identity')
p.add_argument('--rust-url')
raise SystemExit(Followup(p.parse_args()).run())
