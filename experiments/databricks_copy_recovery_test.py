#!/usr/bin/env python3
"""Offline ownership and operation bounds for the explicitly cleared live driver."""
import copy
from collections import Counter
import unittest
import uuid

from databricks_copy_recovery import (CopyRecovery, FIRST, OBJECTS, RUNS, SCHEMAS,
    RecoveryScope, cells, direct_grant, guarded_metadata, mutations, plan)
from databricks_survey import Refused
from databricks_precopy import assert_clone_history, assert_copy_properties


class RecoveryGuards(unittest.TestCase):
    def test_repair_property_transition_is_explicit_and_bounded(self):
        source={'clusterByAuto':'true','delta.enableRowTracking':'true'}
        target={'clusterByAuto':'false','delta.enableRowTracking':'true'}
        with self.assertRaises(Refused):assert_copy_properties(source,target)
        check=assert_copy_properties(source,target,True)
        self.assertEqual(check['preserved'],['delta.enableRowTracking'])
        self.assertEqual(check['explicit_property_transition']['clusterByAuto']['target'],'false')
        for changed in ({'clusterByAuto':'true','delta.enableRowTracking':'true'},
                {'clusterByAuto':'false','delta.enableRowTracking':'false'},
                {'clusterByAuto':'false'}):
            with self.assertRaises(Refused):assert_copy_properties(source,changed,True)

    def test_exact_native_clone_merge_cluster_repair_history(self):
        import json
        source='cdo_demo_db.owned.out'
        history={'columns':[{'name':n} for n in ('version','operation','operationParameters')],
            'rows':[['2','CLUSTER BY',json.dumps({'oldClusteringColumns':'','newClusteringColumns':'','clusterByAuto':'false'})],
                    ['1','MERGE','{}'],['0','CLONE',json.dumps({'source':source,'sourceVersion':'1','isShallow':'true'})]]}
        assert_clone_history(history,source,True)
        with self.assertRaisesRegex(Refused,'physical-copy-history'):assert_clone_history(history,source)
        for field,value in [('version','3'),('operation','WRITE'),('source','cdo_demo_db.foreign.out'),('isShallow','false'),('clusterByAuto','true')]:
            changed=copy.deepcopy(history)
            if field in ('version','operation'):changed['rows'][1][0 if field=='version' else 1]=value
            else:
                row=changed['rows'][0 if field=='clusterByAuto' else 2]
                parameters=json.loads(row[2]);parameters[field]=value;row[2]=json.dumps(parameters)
            with self.subTest(field=field),self.assertRaises(Refused):assert_clone_history(changed,source,True)

    def test_legacy_top_level_refusal_and_exact_grant_mirrors(self):
        self.assertIsNone(direct_grant({'ready_to_clone':None,
            'unable_to_clone':{'explained_decision':{'clone_rejection_reason':10}},'response':None}))
        ready={'request_id':'opaque'}
        self.assertEqual(direct_grant({'ready_to_clone':ready,'response':{'ready_to_clone_v1':ready}}),ready)
        for response in ({'ready_to_clone':ready,'response':None},
                {'ready_to_clone':None,'response':{'ready_to_clone_v1':ready}}):
            with self.assertRaisesRegex(Refused,'direct-mirrors'):direct_grant(response)

    def test_exact_fresh_graph_and_budget(self):
        p=plan()
        self.assertEqual(sum(len(cells(k)) for k in ('full','merge')),25)
        self.assertEqual(Counter(m[-1] for m in mutations()),{'drop':9,'update':4})
        self.assertEqual(len(FIRST),2)
        for phase,oid in OBJECTS.items():
            self.assertEqual(uuid.UUID(oid).version,4)
            self.assertEqual(uuid.UUID(oid).hex,oid)
            self.assertEqual(len(set(p['names'][phase])),5)
            self.assertTrue(all('p154_'+oid+'_' in n for n in p['names'][phase]))
        self.assertTrue(set(p['names']['hosted']).isdisjoint(p['names']['rust']))

    def test_mutations_exclude_input_and_old_relations(self):
        self.assertEqual({m[1] for m in mutations()},{'full','merge'})
        self.assertEqual({m[2] for m in mutations()},{'prod','dev'})
        obj=CopyRecovery.__new__(CopyRecovery);obj.mutation_index=0
        # Refuse before trying any warehouse API, even inside the fresh graph.
        for operation in [('unexpected','full','prod','drop'),mutations()[1],('wrong','inputs','prod','drop')]:
            with self.assertRaisesRegex(Refused,'ordered-mutation-budget'):obj.mutate(*operation)

    def test_current_identity_owner_type_provider_guard(self):
        target=plan()['names']['hosted'][1]
        metadata={'full_name':target,'owner':'max.firman@gmail.com','table_id':str(uuid.uuid4()),
            'data_source_format':'DELTA','table_type':'MANAGED'}
        self.assertEqual(guarded_metadata(metadata,target),metadata['table_id'])
        for field,value in [('full_name',target+'x'),('owner','foreign'),('table_id',None),
                ('table_id','legacy'),('table_id',str(uuid.uuid1())),('data_source_format','PARQUET'),
                ('table_type','EXTERNAL')]:
            changed=dict(metadata);changed[field]=value
            with self.subTest(field=field,value=value),self.assertRaises(Refused):guarded_metadata(changed,target)


if __name__=='__main__':unittest.main()
