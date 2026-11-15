#!/usr/bin/env python3
"""Offline ownership and operation bounds for the explicitly cleared live driver."""
import copy
from collections import Counter
import unittest
import uuid

from databricks_copy_recovery import (CopyRecovery, FIRST, OBJECTS, RUNS, SCHEMAS,
    RecoveryScope, cells, guarded_metadata, mutations, plan)
from databricks_survey import Refused


class RecoveryGuards(unittest.TestCase):
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
