"""Warehouse-free fault probes for #142 immutable policy reference artifacts."""
import copy
import unittest

from databricks_policy import PolicySnapshot, assert_policy_result
from databricks_survey import Refused

COLUMNS = [dict(name=n, type_text=t, type_name=k, position=i)
           for i,(n,t,k) in enumerate([('id','BIGINT','LONG'),('amount','INT','INT'),('tag','STRING','STRING')])]
OLD = [[1,10,'a_run'],[1,10,'a_run'],[2,None,'a_run'],[3,20,'b_run'],[4,None,'b_run']]
FRESH = [[1,17,'a_run'],[1,17,'a_run'],[2,None,'a_run'],[3,20,'b_run'],[4,None,'b_run']]
PROPERTIES = {'rows':[['Type','MANAGED'],['Provider','delta']]}


def result(rows):
    return {'columns':copy.deepcopy(COLUMNS), 'rows':[[None if x is None else str(x) for x in r] for r in rows]}


class PolicyReferences(unittest.TestCase):
    def setUp(self):
        self.snapshot = PolicySnapshot('build0', OLD, {'producer_a':100,'producer_b':101}, 102)

    def test_skip_accepts_saved_build_and_independent_fresh_drift(self):
        self.assertTrue(assert_policy_result(result(OLD), PROPERTIES, self.snapshot,
                        FRESH, 'skip_execution')['different_fresh_reference'])

    def test_build_requires_current_fresh_reference(self):
        assert_policy_result(result(FRESH), PROPERTIES, self.snapshot, FRESH, 'ready_to_execute')
        with self.assertRaisesRegex(Refused,'physical:rows'):
            assert_policy_result(result(OLD), PROPERTIES, self.snapshot, FRESH, 'ready_to_execute')

    def test_fresh_result_cannot_pass_as_policy_skip(self):
        with self.assertRaisesRegex(Refused,'physical:rows'):
            assert_policy_result(result(FRESH), PROPERTIES, self.snapshot, FRESH, 'skip_execution')

    def test_snapshot_is_immutable_after_caller_mutation(self):
        rows=copy.deepcopy(OLD); epochs={'producer_a':100,'producer_b':101}
        snap=PolicySnapshot('build0',rows,epochs,102)
        rows[0][1]=999; epochs['producer_a']=999
        self.assertEqual(snap.rows,tuple(map(tuple,OLD)))
        self.assertEqual(dict(snap.upstream_epochs),{'producer_a':100,'producer_b':101})
        with self.assertRaises((AttributeError,TypeError)):snap.build_epoch=999

    def test_duplicate_null_branch_schema_and_properties_faults_fail(self):
        changed=[]
        r=copy.deepcopy(OLD);r.pop(0);changed.append(result(r))
        r=copy.deepcopy(OLD);r[2][1]=0;changed.append(result(r))
        r=copy.deepcopy(OLD);r[0][2]='b_run';changed.append(result(r))
        r=result(OLD);r['columns'][1]['type_text']='BIGINT';changed.append(r)
        for actual in changed:
            with self.subTest(actual=actual), self.assertRaises(Refused):
                assert_policy_result(actual,PROPERTIES,self.snapshot,FRESH,'skip_execution')
        for props in ({'rows':[['Type','VIEW'],['Provider','delta']]},
                      {'rows':[['Type','MANAGED'],['Provider','parquet']]}):
            with self.assertRaisesRegex(Refused,'physical:properties'):
                assert_policy_result(result(OLD),props,self.snapshot,FRESH,'skip_execution')

    def test_unknown_or_duplicate_epochs_cannot_anchor_snapshot(self):
        for epochs in ({'a':None},{},[('a',100),('a',101)]):
            with self.subTest(epochs=epochs), self.assertRaises(Refused):
                PolicySnapshot('bad',OLD,epochs,102)


if __name__=='__main__':unittest.main()
