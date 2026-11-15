#!/usr/bin/env python3
"""Independent retained/replaced/schema/reference faults for #148."""
import copy
import json
import unittest
from databricks_predicate_recovery import A,B,RB,C,C4,C5,assert_bag,literal,reference,failure_matches
from databricks_survey import Refused


def physical(rows):
    return {'columns':[dict(zip(('name','type_text','type_name','position'),c)) for c in (C4 if len(rows[0])==4 else C5)],
            'rows':copy.deepcopy(rows)}


class PredicateRecovery(unittest.TestCase):
    def test_replacement_preserves_only_outside_old_rows(self):
        assert_bag(physical(RB),RB)
        # Fresh input loses outside B; key-based merge retains predicate A/NULL
        # rows absent from input; deduplication loses a genuine duplicate.
        for bad in (B,RB+[A[-1]+[None]],RB[1:],RB[:-1],RB+[RB[1]]):
            with self.assertRaises(Refused):assert_bag(physical(bad),RB)
        # Widening now removes B's retained row, not just represented keys.
        assert_bag(physical(C),C)
        with self.assertRaises(Refused):assert_bag(physical(C+[RB[0]]),C)

    def test_complete_additive_schema_and_disabled_reference(self):
        actual=physical(RB);bad=copy.deepcopy(actual);bad['columns'][-1]['type_text']='INT'
        with self.assertRaises(Refused):assert_bag(bad,RB)
        typed=[[int(r[0]),int(r[1]),*r[2:]] for r in RB];sql=literal(RB)
        objects=[dict(zip(['id','amount','p','q','note'],r)) for r in typed]
        reference(json.dumps(objects),'',actual,typed,sql,sql+';',sql)
        for changed in (objects[:-1],[dict(o,note='fabricated') for o in objects]):
            with self.assertRaises(Refused):reference(json.dumps(changed),'',actual,typed,sql,sql+';',sql)

    def test_native_error_admission_rejects_unrelated_failures(self):
        schema=("[JinjaError (dbt1501)] The source and target schemas on this incremental model are out of sync! "
                "Source columns not in target: [{'name': 'note', 'dtype': 'STRING'}]")
        constraint=("[DELTA_REPLACE_WHERE_MISMATCH.INVARIANT_VIOLATION] Written data does not conform to "
                    "partial table overwrite condition or constraint '((p = 'A') OR (p IS NULL))'. "
                    "[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] violated by row with values: - p : B.")
        self.assertTrue(failure_matches('schema',schema))
        self.assertTrue(failure_matches('constraint',constraint))
        for text in ('REPLACE_WHERE', 'transport error: connection reset', 'HTTP 401 Unauthorized',
                     'network error: DNS resolution failed', 'gRPC deadline exceeded'):
            for kind in ('schema','constraint'):self.assertFalse(failure_matches(kind,text))
        for text in (constraint.replace('DELTA_VIOLATE_CONSTRAINT_WITH_VALUES','other'),
                     constraint.replace("p = 'A'","p = 'C'"),constraint.replace('p : B.','p : A.'),
                     constraint.replace('DELTA_REPLACE_WHERE_MISMATCH.INVARIANT_VIOLATION','other')):
            self.assertFalse(failure_matches('constraint',text))
        self.assertFalse(failure_matches('schema',schema.replace("'name': 'note'","'name': 'other'")))

if __name__=='__main__':unittest.main()
