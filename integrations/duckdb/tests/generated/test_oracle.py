"""Fixed independent-oracle regressions (also used by the generated runner)."""
import unittest

from oracle import OracleViolation, expected, assert_snapshot


class ReferenceContract(unittest.TestCase):
    def test_duplicates_nulls_and_integer_schema_are_preserved(self):
        self.assertEqual(expected(0, 1), {
            'schema': [['id', 'INTEGER'], ['amount', 'INTEGER']],
            'rows': [[1, 11], [2, 21], [2, 21], [3, None]],
        })

    def test_wrong_skip_is_a_named_rows_violation(self):
        with self.assertRaisesRegex(OracleViolation, 'wrong-skip:rows'):
            assert_snapshot(expected(0, 1), [expected(0, 2)], 'wrong-skip')

    def test_copy_schema_loss_is_detected_even_when_rows_match(self):
        actual = expected(0, 1)
        actual['schema'][1][1] = 'BIGINT'
        with self.assertRaisesRegex(OracleViolation, 'copy:schema'):
            assert_snapshot(actual, [expected(0, 1)], 'copy')


if __name__ == '__main__':
    unittest.main()
