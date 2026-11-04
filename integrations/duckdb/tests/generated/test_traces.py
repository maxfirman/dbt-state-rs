"""Public replay/minimization contract, independent of production internals."""
import unittest
from traces import generate, without, validate, minimize


class TraceContract(unittest.TestCase):
    def test_removing_build_removes_its_copy_and_confirmation(self):
        trace = generate(0, 1)
        operations = trace['operations']
        build = next(op for op in operations if op['op'] == 'execute')
        candidate = dict(trace, operations=without(operations, {build['id']}))
        validate(candidate)
        original_episode_end = next(i for i, op in enumerate(operations[1:], 1)
                                    if op['op'] == 'reset')
        removed_descendants = {op['id'] for op in operations[2:original_episode_end]}
        self.assertFalse(removed_descendants & {op['id'] for op in candidate['operations']})
        self.assertTrue(any(op['op'] == 'execute' for op in candidate['operations']))

    def test_symbolic_reference_must_be_a_declared_prerequisite(self):
        trace = {'version': 1, 'operations': [
            {'id': 's', 'op': 'submit', 'needs': []},
            {'id': 'e', 'op': 'execute', 'submit': 's', 'needs': []},
        ]}
        with self.assertRaisesRegex(ValueError, 'undeclared prerequisite'):
            validate(trace)

    def test_minimizer_retains_required_source_for_same_failure(self):
        trace = generate(3, 2)
        wanted = trace['operations'][9]['id']
        minimal = minimize(trace, lambda t: any(op['id'] == wanted for op in t['operations']))
        validate(minimal)
        self.assertIn(wanted, [op['id'] for op in minimal['operations']])
        self.assertLess(len(minimal['operations']), len(trace['operations']))


if __name__ == '__main__':
    unittest.main()
