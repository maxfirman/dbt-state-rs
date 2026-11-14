"""Credential-free public CLI checks for same-lifetime continuation admission."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class ContinuationAdmission(unittest.TestCase):
    def refused(self, success=True, prior_run='20261114a141aa', backend='hosted'):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prior = root / 'prior'
            prior.mkdir()
            (prior / 'summary.json').write_text(json.dumps({'success': success}))
            (prior / 'configuration.json').write_text(json.dumps({'run_id': prior_run,
                'baseline': {'backend': backend}}))
            result = subprocess.run([sys.executable,
                str(Path(__file__).with_name('databricks_target_lifecycle_continuation.py')),
                '--prerequisite', str(prior), '--run-id', '20261114a141aa',
                '--dbt', str(root / 'absent-dbt'), '--proxy', str(root / 'absent-proxy'),
                '--probe', str(root / 'absent-probe'), '--schema-lease', str(root / 'absent-lease'),
                '--work-dir', str(root / 'new')], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((root / 'new').exists())
            return result.stderr

    def test_failed_history_cannot_be_retried_as_a_continuation(self):
        self.assertIn('requires successful exact phase lifetime', self.refused(success=False))

    def test_other_run_cannot_admit_existing_names(self):
        self.assertIn('requires successful exact phase lifetime', self.refused(prior_run='20261114b141bb'))

    def test_other_backend_cannot_claim_prerequisite_history(self):
        self.assertIn('backend must match prerequisite', self.refused(backend='rust'))


if __name__ == '__main__':
    unittest.main()
