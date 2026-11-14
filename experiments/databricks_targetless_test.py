"""Credential-free campaign checks at the subprocess and inventory boundaries."""
import argparse
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from databricks_targetless import Targetless
from databricks_survey import Refused


class SelectedCampaign(unittest.TestCase):
    def campaign(self, directory):
        # Avoid credential/profile discovery; the real campaign/project/dbt path runs.
        campaign = Targetless.__new__(Targetless)
        campaign.work = Path(directory)
        campaign.project = campaign.work / 'project'
        campaign.profiles = campaign.work / 'profiles'
        campaign.a = argparse.Namespace(run_id='20261114abcdef', mode='selected',
            dbt=__file__, proxy=__file__, probe=__file__)
        campaign.scope = argparse.Namespace(prefix='test')
        campaign.lease = None
        campaign.env = {}
        campaign.secrets = []
        campaign.events = []
        campaign.n = 0
        return campaign

    def execute(self, campaign, fail):
        def command(argv, **kwargs):
            name = argv[3]
            kind = 'analysis' if 'analysis' in name else name.split('_')[1]
            output = ('injected command failure' if fail else
                      'dbt State is enabled\n' + json.dumps([{'marker':'20261114abcdef_' + kind}]))
            return subprocess.CompletedProcess(argv, 17 if fail else 0, output, '')

        def start():
            campaign.save('golden/test.jsonl', '{"method":"ValidateClientVersion"}\n')
            campaign.save('golden/test.jsonl.capture.json',
                dict(complete=True, failed=False, started=1, recorded=1))

        with patch.object(campaign, 'inventory', return_value={'leased':[]}), \
             patch.object(campaign, 'start', side_effect=start), \
             patch.object(campaign, 'stop'), \
             patch('databricks_survey.subprocess.run', side_effect=command):
            return campaign.run()

    def test_failed_command_fails_campaign_after_preserving_original_evidence(self):
        with TemporaryDirectory() as directory:
            campaign = self.campaign(directory)
            with self.assertRaisesRegex(Refused, 'client invocation exited 17'):
                self.execute(campaign, fail=True)
            summary = json.loads((campaign.work/'summary.json').read_text())
            self.assertIn('client invocation exited 17', summary['failure'])
            check = json.loads(next((campaign.work/'checks').glob('*')).read_text())
            self.assertIn('injected command failure', check['error'])
            self.assertEqual(check['rows'], [])
            self.assertIn('injected command failure', next((campaign.work/'dbt').glob('*.log')).read_text())
            self.assertTrue(summary['inventory_unchanged'])

    def test_successful_nine_commands_still_accept(self):
        with TemporaryDirectory() as directory:
            campaign = self.campaign(directory)
            self.assertEqual(self.execute(campaign, fail=False), 0)
            summary = json.loads((campaign.work/'summary.json').read_text())
            self.assertIsNone(summary['failure'])
            self.assertEqual(summary['invocations'], 9)
            checks = [json.loads(p.read_text()) for p in (campaign.work/'checks').glob('*')]
            self.assertEqual(len(checks), 9)
            self.assertTrue(all(c['error'] is None and c['rows'] == [c['expected']] for c in checks))


if __name__ == '__main__':
    unittest.main()
