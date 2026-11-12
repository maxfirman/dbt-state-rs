"""Offline failure-path checks for complete hosted captures."""
import json
from pathlib import Path
import tempfile
import unittest

from capture_integrity import CaptureIntegrityError, verify_capture


class CaptureIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "golden_test.jsonl"
        self.path.write_text('{"service":"ClientValidation","method":"ValidateClientVersion",'
                             '"request":{},"response":{"is_supported":true}}\n')
        self.state(started=1, recorded=1, complete=True, failed=False)

    def state(self, **values):
        Path(str(self.path) + ".capture.json").write_text(json.dumps(values))

    def test_fixture_trim_keeps_rejected_methods_in_order(self):
        from explain_capture import trim
        error = {"method": "GetStateSelection", "status": {"code": 3}, "response": None}
        telemetry_error = {"method": "SubmitTelemetryBatch", "status": {"code": 14}, "response": None}
        kept, dropped = trim([error, {"method": "GetStateSelection"}, telemetry_error])
        self.assertEqual(kept, [error, telemetry_error])
        self.assertEqual(dict(dropped), {"GetStateSelection": 1})

    def test_clean_capture_passes(self):
        self.assertEqual(len(verify_capture(self.path)), 1)

    def test_dropped_line_is_not_a_successful_campaign(self):
        self.path.write_text("")
        with self.assertRaises(CaptureIntegrityError):
            verify_capture(self.path)

    def test_rpc_without_outcome_is_incomplete(self):
        self.state(started=2, recorded=1, complete=False, failed=False)
        with self.assertRaises(CaptureIntegrityError):
            verify_capture(self.path)

    def test_crashed_proxy_without_footer_is_incomplete(self):
        self.state(started=1, recorded=1, complete=False, failed=False)
        with self.assertRaises(CaptureIntegrityError):
            verify_capture(self.path)

    def test_write_failure_is_incomplete(self):
        self.state(started=1, recorded=1, complete=True, failed=True)
        with self.assertRaises(CaptureIntegrityError):
            verify_capture(self.path)

    def test_legacy_fixtures_cannot_establish_campaign_completeness(self):
        Path(str(self.path) + ".capture.json").unlink()
        with self.assertRaises(CaptureIntegrityError):
            verify_capture(self.path)

    def test_truncated_json_is_incomplete(self):
        self.path.write_text('{"service":')
        with self.assertRaises(CaptureIntegrityError):
            verify_capture(self.path)


if __name__ == "__main__":
    unittest.main()
