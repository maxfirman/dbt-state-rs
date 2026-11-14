"""Certify a recording session before accepting a live campaign.

Old promoted JSONL fixtures remain replayable without a sidecar. A new live
campaign must retain its proxy's sidecar and finish the proxy gracefully.
"""
import json
from pathlib import Path


class CaptureIntegrityError(Exception):
    """Missing, damaged or unfinished capture evidence."""


def verify_capture(path):
    """Return decoded rows only if every started RPC has a durable outcome."""
    try:
        path = Path(path)
        state = json.loads(Path(str(path) + ".capture.json").read_text())
        rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
        if (state.get("complete") is not True or state.get("failed") is not False
                or type(state.get("started")) is not int or type(state.get("recorded")) is not int
                or state["started"] != state["recorded"] or state["recorded"] != len(rows)):
            raise CaptureIntegrityError("incomplete capture: started, recorded and durable RPC counts must match after clean shutdown")
        return rows
    except (OSError, ValueError, TypeError, AttributeError) as error:
        raise CaptureIntegrityError(f"cannot certify capture: {error}") from error
