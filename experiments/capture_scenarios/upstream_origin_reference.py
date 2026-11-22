"""#237 follow-up of upstream_origin (review of PR #263): the Reference execution again, with
dbt State explicitly disabled (`--no-manage-state`) and the run's proxy as a trap for any
State call. Run it with `--run-id` of the run 1 capture so it rebuilds that run's dev nodes from
their inputs. Steps: `_upstream_origin.reference_steps`.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _upstream_origin import NAMES, reference_steps  # noqa: E402
from explain_capture import Scenario  # noqa: E402

SCENARIO = Scenario(
    name="upstream_origin_reference",
    summary="State-free Reference re-execution (--no-manage-state) of an upstream_origin run's dev "
            "nodes, with input and result reads (#237; use --run-id)",
    names=NAMES,
    steps=reference_steps("incremental"),
)
