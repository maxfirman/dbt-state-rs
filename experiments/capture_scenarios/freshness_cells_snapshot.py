"""#104 (104a): follow-up to `freshness_cells`, run with that run's `--run-id`
more than 45 minutes after its round 1, so it extends the same nodes:

  fd_sn  snapshot over fs1, built in round 1 and reused within the default
         45 minute Lag tolerance after fs1 drifted (rounds 2-3). Now its build
         is older than the tolerance (a snapshot cannot set its own), so the
         drift rebuilds it (`upstream_outdated`, SHAPE 9 data only).

The explained decision's State history is in the first run's window
(`history=False` here); the fixture is the two windows concatenated.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from explain_capture import Build, Explain, Scenario, Write  # noqa: E402
from freshness_cells import NAMES, ONE, SOURCES, WINDOW, snapshot  # noqa: E402

SCENARIO = Scenario(
    name="freshness_cells_snapshot",
    summary="follow-up (--run-id of freshness_cells, 45+ minutes later): snapshot drift past "
            "the default Lag tolerance (#104)",
    names=NAMES,
    steps=[
        Write("models/${fs1}.yml", SOURCES),
        snapshot("fd_sn", ONE),
        Build("${fd_sn}", args=WINDOW),
        Explain("${fd_sn}", intent="snapshot drift past the default 45 minute Lag tolerance",
                decision="EXECUTE", execution_type=7, history=False),
    ],
)
