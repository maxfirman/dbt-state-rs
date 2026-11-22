"""#237 run 1: dev upstream origin vs a FULL downstream's submit-time Result
copy, with a merge incremental upstream (`unique_key='id'`). Cases and
steps: `_upstream_origin.py`.

Hosted never copies an incremental on submit (FULL only), so the client's
pre-copy alone decides the dev upstream's origin: B keeps the default
`pre_clone` and the client copies it with RegisterClone (dev relation
absent) and confirms; C and D set `pre_clone: never`, so dev builds it.

Outcome (2026-11-21, run 234451b8ea12): only A's downstream was copied; B, C
and D found no copy source ("there is no other table that the model could be
cloned from"), so this upstream kind does not separate the cases. Run 2
(`upstream_origin_table.py`) uses a table upstream.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _upstream_origin import NAMES, steps  # noqa: E402
from explain_capture import Scenario  # noqa: E402

SCENARIO = Scenario(
    name="upstream_origin",
    summary="dev upstream origin (same physical / confirmed copy / independent build / dev marker) "
            "vs a FULL downstream's submit-time Result copy and served rows; merge incremental "
            "upstream (#237 run 1)",
    names=NAMES,
    steps=steps("incremental"),
)
