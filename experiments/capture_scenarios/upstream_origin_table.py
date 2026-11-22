"""#237 run 2: dev upstream origin vs a FULL downstream's submit-time Result
copy, with a table upstream (the upstream kind of the earlier cross-schema
copies, e.g. `explain_clone_history.jsonl`). Cases and steps:
`_upstream_origin.py`.

B's dev upstream reads the shared prod input with prod's SQL, so hosted copies
it on submit (`ready_to_clone`) and the client confirms the copy. C and D's
dev upstream is first built on `dev_noclone` (`dev_clone` with
`allow_clones: false`), so hosted must build it (codes 1 / 10); the
`dev_clone` round that follows submits it again (expected SKIP over that
build) and then the downstream. Only those builds use `dev_noclone`; every
downstream submit carries `dev_clone`'s copy permissions.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _upstream_origin import NAMES, steps  # noqa: E402
from explain_capture import Scenario  # noqa: E402

SCENARIO = Scenario(
    name="upstream_origin_table",
    summary="dev upstream origin (same physical / confirmed copy / independent build / dev marker) "
            "vs a FULL downstream's submit-time Result copy and served rows; table upstream "
            "(#237 run 2)",
    names=NAMES,
    steps=steps("table"),
)
