"""#83: follow-up to `execution_type_change`, run with that run's `--run-id`
so it extends the same nodes and their State history: an APPEND submit with
the same SQL and semantic extras over a history with no APPEND build.

  tv  history FULL, VIEW, FULL  -> incremental append (APPEND), explained
  mi  history MERGE, DELETE_INSERT, MERGE -> incremental append with the same
      unique_key (APPEND), submit only

The explained decision's State history is in the first run's window
(`history=False` here); the fixture is the two windows concatenated.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from execution_type_change import BODY  # noqa: E402
from explain_capture import Build, Explain, Scenario, Write  # noqa: E402

APPEND = "materialized='incremental', incremental_strategy='append'"


def model(name, config):
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config({config}) }}}}\n{BODY[name]}\n")


SCENARIO = Scenario(
    name="execution_type_change_append",
    summary="follow-up (--run-id of execution_type_change): APPEND over history without an "
            "APPEND build (#83)",
    names=("r83", "ta", "mi", "tv", "rf"),
    steps=[
        Write("models/${r83}.yml", """sources:
  - name: ${r83}
    schema: raw
    tables:
      - name: raw_orders
"""),
        model("tv", APPEND),
        model("mi", APPEND + ", unique_key='id'"),
        Build("${tv}", "${mi}"),
        Explain("${tv}", intent="APPEND over FULL, VIEW, FULL history, same SQL",
                execution_type=2, history=False),
    ],
)
