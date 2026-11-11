"""#104 (group b): a DBT_CUSTOM node's execution type change with an upstream.

A custom node sends only `lookback` in its semantic extras, a FULL table also
`on_schema_change`; a view (the only other kind with `lookback` alone)
reports no upstreams (`reason_kind_cells` `vc`, `cv`). A custom node that sets
`on_schema_change='ignore'` sends a FULL table's extras, so the two share an
identity over the same upstream.

Nodes (per-run, prod_state_demo; upstream: the jaffle-shop raw orders table):
  cf  R1 table (FULL), R2 custom (DBT_CUSTOM over FULL history)
  fc  R1 custom, R2 table (FULL over DBT_CUSTOM history), R3 custom (over
      its R1 build, rewritten by the R2 table build)
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from explain_capture import Build, Explain, Scenario, Write  # noqa: E402
from reason_kind_cells import MATERIALIZATION  # noqa: E402

TABLE = "materialized='table'"
CUSTOM = "materialized='${mat}', on_schema_change='ignore'"


def model(name, config):
    return Write(f"models/marts/${{{name}}}.sql",
                 f"{{{{ config({config}) }}}}\n"
                 f"select id, store_id as v, '{name}_${{run}}' as r "
                 f"from {{{{ source('${{src}}', 'raw_orders') }}}}\n")


def probe(node, intent, decision=None, et=None):
    return Explain(f"${{{node}}}", intent=intent, decision=decision, execution_type=et,
                   via="probe")


SCENARIO = Scenario(
    name="reason_kind_cells_custom",
    summary="#104b: DBT_CUSTOM (with on_schema_change) <-> FULL execution type change over "
            "an upstream",
    names=("src", "mat", "cf", "fc"),
    steps=[
        Write("macros/${mat}.sql", MATERIALIZATION),
        Write("models/${src}.yml", """sources:
  - name: ${src}
    schema: raw
    tables:
      - name: raw_orders
"""),
        model("cf", TABLE),
        model("fc", CUSTOM),
        Build("${cf}", "${fc}"),
        model("cf", CUSTOM),
        model("fc", TABLE),
        Build("${cf}", "${fc}"),
        probe("cf", "custom over FULL history, same SQL and extras, upstream", "EXECUTE", 11),
        probe("fc", "table over custom history, same SQL and extras, upstream", "EXECUTE", 1),
        model("fc", CUSTOM),
        Build("${fc}"),
        probe("fc", "custom over its R1 build and a later table build, upstream", None, 11),
    ],
)
