"""#73 (73a): APPEND (2) and MERGE (3) with the dev-clone pre-step turned off
(`state.pre_clone: never`), so the only copy path left is the submit-time
Result copy: does hosted answer `ready_to_clone` to an incremental submit for
an absent dev relation whose prod equivalent exists, and with which tree?

One attempt (spec #40: one attempt per negative copy probe).

Nodes (per-run):
  s73s   source table in prod_state_demo created by this run
  sa73   append incremental over s73s, pre_clone never
  sm73   merge incremental over s73s, pre_clone never
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
NEVER = ", state={'pre_clone': 'never'}"


def model(name, config):
    return Write(f"models/marts/${{{name}}}.sql",
                 f"{{{{ config({config}{NEVER}) }}}}\n"
                 "select id, v from {{ source('${s73s}', '${s73s}') }}\n")


SCENARIO = Scenario(
    name="incremental_submit_copy",
    summary="append and merge with pre_clone never: submit-time Result copy into an absent "
            "dev relation, and its tree (#73)",
    names=("s73s", "sa73", "sm73"),
    steps=[
        Write("models/${s73s}.yml", """sources:
  - name: ${s73s}
    schema: prod_state_demo
    tables:
      - name: ${s73s}
"""),
        model("sa73", "materialized='incremental', incremental_strategy='append'"),
        model("sm73", "materialized='incremental', incremental_strategy='merge', unique_key='id'"),
        Sql(f"create or replace table {DEMO}.${{s73s}} as select 1 as id, 's0' as v"),

        # Prod: first builds, confirmed (the equivalents).
        Build("${sa73}", "${sm73}"),

        # Dev: absent relations, no pre-step, so the submit decides.
        Build("${sa73}", "${sm73}", target="dev_clone"),
        Explain("${sa73}", intent="append submit into an absent dev relation (copy?)",
                execution_type=2, target="dev_clone"),
        Explain("${sm73}", intent="merge submit into an absent dev relation (copy?)",
                execution_type=3, target="dev_clone"),
    ],
)
