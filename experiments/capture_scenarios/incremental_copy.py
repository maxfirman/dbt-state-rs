"""#73 (73a): APPEND (2) and MERGE (3) Result copy through the dev-clone
pre-step, and the post-copy decision's Explain tree.

The `dev_clone` target defers to `prod_demo` through the profile's
`defer_to_target` (auto-defer), and `state.pre_clone` is unset, so the client
uses its default, IfTableMissing: an incremental model whose dev relation is
absent is copied with `Clone.RegisterClone` and confirmed before its
materialization submit.

Nodes (per-run):
  c73s   source table in prod_state_demo created by this run
  ca73   append incremental over c73s
  cm73   merge incremental over c73s
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"


def model(name, config):
    return Write(f"models/marts/${{{name}}}.sql",
                 f"{{{{ config({config}) }}}}\n"
                 "select id, v from {{ source('${c73s}', '${c73s}') }}\n")


SCENARIO = Scenario(
    name="incremental_copy",
    summary="append and merge Result copy via the dev-clone pre-step (RegisterClone), "
            "then the post-copy decision and its tree (#73)",
    names=("c73s", "ca73", "cm73"),
    steps=[
        Write("models/${c73s}.yml", """sources:
  - name: ${c73s}
    schema: prod_state_demo
    tables:
      - name: ${c73s}
"""),
        model("ca73", "materialized='incremental', incremental_strategy='append'"),
        model("cm73", "materialized='incremental', incremental_strategy='merge', unique_key='id'"),
        Sql(f"create or replace table {DEMO}.${{c73s}} as select 1 as id, 'c0' as v"),

        # Prod: first builds, confirmed (the copy sources).
        Build("${ca73}", "${cm73}"),

        # Dev: the relations are absent, so the pre-step copies them, then submits.
        Build("${ca73}", "${cm73}", target="dev_clone"),
        Explain("${ca73}", intent="append decision after the dev-clone copy",
                execution_type=2, target="dev_clone"),
        Explain("${cm73}", intent="merge decision after the dev-clone copy",
                execution_type=3, target="dev_clone"),
    ],
)
