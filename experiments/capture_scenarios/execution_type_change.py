"""#83 / #90: which State history row hosted judges a submit against when the
node's execution type changes and its SQL does not.

Hypotheses (from `explain_snapshot_history.jsonl` entry 58 and
`explain_incremental_append_merge_history.jsonl` entries 74-75):
  H1  the latest row in scope, with RecordExecutions rows left out;
  H2  the latest row of the submit's execution type, else the latest row,
      RecordExecutions rows included;
  H3  as H2, RecordExecutions rows left out.
Round 3 of `ta`, `mi` and `tv` separates H1 from H2/H3 (an older build of the
submit's own type exists under a newer build of another type); round 3 of
`rf` separates H2 from H1/H3 (the only row of the submit's type is a
RecordExecutions row).

Every node reads the jaffle-shop raw orders table (`CDO_DEMO_DB.RAW`, read
only) and selects a per-run literal, so no node has an equivalent under
another name (no Result copy). On client 2.0.11 a FULL table and an APPEND
incremental send the same semantic extras (`lookback`, `on_schema_change`),
and so do MERGE and DELETE_INSERT with the same `unique_key`; a VIEW sends
only `lookback`.

Nodes (per-run, prod_state_demo):
  r83  source name for raw.raw_orders
  ta   table (FULL) -> incremental append (APPEND) -> table (FULL)
  mi   merge (MERGE) -> delete+insert (DELETE_INSERT) -> merge (MERGE)
  tv   table (FULL) -> view (VIEW) -> table (FULL)
  rf   incremental append (APPEND) -> --full-refresh (RecordExecutions, FULL)
       -> table (FULL)

`golden/fixtures/explain_execution_type_change.jsonl` was captured with the
round-2 builds in the other order (the full refresh last), so its round-2
Explain steps captured nothing; the follow-up `execution_type_change_append`
explains an APPEND execution type change instead.
"""
from explain_capture import Build, Explain, Scenario, Write

TABLE = "materialized='table'"
APPEND = "materialized='incremental', incremental_strategy='append'"
MERGE = "materialized='incremental', incremental_strategy='merge', unique_key='id'"
DELETE_INSERT = "materialized='incremental', incremental_strategy='delete+insert', unique_key='id'"
VIEW = "materialized='view'"
BODY = {
    "ta": "select id, customer, 'ta_${run}' as r from {{ source('${r83}', 'raw_orders') }}",
    "mi": "select id, ordered_at, 'mi_${run}' as r from {{ source('${r83}', 'raw_orders') }}",
    "tv": "select id, store_id, 'tv_${run}' as r from {{ source('${r83}', 'raw_orders') }}",
    "rf": "select id, order_total, 'rf_${run}' as r from {{ source('${r83}', 'raw_orders') }}",
}
ALL = ("${ta}", "${mi}", "${tv}", "${rf}")


def model(name, config):
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config({config}) }}}}\n{BODY[name]}\n")


SCENARIO = Scenario(
    name="execution_type_change",
    summary="same SQL, other execution type: FULL<->APPEND, MERGE<->DELETE_INSERT, FULL<->VIEW, "
            "and FULL after a RecordExecutions full refresh (#83, #90)",
    names=("r83", "ta", "mi", "tv", "rf"),
    steps=[
        Write("models/${r83}.yml", """sources:
  - name: ${r83}
    schema: raw
    tables:
      - name: raw_orders
"""),
        model("ta", TABLE),
        model("mi", MERGE),
        model("tv", TABLE),
        model("rf", APPEND),

        # Round 1: first builds, confirmed.
        Build(*ALL),

        # Round 2: the same SQL under another execution type; rf is fully
        # refreshed (the client records it with RecordExecutions as FULL).
        # Explain explains the latest invocation only, so the build explained
        # next comes last.
        model("ta", APPEND),
        model("mi", DELETE_INSERT),
        model("tv", VIEW),
        Build("${rf}", full_refresh=True),
        Build("${ta}", "${mi}", "${tv}"),
        Explain("${ta}", intent="APPEND over its FULL build, same SQL", execution_type=2),
        Explain("${mi}", intent="DELETE_INSERT over its MERGE build, same SQL", execution_type=5),

        # Round 3: back to the first execution type (an older build of that
        # type exists under the round-2 build); rf as a table.
        model("ta", TABLE),
        model("mi", MERGE),
        model("tv", TABLE),
        model("rf", TABLE),
        Build(*ALL),
        Explain("${ta}", intent="FULL over APPEND over FULL, same SQL", execution_type=1),
        Explain("${mi}", intent="MERGE over DELETE_INSERT over MERGE, same SQL", execution_type=3),
        Explain("${tv}", intent="FULL over VIEW over FULL, same SQL", execution_type=1),
        Explain("${rf}", intent="FULL after a RecordExecutions full refresh over APPEND, same SQL",
                execution_type=1),
    ],
)
