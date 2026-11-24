"""#211 (parent #201): the first Snowflake capture batch of the remaining
Volatile evaluation cells.

Follows `volatile_cells.py` (#202): per-run nodes in
CDO_DEMO_DB.PROD_STATE_DEMO, evaluation requested on all five models with
`state: {evaluate_volatile_sql: true}` (client 2.0.11 sends it as
`tolerate_nondeterminism = false`), a `Read` of every table after every build,
and one `state-probe` Explain of every decision right after the build that
decided it.

1. Does a constant-generator call alone prevent reuse? Three single-row FULL
   tables without upstreams, built once and rerun unchanged:
     gl  a literal projection (the control)
     gu  `uniform(1, 100, 1234)` alone (#202's `frb` also called `random()`)
     gr  `random()` alone
2. How does Volatile evaluation behave on a MERGE incremental when upstream
   data changes? Two keyed MERGE models over the same source table `ms`
   (created by this run, so it has no Upstream producer and a change to it is
   upstream data drift, not an External modification of a dbt-built
   relation), each projecting the key and the upstream value:
     mv  plus `current_timestamp()`
     mc  plus a fixed timestamp literal (the control)
   Both have `lag_tolerance: 0s`, so the control rebuilds on drift instead of
   reusing within the 45-minute default. Their SQL and config never change.

Rounds:
  1  create ms; first build of all five models, each explained, each read
  2  unchanged rerun of all five, each explained, each read
  3  update ms's value for the existing key; rerun mv and mc, each explained,
     each read, and ms read

Metered submits: 5 + 5 + 2 = 12 (3 builds), 12 `state-probe` explains,
14 `dbt show` reads, 2 run-operations.
"""
from explain_capture import Build, Explain, Read, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
ON = "'evaluate_volatile_sql': true"
TABLE_ON = f"materialized='table', state={{{ON}}}"
MERGE_ON = ("materialized='incremental', incremental_strategy='merge', unique_key='id', "
            f"state={{{ON}, 'lag_tolerance': '0s'}}")
TABLES = ("gl", "gu", "gr")
MERGES = ("mv", "mc")
MODELS = TABLES + MERGES
EXECUTION_TYPE = {**{n: 1 for n in TABLES}, **{n: 3 for n in MERGES}}


def model(name, body, config):
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config({config}) }}}}\n{body}\n")


def merge(name, ts):
    return model(name, f"select id, v, {ts} as ts\nfrom {{{{ source('${{ms}}', '${{ms}}') }}}}",
                 MERGE_ON)


def explains(tag, nodes, first):
    return [Explain(f"${{{n}}}", intent=f"{tag}: {n}", decision="EXECUTE" if first else None,
                    execution_type=EXECUTION_TYPE[n], history=not first, via="probe")
            for n in nodes]


def reads(nodes):
    return [Read(f"{DEMO}.${{{n}}}") for n in nodes]


def build(nodes):
    return Build(*(f"${{{n}}}" for n in nodes))


SCENARIO = Scenario(
    name="volatile_generator_merge",
    summary="Volatile evaluation on Snowflake (#211): a constant-generator call alone, and a "
            "keyed MERGE incremental with current_timestamp() over upstream data drift",
    names=("ms", *MODELS),
    steps=[
        Write("models/${ms}.yml", """sources:
  - name: ${ms}
    schema: prod_state_demo
    tables:
      - name: ${ms}
"""),
        model("gl", "select 1 as id, 'gl_${run}' as v", TABLE_ON),
        model("gu", "select 1 as id, uniform(1, 100, 1234) as u", TABLE_ON),
        model("gr", "select 1 as id, random() as r", TABLE_ON),
        merge("mv", "current_timestamp()"),
        merge("mc", "'2026-01-01 00:00:00'::timestamp_ltz"),
        Sql(f"create or replace table {DEMO}.${{ms}} as select 1 as id, 'a' as v"),
        Read(f"{DEMO}.${{ms}}"),

        # Round 1: first builds.
        build(MODELS),
        *explains("first build", MODELS, True),
        *reads(MODELS),

        # Round 2: unchanged reruns.
        build(MODELS),
        *explains("unchanged rerun", MODELS, False),
        *reads(MODELS),

        # Round 3: only the upstream value of the existing key changes.
        Sql(f"update {DEMO}.${{ms}} set v = 'b' where id = 1"),
        Read(f"{DEMO}.${{ms}}"),
        build(MERGES),
        *explains("rerun after upstream data drift", MERGES, False),
        *reads(MERGES),
    ],
)
