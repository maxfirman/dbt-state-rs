"""#72: snapshot (SNAPSHOT=7) Explain trees over State history, plus the #78
FULL and VIEW logic-change rebuilds (SHAPE 7).

Nodes (all per-run, in prod_state_demo):
  s72src  source table created by this run (no Upstream producer)
  s72st   static table model
  s72up   table model whose logic changes in round 3
  s72sd   snapshot of s72src          -> source rows added (data drift)
  s72su   snapshot of ref(s72up)      -> upstream query change
  s72sl   snapshot of ref(s72st)      -> its own logic changes
  s72sf   snapshot of ref(s72st)      -> rerun under --full-refresh (sent as FULL)
  s72fl   table over ref(s72st)       -> its own logic changes (#78, FULL SHAPE 7)
  s72vl   view over ref(s72st)        -> its own logic changes (#78, VIEW SHAPE 7)
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
ALL = ("${s72st}", "${s72up}", "${s72sd}", "${s72su}", "${s72sl}", "${s72sf}", "${s72fl}", "${s72vl}")


def snapshot(name, body):
    return Write(f"snapshots/${{{name}}}.sql", f"""{{% snapshot ${{{name}}} %}}
{{{{ config(target_schema=target.schema, unique_key='id', strategy='check', check_cols=['v']) }}}}
{body}
{{% endsnapshot %}}
""")


SCENARIO = Scenario(
    name="snapshot_explain",
    summary="snapshot first build, SKIP, source drift, upstream query change, logic change, "
            "--full-refresh; FULL and VIEW logic-change rebuilds (#72, #78)",
    names=("s72src", "s72st", "s72up", "s72sd", "s72su", "s72sl", "s72sf", "s72fl", "s72vl"),
    steps=[
        Write("models/${s72src}.yml", """sources:
  - name: ${s72src}
    schema: prod_state_demo
    tables:
      - name: ${s72src}
"""),
        Write("models/marts/${s72st}.sql", "select 1 as id, 'st_${run}' as v\n"),
        Write("models/marts/${s72up}.sql", "select 1 as id, 'up0_${run}' as v\n"),
        snapshot("s72sd", "select id, v from {{ source('${s72src}', '${s72src}') }}"),
        snapshot("s72su", "select id, v from {{ ref('${s72up}') }}"),
        snapshot("s72sl", "select id, v from {{ ref('${s72st}') }}"),
        snapshot("s72sf", "select id, v from {{ ref('${s72st}') }}"),
        Write("models/marts/${s72fl}.sql", "select id, v from {{ ref('${s72st}') }}\n"),
        Write("models/marts/${s72vl}.sql",
              "{{ config(materialized='view') }}\nselect id, v from {{ ref('${s72st}') }}\n"),
        Sql(f"create or replace table {DEMO}.${{s72src}} as select 1 as id, 'src0' as v"),

        # Round 1: first builds, confirmed.
        Build(*ALL),
        Explain("${s72sd}", intent="snapshot first execution", decision="EXECUTE",
                execution_type=7, history=False),

        # Round 2: unchanged rerun.
        Build(*ALL),
        Explain("${s72sd}", intent="snapshot unchanged SKIP (source upstream)", decision="SKIP",
                execution_type=7),
        Explain("${s72su}", intent="snapshot unchanged SKIP (model upstream)", decision="SKIP",
                execution_type=7),

        # Round 3: source gains a row; upstream model logic, snapshot logic and the
        # FULL / VIEW logic change.
        Sql(f"insert into {DEMO}.${{s72src}} select 2 as id, 'src1' as v"),
        Write("models/marts/${s72up}.sql", "select 1 as id, 'up1_${run}' as v\n"),
        snapshot("s72sl", "select id, v from {{ ref('${s72st}') }} where id >= 1"),
        Write("models/marts/${s72fl}.sql", "select id, v from {{ ref('${s72st}') }} where id >= 1\n"),
        Write("models/marts/${s72vl}.sql",
              "{{ config(materialized='view') }}\nselect id, v from {{ ref('${s72st}') }} where id >= 1\n"),
        Build(*ALL),
        Explain("${s72sd}", intent="snapshot after its source gained rows (data drift)",
                execution_type=7),
        Explain("${s72su}", intent="snapshot after an upstream query change", decision="EXECUTE",
                execution_type=7),
        Explain("${s72sl}", intent="snapshot after its own logic changed", decision="EXECUTE",
                execution_type=7),
        Explain("${s72fl}", intent="FULL model after its own logic changed (#78)", decision="EXECUTE",
                execution_type=1),
        Explain("${s72vl}", intent="VIEW after its own logic changed (#78)", decision="EXECUTE",
                execution_type=10),

        # Round 4: the snapshot under --full-refresh (the client sends FULL).
        Build("${s72sf}", full_refresh=True),
        Explain("${s72sf}", intent="snapshot under --full-refresh (sent as FULL)", execution_type=1),
    ],
)
