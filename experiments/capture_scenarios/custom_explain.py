"""#74: custom materialization (DBT_CUSTOM=11) Explain trees over State history.

The client sends DBT_CUSTOM for a materialization outside the built-in list,
so this run writes its own materialization macro `c74mat` (a plain
`create or replace table ... as (sql)`) and uses it for every custom node.

Nodes (all per-run, in prod_state_demo):
  c74src  source table created by this run (no Upstream producer)
  c74st   static table model
  c74up   table model whose logic changes in round 3
  c74c0   custom, no upstream                -> unchanged SKIP
  c74cd   custom over c74src (default tolerance) -> first build, SKIP, source drift
  c74cz   custom over c74src, lag_tolerance 0s   -> source drift
  c74cl   custom over ref(c74st)             -> SKIP, then its own logic changes
  c74cu   custom over ref(c74up)             -> upstream query change
  c74cf   custom over ref(c74st)             -> rerun under --full-refresh
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
ALL = ("${c74st}", "${c74up}", "${c74c0}", "${c74cd}", "${c74cz}", "${c74cl}", "${c74cu}",
       "${c74cf}")

MATERIALIZATION = """{% materialization ${c74mat}, default %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {{ run_hooks(pre_hooks) }}
  {% call statement('main') -%}
    create or replace table {{ target_relation }} as ({{ sql }})
  {%- endcall %}
  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""


def custom(name, body, state=""):
    cfg = f"materialized='${{c74mat}}'{', ' + state if state else ''}"
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config({cfg}) }}}}\n{body}\n")


SRC = "select id, v from {{ source('${c74src}', '${c74src}') }}"

SCENARIO = Scenario(
    name="custom_explain",
    summary="custom materialization first build, SKIPs, source drift (default and 0s "
            "tolerance), upstream query change, logic change, --full-refresh (#74)",
    names=("c74mat", "c74src", "c74st", "c74up", "c74c0", "c74cd", "c74cz", "c74cl", "c74cu",
           "c74cf"),
    steps=[
        Write("macros/${c74mat}.sql", MATERIALIZATION),
        Write("models/${c74src}.yml", """sources:
  - name: ${c74src}
    schema: prod_state_demo
    tables:
      - name: ${c74src}
"""),
        Write("models/marts/${c74st}.sql", "select 1 as id, 'st_${run}' as v\n"),
        Write("models/marts/${c74up}.sql", "select 1 as id, 'up0_${run}' as v\n"),
        custom("c74c0", "select 1 as id, 'c0_${run}' as v"),
        custom("c74cd", SRC),
        custom("c74cz", SRC, "state={'lag_tolerance': '0s'}"),
        custom("c74cl", "select id, v from {{ ref('${c74st}') }}"),
        custom("c74cu", "select id, v from {{ ref('${c74up}') }}"),
        custom("c74cf", "select id, v from {{ ref('${c74st}') }}"),
        Sql(f"create or replace table {DEMO}.${{c74src}} as select 1 as id, 'src0' as v"),

        # Round 1: first builds, confirmed.
        Build(*ALL),
        Explain("${c74cd}", intent="custom first execution", decision="EXECUTE",
                execution_type=11, history=False),

        # Round 2: unchanged rerun.
        Build(*ALL),
        Explain("${c74c0}", intent="custom unchanged SKIP (no upstream)", decision="SKIP",
                execution_type=11),
        Explain("${c74cd}", intent="custom unchanged SKIP (source upstream)", decision="SKIP",
                execution_type=11),
        Explain("${c74cl}", intent="custom unchanged SKIP (model upstream)", decision="SKIP",
                execution_type=11),

        # Round 3: the source gains a row; the upstream model's logic and c74cl's own
        # logic change.
        Sql(f"insert into {DEMO}.${{c74src}} select 2 as id, 'src1' as v"),
        Write("models/marts/${c74up}.sql", "select 1 as id, 'up1_${run}' as v\n"),
        custom("c74cl", "select id, v from {{ ref('${c74st}') }} where id >= 1"),
        Build(*ALL),
        Explain("${c74cd}", intent="custom after its source gained rows (default tolerance)",
                execution_type=11),
        Explain("${c74cz}", intent="custom after its source gained rows (0s tolerance)",
                execution_type=11),
        Explain("${c74cu}", intent="custom after an upstream query change", decision="EXECUTE",
                execution_type=11),
        Explain("${c74cl}", intent="custom after its own logic changed", decision="EXECUTE",
                execution_type=11),

        # Round 4: the unchanged custom node under --full-refresh.
        Build("${c74cf}", full_refresh=True),
        Explain("${c74cf}", intent="custom under --full-refresh"),
    ],
)
