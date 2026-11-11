"""#104 (104a): freshness Decision reason cells over State history.

Two sources: `fs1` gains a row in round 3, `fs2` never changes. Every model
reads them with a fixed microbatch window (`--event-time-start/--event-time-end`),
so the microbatch nodes can SKIP.

  fa_*  ALL policy (`require_fresh_data_from: all`) over fs1 and fs2: only one
        of two upstreams drifts, so the reuse tolerates it
        (`reused.tolerated_stale`). APPEND, MERGE, INSERT_OVERWRITE,
        DELETE_INSERT, MICROBATCH, DBT_CUSTOM. A snapshot cannot set the
        policy (the client reads `state` only from a model).
  fz_*  `lag_tolerance: 0s` over fs1: the drift is past the Lag tolerance
        (`upstream_outdated`, SHAPE 9 data only). INSERT_OVERWRITE,
        DELETE_INSERT, MICROBATCH. The snapshot cell needs its build older
        than the default 45 minute tolerance: `freshness_cells_snapshot`.
  fr_*  default Lag tolerance over fs1: logic A (round 1), logic B (round 2),
        back to A in round 3 while fs1 drifts. The latest build (B) does not
        match; the older A build would be reused stale (SHAPE 7, the PR #105
        Codex cell). FULL, the five incrementals, SNAPSHOT, DBT_CUSTOM.
  fd_sn snapshot over fs1, built in round 1; the follow-up scenario rebuilds it
        past the Lag tolerance.
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
WINDOW = ("--event-time-start", "2026-11-01", "--event-time-end", "2026-11-03")

INC = "materialized='incremental', "
CONFIG = {
    "ap": INC + "incremental_strategy='append'",
    "mg": INC + "incremental_strategy='merge', unique_key='id'",
    "io": INC + "incremental_strategy='insert_overwrite'",
    "di": INC + "incremental_strategy='delete+insert', unique_key='id'",
    "mb": INC + "incremental_strategy='microbatch', event_time='ts', begin='2026-11-01', "
          "batch_size='day'",
    "cu": "materialized='${fmat}'",
    "fl": "materialized='table'",
}
KIND = {"fl": 1, "ap": 2, "mg": 3, "io": 4, "di": 5, "mb": 6, "sn": 7, "cu": 11}
ALL_POLICY = "state={'require_fresh_data_from': 'all'}"
ZERO = "state={'lag_tolerance': '0s'}"

ONE = "select id, v, ts from {{ source('${fs1}', '${fs1}') }}"
TWO = ONE + "\nunion all\nselect id, v, ts from {{ source('${fs1}', '${fs2}') }}"
LOGIC_B = ONE + " where id >= 0"

FA = ("ap", "mg", "io", "di", "mb", "cu")
FZ = ("io", "di", "mb")
FR = ("fl", "ap", "mg", "io", "di", "mb", "sn", "cu")

MATERIALIZATION = """{% materialization ${fmat}, default %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {{ run_hooks(pre_hooks) }}
  {% call statement('main') -%}
    create or replace table {{ target_relation }} as ({{ sql }})
  {%- endcall %}
  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""

SOURCES = """sources:
  - name: ${fs1}
    schema: prod_state_demo
    tables:
      - name: ${fs1}
        config:
          event_time: ts
      - name: ${fs2}
        config:
          event_time: ts
"""


def snapshot(name, body):
    return Write(f"snapshots/${{{name}}}.sql", f"""{{% snapshot ${{{name}}} %}}
{{{{ config(target_schema=target.schema, unique_key='id', strategy='check', check_cols=['v']) }}}}
{body}
{{% endsnapshot %}}
""")


def model(name, kind, body, state=None):
    cfg = CONFIG[kind] + (f", {state}" if state else "")
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config({cfg}) }}}}\n{body}\n")


def fr(kind, body):
    if kind == "sn":
        return snapshot("fr_sn", body)
    return model(f"fr_{kind}", kind, body)


NAMES = (("fmat", "fs1", "fs2", "fd_sn") + tuple(f"fa_{k}" for k in FA)
         + tuple(f"fz_{k}" for k in FZ) + tuple(f"fr_{k}" for k in FR))
NODES = tuple(f"${{{n}}}" for n in NAMES if n not in ("fmat", "fs1", "fs2"))

SCENARIO = Scenario(
    name="freshness_cells",
    summary="ALL-policy partial drift, drift past a 0s Lag tolerance, and a stale older "
            "match after a logic revert, per node kind (#104)",
    names=NAMES,
    steps=[
        Write("macros/${fmat}.sql", MATERIALIZATION),
        Write("models/${fs1}.yml", SOURCES),
        *[model(f"fa_{k}", k, TWO, ALL_POLICY) for k in FA],
        *[model(f"fz_{k}", k, ONE, ZERO) for k in FZ],
        *[fr(k, ONE) for k in FR],
        snapshot("fd_sn", ONE),
        Sql(f"create or replace table {DEMO}.${{fs1}} as "
            "select 1 as id, 's1_0' as v, '2026-11-01 10:00:00'::timestamp_ntz as ts",
            f"create or replace table {DEMO}.${{fs2}} as "
            "select 101 as id, 's2_0' as v, '2026-11-01 11:00:00'::timestamp_ntz as ts"),

        # Round 1: first builds (logic A), confirmed.
        Build(*NODES, args=WINDOW),

        # Round 2: the fr_* nodes change to logic B and rebuild; the rest SKIP.
        *[fr(k, LOGIC_B) for k in FR],
        Build(*NODES, args=WINDOW),

        # Round 3: fs1 gains a row; the fr_* nodes go back to logic A.
        Sql(f"insert into {DEMO}.${{fs1}} "
            "select 2 as id, 's1_1' as v, '2026-11-02 10:00:00'::timestamp_ntz as ts"),
        *[fr(k, ONE) for k in FR],
        Build(*NODES, args=WINDOW),
        *[Explain(f"${{fa_{k}}}", intent="ALL policy, one of two upstreams drifted",
                  decision="SKIP", execution_type=KIND[k]) for k in FA],
        *[Explain(f"${{fz_{k}}}", intent="upstream drift past a 0s Lag tolerance",
                  decision="EXECUTE", execution_type=KIND[k]) for k in FZ],
        *[Explain(f"${{fr_{k}}}", intent="reverted to an older build's logic while its "
                  "upstream drifted within the Lag tolerance", decision="EXECUTE",
                  execution_type=KIND[k]) for k in FR],
    ],
)
