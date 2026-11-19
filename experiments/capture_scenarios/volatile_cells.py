"""#202 (parent #201): the Snowflake Volatile evaluation cells #191 left uncaptured.

Follows `volatile_evaluation.py` (#191): per-run nodes in
CDO_DEMO_DB.PROD_STATE_DEMO, evaluation requested with
`state: {evaluate_volatile_sql: true}` (client 2.0.11 sends it as
`tolerate_nondeterminism = false`), a `Read` of every table after every build
(views are not read: a view stores no value), and one `state-probe` Explain
of every decision right after the build that decided it.

Nodes, by follow-up ticket ("on" = evaluation requested, "off" = not):

  vb    base     deterministic table, off; upstream of the tests and tables below
  #203 call forms and functions outside the detector set (tables, on):
  fct   `current_time` (no parentheses)
  flt   `localtime` (no parentheses)
  fts   `current_timestamp(3)`
  fra   `uniform(1, 10, random())`, `randstr(5, 1234)`, `normal(5, 2, 42)`
  frb   `uniform(1, 100, 1234)`, `randstr(8, random())`, `normal(0, 1, random())`
  fcu   `current_user()`
  fcx   `current_role()`, `current_warehouse()`, `current_database()` (stable per session)
  fcq   `current_session()`, `current_statement()`, `last_query_id()` (differ per run)
  #204 combinations:
  vw    view calling `current_timestamp()`, off (the volatile dependency)
  cmb   table over ref(vw) calling `uuid_string()` itself, on
  ctv   data test over ref(vw), no function of its own, on
  cto   data test calling `current_timestamp()`, off
  cdo   table over ref(vw), off
  cvo   view calling `current_timestamp()`, on
  #205 execution types (on):
  xi    append incremental over ref(vb) calling `current_timestamp()`
  xs    snapshot of ref(vb) with a `current_timestamp()` column (check on v)
  xc    custom materialization (`xmat`) calling `current_timestamp()`
  xt    store_failures data test (severity warn, one failing row) calling
        `current_timestamp()`
  #206 rebuild reasons:
  rq    table, on: its query changes in round 2
  rx    table, on: a row is inserted (External modification) before round 2
  ro    table, off: its query changes in round 2 (an evaluation-off rebuild)
  rt    data test, on: its logic changes in round 2
  #206 Explain trees (tables, on):
  eu    calling `current_timestamp()` over ref(vb) (a model with an upstream table)
  vwd   deterministic view, off (a deterministic query dependency)
  eq    calling `current_timestamp()` over ref(vwd) (a model with a query dependency)
  ecu   calling `current_timestamp()` under `compare_unrendered_code`
  edu   over ref(vw) joined to ref(vb), no function of its own (volatile dependency
        plus an upstream table)
  #207 copy and zero Lag tolerance (off):
  pc    table calling `current_timestamp()`: built in prod, then in dev_clone
  lz    table over ref(eu), Lag tolerance 0s

Rounds:
  1  first build of every node (pc in prod), each explained, each table read
  2  rerun of every node except pc, after rq/ro/rt edits and the rx insert
  3  pc in dev_clone (Result copy?), explained, the dev table read

Metered submits: 30 + 29 + 1 = 60 (3 builds), 60 `state-probe` explains,
48 `dbt show` reads, 1 run-operation.
"""
from explain_capture import Build, Explain, Read, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
DEV = "CDO_DEMO_DB.DEV_CLONE_DEMO"
ON = "state={'evaluate_volatile_sql': true}"
TABLE_ON = f"materialized='table', {ON}"

MATERIALIZATION = """{% materialization ${xmat}, default %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {{ run_hooks(pre_hooks) }}
  {% call statement('main') -%}
    create or replace table {{ target_relation }} as ({{ sql }})
  {%- endcall %}
  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""

# Call forms (#203): one model per form whose key could collide with another's.
FORMS = {
    "fct": "current_time as v",
    "flt": "localtime as v",
    "fts": "current_timestamp(3) as v",
    "fra": "uniform(1, 10, random()) as u, randstr(5, 1234) as r, normal(5, 2, 42) as n",
    "frb": "uniform(1, 100, 1234) as u, randstr(8, random()) as r, normal(0, 1, random()) as n",
    "fcu": "current_user() as v",
    "fcx": "current_role() as a, current_warehouse() as b, current_database() as c",
    "fcq": "current_session() as a, current_statement() as b, last_query_id() as c",
}

# Every node built in prod, in `names` order; models, then data tests.
MODELS = ("vb", *FORMS, "vw", "cmb", "cdo", "cvo", "xi", "xs", "xc", "rq", "rx", "ro",
          "eu", "vwd", "eq", "ecu", "edu", "pc", "lz")
TESTS = ("ctv", "cto", "xt", "rt")
# Execution type of each node (FULL unless listed).
EXECUTION_TYPE = {"vw": 10, "cvo": 10, "vwd": 10, "xi": 2, "xs": 7, "xc": 11,
                  **{t: 8 for t in TESTS}}
# Relations each build writes a value to (views store none). xt's is its audit table.
TABLES = tuple(n for n in MODELS + ("xt",) if EXECUTION_TYPE.get(n, 1) not in (8, 10)
               or n == "xt")

TEST_WHERE = "where id > {floor} and current_timestamp() < '2000-01-01'::timestamp_ltz"


def model(name, body, config=TABLE_ON):
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config({config}) }}}}\n{body}\n")


def data_test(name, body, config=""):
    head = f"{{{{ config({config}) }}}}\n" if config else ""
    return Write(f"data-tests/${{{name}}}.sql", f"{head}{body}\n")


def snapshot(name, body):
    return Write(f"snapshots/${{{name}}}.sql", f"""{{% snapshot ${{{name}}} %}}
{{{{ config(target_schema=target.schema, unique_key='id', strategy='check', check_cols=['v'], {ON}) }}}}
{body}
{{% endsnapshot %}}
""")


def explain(name, intent, first):
    sel = f"test.jaffle_shop.${{{name}}}*" if name in TESTS else f"${{{name}}}"
    return Explain(sel, intent=intent, node=f"${{{name}}}",
                   decision="EXECUTE" if first else None,
                   execution_type=EXECUTION_TYPE.get(name, 1), history=not first, via="probe")


def explains(tag, first, nodes):
    return [explain(n, f"{tag}: {n}", first) for n in nodes]


def reads(nodes):
    return [Read(f"{DEMO}.${{{n}}}") for n in nodes]


ROUND2 = tuple(n for n in MODELS + TESTS if n != "pc")

SCENARIO = Scenario(
    name="volatile_cells",
    summary="Volatile evaluation on Snowflake, the cells #191 left uncaptured: call forms, "
            "combinations, execution types, rebuild reasons, Explain trees, copy, zero Lag "
            "tolerance (#202)",
    names=("xmat", *MODELS, *TESTS),
    steps=[
        Write("macros/${xmat}.sql", MATERIALIZATION),
        model("vb", "select 1 as id, 'vb_${run}' as v", "materialized='table'"),
        *[model(n, f"select 1 as id, '{n}_${{run}}' as tag, {b}") for n, b in FORMS.items()],
        # #204 combinations.
        model("vw", "select 1 as id, current_timestamp() as ts, 'vw_${run}' as v",
              "materialized='view'"),
        model("cmb", "select id, ts, v, uuid_string() as u from {{ ref('${vw}') }}"),
        data_test("ctv", "select id from {{ ref('${vw}') }} where id > 100", ON),
        data_test("cto", "select id, v from {{ ref('${vb}') }}\n" + TEST_WHERE.format(floor=100)),
        model("cdo", "select id, ts, v from {{ ref('${vw}') }}", "materialized='table'"),
        model("cvo", "select 1 as id, current_timestamp() as ts, 'cvo_${run}' as v",
              f"materialized='view', {ON}"),
        # #205 execution types.
        model("xi", "select id, v, current_timestamp() as ts from {{ ref('${vb}') }}",
              f"materialized='incremental', incremental_strategy='append', {ON}"),
        snapshot("xs", "select id, v, current_timestamp() as ts from {{ ref('${vb}') }}"),
        model("xc", "select 1 as id, current_timestamp() as ts, 'xc_${run}' as v",
              f"materialized='${{xmat}}', {ON}"),
        data_test("xt", "select id, v, current_timestamp() as ts from {{ ref('${vb}') }}",
                  f"store_failures=true, severity='warn', {ON}"),
        # #206 rebuild reasons.
        model("rq", "select 1 as id, current_timestamp() as ts, 'rq0_${run}' as v"),
        model("rx", "select 1 as id, current_timestamp() as ts"),
        model("ro", "select 1 as id, current_timestamp() as ts, 'ro0_${run}' as v",
              "materialized='table'"),
        data_test("rt", "select id, v from {{ ref('${vb}') }}\n" + TEST_WHERE.format(floor=100),
                  ON),
        # #206 Explain trees.
        model("eu", "select id, v, current_timestamp() as ts from {{ ref('${vb}') }}"),
        model("vwd", "select 1 as id, 'vwd_${run}' as v", "materialized='view'"),
        model("eq", "select id, v, current_timestamp() as ts from {{ ref('${vwd}') }}"),
        model("ecu", "select 1 as id, current_timestamp() as ts, 'ecu_${run}' as v",
              "materialized='table', state={'evaluate_volatile_sql': true, "
              "'compare_unrendered_code': true}"),
        model("edu", "select w.id, w.ts, b.v from {{ ref('${vw}') }} as w\n"
                     "join {{ ref('${vb}') }} as b on b.id = w.id"),
        # #207 copy and zero Lag tolerance.
        model("pc", "select 1 as id, current_timestamp() as ts, 'pc_${run}' as v",
              "materialized='table'"),
        model("lz", "select id, ts as upstream_ts, 'lz_${run}' as v from {{ ref('${eu}') }}",
              "materialized='table', state={'lag_tolerance': '0s'}"),

        # Round 1: first builds.
        Build(*(f"${{{n}}}" for n in MODELS + TESTS)),
        *explains("first build", True, MODELS + TESTS),
        *reads(TABLES),

        # Round 2: reruns; rq, ro and rt change, rx is modified outside dbt.
        model("rq", "select 1 as id, current_timestamp() as ts, 'rq1_${run}' as v"),
        model("ro", "select 1 as id, current_timestamp() as ts, 'ro1_${run}' as v",
              "materialized='table'"),
        data_test("rt", "select id, v from {{ ref('${vb}') }}\n" + TEST_WHERE.format(floor=101),
                  ON),
        Sql(f"insert into {DEMO}.${{rx}} select 2 as id, current_timestamp() as ts"),
        Build(*(f"${{{n}}}" for n in ROUND2)),
        *explains("rerun", False, ROUND2),
        *reads(tuple(n for n in TABLES if n != "pc")),

        # Round 3: pc's prod equivalent exists; its dev relation does not.
        Build("${pc}", target="dev_clone"),
        Explain("${pc}", intent="dev run of the prod-built volatile table, evaluation off",
                execution_type=1, target="dev_clone", via="probe"),
        Read(f"{DEV}.${{pc}}", target="dev_clone"),
    ],
)
