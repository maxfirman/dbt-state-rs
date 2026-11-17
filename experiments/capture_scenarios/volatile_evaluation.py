"""#191 (spec #67): Snowflake Volatile evaluation capture batch.

Characterizes what hosted dbt State returns in `transformed_nodes_by_query`, the
decision and the Explain tree when a submit requests Volatile evaluation
(`state: {evaluate_volatile_sql: true}`, sent by client 2.0.11 as
`tolerate_nondeterminism = false`). After every build the driver reads back the
value the Warehouse wrote (`Read`, `dbt show` without dbt State), for comparison
with the value hosted rendered.

Nodes (all per-run, all in CDO_DEMO_DB.PROD_STATE_DEMO):

  vm  cell 1/2  table calling every Volatile function the Snowflake detector
                knows (crates/server/src/sql_norm.rs VOLATILE_FUNCTIONS, plus
                the parenless CURRENT_DATE form), CURRENT_TIMESTAMP() twice,
                RANDOM() also nested in UNIFORM(); evaluation on
  vc  cell 3    deterministic control table; evaluation on
  vo  cell 4    vm's select list in its own table; evaluation off (default)
  vv  cell 5    view calling CURRENT_TIMESTAMP(); evaluation off (default)
  vd  cell 5    table over ref(vv); evaluation on
  vt  cell 6    singular data test over ref(vc) calling CURRENT_TIMESTAMP();
                evaluation on; passes (no rows)
  vk  cell 7    table over ref(vm), Lag tolerance 3600 s; evaluation off

Rounds:
  1  first build of every node, each explained (cell 8), each table read
  2  unchanged rerun of every node, explained and read (cells 2-7 reruns)
  3  vm with evaluation switched off, rebuilt over its evaluation-on history,
     explained and read (cell 4, toggled on the same node)

3 builds (15 submits), 15 `dbt state explain`, 11 `dbt show` reads.
"""
from explain_capture import Build, Explain, Read, Scenario, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
ON = "state={'evaluate_volatile_sql': true}"

# Every function in the Snowflake detector's VOLATILE_FUNCTIONS, once each, with
# CURRENT_TIMESTAMP() repeated and RANDOM() nested in UNIFORM(). RANDSTR, NORMAL
# and ZIPF take a constant generator so each appears once.
VOLATILE_SELECT = """select
  1 as id,
  '${run}' as run_id,
  current_timestamp() as f_current_timestamp,
  current_timestamp() as f_current_timestamp_repeat,
  localtimestamp() as f_localtimestamp,
  systimestamp() as f_systimestamp,
  sysdate() as f_sysdate,
  getdate() as f_getdate,
  current_time() as f_current_time,
  localtime() as f_localtime,
  current_date() as f_current_date,
  current_date as f_current_date_bare,
  random() as f_random,
  randstr(8, 1234) as f_randstr,
  uuid_string() as f_uuid_string,
  normal(0, 1, 1234) as f_normal,
  uniform(1, 100, random()) as f_uniform,
  zipf(1, 10, 1234) as f_zipf,
  seq1() as f_seq1,
  seq2() as f_seq2,
  seq4() as f_seq4,
  seq8() as f_seq8
"""


def model(name, body, config="materialized='table'"):
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config({config}) }}}}\n{body}")


def explain(name, intent, decision=None, history=True, et=1):
    return Explain(f"${{{name}}}", intent=intent, decision=decision, execution_type=et,
                   history=history)


def explain_test(intent, decision=None, history=True):
    return Explain("test.jaffle_shop.${vt}*", intent=intent, node="${vt}", decision=decision,
                   execution_type=8, history=history)


def read(name):
    return Read(f"{DEMO}.${{{name}}}")


TABLES = ("vm", "vo", "vc", "vd", "vk")
ALL = ("${vm}", "${vo}", "${vc}", "${vv}", "${vd}", "${vt}", "${vk}")


def round_steps(tag, first):
    d = "EXECUTE" if first else None
    h = not first
    return [
        Build(*ALL),
        explain("vm", f"{tag}: every Volatile function, evaluation on", d, h),
        explain("vo", f"{tag}: same functions, evaluation off", d, h),
        explain("vc", f"{tag}: deterministic control, evaluation on", d, h),
        explain("vv", f"{tag}: view calling CURRENT_TIMESTAMP(), evaluation off", d, h, et=10),
        explain("vd", f"{tag}: table over the volatile view, evaluation on", d, h),
        explain_test(f"{tag}: data test calling CURRENT_TIMESTAMP(), evaluation on", d, h),
        explain("vk", f"{tag}: table downstream of vm, Lag tolerance 3600 s", d, h),
        *[read(t) for t in TABLES],
    ]


SCENARIO = Scenario(
    name="volatile_evaluation",
    summary="Volatile evaluation on Snowflake: every detector function, rerun, deterministic "
            "control, evaluation off, volatile view dependency, data test, downstream table (#191)",
    names=("vm", "vo", "vc", "vv", "vd", "vt", "vk"),
    steps=[
        model("vm", VOLATILE_SELECT, f"materialized='table', {ON}"),
        model("vo", VOLATILE_SELECT),
        model("vc", "select 1 as id, 'vc_${run}' as v\n", f"materialized='table', {ON}"),
        model("vv", "select 1 as id, current_timestamp() as ts, 'vv_${run}' as v\n",
              "materialized='view'"),
        model("vd", "select id, ts, v from {{ ref('${vv}') }}\n", f"materialized='table', {ON}"),
        Write("data-tests/${vt}.sql",
              f"{{{{ config({ON}) }}}}\n"
              "select id, v from {{ ref('${vc}') }}\n"
              "where id > 100 and current_timestamp() < '2000-01-01'::timestamp_ltz\n"),
        model("vk", "select id, f_current_timestamp as upstream_ts, 'vk_${run}' as v\n"
                    "from {{ ref('${vm}') }}\n",
              "materialized='table', state={'lag_tolerance': '3600s'}"),

        # Round 1: first builds.
        *round_steps("first build", first=True),
        # Round 2: unchanged rerun of every node over its existing Target relation.
        *round_steps("unchanged rerun", first=False),
        # Round 3: vm's evaluation switched off, over its evaluation-on history.
        model("vm", VOLATILE_SELECT),
        Build("${vm}"),
        explain("vm", "evaluation switched off over an evaluation-on build"),
        read("vm"),
    ],
)
