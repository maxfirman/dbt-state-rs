"""#200: per-Target data-test history, through the real client (2.0.11).

Singular data tests built alternately on `prod_demo` (CDO_DEMO_DB.PROD_STATE_DEMO)
and `dev_clone` (CDO_DEMO_DB.DEV_CLONE_DEMO), each explained with `state-probe`
right after the build that decided it. Every test has its own SQL, except
the deliberate pairs (xa/xb, ya/yb), so no two cells share a SQL text by
accident. A hosted SKIP echoes the runtime of the execution it reused;
`experiments/data_test_history.py table window.jsonl` attributes each echo.

Nodes:

  u    table, built on both Targets (dev reads its own copy)
  uf   table, built on both Targets; a failing row is inserted into the dev one
  sh   per-run table created in PROD_STATE_DEMO, declared as a source both
       Targets read (`schema: prod_state_demo`)
  ta   (Q1)  over ref(u): prod, dev, prod, dev with unchanged inputs
  tf   (Q3)  over ref(uf): prod pass, dev pass, dev fail (dev uf gains a bad
             row), prod again, dev again
  tr   (Q3)  over ref(u): prod S1, dev S1, dev S2 (fails), dev S1 again, prod S1,
             dev S1: does the dev S1 rerun reuse the older dev S1 pass?
  tc   (Q4)  over ref(u) under `compare_unrendered_code`: prod, dev, prod, dev
  ts   (Q5)  over the shared source: the SQL is identical on both Targets
  xa/xb (Q6) identical SQL over the shared source, two unique ids, built one
             at a time on prod: xa, xb, xa, xb
  ya/yb (Q6) as xa/xb with evaluation off (ya) and on (yb), deterministic SQL

Metered submits (SubmitEnrichedSQL): 7 + 7 + 1 + 2 + 5 + 5 + 8 = 35.
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

PROD = "CDO_DEMO_DB.PROD_STATE_DEMO"
DEV = "CDO_DEMO_DB.DEV_CLONE_DEMO"
DEV_T = "dev_clone"

TESTS = ("ta", "tf", "tr", "tc", "ts")
PAIRS = ("xa", "ya", "xb", "yb")
# The pairs read the shared source, not ref(u): `dbt build --select u` also
# selects every test over u (eager indirect selection), which would run both
# tests of a pair in one invocation.
SRC = "{{ source('${sh}', '${sh}') }}"
X_SQL = f"select id, v from {SRC} where id < -3"
Y_SQL = f"select id, v from {SRC} where id < -4"
TR_PASS = "select id, v from {{ ref('${u}') }} where id < -1"
TR_FAIL = "select id, v from {{ ref('${u}') }} where id > 0"


def model(name, body):
    return Write(f"models/marts/${{{name}}}.sql",
                 f"{{{{ config(materialized='table') }}}}\n{body}\n")


def data_test(name, body, config=""):
    head = f"{{{{ config({config}) }}}}\n" if config else ""
    return Write(f"data-tests/${{{name}}}.sql", f"{head}{body}\n")


def explain(name, intent, target="prod_demo", first=False):
    return Explain(f"test.jaffle_shop.${{{name}}}*", intent=intent, node=f"${{{name}}}",
                   decision="EXECUTE" if first else None, execution_type=8,
                   history=not first, target=target, via="probe")


def explains(tag, nodes, target="prod_demo", first=False):
    return [explain(n, f"{tag}: {n}", target, first) for n in nodes]


def sel(*nodes):
    return tuple(f"${{{n}}}" for n in nodes)


SCENARIO = Scenario(
    name="data_test_history",
    summary="data-test history per Target: alternating prod/dev runs, a failing newer run, "
            "compare_unrendered_code, identical SQL on both Targets, identical SQL under two "
            "unique ids (#200)",
    names=("u", "uf", "sh", *TESTS, *PAIRS),
    steps=[
        model("u", "select 1 as id, 'u_${run}' as v"),
        model("uf", "select 1 as id, 'uf_${run}' as v"),
        Write("models/${sh}.yml", """sources:
  - name: ${sh}
    schema: prod_state_demo
    tables:
      - name: ${sh}
"""),
        data_test("ta", "select id, v from {{ ref('${u}') }} where id < 0"),
        data_test("tf", "select id, v from {{ ref('${uf}') }} where id < 0"),
        data_test("tr", TR_PASS),
        data_test("tc", "select id, v from {{ ref('${u}') }} where id < -2",
                  "state={'compare_unrendered_code': true}"),
        data_test("ts", f"select id, v from {SRC} where id < 0"),
        data_test("xa", X_SQL),
        data_test("xb", X_SQL),
        data_test("ya", Y_SQL, "state={'evaluate_volatile_sql': false}"),
        data_test("yb", Y_SQL, "state={'evaluate_volatile_sql': true}"),
        Sql(f"create or replace table {PROD}.${{sh}} as select 1 as id, 'sh_${{run}}' as v"),

        # R1 prod: first execution of every alternating test.
        Build(*sel("u", "uf", *TESTS)),
        *explains("R1 prod first execution", TESTS, first=True),
        # R2 dev: the same tests on the dev Target (u and uf built there too).
        Build(*sel("u", "uf", *TESTS), target=DEV_T),
        *explains("R2 dev", TESTS, DEV_T),
        # Q3: dev uf gains a failing row; dev tf fails.
        Sql(f"insert into {DEV}.${{uf}} select -1 as id, 'bad' as v", target=DEV_T),
        Build(*sel("tf"), target=DEV_T, test_failures=True),
        explain("tf", "Q3 dev run after a failing row arrived", DEV_T),
        # Q3: dev tr's logic changes so it fails, then changes back.
        data_test("tr", TR_FAIL),
        Build(*sel("tr"), target=DEV_T, test_failures=True),
        explain("tr", "Q3 dev run of the failing logic S2", DEV_T),
        data_test("tr", TR_PASS),
        Build(*sel("tr"), target=DEV_T),
        explain("tr", "Q3 dev run of S1 again after the failing S2 run", DEV_T),
        # R3 prod, R4 dev: unchanged inputs.
        Build(*sel(*TESTS)),
        *explains("R3 prod", TESTS),
        Build(*sel(*TESTS), target=DEV_T, test_failures=True),
        *explains("R4 dev", TESTS, DEV_T),
        # Q6: identical SQL under two unique ids, one build at a time.
        Build(*sel("xa", "ya")),
        explain("xa", "Q6 first execution of xa", first=True),
        explain("ya", "Q6 first execution of ya (evaluation off)", first=True),
        Build(*sel("xb", "yb")),
        Explain("test.jaffle_shop.${xb}*", intent="Q6 first submit of xb (same SQL as xa)",
                node="${xb}", execution_type=8, history=False, via="probe"),
        Explain("test.jaffle_shop.${yb}*", intent="Q6 first submit of yb (same SQL as ya, "
                "evaluation on)", node="${yb}", execution_type=8, history=False, via="probe"),
        Build(*sel("xa", "ya")),
        explain("xa", "Q6 xa again"),
        explain("ya", "Q6 ya again"),
        Build(*sel("xb", "yb")),
        Explain("test.jaffle_shop.${xb}*", intent="Q6 xb again", node="${xb}", execution_type=8,
                history=False, via="probe"),
        Explain("test.jaffle_shop.${yb}*", intent="Q6 yb again", node="${yb}", execution_type=8,
                history=False, via="probe"),
    ],
)
