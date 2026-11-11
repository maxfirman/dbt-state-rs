"""#75: data test (DBT_DATA_TEST=8) EXECUTE Explain trees over State history.

Three singular data tests over one source table created by this run:

  t75src  source table (no Upstream producer), created by a Sql step
  t75p    passing test  (`where id < 0`: no rows)
  t75f    failing test  (`where id > 0`: one row, severity error, so dbt build exits 1)
  t75s    passing test with `store_failures: true`: its audit relation is a
          table named after the test in the target schema

Rounds:
  1  first execution of all three
  2  unchanged rerun (does a failing test or a store_failures test SKIP?)
  3  the passing and failing tests' logic changes (they still pass / fail)
  4  the source gains a row (upstream data drift); the tests still pass / fail

Data tests are explained by unique-id glob (`test.jaffle_shop.<name>*`); the
oracle matches them by `labels.dbt_node_name`.
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
TESTS = ("${t75p}", "${t75f}", "${t75s}")


def test(name, body, config=""):
    return Write(f"data-tests/${{{name}}}.sql", f"{config}{body}\n")


def explain(name, intent, decision=None, history=True):
    return Explain(f"test.jaffle_shop.${{{name}}}*", intent=intent, node=f"${{{name}}}",
                   decision=decision, execution_type=8, history=history)


SRC = "{{ source('${t75src}', '${t75src}') }}"

SCENARIO = Scenario(
    name="data_test_execute",
    summary="data test first execution, unchanged rerun, logic change and upstream data drift, "
            "for a passing, a failing and a store_failures test (#75)",
    names=("t75src", "t75p", "t75f", "t75s"),
    steps=[
        Write("models/${t75src}.yml", """sources:
  - name: ${t75src}
    schema: prod_state_demo
    tables:
      - name: ${t75src}
"""),
        test("t75p", f"select id, v from {SRC} where id < 0"),
        test("t75f", f"select id, v from {SRC} where id > 0"),
        test("t75s", f"select id, v from {SRC} where id < 0",
             config="{{ config(store_failures=true) }}\n"),
        Sql(f"create or replace table {DEMO}.${{t75src}} as select 1 as id, 'src0' as v"),

        # Round 1: first execution.
        Build(*TESTS, test_failures=True),
        explain("t75p", "passing test first execution", decision="EXECUTE", history=False),
        explain("t75f", "failing test first execution", decision="EXECUTE", history=False),
        explain("t75s", "store_failures test first execution", decision="EXECUTE", history=False),

        # Round 2: unchanged rerun.
        Build(*TESTS, test_failures=True),
        explain("t75p", "passing test unchanged rerun"),
        explain("t75f", "failing test unchanged rerun"),
        explain("t75s", "store_failures test unchanged rerun"),

        # Round 3: the passing and failing tests' logic changes.
        test("t75p", f"select id, v from {SRC} where id < 0 and v is not null"),
        test("t75f", f"select id, v from {SRC} where id > 0 and v is not null"),
        Build(*TESTS, test_failures=True),
        explain("t75p", "passing test after its logic changed", decision="EXECUTE"),
        explain("t75f", "failing test after its logic changed", decision="EXECUTE"),

        # Round 4: the source gains a row (upstream data drift).
        Sql(f"insert into {DEMO}.${{t75src}} select 2 as id, 'src1' as v"),
        Build(*TESTS, test_failures=True),
        explain("t75p", "passing test after upstream data drift"),
        explain("t75f", "failing test after upstream data drift"),
        explain("t75s", "store_failures test after upstream data drift"),
    ],
)
