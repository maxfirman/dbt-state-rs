"""#88: a store_failures data test's own audit relation, for the two cases
`data_test_execute` did not capture.

Two singular data tests over one source table created by this run:

  t88src  source table (no Upstream producer), created by a Sql step
  t88f    failing test (`where id > 0`: one row, severity error, so dbt build
          exits 1) with `store_failures: true`: its audit relation is a table
          holding the failing row
  t88v    passing test with `store_failures: true, store_failures_as: 'view'`:
          its audit relation is a view

Rounds:
  1  first execution of both (each writes its audit relation)
  2  unchanged rerun: does hosted SKIP although each test now reports its
     audit relation in `tables`, and does the tree list it?

Data tests are explained by unique-id glob (`test.jaffle_shop.<name>*`); the
oracle matches them by `labels.dbt_node_name`.
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
TESTS = ("${t88f}", "${t88v}")
SRC = "{{ source('${t88src}', '${t88src}') }}"


def test(name, body, config):
    return Write(f"data-tests/${{{name}}}.sql", f"{config}{body}\n")


def explain(name, intent):
    return Explain(f"test.jaffle_shop.${{{name}}}*", intent=intent, node=f"${{{name}}}",
                   decision="SKIP", execution_type=8)


SCENARIO = Scenario(
    name="store_failures_audit",
    summary="store_failures data tests whose audit relation is a table holding failures "
            "or a view, first execution and unchanged rerun (#88)",
    names=("t88src", "t88f", "t88v"),
    steps=[
        Write("models/${t88src}.yml", """sources:
  - name: ${t88src}
    schema: prod_state_demo
    tables:
      - name: ${t88src}
"""),
        test("t88f", f"select id, v from {SRC} where id > 0",
             config="{{ config(store_failures=true) }}\n"),
        test("t88v", f"select id, v from {SRC} where id < 0",
             config="{{ config(store_failures=true, store_failures_as='view') }}\n"),
        Sql(f"create or replace table {DEMO}.${{t88src}} as select 1 as id, 'src0' as v"),

        # Round 1: first execution writes both audit relations.
        Build(*TESTS, test_failures=True),

        # Round 2: unchanged rerun.
        Build(*TESTS, test_failures=True),
        explain("t88f", "failing store_failures test (audit table) unchanged rerun"),
        explain("t88v", "store_failures_as view test unchanged rerun"),
    ],
)
