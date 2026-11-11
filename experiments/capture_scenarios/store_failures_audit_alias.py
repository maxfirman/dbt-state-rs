"""#106: a store_failures data test with a configured `alias`, so its audit
relation is not named after the test.

  t106asrc  source table in PROD_STATE_DEMO (no Upstream producer)
  t106a     passing test with `store_failures: true, alias: <t106a>_aud`: its
            audit relation is the table PROD_STATE_DEMO.<T106A>_AUD
  t106b     passing test with `store_failures: true, store_failures_as: 'view',
            alias: <t106b>_aud`: its audit relation is a view

Rounds:
  1  first execution of both (each writes its audit relation)
  2  unchanged rerun: does the client send the alias anywhere, does hosted
     SKIP, and does the tree list the audit relation?

`t106b` is explained with state-probe (no dbt invocation).
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
TESTS = ("${t106a}", "${t106b}")
SRC = "{{ source('${t106asrc}', '${t106asrc}') }}"


def test(name, body, config):
    return Write(f"data-tests/${{{name}}}.sql", f"{config}{body}\n")


SCENARIO = Scenario(
    name="store_failures_audit_alias",
    summary="store_failures data tests with a configured alias (audit table, audit view), "
            "first execution and unchanged rerun (#106)",
    names=("t106asrc", "t106a", "t106b"),
    steps=[
        Write("models/${t106asrc}.yml", """sources:
  - name: ${t106asrc}
    schema: prod_state_demo
    tables:
      - name: ${t106asrc}
"""),
        test("t106a", f"select id, v from {SRC} where id < 0",
             config="{{ config(store_failures=true, alias='${t106a}_aud') }}\n"),
        test("t106b", f"select id, v from {SRC} where id < 0",
             config="{{ config(store_failures=true, store_failures_as='view', "
                    "alias='${t106b}_aud') }}\n"),
        Sql(f"create or replace table {DEMO}.${{t106asrc}} as select 1 as id, 'src0' as v"),

        # Round 1: first execution writes both audit relations.
        Build(*TESTS),

        # Round 2: unchanged rerun.
        Build(*TESTS),
        Explain("test.jaffle_shop.${t106a}*", intent="aliased audit table, unchanged rerun",
                node="${t106a}", execution_type=8),
        Explain("test.jaffle_shop.${t106b}*", intent="aliased audit view, unchanged rerun",
                node="${t106b}", execution_type=8, via="probe"),
    ],
)
