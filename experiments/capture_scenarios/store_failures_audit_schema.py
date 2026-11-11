"""#106: a store_failures data test whose audit relation is in a schema other
than the target schema.

jaffle-shop's `generate_schema_name` puts every custom schema in the target
schema on `prod_demo`, so the run's project copy replaces it: a test with a
custom schema (dbt's default test schema `dbt_test__audit`) goes to
`<target schema>_<custom schema>` as in dbt's own default macro
(`PROD_STATE_DEMO_DBT_TEST__AUDIT`, still in CDO_DEMO_DB). Other nodes keep
jaffle-shop's placement; only this run's nodes are built.

  t106src  source table in PROD_STATE_DEMO (no Upstream producer)
  t106s    passing test with `store_failures: true`: its audit relation is a
           table in the audit schema
  t106v    passing test with `store_failures: true, store_failures_as: 'view'`:
           its audit relation is a view in the audit schema

Rounds:
  1  first execution of both (each writes its audit relation)
  2  unchanged rerun: what `default_schema` and `tables` does the client send,
     does hosted SKIP, and does the tree list the audit relation?

`t106v` is explained with state-probe (no dbt invocation).
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
TESTS = ("${t106s}", "${t106v}")
SRC = "{{ source('${t106src}', '${t106src}') }}"

SCHEMA_MACRO = """{% macro generate_schema_name(custom_schema_name, node) %}
    {% if node.resource_type == 'seed' %}
        {{ custom_schema_name | trim }}
    {% elif custom_schema_name is none %}
        {{ target.schema }}
    {% elif node.resource_type == 'test' %}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {% else %}
        {{ target.schema }}
    {% endif %}
{% endmacro %}
"""


def test(name, body, config):
    return Write(f"data-tests/${{{name}}}.sql", f"{config}{body}\n")


SCENARIO = Scenario(
    name="store_failures_audit_schema",
    summary="store_failures data tests whose audit relation (table, view) is in a separate "
            "audit schema, first execution and unchanged rerun (#106)",
    names=("t106src", "t106s", "t106v"),
    steps=[
        Write("macros/generate_schema_name.sql", SCHEMA_MACRO),
        Write("models/${t106src}.yml", """sources:
  - name: ${t106src}
    schema: prod_state_demo
    tables:
      - name: ${t106src}
"""),
        test("t106s", f"select id, v from {SRC} where id < 0",
             config="{{ config(store_failures=true) }}\n"),
        test("t106v", f"select id, v from {SRC} where id < 0",
             config="{{ config(store_failures=true, store_failures_as='view') }}\n"),
        Sql(f"create or replace table {DEMO}.${{t106src}} as select 1 as id, 'src0' as v"),

        # Round 1: first execution writes both audit relations.
        Build(*TESTS),

        # Round 2: unchanged rerun.
        Build(*TESTS),
        Explain("test.jaffle_shop.${t106s}*", intent="audit table in another schema, unchanged rerun",
                node="${t106s}", execution_type=8),
        Explain("test.jaffle_shop.${t106v}*", intent="audit view in another schema, unchanged rerun",
                node="${t106v}", execution_type=8, via="probe"),
    ],
)
