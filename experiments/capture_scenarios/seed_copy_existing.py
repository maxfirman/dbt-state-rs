"""#91: seed copies on submit into a dev seed table that already exists.

Hosted copies a seed on submit into an absent dev table when prod holds a
load with the same values (`explain_copy_probes.jsonl` entry 12). This run
asks what hosted does when the dev table exists. One attempt, no retries.

Nodes (per run; each seed yml puts it in target.schema, so prod and dev are
different relations, as in `copy_probes`):
  s91a  case (i): loaded in prod_demo with values Y, loaded in dev_clone with
        values X, then built in dev_clone with values Y. The dev table exists
        with other values; prod's latest load matches the dev submit.
  s91b  case (ii): loaded in prod_demo, built in dev_clone (its table absent:
        the known copy), then the dev table is modified outside dbt (one row
        inserted) and built in dev_clone again with the same values. The dev
        table exists and was modified; its values match prod.

dbt invocations: 2 builds, 1 run-operation, 1 build, 1 state explain = 5.
The other explains are `state-probe` GetExplainMessages calls through this
run's proxy (no dbt invocation).
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEV = "CDO_DEMO_DB.DEV_CLONE_DEMO"
SEEDS = ("${s91a}", "${s91b}")


def seed_yml(name):
    return f"""seeds:
  - name: ${{{name}}}
    config:
      schema: "{{{{ target.schema }}}}"
"""


SCENARIO = Scenario(
    name="seed_copy_existing",
    summary="seed submit copy into an existing dev seed table: other values (i), "
            "modified outside dbt (ii), prod_demo -> dev_clone (#91)",
    names=("s91a", "s91b"),
    steps=[
        Write("seeds/${s91a}.yml", seed_yml("s91a")),
        Write("seeds/${s91b}.yml", seed_yml("s91b")),
        Write("seeds/${s91a}.csv", "id,v\n1,y_${run}\n2,y_${run}\n"),
        Write("seeds/${s91b}.csv", "id,v\n1,b_${run}\n2,b_${run}\n"),

        # prod_demo: first loads (the copy sources).
        Build(*SEEDS),

        # dev_clone: s91a with other values (loads X); s91b into an absent table
        # (the known copy from prod).
        Write("seeds/${s91a}.csv", "id,v\n1,x_${run}\n2,x_${run}\n"),
        Build(*SEEDS, target="dev_clone"),
        Explain("${s91a}", intent="s91a dev load with other values", execution_type=9,
                via="probe"),
        Explain("${s91b}", intent="s91b dev copy into an absent table", execution_type=9,
                via="probe"),

        # Modify the dev s91b outside dbt.
        Sql(f"insert into {DEV}.${{s91b}} (id, v) values (3, 'ext_${{run}}')", target="dev_clone"),

        # dev_clone again: s91a now has prod's values (case i); s91b unchanged
        # values over its modified dev table (case ii).
        Write("seeds/${s91a}.csv", "id,v\n1,y_${run}\n2,y_${run}\n"),
        Build(*SEEDS, target="dev_clone"),
        Explain("${s91a}", intent="case (i): existing dev table with other values, prod matches",
                execution_type=9, target="dev_clone"),
        Explain("${s91b}", intent="case (ii): existing dev table modified outside dbt, prod matches",
                execution_type=9, via="probe"),
    ],
)
