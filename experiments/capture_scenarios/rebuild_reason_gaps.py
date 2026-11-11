"""#40 code review: rebuild reasons and data-test cells that rendered a tree
without a capture.

Nodes (all per-run, in prod_state_demo):
  r40src  source table created by this run (no Upstream producer)
  r40k    static table model
  r40q    table over the source  -> its logic changes while the source drifts
  r40m    table over the source  -> its logic changes (the model r40tm tests)
  r40tm   data test over ref(r40m)          -> its upstream's logic changed
  r40t0   data test with no upstreams       -> first execution, logic change, SKIP
  r40t2   data test over the source and r40k -> source drift, then SKIP

Rounds:
  1  first builds and executions, confirmed
  2  the source gains a row; r40q, r40m and r40t0 change their logic
  3  unchanged rerun

Data tests are explained by unique-id glob (`test.jaffle_shop.<name>*`); the
oracle matches them by `labels.dbt_node_name`.
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
SRC = "{{ source('${r40src}', '${r40src}') }}"
ALL = ("${r40k}", "${r40q}", "${r40m}", "${r40tm}", "${r40t0}", "${r40t2}")


def model(name, body):
    return Write(f"models/marts/${{{name}}}.sql", f"{body}\n")


def test(name, body):
    return Write(f"data-tests/${{{name}}}.sql", f"{body}\n")


def explain_test(name, intent, decision=None, history=True):
    return Explain(f"test.jaffle_shop.${{{name}}}*", intent=intent, node=f"${{{name}}}",
                   decision=decision, execution_type=8, history=history)


SCENARIO = Scenario(
    name="rebuild_reason_gaps",
    summary="FULL logic change under upstream drift; data tests over a changed model, "
            "with no upstreams and over two upstreams (#40 code review)",
    names=("r40src", "r40k", "r40q", "r40m", "r40tm", "r40t0", "r40t2"),
    steps=[
        Write("models/${r40src}.yml", """sources:
  - name: ${r40src}
    schema: prod_state_demo
    tables:
      - name: ${r40src}
"""),
        model("r40k", "select 1 as id, 'k_${run}' as v"),
        model("r40q", f"select id, v from {SRC}"),
        model("r40m", f"select id, v from {SRC}"),
        test("r40tm", "select id from {{ ref('${r40m}') }} where id < 0"),
        test("r40t0", "select 1 as id where 1 = 0"),
        test("r40t2", f"select s.id from {SRC} s join {{{{ ref('${{r40k}}') }}}} k "
                      "on s.id = k.id where s.id < 0"),
        Sql(f"create or replace table {DEMO}.${{r40src}} as select 1 as id, 'src0' as v"),

        # Round 1: first builds and executions.
        Build(*ALL),
        explain_test("r40t0", "data test with no upstreams, first execution",
                     decision="EXECUTE", history=False),

        # Round 2: the source gains a row; three nodes change their logic.
        Sql(f"insert into {DEMO}.${{r40src}} select 2 as id, 'src1' as v"),
        model("r40q", f"select id, v from {SRC} where id >= 0"),
        model("r40m", f"select id, v from {SRC} where id >= 1"),
        test("r40t0", "select 2 as id where 1 = 0"),
        Build(*ALL),
        Explain("${r40q}", intent="FULL model whose logic changed while its upstream drifted",
                decision="EXECUTE", execution_type=1),
        explain_test("r40tm", "data test over a model whose logic changed"),
        explain_test("r40t0", "data test with no upstreams after its logic changed",
                     decision="EXECUTE"),
        explain_test("r40t2", "data test over two upstreams after one drifted"),

        # Round 3: unchanged rerun.
        Build(*ALL),
        explain_test("r40t0", "data test with no upstreams, unchanged rerun"),
        explain_test("r40t2", "data test over two upstreams, unchanged rerun"),
    ],
)
