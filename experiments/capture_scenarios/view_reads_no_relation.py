"""#117: VIEW Explain trees for a view that reads no relation, next to views
that read one.

Client 2.0.11 reports no upstreams for a view (only its own Target
relation), so hosted's wording can only follow the view's SQL. Earlier
captures show `no upstream dependencies` for a view that reads no relation
(explain_reason_kind_cells.jsonl `vc0`, `cv0`) and `upstream data is up to
date` for a view over a `ref` (explain_happy_history.jsonl,
explain_snapshot_history.jsonl `s72vl`). This run captures the SKIP and the
logic-change rebuild of each, and the SQL forms between the two:

  vr0  view, `select` without FROM                     (reads no relation)
  vrc  view, FROM a CTE that itself has no FROM
  vrv  view, FROM a VALUES list
  vrf  view, FROM a table function (FLATTEN)
  vrt  table model                                     (vrr's upstream)
  vrr  view over ref(vrt)                              (reads a relation)

Rounds: first build, unchanged rerun (SKIP, explained), then the views'
logic changes (rebuild, explained). 4 dbt invocations.
"""
from explain_capture import Build, Explain, Scenario, Write

VIEWS = ("vr0", "vrc", "vrv", "vrf", "vrr")


def view(name, body):
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config(materialized='view') }}}}\n{body}\n")


def bodies(tag):
    return [
        view("vr0", f"select 1 as id, 'vr0_{tag}_${{run}}' as v"),
        view("vrc", f"with a as (select 1 as id, 'vrc_{tag}_${{run}}' as v)\nselect id, v from a"),
        view("vrv", f"select column1 as id, column2 as v from (values (1, 'vrv_{tag}_${{run}}'))"),
        view("vrf", f"select f.value::int as id, 'vrf_{tag}_${{run}}' as v\n"
                    "from table(flatten(input => array_construct(1, 2))) f"),
        view("vrr", f"select id, v, 'vrr_{tag}_${{run}}' as r from {{{{ ref('${{vrt}}') }}}}"),
    ]


def probe(name, intent, decision):
    return Explain(f"${{{name}}}", intent=intent, decision=decision, execution_type=10,
                   via="probe")


ALL = ("${vrt}", *(f"${{{v}}}" for v in VIEWS))

SCENARIO = Scenario(
    name="view_reads_no_relation",
    summary="VIEW SKIP and logic-change rebuild for views that read no relation, a CTE, "
            "VALUES, a table function, and a ref (#117)",
    names=("vrt", *VIEWS),
    steps=[
        Write("models/marts/${vrt}.sql", "select 1 as id, 'vrt_${run}' as v\n"),
        *bodies("0"),

        # Round 1: first builds, confirmed.
        Build(*ALL),

        # Round 2: unchanged rerun.
        Build(*ALL),
        Explain("${vr0}", intent="view without FROM, unchanged SKIP (dbt state explain)",
                decision="SKIP", execution_type=10),
        probe("vrc", "view over a CTE without FROM, unchanged SKIP", "SKIP"),
        probe("vrv", "view over VALUES, unchanged SKIP", "SKIP"),
        probe("vrf", "view over a table function, unchanged SKIP", "SKIP"),
        probe("vrr", "view over a ref, unchanged SKIP", "SKIP"),

        # Round 3: every view's own logic changes.
        *bodies("1"),
        Build(*(f"${{{v}}}" for v in VIEWS)),
        *[probe(v, "view, own logic changed", "EXECUTE") for v in VIEWS],
    ],
)
