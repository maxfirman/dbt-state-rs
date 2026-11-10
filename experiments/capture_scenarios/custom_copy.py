"""#74: does hosted copy a custom materialization (DBT_CUSTOM=11) across targets?

One attempt (spec #40: one attempt per negative copy probe). A custom node and
a FULL control with the same shape and no upstream are built in prod_demo,
then built in dev_clone (allow_clones, defer to prod_demo), where neither
Target relation exists. The FULL control shows the submit-time Result copy
path is open in this setup; the custom node's dev decision and Explain tree
are the evidence for the DBT_CUSTOM x CLONE cell.

Nodes (per-run):
  c74kmat  the run's custom materialization (create or replace table as)
  c74k     custom, no upstream
  c74kt    FULL table control, no upstream
"""
from explain_capture import Build, Explain, Scenario, Write

MATERIALIZATION = """{% materialization ${c74kmat}, default %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {{ run_hooks(pre_hooks) }}
  {% call statement('main') -%}
    create or replace table {{ target_relation }} as ({{ sql }})
  {%- endcall %}
  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""

SCENARIO = Scenario(
    name="custom_copy",
    summary="custom materialization built in prod_demo, then in dev_clone where its "
            "Target relation is absent: CLONE or EXECUTE? FULL control (#74)",
    names=("c74kmat", "c74k", "c74kt"),
    steps=[
        Write("macros/${c74kmat}.sql", MATERIALIZATION),
        Write("models/marts/${c74k}.sql",
              "{{ config(materialized='${c74kmat}') }}\nselect 1 as id, 'k_${run}' as v\n"),
        Write("models/marts/${c74kt}.sql",
              "{{ config(materialized='table') }}\nselect 1 as id, 'kt_${run}' as v\n"),
        Build("${c74k}", "${c74kt}"),
        Build("${c74k}", "${c74kt}", target="dev_clone"),
        Explain("${c74k}", intent="custom built in dev_clone over a prod equivalent",
                execution_type=11, target="dev_clone"),
    ],
)
