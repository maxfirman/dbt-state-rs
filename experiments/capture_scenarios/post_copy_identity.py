"""#93: which identity hosted uses to reuse a dev-clone pre-step copy.

`dev_clone` defers to `prod_demo` (auto-deferral, `state.pre_clone` default
IfTableMissing), so a snapshot or incremental whose dev relation is missing is
copied with `RegisterClone` and confirmed before its own submit. The post-copy
submit is then judged against the copy. Three upstream forms separate the
candidate identities:

  c93sa  snapshot over a source table in prod_state_demo: the post-copy submit's
         SQL text is byte-identical to the prod build's (the #76 control).
  c93st  snapshot over ref(c93upt), a table model built in both targets: the
         SQL text differs from the prod build's only in the upstream's schema.
  c93sv  snapshot over ref(c93upv), a view built in both targets: as c93st, and
         the dependency's definition reads the prod_state_demo source in both.
  c93it  append incremental over ref(c93upt): the incremental form of c93st.

Nodes (per-run): c93src (source table created by this run), c93upt, c93upv.
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"


def snapshot(name, select):
    return Write(f"snapshots/${{{name}}}.sql",
                 f"{{% snapshot ${{{name}}} %}}\n"
                 "{{ config(unique_key='id', strategy='check', check_cols=['v']) }}\n"
                 f"{select}\n"
                 "{% endsnapshot %}\n")


NODES = ("${c93upt}", "${c93upv}", "${c93sa}", "${c93st}", "${c93sv}", "${c93it}")

SCENARIO = Scenario(
    name="post_copy_identity",
    summary="post-RegisterClone submits over a fixed source, a ref'd table and a ref'd view, "
            "prod_demo -> dev_clone (#93)",
    names=("c93src", "c93upt", "c93upv", "c93sa", "c93st", "c93sv", "c93it"),
    steps=[
        Write("models/${c93src}.yml", """sources:
  - name: ${c93src}
    schema: prod_state_demo
    tables:
      - name: ${c93src}
"""),
        Write("models/marts/${c93upt}.sql",
              "{{ config(materialized='table') }}\n"
              "select id, v from {{ source('${c93src}', '${c93src}') }}\n"),
        Write("models/marts/${c93upv}.sql",
              "{{ config(materialized='view') }}\n"
              "select id, v from {{ source('${c93src}', '${c93src}') }}\n"),
        snapshot("c93sa", "select id, v from {{ source('${c93src}', '${c93src}') }}"),
        snapshot("c93st", "select id, v from {{ ref('${c93upt}') }}"),
        snapshot("c93sv", "select id, v from {{ ref('${c93upv}') }}"),
        Write("models/marts/${c93it}.sql",
              "{{ config(materialized='incremental', incremental_strategy='append') }}\n"
              "select id, v from {{ ref('${c93upt}') }}\n"),
        Sql(f"create or replace table {DEMO}.${{c93src}} as select 1 as id, 'src_${{run}}' as v"),
        # prod_demo: the copy sources, built and confirmed.
        Build(*NODES),
        # dev_clone: the upstreams are built (or copied on submit) first; each
        # snapshot and the incremental is missing, so the pre-step sends
        # RegisterClone before its own submit (the post-copy decision).
        Build(*NODES, target="dev_clone"),
        Explain("${c93sa}", intent="post-copy decision, upstream physically identical (control)",
                execution_type=7, target="dev_clone"),
        Explain("${c93st}", intent="post-copy decision, upstream a ref'd table resolved per target",
                execution_type=7, target="dev_clone"),
        Explain("${c93sv}", intent="post-copy decision, upstream a ref'd view resolved per target",
                execution_type=7, target="dev_clone"),
        Explain("${c93it}", intent="post-copy append decision, upstream a ref'd table",
                execution_type=2, target="dev_clone"),
    ],
)
