"""#76 + #78 item 1: one live copy attempt per path, dev_clone deferring to prod_demo.

Each node is built once in prod_demo, then once in dev_clone. dev_clone has
`allow_clones: true` and `defer_to_target: prod_demo`, so the client
synthesizes defer nodes from prod_demo (dbt State auto-deferral; no prod
manifest is needed for `--defer --state`).

Nodes (all per-run; the source table is shared by both targets):
  c76src   source table in prod_state_demo created by this run (no Upstream producer)
  c76seed  seed (SubmitValues); its yml puts it in target.schema, not jaffle-shop's
           global `raw` seed schema, so prod and dev are different relations.
           Path 1: does hosted answer the dev SubmitValues with ready_to_clone?
  c76snap  snapshot of c76src with `target_schema=target.schema`. Meant as path 2,
           but under auto-deferral the client keeps the rendered target_schema
           for the deferred relation, so the dev-clone pre-step finds source ==
           target and sends no RegisterClone (diagnosed offline against a local
           server). It still records hosted's submit for a missing dev snapshot
           target. The RegisterClone attempt is `snapshot_register_clone`.
  c76view  view over c76src. Path 3 (#78 item 1): does hosted answer the dev
           SubmitEnrichedSQL (execution type 10) with ready_to_clone?

One attempt per path, no retries (spec #40 "Out of Scope").
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
NODES = ("${c76seed}", "${c76snap}", "${c76view}")

SCENARIO = Scenario(
    name="copy_probes",
    summary="seed CLONE, snapshot RegisterClone and VIEW CLONE attempts, prod_demo -> dev_clone "
            "(#76, #78 item 1)",
    names=("c76src", "c76seed", "c76snap", "c76view"),
    steps=[
        Write("models/${c76src}.yml", """sources:
  - name: ${c76src}
    schema: prod_state_demo
    tables:
      - name: ${c76src}
"""),
        Write("seeds/${c76seed}.csv", "id,v\n1,seed_${run}\n2,seed_${run}\n"),
        Write("seeds/${c76seed}.yml", """seeds:
  - name: ${c76seed}
    config:
      schema: "{{ target.schema }}"
"""),
        Write("snapshots/${c76snap}.sql", """{% snapshot ${c76snap} %}
{{ config(target_schema=target.schema, unique_key='id', strategy='check', check_cols=['v']) }}
select id, v from {{ source('${c76src}', '${c76src}') }}
{% endsnapshot %}
"""),
        Write("models/marts/${c76view}.sql",
              "{{ config(materialized='view') }}\n"
              "select id, v from {{ source('${c76src}', '${c76src}') }}\n"),
        Sql(f"create or replace table {DEMO}.${{c76src}} as select 1 as id, 'src_${{run}}' as v"),

        # prod_demo: first builds, confirmed (the copy sources).
        Build(*NODES),

        # dev_clone: every dev target is missing; one attempt per copy path.
        Build(*NODES, target="dev_clone"),
        Explain("${c76seed}", intent="seed in dev_clone (seed CLONE attempt)", execution_type=9,
                target="dev_clone"),
        Explain("${c76snap}", intent="snapshot post-copy decision after the RegisterClone pre-step",
                execution_type=7, target="dev_clone"),
        Explain("${c76view}", intent="view in dev_clone (VIEW CLONE attempt, #78)", execution_type=10,
                target="dev_clone"),
    ],
)
