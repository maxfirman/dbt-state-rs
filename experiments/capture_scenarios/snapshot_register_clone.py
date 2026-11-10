"""#76: the snapshot copy through RegisterClone (execution type 7), dev_clone deferring to prod_demo.

The snapshot sets no `target_schema`, so it lands in each target's own schema.
Under dbt State auto-deferral the client keeps a snapshot's rendered
`target_schema` for the deferred relation: with `target_schema=target.schema`
(the `copy_probes` scenario) the deferred relation is the dev relation itself
and the 2.0.11 dev-clone pre-step logs "source and target relations are
identical" and sends no RegisterClone. Without it the deferred relation is
in prod_state_demo and the pre-step (`state.pre_clone` default IfTableMissing)
sends RegisterClone before the snapshot's own submit.

Nodes (per-run):
  c76rsrc  source table in prod_state_demo created by this run (no Upstream producer)
  c76rsnap snapshot of c76rsrc: built in prod_demo, then in dev_clone where its
           target is missing.
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"

SCENARIO = Scenario(
    name="snapshot_register_clone",
    summary="snapshot dev-clone pre-step RegisterClone and its post-copy decision, "
            "prod_demo -> dev_clone (#76)",
    names=("c76rsrc", "c76rsnap"),
    steps=[
        Write("models/${c76rsrc}.yml", """sources:
  - name: ${c76rsrc}
    schema: prod_state_demo
    tables:
      - name: ${c76rsrc}
"""),
        Write("snapshots/${c76rsnap}.sql", """{% snapshot ${c76rsnap} %}
{{ config(unique_key='id', strategy='check', check_cols=['v']) }}
select id, v from {{ source('${c76rsrc}', '${c76rsrc}') }}
{% endsnapshot %}
"""),
        Sql(f"create or replace table {DEMO}.${{c76rsrc}} as select 1 as id, 'src_${{run}}' as v"),
        # prod_demo: the copy source, built and confirmed.
        Build("${c76rsnap}"),
        # dev_clone: the target is missing, so the pre-step sends RegisterClone,
        # then the client submits the snapshot (the post-copy decision).
        Build("${c76rsnap}", target="dev_clone"),
        Explain("${c76rsnap}", intent="snapshot post-copy decision after the RegisterClone pre-step",
                execution_type=7, target="dev_clone"),
    ],
)
