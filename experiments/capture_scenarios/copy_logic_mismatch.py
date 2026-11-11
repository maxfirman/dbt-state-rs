"""#104 (104c): rebuilds over a dev-clone pre-step copy whose submit does not
have the copy's logic, and the FULL copy refused by `allow_clones: false`.

The `dev_clone` target defers to `prod_demo` and leaves `state.pre_clone`
unset (IfTableMissing), so an incremental or snapshot whose dev relation is
absent is copied with `Clone.RegisterClone` and confirmed before its own
submit (`run_cache_dev_clone.rs::dev_clone_candidate_for_node`: only
incremental models and snapshots get the pre-step). Every node reads the
jaffle-shop raw orders table (`CDO_DEMO_DB.RAW`, read only), outside both
targets' default schemas, so a node's prod and dev SQL are byte-identical
unless the run changes it, and selects a per-node literal, so no node is
another's equivalent.

Round 1 (prod_demo): every node is built and confirmed (the copy sources).

Round 2 (dev_clone): six nodes change their logic (`where id is not null`)
before their first dev build. The pre-step copies the prod relation, then the
post-copy submit carries other logic than the copy inherited (this server's
`no_reuse_candidate.copy_logic_mismatch`):
  la4 append, lm4 merge, li4 insert_overwrite, ld4 delete+insert,
  lb4 microbatch, ls4 snapshot.
Three more append incrementals keep their logic (post-copy SKIP, #93): kf4,
kv4, kc4.

Round 3 (dev_clone): kf4, kv4 and kc4 change only their materialization, to a
table (FULL), a view (VIEW) and a custom materialization (DBT_CUSTOM). The
only kinds that can follow a RegisterClone copy without one are reached this
way: the submit lands on the APPEND copy row.

Round 4 (dev_noclone, a copy of `dev_clone` with `allow_clones: false`): cf4,
a table whose dev relation is absent and whose prod build is its equivalent
(this server's `copy_chain_limit`).

Nodes (per-run): r104 (source name for raw.raw_orders), m104 (the custom
materialization macro), the models above.
"""
from explain_capture import Build, Explain, Scenario, Write

WINDOW = ("--event-time-start", "2026-11-01", "--event-time-end", "2026-11-03")
INCREMENTALS = (
    ("la4", 2, "incremental_strategy='append'"),
    ("lm4", 3, "incremental_strategy='merge', unique_key='id'"),
    ("li4", 4, "incremental_strategy='insert_overwrite'"),
    ("ld4", 5, "incremental_strategy='delete+insert', unique_key='id'"),
    ("lb4", 6, "incremental_strategy='microbatch', event_time='ordered_at', "
               "begin='2026-11-01', batch_size='day'"),
)
APPEND = "materialized='incremental', incremental_strategy='append'"
KINDS = (("kf4", 1, "materialized='table'"), ("kv4", 10, "materialized='view'"),
         ("kc4", 11, "materialized='${m104}'"))
LOGIC = tuple(n for n, _, _ in INCREMENTALS) + ("ls4",)
CHANGED = tuple(n for n, _, _ in KINDS)

MATERIALIZATION = """{% materialization ${m104}, default %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {{ run_hooks(pre_hooks) }}
  {% call statement('main') -%}
    create or replace table {{ target_relation }} as ({{ sql }})
  {%- endcall %}
  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""


def body(name, changed=False):
    where = " where id is not null" if changed else ""
    return (f"select id, customer, ordered_at, '{name}_${{run}}' as r "
            f"from {{{{ source('${{r104}}', 'raw_orders') }}}}{where}")


def model(name, config, changed=False):
    return Write(f"models/marts/${{{name}}}.sql",
                 f"{{{{ config({config}) }}}}\n{body(name, changed)}\n")


def snapshot(changed=False):
    return Write("snapshots/${ls4}.sql",
                 "{% snapshot ${ls4} %}\n"
                 "{{ config(unique_key='id', strategy='check', check_cols=['customer']) }}\n"
                 f"{body('ls4', changed)}\n"
                 "{% endsnapshot %}\n")


def logic_models(changed):
    return [*(model(n, f"materialized='incremental', {cfg}", changed)
              for n, _, cfg in INCREMENTALS),
            snapshot(changed)]


def ref(names):
    return tuple(f"${{{n}}}" for n in names)


SCENARIO = Scenario(
    name="copy_logic_mismatch",
    summary="rebuilds over a RegisterClone copy (logic change: five incrementals and a "
            "snapshot; kind change: FULL, VIEW, DBT_CUSTOM) and a FULL copy refused by "
            "allow_clones: false (#104)",
    names=("r104", "m104", *LOGIC, *CHANGED, "cf4"),
    steps=[
        Write("models/${r104}.yml", """sources:
  - name: ${r104}
    schema: raw
    tables:
      - name: raw_orders
        config:
          event_time: ordered_at
"""),
        Write("macros/${m104}.sql", MATERIALIZATION),
        *logic_models(changed=False),
        *(model(n, APPEND) for n in CHANGED),
        model("cf4", "materialized='table'"),

        # Round 1, prod_demo: the copy sources, built and confirmed.
        Build(*ref(LOGIC + CHANGED + ("cf4",)), args=WINDOW),

        # Round 2, dev_clone: copied by the pre-step, then submitted.
        *logic_models(changed=True),
        Build(*ref(LOGIC + CHANGED), target="dev_clone", args=WINDOW),
        *(Explain(f"${{{n}}}", intent=f"{cfg.split(',')[0]}: logic change after a "
                  "RegisterClone copy", decision="EXECUTE", execution_type=et,
                  target="dev_clone")
          for n, et, cfg in INCREMENTALS),
        Explain("${ls4}", intent="snapshot: logic change after a RegisterClone copy",
                decision="EXECUTE", execution_type=7, target="dev_clone"),

        # Round 3, dev_clone: same SQL, another kind, over the APPEND copy row.
        *(model(n, cfg) for n, _, cfg in KINDS),
        Build(*ref(CHANGED), target="dev_clone", args=WINDOW),
        *(Explain(f"${{{n}}}", intent=f"{cfg}: kind change over an APPEND RegisterClone "
                  "copy, same SQL", execution_type=et, target="dev_clone")
          for n, et, cfg in KINDS),

        # Round 4, dev_noclone: a FULL equivalent exists, copies are forbidden.
        Build("${cf4}", target="dev_noclone", args=WINDOW),
        Explain("${cf4}", intent="table with a prod equivalent, allow_clones: false",
                decision="EXECUTE", execution_type=1, target="dev_noclone"),
    ],
)
