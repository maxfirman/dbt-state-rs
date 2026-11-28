"""#271: Snowflake reuse after a client-requested copy (RegisterClone) when
dev built its own upstream.

`dev_clone` defers to `prod_demo` with the default `state.pre_clone`
(IfTableMissing), so a merge incremental or a snapshot whose dev relation is
absent is copied from prod by the client's pre-step (`RegisterClone`, then
`ConfirmExecution`) before the downstream's own `SubmitEnrichedSQL`: the
post-copy decision (#93 `post_copy_identity`). Each case has a merge
incremental downstream (`unique_key='id'`, `lag_tolerance: 0s`) and a
snapshot downstream (`check` on `v`; a snapshot cannot set its Lag tolerance,
so it keeps the client's default), both `select id, v from <upstream>`.

  I  independent input: the upstream `ri_up` (a table, `lag_tolerance: 0s`)
     reads `ri_in` in its Target's own schema. Prod's input holds
     (1, 'prod'), dev's (1, 'dev'). Dev's upstream is built and confirmed on
     `dev_noclone` (no copy permission) before the downstream copy, so the
     copy holds rows computed from prod's upstream while the post-copy
     submit reads dev's. A post-copy SKIP that leaves 'prod' where the
     Reference holds 'dev' is the wrong-marker failure.
  S  shared-input control: the downstreams read prod's upstream `rs_up`
     through a source fixed to prod_state_demo (`rs_fix`), so prod and dev
     read the same physical upstream. A post-copy SKIP serves the right rows.

After each dev round: a `state-probe` Explain of each downstream decision
and State-free reads of what dev serves. The second round is the unchanged
rerun. Last, the Reference: dev's input copied into `ref_state_demo`, then
`dbt build --no-manage-state --full-refresh` of every dev node on `ref_demo`
(an independent schema, so the snapshot's Reference is a first build over
the intended inputs), read back.

`ref_demo` (CDO_DEMO_DB.ref_state_demo, no deferral, `allow_clones: false`)
and `dev_noclone` (`dev_clone` with `allow_clones: false`) must be outputs of
the `--profiles` file: use a private copy (mode 600, outside the repo) and
delete it after the run. `ref_state_demo` must exist before the run.
`experiments/register_clone_origin.py WORK_DIR` prints the per-case record.
"""
from explain_capture import Build, Explain, Read, Reference, Scenario, Sql, Write

PROD = "CDO_DEMO_DB.PROD_STATE_DEMO"
DEV = "CDO_DEMO_DB.DEV_CLONE_DEMO"
REF = "CDO_DEMO_DB.REF_STATE_DEMO"
CASES = ("i", "s")
NAMES = ("ri_in", "ri_up", "ri_dm", "ri_ds", "rs_in", "rs_up", "rs_fix", "rs_dm", "rs_ds")
ZERO = "state={'lag_tolerance': '0s'}"
NO_COPY = "dev_noclone"
REF_TARGET = "ref_demo"

SOURCES = """sources:
  - name: ${ri_in}
    schema: "{{ target.schema }}"
    tables:
      - name: ${ri_in}
  - name: ${rs_in}
    schema: prod_state_demo
    tables:
      - name: ${rs_in}
  - name: ${rs_fix}
    schema: prod_state_demo
    tables:
      - name: ${rs_up}
"""


def _up(c):
    return Write(f"models/marts/${{r{c}_up}}.sql",
                 f"{{{{ config(materialized='table', {ZERO}) }}}}\n"
                 f"select id, v from {{{{ source('${{r{c}_in}}', '${{r{c}_in}}') }}}}\n")


def _from(c):
    return ("{{ source('${rs_fix}', '${rs_up}') }}" if c == "s"
            else "{{ ref('${ri_up}') }}")


def _merge(c):
    return Write(f"models/marts/${{r{c}_dm}}.sql",
                 "{{ config(materialized='incremental', incremental_strategy='merge', "
                 f"unique_key='id', {ZERO}) }}}}\n"
                 f"select id, v from {_from(c)}\n")


def _snapshot(c):
    name = f"${{r{c}_ds}}"
    return Write(f"snapshots/{name}.sql",
                 f"{{% snapshot {name} %}}\n"
                 "{{ config(unique_key='id', strategy='check', check_cols=['v']) }}\n"
                 f"select id, v from {_from(c)}\n"
                 "{% endsnapshot %}\n")


def downstreams(c):
    return (f"r{c}_dm", f"r{c}_ds")


def dev_nodes(c):
    """What dev builds in case c: I also its own upstream; S reads prod's."""
    return (("ri_up",) if c == "i" else ()) + downstreams(c)


def _dev_round(c, label):
    nodes = dev_nodes(c)
    return [
        Build(*[f"${{{n}}}" for n in nodes], target="dev_clone"),
        *[Explain(f"${{{n}}}", intent=f"{c.upper()} {label}: {n}", target="dev_clone",
                  via="probe") for n in downstreams(c)],
        *[Read(f"{DEV}.${{{n}}}", target="dev_clone") for n in nodes],
    ]


STEPS = [
    Write("models/register_clone_origin_${run}.yml", SOURCES),
    _up("i"), _up("s"),
    *[w(c) for c in CASES for w in (_merge, _snapshot)],
    Sql(f"create or replace table {PROD}.${{ri_in}} as select 1 as id, 'prod' as v",
        f"create or replace table {DEV}.${{ri_in}} as select 1 as id, 'dev' as v",
        f"create or replace table {PROD}.${{rs_in}} as select 1 as id, 'prod' as v"),
    # prod_demo: the upstreams, then the downstreams (S reads its upstream
    # through a source, so it has no DAG edge and needs its own invocation).
    Build("${ri_up}", "${rs_up}"),
    Build("${ri_dm}", "${ri_ds}", "${rs_dm}", "${rs_ds}"),
    *[Read(f"{PROD}.${{{n}}}") for c in CASES for n in downstreams(c)],
    # I: dev's own upstream, built and confirmed without copy permission
    # before any downstream copy.
    Build("${ri_up}", target=NO_COPY),
    Explain("${ri_up}", intent="I: dev upstream built without copy permission",
            target=NO_COPY, via="probe"),
    # First dev round (the RegisterClone pre-step and the post-copy submit),
    # then the unchanged rerun.
    *[s for c in CASES for s in _dev_round(c, "first dev build")],
    *[s for c in CASES for s in _dev_round(c, "unchanged rerun")],
    # The Reference over the intended inputs, in an independent schema.
    Sql(f"create or replace table {REF}.${{ri_in}} as select * from {DEV}.${{ri_in}}"),
    Read(f"{REF}.${{ri_in}}", target=REF_TARGET),
    Reference(*[f"${{{n}}}" for c in CASES for n in dev_nodes(c)], target=REF_TARGET),
    *[Read(f"{REF}.${{{n}}}", target=REF_TARGET) for c in CASES for n in dev_nodes(c)],
]

SCENARIO = Scenario(
    name="register_clone_input_origin",
    summary="post-RegisterClone reuse of a merge incremental and a snapshot whose dev upstream "
            "was built independently (dev marker) vs a shared prod upstream, with an "
            "independent-schema Reference (#271)",
    names=NAMES,
    steps=STEPS,
)
