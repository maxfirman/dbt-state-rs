"""#104 (group b): the query and Target relation Decision reason x node kind
cells, every node kind in one campaign.

Kinds: the five incremental strategies (a append, m merge, io insert_overwrite,
di delete+insert, mb microbatch), sn snapshot, and VIEW / DBT_CUSTOM pairs. An
upstream is the jaffle-shop raw orders table (`CDO_DEMO_DB.RAW`, read only)
through a per-run source (`src`; `srcmb` declares its `event_time` for the
microbatch nodes) or the table model `uq`. "No upstream" nodes select
literals. Every node selects a per-run, per-node literal, so no node has an
equivalent under another name.

An execution type change uses `--full-refresh`: the client records an
incremental's full refresh with RecordExecutions as FULL and submits a
snapshot as FULL, with the SQL identity of the ordinary run
(`explain_incremental_append_merge_history.jsonl` 69-79,
`explain_snapshot_history.jsonl` 58-60). A VIEW and a DBT_CUSTOM node send the
same semantic extras (`lookback`), so they swap materializations.

Roles (node `<kind>_<role>`):
  z   no upstream: R1 build, R3 unchanged (reused.fresh), R4 own logic changes
      (identity_mismatch)
  c   upstream: R1, R2 --full-refresh, R3 (own_target_modified against R1)
  cz  as c, no upstream
  e   upstream: first built by R2 --full-refresh, R3 (execution_type_changed)
  ez  as e, no upstream
  q   over ref(uq): R1, R3 after uq's logic changed (upstream_query_changed)
  d   upstream: R1, its Target relation dropped, R4 own logic changes
      (identity_mismatch over an absent Target relation)
The snapshot `sn_c` / `sn_cz` R2 submits are FULL over SNAPSHOT history
(execution_type_changed for FULL, with and without an upstream).

VIEW / DBT_CUSTOM (`vc*` start as a view, `cv*` as a custom node; `0` = no
upstream):
  R1 first type, R3 the other type (execution_type_changed), R4 the first type
  again (own_target_modified against R1).
Plus `vw_d` / `cu_d` in role d.

Every explain but one is sent with state-probe right after the build that
decided it (`via="probe"`); `di_d` is also explained by `dbt state explain`
to check the two answers agree.

The capture (run `141724396a43`) stopped in round 3: its uq change compared
the uuid `id` with 1, so uq failed and dbt skipped the q nodes, and the round
3 explains were not sent. `reason_kind_cells_more` continued that run
(`--run-id 141724396a43 --prior-window <its window.jsonl>`); the fixture is
the two fixture candidates concatenated.
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
WINDOW = ("--event-time-start", "2026-11-01", "--event-time-end", "2026-11-03")
INCR = {
    "a": "materialized='incremental', incremental_strategy='append'",
    "m": "materialized='incremental', incremental_strategy='merge', unique_key='id'",
    "io": "materialized='incremental', incremental_strategy='insert_overwrite'",
    "di": "materialized='incremental', incremental_strategy='delete+insert', unique_key='id'",
    "mb": "materialized='incremental', incremental_strategy='microbatch', event_time='ts', "
          "begin='2026-11-01', batch_size='day'",
}
ET = {"a": 2, "m": 3, "io": 4, "di": 5, "mb": 6, "sn": 7, "vw": 10, "cu": 11}
KINDS = ("a", "m", "io", "di", "mb", "sn")
ROLES = {
    "z": KINDS, "c": ("io", "di", "mb", "sn"), "cz": KINDS, "e": ("m", "io", "di", "mb", "sn"),
    "ez": KINDS, "q": ("a", "m", "io", "di", "mb"), "d": KINDS + ("vw", "cu"),
}
PAIRS = ("vc", "vc0", "cv", "cv0")  # view first / custom first; 0 = no upstream


def n(kind, role):
    return f"{kind}_{role}"


NODES = [n(k, r) for r, ks in ROLES.items() for k in ks]
NAMES = ("src", "srcmb", "mat", "uq", *NODES, *PAIRS)

MATERIALIZATION = """{% materialization ${mat}, default %}
  {%- set existing = adapter.get_relation(database=this.database, schema=this.schema,
                                          identifier=this.identifier) -%}
  {%- if existing is not none and existing.is_view -%}
    {% do adapter.drop_relation(existing) %}
  {%- endif -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {{ run_hooks(pre_hooks) }}
  {% call statement('main') -%}
    create or replace table {{ target_relation }} as ({{ sql }})
  {%- endcall %}
  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""


def body(kind, role, tag):
    """The select of node `<kind>_<role>`; `tag` varies it for a logic change."""
    lit = f"'{kind}_{role}_{tag}_${{run}}' as r"
    if role in ("z", "cz", "ez"):
        ts = ", '2026-11-01 10:00:00'::timestamp_ntz as ts" if kind == "mb" else ""
        return f"select 1 as id, 'x' as v{ts}, {lit}"
    if role == "q":
        cols = "id, v, ts" if kind == "mb" else "id, v"
        return f"select {cols}, {lit} from {{{{ ref('${{uq}}') }}}}"
    if kind == "mb":
        return f"select id, store_id as v, ordered_at as ts, {lit} from {{{{ source('${{srcmb}}', 'raw_orders') }}}}"
    return f"select id, store_id as v, {lit} from {{{{ source('${{src}}', 'raw_orders') }}}}"


def write(kind, role, tag="0", as_kind=None):
    """Write node `<kind>_<role>` materialized as `as_kind` (default: its own kind)."""
    name = f"${{{n(kind, role)}}}"
    k = as_kind or kind
    sql = body(kind, role, tag)
    if k == "sn":
        return Write(f"snapshots/{name}.sql", f"""{{% snapshot {name} %}}
{{{{ config(target_schema=target.schema, unique_key='id', strategy='check', check_cols=['v']) }}}}
{sql}
{{% endsnapshot %}}
""")
    config = {"vw": "materialized='view'", "cu": "materialized='${mat}'"}.get(k) or INCR[k]
    return Write(f"models/marts/{name}.sql", f"{{{{ config({config}) }}}}\n{sql}\n")


def pair(name, as_kind, tag="0"):
    """A VIEW / DBT_CUSTOM pair node (`vc*`, `cv*`) materialized as `as_kind`."""
    lit = f"'{name}_{tag}_${{run}}' as r"
    sql = (f"select 1 as id, 'x' as v, {lit}" if name.endswith("0") else
           f"select id, store_id as v, {lit} from {{{{ source('${{src}}', 'raw_orders') }}}}")
    config = "materialized='view'" if as_kind == "vw" else "materialized='${mat}'"
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config({config}) }}}}\n{sql}\n")


def sel(*roles):
    return tuple(f"${{{n(k, r)}}}" for r in roles for k in ROLES[r])


def probe(node, intent, decision=None, et=None, history=True):
    return Explain(f"${{{node}}}", intent=intent, decision=decision, execution_type=et,
                   history=history, via="probe")


# uq after its logic change (round 3). The raw orders `id` is a uuid string.
UQ1 = Write("models/marts/${uq}.sql",
            "{{ config(event_time='ts') }}\n"
            "select id, store_id as v, ordered_at as ts, 'uq1_${run}' as r "
            "from {{ source('${src}', 'raw_orders') }}\n")

PAIR_SEL = tuple(f"${{{p}}}" for p in PAIRS)
FIRST = {"vc": "vw", "vc0": "vw", "cv": "cu", "cv0": "cu"}
OTHER = {"vw": "cu", "cu": "vw"}

steps = [
    Write("macros/${mat}.sql", MATERIALIZATION),
    Write("models/${src}.yml", """sources:
  - name: ${src}
    schema: raw
    tables:
      - name: raw_orders
  - name: ${srcmb}
    schema: raw
    tables:
      - name: raw_orders
        config:
          event_time: ordered_at
"""),
    Write("models/marts/${uq}.sql",
          "{{ config(event_time='ts') }}\n"
          "select id, store_id as v, ordered_at as ts, 'uq0_${run}' as r "
          "from {{ source('${src}', 'raw_orders') }}\n"),
    *[write(k, r) for r, ks in ROLES.items() for k in ks],
    *[pair(p, FIRST[p]) for p in PAIRS],

    # Round 1: first builds, confirmed.
    Build("${uq}", *sel("z", "c", "cz", "q", "d"), *PAIR_SEL, args=WINDOW),

    # Round 2: --full-refresh. The c / cz incrementals are recorded as FULL
    # over their R1 build; the e / ez nodes are first built this way. The
    # snapshots are submitted as FULL.
    Build(*sel("c", "cz", "e", "ez"), full_refresh=True, args=WINDOW),
    probe(n("sn", "c"), "snapshot under --full-refresh (FULL) over SNAPSHOT history, upstream",
          "EXECUTE", 1),
    probe(n("sn", "cz"), "snapshot under --full-refresh (FULL) over SNAPSHOT history, "
          "no upstream", "EXECUTE", 1),

    # Round 3: ordinary run. uq's logic changes; each pair node takes the
    # other materialization.
    UQ1,
    *[pair(p, OTHER[FIRST[p]]) for p in PAIRS],
    Build("${uq}", *sel("z", "c", "cz", "e", "ez", "q"), *PAIR_SEL, args=WINDOW),
    *[probe(n(k, "z"), "unchanged rerun, no upstream", "SKIP", ET[k]) for k in ROLES["z"]],
    *[probe(n(k, "c"), "ordinary run over its R1 build and a later FULL full refresh, upstream",
            "EXECUTE", ET[k]) for k in ROLES["c"]],
    *[probe(n(k, "cz"), "ordinary run over its R1 build and a later FULL full refresh, "
            "no upstream", "EXECUTE", ET[k]) for k in ROLES["cz"]],
    # An incremental's full refresh is a RecordExecutions row, not a
    # confirmed submit, so the oracle cannot see its build (history=False).
    *[probe(n(k, "e"), "first ordinary run over only a FULL full-refresh build, upstream",
            "EXECUTE", ET[k], history=k == "sn") for k in ROLES["e"]],
    *[probe(n(k, "ez"), "first ordinary run over only a FULL full-refresh build, no upstream",
            "EXECUTE", ET[k], history=k == "sn") for k in ROLES["ez"]],
    *[probe(n(k, "q"), "upstream uq's logic changed", "EXECUTE", ET[k]) for k in ROLES["q"]],
    *[probe(p, f"{OTHER[FIRST[p]]} over {FIRST[p]} history, same SQL", "EXECUTE",
            ET[OTHER[FIRST[p]]]) for p in PAIRS],

    # Round 4: the d nodes' Target relations are dropped and their logic
    # changes; the z nodes' logic changes; the pairs return to their first
    # materialization.
    Sql(*[f"drop {'view' if k == 'vw' else 'table'} {DEMO}.${{{n(k, 'd')}}}"
          for k in ROLES["d"]]),
    *[write(k, "d", "1") for k in ROLES["d"]],
    *[write(k, "z", "1") for k in ROLES["z"]],
    *[pair(p, FIRST[p]) for p in PAIRS],
    Build(*sel("z", "d"), *PAIR_SEL, args=WINDOW),
    Explain("${di_d}", intent="delete+insert: own logic changed, Target relation dropped "
            "(dbt state explain)", decision="EXECUTE", execution_type=5),
    *[probe(n(k, "z"), "own logic changed, no upstream", "EXECUTE", ET[k]) for k in ROLES["z"]],
    *[probe(n(k, "d"), "own logic changed, Target relation dropped", "EXECUTE", ET[k])
      for k in ROLES["d"]],
    *[probe(p, f"{FIRST[p]} over {OTHER[FIRST[p]]} and {FIRST[p]} history, same SQL", "EXECUTE",
            ET[FIRST[p]]) for p in PAIRS],
]

SCENARIO = Scenario(
    name="reason_kind_cells",
    summary="#104b: no-upstream reuse and logic change, own Target relation modified, "
            "execution type change, upstream query change and logic change over a dropped "
            "Target relation, for every model kind but FULL",
    names=NAMES,
    steps=steps,
)
