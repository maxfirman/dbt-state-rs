"""Shared steps of the #237 upstream-origin scenarios (spec #236, Experiment 1).

`steps("incremental")` is `upstream_origin.py` (run 1), `steps("table")` is
`upstream_origin_table.py` (run 2); each module says how its upstream is
copied or built. Every case x in a..d has a per-run input ox_in, upstream
ox_up and downstream ox_dn (oa_fix is the source name A reads prod's
upstream through). The downstream is always a table (FULL) with
`state.pre_clone: never` and `lag_tolerance: 0s`, built once on `prod_demo`
(the one FULL candidate) and then submitted on `dev_clone` (`allow_clones:
true`, `defer_to_target: prod_demo`) with its dev relation absent, so its
decision comes from an ordinary `SubmitEnrichedSQL`.

  A  same physical upstream: the downstream reads prod's upstream through a
     source fixed to prod_state_demo, so prod and dev submit byte-identical
     SQL. Dev builds only the downstream. Positive control for automatic copy.
  B  dev upstream is a confirmed copy of prod's. Its input is the shared
     prod table, so its SQL is prod's.
  C  dev upstream built independently over an input in the Target's own
     schema (`{{ target.schema }}`); the prod and dev inputs hold identical
     rows.
  D  as C, but the dev input holds v='dev' where prod's holds v='prod'. A dev
     downstream serving 'prod' while the Reference execution serves 'dev' is
     a wrong-marker correctness finding.

After the first dev round of each case: State-free reads of what dev serves
and a `state-probe` Explain of each decision. Then the next unchanged dev
build of every case, explained and read again. Last, a State-disabled
`--full-refresh` Reference execution of every dev node, read back.
`experiments/upstream_origin.py WORK_DIR` prints the per-case record.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from explain_capture import Build, Explain, Read, Reference, Sql, Write  # noqa: E402

PROD = "CDO_DEMO_DB.PROD_STATE_DEMO"
DEV = "CDO_DEMO_DB.DEV_CLONE_DEMO"
CASES = ("a", "b", "c", "d")
NAMES = tuple(f"o{c}_{k}" for c in CASES for k in ("in", "up", "dn")) + ("oa_fix",)
ZERO = "'lag_tolerance': '0s'"
# `dev_clone` with `allow_clones: false`: the table variant builds C and D's
# dev upstream there, so hosted cannot copy it from prod.
NO_COPY = "dev_noclone"
UP = {
    "incremental": "materialized='incremental', incremental_strategy='merge', unique_key='id', ",
    "table": "materialized='table', ",
}
DN = ("{{{{ config(materialized='table', state={{'pre_clone': 'never', " + ZERO + "}}) }}}}\n"
      "select id, v from {from_}\n")

SOURCES = """sources:
  - name: ${oa_in}
    schema: prod_state_demo
    tables:
      - name: ${oa_in}
  - name: ${oa_fix}
    schema: prod_state_demo
    tables:
      - name: ${oa_up}
  - name: ${ob_in}
    schema: prod_state_demo
    tables:
      - name: ${ob_in}
  - name: ${oc_in}
    schema: "{{ target.schema }}"
    tables:
      - name: ${oc_in}
  - name: ${od_in}
    schema: "{{ target.schema }}"
    tables:
      - name: ${od_in}
"""


def _up(kind, c):
    never = kind == "incremental" and c in ("c", "d")
    state = ("'pre_clone': 'never', " + ZERO) if never else ZERO
    return Write(f"models/marts/${{o{c}_up}}.sql",
                 f"{{{{ config({UP[kind]}state={{{state}}}) }}}}\n"
                 f"select id, v from {{{{ source('${{o{c}_in}}', '${{o{c}_in}}') }}}}\n")


def _dn(c):
    from_ = ("{{ source('${oa_fix}', '${oa_up}') }}" if c == "a"
             else f"{{{{ ref('${{o{c}_up}}') }}}}")
    return Write(f"models/marts/${{o{c}_dn}}.sql", DN.format(from_=from_))


def dev_nodes(c):
    """Logical names of what dev builds in case c: A only the downstream (it reads prod's upstream)."""
    return (f"o{c}_dn",) if c == "a" else (f"o{c}_up", f"o{c}_dn")


def _dev_round(kind, c, label, first):
    """One dev round of case c: its build(s), its decisions explained, and what dev serves."""
    nodes = dev_nodes(c)
    steps = []
    if kind == "table" and first and c in ("c", "d"):
        steps.append(Build(f"${{o{c}_up}}", target=NO_COPY))
        steps.append(Explain(f"${{o{c}_up}}", intent=f"{c.upper()} {label}: o{c}_up without copy "
                             "permission", target=NO_COPY, via="probe"))
    steps.append(Build(*[f"${{{n}}}" for n in nodes], target="dev_clone"))
    steps += [Explain(f"${{{n}}}", intent=f"{c.upper()} {label}: {n}", target="dev_clone",
                      via="probe") for n in nodes]
    steps += [Read(f"{DEV}.${{{n}}}", target="dev_clone") for n in nodes]
    return steps


def steps(kind):
    if kind not in UP:
        raise ValueError(f"upstream kind {kind!r} is not one of {sorted(UP)}")
    return [
        Write("models/upstream_origin_${run}.yml", SOURCES),
        *[_up(kind, c) for c in CASES],
        *[_dn(c) for c in CASES],
        Sql(f"create or replace table {PROD}.${{oa_in}} as select 1 as id, 'prod' as v",
            f"create or replace table {PROD}.${{ob_in}} as select 1 as id, 'prod' as v",
            f"create or replace table {PROD}.${{oc_in}} as select 1 as id, 'same' as v",
            f"create or replace table {DEV}.${{oc_in}} as select 1 as id, 'same' as v",
            f"create or replace table {PROD}.${{od_in}} as select 1 as id, 'prod' as v",
            f"create or replace table {DEV}.${{od_in}} as select 1 as id, 'dev' as v"),
        # prod_demo: the upstreams, then the downstreams (A's reads its upstream
        # through a source, so it has no DAG edge and needs its own invocation).
        Build(*[f"${{o{c}_up}}" for c in CASES]),
        Build(*[f"${{o{c}_dn}}" for c in CASES]),
        *[Read(f"{PROD}.${{o{c}_dn}}") for c in CASES],
        # dev_clone: first dev round of each case, one invocation per case.
        *[s for c in CASES for s in _dev_round(kind, c, "first dev build", True)],
        # The next unchanged submit of each case.
        *[s for c in CASES for s in _dev_round(kind, c, "next unchanged dev build", False)],
        # The State-disabled Reference execution of every dev node, read back.
        Reference(*[f"${{{n}}}" for c in CASES for n in dev_nodes(c)], target="dev_clone"),
        *[Read(f"{DEV}.${{{n}}}", target="dev_clone") for c in CASES for n in dev_nodes(c)],
    ]


def reference_steps(kind):
    """A State-free follow-up of an earlier run (`--run-id`): its project files, the inputs
    read back, then the Reference execution of every dev node with `--no-manage-state`, read
    back. No step contacts dbt State, and the driver fails the run if the proxy records an RPC.
    """
    if kind not in UP:
        raise ValueError(f"upstream kind {kind!r} is not one of {sorted(UP)}")
    inputs = [f"{PROD}.${{o{c}_in}}" for c in CASES] + [f"{DEV}.${{o{c}_in}}" for c in ("c", "d")]
    return [
        Write("models/upstream_origin_${run}.yml", SOURCES),
        *[_up(kind, c) for c in CASES],
        *[_dn(c) for c in CASES],
        *[Read(rel, target="prod_demo" if rel.startswith(PROD) else "dev_clone") for rel in inputs],
        Reference(*[f"${{{n}}}" for c in CASES for n in dev_nodes(c)], target="dev_clone"),
        *[Read(f"{DEV}.${{{n}}}", target="dev_clone") for c in CASES for n in dev_nodes(c)],
    ]
