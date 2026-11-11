"""#73 (73c) / #97: submit-time Result copy of every incremental strategy over an
existing dev relation whose own build has other logic.

Why this shape. Every incremental submit copy hosted has granted had the
Target relation listed in `tables`, i.e. present (client 2.0.6:
`clone_failed_fallback.jsonl` entry 2, `clone_submit_equivalent_fallback.jsonl`
entry 1). The one 2.0.11 probe into an absent dev relation
(`explain_incremental_submit_copy.jsonl` entries 9-10) was built with codes
1 / 6: hosted found the equivalent (TARGET_TABLE_MISMATCH) but no suitable
copy (NO_SUITABLE_MATCH_FOUND), where it copies an absent FULL target
(`explain_custom_copy.jsonl` entry 8). An absent incremental is the client's
dev-clone pre-step's job (`run_cache_dev_clone.rs`, IfTableMissing), whose
RegisterClone mints no execution_decision_id. So this run makes the dev
relation exist first, with an own build hosted cannot reuse (other logic),
then submits the prod logic again.

`state.pre_clone: never` on every model keeps the pre-step out of both dev
builds (the first dev build's relation is absent). The upstream is the
jaffle-shop raw orders table (`CDO_DEMO_DB.RAW`, read only), outside both
targets' default schemas, so the prod and dev submits carry byte-identical
SQL and the same schema-abstracted logic (unlike a prod-schema source, #93).
Every build uses one fixed microbatch window, so the microbatch identity is
the same in both targets.

One attempt for all five strategies (spec #40: one attempt per negative copy
probe).

Nodes (per-run):
  r73   source name for raw.raw_orders (event_time ordered_at)
  ka73  append            (2)
  km73  merge             (3), unique_key id
  ki73  insert_overwrite  (4)
  kd73  delete+insert     (5), unique_key id
  kb73  microbatch        (6), event_time ordered_at, day batches
"""
from explain_capture import Build, Explain, Scenario, Write

WINDOW = ("--event-time-start", "2026-11-01", "--event-time-end", "2026-11-03")
NEVER = "state={'pre_clone': 'never'}"
MODELS = (
    ("ka73", 2, "incremental_strategy='append'"),
    ("km73", 3, "incremental_strategy='merge', unique_key='id'"),
    ("ki73", 4, "incremental_strategy='insert_overwrite'"),
    ("kd73", 5, "incremental_strategy='delete+insert', unique_key='id'"),
    ("kb73", 6, "incremental_strategy='microbatch', event_time='ordered_at', "
                "begin='2026-11-01', batch_size='day'"),
)
NODES = tuple(f"${{{n}}}" for n, _, _ in MODELS)
PROD_SQL = "select id, customer, ordered_at from {{ source('${r73}', 'raw_orders') }}\n"
DEV_SQL = PROD_SQL.rstrip("\n") + " where id is not null\n"


def models(select):
    return [Write(f"models/marts/${{{n}}}.sql",
                  f"{{{{ config(materialized='incremental', {cfg}, {NEVER}) }}}}\n{select}")
            for n, _, cfg in MODELS]


SCENARIO = Scenario(
    name="incremental_copy_over",
    summary="all five incremental strategies: submit-time Result copy from prod over an existing "
            "dev relation built with other logic (#73, #97)",
    names=("r73",) + tuple(n for n, _, _ in MODELS),
    steps=[
        Write("models/${r73}.yml", """sources:
  - name: ${r73}
    schema: raw
    tables:
      - name: raw_orders
        config:
          event_time: ordered_at
"""),
        *models(PROD_SQL),
        # prod_demo: first builds, confirmed (the copy sources).
        Build(*NODES, args=WINDOW),

        # dev_clone: first builds with other logic, so each dev relation exists
        # with an own build hosted cannot reuse for the prod logic.
        *models(DEV_SQL),
        Build(*NODES, target="dev_clone", args=WINDOW),

        # dev_clone: back to the prod logic. Copy from prod over the dev relation?
        *models(PROD_SQL),
        Build(*NODES, target="dev_clone", args=WINDOW),
        *[Explain(f"${{{n}}}", intent=f"{cfg.split(',')[0]} back on the prod logic over an "
                  "existing dev relation (copy?)", execution_type=et, target="dev_clone")
          for n, et, cfg in MODELS],
    ],
)
