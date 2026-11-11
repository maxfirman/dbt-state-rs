"""Shared step list for the per-strategy incremental Explain scenarios (#73).

Not a scenario itself (the driver skips modules starting with `_`).

Nodes (all per-run, in prod_state_demo), `<p>` being the strategy prefix:
  <p>src  source table created by this run (no Upstream producer)
  <p>st   static table model
  <p>d    incremental over the source  -> first build, unchanged SKIP, source data drift
  <p>l    incremental over ref(<p>st)  -> its own logic changes in round 3
  <p>s    incremental over ref(<p>st) that reads its own Target relation through
          {{ this }} when incremental -> first incremental run, then an unchanged rerun

`config` is the model config call's keyword arguments (strategy, unique_key, ...);
`build_args` extra `dbt build` flags for every round (the microbatch window);
`columns` the select list, `rows` the source rows and `drift_row` the row added in
round 3.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from explain_capture import Build, Explain, Sql, Write  # noqa: E402

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"


def names(p):
    return tuple(f"{p}{n}" for n in ("src", "st", "d", "l", "s"))


def incremental(config, select):
    return f"{{{{ config(materialized='incremental', {config}) }}}}\n{select}"


def steps(p, execution_type, config, *, columns="id, v", st_select, src_rows, drift_row,
          st_config="", source_config="", build_args=()):
    src, st, d, lg, s = (f"${{{n}}}" for n in names(p))
    nodes = (st, d, lg, s)
    self_read = (f"select {columns} from {{{{ ref('{st}') }}}}\n"
                 "{% if is_incremental() %}\n"
                 "where id >= (select coalesce(min(id), 0) from {{ this }})\n"
                 "{% endif %}\n")
    return [
        Write(f"models/{src}.yml", f"""sources:
  - name: {src}
    schema: prod_state_demo
    tables:
      - name: {src}
{source_config}"""),
        Write(f"models/marts/{st}.sql", f"{st_config}{st_select}\n"),
        Write(f"models/marts/{d}.sql",
              incremental(config, f"select {columns} from {{{{ source('{src}', '{src}') }}}}\n")),
        Write(f"models/marts/{lg}.sql",
              incremental(config, f"select {columns} from {{{{ ref('{st}') }}}}\n")),
        Write(f"models/marts/{s}.sql", incremental(config, self_read)),
        Sql(f"create or replace table {DEMO}.{src} as {src_rows}"),

        # Round 1: first builds, confirmed.
        Build(*nodes, args=build_args),
        Explain(d, intent="first build", decision="EXECUTE", execution_type=execution_type,
                history=False),

        # Round 2: unchanged rerun; the self-reading model's first incremental run.
        Build(*nodes, args=build_args),
        Explain(d, intent="unchanged rerun SKIP", decision="SKIP", execution_type=execution_type),
        Explain(s, intent="self-read: first incremental run (is_incremental() SQL)",
                execution_type=execution_type),

        # Round 3: the source gains a row; one model's logic changes; the
        # self-reading model reruns unchanged.
        Sql(f"insert into {DEMO}.{src} {drift_row}"),
        Write(f"models/marts/{lg}.sql",
              incremental(config, f"select {columns} from {{{{ ref('{st}') }}}} where id >= 1\n")),
        Build(*nodes, args=build_args),
        Explain(d, intent="upstream data drift (source gained a row)",
                execution_type=execution_type),
        Explain(lg, intent="its own logic changed", decision="EXECUTE",
                execution_type=execution_type),
        Explain(s, intent="self-read: unchanged incremental rerun", execution_type=execution_type),
    ]
