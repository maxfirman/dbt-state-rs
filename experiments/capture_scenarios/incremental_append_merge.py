"""#73 (73a): APPEND (2) and MERGE (3) incremental Explain trees over State
history, the unset-strategy finding and the `--full-refresh` finding.

Nodes (all per-run, in prod_state_demo):
  i73s   source table created by this run, never changed
  i73d   source table created by this run that gains a row in round 3
  a73b / m73b  append / merge over i73s: first build, SKIP, own logic change,
               then a `--full-refresh` run and the decision after it
  a73d / m73d  append / merge over i73d: upstream data drift, default Lag tolerance
  a73z / m73z  append / merge over i73d: upstream data drift, lag_tolerance 0s
  a73t / m73t  append / merge over i73s that read their own Target relation
               through `{{ this }}` once incremental
  u73          incremental with a unique_key and no strategy (unset strategy)
"""
from explain_capture import Build, Explain, Scenario, Sql, Write

DEMO = "CDO_DEMO_DB.PROD_STATE_DEMO"
APPEND = "materialized='incremental', incremental_strategy='append'"
MERGE = "materialized='incremental', incremental_strategy='merge', unique_key='id'"
ZERO = ", state={'lag_tolerance': '0s'}"
ALL = ("${a73b}", "${a73d}", "${a73z}", "${a73t}",
       "${m73b}", "${m73d}", "${m73z}", "${m73t}", "${u73}")
SELF_READ = """
{% if is_incremental() %}where id > (select coalesce(max(id), 0) from {{ this }}){% endif %}
"""


def model(name, config, body):
    return Write(f"models/marts/${{{name}}}.sql", f"{{{{ config({config}) }}}}\n{body}\n")


def src(table):
    return f"select id, v from {{{{ source('${{i73s}}', '${{{table}}}') }}}}"


SCENARIO = Scenario(
    name="incremental_append_merge",
    summary="append and merge incremental first build, SKIP, logic change, upstream drift, "
            "{{ this }} self-read, unset strategy, --full-refresh (#73)",
    names=("i73s", "i73d", "a73b", "a73d", "a73z", "a73t",
           "m73b", "m73d", "m73z", "m73t", "u73"),
    steps=[
        Write("models/${i73s}.yml", """sources:
  - name: ${i73s}
    schema: prod_state_demo
    tables:
      - name: ${i73s}
      - name: ${i73d}
"""),
        model("a73b", APPEND, src("i73s")),
        model("m73b", MERGE, src("i73s")),
        model("a73d", APPEND, src("i73d")),
        model("m73d", MERGE, src("i73d")),
        model("a73z", APPEND + ZERO, src("i73d")),
        model("m73z", MERGE + ZERO, src("i73d")),
        model("a73t", APPEND, src("i73s") + SELF_READ),
        model("m73t", MERGE, src("i73s") + SELF_READ),
        model("u73", "materialized='incremental', unique_key='id'", src("i73s")),
        Sql(f"create or replace table {DEMO}.${{i73s}} as select 1 as id, 's0' as v",
            f"create or replace table {DEMO}.${{i73d}} as select 1 as id, 'd0' as v"),

        # Round 1: first builds, confirmed.
        Build(*ALL),
        Explain("${a73b}", intent="append first build", decision="EXECUTE",
                execution_type=2, history=False),
        Explain("${m73b}", intent="merge first build", decision="EXECUTE",
                execution_type=3, history=False),

        # Round 2: unchanged rerun. The self-read models now run incrementally,
        # so their compiled SQL gains the `{{ this }}` filter.
        Build(*ALL),
        Explain("${a73b}", intent="append unchanged SKIP", decision="SKIP", execution_type=2),
        Explain("${m73b}", intent="merge unchanged SKIP", decision="SKIP", execution_type=3),
        Explain("${a73t}", intent="append first incremental run reading {{ this }}",
                execution_type=2),
        Explain("${m73t}", intent="merge first incremental run reading {{ this }}",
                execution_type=3),
        Explain("${u73}", intent="unset strategy with unique_key: which execution type"),

        # Round 3: own logic change (a73b, m73b) and upstream data drift (i73d).
        Write("models/marts/${a73b}.sql",
              f"{{{{ config({APPEND}) }}}}\n{src('i73s')} where id >= 1\n"),
        Write("models/marts/${m73b}.sql",
              f"{{{{ config({MERGE}) }}}}\n{src('i73s')} where id >= 1\n"),
        Sql(f"insert into {DEMO}.${{i73d}} select 2 as id, 'd1' as v"),
        Build(*ALL),
        Explain("${a73b}", intent="append after its own logic changed", decision="EXECUTE",
                execution_type=2),
        Explain("${m73b}", intent="merge after its own logic changed", decision="EXECUTE",
                execution_type=3),
        Explain("${a73d}", intent="append after upstream data drift (default tolerance)",
                execution_type=2),
        Explain("${m73d}", intent="merge after upstream data drift (default tolerance)",
                execution_type=3),
        Explain("${a73z}", intent="append after upstream data drift (tolerance 0s)",
                execution_type=2),
        Explain("${m73z}", intent="merge after upstream data drift (tolerance 0s)",
                execution_type=3),
        Explain("${a73t}", intent="append second incremental run reading {{ this }}",
                execution_type=2),
        Explain("${m73t}", intent="merge second incremental run reading {{ this }}",
                execution_type=3),

        # Round 4: --full-refresh (the client is expected to send no Submit),
        # then an ordinary run over the fully refreshed Target relations.
        Build("${a73b}", "${m73b}", full_refresh=True),
        Build("${a73b}", "${m73b}"),
        Explain("${a73b}", intent="append run after a --full-refresh run", execution_type=2),
        Explain("${m73b}", intent="merge run after a --full-refresh run", execution_type=3),
    ],
)
