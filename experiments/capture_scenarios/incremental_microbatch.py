"""#73: MICROBATCH (6) incremental Explain trees over State history.

Rounds 1-3 build with a fixed `--event-time-start/--event-time-end` window, so an
unchanged rerun can SKIP: first build, unchanged SKIP, source data drift inside
the window, own logic change, and a model that reads its own Target relation
through {{ this }} (nodes: see _incremental.py).

Then `mbm` builds three times with the default moving window. The client folds
the resolved window into the semantic extras (`__microbatch_event_time_start`,
`__microbatch_event_time_end`): the first build runs from `begin`, an
incremental run from `lookback` + 1 batches before the ceiling of now, so the
second build sees a different window and the third (same UTC day) the same one.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _incremental import incremental, names, steps  # noqa: E402
from explain_capture import Build, Explain, Scenario, Write  # noqa: E402

CONFIG = "incremental_strategy='microbatch', event_time='ts', begin='2026-11-01', batch_size='day'"
WINDOW = ("--event-time-start", "2026-11-01", "--event-time-end", "2026-11-03")

SCENARIO = Scenario(
    name="incremental_microbatch",
    summary="microbatch incremental: fixed window first build, SKIP, source drift, logic change, "
            "{{ this }} self-read; three moving-window builds (#73)",
    names=names("mb") + ("mbm",),
    steps=steps(
        "mb", 6, CONFIG,
        columns="id, v, ts",
        st_config="{{ config(event_time='ts') }}\n",
        st_select="select 1 as id, 'st_${run}' as v, '2026-11-01 10:00:00'::timestamp_ntz as ts",
        src_rows="select 1 as id, 'src0' as v, '2026-11-01 10:00:00'::timestamp_ntz as ts",
        drift_row="select 2 as id, 'src1' as v, '2026-11-02 10:00:00'::timestamp_ntz as ts",
        source_config="        config:\n          event_time: ts\n",
        build_args=WINDOW,
    ) + [
        # Default moving window.
        Write("models/marts/${mbm}.sql",
              incremental(CONFIG, "select id, v, ts from {{ ref('${mbst}') }}\n")),
        Build("${mbm}"),
        Build("${mbm}"),
        Explain("${mbm}", intent="moving window: first incremental run (window moved from begin)",
                execution_type=6),
        Build("${mbm}"),
        Explain("${mbm}", intent="moving window: rerun in the same batch period",
                execution_type=6),
    ],
)
