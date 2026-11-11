"""#73: INSERT_OVERWRITE (4) incremental Explain trees over State history.

First build, unchanged SKIP, source data drift, own logic change, and a model
that reads its own Target relation through {{ this }}. Nodes: see _incremental.py.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _incremental import names, steps  # noqa: E402
from explain_capture import Scenario  # noqa: E402

SCENARIO = Scenario(
    name="incremental_insert_overwrite",
    summary="insert_overwrite incremental: first build, SKIP, source drift, logic change, "
            "{{ this }} self-read (#73)",
    names=names("io"),
    steps=steps(
        "io", 4, "incremental_strategy='insert_overwrite'",
        st_select="select 1 as id, 'st_${run}' as v",
        src_rows="select 1 as id, 'src0' as v",
        drift_row="select 2 as id, 'src1' as v",
    ),
)
