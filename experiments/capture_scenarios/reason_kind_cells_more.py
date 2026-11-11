"""#104 (group b): follow-up to `reason_kind_cells`, run with that run's
`--run-id` and `--prior-window <its window.jsonl>`, so it extends the same
nodes and their State history.

That run stopped in round 3 (uq failed and dbt skipped the q nodes) before
its round 3 explains. This one:
  1. explains the round 3 decisions (state-probe, over the earlier window);
  2. rebuilds uq after its logic change and the q nodes over it
     (upstream_query_changed);
  3. round 4 as planned (z logic change, d logic change over a dropped
     Target relation, the pairs back to their first materialization), plus a
     plain External modification: the e / ez nodes and `vc` (now a custom
     node with an upstream), all built by round 3, get a row inserted, then
     rerun unchanged (own_target_modified against that build).

Round 3 showed that hosted SKIPs a snapshot over its own build, a later
`--full-refresh` (FULL) build and an unchanged Target relation (`sn_c`,
`sn_cz`), so the snapshot's own_target_modified needs the External
modification path (`sn_e`, `sn_ez`).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from explain_capture import Build, Explain, Scenario, Sql  # noqa: E402
from reason_kind_cells import (  # noqa: E402
    DEMO, ET, FIRST, NAMES, OTHER, PAIRS, ROLES, UQ1, WINDOW, n, pair, probe, sel, write, steps,
)

# The project as round 3 left it: the macro, the sources, uq after its
# change, every node at tag 0 and the pairs in their second materialization.
SETUP = [s for s in steps[:3] if "uq" not in getattr(s, "path", "")] + [
    UQ1,
    *[write(k, r) for r, ks in ROLES.items() for k in ks],
    *[pair(p, OTHER[FIRST[p]]) for p in PAIRS],
]
EXTERNAL = [n(k, "e") for k in ROLES["e"]] + [n(k, "ez") for k in ROLES["ez"]] + ["vc"]
# R4 materializations: `vc` stays a custom node (External modification), the
# others return to their first materialization.
R4_KIND = {"vc": "cu", "vc0": "vw", "cv": "cu", "cv0": "cu"}

SCENARIO = Scenario(
    name="reason_kind_cells_more",
    summary="follow-up (--run-id and --prior-window of reason_kind_cells): its round 3 explains, "
            "upstream query change, round 4 and External modifications (#104b)",
    names=NAMES,
    steps=[
        *SETUP,
        # 1. The earlier run's round 3 decisions.
        *[probe(n(k, "z"), "unchanged rerun, no upstream", "SKIP", ET[k]) for k in ROLES["z"]],
        *[probe(n(k, "c"), "ordinary run over its R1 build and a later FULL full refresh, "
                "upstream", None, ET[k]) for k in ROLES["c"]],
        *[probe(n(k, "cz"), "ordinary run over its R1 build and a later FULL full refresh, "
                "no upstream", None, ET[k]) for k in ROLES["cz"]],
        # An incremental's full refresh is a RecordExecutions row, not a
        # confirmed submit, so the oracle cannot see its build (history=False).
        *[probe(n(k, "e"), "first ordinary run over only a FULL full-refresh build, upstream",
                "EXECUTE", ET[k], history=k == "sn") for k in ROLES["e"]],
        *[probe(n(k, "ez"), "first ordinary run over only a FULL full-refresh build, "
                "no upstream", "EXECUTE", ET[k], history=k == "sn") for k in ROLES["ez"]],
        *[probe(p, f"{OTHER[FIRST[p]]} over {FIRST[p]} history, same SQL", "EXECUTE",
                ET[OTHER[FIRST[p]]]) for p in PAIRS],

        # 2. uq's logic changed; the q nodes rebuild over it.
        Build("${uq}", *sel("q"), args=WINDOW),
        *[probe(n(k, "q"), "upstream uq's logic changed", "EXECUTE", ET[k]) for k in ROLES["q"]],

        # 3. Round 4.
        Sql(*[f"drop {'view' if k == 'vw' else 'table'} {DEMO}.${{{n(k, 'd')}}}"
              for k in ROLES["d"]],
            *[f"insert into {DEMO}.${{{x}}} (r) values ('external')" for x in EXTERNAL]),
        *[write(k, "d", "1") for k in ROLES["d"]],
        *[write(k, "z", "1") for k in ROLES["z"]],
        *[pair(p, R4_KIND[p]) for p in PAIRS],
        Build(*sel("z", "d", "e", "ez"), *(f"${{{p}}}" for p in PAIRS), args=WINDOW),
        Explain("${di_d}", intent="delete+insert: own logic changed, Target relation dropped "
                "(dbt state explain)", decision="EXECUTE", execution_type=5),
        *[probe(n(k, "z"), "own logic changed, no upstream", "EXECUTE", ET[k])
          for k in ROLES["z"]],
        *[probe(n(k, "d"), "own logic changed, Target relation dropped", "EXECUTE", ET[k])
          for k in ROLES["d"]],
        *[probe(x, "unchanged rerun after an External modification of its Target relation",
                "EXECUTE", ET[x.split("_")[0]] if "_" in x else ET["cu"]) for x in EXTERNAL],
        *[probe(p, f"{R4_KIND[p]} over {OTHER[FIRST[p]]} and {FIRST[p]} history, same SQL",
                None, ET[R4_KIND[p]]) for p in PAIRS if p != "vc"],
    ],
)
