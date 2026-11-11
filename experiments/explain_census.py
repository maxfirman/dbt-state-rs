#!/usr/bin/env python3
"""Census of the markers and badges in captured `dbt state explain` trees (#70).

Reads every tracked fixture (`golden/fixtures/*.jsonl`) plus any extra capture
files or directories given as arguments (directories are searched recursively
for `*.jsonl`). Each Explain message is assigned a node kind through the submit
that minted its `execution_decision_id`: SubmitEnrichedSQL and RegisterClone
by their request `execution_type`, SubmitValues as VALUES (seed). Ids are
looked up across all inputs, so an Explain call recorded in a later proxy file
still maps. The decision class is the message's `decision`.

The origin of a tree is that of its minting submit: `crafted` for a
`state-probe` row (no `x-system-user-id` metadata), otherwise the real client
with the `dbt_run_cache_version` of the file's latest ValidateClientVersion.

For each node kind × decision class it reports the number of trees by origin,
how many are empty, and the count of each marker and badge over every line of every
tree. A line with no marker or badge counts as `none`; only an explicit `0`
counts as EM_UNSPECIFIED / EB_UNSPECIFIED. It names the fixture and entry
(0-based line index, as in `golden/replay_gaps.json`) of every unspecified or
INFO value, and of every tree whose decision id no input minted.

Usage: explain_census.py [--no-fixtures] [CAPTURE_FILE_OR_DIR ...]
Self-test: python3 experiments/explain_census_test.py
"""
import glob
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass, field

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from decision_rows import DECISION, KIND, minted  # noqa: E402
FIXTURES = os.path.join(REPO, "golden", "fixtures")

NONE = "none"
UNMAPPED = "UNMAPPED"
CRAFTED = "crafted"

# explain_service.proto
MARKER = {0: "EM_UNSPECIFIED", 1: "SUCCESS", 2: "FAIL", 3: "INFO"}
BADGE = {
    0: "EB_UNSPECIFIED", 1: "FRESH", 2: "WITHIN_TOLERANCE", 3: "OUTDATED",
    4: "NODE_QUERY_CHANGED", 5: "NODE_QUERY_UNCHANGED", 6: "UPSTREAM_QUERY_CHANGED",
    7: "UPSTREAM_QUERY_UNCHANGED", 8: "TARGET_TABLE_EXISTS",
}
FLAGGED = {"EM_UNSPECIFIED", "INFO", "EB_UNSPECIFIED"}


@dataclass
class Cell:
    trees: int = 0
    empty: int = 0
    origins: Counter = field(default_factory=Counter)
    markers: Counter = field(default_factory=Counter)
    badges: Counter = field(default_factory=Counter)


@dataclass
class Finding:
    fixture: str
    entry: int
    edid: str
    kind: str
    decision: str
    field: str
    value: str
    text: str


@dataclass
class Census:
    files: int = 0
    trees: int = 0
    cells: dict = field(default_factory=dict)
    findings: list = field(default_factory=list)


def input_paths(extra, include_fixtures=True):
    """Tracked fixtures (unless excluded) plus each extra file or directory."""
    paths = sorted(glob.glob(os.path.join(FIXTURES, "*.jsonl"))) if include_fixtures else []
    for arg in extra:
        if os.path.isdir(arg):
            paths += sorted(glob.glob(os.path.join(arg, "**", "*.jsonl"), recursive=True))
        else:
            paths.append(arg)
    seen, out = set(), []
    for p in paths:
        key = os.path.realpath(p)
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def _label(path):
    """Tracked fixtures by file name, extra captures by the path given."""
    if os.path.dirname(os.path.realpath(path)) == os.path.realpath(FIXTURES):
        return os.path.basename(path)
    return path


def _load(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def _name(value, table):
    if value is None:
        return NONE
    return table.get(value, f"?{value}")


def _origin(row, version):
    """Real-client rows carry `x-system-user-id`; `state-probe` rows do not."""
    if not any(k == "x-system-user-id" for k, _ in row.get("metadata") or []):
        return CRAFTED
    return f"client {version}" if version else "client (version unrecorded)"


def _walk(lines):
    for ln in lines or []:
        yield ln
        yield from _walk(ln.get("children"))


def census(paths):
    loaded = [(_label(p), _load(p)) for p in paths]
    kind_of, origin_of = {}, {}
    for _, rows in loaded:
        version = None
        for row in rows:
            if row.get("method") == "ValidateClientVersion":
                version = (row.get("request") or {}).get("dbt_run_cache_version") or version
            edid, et = minted(row)
            if edid:
                kind_of[edid] = _name(et, KIND)
                origin_of[edid] = _origin(row, version)

    c = Census(files=len(loaded))
    for label, rows in loaded:
        for entry, row in enumerate(rows):
            if row.get("method") != "GetExplainMessages":
                continue
            for msg in (row.get("response") or {}).get("messages") or []:
                edid = msg.get("execution_decision_id")
                kind = kind_of.get(edid, UNMAPPED)
                decision = _name(msg.get("decision"), DECISION)
                cell = c.cells.setdefault((kind, decision), Cell())
                cell.trees += 1
                cell.origins[origin_of.get(edid, UNMAPPED)] += 1
                c.trees += 1
                lines = msg.get("explain_lines") or []
                if not lines:
                    cell.empty += 1
                if kind == UNMAPPED:
                    c.findings.append(Finding(label, entry, edid, kind, decision, "node_kind", UNMAPPED, ""))
                for ln in _walk(lines):
                    for fld, table, counter in (("marker", MARKER, cell.markers), ("badge", BADGE, cell.badges)):
                        name = _name(ln.get(fld), table)
                        counter[name] += 1
                        if name in FLAGGED:
                            c.findings.append(
                                Finding(label, entry, edid, kind, decision, fld, name, ln.get("text", "")))
    return c


def _counts(counter):
    return ", ".join(f"{k}={v}" for k, v in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))) or "-"


def render(c):
    out = [f"# Explain tree census: {c.trees} trees in {c.files} files", ""]
    order = {name: i for i, name in enumerate(list(KIND.values()) + [UNMAPPED])}
    dorder = {name: i for i, name in enumerate(DECISION.values())}
    out.append("## Node kind x decision")
    for (kind, decision), cell in sorted(
            c.cells.items(), key=lambda kv: (order.get(kv[0][0], 99), dorder.get(kv[0][1], 99), kv[0])):
        out.append(f"- {kind} {decision}: trees={cell.trees} empty={cell.empty}")
        out.append(f"    origin:  {_counts(cell.origins)}")
        out.append(f"    markers: {_counts(cell.markers)}")
        out.append(f"    badges:  {_counts(cell.badges)}")
    out.append("")
    markers, badges = Counter(), Counter()
    for cell in c.cells.values():
        markers.update(cell.markers)
        badges.update(cell.badges)
    out.append("## Vocabulary totals (line counts; 0 = not observed)")
    for name in [NONE] + list(MARKER.values()):
        out.append(f"- marker {name}: {markers.get(name, 0)}")
    for name in [NONE] + list(BADGE.values()):
        out.append(f"- badge {name}: {badges.get(name, 0)}")
    out.append("")
    out.append("## Findings (unspecified, INFO, unmapped)")
    if not c.findings:
        out.append("- none")
    for f in c.findings:
        out.append(f"- {f.fixture} entry {f.entry}: {f.field}={f.value} "
                   f"edid={f.edid} {f.kind} {f.decision} {f.text!r}")
    return "\n".join(out)


def main(argv):
    include = "--no-fixtures" not in argv
    extra = [a for a in argv if a != "--no-fixtures"]
    print(render(census(input_paths(extra, include_fixtures=include))))


if __name__ == "__main__":
    main(sys.argv[1:])
