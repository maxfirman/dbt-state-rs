#!/usr/bin/env python3
"""Pretty-print lag_matrix.py JSON rows from stdin, with explain trees."""
import json
import sys


def walk(lines, depth):
    for line in lines:
        tags = ""
        if line.get("badge"):
            tags += f" [badge {line['badge']}]"
        if line.get("marker"):
            tags += f" [marker {line['marker']}]"
        print("    " + "  " * depth + "- " + line["text"] + tags)
        walk(line["children"], depth + 1)


for raw in sys.stdin:
    try:
        row = json.loads(raw)
    except ValueError:
        print(raw, end="")
        continue
    explain = row.pop("explain", None)
    print(json.dumps(row))
    for message in (explain or {}).get("messages", []):
        walk(message["explain_lines"], 0)
