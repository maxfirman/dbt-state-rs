#!/usr/bin/env python3
"""Warehouse-free tests for the #239 copy source choice tools (`copy_source_choice.py`).

Covers the two parts a live run depends on:

- the crafted history plan: every history keeps one model unique id, output
  object name, catalog and connection namespace across distinct source
  schemas, the destination is absent, the completion epochs and confirmation
  orders are the ones the history names, every epoch is physically plausible
  at the moment it is sent, and the plan stays inside its submit budget;
- the capture table: which source a destination grant names (by relation,
  build epoch and runtime), or BUILD, and when a history is inconclusive.

No warehouse, no proxy, no dbt. Exit 0 on success.

Run: python3 experiments/copy_source_choice_test.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import copy_source_choice as csc  # noqa: E402

passed = 0


def ok(name):
    global passed
    passed += 1
    print(f"PASS: {name}")


def fail(name, why):
    print(f"FAIL: {name} — {why}")
    sys.exit(1)


def check(name, cond, why=""):
    if not cond:
        fail(name, why)
    ok(name)


RUN = "120000abcdef"
BASE = 1_800_000_000_000

# --- the plan ----------------------------------------------------------------------------

# The histories the issue asks for, plus the oldest+middle control that tells
# "the newest equivalent" from "the first after a freshness rejection", and
# `_late` controls that move the fresh source's completion after the others.
# name: (sources, confirmation order, completion order oldest first)
WANT = {
    "old_alone": (["old"], ["old"], ["old"]),
    "mid_alone": (["mid"], ["mid"], ["mid"]),
    "mid_current_alone": (["mid"], ["mid"], ["mid"]),
    "new_alone": (["new"], ["new"], ["new"]),
    "old_mid": (["old", "mid"], ["old", "mid"], ["old", "mid"]),
    "old_mid_rev": (["old", "mid"], ["mid", "old"], ["old", "mid"]),
    "old_mid_late": (["old", "mid"], ["mid", "old"], ["mid", "old"]),
    "old_new": (["old", "new"], ["old", "new"], ["old", "new"]),
    "old_new_rev": (["old", "new"], ["new", "old"], ["old", "new"]),
    "old_new_late": (["old", "new"], ["new", "old"], ["new", "old"]),
    "old_mid_new": (["old", "mid", "new"], ["old", "mid", "new"], ["old", "mid", "new"]),
    "old_mid_new_rev": (["old", "mid", "new"], ["new", "mid", "old"], ["old", "mid", "new"]),
    "old_mid_new_late": (["old", "mid", "new"], ["new", "mid", "old"], ["new", "mid", "old"]),
}
check("the plan names exactly the planned histories", sorted(csc.HISTORIES) == sorted(WANT),
      sorted(csc.HISTORIES))
for name, (sources, order, completion) in WANT.items():
    h = csc.HISTORIES[name]
    check(f"{name}: sources {sources} confirmed in the order {order}, completed in {completion}",
          sorted(h["sources"]) == sorted(sources) and h["confirm"] == order
          and h["completion"] == completion, h)

plans = {name: csc.plan_history(RUN, name, BASE) for name in csc.HISTORIES}


def steps(plan, op, role=None):
    return [s for s in plan["steps"] if s["op"] == op and (role is None or s["role"] == role)]


def submit_of(plan, role):
    (s,) = steps(plan, "submit", role)
    return s


def confirm_of(plan, role):
    (s,) = steps(plan, "confirm", role)
    return s


def epoch_of(req, name):
    (t,) = [t for t in req["tables"] if t["name"] == name]
    return t["last_modified_epoch"]


for name, plan in plans.items():
    roles = WANT[name][0]
    subs = {r: submit_of(plan, r)["request"] for r in roles + ["dest"]}
    targets = {r: req["target_table"] for r, req in subs.items()}
    check(f"{name}: each source and the destination is a distinct relation",
          len(set(targets.values())) == len(targets), targets)
    parts = {r: csc.split_fqn(t) for r, t in targets.items()}
    check(f"{name}: one catalog and one output object name across schemas",
          len({(p[0], p[2]) for p in parts.values()}) == 1, parts)
    check(f"{name}: one model unique id, catalog and connection namespace",
          len({(q["dbt_node_state"]["node_unique_id"], q["labels"]["dbt_node_unique_id"],
                q["default_catalog"], q["table_namespace"]) for q in subs.values()}) == 1,
          {r: q["dbt_node_state"]["node_unique_id"] for r, q in subs.items()})
    check(f"{name}: each submit's default schema is its Target relation's schema",
          all(q["default_schema"].upper() == parts[r][1] for r, q in subs.items()),
          {r: q["default_schema"] for r, q in subs.items()})
    dest = subs["dest"]
    check(f"{name}: the destination is absent (its Target relation is not reported)",
          all(t["name"] != dest["target_table"] for t in dest["tables"]), dest["tables"])
    for r, q in subs.items():
        check(f"{name}/{r}: FULL, tolerance 0, ANY, copies allowed, no chain or time-travel limit",
              q["execution_type"] == 1 and q["freshness_tolerance_seconds"] == 0
              and q["stale_upstream_policy"] == 0 and q["allow_clones"] is True
              and q["defer_enabled"] is True and q["clone_chain_depth_limit"] is None
              and q["clone_time_travel_limit"] is None
              and q["ignore_external_modifications"] is False
              and q["compare_unrendered_code"] is False, q)
    abstracted = {csc.abstract_schemas(q["sql"]) for q in subs.values()}
    check(f"{name}: every submit has one logic once schemas are abstracted",
          len(abstracted) == 1, abstracted)
    up = csc.upstream(RUN, name)
    check(f"{name}: the destination reads the shared upstream",
          [t["name"] for t in dest["tables"]] == [up], dest["tables"])
    if "old" in roles:
        check(f"{name}: the oldest source read the upstream at the destination's epoch",
              epoch_of(subs["old"], up) == epoch_of(dest, up), subs["old"]["tables"])
    if "new" in roles:
        check(f"{name}: the newer source read the upstream before it changed",
              epoch_of(subs["new"], up) < epoch_of(dest, up), subs["new"]["tables"])
    if "mid" in roles:
        alt = csc.upstream(RUN, name, alternate=True)
        check(f"{name}: the middle source reads another relation of the upstream's name",
              [t["name"] for t in subs["mid"]["tables"]] == [alt] and alt != up
              and csc.split_fqn(alt)[2] == csc.split_fqn(up)[2], subs["mid"]["tables"])
        same = epoch_of(subs["mid"], alt) == epoch_of(dest, up)
        check(f"{name}: the middle source reads its relation at the destination's epoch only in "
              "mid_current_alone", same == (name == "mid_current_alone"), subs["mid"]["tables"])

    # Physical plausibility and the two orders.
    for s in plan["steps"]:
        for t in (s.get("request") or {}).get("tables") or []:
            check(f"{name}: {s['op']} {s['role']} reports no future epoch",
                  t["last_modified_epoch"] <= s["at"], (s["at"], t))
    completion = {r: confirm_of(plan, r)["request"]["last_modified_epoch"] for r in roles}
    check(f"{name}: completion epochs follow {WANT[name][2]}",
          sorted(completion, key=completion.get) == WANT[name][2]
          and len(set(completion.values())) == len(roles), completion)
    for r in roles:
        sub_at, conf = submit_of(plan, r)["at"], confirm_of(plan, r)
        check(f"{name}/{r}: completed after its submit and confirmed after completing",
              sub_at < conf["request"]["last_modified_epoch"] < conf["at"], (sub_at, conf["at"]))
    confirms = steps(plan, "confirm")
    check(f"{name}: confirmed in the planned order",
          [c["role"] for c in sorted(confirms, key=lambda c: c["at"])] == WANT[name][1],
          [c["role"] for c in confirms])
    check(f"{name}: every source is submitted before any is confirmed",
          max(s["at"] for s in steps(plan, "submit") if s["role"] != "dest")
          < min(c["at"] for c in confirms))
    check(f"{name}: the destination is submitted after the last confirm, then explained",
          submit_of(plan, "dest")["at"] > max(c["at"] for c in confirms)
          and [s["op"] for s in plan["steps"]][-2:] == ["submit", "explain"])
    ats = [s["at"] for s in plan["steps"]]
    check(f"{name}: steps are in time order, at least {csc.GAP_MS} ms apart",
          all(b - a >= csc.GAP_MS for a, b in zip(ats, ats[1:])), ats)
    check(f"{name}: the newer source is submitted first (it read the upstream before it changed)",
          "new" not in roles or steps(plan, "submit")[0]["role"] == "new")
    for c in confirms:
        q = c["request"]
        check(f"{name}/{c['role']}: the confirm is a successful model execution",
              q["failed_to_clone"] is False and q["table_type"] == "TRANSIENT TABLE"
              and q["execution_results"] is None and q["labels"] == subs[c["role"]]["labels"], q)

def rel_completion(p):
    return {s["role"]: s["request"]["last_modified_epoch"] - p["base"] for s in steps(p, "confirm")}


def rel_confirms(p):
    return [(s["role"], s["at"] - p["base"]) for s in steps(p, "confirm")]


def submits(p):
    return [(s["role"], s["at"] - p["base"]) for s in steps(p, "submit")]


for fwd in ("old_mid", "old_new", "old_mid_new"):
    rev, late = f"{fwd}_rev", f"{fwd}_late"
    check(f"{rev}: the same completion epochs as {fwd}, only the confirmation order differs",
          rel_completion(plans[fwd]) == rel_completion(plans[rev])
          and submits(plans[fwd]) == submits(plans[rev]), (rel_completion(plans[fwd]), rel_completion(plans[rev])))
    check(f"{late}: the same confirmations as {rev}, only the completion epochs differ",
          rel_confirms(plans[late]) == rel_confirms(plans[rev])
          and submits(plans[late]) == submits(plans[rev])
          and sorted(rel_completion(plans[late]).values()) == sorted(rel_completion(plans[rev]).values())
          and rel_completion(plans[late]) != rel_completion(plans[rev]),
          (rel_confirms(plans[late]), rel_completion(plans[late])))
    check(f"{late}: the fresh source completes last and is confirmed last",
          WANT[late][2][-1] == "old" and WANT[late][1][-1] == "old")

runtimes = [c["request"]["execution_runtime_ms"] for p in plans.values() for c in steps(p, "confirm")]
check("every confirmed runtime is distinct", len(runtimes) == len(set(runtimes)), runtimes)
objects = [csc.split_fqn(submit_of(p, "dest")["request"]["target_table"])[2] for p in plans.values()]
check("every history has its own object name", len(set(objects)) == len(objects), objects)
upstreams = [csc.upstream(RUN, n) for n in plans]
check("every history has its own upstream", len(set(upstreams)) == len(upstreams))


def strings(obj):
    if isinstance(obj, dict):
        for v in obj.values():
            yield from strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from strings(v)
    elif isinstance(obj, str):
        yield obj


for name, plan in plans.items():
    for s in plan["steps"]:
        q = s.get("request") or {}
        for rel in [q.get("target_table")] + [t["name"] for t in q.get("tables") or []] \
                + csc.sql_relations(q.get("sql") or ""):
            if rel:
                check(f"{name}: {rel} is a per-run CDO_DEMO_DB relation", csc.demo_relation(rel, RUN),
                      rel)
check("a relation outside CDO_DEMO_DB is refused",
      not csc.demo_relation(f'"OTHER_DB"."CSC_OLD"."CSC{RUN}_X"', RUN))
check("a relation of another run is refused",
      not csc.demo_relation('"CDO_DEMO_DB"."CSC_OLD"."CSC000000000000_X"', RUN))
check("the plan's submits stay within the budget",
      csc.max_submits() <= csc.SUBMIT_BUDGET, (csc.max_submits(), csc.SUBMIT_BUDGET))
check("the submit count is one per source plus the destination",
      csc.max_submits() == sum(len(s) + 1 for s, _, _ in WANT.values())
      and csc.max_submits(["old_mid_new", "old_alone"]) == 6, csc.max_submits())

# --- the capture table -----------------------------------------------------------------------

P = plans["old_mid_new_rev"]
OLD, MID, NEW = (submit_of(P, r)["request"] for r in ("old", "mid", "new"))
DEST = submit_of(P, "dest")["request"]


def entry(method, request, response, at):
    return {"method": method, "recorded_at": at, "request": request, "response": response}


def execute(rid):
    return {"response": {"ready_to_execute": {
        "request_id": rid, "explained_decision": {
            "decision": 1, "skip_rejection_reason": 6, "clone_rejection_reason": 6,
            "is_stale": False, "decision_description": "model was executed because its table did not exist"}}}}


def granted(source, epoch, runtime):
    return {"response": {"ready_to_clone": {
        "request_id": "rd", "clone_source": source, "clone_target": DEST["target_table"],
        "clone_required_last_modified_epoch": epoch, "execution_runtime_ms": runtime,
        "explained_decision": {"decision": 3, "skip_rejection_reason": 1,
                               "clone_rejection_reason": None, "is_stale": False,
                               "decision_description": "an equivalent model exists under another name so we cloned that one"},
        "execution_decision_id": "ed"}}}


def confirm(rid, epoch, runtime, success=True):
    return entry("ConfirmExecution", {"request_id": rid, "last_modified_epoch": epoch,
                                      "execution_runtime_ms": runtime},
                 {"success": success, "request_id": rid}, f"t{rid}")


def capture(dest_response, confirm_success=True, new_variant=execute):
    return [
        entry("SubmitEnrichedSQL", NEW, new_variant("rn"), "t1"),
        entry("SubmitEnrichedSQL", OLD, execute("ro"), "t2"),
        entry("SubmitEnrichedSQL", MID, execute("rm"), "t3"),
        confirm("rn", 3000, 333),
        confirm("rm", 2000, 222, success=confirm_success),
        confirm("ro", 1000, 111),
        entry("SubmitEnrichedSQL", DEST, dest_response, "t7"),
        entry("GetExplainMessages", {"execution_decision_ids": ["ed"]}, {"messages": []}, "t8"),
    ]


(row,) = csc.history_rows(capture(granted(OLD["target_table"], 1000, 111)))
check("one row per history, with its object and history names",
      row["object"] == csc.object_name(RUN, "old_mid_new_rev") and row["history"] == "old_mid_new_rev",
      (row["object"], row["history"]))
check("sources are listed in submit order with their roles",
      [s["role"] for s in row["sources"]] == ["new", "old", "mid"], row["sources"])
check("the confirmation order is read from the capture",
      row["confirm_order"] == ["new", "mid", "old"], row["confirm_order"])
check("the completion-epoch order is read from the confirms",
      row["completion_order"] == ["old", "mid", "new"], row["completion_order"])
check("a grant is attributed to the source it names, with agreeing epoch and runtime",
      row["result"] == "CLONE old" and row["agrees"] is True, row)
check("the entry of the destination submit is kept", row["dest_entry"] == 6, row["dest_entry"])

(row,) = csc.history_rows(capture(granted(OLD["target_table"], 2000, 111)))
check("a grant whose epoch is another source's is flagged", row["agrees"] is False, row)

(row,) = csc.history_rows(capture(execute("rd")))
check("a rebuild is BUILD with its codes", row["result"] == "BUILD 6/6" and row["agrees"] is None, row)

(row,) = csc.history_rows(capture(granted(OLD["target_table"], 1000, 111), confirm_success=False))
check("a refused source confirm makes the history inconclusive, once",
      row["inconclusive"] == ["mid confirm at entry 4 refused"], row["inconclusive"])

(row,) = csc.history_rows(capture(granted(OLD["target_table"], 1000, 111),
                                  new_variant=lambda rid: granted(OLD["target_table"], 1, 1)))
check("a source submit answered with anything but a first build makes the history inconclusive",
      row["inconclusive"] == ["new submit answered ready_to_clone, not a first build"],
      row["inconclusive"])

(row,) = csc.history_rows(capture(granted('"CDO_DEMO_DB"."ELSEWHERE"."X"', 1000, 111)))
check("a grant naming no source of the history is reported by name",
      row["result"] == 'CLONE "CDO_DEMO_DB"."ELSEWHERE"."X"' and row["agrees"] is False, row)

(row,) = csc.history_rows(capture(granted(DEST["target_table"], 1000, 111)))
check("a grant naming the destination itself never agrees",
      row["result"] == "CLONE dest" and row["agrees"] is False, row)

present = dict(DEST, tables=DEST["tables"] + [{"name": DEST["target_table"], "last_modified_epoch": 1}])
rows = capture(granted(OLD["target_table"], 1000, 111))
rows[6] = entry("SubmitEnrichedSQL", present, granted(OLD["target_table"], 1000, 111), "t7")
check("a destination that reports its own Target relation is inconclusive",
      "destination not absent" in csc.history_rows(rows)[0]["inconclusive"],
      csc.history_rows(rows)[0]["inconclusive"])


def sent_at(e, ms):
    return dict(e, metadata=[["x-submitted-at-epoch", str(ms)]])


rows = capture(granted(OLD["target_table"], 1000, 111))
up_epoch = OLD["tables"][0]["last_modified_epoch"]
rows[1] = sent_at(rows[1], up_epoch - 1)
check("a submit reporting an epoch after its own send time is inconclusive",
      any("old submit" in x and "future" in x for x in csc.history_rows(rows)[0]["inconclusive"]),
      csc.history_rows(rows)[0]["inconclusive"])
rows = capture(granted(OLD["target_table"], 1000, 111))
rows[1] = sent_at(rows[1], up_epoch)
rows[5] = sent_at(rows[5], 999)
check("a confirm whose completion epoch is after its delivery is inconclusive, an epoch at "
      "the send time is not",
      [x for x in csc.history_rows(rows)[0]["inconclusive"]] == ["old confirmed at entry 5 with a future epoch"],
      csc.history_rows(rows)[0]["inconclusive"])

rows = capture(granted(OLD["target_table"], 1000, 111))
check("a history whose destination was never submitted is inconclusive",
      csc.history_rows(rows[:6])[0]["inconclusive"], csc.history_rows(rows[:6]))

Q = plans["old_new"]
other = [entry("SubmitEnrichedSQL", submit_of(Q, "old")["request"], execute("qo"), "u1"),
         entry("SubmitEnrichedSQL", submit_of(Q, "new")["request"], execute("qn"), "u2"),
         confirm("qo", 1000, 444), confirm("qn", 3000, 555),
         entry("SubmitEnrichedSQL", submit_of(Q, "dest")["request"], execute("qd"), "u5")]
both = csc.history_rows(rows + other)
check("histories are separated by object name", [r["object"] for r in both]
      == [csc.object_name(RUN, "old_mid_new_rev"), csc.object_name(RUN, "old_new")],
      [r["object"] for r in both])
check("a source that was never confirmed makes the history inconclusive",
      csc.history_rows(rows[:4] + rows[5:])[0]["inconclusive"] == ["mid not confirmed"],
      csc.history_rows(rows[:4] + rows[5:])[0]["inconclusive"])

print(f"\nALL {passed} COPY SOURCE CHOICE TESTS PASSED")
