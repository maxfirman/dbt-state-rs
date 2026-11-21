#!/usr/bin/env python3
"""Warehouse-free tests for the #200 data-test history tools (`data_test_history.py`).

Covers the two parts a live run depends on:

- echo attribution: which confirmed execution a data-test SKIP's
  `execution_runtime_ms` matches, so the note can say whose run each SKIP
  reused;
- the crafted one-factor probe plan: every variant differs from its base in
  exactly the field the cell names, every confirm runtime is distinct (so an
  echo names one execution), every name is per-run and every relation is in
  a CDO_DEMO_DB demo schema, and the plan stays inside its submit budget.

No warehouse, no proxy, no dbt. Exit 0 on success.

Run: python3 experiments/data_test_history_test.py
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import data_test_history as dth  # noqa: E402

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


# --- echo attribution ---------------------------------------------------------------


def submit(uid, sql, variant, rid=None, runtime=None, target="prod_demo", schema="prod_state_demo"):
    body = {"explained_decision": {"decision": 0 if variant == "skip_execution" else 1,
                                   "skip_rejection_reason": None if variant == "skip_execution" else 6,
                                   "clone_rejection_reason": None if variant == "skip_execution" else 6,
                                   "decision_description": "d"},
            "execution_decision_id": f"e-{uid}-{rid or runtime}"}
    if variant == "skip_execution":
        body["execution_runtime_ms"] = runtime
        body["execution_results"] = {"fields": {"failures": {"kind": {"int_value": 0}}}}
    else:
        body["request_id"] = rid
    return {"method": "SubmitEnrichedSQL",
            "request": {"execution_type": 8, "sql": sql, "default_schema": schema,
                        "labels": {"dbt_node_unique_id": uid, "dbt_node_name": uid.split(".")[-1]},
                        "dbt_node_state": {"node_unique_id": uid, "target_name": target}},
            "response": {"response": {variant: body}}}


def confirm(rid, runtime, success=True, failures=0):
    return {"method": "ConfirmExecution",
            "request": {"request_id": rid, "execution_runtime_ms": runtime,
                        "execution_results": {"fields": {"failures": {"kind": {"int_value": failures}}}}},
            "response": {"success": success, "request_id": rid}}


A, B = "test.p.a", "test.p.b"
SQL = "select id from x where id < 0"
rows = [
    submit(A, SQL, "ready_to_execute", rid="r1"),          # 0
    confirm("r1", 1701),                                     # 1
    submit(B, "\n" + SQL + "  ", "ready_to_execute", rid="r2"),  # 2
    confirm("r2", 2136),                                     # 3
    submit(A, SQL, "skip_execution", runtime=2136),         # 4: echoes B's run
    submit(A, SQL, "skip_execution", runtime=999),          # 5: echoes nothing in the window
    submit(A, SQL, "ready_to_execute", rid="r3"),           # 6
    confirm("r3", 555, success=False),                       # 7: refused confirm records nothing
    submit(A, SQL, "skip_execution", runtime=555),          # 8
    submit(B, SQL, "skip_execution", runtime=3000),         # 9: confirmed only later
    confirm("rX", 3000),                                     # 10
]
table = dth.data_test_rows(rows)
by_entry = {r["entry"]: r for r in table}
check("one row per data-test submit", sorted(by_entry) == [0, 2, 4, 5, 6, 8, 9], sorted(by_entry))
check("an execution row carries its confirmed runtime",
      by_entry[0]["confirmed_runtime"] == 1701 and by_entry[0]["confirm_entry"] == 1, by_entry[0])
m = by_entry[4]["matches"]
check("a SKIP echo is attributed to the execution with that runtime",
      [x["entry"] for x in m] == [2], m)
check("the attribution says the matched run is another unique id with the same SQL",
      m[0]["same_uid"] is False and m[0]["same_sql"] is True and m[0]["target_name"] == "prod_demo", m)
check("SQL is compared after trimming and collapsing whitespace",
      dth.normalized_sql("\n select  1\n") == "select 1", dth.normalized_sql("\n select  1\n"))
check("an echo no confirmed run in the window has is unattributed", by_entry[5]["matches"] == [],
      by_entry[5])
check("a refused confirm is not an execution", by_entry[8]["matches"] == [] and
      by_entry[6]["confirmed_runtime"] is None, (by_entry[6], by_entry[8]))
check("only executions confirmed before the SKIP count", by_entry[9]["matches"] == [], by_entry[9])
check("a SKIP row carries the echoed runtime and failures",
      by_entry[4]["echoed_runtime"] == 2136 and by_entry[4]["failures"] == 0, by_entry[4])
check("the latest execution of the same unique id is named for comparison",
      by_entry[4]["own_latest"] == {"entry": 0, "runtime": 1701}, by_entry[4])
check("decision names are the wire names", by_entry[4]["decision"] == "SKIP"
      and by_entry[0]["decision"] == "EXECUTE", (by_entry[4], by_entry[0]))

# --- crafted one-factor probe plan ----------------------------------------------------

RUN = "120000abcdef"
cells = dth.plan(RUN, epoch=1_000_000)
names = [c["cell"] for c in cells]
check("the plan has one cell per factor", sorted(names) == sorted(dth.FACTORS), names)
for c in cells:
    want = dth.FACTORS[c["factor"]]["paths"]
    got = dth.diff_paths(c["base"], c["variant"])
    check(f"cell {c['cell']}: the variant differs from its base exactly in {sorted(want)}",
          got == set(want), sorted(got))

runtimes = [rt for c in cells for rt in c["runtimes"].values()]
check("every planned confirm runtime is distinct", len(runtimes) == len(set(runtimes)), runtimes)


def strings(obj):
    if isinstance(obj, dict):
        for v in obj.values():
            yield from strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from strings(v)
    elif isinstance(obj, str):
        yield obj


for c in cells:
    for req in (c["base"], c["variant"]):
        uid = req["labels"]["dbt_node_unique_id"]
        check(f"cell {c['cell']}: node names are per-run ({uid})", RUN in uid, uid)
        for t in req["tables"]:
            check(f"cell {c['cell']}: {t['name']} is a per-run demo relation",
                  dth.demo_relation(t["name"], RUN), t["name"])
        check(f"cell {c['cell']}: the SQL reads only per-run demo relations",
              all(dth.demo_relation(r, RUN) for r in dth.sql_relations(req["sql"])),
              req["sql"])

check("a relation outside the demo schemas is refused",
      not dth.demo_relation('"CDO_DEMO_DB"."RAW"."X_120000ABCDEF"', RUN))
check("a relation of another run is refused",
      not dth.demo_relation('"CDO_DEMO_DB"."PROD_STATE_DEMO"."X_000000000000"', RUN))
check("the plan's submits stay within the budget",
      dth.max_submits(cells) <= dth.SUBMIT_BUDGET, (dth.max_submits(cells), dth.SUBMIT_BUDGET))

# --- same-key order cells ----------------------------------------------------------------
# Two unique ids with one SQL, both submitted before either confirms (as when one
# invocation runs both), confirmed in the other order, then each submitted again.
order = dth.plan_order(RUN, epoch=1_000_000)
check("one order cell per ordering", sorted(c["cell"] for c in order) == ["fail_confirmed_last",
                                                                          "fail_submitted_last"],
      [c["cell"] for c in order])
for c in order:
    a, b = c["first"], c["second"]
    check(f"order cell {c['cell']}: the pair differs only in its unique id",
          dth.diff_paths(a, b) == dth.FACTORS["unique_id"]["paths"], sorted(dth.diff_paths(a, b)))
    check(f"order cell {c['cell']}: the pair is confirmed in the reverse of submit order",
          c["confirm_order"] == ["second", "first"], c["confirm_order"])
    check(f"order cell {c['cell']}: exactly one of the pair fails",
          sorted(c["failing"].values()) == [False, True], c["failing"])
    check(f"order cell {c['cell']}: names are per-run", RUN in a["labels"]["dbt_node_unique_id"]
          and RUN in b["labels"]["dbt_node_unique_id"])
check("the failing run is the later confirm in one cell and the later submit in the other",
      {c["cell"]: c["failing"]["first"] for c in order}
      == {"fail_confirmed_last": True, "fail_submitted_last": False},
      [(c["cell"], c["failing"]) for c in order])
order_runtimes = [rt for c in order for rt in c["runtimes"].values()]
check("every order-cell runtime is distinct, also from the factor cells",
      len(set(order_runtimes)) == len(order_runtimes) and not set(order_runtimes) & set(runtimes),
      order_runtimes)
check("the order cells cost four submits each", dth.order_submits(order) == 8,
      dth.order_submits(order))

print(f"\nALL {passed} DATA TEST HISTORY TESTS PASSED")
