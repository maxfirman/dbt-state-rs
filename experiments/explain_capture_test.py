#!/usr/bin/env python3
"""Warehouse-free tests for the shared live Explain capture driver (#72).

Covers the parts that decide whether a live run is safe and whether its capture
is usable: the invocation plan, per-run names, the profile, port and SQL safety
checks, the explained-over-history oracle and the fixture trim. Every bundled
scenario must bind and plan. No warehouse, no proxy, no dbt. Exit 0 on success.

Run: python3 experiments/explain_capture_test.py
"""
import os
import socket
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import explain_capture as ec  # noqa: E402

passed = 0


def ok(name):
    global passed
    passed += 1
    print(f"PASS: {name}")


def fail(name, why):
    print(f"FAIL: {name} — {why}")
    sys.exit(1)


def expect_refusal(name, fn):
    try:
        fn()
    except ec.CaptureError:
        ok(name)
        return
    fail(name, "expected CaptureError, got none")


def expect_accept(name, fn):
    try:
        fn()
    except ec.CaptureError as e:
        fail(name, f"unexpected refusal: {e}")
    ok(name)


# --- scenario binding and plan ----------------------------------------------
TOY = ec.Scenario(
    name="toy",
    summary="a table and a view over it",
    names=("tup", "tview"),
    steps=[
        ec.Write("models/marts/${tup}.sql", "{{ config(materialized='table') }}\nselect 1 as id, '${run}' as tag\n"),
        ec.Write("models/marts/${tview}.sql", "select id from {{ ref('${tup}') }}\n"),
        ec.Build("${tup}", "${tview}"),
        ec.Explain("${tup}", intent="first build", decision="EXECUTE", execution_type=1, history=False),
        ec.Build("${tup}", "${tview}"),
        ec.Sql("insert into CDO_DEMO_DB.PROD_STATE_DEMO.${tup} select 2, 'x'"),
        ec.Explain("${tview}", intent="unchanged SKIP", decision="SKIP", execution_type=10),
        ec.Explain("${tup}", intent="unchanged SKIP", decision="SKIP"),
    ],
)

run_a, run_b = ec.new_run_id(), ec.new_run_id()
if run_a == run_b:
    fail("run ids differ", f"two run ids are both {run_a}")
ok("two run ids differ")
if not (len(run_a) == 12 and run_a[:6].isdigit() and all(c in "0123456789abcdef" for c in run_a[6:])):
    fail("run id is HHMMSS plus 6 random hex digits", run_a)
ok("run id is HHMMSS plus 6 random hex digits")

bound = ec.bind(TOY, "120000abcdef")
if bound.names != {"tup": "tup_120000abcdef", "tview": "tview_120000abcdef"}:
    fail("names get the run id suffix", bound.names)
ok("names get the run id suffix")
if bound.steps[0].path != "models/marts/tup_120000abcdef.sql" or "120000abcdef" not in bound.steps[0].text:
    fail("Write path and text are templated", bound.steps[0])
ok("Write path and text are templated")
if "{{ config(materialized='table') }}" not in bound.steps[0].text:
    fail("Jinja braces survive templating", bound.steps[0].text)
ok("Jinja braces survive templating")
if bound.steps[3].node != "tup_120000abcdef" or bound.steps[3].select != "tup_120000abcdef":
    fail("Explain node defaults to its selector", bound.steps[3])
ok("Explain node defaults to its selector")

plan = ec.plan(bound)
if (plan.builds, plan.run_operations, plan.explains, plan.total) != (2, 1, 3, 6):
    fail("plan counts every dbt invocation", plan)
ok("plan counts every dbt invocation")
if plan.targets != {"prod_demo"}:
    fail("plan lists the targets it will use", plan.targets)
ok("plan lists the targets it will use")

expect_refusal(
    "an undeclared name is refused at bind time",
    lambda: ec.bind(ec.Scenario("bad", "", ("a",), [ec.Build("${b}")]), "120000abcdef"),
)
expect_refusal(
    "a Write outside the project is refused",
    lambda: ec.bind(ec.Scenario("bad", "", ("a",), [ec.Write("../${a}.sql", "x")]), "120000abcdef"),
)
expect_refusal(
    "a Write that does not name a per-run node is refused",
    lambda: ec.bind(ec.Scenario("bad", "", ("a",), [ec.Write("models/marts/orders.sql", "x")]), "120000abcdef"),
)

# --- profile safety -------------------------------------------------------
PROFILES = """
snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      database: 'dp_model_ci_db'
      schema: 'user_x'
    prod_demo:
      type: snowflake
      database: 'CDO_DEMO_DB'
      schema: 'prod_state_demo'
    dev_clone:
      type: snowflake
      database: 'cdo_demo_db'
      schema: 'dev_clone_demo'
    duck:
      type: duckdb
      database: 'CDO_DEMO_DB'
"""
with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False) as fh:
    fh.write(PROFILES)
    profiles = fh.name
expect_accept("prod_demo and dev_clone resolve to CDO_DEMO_DB",
              lambda: ec.check_profile(profiles, "snowflake", {"prod_demo", "dev_clone"}))
expect_refusal("a target in another database is refused",
               lambda: ec.check_profile(profiles, "snowflake", {"prod_demo", "dev"}))
expect_refusal("a missing target is refused",
               lambda: ec.check_profile(profiles, "snowflake", {"nope"}))
expect_refusal("a non-Snowflake target is refused",
               lambda: ec.check_profile(profiles, "snowflake", {"duck"}))
expect_refusal("a missing profile is refused",
               lambda: ec.check_profile(profiles, "other", {"prod_demo"}))
os.unlink(profiles)

# --- port safety ------------------------------------------------------------
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
sock.listen(1)
busy = sock.getsockname()[1]
expect_refusal("a port someone else listens on is refused", lambda: ec.check_port_free(busy))
sock.close()
expect_accept("a free port is accepted", lambda: ec.check_port_free(busy))

# --- SQL safety ---------------------------------------------------------------
RUN = "120000abcdef"
expect_accept("a statement on a per-run CDO_DEMO_DB object is accepted",
              lambda: ec.check_sql(f"insert into CDO_DEMO_DB.PROD_STATE_DEMO.src_{RUN} select 1", RUN))
expect_refusal("a statement without the run id is refused",
               lambda: ec.check_sql("delete from CDO_DEMO_DB.PROD_STATE_DEMO.ORDERS", RUN))
expect_refusal("a statement naming another database is refused",
               lambda: ec.check_sql(f"insert into OTHER_DB.S.src_{RUN} select 1", RUN))
expect_refusal("a statement on a raw schema is refused",
               lambda: ec.check_sql(f"insert into CDO_DEMO_DB.RAW.src_{RUN} select 1", RUN))
expect_refusal("an unqualified statement is refused",
               lambda: ec.check_sql(f"insert into src_{RUN} select 1", RUN))
SRC = f"CDO_DEMO_DB.PROD_STATE_DEMO.src_{RUN}"
expect_refusal("a destination spelled with spaces around the dots is refused (Codex #85)",
               lambda: ec.check_sql(f"insert into OTHER_DB . PUBLIC . ORDERS select * from {SRC}", RUN))
expect_refusal("an unqualified destination is refused even with a per-run source",
               lambda: ec.check_sql(f"insert into ORDERS select * from {SRC}", RUN))
expect_refusal("a two-part name is refused",
               lambda: ec.check_sql(f"insert into {SRC} select * from PUBLIC.ORDERS", RUN))
expect_refusal("a four-part name is refused",
               lambda: ec.check_sql(f"insert into {SRC}.x select 1", RUN))
expect_refusal("Jinja braces are refused (the macro would render them)",
               lambda: ec.check_sql(f"insert into {SRC} select '{{{{ target.name }}}}'", RUN))
expect_refusal("a second statement is refused",
               lambda: ec.check_sql(f"insert into {SRC} select 1; delete from ORDERS", RUN))
expect_refusal("a comment is refused",
               lambda: ec.check_sql(f"insert into {SRC} select 1 -- x", RUN))
expect_refusal("a case-mismatched quoted database is refused",
               lambda: ec.check_sql(f'insert into "cdo_demo_db"."PROD_STATE_DEMO"."src_{RUN}" select 1', RUN))
expect_refusal("an IDENTIFIER() reference is refused",
               lambda: ec.check_sql(f"insert into {SRC} select * from identifier('OTHER_DB.S.T')", RUN))
expect_refusal("an unsupported statement form is refused",
               lambda: ec.check_sql(f"merge into {SRC} using {SRC} s on true when matched then delete", RUN))
expect_refusal("an unterminated string is refused",
               lambda: ec.check_sql(f"insert into {SRC} select 'x", RUN))
for stmt in (
    f"create or replace table {SRC} as select 1 as id, 'src0' as v",
    f"insert into {SRC} select 2 as id, 'a.b.c' as v",
    f"insert into {SRC} (id, v) values (1, 'x')",
    f'insert into "CDO_DEMO_DB"."PROD_STATE_DEMO"."src_{RUN}" select 1.5',
    f"insert into CDO_DEMO_DB . PROD_STATE_DEMO . src_{RUN} select 1",
    f"update {SRC} set v = 'y' where id = 1",
    f"delete from {SRC} where id = 2",
    f"truncate table {SRC}",
    f"drop table if exists {SRC}",
):
    expect_accept(f"accepted: {stmt[:48]}…", lambda: ec.check_sql(stmt, RUN))

# --- profiles dir -----------------------------------------------------------------
expect_refusal("a --profiles file not named profiles.yml is refused (dbt could not load it)",
               lambda: ec.profiles_dir("/tmp/x/other.yml"))
if ec.profiles_dir("/tmp/x/profiles.yml") != "/tmp/x":
    fail("the profiles dir is the validated file's directory", ec.profiles_dir("/tmp/x/profiles.yml"))
ok("the profiles dir is the validated file's directory")

with tempfile.TemporaryDirectory() as work:
    run = ec.Run(ec.bind(TOY, RUN), 1, work, "/nonexistent", False, "/validated/dir")
    os.makedirs(run.logs)
    os.makedirs(os.path.join(run.project, "macros"))
    seen = []
    real_run = ec.subprocess.run

    class _Done:
        returncode = 0

    ec.subprocess.run = lambda cmd, **kw: seen.append(cmd) or _Done()
    try:
        for k, step in enumerate(run.bound.steps):
            if not isinstance(step, (ec.Write, ec.Remove)):
                run.run_step(k, step)
    finally:
        ec.subprocess.run = real_run
    if not seen or any(c[c.index("--profiles-dir") + 1] != "/validated/dir"
                       if "--profiles-dir" in c else True for c in seen):
        fail("every dbt invocation uses the validated profiles dir (Codex #85)", seen)
    ok(f"every dbt invocation uses the validated profiles dir ({len(seen)} invocations)")

# --- build flags (#73: microbatch event-time window) ------------------------------
WINDOW = ("--event-time-start", "2026-11-01", "--event-time-end", "2026-11-03")
mb = ec.Scenario("mb", "", ("m",), [ec.Build("${m}", args=WINDOW)])
with tempfile.TemporaryDirectory() as work:
    run = ec.Run(ec.bind(mb, RUN), 1, work, "/nonexistent", False, "/validated/dir")
    os.makedirs(run.logs)
    seen = []
    real_run = ec.subprocess.run
    ec.subprocess.run = lambda cmd, **kw: seen.append(cmd) or _Done()
    try:
        run.run_step(0, run.bound.steps[0])
    finally:
        ec.subprocess.run = real_run
    cmd = seen[0] if seen else []
    if [a for a in cmd if a in WINDOW] != list(WINDOW) or f"m_{RUN}" not in cmd:
        fail("a Build passes its event-time window flags to dbt build", cmd)
    ok("a Build passes its event-time window flags to dbt build")
expect_refusal(
    "a Build flag outside the allowed set is refused at bind time",
    lambda: ec.bind(ec.Scenario("bad", "", ("m",), [ec.Build("${m}", args=("--target", "dev"))]), RUN),
)
expect_refusal(
    "a Build flag without its value is refused at bind time",
    lambda: ec.bind(ec.Scenario("bad", "", ("m",), [ec.Build("${m}", args=("--event-time-end",))]), RUN),
)
expect_refusal(
    "a Build flag value that looks like another flag is refused",
    lambda: ec.bind(ec.Scenario("bad", "", ("m",),
                                [ec.Build("${m}", args=("--event-time-end", "--full-refresh"))]), RUN),
)

# --- failing data tests (#75) ---------------------------------------------------------
# `dbt build` exits 1 when a data test fails. A Build that expects failing tests
# accepts exit 1 (and only 1); every other Build still stops the run.
with tempfile.TemporaryDirectory() as work:
    run = ec.Run(ec.bind(TOY, RUN), 1, work, "/nonexistent", False, "/validated/dir")
    os.makedirs(run.logs)
    real_run = ec.subprocess.run

    def exits(rc):
        class _Rc:
            returncode = rc
        ec.subprocess.run = lambda cmd, **kw: _Rc()

    plain = ec.Build("tup_120000abcdef")
    failing = ec.bind(ec.Scenario("f", "", ("tup",), [ec.Build("${tup}", test_failures=True)]),
                      "120000abcdef").steps[0]
    try:
        if not failing.test_failures:
            fail("binding keeps test_failures", failing)
        ok("binding keeps test_failures")
        exits(1)
        expect_refusal("a Build exiting 1 stops the run", lambda: run.run_step(0, plain))
        expect_accept("a Build expecting failing tests accepts exit 1", lambda: run.run_step(0, failing))
        exits(2)
        expect_refusal("a Build expecting failing tests still stops on exit 2",
                       lambda: run.run_step(0, failing))
    finally:
        ec.subprocess.run = real_run


# --- oracle -------------------------------------------------------------------
def submit(node, variant, edid, rid=None, et=1, decision=None):
    inner = {"execution_decision_id": edid,
             "explained_decision": {"decision": decision if decision is not None else
                                    {"skip_execution": 0, "ready_to_execute": 1, "ready_to_clone": 3}[variant],
                                    "decision_description": f"{node} {variant}"}}
    if rid:
        inner["request_id"] = rid
    return {"service": "SQL", "method": "SubmitEnrichedSQL",
            "request": {"execution_type": et, "target_table": f'"CDO_DEMO_DB"."PROD_STATE_DEMO"."{node.upper()}"',
                        "labels": {"dbt_node_name": node}},
            "response": {"response": {variant: inner}}}


def confirm(rid):
    return {"service": "Execution", "method": "ConfirmExecution",
            "request": {"request_id": rid}, "response": {"success": True, "request_id": rid}}


def explain(edid, decision, lines=None):
    return {"service": "Explain", "method": "GetExplainMessages",
            "request": {"execution_decision_ids": [edid]},
            "response": {"messages": [{"execution_decision_id": edid, "decision": decision,
                                       "decision_description": "d",
                                       "explain_lines": lines if lines is not None else
                                       [{"text": "query analysis", "marker": None, "badge": None, "children": []}]}]}}


def telemetry():
    return {"service": "ClientTelemetry", "method": "SubmitTelemetryBatch", "request": {}, "response": {}}


up, view = bound.names["tup"], bound.names["tview"]
# One row list per bound step, in step order (non-dbt steps capture nothing).
per_step = [
    [], [],
    [submit(up, "ready_to_execute", "e1", "r1"), submit(view, "ready_to_execute", "e2", "r2", et=10),
     confirm("r1"), confirm("r2"), telemetry()],
    [explain("e1", 1)],
    [submit(up, "skip_execution", "e3"), submit(view, "skip_execution", "e4", et=10)],
    [],
    [explain("e4", 0)],
    [explain("e3", 0)],
]


def flatten(steps):
    rows, marks = [], []
    for chunk in steps:
        rows.extend(chunk)
        marks.append(len(rows))
    return rows, marks


def run_oracle(steps):
    rows, marks = flatten(steps)
    return ec.oracle(bound, rows, marks)


results = run_oracle(per_step)
if [r.ok for r in results] != [True, True, True] or any(r.problems for r in results):
    fail("a complete campaign passes the oracle", [(r.intent, r.problems) for r in results])
ok("a complete campaign passes the oracle")
if (results[1].decision, results[1].execution_type, results[1].lines) != ("SKIP", 10, ["- query analysis"]):
    fail("oracle reports the explained decision, kind and tree", results[1])
ok("oracle reports the explained decision, kind and tree")

no_confirm = [list(c) for c in per_step]
no_confirm[2] = [r for r in no_confirm[2] if not (r["method"] == "ConfirmExecution" and r["request"]["request_id"] == "r2")]
res = run_oracle(no_confirm)
if res[1].ok or not any("history" in p for p in res[1].problems):
    fail("a SKIP without a confirmed build in the window fails the oracle", res[1])
ok("a SKIP without a confirmed build in the window fails the oracle")
if not res[2].ok:
    fail("other nodes keep their history", res[2])
ok("other nodes keep their history")

for label, resp in (("success=false", {"success": False, "request_id": "r2"}),
                    ("success omitted", {"request_id": "r2"})):
    rejected = [list(c) for c in per_step]
    rejected[2] = [dict(r, response=resp) if r["method"] == "ConfirmExecution"
                   and r["request"]["request_id"] == "r2" else r for r in rejected[2]]
    res = run_oracle(rejected)
    if res[1].ok or not any("history" in p for p in res[1].problems):
        fail(f"a SKIP whose build's confirmation was rejected ({label}) fails the oracle (Codex #85)",
             res[1])
    ok(f"a SKIP whose build's confirmation was rejected ({label}) fails the oracle")

wrong_node = [list(c) for c in per_step]
wrong_node[6] = [explain("e3", 0)]
res = run_oracle(wrong_node)
if res[1].ok or not any("not explained" in p for p in res[1].problems):
    fail("explaining another node's decision fails the oracle", res[1])
ok("explaining another node's decision fails the oracle")

stale = [list(c) for c in per_step]
stale[7] = [explain("e1", 1)]
res = run_oracle(stale)
if res[2].ok or not any("not explained" in p for p in res[2].problems):
    fail("explaining an older decision of the node fails the oracle", res[2])
ok("explaining an older decision of the node fails the oracle")

missed = [list(c) for c in per_step]
missed[4] = [submit(up, "ready_to_execute", "e3", "r3"), submit(view, "skip_execution", "e4", et=10)]
missed[7] = [explain("e3", 1)]
res = run_oracle(missed)
if res[2].ok or not any("intent" in p for p in res[2].problems):
    fail("a decision other than the intended one fails the oracle", res[2])
ok("a decision other than the intended one fails the oracle")

no_explain = [list(c) for c in per_step]
no_explain[6] = []
res = run_oracle(no_explain)
if res[1].ok or not any("no GetExplainMessages" in p for p in res[1].problems):
    fail("an explain step that captured no call fails the oracle", res[1])
ok("an explain step that captured no call fails the oracle")

no_message = [list(c) for c in per_step]
no_message[6] = [{**explain("e4", 0), "response": {"messages": []}}]
res = run_oracle(no_message)
if res[1].ok or not any("no message" in p for p in res[1].problems):
    fail("a hosted reply without the message fails the oracle", res[1])
ok("a hosted reply without the message fails the oracle")

empty_tree = [list(c) for c in per_step]
empty_tree[6] = [explain("e4", 0, lines=[])]
res = run_oracle(empty_tree)
if not res[1].ok or res[1].lines != []:
    fail("an empty hosted tree is evidence, not a failure", res[1])
ok("an empty hosted tree is evidence, not a failure")

# --- demo database check (#75) ------------------------------------------------------
# A data test has no target (`target_table: null`); it is not a submit outside the
# demo database, and it must not crash the report.
demo_rows = [
    {"method": "SubmitEnrichedSQL", "request": {"target_table": None, "execution_type": 8}},
    {"method": "SubmitEnrichedSQL", "request": {"target_table": '"CDO_DEMO_DB"."PROD_STATE_DEMO"."M"'}},
    {"method": "SubmitEnrichedSQL", "request": {"target_table": '"OTHER_DB"."S"."M"'}},
]
off = ec.off_demo_submits(demo_rows)
if off != [demo_rows[2]]:
    fail("only a submit with a target outside CDO_DEMO_DB is off-demo", off)
ok("only a submit with a target outside CDO_DEMO_DB is off-demo (a data test has none)")

# --- fixture trim ---------------------------------------------------------------
rows, _ = flatten(per_step)
rows = rows + [{"service": "SQL", "method": "RecordExecutions", "request": {}, "response": {}},
               {"service": "SelectorService", "method": "GetStateSelection", "request": {}, "response": {}},
               {"service": "ClientValidation", "method": "ValidateClientVersion", "request": {}, "response": {}}]
kept, dropped = ec.trim(rows)
if [r["method"] for r in kept] != [r["method"] for r in rows if r["method"] in ec.REPLAYED_METHODS]:
    fail("trim keeps replayed methods in order", [r["method"] for r in kept])
ok("trim keeps replayed methods in order")
if dict(dropped) != {"SubmitTelemetryBatch": 1, "GetStateSelection": 1}:
    fail("trim reports what it dropped", dropped)
ok("trim reports what it dropped")
# A `--full-refresh` incremental run records its build with RecordExecutions
# (client 2.0.11, #73); the replay drives it, so the fixture keeps it.
if "RecordExecutions" not in [r["method"] for r in kept]:
    fail("trim keeps RecordExecutions", [r["method"] for r in kept])
ok("trim keeps RecordExecutions")

# --- bundled scenarios ------------------------------------------------------------
names = ec.scenario_names()
if "snapshot_explain" not in names:
    fail("the snapshot scenario is bundled", names)
for name in names:
    sc = ec.load_scenario(name)
    b = ec.bind(sc, ec.new_run_id())
    p = ec.plan(b)
    if p.total <= 0 or not any(isinstance(s, ec.Explain) for s in b.steps):
        fail(f"scenario {name} plans dbt invocations and explains", p)
    ok(f"scenario {name} binds and plans ({p.total} dbt invocations)")

# --- command line -----------------------------------------------------------------
import subprocess  # noqa: E402

cli = subprocess.run([sys.executable, os.path.join(HERE, "explain_capture.py"), "snapshot_explain",
                      "--port", "1", "--dry-run", "--profiles", "/nonexistent/profiles.yml"],
                     capture_output=True, text=True)
expected = ec.plan(ec.bind(ec.load_scenario("snapshot_explain"), "120000abcdef")).total
if f"dbt invocations: {expected} " not in cli.stdout:
    fail("the CLI prints the scenario's invocation count before any check", cli.stdout + cli.stderr)
ok("the CLI prints the scenario's invocation count before any check")
if cli.returncode != 2 or "REFUSED" not in cli.stdout:
    fail("the CLI refuses without a readable profile", cli.stdout + cli.stderr)
ok("the CLI refuses without a readable profile")

print(f"\nALL {passed} CAPTURE DRIVER TESTS PASSED")
