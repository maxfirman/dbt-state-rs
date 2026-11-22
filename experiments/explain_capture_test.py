#!/usr/bin/env python3
"""Warehouse-free tests for the shared live Explain capture driver (#72).

Covers the parts that decide whether a live run is safe and whether its capture
is usable: the invocation plan, per-run names, the profile, port and SQL safety
checks, the explained-over-history oracle and the fixture trim. Every bundled
scenario must bind and plan. No warehouse, no proxy, no dbt. Exit 0 on success.

Run: python3 experiments/explain_capture_test.py
"""
import json
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


def _state_env(env):
    """Only the State-related variables of an environment (never print the rest: it may hold
    credentials)."""
    return {k: v for k, v in env.items()
            if k in ("DBT_ENGINE_MANAGE_STATE", "RUN_CACHE_API_URL", "RUN_CACHE_API_SECURE",
                     "PROBE_URL")}


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
expect_refusal(
    "a Remove of an overridable shared file is refused",
    lambda: ec.bind(ec.Scenario("bad", "", ("a",), [ec.Remove("macros/generate_schema_name.sql")]),
                    "120000abcdef"),
)
_gsn = ec.bind(ec.Scenario("gsn", "", ("a",), [ec.Write("macros/generate_schema_name.sql", "x")]),
               "120000abcdef")
if _gsn.steps[0].path != "macros/generate_schema_name.sql":
    fail("a Write may replace the schema macro in the run's own copy (#106)", _gsn.steps[0])
ok("a Write may replace the schema macro in the run's own copy (#106)")

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
    f"drop view if exists {SRC}",
    f"drop view {SRC}",
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


# --- probe explains (#104) ------------------------------------------------------------
# `via="probe"` sends GetExplainMessages for the node's current decision with
# state-probe through the run's proxy: no dbt invocation, no target.
PROBED = ec.Scenario("p", "", ("pa",), [
    ec.Build("${pa}"),
    ec.Explain("${pa}", intent="probe", via="probe"),
    ec.Explain("${pa}", intent="dbt"),
])
pb = ec.bind(PROBED, RUN)
pp = ec.plan(pb)
if (pp.explains, pp.probe_explains, pp.total) != (1, 1, 2):
    fail("a probe explain is not a dbt invocation", pp)
ok("a probe explain is not a dbt invocation")
expect_refusal("an Explain via something else is refused at bind time",
               lambda: ec.bind(ec.Scenario("bad", "", ("pa",),
                                           [ec.Explain("${pa}", intent="x", via="curl")]), RUN))
with tempfile.TemporaryDirectory() as work:
    run = ec.Run(pb, 50999, work, "/nonexistent", False, "/validated/dir")
    os.makedirs(run.logs)
    pa = pb.names["pa"]
    older = {"method": "SubmitEnrichedSQL", "request": {"execution_type": 2, "labels": {"dbt_node_name": pa}},
             "response": {"response": {"ready_to_execute": {"execution_decision_id": "old", "request_id": "r"}}}}
    newer = {"method": "SubmitEnrichedSQL", "request": {"execution_type": 2, "labels": {"dbt_node_name": pa}},
             "response": {"response": {"skip_execution": {"execution_decision_id": "new"}}}}
    other = {"method": "SubmitEnrichedSQL", "request": {"execution_type": 1, "labels": {"dbt_node_name": "x"}},
             "response": {"response": {"skip_execution": {"execution_decision_id": "other"}}}}
    run.rows = lambda: [older, newer, other]
    calls = []
    real_run = ec.subprocess.run

    class _Ok:
        returncode = 0

    ec.subprocess.run = lambda cmd, **kw: calls.append((cmd, kw)) or _Ok()
    try:
        run.run_step(1, pb.steps[1])
    finally:
        ec.subprocess.run = real_run
    if (len(calls) != 1 or calls[0][0] != [ec.PROBE_BIN, "explain"]
            or json.loads(calls[0][1]["input"]) != {"execution_decision_ids": ["new"]}
            or calls[0][1]["env"].get("PROBE_URL") != "http://127.0.0.1:50999"):
        fail("a probe explain asks the run's proxy for the node's latest decision",
             [(c, kw.get("input"), _state_env(kw.get("env") or {})) for c, kw in calls])
    ok("a probe explain asks the run's proxy for the node's latest decision")


# --- warehouse read-back (#191) -----------------------------------------------------------
# `Read` selects every row of one per-run demo relation with `dbt show`, without
# dbt State, so a capture can compare what the Warehouse wrote with what the
# service rendered.
READS = ec.Scenario("r", "", ("ra",), [
    ec.Build("${ra}"),
    ec.Read("CDO_DEMO_DB.PROD_STATE_DEMO.${ra}"),
    ec.Explain("${ra}", intent="dbt"),
])
rb = ec.bind(READS, RUN)
if rb.steps[1].query != f"select * from CDO_DEMO_DB.PROD_STATE_DEMO.ra_{RUN}":
    fail("a Read selects every row of its per-run relation", rb.steps[1])
ok("a Read selects every row of its per-run relation")
rp = ec.plan(rb)
if (rp.reads, rp.total) != (1, 3):
    fail("a Read is a dbt invocation", rp)
ok("a Read is a dbt invocation")
for bad in ("CDO_DEMO_DB.PROD_STATE_DEMO.ORDERS", "OTHER_DB.PROD_STATE_DEMO.${ra}",
            "PROD_STATE_DEMO.${ra}", "CDO_DEMO_DB.RAW.${ra}",
            "CDO_DEMO_DB.PROD_STATE_DEMO.${ra} where 1=1"):
    expect_refusal(f"a Read of {bad!r} is refused",
                   lambda: ec.bind(ec.Scenario("bad", "", ("ra",), [ec.Read(bad)]), RUN))

SHOW_LOG = """       dbt 2.0.11
   Loading ~/.dbt/profiles.yml
[{"TS":"2026-11-17T16:39:51.373Z","R":5488611025542342294,"U":"d2208da5-9b9d-4b51-a3a1-b4b101062c49"}]
 Succeeded model prod_state_demo.inline (ephemeral) [1 of 1 in 4.00s]
"""
if ec.parse_show_rows(SHOW_LOG) != [{"TS": "2026-11-17T16:39:51.373Z", "R": 5488611025542342294,
                                     "U": "d2208da5-9b9d-4b51-a3a1-b4b101062c49"}]:
    fail("dbt show JSON rows are parsed from the log", ec.parse_show_rows(SHOW_LOG))
ok("dbt show JSON rows are parsed from the log")
expect_refusal("a show log without JSON rows is refused",
               lambda: ec.parse_show_rows("  Loading\n Succeeded\n"))

with tempfile.TemporaryDirectory() as work:
    run = ec.Run(rb, 50999, work, "/nonexistent", False, "/validated/dir")
    os.makedirs(run.logs)
    calls = []
    real_run = ec.subprocess.run

    class _Shown:
        returncode = 0

    def fake_show(cmd, **kw):
        calls.append((cmd, kw))
        kw["stdout"].write(SHOW_LOG)
        return _Shown()

    ec.subprocess.run = fake_show
    try:
        run.run_step(1, rb.steps[1])
    finally:
        ec.subprocess.run = real_run
    cmd, kw = calls[0]
    if cmd[:2] != ["dbt", "show"] or cmd[cmd.index("--inline") + 1] != rb.steps[1].query:
        fail("a Read runs dbt show --inline with its query", cmd)
    ok("a Read runs dbt show --inline with its query")
    if "DBT_ENGINE_MANAGE_STATE" in kw["env"] or kw["env"].get("RUN_CACHE_API_URL") != "127.0.0.1:50999":
        fail("a Read runs without dbt State, any State call trapped by the run's proxy", _state_env(kw["env"]))
    ok("a Read runs without dbt State, any State call trapped by the run's proxy")
    if "--no-manage-state" not in cmd:
        fail("a Read disables dbt State on the command line", cmd)
    ok("a Read disables dbt State on the command line")
    if run.reads != [{"step": 1, "relation": rb.steps[1].relation, "rows": ec.parse_show_rows(SHOW_LOG)}]:
        fail("a Read records the rows it read", run.reads)
    ok("a Read records the rows it read")

# --- State-disabled Reference execution (#237) ---------------------------------------------
# `Reference` rebuilds per-run nodes from scratch with dbt State off, so a capture
# can compare the rows State served with the rows an independent run produces.
REFS = ec.Scenario("ref", "", ("fa", "fb"), [
    ec.Build("${fa}", "${fb}", target="dev_clone"),
    ec.Reference("${fa}", "${fb}", target="dev_clone"),
    ec.Explain("${fb}", intent="probe", via="probe"),
])
fb_ = ec.bind(REFS, RUN)
if fb_.steps[1].select != (f"fa_{RUN}", f"fb_{RUN}") or fb_.steps[1].target != "dev_clone":
    fail("a Reference binds its per-run selectors and target", fb_.steps[1])
ok("a Reference binds its per-run selectors and target")
fp = ec.plan(fb_)
if (fp.builds, fp.references, fp.total, fp.targets) != (1, 1, 2, {"dev_clone"}):
    fail("a Reference is a dbt invocation on its target, not a State build", fp)
ok("a Reference is a dbt invocation on its target, not a State build")
# A Reference full-refreshes what it selects, so each selector must be exactly
# one of the run's node names: dbt reads one string as several selectors.
for bad in ("orders", "+${fa}", "${fa}+", "orders ${fa}", "${fa} orders", "${fa},orders",
            "tag:${fa}", "${run}"):
    expect_refusal(f"a Reference selecting {bad!r} is refused",
                   lambda: ec.bind(ec.Scenario("bad", "", ("fa",), [ec.Reference(bad)]), RUN))
expect_refusal("a Reference selecting nothing is refused",
               lambda: ec.bind(ec.Scenario("bad", "", ("fa",), [ec.Reference()]), RUN))
with tempfile.TemporaryDirectory() as work:
    run = ec.Run(fb_, 50999, work, "/nonexistent", False, "/validated/dir")
    os.makedirs(run.logs)
    calls = []
    real_run = ec.subprocess.run

    class _Rc0:
        returncode = 0

    ec.subprocess.run = lambda cmd, **kw: calls.append((cmd, kw)) or _Rc0()
    try:
        run.run_step(1, fb_.steps[1])
    finally:
        ec.subprocess.run = real_run
    cmd, kw = calls[0]
    if (cmd[:2] != ["dbt", "build"] or "--full-refresh" not in cmd
            or cmd[cmd.index("--target") + 1] != "dev_clone"
            or cmd[cmd.index("--select") + 1:cmd.index("--select") + 3] != [f"fa_{RUN}", f"fb_{RUN}"]):
        fail("a Reference runs dbt build --full-refresh of its nodes", cmd)
    ok("a Reference runs dbt build --full-refresh of its nodes")
    if "DBT_ENGINE_MANAGE_STATE" in kw["env"] or kw["env"].get("RUN_CACHE_API_URL") != "127.0.0.1:50999":
        fail("a Reference runs without dbt State, any State call trapped by the run's proxy", _state_env(kw["env"]))
    ok("a Reference runs without dbt State, any State call trapped by the run's proxy")
    inv = run.state_free[-1]
    if (inv["step"], inv["kind"], inv["exit"], inv["state_rpcs"]) != (1, "Reference", 0, 0) \
            or "/validated/dir" in inv["argv"] or "<profiles-dir>" not in inv["argv"] \
            or "--no-manage-state" not in inv["argv"]:
        fail("a Reference's invocation is recorded, sanitized, with its State RPC count", inv)
    ok("a Reference's invocation is recorded, sanitized, with its State RPC count")
    if "--no-manage-state" not in cmd:
        fail("a Reference disables dbt State on the command line", cmd)
    ok("a Reference disables dbt State on the command line (project or user settings cannot enable it)")

    class _Rc1:
        returncode = 1

    ec.subprocess.run = lambda cmd, **kw: _Rc1()
    try:
        expect_refusal("a failed Reference stops the run", lambda: run.run_step(1, fb_.steps[1]))
    finally:
        ec.subprocess.run = real_run

    # Any State RPC the proxy records during a State-free step fails the run.
    seen = [[], [{"method": "ValidateClientVersion"}]]
    run.rows = lambda: seen.pop(0) if len(seen) > 1 else seen[0]
    ec.subprocess.run = lambda cmd, **kw: _Rc0()
    try:
        expect_refusal("a Reference during which the proxy saw a State RPC stops the run",
                       lambda: run.run_step(1, fb_.steps[1]))
    finally:
        ec.subprocess.run = real_run
    if run.state_free[-1]["state_rpcs"] != 1:
        fail("the trapped State RPC is counted in the invocation record", run.state_free[-1])
    ok("the trapped State RPC is counted in the invocation record")

# A scenario of only Write, Read and Reference steps is State-free: its capture
# must hold no RPC at all, and it needs no ValidateClientVersion call.
FREE = ec.Scenario("free", "", ("fa",), [
    ec.Write("models/${fa}.sql", "select 1 as id\n"),
    ec.Reference("${fa}", target="dev_clone"),
    ec.Read("CDO_DEMO_DB.DEV_CLONE_DEMO.${fa}", target="dev_clone"),
])
fp2 = ec.plan(ec.bind(FREE, RUN))
if not fp2.state_free or ec.plan(fb_).state_free:
    fail("a plan is State-free only without Build, Sql and Explain steps", (fp2, ec.plan(fb_)))
ok("a plan is State-free only without Build, Sql and Explain steps")
if ec.capture_problem(fp2, [], {"wire_dbt_run_cache_versions": []}) is not None:
    fail("an empty capture is the expected result of a State-free scenario", fp2)
ok("an empty capture is the expected result of a State-free scenario")
if ec.capture_problem(fp2, [{"method": "SubmitTelemetryBatch"}], {"wire_dbt_run_cache_versions": []}) is None:
    fail("a State-free scenario that recorded an RPC is a failed capture", fp2)
ok("a State-free scenario that recorded an RPC is a failed capture")
if ec.capture_problem(ec.plan(fb_), [], {"wire_dbt_run_cache_versions": []}) is None:
    fail("a State scenario still needs a ValidateClientVersion call", fb_)
ok("a State scenario still needs a ValidateClientVersion call")

# --- client version (#191) ----------------------------------------------------------------
if ec.parse_dbt_version("       dbt 2.0.11\n") != "2.0.11":
    fail("the dbt version is parsed from `dbt --version`", ec.parse_dbt_version("dbt 2.0.11"))
ok("the dbt version is parsed from `dbt --version`")
wire = [{"method": "ValidateClientVersion", "request": {"dbt_run_cache_version": "2.0.11"}},
        {"method": "SubmitEnrichedSQL", "request": {}},
        {"method": "ValidateClientVersion", "request": {"dbt_run_cache_version": "2.0.11"}}]
if ec.wire_client_versions(wire) != ["2.0.11"]:
    fail("the client versions sent on the wire are collected", ec.wire_client_versions(wire))
ok("the client versions sent on the wire are collected")
rec = ec.client_record("dbt 2.0.11\n", "/bin/dbt", "ab" * 32, wire)
if rec != {"dbt_version": "2.0.11", "dbt_path": "/bin/dbt", "dbt_sha256": "ab" * 32,
           "wire_dbt_run_cache_versions": ["2.0.11"]}:
    fail("the client record names the executable and the wire version", rec)
ok("the client record names the executable and the wire version")
expect_refusal("a client record whose wire version differs from the executable's is refused",
               lambda: ec.client_record("dbt 2.0.12\n", "/bin/dbt", "ab" * 32, wire))


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

# A decision hosted makes on the speculative call (#103) is the node's current
# decision: the client sends no ordinary submit after it.
spec = [list(c) for c in per_step]
spec_skip = submit(view, "skip_execution", "e4", et=10)
spec_skip["method"] = "SubmitEnrichedSQLSpeculative"
spec[4] = [submit(up, "skip_execution", "e3"), spec_skip]
res = run_oracle(spec)
if not res[1].ok or res[1].decision != "SKIP":
    fail("a speculative SKIP is the node's current decision", res[1])
ok("a speculative SKIP is the node's current decision")

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
    if p.total <= 0 or not (p.state_free or any(isinstance(s, ec.Explain) for s in b.steps)):
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
