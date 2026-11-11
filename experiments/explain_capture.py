#!/usr/bin/env python3
"""Shared live Explain capture driver (#72, spec #40).

Runs one declarative scenario (`experiments/capture_scenarios/<name>.py`)
against hosted dbt State through a recording proxy that this run starts and
stops itself:

1. checks that every target the scenario uses resolves to Snowflake
   `CDO_DEMO_DB`, that the dbt Cloud credential exists and that the chosen
   proxy port is free, then prints the dbt invocation count;
2. clones `~/projects/jaffle-shop` into the run's work directory (plus its
   `dbt_packages/`), so the shared checkout is never written;
3. starts `target/debug/record-proxy` on the chosen port, with its own
   `GOLDEN_DIR` and log file under the work directory;
4. runs the scenario steps: writes temporary nodes with per-run unique names
   (`<logical>_<HHMMSS><6 random hex>`), runs SQL on per-run objects,
   builds, and explains one node per `dbt state explain` call;
5. slices the captured window, runs the oracle (every Explain step explained
   the node's current decision, over a confirmed build in the window unless
   the step says `history=False`, with the intended decision and kind), and
   writes a trimmed fixture candidate;
6. stops only its own proxy and deletes its project copy.

Usage:
  explain_capture.py SCENARIO --port PORT [--work-dir DIR] [--dry-run] [--keep-project]
  explain_capture.py --list

Build the proxy first: `cargo build -p dbt-state-harness --bin record-proxy`.
Self-test: python3 experiments/explain_capture_test.py
How to add a scenario and run campaigns concurrently: docs/harness.md.
"""
import argparse
import importlib.util
import json
import os
import posixpath
import re
import secrets
import shutil
import signal
import socket
import string
import subprocess
import sys
import tempfile
import time
from collections import Counter
from dataclasses import dataclass, field, replace

# Scenario modules import this module by name; run as a script, make that the same module.
sys.modules.setdefault("explain_capture", sys.modules[__name__])

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SCENARIO_DIR = os.path.join(HERE, "capture_scenarios")
PROXY_BIN = os.path.join(REPO, "target", "debug", "record-proxy")
DEMO_DB = "CDO_DEMO_DB"
DEMO_SCHEMAS = {"PROD_STATE_DEMO", "DEV_CLONE_DEMO"}
PROFILE = "snowflake"

# Methods the generic conformance replay handles (crates/harness/tests/conformance.rs).
# Anything else in a promoted fixture makes the replay panic.
REPLAYED_METHODS = {
    "SubmitEnrichedSQL", "SubmitValues", "ConfirmExecution", "ValidateClientVersion",
    "SubmitEnrichedSQLSpeculative", "GetExplainMessages", "RegisterClone",
    "ResolveDeferredRelations", "RecordExecutions",
}
DECISION = {0: "SKIP", 1: "EXECUTE", 3: "CLONE", 4: "UNKNOWN"}
EXECUTION_TYPE = {
    0: "UNSPECIFIED", 1: "FULL", 2: "APPEND", 3: "MERGE", 4: "INSERT_OVERWRITE",
    5: "DELETE_INSERT", 6: "MICROBATCH", 7: "SNAPSHOT", 8: "DBT_DATA_TEST",
    9: "VALUES", 10: "VIEW", 11: "DBT_CUSTOM",
}


class CaptureError(Exception):
    """The driver refuses to run, or a step failed."""


# --- scenario language ---------------------------------------------------------
#
# Every string field is a `string.Template`: `${<logical name>}` becomes the
# per-run physical name and `${run}` the run id. Jinja braces pass through.


def _sub(text, mapping, where):
    try:
        return string.Template(text).substitute(mapping)
    except (KeyError, ValueError) as e:
        raise CaptureError(f"{where}: undeclared or malformed name {e} in {text!r}") from None


@dataclass
class Write:
    """Write (or overwrite) a file in the project copy. The path must name a per-run node."""
    path: str
    text: str

    def bind(self, m, run):
        path = _sub(self.path, m, "Write path")
        norm = posixpath.normpath(path)
        if norm.startswith("/") or norm.startswith(".."):
            raise CaptureError(f"Write path escapes the project: {path}")
        if run not in norm:
            raise CaptureError(f"Write path does not name a per-run node: {path}")
        return replace(self, path=norm, text=_sub(self.text, m, f"Write {path}"))


@dataclass
class Remove:
    """Delete a per-run file from the project copy."""
    path: str

    def bind(self, m, run):
        w = Write(self.path, "").bind(m, run)
        return replace(self, path=w.path)


@dataclass(init=False)
class Sql:
    """Run statements on per-run CDO_DEMO_DB objects through one `dbt run-operation`."""
    statements: tuple
    target: str = "prod_demo"

    def __init__(self, *statements, target="prod_demo"):
        self.statements = tuple(statements)
        self.target = target

    def bind(self, m, run):
        stmts = tuple(_sub(s, m, "Sql") for s in self.statements)
        for s in stmts:
            check_sql(s, run)
        return Sql(*stmts, target=self.target)


@dataclass(init=False)
class Build:
    """One `dbt build --select ...` (optionally `--full-refresh` and `BUILD_FLAGS`).

    `test_failures=True` when a selected data test is meant to fail: dbt then
    exits 1, which this step accepts (any other non-zero exit still stops the
    run). Check the step's log for which nodes failed.
    """
    select: tuple
    target: str = "prod_demo"
    full_refresh: bool = False
    test_failures: bool = False
    args: tuple = ()

    def __init__(self, *select, target="prod_demo", full_refresh=False, test_failures=False,
                 args=()):
        self.select = tuple(select)
        self.target = target
        self.full_refresh = full_refresh
        self.test_failures = test_failures
        self.args = tuple(args)

    def bind(self, m, run):
        sel = tuple(_sub(s, m, "Build select") for s in self.select)
        for s in sel:
            if run not in s or s.startswith("+"):
                raise CaptureError(f"Build selects outside the run's nodes: {s}")
        args = tuple(_sub(a, m, "Build args") for a in self.args)
        flags, values = args[0::2], args[1::2]
        if len(flags) != len(values):
            raise CaptureError(f"Build flag without a value: {args}")
        for f, v in zip(flags, values):
            if f not in BUILD_FLAGS or v.startswith("-"):
                raise CaptureError(f"Build flag {f} {v!r} is not one of {sorted(BUILD_FLAGS)} "
                                   "with a value")
        return Build(*sel, target=self.target, full_refresh=self.full_refresh,
                     test_failures=self.test_failures, args=args)


# Extra `dbt build` flags a scenario may pass, each followed by one value: the
# microbatch event-time window (#73). They select which batches run, nothing else.
BUILD_FLAGS = {"--event-time-start", "--event-time-end"}


@dataclass
class Explain:
    """One `dbt state explain --verbose --select SELECT`, and the oracle's intent for it.

    `node` is the `labels.dbt_node_name` whose current decision must be the one
    explained (defaults to `select`; set it for data tests selected by fqn).
    `decision` (SKIP / EXECUTE / CLONE) and `execution_type` are the intended
    cell; None means "record whatever hosted decides". `history=False` for a
    first build, which has no State history by definition.
    """
    select: str
    intent: str
    node: str = None
    decision: str = None
    execution_type: int = None
    history: bool = True
    target: str = "prod_demo"

    def bind(self, m, run):
        sel = _sub(self.select, m, "Explain select")
        node = _sub(self.node, m, "Explain node") if self.node else sel
        if run not in sel or run not in node:
            raise CaptureError(f"Explain selects outside the run's nodes: {sel}")
        return replace(self, select=sel, node=node)


@dataclass
class Scenario:
    name: str
    summary: str
    names: tuple
    steps: list


@dataclass
class Bound:
    scenario: Scenario
    run: str
    names: dict
    steps: list


@dataclass
class Plan:
    builds: int
    run_operations: int
    explains: int
    targets: set

    @property
    def total(self):
        return self.builds + self.run_operations + self.explains


def new_run_id():
    return time.strftime("%H%M%S") + secrets.token_hex(3)


def bind(scenario, run):
    names = {n: f"{n}_{run}" for n in scenario.names}
    mapping = dict(names, run=run)
    return Bound(scenario, run, names, [s.bind(mapping, run) for s in scenario.steps])


def plan(bound):
    steps = bound.steps
    return Plan(
        builds=sum(isinstance(s, Build) for s in steps),
        run_operations=sum(isinstance(s, Sql) for s in steps),
        explains=sum(isinstance(s, Explain) for s in steps),
        targets={s.target for s in steps if isinstance(s, (Build, Sql, Explain))},
    )


def scenario_names():
    return sorted(f[:-3] for f in os.listdir(SCENARIO_DIR)
                  if f.endswith(".py") and not f.startswith("_"))


def load_scenario(name):
    path = os.path.join(SCENARIO_DIR, f"{name}.py")
    if not os.path.exists(path):
        raise CaptureError(f"no scenario {name!r}; known: {', '.join(scenario_names())}")
    spec = importlib.util.spec_from_file_location(f"capture_scenarios.{name}", path)
    mod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, HERE)
    spec.loader.exec_module(mod)
    return mod.SCENARIO


# --- safety checks ----------------------------------------------------------------


def check_profile(path, profile, targets):
    """Every target must be a Snowflake output whose database is literally CDO_DEMO_DB."""
    import yaml  # PyYAML; only needed for live runs and this check

    try:
        with open(path) as fh:
            doc = yaml.safe_load(fh) or {}
    except OSError as e:
        raise CaptureError(f"cannot read profiles: {e}") from None
    outputs = (doc.get(profile) or {}).get("outputs")
    if not outputs:
        raise CaptureError(f"profile {profile!r} has no outputs in {path}")
    for t in sorted(targets):
        out = outputs.get(t)
        if not out:
            raise CaptureError(f"profile {profile!r} has no target {t!r}")
        if out.get("type") != "snowflake":
            raise CaptureError(f"target {t!r} is {out.get('type')!r}, not snowflake")
        db = str(out.get("database", ""))
        if db.upper() != DEMO_DB:
            raise CaptureError(f"target {t!r} database is {db!r}, not {DEMO_DB}")


def profiles_dir(path):
    """The directory every dbt invocation loads profiles from: the validated file's own.

    dbt reads `profiles.yml` from `--profiles-dir`, so the validated file must
    have that name; otherwise dbt would discover another file (the project
    root's first, then `~/.dbt/`).
    """
    path = os.path.abspath(path)
    if os.path.basename(path) != "profiles.yml":
        raise CaptureError(f"--profiles must name a file called profiles.yml, not {path}")
    return os.path.dirname(path)


def check_port_free(port):
    with socket.socket() as s:
        s.settimeout(1)
        if s.connect_ex(("127.0.0.1", port)) == 0:
            raise CaptureError(f"127.0.0.1:{port} is already in use; pick a port of your own")


_FORBIDDEN = re.compile(
    r"\b(database|schema|grant|revoke|role|user|warehouse|account|call|execute|use|alter|identifier)\b",
    re.I)
# Lexer: string literals, quoted identifiers, bare identifiers, numbers, then any one character.
_TOKEN = re.compile(r"""\s+|'(?:[^'\\]|\\.)*'|"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*|\d+(?:\.\d+)?|.""", re.S)
# Statement heads; each is followed by the one relation the statement writes.
_HEADS = (("create", "or", "replace", "table"), ("create", "table"), ("insert", "into"),
          ("delete", "from"), ("update",), ("truncate", "table"), ("drop", "table", "if", "exists"),
          ("drop", "table"))


def _is_ident(tok):
    return tok[0] == '"' or tok[0].isalpha() or tok[0] == "_"


def _relations(statement):
    """Lex `statement` into tokens and dotted name chains; refuse what the check cannot see through."""
    if "{" in statement or "}" in statement:
        # The run-operation macro embeds statements verbatim, so Jinja would render them, even
        # inside a string literal.
        raise CaptureError(f"statement contains Jinja braces: {statement!r}")
    toks, code = [], []  # code: the statement with string literals blanked
    for m in _TOKEN.finditer(statement):
        t = m.group()
        if t == "'":
            raise CaptureError(f"statement has an unterminated string: {statement!r}")
        code.append("''" if t[0] == "'" else t)
        if not t.isspace():
            toks.append(t)
    code = "".join(code)
    for bad in (";", "--", "/*", "//", "$$"):
        if bad in code:
            raise CaptureError(f"statement contains {bad!r} (a second statement or a comment): "
                               f"{statement!r}")
    # Join `ident . ident . ident` (whitespace allowed around the dots) into one chain.
    chains, i = [], 0
    while i < len(toks):
        if toks[i] == ".":
            raise CaptureError(f"statement has a dot outside a name: {statement!r}")
        if not _is_ident(toks[i]):
            i += 1
            continue
        parts, j = [toks[i]], i + 1
        while j + 1 < len(toks) and toks[j] == "." and _is_ident(toks[j + 1]):
            parts.append(toks[j + 1])
            j += 2
        if j < len(toks) and toks[j] == ".":
            raise CaptureError(f"statement has a dot outside a name: {statement!r}")
        chains.append((i, parts))
        i = j
    return toks, chains


def _demo_object(parts, run):
    """A three-part per-run object in a demo schema. Quoted parts are case-sensitive in Snowflake."""
    if len(parts) != 3:
        return False
    db, schema, obj = (p[1:-1] if p[0] == '"' else p.upper() for p in parts)
    return db == DEMO_DB and schema in DEMO_SCHEMAS and run.upper() in obj.upper()


def check_sql(statement, run):
    """A statement may only touch three-part per-run objects in the CDO_DEMO_DB demo schemas.

    It must be one of the `_HEADS` forms followed by the per-run relation it
    writes, and every other dotted name in it must also be a per-run demo
    object. Unqualified names (columns, functions, reads of current-schema
    tables) are allowed; Jinja, comments, multiple statements and two- or
    four-part names are refused.
    """
    if run.lower() not in statement.lower():
        raise CaptureError(f"statement does not name a per-run object: {statement!r}")
    if _FORBIDDEN.search(statement):
        raise CaptureError(f"statement uses a forbidden keyword: {statement!r}")
    toks, chains = _relations(statement)
    for _, parts in chains:
        if len(parts) > 1 and not _demo_object(parts, run):
            name = ".".join(parts)
            raise CaptureError(f"statement touches {name}, outside the run's demo objects")
    lower = [t.lower() for t in toks]
    for head in _HEADS:
        if tuple(lower[:len(head)]) == head:
            dest = next((p for i, p in chains if i == len(head)), None)
            if dest is None or not _demo_object(dest, run):
                raise CaptureError(f"statement does not write a three-part per-run demo object: "
                                   f"{statement!r}")
            return
    raise CaptureError(f"statement form is not one of create table / insert / update / delete / "
                       f"truncate / drop table: {statement!r}")


# --- oracle ----------------------------------------------------------------------------


@dataclass
class OracleResult:
    step: int
    node: str
    intent: str
    ok: bool
    problems: list = field(default_factory=list)
    decision: str = None
    execution_type: int = None
    description: str = None
    lines: list = None
    edid: str = None


def _variant(row):
    resp = (row.get("response") or {})
    if row.get("method") == "RegisterClone":
        for v in ("ready_to_clone", "unable_to_clone"):
            if resp.get(v):
                return v, resp[v]
        return None, {}
    inner = resp.get("response") or {}
    v = next(iter(inner), None)
    return v, (inner.get(v) or {})


def _mints(rows):
    """edid -> (row index, node name, execution type, variant, request_id) for every minting row."""
    out = {}
    for i, r in enumerate(rows):
        m = r.get("method")
        if m not in ("SubmitEnrichedSQL", "SubmitValues", "RegisterClone"):
            continue
        req = r.get("request") or {}
        variant, inner = _variant(r)
        edid = inner.get("execution_decision_id")
        if not edid:
            continue
        et = 9 if m == "SubmitValues" else req.get("execution_type")
        node = (req.get("labels") or {}).get("dbt_node_name", "")
        out[edid] = (i, node, et, variant, inner.get("request_id"))
    return out


def _render(lines, depth=0, out=None):
    sys.path.insert(0, HERE)
    import explain_slice  # noqa: E402

    return explain_slice.render_lines(lines, depth, out)


def oracle(bound, rows, marks):
    """Check every Explain step. `marks[i]` is the row count after bound step i."""
    mints = _mints(rows)
    confirms = {}
    for i, r in enumerate(rows):
        # Only an accepted confirmation records a build; hosted answers `success: true`.
        if r.get("method") == "ConfirmExecution" and (r.get("response") or {}).get("success") is True:
            confirms.setdefault((r.get("request") or {}).get("request_id"), []).append(i)
    results = []
    for k, step in enumerate(bound.steps):
        if not isinstance(step, Explain):
            continue
        lo, hi = (marks[k - 1] if k else 0), marks[k]
        res = OracleResult(step=k, node=step.node, intent=step.intent, ok=False)
        results.append(res)
        calls = [r for r in rows[lo:hi] if r.get("method") == "GetExplainMessages"]
        if not calls:
            res.problems.append("no GetExplainMessages captured for this step")
            continue
        # The node's current decision: its latest minting row before this step.
        current = max((v for v in mints.values() if v[1] == step.node and v[0] < lo),
                      default=None, key=lambda v: v[0])
        if current is None:
            res.problems.append(f"no submit for {step.node} before this step")
            continue
        cur_edid = next(e for e, v in mints.items() if v == current)
        asked = {i for c in calls for i in (c.get("request") or {}).get("execution_decision_ids", [])}
        if cur_edid not in asked:
            res.problems.append(f"current decision {cur_edid} of {step.node} not explained "
                                f"(explained {sorted(asked)})")
            continue
        msg = next((m for c in calls for m in (c.get("response") or {}).get("messages", []) or []
                    if m.get("execution_decision_id") == cur_edid), None)
        if msg is None:
            res.problems.append(f"hosted returned no message for {cur_edid}")
            continue
        res.edid = cur_edid
        res.decision = DECISION.get(msg.get("decision"), msg.get("decision"))
        res.execution_type = current[2]
        res.description = msg.get("decision_description")
        res.lines = _render(msg.get("explain_lines"))
        if step.history:
            built = [v for v in mints.values()
                     if v[1] == step.node and v[0] < current[0]
                     and v[3] in ("ready_to_execute", "ready_to_clone") and v[4]
                     and any(v[0] < c < current[0] for c in confirms.get(v[4], []))]
            if not built:
                res.problems.append(f"no confirmed build of {step.node} before the explained "
                                    "decision (not explained over State history)")
        if step.decision and res.decision != step.decision:
            res.problems.append(f"intent missed: decision {res.decision}, wanted {step.decision}")
        if step.execution_type is not None and res.execution_type != step.execution_type:
            res.problems.append(f"intent missed: execution_type {res.execution_type}, "
                                f"wanted {step.execution_type}")
        res.ok = not res.problems
    return results


def trim(rows):
    kept = [r for r in rows if r.get("method") in REPLAYED_METHODS]
    dropped = Counter(r.get("method") for r in rows if r.get("method") not in REPLAYED_METHODS)
    return kept, dropped


# --- live run ----------------------------------------------------------------------------


class Run:
    def __init__(self, bound, port, work, shared, keep_project, profiles_dir):
        self.bound, self.port, self.work = bound, port, work
        self.shared, self.keep_project = shared, keep_project
        self.profiles_dir = profiles_dir
        self.project = os.path.join(work, "jaffle-shop")
        self.golden_dir = os.path.join(work, "golden")
        self.logs = os.path.join(work, "dbt")
        self.proxy = None
        self.invocations = Counter()

    # environment
    def client_env(self):
        env = dict(os.environ)
        env.update(DBT_ENGINE_MANAGE_STATE="true", RUN_CACHE_API_URL=f"127.0.0.1:{self.port}",
                   RUN_CACHE_API_SECURE="false")
        env.pop("PROBE_URL", None)
        return env

    def copy_project(self):
        subprocess.run(["git", "clone", "-q", self.shared, self.project], check=True)
        pkgs = os.path.join(self.shared, "dbt_packages")
        if os.path.isdir(pkgs):
            shutil.copytree(pkgs, os.path.join(self.project, "dbt_packages"), symlinks=True)

    def start_proxy(self):
        os.makedirs(self.golden_dir, exist_ok=True)
        env = dict(os.environ, PROXY_LISTEN=f"127.0.0.1:{self.port}",
                   GOLDEN_DIR=self.golden_dir, RUST_LOG="warn")
        log = open(os.path.join(self.work, "proxy.log"), "w")
        self.proxy = subprocess.Popen([PROXY_BIN], env=env, stdout=log, stderr=subprocess.STDOUT,
                                      start_new_session=True)
        with open(os.path.join(self.work, "proxy.pid"), "w") as fh:
            fh.write(str(self.proxy.pid))
        for _ in range(60):
            if self.proxy.poll() is not None:
                raise CaptureError(f"record-proxy exited {self.proxy.returncode}; see {log.name}")
            with socket.socket() as s:
                s.settimeout(1)
                if s.connect_ex(("127.0.0.1", self.port)) == 0:
                    print(f"record-proxy pid {self.proxy.pid} listening on 127.0.0.1:{self.port}")
                    return
            time.sleep(1)
        raise CaptureError(f"record-proxy did not listen on {self.port} within 60 s")

    def stop_proxy(self):
        if not self.proxy or self.proxy.poll() is not None:
            return
        self.proxy.send_signal(signal.SIGTERM)
        try:
            self.proxy.wait(10)
        except subprocess.TimeoutExpired:
            self.proxy.kill()
            self.proxy.wait()

    def golden_file(self):
        files = sorted(f for f in os.listdir(self.golden_dir) if f.startswith("golden_"))
        if len(files) > 1:
            raise CaptureError(f"more than one golden file in {self.golden_dir}")
        return os.path.join(self.golden_dir, files[0]) if files else None

    def rows(self):
        gf = self.golden_file()
        if not gf:
            return []
        with open(gf) as fh:
            return [json.loads(line) for line in fh if line.strip()]

    # steps
    def dbt(self, k, label, args):
        log = os.path.join(self.logs, f"{k:02d}_{label}.log")
        cmd = ["dbt", *args, "--profiles-dir", self.profiles_dir, "--profile", PROFILE,
               "--skip-semantic-manifest-validation"]
        print(f"  $ {' '.join(cmd)}")
        with open(log, "w") as fh:
            rc = subprocess.run(cmd, cwd=self.project, env=self.client_env(),
                                stdout=fh, stderr=subprocess.STDOUT).returncode
        self.invocations[label] += 1
        return rc, log

    def run_step(self, k, step):
        if isinstance(step, Write):
            dest = os.path.join(self.project, step.path)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "w") as fh:
                fh.write(step.text)
            print(f"  wrote {step.path}")
        elif isinstance(step, Remove):
            os.remove(os.path.join(self.project, step.path))
            print(f"  removed {step.path}")
        elif isinstance(step, Sql):
            name = f"_cap_{self.bound.run}"
            body = "".join(
                f"{{% set s{i} %}}{s}{{% endset %}}{{% do run_query(s{i}) %}}\n"
                for i, s in enumerate(step.statements))
            macro = (f"{{% macro {name}() %}}\n"
                     f"{{% if target.database | upper != '{DEMO_DB}' %}}"
                     f"{{{{ exceptions.raise_compiler_error('refusing: target database ' ~ target.database) }}}}"
                     f"{{% endif %}}\n{body}"
                     f"{{% do log('ran {len(step.statements)} statement(s)', info=true) %}}\n"
                     f"{{% endmacro %}}\n")
            path = os.path.join(self.project, "macros", f"{name}.sql")
            with open(path, "w") as fh:
                fh.write(macro)
            try:
                rc, log = self.dbt(k, "run-operation", ["run-operation", name, "--target", step.target])
            finally:
                os.remove(path)
            if rc:
                raise CaptureError(f"run-operation exited {rc}; see {log}")
        elif isinstance(step, Build):
            args = ["build", "--target", step.target, "--select", *step.select]
            if step.full_refresh:
                args.append("--full-refresh")
            args.extend(step.args)
            rc, log = self.dbt(k, "build", args)
            if rc and not (rc == 1 and step.test_failures):
                raise CaptureError(f"dbt build exited {rc}; see {log}")
            if rc:
                print(f"  (dbt build exited 1 with failing data tests expected; see {log})")
        elif isinstance(step, Explain):
            rc, log = self.dbt(k, "explain", ["state", "explain", "--verbose", "--target",
                                              step.target, "--select", step.select])
            if rc:
                print(f"  (dbt state explain exited {rc}; see {log})")
        time.sleep(0.2)


def shared_status(path):
    return subprocess.run(["git", "-C", path, "status", "--porcelain"], check=True,
                          capture_output=True, text=True).stdout


def off_demo_submits(rows):
    """SQL submits whose target is outside CDO_DEMO_DB. A data test has no
    target (`target_table: null`) and is not one."""
    return [r for r in rows if r.get("method") == "SubmitEnrichedSQL"
            and (r.get("request") or {}).get("target_table")
            and not r["request"]["target_table"].upper().startswith(f'"{DEMO_DB}".')]


def report(bound, rows, marks, work):
    sys.path.insert(0, HERE)
    import explain_slice  # noqa: E402

    with open(os.path.join(work, "window.jsonl"), "w") as fh:
        fh.writelines(json.dumps(r) + "\n" for r in rows)
    kept, dropped = trim(rows)
    cand = os.path.join(work, "fixture_candidate.jsonl")
    with open(cand, "w") as fh:
        fh.writelines(json.dumps(r) + "\n" for r in kept)
    with open(os.path.join(work, "steps.json"), "w") as fh:
        json.dump({"scenario": bound.scenario.name, "run": bound.run, "names": bound.names,
                   "steps": [{"kind": type(s).__name__, **vars(s), "rows_after": m}
                             for s, m in zip(bound.steps, marks)]}, fh, indent=1, default=list)
    explain_slice.slice_window(rows)
    off_demo = off_demo_submits(rows)
    print(f"## fixture candidate: {cand} ({len(kept)} rows; dropped {dict(dropped)})")
    results = oracle(bound, rows, marks)
    print("## ORACLE (each Explain step explained the node's current decision over State history)")
    for r in results:
        kind = EXECUTION_TYPE.get(r.execution_type, r.execution_type)
        print(f"  [{'PASS' if r.ok else 'FAIL'}] step {r.step} {r.node} — {r.intent}: "
              f"{r.decision} {kind} {r.description!r}")
        for p in r.problems:
            print(f"      problem: {p}")
        for line in r.lines or ([] if r.lines is None else ["(empty tree)"]):
            print(f"      {line}")
    if off_demo:
        print(f"  [FAIL] {len(off_demo)} submit(s) targeted outside {DEMO_DB}")
    return all(r.ok for r in results) and not off_demo


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario", nargs="?")
    ap.add_argument("--list", action="store_true", help="list bundled scenarios")
    ap.add_argument("--port", type=int, help="port for this run's own record-proxy")
    ap.add_argument("--work-dir", help="empty or new directory for the copy, capture and logs")
    ap.add_argument("--jaffle-shop", default=os.path.expanduser("~/projects/jaffle-shop"))
    ap.add_argument("--profiles", default=os.path.expanduser("~/.dbt/profiles.yml"))
    ap.add_argument("--dry-run", action="store_true", help="check and print the plan, run nothing")
    ap.add_argument("--keep-project", action="store_true", help="keep the project copy afterwards")
    a = ap.parse_args(argv)
    if a.list:
        for n in scenario_names():
            print(f"{n}: {load_scenario(n).summary}")
        return 0
    if not a.scenario or not a.port:
        ap.error("SCENARIO and --port are required")

    bound = bind(load_scenario(a.scenario), new_run_id())
    p = plan(bound)
    print(f"scenario {bound.scenario.name}: {bound.scenario.summary}")
    print(f"run id {bound.run}; nodes: {', '.join(bound.names.values())}")
    print(f"dbt invocations: {p.total} ({p.builds} build, {p.run_operations} run-operation, "
          f"{p.explains} state explain); targets: {', '.join(sorted(p.targets))}")

    try:
        profiles = profiles_dir(a.profiles)
        check_profile(a.profiles, PROFILE, p.targets)
        if not os.path.exists(os.path.expanduser("~/.dbt/dbt_cloud.yml")):
            raise CaptureError("~/.dbt/dbt_cloud.yml is missing (the proxy needs the dbt Cloud credential)")
        check_port_free(a.port)
        if not os.access(PROXY_BIN, os.X_OK):
            raise CaptureError(f"{PROXY_BIN} missing; run `cargo build -p dbt-state-harness --bin record-proxy`")
        if not os.path.isdir(os.path.join(a.jaffle_shop, ".git")):
            raise CaptureError(f"{a.jaffle_shop} is not a git checkout")
    except CaptureError as e:
        print(f"REFUSED: {e}")
        return 2
    if a.dry_run:
        print("dry run: checks passed, nothing run")
        return 0

    work = a.work_dir or tempfile.mkdtemp(prefix=f"explain-capture-{bound.scenario.name}-")
    os.makedirs(work, exist_ok=True)
    if os.listdir(work):
        print(f"REFUSED: work dir {work} is not empty")
        return 2
    print(f"work dir {work}")
    before = shared_status(a.jaffle_shop)
    run = Run(bound, a.port, work, a.jaffle_shop, a.keep_project, profiles)
    os.makedirs(run.logs)
    marks, failure = [], None
    try:
        run.copy_project()
        run.start_proxy()
        for k, step in enumerate(bound.steps):
            print(f"step {k}: {type(step).__name__}")
            run.run_step(k, step)
            marks.append(len(run.rows()))
    except (CaptureError, subprocess.CalledProcessError, KeyboardInterrupt) as e:
        failure = e
        print(f"STOPPED at step {len(marks)}: {e!r}")
    finally:
        run.stop_proxy()
        if not run.keep_project and os.path.isdir(run.project):
            shutil.rmtree(run.project)
    marks += [marks[-1] if marks else 0] * (len(bound.steps) - len(marks))
    ok = report(bound, run.rows() if os.path.isdir(run.golden_dir) else [], marks, work)
    after = shared_status(a.jaffle_shop)
    if after != before:
        print(f"WARNING: {a.jaffle_shop} status changed during the run:\n{after}")
        ok = False
    spent = sum(run.invocations.values())
    print(f"dbt invocations run: {spent} of {p.total} planned ({dict(run.invocations)})")
    print(f"capture: {run.golden_file() if os.path.isdir(run.golden_dir) else None}")
    if failure or not ok:
        return 1
    print("CAPTURE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
