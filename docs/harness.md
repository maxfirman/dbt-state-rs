# Test harness

`crates/harness` captures real traffic between the official dbt client and the
hosted dbt State service (`api.state.dbt.com`), and provides tools to compare
this server against it. Wire semantics and the captured protocol facts live in
[protocol.md](protocol.md); test strategy and the replay rules live in
[testing.md](testing.md).

## Components

| Binary (`crates/harness/Cargo.toml`) | Source | Purpose |
|---|---|---|
| `record-proxy` | `src/bin/record_proxy.rs` | Recording proxy (below). |
| `state-probe` | `src/bin/state_probe.rs` | Sends one crafted RPC (`submit`, `values`, `confirm`, `explain`) read as golden-format JSON from stdin, through the proxy, and prints the response. Used by `experiments/lag_matrix.py`, `seed_upstream.py` and `ambiguous_rebuilds.py` for synthetic-epoch sequences that need no warehouse. |
| `diff-fuzz` | `src/bin/diff_fuzz.rs` | Opt-in differential discovery against the hosted service; needs `--features fuzz` ([testing.md](testing.md#hosted-discovery-differential-fuzz)). |

### Recording proxy

An insecure local tonic server for the SQL, Execution, Clone,
ClientValidation, Explain, SelectorService and ClientTelemetry services. The
dbt client points at it. Each call is forwarded over TLS to the hosted service
with the client's own metadata (`x-submitted-at-epoch`, `x-dbt-invocation-id`,
and so on; transport and auth headers excluded) plus a minted Bearer token. The
proxy returns the hosted response unchanged and appends one JSON line per call
to `golden_<timestamp>.jsonl`.

- `src/auth.rs` reads the active host and token from `~/.dbt/dbt_cloud.yml`,
  performs the dbt platform token exchange, caches the token with 60s expiry
  slack, and extracts the organization ID from the
  `runcache:scope:org:<ORG_ID>:...` scope.
- `src/golden.rs` defines `GoldenEntry` {`service`, `method`, `recorded_at`,
  `metadata`, `request`, `response`} and the append-only writer.
- Environment: `PROXY_LISTEN` (default `127.0.0.1:50099`), `PROXY_UPSTREAM`
  (default `https://api.state.dbt.com:443`), `GOLDEN_DIR` (default `./golden`).

Older fixtures were captured before the proxy forwarded client metadata, so
the hosted service did not receive those headers. Their recorded `metadata`
still shows what the client sent.

## Running a capture

```bash
cargo build -p dbt-state-harness
RUST_LOG=info GOLDEN_DIR=./golden PROXY_LISTEN=127.0.0.1:50099 ./target/debug/record-proxy

# In the dbt project (a Snowflake target; DuckDB does not trigger dbt State):
DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL=127.0.0.1:50099 RUN_CACHE_API_SECURE=false \
  dbt build --profile snowflake --target prod_demo --select <model> --skip-semantic-manifest-validation
```

To force a build, make a semantic SQL change such as adding a column. Comment
or whitespace edits are normalized away and still SKIP
([sql-normalization.md](sql-normalization.md)). For an incremental model,
`--full-refresh` makes the client bypass dbt State (no submit).

Promote a useful capture into `golden/fixtures/` and drive it from a test
([golden/README.md](../golden/README.md)).

## Experiment drivers (`experiments/`)

The drivers start from a clean `jaffle-shop` tree, run one focused scenario
against `--target prod_demo` or `dev_clone` (`defer_to_target: prod_demo`),
print the newly captured decisions and `git restore` the project. They only
ever mutate `CDO_DEMO_DB`.

| Area | Drivers |
|---|---|
| Proxy and generic runs | `proxy_up.sh` (start or restart the proxy; captures go to `$GOLDEN_DIR`, default `/tmp/conf_golden`), `run.sh LABEL SELECT [EDIT_PY] [EXTRA…]` |
| Result copy (clone) | `clone_experiment.sh LABEL {incremental\|table\|snapshot} [--fresh-source] [--no-clones]`, `clone_logic_sensitivity.sh`, `clone_eligibility.sh`, `clone_fallback_xenv.sh`, `clone_fallback_control.sh`, `clone_fallback_freshness.sh`, `clone_chain_provenance.sh`, `registerclone_source_age.sh`, `p0_fallback_over_existing.sh`, `physical_state_exec_type.sh` |
| Dependency and SQL identity | `finding3_*.sh`, `quoted_identifier_case.sh`, `resolution_context.sh` |
| Lag tolerance | `lag_experiment.sh LABEL {any\|all} LAG {none\|a\|b\|both}`, `lag_buildage.sh`, `lag_isolate.sh`, `lag_docs_example.sh` (`DIRECT=1` runs without the proxy as a control), `lag_matrix.py ARM…`, `copy_candidates.py`, `seed_upstream.py` |
| Data tests and templates | `data_test_freshness.sh`, `data_test_freshness_control.sh`, `data_test_template.sh`, `template_comparison.sh` |
| Selectors and deferral | `selector_experiment.sh {baseline\|query CRIT\|mutate CRIT EDIT\|all-clean}`, `selector_last_write_wins.sh`, `deferral_experiment.sh` |
| Explain | `explain_capture.py SCENARIO --port PORT` (the shared driver, [below](#shared-live-capture-driver-experimentsexplain_capturepy)), `explain_experiment.sh`, `explain_*_history.sh`, `explain_tolerance.sh`, `explain_clone_deferral.sh`, `explain_dependency_set_changed.sh`, `explain_dep_set_*.sh`, `ambiguous_rebuilds.py PROBE…` (crafted ambiguous Target relation probes d1–d4; point `PROBE_URL` at your own proxy); `explain_slice.py` renders captures; `explain_census.py [captures]` counts markers and badges per node kind × decision (tested by `explain_census_test.py`) |
| Inspection and validation | `golden_tail.py FILE N [PREFIX]`, `lag_show.py`, `lag_rule_check.py [files]`, `trace_validate.py` and `oracles.py` (correlate captured source, clone and confirm evidence; tested by `trace_validate_test.py`) |

`lag_experiment.sh` bumps upstreams by External modification of dbt-built
tables, which hosted treats as an upstream query change; `lag_matrix.py`
covers the representative Lag tolerance cases with crafted calls.

## Shared live capture driver (`experiments/explain_capture.py`)

The #40 live capture tickets add declarative scenarios to one driver instead of
copying shell scripts. Each scenario is a Python module in
`experiments/capture_scenarios/`. The driver starts its own recording proxy
and refuses to run unless every target the scenario uses is
a Snowflake output of the `snowflake` profile whose database is `CDO_DEMO_DB`,
`~/.dbt/dbt_cloud.yml` exists, and the chosen port is free. The profile is
read from `--profiles` (default `~/.dbt/profiles.yml`, and the file must be
named `profiles.yml`). Every dbt invocation gets that file's directory as
`--profiles-dir`, so dbt uses the checked file and not one found in the
project copy. It prints the dbt invocation count before it starts.

```bash
cargo build -p dbt-state-harness --bin record-proxy
python3 experiments/explain_capture.py --list
python3 experiments/explain_capture.py snapshot_explain --port 50172 --dry-run   # checks + plan only
python3 experiments/explain_capture.py snapshot_explain --port 50172 \
  --work-dir /tmp/spec40-72/run1
python3 experiments/explain_capture_test.py                                      # warehouse-free self-test
```

A run:

1. clones `~/projects/jaffle-shop` into `<work-dir>/jaffle-shop` and copies its
   `dbt_packages/`. The shared checkout is only read; the driver compares its
   `git status` before and after and fails the run if it changed;
2. starts `target/debug/record-proxy` on `127.0.0.1:<port>` with
   `GOLDEN_DIR=<work-dir>/golden` and log `<work-dir>/proxy.log`, and points
   the client at it (`RUN_CACHE_API_URL`, `RUN_CACHE_API_SECURE=false`,
   `DBT_ENGINE_MANAGE_STATE=true`);
3. runs the steps with `--profile snowflake --target <step target>`, one log
   per dbt call in `<work-dir>/dbt/`;
4. stops only its own proxy (by pid), deletes the project copy (keep it with
   `--keep-project`), and writes `window.jsonl` (the whole capture),
   `fixture_candidate.jsonl` (only the methods the conformance replay
   handles, including `RecordExecutions`; telemetry, `GetStateSelection` and
   the like are
   dropped and counted), `steps.json` (bound steps and row offsets) and the
   oracle report on stdout.

The oracle passes an `Explain` step when the step captured a
`GetExplainMessages` call for the node's current decision (its latest submit
before the step), hosted returned a message for it, the window holds a
confirmed build of the node before that decision (a build whose
ConfirmExecution hosted answered with `success: true`; unless
`history=False`),
and the decision and execution type match the step's intent when given. An
empty hosted tree is evidence, not a failure. The exit code is 0 only when
every step ran and every Explain step passed.

### Adding a scenario

Add `experiments/capture_scenarios/<name>.py` that defines `SCENARIO`
(see `snapshot_explain.py`):

```python
from explain_capture import Build, Explain, Remove, Scenario, Sql, Write

SCENARIO = Scenario(
    name="my_scenario",
    summary="one line for --list",
    names=("m1up", "m1node"),            # logical names; each run binds <name>_<run id>
    steps=[
        Write("models/marts/${m1up}.sql", "select 1 as id, '${run}' as v\n"),
        Write("models/marts/${m1node}.sql", "select id from {{ ref('${m1up}') }}\n"),
        Build("${m1up}", "${m1node}"),
        Explain("${m1node}", intent="first build", decision="EXECUTE",
                execution_type=1, history=False),
        Build("${m1up}", "${m1node}"),
        Explain("${m1node}", intent="unchanged SKIP", decision="SKIP"),
    ],
)
```

- Strings are `string.Template`s: `${<logical name>}` is the per-run name and
  `${run}` the run id (`HHMMSS` plus 6 random hex digits). Jinja braces pass
  through; write a literal `$` as `$$`.
- `Write(path, text)` / `Remove(path)`: the path must stay inside the project
  and contain the run id, so tracked jaffle-shop files are never edited.
- `Build(*select, target="prod_demo", full_refresh=False)`: every selector must
  name a per-run node and must not start with `+`, so shared nodes are never
  rebuilt.
- `Sql(*statements, target="prod_demo")`: one `dbt run-operation` of a
  temporary macro that refuses a target database other than `CDO_DEMO_DB`.
  Every statement must name only three-part per-run objects in
  `CDO_DEMO_DB.PROD_STATE_DEMO` or `CDO_DEMO_DB.DEV_CLONE_DEMO`. A statement
  must be `create [or replace] table`, `insert into`, `update`, `delete from`,
  `truncate table` or `drop table [if exists]` followed by the per-run
  relation it writes. Unqualified names are allowed; two- or four-part names,
  `IDENTIFIER(...)`, Jinja braces, comments and `;` are refused.
- `Explain(select, intent, node=None, decision=None, execution_type=None,
  history=True, target="prod_demo")`: one `dbt state explain --verbose` per
  node. Set `node` to the `dbt_node_name` when the selector is not the name
  (data tests: `test.jaffle_shop.<test>.*`). `decision=None` records whatever
  hosted decides.
- Use `target="dev_clone"` for steps against the dev target; it is checked
  like `prod_demo`.

`experiments/explain_capture_test.py` binds and plans every bundled scenario,
so a malformed one fails offline. Promote a capture by copying
`fixture_candidate.jsonl` into `golden/fixtures/` (see
[testing.md](testing.md#captured-traffic-replay)).

### Concurrent campaigns

Several runs can share the machine, the hosted organization and the demo
schemas when each uses its own port and work directory:

- give every run a distinct `--port` (the driver refuses a port already
  listening) and a distinct, empty `--work-dir`; the default is a fresh
  `mktemp` directory under `/tmp`;
- do not use `experiments/proxy_up.sh` while others run: it kills every
  `record-proxy` process. The driver never kills a process it did not start;
- per-run names with a random suffix keep hosted State history and Snowflake
  objects separate, and SQL steps are limited to the run's own objects;
- the dbt invocation counts of parallel runs add up against metering.

The older shell drivers below `cd` into the shared checkout and `git restore`
it, so they are not safe to run alongside another campaign.

## Live clone runs

Forcing the dbt client to take a result copy against this server live depends
on warehouse state (the client only asks to clone when the target relation is
absent). Replay of captured traffic is evidence for the captured cases only;
see [testing.md](testing.md#captured-traffic-replay).
