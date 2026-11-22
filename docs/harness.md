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
| `state-probe` | `src/bin/state_probe.rs` | Sends one crafted RPC (`submit`, `values`, `confirm`, `clone` for `Clone.RegisterClone`, `explain`, `validate`, `select`) read as golden-format JSON from stdin, through the proxy, and prints the response. Used by `experiments/lag_matrix.py`, `seed_upstream.py`, `ambiguous_rebuilds.py`, `incomplete_test_results.py`, `copy_logic_probes.py`, `malformed_requests.py`, `unknown_epoch_probes.py` and `rebuild_tolerance_line_probes.py` for synthetic-epoch sequences that need no warehouse. |
| `diff-fuzz` | `src/bin/diff_fuzz.rs` | Opt-in differential discovery against the hosted service; needs `--features fuzz` ([testing.md](testing.md#hosted-discovery-differential-fuzz)). |

### Recording proxy

An insecure local tonic server for the SQL, Execution, Clone,
ClientValidation, Explain, SelectorService and ClientTelemetry services. The
dbt client points at it. Each call is forwarded over TLS to the hosted service
with the client's own metadata (`x-submitted-at-epoch`, `x-dbt-invocation-id`,
and so on; transport and auth headers excluded) plus a minted Bearer token. The
proxy returns the hosted response unchanged and appends one JSON line per call
to `golden_<timestamp>_<session>.jsonl`.

- `src/auth.rs` reads the active host and token from `~/.dbt/dbt_cloud.yml`,
  performs the dbt platform token exchange, caches the token with 60s expiry
  slack, and extracts the organization ID from the
  `runcache:scope:org:<ORG_ID>:...` scope.
- `src/golden.rs` defines `GoldenEntry` {`service`, `method`, `recorded_at`,
  `metadata`, `request`, `response`, `status`, `response_metadata`} and the append-only writer.
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

Databricks uses this same recording proxy. Start with the
[connection setup](development.md#databricks-live-setup), then use the
[dedicated experiment drivers](#databricks-capability-campaign) for captures.

## Experiment drivers (`experiments/`)

The drivers start from a clean `jaffle-shop` tree, run one focused scenario
against `--target prod_demo` or `dev_clone` (`defer_to_target: prod_demo`),
print the newly captured decisions and `git restore` the project. They only
ever mutate `CDO_DEMO_DB`.

| Area | Drivers |
|---|---|
| Proxy and generic runs | `proxy_up.sh` (start or restart the proxy; captures go to `$GOLDEN_DIR`, default `/tmp/conf_golden`), `run.sh LABEL SELECT [EDIT_PY] [EXTRA…]` |
| Result copy (clone) | `clone_experiment.sh LABEL {incremental\|table\|snapshot} [--fresh-source] [--no-clones]`, `clone_logic_sensitivity.sh`, `clone_eligibility.sh`, `clone_fallback_xenv.sh`, `clone_fallback_control.sh`, `clone_fallback_freshness.sh`, `clone_chain_provenance.sh`, `registerclone_source_age.sh`, `p0_fallback_over_existing.sh`, `physical_state_exec_type.sh`, `upstream_origin.py {WORK_DIR [--json]\|extract OUT_DIR LABEL=SERVICE=WORK_DIR…}` (#237: the per-case record of an `explain_capture.py upstream_origin` or `upstream_origin_table` run, served rows against the Reference execution, and the per-case evidence extract; tested by `upstream_origin_test.py`), `copy_source_choice.py {plan\|run [HISTORY…]\|table [--certified] CAPTURE…}` (#239: crafted Snowflake histories in which an absent FULL destination could copy one of competing sources; through `PROBE_URL`; `run` and `table` exit 1 on a failed RPC, an inconclusive history or, with `--certified`, an uncertified capture; tested by `copy_source_choice_test.py`) |
| Dependency and SQL identity | `finding3_*.sh`, `quoted_identifier_case.sh`, `resolution_context.sh` |
| Lag tolerance | `lag_experiment.sh LABEL {any\|all} LAG {none\|a\|b\|both}`, `lag_buildage.sh`, `lag_isolate.sh`, `lag_docs_example.sh` (`DIRECT=1` runs without the proxy as a control), `lag_matrix.py ARM…`, `copy_candidates.py`, `seed_upstream.py` |
| Data tests and templates | `data_test_freshness.sh`, `data_test_freshness_control.sh`, `data_test_template.sh`, `template_comparison.sh`, `data_test_history.py {table CAPTURE…\|probes\|probes-order}` (#200: attributes each data-test SKIP's echoed runtime to the confirmed run it matches; crafted one-factor and same-key cells through `PROBE_URL`; tested by `data_test_history_test.py`) |
| Selectors and deferral | `selector_experiment.sh {baseline\|query CRIT\|mutate CRIT EDIT\|all-clean}`, `selector_last_write_wins.sh`, `deferral_experiment.sh` |
| Explain | `explain_capture.py SCENARIO --port PORT` (the shared driver, [below](#shared-live-capture-driver-experimentsexplain_capturepy)), `explain_experiment.sh`, `explain_*_history.sh`, `explain_tolerance.sh`, `explain_clone_deferral.sh`, `explain_dependency_set_changed.sh`, `explain_dep_set_*.sh`, `ambiguous_rebuilds.py PROBE…` (crafted ambiguous Target relation probes d1–d4; point `PROBE_URL` at your own proxy); `incomplete_test_results.py PROBE…` (crafted data-test confirms without complete results, #75; same); `copy_logic_probes.py PROBE…` (crafted RegisterClone copies and the rebuilds over them, #104; same); `malformed_requests.py PROBE…` (crafted malformed requests, #104; same); `unknown_epoch_probes.py PROBE…` (crafted upstreams with an unknown epoch, #104, #113; same); `rebuild_tolerance_line_probes.py PROBE…` (crafted data-only rebuilds past a 20 s Lag tolerance, with and without an equivalent elsewhere, #104; same); `explain_slice.py` renders captures; `explain_census.py [captures]` counts markers and badges per node kind × decision (tested by `explain_census_test.py`); `decision_rows.py` holds the wire names and minted-decision helpers both share |
| Databricks Volatile evaluation | `databricks_volatile.py` (#198: pinned native captures of fresh `rand()` tables, a control, a consumer and a singular data test, built twice and explained, with SQL API readbacks; [Databricks capability campaign](#databricks-capability-campaign) setup; tested by `databricks_volatile_test.py`), `databricks_volatile_priority.py --batch {response\|reuse-controls\|checked-reuse\|function-builds\|producer}` (#213: the prioritized cells through dbt 2.0.11, [characterization](reports/databricks-volatile-priority-2026-11-21.md)) |
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
   `fixture_candidate.jsonl` (the driver's decision/Explain subset,
   including `RecordExecutions`; telemetry, `GetStateSelection` and
   the like are
   dropped and counted), `steps.json` (bound steps and row offsets),
   `client.json` (the `dbt` executable's path, version and SHA-256, and every
   `dbt_run_cache_version` it sent; the run fails if they disagree or none was
   sent), `reads.json` (rows each `Read` step read back) and the oracle report
   on stdout.

The generic conformance replay also handles version validation and batched
telemetry. The separate [Databricks auxiliary runner](../experiments/databricks_auxiliary.py)
captures complete auxiliary-only real-client sessions for hosted/Rust checks;
it does not use this Snowflake driver's fixture filter.

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
  and contain the run id, so tracked jaffle-shop files are never edited. The
  one exception is `COPY_OVERRIDES` in the driver: a `Write` may replace
  `macros/generate_schema_name.sql` in the run's own project copy (the shared
  checkout is still never written), to place this run's nodes in another
  schema of `CDO_DEMO_DB` (`store_failures_audit_schema.py`, #106). It cannot
  be removed.
- `Build(*select, target="prod_demo", full_refresh=False, test_failures=False, args=())`:
  every selector must name a per-run node and must not start with `+`, so
  shared nodes are never rebuilt. Set `test_failures=True` when a selected
  data test is meant to fail: dbt then exits 1, which the step accepts (any
  other non-zero exit still stops the run; see `data_test_execute.py`). `args`
  adds flag/value pairs from `BUILD_FLAGS` only (`--event-time-start`,
  `--event-time-end`: the microbatch window); anything else is refused at bind
  time.
- `Sql(*statements, target="prod_demo")`: one `dbt run-operation` of a
  temporary macro that refuses a target database other than `CDO_DEMO_DB`.
  Every statement must name only three-part per-run objects in
  `CDO_DEMO_DB.PROD_STATE_DEMO` or `CDO_DEMO_DB.DEV_CLONE_DEMO`. A statement
  must be `create [or replace] table`, `insert into`, `update`, `delete from`,
  `truncate table`, `drop table [if exists]` or `drop view [if exists]`
  followed by the per-run relation it writes. Unqualified names are allowed; two- or four-part names,
  `IDENTIFIER(...)`, Jinja braces, comments and `;` are refused.
- `Read(relation, target="prod_demo")`: one `dbt show --no-manage-state --inline
  "select * from <relation>" --limit -1 --output json` with dbt State disabled.
  The relation must be a three-part per-run object in a demo schema of
  `CDO_DEMO_DB`. The rows go to `reads.json`, so a capture can compare what the
  Warehouse wrote with what the service returned (`volatile_evaluation.py`,
  #191).
- `Reference(*select, target="prod_demo")`: one `dbt build --no-manage-state
  --full-refresh --select …` with dbt State disabled (no metering): a
  Reference execution of per-run nodes. Each selector must be exactly one of
  the run's node names, because dbt reads one string as several selectors and
  a full refresh must not reach a shared model. It overwrites those relations,
  so put it after every State step that depends on them, then `Read` them
  (`upstream_origin.py`, #237).
- **State-free steps.** `Read` and `Reference` run without
  `DBT_ENGINE_MANAGE_STATE` and pass `--no-manage-state`, so project or user
  settings cannot enable State. Their State URL still names the run's proxy,
  as a trap: a step during which the proxy records an RPC fails the run. Each
  invocation is kept in `state_free.json` (argv with `--profiles-dir`
  sanitized, exit code, RPCs recorded). A scenario of only `Write`, `Read`
  and `Reference` steps is State-free. Its capture must hold no RPC at all,
  and it needs no `ValidateClientVersion` call. `upstream_origin_reference`
  and `upstream_origin_table_reference` are such follow-ups (`--run-id` of
  an `upstream_origin` run): the inputs read back, then the Reference of the
  run's dev nodes, read back (#237).
- `Explain(select, intent, node=None, decision=None, execution_type=None,
  history=True, target="prod_demo")`: one `dbt state explain --verbose` per
  node. Set `node` to the `dbt_node_name` when the selector is not the name
  (data tests: `test.jaffle_shop.<test>.*` for a generic test,
  `test.jaffle_shop.<test>*` for a singular one, whose unique id has no hash). `decision=None` records whatever
  hosted decides.
- `Explain(..., via="probe")` sends the same `GetExplainMessages` call (one
  `execution_decision_id`: the node's latest decision in the window so far)
  with `target/debug/state-probe` through the run's proxy. It is not a dbt
  invocation and is not limited to the latest invocation, so one build can be
  followed by explains of many nodes (`reason_kind_cells.py`). The decision
  is still the real client's; the census counts its tree under the client
  version. Build the probe with `cargo build -p dbt-state-harness --bin
  state-probe`. Send it right after the build that decided the node, as the
  client would.
- Use `target="dev_clone"` for steps against the dev target; it is checked
  like `prod_demo`.
- `dbt state explain` explains the decisions of the latest invocation: an
  `Explain` step must follow the `Build` that decided its node, with no other
  `Build` between (otherwise the client explains nothing for it).
- **Follow-up runs.** `--run-id <id>` binds a scenario to an earlier run's id,
  so it extends that run's nodes and their hosted State history (for example
  after the earlier run's explain steps captured nothing). The follow-up's
  explained decisions have their history in the earlier window: mark those
  steps `history=False` and promote the two `fixture_candidate.jsonl` files
  concatenated, earlier first (`execution_type_change_append.py`). Add
  `--prior-window <earlier run>/window.jsonl` to let probe explains and the
  oracle see the earlier run's decisions and builds; the fixture candidate
  still holds only the new window (`reason_kind_cells_more.py`).

`experiments/explain_capture_test.py` binds and plans every bundled scenario,
so a malformed one fails offline. Modules in `capture_scenarios/` whose name
starts with `_` are shared helpers, not scenarios (`_incremental.py` holds the
per-strategy incremental step list). Promote a capture by copying
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

## Live Fusion acceptance on Snowflake

`scripts/fusion-snowflake.sh` runs the pinned dbt 2.0.11 binary through a
real `dbt build` on Snowflake against a local deployment in `oidc` mode (a
machine and a person, each build's decisions under its own Principal), not
against hosted dbt State and without the recording proxy. It writes only its
own schema in `CDO_DEMO_DB` and drops it at the end. See
[testing.md](testing.md#live-fusion-acceptance-on-snowflake).

## Live clone runs

Forcing the dbt client to take a result copy against this server live depends
on warehouse state (the client only asks to clone when the target relation is
absent). Replay of captured traffic is evidence for the captured cases only;
see [testing.md](testing.md#captured-traffic-replay).

## Successful and rejected RPC captures (#133)

Every forwarded RPC now records `status` (`code`, `message`, opaque `details`
bytes and ASCII/binary metadata). Successful records retain their existing
`response` body; rejected records use `response: null`. `response_metadata`
retains successful response headers. Old fixtures without these fields still
mean a successful RPC. Replay compares protobuf field presence, including
optional empty values and the deprecated clone response mirror, and compares
rejected RPC statuses without turning errors into empty successful responses.

Captures retain hosted ingress headers, but replay excludes only the observed
HTTP ingress fields `server`, `strict-transport-security`,
`x-content-type-options`, `x-envoy-upstream-service-time` and `x-frame-options`.
Status codes, messages, details, application metadata and response bodies are
compared. Transport headers such as `date`, `content-type` and `grpc-*` are not
recorded; gRPC status details are retained separately as bytes.

A session also writes `<golden file>.capture.json`, tracking started and
recorded calls and clean completion. Stop the owned proxy with SIGTERM or
Ctrl-C so it drains in-flight calls and certifies the capture. A kill, pending
RPC, write failure, missing line, malformed JSON or missing sidecar fails live
campaign validation. The Explain driver checks this before declaring success.
Other campaign runners must call `capture_integrity.verify_capture(path)`
from `experiments/capture_integrity.py` after stopping their proxy. Keep the
sidecar with run evidence; promote only the focused JSONL into fixtures.
Historical fixtures remain replayable without a sidecar, but cannot certify a
new live campaign's completeness.

Authorization, cookies and headers containing credential/token/secret/password
or API-key names are excluded from request and response captures, including
error metadata. Token-exchange failure bodies are not printed by the proxy.
Captures are decoded protocol records, not token-bearing transport dumps.
Review SQL, labels and telemetry payloads before publishing evidence: application
payloads are preserved for replay rather than redacted speculatively.

`rpc_success_and_rejection.jsonl` was captured from hosted State on
2026-11-12 with `state-probe validate` (`dbt_run_cache_version: "2.0.11"`) and
`state-probe select` (`selector_criteria: 2`, empty `project_id`). The second
call returns `INVALID_ARGUMENT: project_id must be provided`. Neither call
uses a warehouse or creates State execution history. Both replay through the
production router without exceptions. This is protocol evidence, not a claim
of Databricks decision conformance.

## Databricks capability campaign

`experiments/databricks_survey.py` drives the pinned official client through an
owned recording proxy and live Databricks. It is opt-in and never part of an
offline PR cloud gate. It checks the pinned executable digest and guarded
`cdo_demo_db` profile, uses run-owned schemas/project copies/ports, retains
complete typed physical results and failed artifacts, and requires clean
capture certification. The Snowflake runner's guards are unchanged.

The [Databricks upstream-origin experiment](reports/databricks-upstream-origin-2026-11-21.md)
compares ordinary FULL submits over a shared upstream, a confirmed copy and
independent builds. Its four captures pair each decision with served rows, a
State-disabled dev Reference execution and the next unchanged submit.

Commands, evidence and the copy/snapshot blockers are recorded in the
[2026-11-12 capability report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-capabilities-2026-11-12.md).
The runner's credential-free guard/comparison tests run under ordinary
`cargo test --workspace`. This survey makes no Rust support claim.

The [actual freshness follow-up](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-freshness-actual-2026-11-14.md)
adds a zero-object Reference execution for the evidenced three-column FULL
SELECT contract. `databricks_reference_pilot.py` runs an isolated, State-disabled
`dbt show --inline … --limit -1 --output json`, compares full duplicate/NULL rows
with a complete typed API SELECT, certifies zero RPCs and requires unchanged
inventories. Its `--schema-lease` must be the original ownership evidence with
the intended roles; it creates no relation or schema.

`databricks_freshness_actual.py` and `databricks_freshness_combined_race.py` each require
only two fresh exact owned names in the lease's `prod` schema. The latter holds
one owned ready-to-execute response before model SQL, then one owned Confirm
after SQL, with an owned source UPDATE during each hold; original bytes continue
unchanged. The earlier Confirm-only history remains reproducible through
`databricks_freshness_race.py`. Both runners accept the baseline
`--dbt`, `--proxy`, `--probe`, `--schema-lease`, `--work-dir` arguments and optional
owned-loopback `--rust-url`. Run hosted first, certify/promote/replay its complete
history, then use the ignored Rust tests in `databricks_freshness.rs` with fresh
objects. The actual runner needs PyYAML; the race also needs Python grpcio,
protobuf and `protoc`. No prior campaign object may be written or recycled.

The strict reference adapter fault probes use ordinary Python. The gate's
credential-free real-loopback transport checks require grpcio:

```bash
python3 experiments/databricks_readonly_reference_test.py
uv run --with grpcio python experiments/databricks_confirm_gate_test.py
```

Equal changed-data epochs, delayed metadata and writes inside the warehouse
SQL snapshot/read interval remain separate reachability questions. A successful bounded history does
not close #140 or waive its independently unsafe omitted-source hosted SKIP.

`databricks_cached_tests.py` is a dedicated no-object #143 partial runner for
one selected source-less singular test with explicit `store_failures=false`.
It has an empty warehouse mutation allowlist, checks the exact derived audit
name absent, permits only that name's read-only client metadata probes and
requires literal-only main/aggregate SQL plus exact before/after inventories.
A stable inventory interval must be coordinated with concurrent live work.
It never inherits the physical baseline/identity runners' mutation permissions.

Run hosted first with `--dbt`, `--proxy`, `--probe`, `--schema-lease` and a fresh
`--work-dir`. A fresh run ID supplies both full UID and body nonce. The four
managed cells cover pass/reuse then SQL failure/reuse under false comparison mode, with typed
duplicate/NULL row and aggregate oracles and separately certified zero-RPC
disabled references. Raw failing exits are retained and checked against complete
run_results and successful computation confirmations. `--rust-url` requires
`--contract` from the observed hosted outcomes. The ignored
`real_dbt_cached_failures_match_independent_literal_results` test uses the
production router and the existing `DBT_STATE_DATABRICKS_{PYTHON,DBT,SCHEMA_LEASE,ARTIFACTS}`
environment variables. See the [dated evidence and remaining scope](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-cached-test-results-2026-11-14.md).

The bounded [target lifecycle slice](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-target-lifecycle-2026-11-14.md)
uses `databricks_target_lifecycle_campaign.py` with those same baseline arguments.
It allocates one fresh source/consumer pair per backend and runs a serial table
history plus an explicitly admitted continuation on the same names and State
store. The continuation requires the successful original receipt, exact inventory
and UC identities, and preserves all causal capture prerequisites. It adds no
physical names. Target/source drops and replacements are lifecycle operations
only on those just-created names; the runner performs no cleanup.

Run hosted first, replay the complete causal capture without gaps, then run
`databricks_freshness.rs::real_dbt_target_lifecycle_matches_readonly_reference`
with a fresh `DBT_STATE_DATABRICKS_ARTIFACTS` path and the established pinned
client/Python/lease environment. For separate staged hosted execution, run
`databricks_target_lifecycle.py`, then
`databricks_target_lifecycle_continuation.py --prerequisite <successful-stage>`
with the **same** run ID and backend. Never treat the continuation capture alone
as empty-history evidence. Ordinary project YAML controls the evidenced ignore
policy; retained external contents are checked against explicit snapshots,
separately from the fresh transformation reference. #141 remains blocked by
#140; these bounded cells do not claim full ticket or release completion.

`databricks_cached_config.py` extends that same zero-object boundary with a
fixed body and variable severity/threshold/fail_calc declarations. Its default
four-cell vertical is fresh ERROR failure/reuse then WARN/reuse; `--through
sum_v` runs the complete twelve-cell configuration matrix. Each step still
requires complete typed rows and independent aggregation plus a separately
recorded zero-RPC disabled test and compile precheck. `request-deltas/` records
all changed request fields; configuration changes must not be inferred solely
from CLI flags or node hashes. The raw payload and client classification are
checked independently: WARN may retain should_error=true, while a threshold
pass may retain nonzero failures.

The ignored `real_dbt_cached_configuration_matches_independent_literal_results`
uses the same environment variables. It defaults to the observed four-cell
contract; set `DBT_STATE_DATABRICKS_CONFIG_THROUGH=sum_v`,
`DBT_STATE_DATABRICKS_CONFIG_QUERY_IDENTITY=20261114d143dd` and
`DBT_STATE_DATABRICKS_CONFIG_CONTRACT=<repo>/experiments/databricks_cached_thresholds_contract.json`
for the complete observed matrix. Always use a fresh artifact directory and
an isolated server store; pin the launch source and executable hashes before
actual use. See the [configuration evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-cached-test-configuration-2026-11-14.md).

The bounded `databricks_policy_actual.py` campaign uses four exact fresh
production-role names (`p_<run>_{inputs,producer_a,producer_b,consumer}`) and zero
persistent reference objects. It requires an explicit `--live-clearance` as well
as the evidenced schema lease; the clearance pins hosted/Rust run IDs, exact
allowlists and fresh inventory counts. Ordinary properties YAML uses
`state.require_fresh_data_from: any|all` with `lag_tolerance: 3m`; manifest and wire
must both prove the setting. A legacy `lag_tolerance.updates_on` candidate parsed
but emitted ANY and is retained as a failed discovery, not ALL acceptance.

Hosted capture precedes exact public replay and the ignored
`databricks_policy.rs::real_dbt_policy_matches_saved_and_fresh_independent_references`
Rust acceptance. The test additionally requires `DBT_STATE_DATABRICKS_RUN_ID`,
`DBT_STATE_DATABRICKS_LIVE_CLEARANCE` and
`DBT_STATE_DATABRICKS_POLICY_CONTRACT`, alongside the existing pinned-client,
Python, lease, artifact and query-identity variables. Every consumer BUILD saves
an immutable operation-derived reference R; a later SKIP checks R while a
separate direct typed/State-disabled-show reference computes current F. Actual
elapsed-age checks retain margins and full producer Record histories. Crafted
local-clock equality tests make no exact hosted-equality claim.
The [dated report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-policy-2026-11-14.md) states the bounded
coverage and continuing #138/#140 blockers.

`databricks_cached_template.py` adds the separate five-cell true-template
failing-cache history: fresh failing BUILD/reuse, render-only fresh pass with
permitted retained failing R, then body-and-rendering pass BUILD/reuse. It keeps
the immutable confirmed reference R separate from fresh F, and requires the
render-only actual request delta to be exactly `sql`. The same zero-object
boundary, complete payload/runtime/Explain checks and independently recorded
zero-RPC disabled/compile controls apply. Its observed contract is
`databricks_cached_template_contract.json`; the ignored
`real_dbt_true_template_cached_failure_matches_independent_literal_results`
uses the established pinned dbt/Python/lease/artifact environment. See the
[dated bounded evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-cached-test-template-failure-2026-11-14.md).

`databricks_merge.py` runs the bounded SQL MERGE history with two exact fresh
production-role names, no persistent Reference objects, and an explicit
`--live-clearance` matching its lease and hosted/Rust run IDs. Clearance must
reserve native staging/backup spellings and one concurrent extra persistent
slot; the session-only unqualified `__dbt_tmp` view has a separate allowlist.
The runner retains actual default native SQL rather than disabling a client
materialization route to fit capacity. Independent literal operation snapshots
retain source-deleted rows after MERGE and remove them after full refresh.

Hosted capture and exact public replay precede the ignored
`databricks_merge.rs::real_dbt_merge_matches_independent_operation_history`.
It requires the pinned `DBT_STATE_DATABRICKS_DBT`, persistent
`DBT_STATE_DATABRICKS_PYTHON`, immutable `DBT_STATE_DATABRICKS_PROXY` and
`DBT_STATE_DATABRICKS_PROBE`, plus `DBT_STATE_DATABRICKS_SCHEMA_LEASE`,
`DBT_STATE_DATABRICKS_LIVE_CLEARANCE`, `DBT_STATE_DATABRICKS_RUN_ID` and a fresh
`DBT_STATE_DATABRICKS_ARTIFACTS` directory. Pin source/executable identity before
use; live acceptance runs the production router in an isolated State schema.
The [dated partial report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-merge-2026-11-14.md) records the
actual FULL1 Record after CLI refresh and next ordinary MERGE3 rebuild/reuse,
complete physical checks and coverage still required for whole #145.

`databricks_append.py` adds a bounded #146 stable-render APPEND history with
independent literal cumulative target `R` and separately selected `F`. It uses
one freshly allocated single-schema container, two exact new names per backend,
and a possible one-object persistent native replacement peak. Its dedicated
allocation scope preserves the existing two-reference-container lease guards.
No persistent reference graph, cleanup, custom macro or behavior override is
used. Each checkpoint checks complete typed duplicate/NULL multisets, ordered
schema, MANAGED/Delta properties, a certified zero-RPC disabled literal `R`
Reference and a separate selected `F`. Source updates and deletion preserve
prior target rows until full refresh replaces them; the next ordinary APPEND
build appends to the refreshed result.

Run hosted first using `--dbt`, `--proxy`, `--probe`, `--live-clearance`,
`--run-id` and a fresh `--work-dir`. The clearance must name the allocated
schema UUID/owner/creation evidence, exact phase names and footprint. Promote
and replay the complete unfiltered capture before the one fresh Rust phase.
The ignored
`databricks_append.rs::real_dbt_append_matches_independent_operation_history`
requires the established `DBT_STATE_DATABRICKS_{PYTHON,DBT,PROXY,PROBE,ARTIFACTS}`
variables plus `DBT_STATE_DATABRICKS_LIVE_CLEARANCE` and
`DBT_STATE_DATABRICKS_RUN_ID`. It does not use a two-schema lease.
See the [dated bounded evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-append-2026-11-14.md);
whole #146, its dependencies and full #132 remain incomplete.

`databricks_insert_overwrite.py` adds the bounded #147 SQL partition replacement
history. The actual pinned serverless path is `INSERT INTO TABLE ... REPLACE ON`
with null-safe partition key `p`; a configured unique key does not replace that
key. Fixed first/steady SQL selects A or NULL. Independent literal cumulative
`R` and selected `F` preserve duplicates, DATE/TIMESTAMP values, NULL payloads,
unrepresented NULL partitions and the entire old target when `F` is empty.
Every cell also retains full source/target Delta history, typed schema and
partition metadata, own-runtime reuse, client statuses and a zero-RPC disabled
SELECT-only `R` reference. The ordinary adapter warning, reuse and FULL refresh
statuses are checked separately. Whole-second UTC show timestamps are accepted
only through the independently demonstrated lossless `.000Z` spelling.

The clearance fixes the reused owned container identity, four new names across
both backends, exact staging/backup spellings, one concurrent extra persistent
slot and unqualified session temporary views. It authorizes each backend
separately and preserves all prior objects; it does not authorize replaying
retained names, cleanup or new schemas. Hosted capture and complete exact public
replay precede the ignored
`databricks_insert_overwrite.rs::real_dbt_insert_overwrite_matches_independent_operation_history`.
Use the same pinned-client/tool/artifact/clearance/run-ID environment variables
as APPEND. The [dated evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-insert-overwrite-2026-11-14.md)
records the two preserved helper guard stops, root-reviewed continuation and
whole chronological fixture. The [scoped acceptance](databricks-insert-overwrite.md)
adds `databricks_overwrite_recovery.py`: two partition keys, schema failure/retry,
additive recovery, complementary replacement and empty-input retention. Its
fresh UUID pairs use the source-evidenced lease pattern, with three native
compiles before DDL and two zero-RPC SELECT-only References per managed cell.
Run the new `real_dbt_composite_overwrite_matches_operation_history` wrapper
with the MERGE recovery schema-lease/schema/object-ID variables. Complete hosted
replay precedes fresh Rust execution; no existing names are rerun or cleaned.

For supplemental native replay, retain the exact audit-clock sidecar before the
test State schema is dropped. The147 wrapper calls
`support::native_clock::retain` after the proxy drains; set
`DBT_STATE_EXTRA_REPLAY_CLOCKS` with the corresponding
`DBT_STATE_EXTRA_REPLAY_CAPTURE`. Evidence is capture-hash-bound, complete for
every ordinary submit, and keyed by organization/decision; it never changes a
live request clock. The original147 capture uses independently verified original
WAL recovery rather than a warehouse rerun. See [native clock replay](testing.md).

`databricks_audit.py` characterizes only the bounded #144 source-less native
audit-table histories. Its original failed aliased/default-name campaigns are
retained unchanged. The matching 30/21-row fixtures include every original RPC
and all separately attributed causal transport controls; synthetic requests
retain historical hashes and never publish a result or establish physical rows.
The narrow ordinary error guard adds no audit-input elimination or reuse rule.

The ignored
`databricks_audit.rs::real_dbt_audit_relations_match_independent_recorded_and_fresh_results`
requires the established `DBT_STATE_DATABRICKS_{PYTHON,DBT,PROXY,PROBE,ARTIFACTS}`,
`SCHEMA_LEASE`, `LIVE_CLEARANCE` and `RUN_ID` variables, plus
`DBT_STATE_DATABRICKS_AUDIT_CONTRACT` and a separately issued
`DBT_STATE_DATABRICKS_AUDIT_RUST_CLEARANCE`. The dedicated receipt binds mode
`alias5` or `default3`, exact objects/limits, whole hosted fixture and contract
hashes, green public replay and the actual running server executable hash.
The default mode also requires a separate exact-output retirement receipt.
No further live phase follows from an earlier receipt or this documentation.

The final body-changing cell expects exactly the observed ordinary status 3,
null response and successful native fallback, with no Confirm. Its physical
empty result F is checked independently; the prior confirmed failing State R
is not replaced. Other cells require actual fresh BUILD/Confirm and positive
speculative reuse with complete payload/runtime/Explain and physical oracles.
All compile/reference invocations use independent zero-RPC canaries. See the
[dated status evidence and remaining scope](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-audit-hosted-rejection-2026-11-14.md).

### Bounded Databricks predicate replacement (#148)

`experiments/databricks_replace_where.py` runs thirteen native SQL/managed-Delta
cells per separately cleared backend. It keeps an immutable literal source/R/F
ledger outside the repository, checks the complete ordered
BIGINT/STRING/DATE/TIMESTAMP schema and multisets, and executes State-disabled
SELECT-only literal references with separate zero-RPC canaries. `unique_key`
does not deduplicate replacement rows. An outside source deletion retains its
old target row until the destination predicate widens; empty incoming rows
remove matching target rows. The model's fixed incremental SELECT filter is
independent of that destination predicate.

The driver requires `--operation-ledger` as well as the pinned tools, exact live
clearance and fresh run ID. It verifies the ledger digest/content and backend
clearance before credentials or client execution. Scope is exactly two new
permanent names per backend, with one reserved persistent auxiliary slot and
one serialized session-local temporary view. There is no schema allocation,
cleanup, source reset, macro override or permanent reference object.

Actual pinned replace_where submits and full-refresh Record input both use
DBT_CUSTOM11. The following ordinary run remains11; its rendered SQL changes
from the full unfiltered Record SELECT to the steady filtered SELECT. The
helper's default Record expectation remains FULL1; #148 passes the independently
observed literal expected_type11. Do not generalize another strategy's mapping.
The [dated report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-replace-where-2026-11-14.md) preserves the
initial helper stop, separately cleared continuation and complete90-RPC trace.

The schema/predicate supplement `databricks_predicate_recovery.py` uses a fresh
UUID pair and the source-evidenced existing-schema lease pattern. Four actual
configs compile before DDL. Ten managed cells cover schema fail/retry, additive
recovery and out-of-range input failure/retry followed by predicate widening.
Each cell has complete source/target history and two State-disabled SELECT-only
References. Run its ignored wrapper
`real_dbt_predicate_recovery_matches_operation_history` with the same schema
lease/schema/object-ID variables as MERGE recovery. Hosted capture and complete
exact public replay precede a fresh production-router Rust run. See
[scoped acceptance](databricks-replace-where.md).

The ignored native wrapper is
`databricks_replace_where.rs::real_dbt_replace_where_matches_independent_operation_history`.
In addition to the usual native environment it requires
`DBT_STATE_DATABRICKS_OPERATION_LEDGER`. It retains the actual organization-scoped
audit clocks before State schema cleanup and writes the capture/clock paths to
`native-extra-replay-environment.json`. Supplemental strict replay must supply
that actual sidecar. A byte-identical `databricks_` filename alias satisfies the
extra-capture basename guard without editing the raw proxy file.

## Bounded Databricks DELETE_INSERT acceptance (#149)

`experiments/databricks_delete_insert.py` requires the original immutable
operation ledger, exact phase clearance and frozen tools. Ordinary submissions
are DELETE_INSERT5 and the actual pinned native path is one null-safe
`INSERT ... REPLACE ON target.id <=> temp.id`, with an unqualified session
temporary view. Duplicate incoming keys remain a multiset; missing input keys
retain old rows. No legacy scalar `DELETE ... IN` behavior is substituted.

The hosted phase first executes one literal fractional timestamp SELECT twice,
through typed API and isolated State-disabled show, before any source/consumer
DDL. Its six rows prove exact .123/.456/.789/.012 UTC encodings and duplicate
NULL keys; unknown encodings stop without object creation or coercion. The
13-cell managed campaign has13 separate zero-RPC R references. Native reuses
that observed codec proof and requires literal
`expected_full_refresh_record_execution_type` from the whole hosted history;
this capture observes FULL1, followed by ordinary5. A changed native kind rejects.

The ignored native wrapper is
`databricks_delete_insert.rs::real_dbt_delete_insert_matches_independent_operation_history`.
It uses the standard native environment plus
`DBT_STATE_DATABRICKS_OPERATION_LEDGER`, saves its actual executable identity,
and retains all actual org/decision/audit sampled clocks before State schema
cleanup. Its capture and clock sidecar must pass whole supplemental replay.
See [the dated evidence and unfinished scope](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-delete-insert-2026-11-15.md).


### Bounded MICROBATCH history (#150)

`experiments/databricks_microbatch.py` consumes an immutable independent R/F
ledger and exact phase-specific clearance. Ten native CLI invocations produce
ten whole-command MICROBATCH6 submissions, five builds/nine daily batches and
five fresh own-runtime SKIPs; the SQL sent to State remains unwindowed, with
whole-invocation event-time bounds in semantic extras and native default lookback1.
Ten separate SELECT-only R references retain complete zero-RPC canaries.

The declared source-change cell also inserts one literal NULL-event-time row
into the exact owned target, only after explicit nullable=true UC metadata,
MANAGED Delta/full-name/schema and independent before/after typed-result checks.
The source and target epochs must advance. Later R retains the row while F
remains the current selected source projection; no Target-only policy is inferred.

The ignored `databricks_microbatch.rs` native wrapper uses the standard native
environment plus `DBT_STATE_DATABRICKS_OPERATION_LEDGER`. It retains actual
org/decision-scoped audit clocks before State schema Drop and saves both capture
and clock replay bindings. Its entire capture must pass supplemental replay
with those exact clocks. [Dated results and remaining scope](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-microbatch-2026-11-15.md).

The `databricks_moving_windows.py` supplement uses the existing evidenced
`--schema-lease`, owned `--schema`, exact UUID `--object-id` and guarded
`--utc-day`. Its independent ledger checks source/R/F, default multi-batch
lookback0/2, single-batch predicates and mandatory reuse. The native wrapper is
`real_dbt_moving_windows_match_independent_operation_history`, adding
`DBT_STATE_DATABRICKS_UTC_DAY`. The optional
`DBT_STATE_DATABRICKS_NATIVE_PREFIX` is narrowly bounded to the retained native33
interruption: genuine public history hydration, no repeated managed command,
and independently replayed sessions. It is not a warehouse reset or a general
history import API. [Evidence and capability limit](databricks-microbatch.md).
