# Generated DuckDB physical-state checks

`crates/harness/tests/duckdb_generated.rs` launches the pinned Python driver
against an isolated Postgres schema. The driver launches the normal production
server binary, sends public Python State gRPC requests with organization
metadata, and executes actual DuckDB statements. Restart terminates that binary
and recreates its pools against the same isolated Postgres schema. It also
checks pending execution confirmation across a restart. No production test
endpoint or cloud credential is involved.

The bounded run uses four reproducible seeds, each with a fixed lifecycle spine
and four randomized episodes. Each episode selects an environment, integer
transformation and an interaction: changed logic, upstream data, target rows,
target schema, dropped target, write-only recording, or process restart.
The spine guarantees submit, execute, confirm, record, external mutation, drop,
CTAS result copying, restart and evidence reset. It asserts positive SKIP after
confirmed builds, positive prod-to-dev CTAS, and SKIP after confirmed copying;
an always-build service fails. Reset calls the production recovery operation,
keeps user relations, and starts a new warehouse namespace. It does not truncate
Postgres history.

The independent oracle in `tests/generated/oracle.py` imports only DuckDB and
uses a declared integer workload. It calculates expected row multisets from
controlled input fixtures, including duplicate rows and NULL, and requires
explicit INTEGER column names/types. It never calls decision, fingerprint,
store or evidence helpers to derive expected answers. The driver uses the
production evidence module only to supply protocol metadata. Expected copy
sources are selected from independently tracked confirmed executions before
the submit response is observed. Every returned SKIP checks actual target rows
and schema; every CTAS checks source scope, actual source contents, and actual
copied rows/schema.

One spine operation explicitly permits the recorded source's older result
after the raw input advances. Its `stale` flag and policy annotation declare the
SQL-submit fallback contract described in [result copying](duckdb-result-copy.md).
The oracle requires that declared source result, rather than pretending that
copying always returns current upstream data. Its following SKIP checkpoint explicitly permits that confirmed copied result
as well: its own copy completion is newer than the upstream change. All other
operations require the current fixture result. Externally modified or dropped sources are never
accepted as that older result.

Two fixed mutation traces run through the same production RPC and physical
oracle path. A wrong-SKIP response fault after a logic change leaves the older
physical target and must fail specifically with `wrong-skip:rows`. A
wrong-source executor fault follows a real, validated copy proposal, executes
CTAS from an actual decoy relation with different rows, and must fail with
`wrong-source-copy:rows`. The latter models an executor selecting the wrong
source after validation; it does not disable the production client's race
checks or claim to mutate the server's source selection. Neither check accepts
an arbitrary exception as evidence that the oracle works.

## Commands

```sh
uv sync --project integrations/duckdb --locked
cargo build -p dbt-state-server
DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python" \
DBT_STATE_SERVER_BIN="$PWD/target/debug/dbt-state-server" \
  cargo test -p dbt-state-harness --test duckdb_generated -- --ignored --nocapture
```

`DATABASE_URL` defaults to the ordinary local test Postgres. The Rust launcher
creates/drops a fresh schema, preserving existing URL parameters and setting
that schema as the server's search path. Each trace receives a fresh DuckDB
file and organization. The binary must be built from the same checkout.
`CARGO_TARGET_DIR` users should set `DBT_STATE_SERVER_BIN` accordingly.

A larger manual/scheduled run uses the same gate:

```sh
DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python" \
DBT_STATE_SERVER_BIN="$PWD/target/debug/dbt-state-server" \
DBT_STATE_GENERATED_SEEDS=0,1,2,3,4,5,6,7 \
DBT_STATE_GENERATED_STEPS=40 \
  cargo test -p dbt-state-harness --test duckdb_generated -- --ignored --nocapture
```

`STEPS` counts random episodes, each several causally related operations, in
addition to the fixed spine. The driver reports per-operation and per-decision
coverage, CTAS counts and wall time. The fixed oracle and trace-contract tests
can also be run independently:

```sh
integrations/duckdb/.venv/bin/python -m unittest discover \
  -s integrations/duckdb/tests/generated
```

## Replay and minimization

Set `DBT_STATE_GENERATED_ARTIFACTS` to retain artifacts at a known directory;
otherwise the test prints a temporary directory on failure. Each trace keeps
its seed/version, symbolic operations and dependencies in `trace.json`, public
requests/responses and execution observations in `observations.json`, the
DuckDB file and `server.log`. Failures also have `failure.json` with the failing
index and fingerprint. The complete planned trace is written before execution,
so abrupt failures still leave a replay input. No credentials are recorded.
Artifacts, including the intentionally failing mutation probes, are retained
on successful runs too; CI should upload the configured directory on failure.

Replay with fresh warehouse/service scope:

```sh
DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python" \
DBT_STATE_SERVER_BIN="$PWD/target/debug/dbt-state-server" \
DBT_STATE_GENERATED_REPLAY=/path/to/trace.json \
DBT_STATE_GENERATED_SHRINK=1 \
  cargo test -p dbt-state-harness --test duckdb_generated -- --ignored --nocapture
```

The optional minimizer makes at most 40 clean replays and retains a candidate
only when it reproduces the same named oracle violation. Removing an operation
also removes its dependent operations. Dependencies deliberately form a
conservative chain within each reset-delimited episode, preserving source
builds, confirmations, mutations and pending IDs; independent later episodes
can be removed wholesale. This favors valid causal traces over aggressive
shrinking. It writes `minimized.json` beside the original without overwriting
it. Every shrink attempt retains its own artifacts. A minimized useful failure
should be promoted to a fixed physical regression; the two mutation traces are
permanent regressions for oracle sensitivity. This is local reuse-safety
evidence, alongside the fixed real-dbt tests. Snowflake golden replay remains
separate hosted-conformance evidence.

The generator covers the validated deterministic table/copy subset. Fixed
real-dbt gates separately cover adapter materializations, views, seeds, cached
data tests, incremental workloads, worker ordering and recovery. Passing these
generated traces does not establish arbitrary SQL equivalence, overlapping
writers, or hosted DuckDB conformance.

The initial four-seed/four-episode run measured 146 seconds in the Python driver
(154 seconds including launcher startup), with 50 BUILD, 30 SKIP, eight CTAS
copies, and both mutation probes caught. This is a local measurement, not a CI
time guarantee.
