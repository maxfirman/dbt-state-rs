# Generated DuckDB physical-state checks

`crates/harness/tests/duckdb_generated.rs` runs the Python driver
`integrations/duckdb/tests/generated/runner.py` against an isolated Postgres
schema. The driver starts the normal production server binary, sends public
State gRPC requests with organization metadata, and executes real DuckDB
statements. A restart step stops that binary and starts it again on the same
schema, including with an execution confirmation still pending. No test-only
server endpoint or cloud credential is involved.

## What a run does

Each seed produces a fixed lifecycle spine plus a number of randomized
episodes (defaults in `duckdb_generated.rs`). An episode picks a target, an
integer transformation and one interaction: changed logic, upstream data,
rows or schema of the target relation, a dropped target relation, write-only
recording, or a process restart. The spine always covers submit, execute,
confirm, record, External modification, drop, CTAS result copy, restart and
evidence reset. It requires a SKIP after confirmed builds, a prod-to-dev CTAS
and a SKIP after a confirmed copy, so an always-build server fails. Reset
calls the production recovery operation, keeps user relations and starts a
new warehouse namespace; it does not truncate Postgres history.

The oracle (`integrations/duckdb/tests/generated/oracle.py`) imports only
DuckDB. It computes expected row multisets from controlled inputs, including
duplicates and NULL, and requires explicit INTEGER column names and types. It
never calls decision, fingerprint, store or evidence code. The driver uses the
production evidence module only to fill protocol metadata. Expected copy
sources come from independently tracked confirmed executions, chosen before
the submit response is read. Every SKIP checks the actual rows and schema of
the target relation; every CTAS checks the source scope, the source contents
and the copied rows and schema.

One spine operation is marked `stale`: after the raw input advances, it
accepts the recorded source's older result, as the SQL-submit fallback in
[result copies](duckdb-identity-and-copy.md#client-execution) permits. The
following SKIP also accepts that copied result, because its copy completion is
newer than the upstream change. Every other operation requires the current
result. Externally modified or dropped sources are never accepted.

Two mutation traces check that the oracle catches wrong answers. A wrong-SKIP
fault after a logic change leaves the older rows and must fail with
`wrong-skip:rows`. A wrong-source fault follows a real, validated copy
proposal, runs CTAS from a decoy relation with different rows, and must fail
with `wrong-source-copy:rows`. It models an executor choosing the wrong source
after validation; it does not disable the client's checks or alter the
server's source selection. Neither probe accepts an arbitrary exception.

## Commands

```sh
uv sync --project integrations/duckdb --locked
cargo build -p dbt-state-server
export DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python"
export DBT_STATE_SERVER_BIN="$PWD/target/debug/dbt-state-server"
cargo test -p dbt-state-harness --test duckdb_generated -- --ignored --nocapture
# Larger run, as in the weekly CI job:
DBT_STATE_GENERATED_SEEDS=0,1,2,3,4,5,6,7 DBT_STATE_GENERATED_STEPS=40 \
  cargo test -p dbt-state-harness --test duckdb_generated -- --ignored --nocapture
# Oracle and trace unit tests only:
integrations/duckdb/.venv/bin/python -m unittest discover \
  -s integrations/duckdb/tests/generated
```

`DATABASE_URL` defaults to the local test Postgres. The launcher creates and
drops a fresh schema and sets it as the server's search path. Each trace gets a
fresh DuckDB file and organization. Build the binary from the same checkout;
with `CARGO_TARGET_DIR`, point `DBT_STATE_SERVER_BIN` at it.
`DBT_STATE_GENERATED_STEPS` counts random episodes in addition to the spine.
The driver reports per-operation and per-decision coverage, CTAS counts and
wall time.

## Artifacts, replay and minimization

`DBT_STATE_GENERATED_ARTIFACTS` sets the artifact directory; otherwise the test
uses a temporary directory and prints it on failure. Each trace keeps
`trace.json` (seed, version, symbolic operations and dependencies, written
before execution so abrupt failures still leave a replay input),
`observations.json` (public requests, responses and execution observations),
the DuckDB file and `server.log`. Failures add `failure.json` with the failing
index and fingerprint. Artifacts, including those of the mutation probes, are
kept on success too. No credentials are recorded. `scripts/duckdb-ci.py`
writes them under `artifacts/duckdb/generated`, and CI uploads
`artifacts/duckdb` on every run (`if: always()`), keeping it 14 days for pull
requests and 30 days for the extended job.

Replay with fresh warehouse and server scope:

```sh
DBT_STATE_GENERATED_REPLAY=/path/to/trace.json DBT_STATE_GENERATED_SHRINK=1 \
  cargo test -p dbt-state-harness --test duckdb_generated -- --ignored --nocapture
```

The optional minimizer (`traces.py`) makes at most 40 clean replays and keeps
a candidate only when it reproduces the same named oracle violation. Removing
an operation removes its dependents. Dependencies form a conservative chain
within each reset-delimited episode, so later independent episodes can be
removed whole. It writes `minimized.json` beside the original and keeps each
attempt's artifacts. Promote a useful minimized failure to a fixed physical
regression.

## Scope

The generator covers the deterministic table and copy subset. The fixed
real-dbt tests cover adapter materializations, views, seeds, cached data
tests, incremental models, worker ordering and recovery. Passing traces does
not establish arbitrary SQL equivalence, support for overlapping writers or
hosted conformance; Snowflake golden replay remains the hosted evidence.
