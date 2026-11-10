# Local DuckDB workflow

Real Python dbt projects run against a persistent local DuckDB file and the
Rust gRPC server, with Postgres storing State history. The workflow supports
BUILD, SKIP and result copies between the `prod` and `dev` targets without
cloud credentials. There is no hosted DuckDB service to compare against: the
DuckDB tests check this server's local reuse-safety contract, and the captured
Snowflake traffic remains the hosted conformance evidence.

Related pages: [evidence and eligibility](duckdb-evidence.md),
[SQL identity and result copies](duckdb-identity-and-copy.md),
[incremental models](duckdb-incremental.md),
[generated checks](duckdb-generated.md) and the
[package README](../integrations/duckdb/README.md).

## Quickstart

Requirements: Rust, `protoc`, `uv`, Python 3.12 and Postgres. Start Postgres if
needed:

```sh
docker run -d --name dbt-state-pg -e POSTGRES_USER=dbtstate \
  -e POSTGRES_PASSWORD=dbtstate -e POSTGRES_DB=dbtstate \
  -p 55441:5432 postgres:17-alpine
```

Start the server in its own terminal. Startup applies the migrations:

```sh
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
cargo run -p dbt-state-server
```

In another terminal, from the repository root:

```sh
uv sync --project integrations/duckdb --locked
export DBT_STATE_LOCAL=1
export RUN_CACHE_API_URL=127.0.0.1:50051
export RUN_CACHE_API_SECURE=false
export DBT_SEND_ANONYMOUS_USAGE_STATS=false
export DUCKDB_WORKFLOW_DIR=$(mktemp -d)
cp -R integrations/duckdb/example "$DUCKDB_WORKFLOW_DIR/project"
export DUCKDB_WORKFLOW_PATH="$DUCKDB_WORKFLOW_DIR/warehouse.duckdb"
export DBT_PROJECT_DIR="$DUCKDB_WORKFLOW_DIR/project"
export DBT_PROFILES_DIR="$DBT_PROJECT_DIR"
dbt_state() { uv run --project integrations/duckdb dbt-state-duckdb "$@"; }

# Stage the raw seed, then build the models and the data test.
dbt_state seed --target prod
dbt_state build --target prod --exclude resource_type:seed
# Separate processes now SKIP the seed and every supported node.
dbt_state seed --target prod
dbt_state build --target prod --exclude resource_type:seed

# dev defers to prod; eligible missing results are copied by CTAS, then SKIP.
dbt_state build --target dev --exclude resource_type:seed
dbt_state build --target dev --exclude resource_type:seed
```

Calling plain `dbt` does not install the extension; use `dbt-state-duckdb`.

The example (`integrations/duckdb/example`) has four dbt worker threads, an
INTEGER seed with duplicate and NULL rows, a table (`orders`), a view
(`order_view`), a `delete+insert` incremental model (`ledger`) and a cached
data test (`nonnegative`). The seed is written to schema `raw` by the
project's `generate_schema_name` macro; models are written to `prod` or `dev`.
The incremental SQL reads `warehouse.raw.inputs` literally, because
initialization copies accept literal physical inputs rather than
`ref()`/`source()`. That is why the file must be named `warehouse.duckdb` and
why `build` excludes the seed. Expected results: `orders` and `order_view`
hold `(1,11), (2,21), (2,21), (3,NULL)`; `ledger` holds `(1,10), (2,20),
(2,20), (3,NULL)`. `crates/harness/tests/duckdb_workflow.rs` runs this
example from a fresh directory and checks these rows and types.

### Changing inputs

Edit the copied `seeds/inputs.csv`, run `seed`, then `build` in prod. The
example profile sets no Lag tolerance, so within the client's 45-minute
default a node whose upstream data changed can SKIP and keep its earlier rows
(see [Lag tolerance](#operations)). Add `run_cache_freshness_tolerance: 0s` to
both outputs in `profiles.yml` to rebuild on every change. The ledger keeps
rows whose keys disappear from the source ([incremental](duckdb-incremental.md)),
and a dev copy can take a valid older prod result ([copies](duckdb-identity-and-copy.md)).

## Supported versions

This is the only place the versions are listed. `integrations/duckdb/pyproject.toml`
pins them and `integrations/duckdb/uv.lock` pins every transitive dependency.
They are the validated combination; an upgrade must pass the full gate.

| Component | Version |
| --- | --- |
| Python | 3.12 |
| dbt Core | 1.10.15 |
| dbt-duckdb | 1.10.0 |
| DuckDB | 1.4.1 |
| dbt-state Python client | 2.62.6 |
| query-cache-common / query-cache-protobuf | 2.7.0 |

## Support matrix

| Workload or capability | Contract |
| --- | --- |
| SQL tables | Same-target reuse with supported deterministic SQL and known evidence for the target relation |
| SQL views | Checked local dependency graphs; definition/schema evidence plus freshness of the resolved leaf tables |
| Seeds | CSV/config identity plus current physical evidence |
| Data tests | Complete cached pass/fail/warn outcomes with project-scoped identity; stored failures execute ordinarily |
| Incremental | Explicit `delete+insert`, one non-NULL `unique_key`, explicit `on_schema_change='fail'` |
| Result copies | Eligible tables and checked view chains between `prod` and `dev` by CTAS (rows, names, types); narrower incremental initialization |
| Parallelism | Normal dbt workers inside one exclusive invocation |
| Target scope | `prod`/`dev` schemas in one persistent file; separate files have separate history |
| Unsupported | Snapshots, Python and custom materializations, other incremental strategies, attached/in-memory/remote files, extensions, server-backed State selectors and deferred-relation RPCs |

Unsupported materializations and strategies bypass State and run as ordinary
dbt. Workloads outside the [eligibility subset](duckdb-evidence.md#sql-eligibility-subset)
report unknown evidence, so they always build. Copy-only limitations
(constraints, defaults, indexes, hooks, contracts, uncertain identity) fall
back to ordinary execution or same-target reuse. CTAS does not preserve
other physical properties.

## Operations

**Exclusive writer.** Each invocation and `reset` takes a nonblocking advisory
lock on `<file>.dbt-state.lock` beside the database. An overlapping State
command fails before warehouse work. Do not delete the lock file while
commands run. Symlink aliases share the lock; hard-linked files are rejected.
External tools do not honor the lock, so make manual edits between runs only.

**Reset after restoring a backup.** Restored metadata cannot detect its own
rollback. Stop all writers, restore the file, then reset before the next State
command:

```sh
dbt_state reset --database "$DUCKDB_WORKFLOW_PATH"
dbt_state build --target prod --exclude resource_type:seed
```

Reset keeps user relations and starts a new evidence generation. The next run
builds cold; later unchanged runs can SKIP. Details are in
[recovery](duckdb-evidence.md#recovery-and-the-dirty-marker).

**Lag tolerance** ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md))
applies on DuckDB. The client default is 45 minutes (`FRESHNESS_TOLERANCE_DEFAULT`
in the pinned `dbt_state/config.py`). Set `run_cache_freshness_tolerance: 0s`
on a profile output, or `state={'lag_tolerance': '0s'}` on a model, to rebuild
on every upstream change. Upstream query changes and External modifications
of a dbt-built upstream always rebuild consumers.

## Validation

```sh
# Postgres required, no cloud credentials.
./scripts/duckdb-ci.sh --jobs 4
cargo test --workspace
```

`scripts/duckdb-ci.sh` runs `scripts/duckdb-ci.py`, which syncs the locked
Python environment, builds all workspace targets once, then runs, with bounded
parallelism:

- every ignored test in each harness target listed in `FIXED`: table evidence
  and Lag tolerance (`duckdb_python`), function, hook, type and collation
  semantics (`duckdb_functions_python`), formatting identity, recovery,
  interruptions, parallel workers, seeds, cached data tests, result copies,
  incremental models and the mixed example project (`duckdb_workflow`);
- the oracle unit tests in `integrations/duckdb/tests/generated`;
- bounded generation (`duckdb_generated`, default seeds and steps in
  `crates/harness/tests/duckdb_generated.rs`) with the mutation probes.

Every pull request runs this script after `cargo test --workspace` in the
`test` job of `.github/workflows/ci.yml`. Each case gets its own Postgres
schema and DuckDB file. Logs, `timing.json` and generated traces go to
`artifacts/duckdb`; failed fixed-case projects stay under
`artifacts/duckdb/fixed`. CI uploads the directory on every run and keeps it
for 14 days. ADR 0001 sets an added-runtime target of about five minutes;
read actual timings from `timing.json`.

**Extended run.** A weekly schedule and manual dispatch run eight seeds with
40 episodes each (manual `generated_steps` overrides 40) and keep artifacts for
30 days. Reproduce locally:

```sh
DBT_STATE_GENERATED_SEEDS=0,1,2,3,4,5,6,7 DBT_STATE_GENERATED_STEPS=40 \
  ./scripts/duckdb-ci.sh --generated-only
```

**Replay a trace** after installing and building:

```sh
export DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python"
export DBT_STATE_SERVER_BIN="$PWD/target/debug/dbt-state-server"
DBT_STATE_GENERATED_REPLAY=/path/to/trace.json \
  cargo test -p dbt-state-harness --test duckdb_generated -- --ignored --nocapture
```

See [generated checks](duckdb-generated.md) for artifacts and minimization.

## Snowflake compatibility assessment

Each DuckDB difference is scoped and tested separately. DuckDB success does
not establish Snowflake DDL, grants, time travel or hosted policy.

| Difference | Assessment and evidence |
| --- | --- |
| Observed-change ordinals instead of modification timestamps | DuckDB has no per-table modification timestamp. Identical external rewrites may be reused; known writes still advance. `duckdb_python`, `duckdb_parallel`, `duckdb_recovery`. Snowflake timestamps unchanged. |
| Views carry definition, schema and leaf dependencies | Detects physical view and binding changes. `duckdb_python` view test. Captured Snowflake view policy unchanged. |
| DuckDB identifier rules and a narrow SELECT identity | Kept behind the Warehouse rules; unsupported syntax gets no equivalence. `duckdb_identity`, `duckdb_identity_python`, `duckdb_copy`. Snowflake identities unchanged. |
| CTAS copies with source inspection | Local result copy; unsupported physical requirements build normally. `duckdb_copy_python`, `duckdb_incremental_python`. Snowflake CLONE SQL and grants unchanged. |
| Failed-copy confirmation clears unproven provenance | Shared correctness correction, not characterized hosted behavior. See [the assessment](duckdb-identity-and-copy.md#snowflake-confirmation-compatibility-assessment); `failed_clone_confirmation_does_not_certify_source_logic` in `crates/harness/tests/clone_lifecycle.rs`. |

The shared stale-source policy still applies, and tests accept the permitted
older results. The DuckDB gate weakens no Snowflake capture or replay exemption.
