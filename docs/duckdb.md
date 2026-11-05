# Local DuckDB workflow

The first release runs real Python dbt projects against a persistent local
DuckDB file and the Rust gRPC service, with Postgres storing State history.
It supports BUILD, SKIP and eligible prod/dev result copies without cloud
credentials. DuckDB tests establish the declared local reuse-safety contract;
the existing Snowflake captures establish hosted conformance for their covered
cases. There is no official hosted DuckDB oracle.

## Start a fresh project

Requirements: Rust (the repository toolchain), `protoc`, `uv`, Python 3.12 and
Postgres. From the repository root, start Postgres if needed:

```sh
docker run -d --name dbt-state-pg -e POSTGRES_USER=dbtstate \
  -e POSTGRES_PASSWORD=dbtstate -e POSTGRES_DB=dbtstate \
  -p 55441:5432 postgres:17-alpine
```

Start the server in its own terminal. Startup applies Postgres migrations:

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

# Stage the shared raw inputs, then run all models and the data test.
uv run --project integrations/duckdb dbt-state-duckdb seed --target prod
uv run --project integrations/duckdb dbt-state-duckdb build --target prod --exclude resource_type:seed
# Separate processes now reuse the seed and every supported result.
uv run --project integrations/duckdb dbt-state-duckdb seed --target prod
uv run --project integrations/duckdb dbt-state-duckdb build --target prod --exclude resource_type:seed

# dev's profile defers to prod; missing eligible results initialize by CTAS.
uv run --project integrations/duckdb dbt-state-duckdb build --target dev --exclude resource_type:seed
uv run --project integrations/duckdb dbt-state-duckdb build --target dev --exclude resource_type:seed
```

The example has four dbt worker threads, an INTEGER seed with duplicate/NULL
rows, an ordinary table, a view, an incremental ledger and a cached data test.
The raw seed is shared by both schemas. Its small `generate_schema_name` macro
uses the explicitly configured seed schema `raw`; ordinary models use `prod`
or `dev`. The filename must remain `warehouse.duckdb`: the incremental SQL
explicitly binds `warehouse.raw.inputs`. Put copies in different directories
when experimenting with separate files. Incremental initialization deliberately
accepts literal physical base-table references rather than `ref()`/`source()`
Jinja, so seed staging is a separate command. This is also why `build` excludes
the seed. Table/view/test dependencies use ordinary dbt `ref()` ordering.

With this input, `orders` and `order_view` contain `(1,11), (2,21), (2,21),
(3,NULL)`; `ledger` contains `(1,10), (2,20), (2,20), (3,NULL)`. Both columns
are INTEGER and `nonnegative` passes. The mixed gate verifies these literal
rows and types independently, observes public State decisions and correlated
confirmations, and requires real table and incremental copies followed by SKIP.
It runs the checked-in example from a fresh directory, recovers from an actual
SQL worker error, then restores/resets it
and checks a second copied database file. The focused worker gate additionally
forces simultaneous fan-out submissions and tests fan-in visibility/recovery.

To change inputs, edit the copied CSV, run `seed`, then `build` in prod. The
pinned delete+insert ledger updates keys selected by the query and retains rows
whose keys disappear from the source. A dev initialization can intentionally
copy a physically valid older prod result under the shared stale-source policy;
it is not an unconditional latest-data rebuild. See the exact
[incremental](duckdb-incremental.md) and [copy](duckdb-result-copy.md) contracts.

## Supported versions and features

The lockfile pins all transitive dependencies. Upgrades require running the
entire gate; these versions are the validated combination, not open-ended ranges.

| Component | Pinned/tested version |
| --- | --- |
| Python | 3.12 |
| dbt Core | 1.10.15 |
| dbt-duckdb | 1.10.0 |
| DuckDB | 1.4.1 |
| dbt-state Python client | 2.62.6 |
| query-cache-common / query-cache-protobuf | 2.7.0 |

| Workload/capability | First-release contract |
| --- | --- |
| SQL tables | Same-target reuse with supported deterministic inputs and known own-target evidence |
| SQL views | Checked local dependency graphs; definition/schema observations and recursively resolved leaf freshness |
| Seeds | CSV/config identity plus current physical evidence; ordinary dbt seed execution and completion |
| Data tests | Complete cached pass/fail/warning outcomes; project-scoped identity; stored failures execute ordinarily |
| Incremental | Explicit `delete+insert`, one non-NULL identifier key, explicit `on_schema_change='fail'` |
| Result copies | Eligible local prod/dev tables, CTAS rows/names/types, validated current source provenance; narrower incremental initialization |
| Parallelism | Normal dbt workers inside one exclusive invocation, including dependency completion visibility |
| Environment scope | Separate prod/dev schemas in one persistent local file; separate files have separate history |
| Input scalar evidence | BOOLEAN, signed/unsigned integers through HUGEINT, VARCHAR, BLOB, fixed-scale DECIMAL |
| Unsupported | Snapshots, Python/custom materializations, other incremental strategies, attached/in-memory/remote files, arbitrary extensions and unsupported SQL/property requirements |

FLOAT/DOUBLE, date/time, complex/extension types and collated tables receive
unknown evidence. Unknown evidence prevents reuse; ordinary dbt execution still
runs. Unsupported materializations/strategies bypass State handling. Copy-only
limitations (constraints/defaults/indexes/hooks/contracts, uncertain identity,
unsupported functions, unknown dependencies) take ordinary execution or strict
same-target reuse when independently eligible. CTAS never promises arbitrary
physical-property preservation. See [SQL identity](duckdb-sql-identity.md),
[result copying](duckdb-result-copy.md) and the [integration reference](../integrations/duckdb/README.md)
for precise exclusions. Server-backed State selectors and deferred-relation RPC
integration in this Python client remain separate work. Calling ordinary `dbt`
does not install the extension; use `dbt-state-duckdb`.

## Evidence, exclusive writers and recovery

`_dbt_state` is reserved integration metadata. Do not edit it or use it for
project relations. It contains versioned schema/content fingerprints, a global
durable ordinal and a generation UUID. Protocol epochs represent DuckDB
observation/write ordering, not Unix time. Supported unchanged scans retain
their ordinal; changed observed contents/schema and every known committed build
or copy advance it, even if a known write produces identical rows. Identical
manual rewrites or edits reversed before observation can be invisible. This is
observed-change evidence, not a complete write log.

File scope includes canonical path, device/inode and generation. Copying or
recreating a file cannot adopt another file's history. Normal process restarts
preserve scope. Missing/incompatible/corrupt evidence or an unfinished/uncertain
invocation forces a fresh generation and cold execution. Old Postgres history
remains stored but cannot authorize reuse in the new scope. Execution, evidence
commit and State confirmation are separate durability boundaries; a proposal
or pending operation is not a successful result.

Give each invocation exclusive write access to its file. A cooperating advisory
lock covers workers, metadata jobs and cleanup, and excludes simultaneous
State invocations/reset commands. Symlink aliases share the lock; hard links
are rejected. Do not remove the lock file while commands run. External tools
do not honor this lock: manual edits are supported between runs only.

**After restoring any backup, reset before the next State command.** Restored
metadata cannot reliably detect its own rollback. With all writers stopped,
restore the file, then run:

```sh
uv run --project integrations/duckdb dbt-state-duckdb reset --database "$DUCKDB_WORKFLOW_PATH"
uv run --project integrations/duckdb dbt-state-duckdb build --target prod --exclude resource_type:seed
```

Reset preserves user relations and starts a new generation. It deliberately
invalidates old reuse eligibility; a following unchanged run can SKIP after
successful cold execution. The mixed gate tests backup restoration plus this
procedure. The focused recovery/interruption gates cover damaged metadata,
old formats, lost confirmation, killed processes and lock exclusion.

## Mandatory and extended validation

```sh
# Postgres required; no cloud credentials. Installs the locked Python env,
# builds all workspace targets once, then runs fixed cases with four processes.
./scripts/duckdb-ci.sh --jobs 4
# Existing offline/Snowflake captures and shared regressions remain required.
cargo test --workspace
```

Every pull request runs all fixed real-dbt acceptance tests (smoke/table evidence,
views, formatting identity, recovery, interruptions, parallel workers, seeds,
cached tests/stored failures, table/view-chain copying, incremental and the
mixed project), the independent oracle unit tests and four generated seeds with
four random episodes plus the mandatory lifecycle spine and mutation probes.
Each fixed case starts an isolated Postgres schema and DuckDB file. Generated
traces start isolated scope and the normal production server binary built from
the same checkout. Bounded parallelism avoids competing Cargo builds; cases
never share warehouse files. The ordinary workspace CI job retains Snowflake
capture replay and all shared protocol/property/upgrade regressions.

Logs, generated seeds/traces/observations/server logs and `timing.json` are
written under `artifacts/duckdb`; CI retains them for 14 days. Failed fixed-case
projects remain in `artifacts/duckdb/fixed` and print their path in the per-case log. Generated failures have
a `trace.json` replay input and named oracle failure, with optional bounded
minimization. See [generated checks](duckdb-generated.md) for artifact semantics.
Replay an uploaded trace after installing/building:

```sh
export DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python"
export DBT_STATE_SERVER_BIN="$PWD/target/debug/dbt-state-server"
DBT_STATE_GENERATED_REPLAY=/path/to/trace.json \
  cargo test -p dbt-state-harness --test duckdb_generated -- --ignored --nocapture
```

Weekly scheduled and manually dispatched runs add eight seeds with forty random
episodes each (manual `generated_steps` overrides forty). They retain artifacts
for 30 days. Reproduce locally:

```sh
DBT_STATE_GENERATED_SEEDS=0,1,2,3,4,5,6,7 DBT_STATE_GENERATED_STEPS=40 \
  ./scripts/duckdb-ci.sh --generated-only
```

The approximately five-minute added-runtime goal is a target. The script reports
setup/build time, acceptance time, per-case time and total wall time; hosted CI
runtime must be read from actual runs rather than inferred from local timings.
No required case is dropped to meet that goal.

The complete local gate measured on 2026-11-04 passed all fourteen real-client/
generated cases and six independent oracle unit tests with `--jobs 4`:

| Phase | Measured wall time |
| --- | --- |
| Locked environment setup and workspace build | 184.58 s (3 min 5 s) |
| Fixed acceptance cases plus bounded generation | 619.08 s (10 min 19 s) |
| Total standalone gate | 803.67 s (13 min 24 s) |

This **exceeds the approximately five-minute target**. The command was
`./scripts/duckdb-ci.sh --jobs 4 --artifacts /tmp/ticket17-fullgate`, with
`CARGO_TARGET_DIR` pointing to the existing shared Rust dependency cache. All
workspace targets were relinked after source changes. The machine was a shared
Fedora 44 desktop, Intel i7-7700HQ at 2.80 GHz (four cores/eight logical CPUs),
62 GiB RAM, Python 3.12.12, Rust/Cargo 1.98.0, uv 0.12.21 and local Postgres at
port 55441. The CI-pinned uv 0.8.22 separately passed a locked environment
resolution dry-run. Fresh warehouse files and Postgres schemas were used in
both fixed and generated cases. `timing.json` records every case; the mixed
project took 116.89 s, the full incremental gate 281.59 s and the four-seed
runner 70.08 s under contention. CI reuses the earlier workspace build, but
**hosted-runner added time has not been measured**. These are local execution
measurements; further runtime work must preserve all mandatory cases.

## Snowflake compatibility assessments

These distinctions are individually scoped and tested; DuckDB success does
not establish Snowflake DDL, grants, time-travel or hosted-policy correctness.

| Difference | Assessment and regression evidence |
| --- | --- |
| Observed changes/ordinals instead of warehouse modification timestamps | Required because DuckDB has no per-table modification timestamp. Identical external rewrites may reuse; known writes still advance. `duckdb_python`, `duckdb_parallel`, `duckdb_recovery`; Snowflake timestamp behavior remains unchanged. |
| Views include definition/schema and leaf dependencies | Required to detect physical view and binding changes. `duckdb_python` view gate; captured Snowflake view policy remains unchanged. |
| Case-insensitive quoted identifiers and conservative SELECT identity | DuckDB rules stay behind dialect semantics; unsupported syntax prevents automatic equivalence. `duckdb_identity`, `duckdb_identity_python`, `duckdb_copy`; existing Snowflake identities/captures remain unchanged. |
| CTAS rows/schema capabilities and source inspection | Required local result-copy implementation; unsupported physical requirements execute ordinarily. `duckdb_copy_python`, `duckdb_incremental_python`; Snowflake CLONE SQL/grants remain separate. |
| Failed-copy confirmation clears unproven provenance | Intentional common correctness correction, not characterized hosted behavior. Individually assessed in [result copying](duckdb-result-copy.md#snowflake-confirmation-compatibility-assessment); `failed_clone_confirmation_does_not_certify_source_logic` and frozen captured successful-copy regressions. |

The shared stale-source policy is retained and explicitly tested with allowed
older results; DuckDB does not introduce an always-latest rule. No Snowflake
capture or replay exemption is weakened by the mandatory DuckDB gate.
