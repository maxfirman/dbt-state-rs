# Local DuckDB State client

For the complete credential-free project, supported feature matrix, recovery
procedure and mandatory CI commands, start with the [DuckDB workflow](../../docs/duckdb.md).

This installable extension uses the maintained `dbt-state` Python plugin and
real dbt Core execution. Small deterministic SQL table workloads now BUILD,
confirm after committed completion, and SKIP in subsequent dbt processes.

Run the credential-free smoke gate from the repository root:

```sh
# Postgres must be running; DATABASE_URL overrides localhost:55441/dbtstate.
./scripts/duckdb-smoke.sh
```

The command installs the locked Python environment with `uv`, starts the
production gRPC router against a fresh migrated Postgres schema, and executes
real `dbt build` processes with two dbt worker threads. The project contains a
seed, a table model and a singular data test. Independent literal assertions
check output rows (including duplicate seed rows) and the dbt test outcome.
The gate positively observes BUILD/SKIP and successful confirmations through the
upstream public client. Separate processes check unchanged restarts, identical
manual rewrites, input edits (including duplicates and NULL), own-target edits,
drops and schema changes, logic changes, and identical-result known writes.
Independent SQL recomputes expected tables; literal examples guard the oracle.
It also covers scalar representations, post-hook completion, unknown types and
metadata, same-named tables in independent files, and ordinary execution after
an injected State transport failure. Evidence reads see frozen/regressed wall
time while transport retains its real deadline clock.
Real-client Rust tests are ignored by plain `cargo test` because they require
the pinned Python environment. The mandatory `scripts/duckdb-ci.sh` gate installs
that environment and explicitly runs every fixed case plus bounded generation.

For your own project, install with `uv pip install ./integrations/duckdb`, then:

```sh
DBT_STATE_LOCAL=1 RUN_CACHE_API_URL=127.0.0.1:50051 \
  RUN_CACHE_API_SECURE=false dbt-state-duckdb build --profiles-dir /path/to/profiles
```

Use a persistent local DuckDB file. The wrapper invokes the normal dbt CLI;
SQL is compiled and executed by dbt-duckdb, and decisions use the upstream
client's public gRPC transport. `DBT_STATE_LOCAL=1` explicitly patches the
runner's early headless-authentication guard only for DuckDB. It also requires
an explicitly insecure loopback endpoint. Without this mode, the upstream
authentication and adapter behavior are preserved. Calling ordinary `dbt`
directly does not activate this extension.

The tested combination is Python 3.12, dbt Core 1.10.15, dbt-duckdb 1.10.0,
DuckDB 1.4.1, dbt-state 2.62.6, and query-cache-common/query-cache-protobuf 2.7.0.
All transitive dependencies are pinned in `uv.lock`. Unlike the planning source
SHA, this released State package already serializes warehouse namespace and
node identity across submit, record, and copy paths. The extension replaces its
adapter-credential namespace with file/generation scope; no upstream source
checkout or unbuildable monorepo packaging hooks are required.

The `_dbt_state` schema holds reserved, versioned evidence and a generation UUID.
Namespaces hash the canonical file path, device/inode identity and generation;
copying a database to another path cannot share history. Normal restarts retain
the same scope and eligible results SKIP. Recreating a file, missing metadata,
incompatible formats, malformed evidence, invalid clock ordering, and an
unfinished prior invocation establish a fresh generation before State requests.
Old history stays in Postgres but cannot authorize reuse in the new scope.
Do not edit the reserved schema.

Table evidence supports BOOLEAN, signed/unsigned integers through HUGEINT,
VARCHAR, BLOB and fixed-scale DECIMAL. FLOAT/DOUBLE, dates/times, complex and
extension types, collated tables and attached/external relations have unknown
evidence. User source-freshness overrides never substitute wall time.
Only static own-target UPDATE post-hooks with column/literal integer arithmetic
are eligible for reuse. The only supported late-rendered context is `{{ this }}`;
functions, subqueries, predicates, other relations and opaque/dynamic hook forms
have unknown evidence. All pre-hooks have unknown evidence because they execute
after State's inspection and can change session function bindings. Seed hooks
also have unknown evidence. These workloads execute ordinarily on every run.
The full pre/post-hook configuration is part of the versioned local identity,
including empty hook lists, so changing or removing a hook rebuilds. The tested
post-hook writes its own model result; evidence is collected after materialization
return, which follows dbt's commit and outside-transaction post-hooks. Repeated completion reads do not
advance the sequence again. A known successful ordinary build advances it once,
including when State submit failed and dbt built without a State request.

SQL and seed requests always include the local `functions-hooks-v1` execution
contract marker, including workloads without hooks. Upgrading cold-misses earlier
unverified outcomes even if a former hook or warehouse macro has already been
removed. Incremental initialization uses the `duckdb-incremental-init-v2` identity;
pre-upgrade sources cannot populate a development target before the new SQL
contract is checked. Rebuild the production source under the new contract before
expecting initialization copies again.

The selected incremental strategy is explicit `delete+insert`, with a single
non-NULL key and `on_schema_change='fail'`. Real profile-based development
initialization, ordinary updates and reuse are described in
[the incremental contract](../../docs/duckdb-incremental.md). Seed, test and view
support is described below.
The upstream client emits an UNIMPLEMENTED telemetry warning because this
server does not implement ClientTelemetry. Existing Snowflake replay remains
the hosted conformance contract.

## Evidence format and ordering

`_dbt_state.evidence_clock` version 1 holds one durable positive int64 ordinal
for the entire file/generation. `_dbt_state.relation_evidence` stores version 1
schema/content SHA-256 digests and each relation's ordinal. These are protocol
`last_modified_epoch` values interpreted as DuckDB **ordinals**, not Unix
milliseconds. Current shared freshness/target policies compare ordering only;
Snowflake timestamps and decision policy are unchanged. A future policy needing
elapsed time must explicitly support this distinction before enabling it here.

A single process-wide lock serializes fresh transaction snapshots, scans and
metadata writes. Unchanged supported observations retain their ordinal. Changed
schema/content, first observations and known committed writes allocate the next
global ordinal in the same transaction as the digest. Identical known writes
therefore advance ordering; a manual rewrite with identical observed contents
and schema does not. Operations in the same clock tick, regressed wall time and
normal restarts preserve ordering without consulting a clock. This is observed
change evidence, not a complete write log: edits reversed between observations
may be invisible. Epoch caches and asynchronous epoch futures are bypassed;
completion clears other adapter metadata before downstream execution proceeds.

The byte format uses eight-byte unsigned big-endian length framing. Schema
hashes start with ASCII `duckdb-schema-v1`, then, for each column in ordinal
order, framed decimal ordinal and framed UTF-8 `str` values from DuckDB 1.4.1's
DESCRIBE tuple (name, type, nullability, key, default, extra). Content hashes
start with ASCII `duckdb-content-v1`. Each row concatenates fields: byte `N`
for NULL, otherwise byte `V`, framed DuckDB type, and framed payload. Payloads
are UTF-8 decimal integers, ASCII `True`/`False`, exact UTF-8 VARCHAR, raw BLOB,
or exact decimal text with the type's fixed scale. Each row is framed, sorted
lexicographically as bytes, and concatenated, preserving duplicate rows while
ignoring physical order. Unsupported scans remain unknown and cannot confirm.
Changes to this format require a new version and invalidation.

## Reset, restoration and exclusive writers

Reset preserves user tables, views and schemas, replaces the reserved evidence
tables, and changes the generation. The next eligible run BUILDs and confirms;
subsequent unchanged runs can SKIP. No server or cloud credential is required:

```sh
dbt-state-duckdb reset --database /absolute/path/warehouse.duckdb
```

Stop all dbt invocations and external writers before restoring a backup. Restore
the file, then run this reset command **before using State again**. Restored
metadata may contain a clean marker, generation and clock from an older point
in history; automatic detection of arbitrary restoration is outside the
contract. Reset is required even if the restored rows look unchanged.

Each cooperating invocation and reset takes a nonblocking advisory lock beside
the canonical database path (`warehouse.duckdb.dbt-state.lock`). An overlapping
invocation/reset fails before warehouse execution; never remove the lock file
while a process is running. Symlink aliases share the lock; hard-linked files
are rejected. External tools do not honor this advisory lock: manual writes are
supported only between invocations, and overlapping external writers are not
supported. Normal dbt worker threads remain within the invocation's lock.

Before project execution/on-run hooks, the integration commits a dirty marker.
It clears this marker only after successful CLI completion, adapter cleanup and
metadata worker draining. Failed/uncertain State completion RPCs or unknown
known-write evidence leave the marker dirty even when dbt itself succeeds.
Interrupted invocations therefore cold-build in a fresh generation on restart,
including a crash between warehouse commit and confirmation or immediately
after confirmation. This deliberately sacrifices reuse following uncertainty;
it does not claim a transaction spanning DuckDB and Postgres.

The recovery regression runs real dbt commands against the production router:

```sh
DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python" \
  cargo test -p dbt-state-harness --test duckdb_recovery -- --ignored --nocapture
```

It checks literal rows and public BUILD/confirmation/SKIP outcomes for reset,
copy/recreation, backup restoration with reset, damaged metadata, a frozen
historical format captured using commit `3907d3c`, uncertain confirmation,
process interruption and invocation/reset lock exclusion. Failed tests retain
the temporary project and logs for reproduction.

SQL formatting equivalence is limited to a [validated SELECT subset](../../docs/duckdb-sql-identity.md).
Run its separate real-client rows/schema gate with:

```sh
DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python" \
  cargo test -p dbt-state-harness --test duckdb_identity_python -- --ignored
```

The focused interruption matrix also exercises the gaps before materialization,
while actual DuckDB SQL is running, a dbt SQL execution error,
before/during/after the evidence transaction, and before confirmation. It verifies
literal physical rows after each process termination, then requires a fresh-generation BUILD/confirmation and subsequent
SKIP. The public protocol regressions check correlated, immutable confirmation
retries and rejected Record batches without reading private store tables:

```sh
DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python" \
  cargo test -p dbt-state-harness --test duckdb_interruptions -- --ignored
cargo test -p dbt-state-harness --test duckdb_outcomes
```

Set `INTERRUPT_ONLY_PHASE` to a named phase from `tests/interruptions.py` to replay
one interruption. Warehouse completion, evidence commit and State confirmation
are separate durability boundaries; recovery invalidates uncertain scope rather
than claiming atomicity across DuckDB and Postgres or exactly-once execution.

## View evidence and dependencies

Supported local views can BUILD, confirm and SKIP across dbt processes. Each
view observation hashes its committed DuckDB catalog definition and its bound
DESCRIBE schema, using `duckdb-view-definition-v1` and
`duckdb-view-schema-v1` prefixes. Schema fields use the length framing above,
in column order. The shared durable ordinal records an observed definition or
schema change, or a known successful view DDL write; it does not timestamp the
rows a view returns. Freshness still comes from the recursively resolved leaf
tables, and each physical dependency view's name, definition and resolution
context participates in the request identity. DuckDB view requests intentionally
include dependencies, unlike the captured Snowflake view policy.

Metadata traversal reads current physical definitions in a fresh transaction
for every request. Compiled view SQL never populates the physical definition
cache. These uncached reads and post-commit completion observations ensure a
child in the same dbt DAG sees its parent's committed definition/schema. A
wildcard view whose stored schema no longer binds receives unknown own-target
evidence and rebuilds; the fixed real-client gate exercises both column addition
and removal through a view -> nested view -> table graph with four workers.

The initial subset accepts SELECT/UNION queries over schema-qualified local
relations, with ordinary projections and a small set of deterministic built-in
functions (exact casts, sum/count/min/max, concat/chr/unhex, coalesce/nullif and
lower/upper). Callable names also require current built-in catalog evidence from
both the dbt worker session and the persistent database. Any user-defined override
of a built-in function or operator name, including a temporary macro, produces
unknown evidence for every query and prevents reuse and incremental
initialization copying. Operators and implicit calls also use mutable function
bindings. This conservative check rejects overrides in every schema even when the current
query does not explicitly use the overridden name; precise
overload/search-path resolution is outside the initial subset. Order-sensitive
LIMIT/OFFSET/SAMPLE/DISTINCT ON, windows, collations, special join methods,
approximate intermediates and AVG are outside this subset. The same eligibility gate applies
to submitted models/tests as well as physical view definitions.
Search-path-dependent unqualified physical references, cycles, missing
relations, table functions, external relations and unsupported functions/syntax
produce explicit unknown dependency evidence. dbt executes these workloads
normally; unknown inputs cannot authorize SKIP. Fresh physical metadata is
required even when the compiled model text is unchanged. This scope does not
add result copying or alter Snowflake requests or policy.


Physical tables whose stored DDL contains COLLATE have unknown evidence: DuckDB
DESCRIBE omits column collation, and MIN/MAX over collated ties can depend on
physical row order. Explicit expression collations and a configured
`default_collation` also prevent local reuse. The fixed gate includes a
collated-input reorder with a BLOB output, so the regression demonstrates
unknown input evidence without relying on an unsupported output type. Run
that focused case with `VIEW_SCENARIO=collation` and the view test filter.

### Cached data tests

With `run_cache_enable_data_tests: true`, supported data tests reuse the complete
`failures`, `should_error`, and `should_warn` result. A cached severity=error
failure retains dbt's failing status and nonzero exit code; a warning retains its
warning status. Completed test failures are clean invocation outcomes for evidence
recovery. SQL execution errors and incomplete/unconfirmed results cannot certify
a cached pass. Tests have no physical target and never receive a target build
epoch: freshness comes from their recorded upstream evidence.

Local requests carry a versioned project ID derived from the canonical project
root and project name, separately from the DuckDB file/generation namespace.
Same-named projects at different roots therefore have separate cached outcomes,
while sharing the physical warehouse namespace. Moving a project root safely
starts new test history. Organization isolation remains enforced by the service.

`store_failures` / `store_failures_as` and the CLI `--store-failures` option use
ordinary dbt execution, including failure-relation DDL; State reuse for those
relations is unsupported until their separate lifecycle is validated.

The real-client cached-test gate uses three dbt worker threads, independent SQL
counts and literal duplicate/NULL inputs, positive cached pass/fail/warn outcomes,
both mutation directions, same-named project and copied-file isolation, unknown
input types, missing inputs, and repeated SQL errors. A separate gate verifies
stored-failure fallback. Run both with the pinned Python environment:

```bash
DBT_STATE_DUCKDB_PYTHON=/tmp/dbt-duckdb-venv/bin/python \
PYTHONPATH="$PWD/integrations/duckdb/src" \
cargo test -p dbt-state-harness --test duckdb_data_tests_python -- --ignored
cargo test -p dbt-state-harness --test duckdb_data_tests --test data_test_freshness
```

### Parallel workers

The supported writer contract permits normal dbt workers in one invocation.
Evidence scans and completion writes share a process lock and durable global
ordinal; each scan uses a fresh committed snapshot. Successful materialization
publishes completion evidence before returning to dbt's DAG scheduler, so children
observe their committed parents. DuckDB epoch reads bypass asynchronous timestamp
caches. Independent SQL branches still run on separate dbt worker connections.

The four-worker fan-out/fan-in gate delays public RPC responses until three
independent branches have submitted concurrently. It checks correlated parent
confirmation/input epochs, unique globally ordered completions, literal joined
rows with duplicates and NULLs, and column types. Unchanged runs must SKIP;
logic changes producing identical rows must advance evidence and rebuild their
children even with a frozen evidence wall clock. A genuine SQL worker error
withholds the join, leaves recovery dirty, and requires fresh-generation BUILD
then SKIP on restart.

```sh
PYTHONPATH="$PWD/integrations/duckdb/src" \
DBT_STATE_DUCKDB_PYTHON=/tmp/dbt-duckdb-venv/bin/python \
  cargo test -p dbt-state-harness --test duckdb_parallel -- --ignored
```

The invocation/reset advisory file lock rejects overlapping cooperating State
commands and remains held until workers and metadata jobs finish (covered by
`duckdb_recovery`). It cannot prohibit arbitrary external writers. Give one
invocation exclusive write access to the DuckDB file; manual edits are supported
between invocations. Concurrent external writes and overlapping ordinary dbt
commands remain outside this support contract.

## Seeds

Seeds use the upstream SubmitValues request, shared server decisions and ordinary
`dbt seed` materialization. Successful committed loads advance physical evidence
before confirmation and dependent model submission. Unchanged CSV/configuration
and current target evidence permit SKIP across processes; a file hash alone does
not authorize reuse. Configuration-only changes also record a known write even
when resulting rows are identical. Schema/type transitions follow dbt's normal
rules: use `--full-refresh` where required. Unsupported physical types such as
DOUBLE still execute normally and never receive invented seed evidence.

The fixed real-client seed gate checks literal duplicate/NULL rows, column types
and downstream aggregates independently. It covers CSV and quoting/type config
changes, failed conversion/retry, returned unsuccessful adapter outcomes,
write-only failure publication, dropped/edited seed targets, and a copied database
at another path, and organization isolation. It requires positive seed/model SKIP and correlated successful
confirmation, and rejects successful publication for failed or unknown seeds.
Run with the same pinned environment as the smoke gate:

```sh
DBT_STATE_DUCKDB_PYTHON=integrations/duckdb/.venv/bin/python \
  cargo test -p dbt-state-harness --test duckdb_seeds_python -- --ignored --nocapture
```

The completion guard also covers ordinary DuckDB model materializations. A
returned error/partial outcome keeps target evidence unknown for the invocation
and recovery dirty, so neither confirmation nor write-only recording can certify
an old target. Completed data-test failure/warning results use their distinct
aggregate-result lifecycle and remain cacheable.

## Result copying

Eligible table models in profile schemas `prod` and `dev` can copy results with
CTAS and later SKIP. Validated view graphs preserve exact base-table names and
bind references to their definitions. The common path validates source
properties and current physical evidence, supports up to two bounded copy hops,
and falls back to ordinary dbt execution on unsupported requirements or failed
copies. Incremental models also have a narrower pre-compilation initialization
contract. See [result copying](../../docs/duckdb-result-copy.md) and
[incremental execution](../../docs/duckdb-incremental.md), including permitted
stale-source policy and the Snowflake confirmation assessment.

```sh
DBT_STATE_DUCKDB_PYTHON=integrations/duckdb/.venv/bin/python \
  cargo test -p dbt-state-harness --test duckdb_copy_python -- --ignored --nocapture
cargo test -p dbt-state-harness --test duckdb_copy --test clone_lifecycle
```

The [generated physical-state gate](../../docs/duckdb-generated.md) complements
these fixed real-dbt checks with reproducible lifecycle traces, independent
rows/schema checks, real server restarts and oracle mutation probes.
