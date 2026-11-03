# Local DuckDB State client

This installable extension uses the maintained `dbt-state` Python plugin and
real dbt Core execution. It is a compatibility milestone for issue #4, not yet
a reusable cache: unknown freshness evidence deliberately produces BUILD and
leaves model/seed requests pending. Durable observations and confirmation are
tracked in issue #5.

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
The test asserts production execution records for the cold BUILD responses,
transported node identity, isolated namespaces for same-named relations in two
files, stable namespaces on restart, conservative fallback after incompatible
metadata, and no successful model/seed confirmations with unknown evidence.
The optional Rust test is ignored in the default suite until the later CI gate
milestone; this script explicitly runs it.

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

`_dbt_state.database_identity` is reserved for a versioned generation UUID.
Namespaces hash the canonical file path, device/inode identity and generation;
copying a database to another path cannot share state. Metadata with an unknown
version or invalid generation uses a fresh untrusted namespace for the current
invocation and keeps all freshness unknown. Do not edit the reserved schema.
Backups restored over the same file will need the explicit reset operation
introduced by the recovery milestone; automatic restoration detection is not
claimed.

Views, external sources, custom materializations and result copying have no
reuse support in this milestone. The adapter supplies unknown freshness,
including for user source-freshness overrides, and never fabricates clock-based
epochs. The selected first-release incremental strategy is dbt-duckdb's
`delete+insert`; execution/reuse coverage belongs to the incremental ticket.
The upstream client emits an UNIMPLEMENTED telemetry warning because this
server does not implement ClientTelemetry; model decision RPCs use the existing
production SQL and Execution services. Existing Snowflake replay remains the
hosted conformance contract.
