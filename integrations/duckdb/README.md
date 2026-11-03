# Local DuckDB State client

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

Table evidence supports BOOLEAN, signed/unsigned integers through HUGEINT,
VARCHAR, BLOB and fixed-scale DECIMAL. FLOAT/DOUBLE, dates/times, complex and
extension types, views, attached/external relations, and result copying have
unknown evidence. User source-freshness overrides never substitute wall time.
Arbitrary hooks writing other relations and unsupported/custom materializations
are outside this table contract. The tested post-hook writes its own model
result; evidence is collected after materialization return, which follows dbt's
commit and outside-transaction post-hooks. Repeated completion reads do not
advance the sequence again. A known successful ordinary build advances it once,
including when State submit failed and dbt built without a State request.

The selected first-release incremental strategy is dbt-duckdb's `delete+insert`;
seeds, views, tests and incremental lifecycle support have separate milestones.
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

Missing both evidence tables rotates the generation before creating a new
sequence; partial or incompatible metadata fails closed for the invocation.
Recovery markers, exclusive invocation locking and explicit reset are the next
milestone. For this milestone use one invocation with exclusive write access;
manual SQL edits are supported between runs. Backup restoration requires the
forthcoming reset operation before reuse; it is not automatically detected.

SQL formatting equivalence is limited to a [validated SELECT subset](../../docs/duckdb-sql-identity.md).
Run its separate real-client rows/schema gate with:

```sh
DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python" \
  cargo test -p dbt-state-harness --test duckdb_identity_python -- --ignored
```
