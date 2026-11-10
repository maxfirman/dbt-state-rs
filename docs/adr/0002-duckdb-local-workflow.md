# Integrate DuckDB through Python dbt and copy only eligible results

## Decision

Start with Python dbt Core plus dbt-duckdb and a maintained DuckDB extension or
patch to the Python State plugin, subject to a focused compatibility
investigation. Both existing State clients exclude DuckDB; this choice uses
the Python plugin's warehouse-extension interface rather than starting with a
Fusion fork. Fusion integration is a separate milestone. This is a chosen
direction, not a claim that Python client compatibility has been demonstrated.

Source inspection of Python State commit
`b0f3030c90dd0766f2d1625b3305829d881fc2ae` found that adapter registration
alone is insufficient: a pre-transport CI authentication guard needs an
explicit local-mode path, and request models/serialization need warehouse
namespace support. Relevant per-node state fields also need checking against
this server's requirements. The first compatibility gate must demonstrate
credential-free BUILD, confirmation and SKIP across separate real dbt
processes, invalidation after changes, and isolation between database files.

Note: the pinned released client already serializes warehouse namespace and
node identity; the extension supplies local authentication handling and
file/generation scope. Pinned versions are listed in [the workflow guide](../duckdb.md#supported-versions).

Retain the existing client/server split: the Python integration collects
warehouse evidence and executes builds and copies; the Rust server owns shared
decision policy and execution history in Postgres. Warehouse-specific SQL
interpretation and copy capabilities have explicit interfaces. The server
does not connect to DuckDB, and DuckDB does not replace the state store.

Initially support one persistent DuckDB file with environments represented by
separate schemas. Each integration test receives an isolated file, and State
history must remain isolated between distinct files. Copying between attached
files, in-memory databases and remote or file-backed sources are later
extensions.

Manual changes to supported local tables between dbt runs must be detected,
including changes made outside this integration. Scanning small tables is an
acceptable cost for reliable evidence. DuckDB's documented table metadata has
no per-table last-modified timestamp; [ADR 0003](0003-duckdb-observed-change-evidence.md)
defines the accepted observation semantics. Resolved by ADR 0003; the
mechanism is described in [DuckDB evidence](../duckdb-evidence.md). Each dbt invocation requires exclusive write access to its
DuckDB file. Manual edits are supported between runs; overlapping invocations
and concurrent external writers are outside the initial scope. Normal dbt
worker threads and dependency ordering remain in scope, subject to a correct
metadata/execution lifecycle. Missing or conflicting evidence prevents reuse.

Use the same supported freshness policies as Snowflake, including explicitly
permitted stale-source copying. Tests must represent those allowed outcomes
rather than always demanding the newest possible result. This does not imply
that copying a source which was externally edited after its recorded build is
safe; physical source validity and upstream freshness are separate checks.

Amended by ADR 0007: freshness also compares recorded upstream epochs and
producers, gated by Lag tolerance; on DuckDB, Build age is measured from the
server's receipt of the build's confirmation.

Treat result copying as a shared operation with warehouse-specific
capabilities. Retain the existing CLONE wire names for compatibility. DuckDB
CTAS copies rows and column names/types but does not preserve indexes,
constraints or defaults. Copy only when those capabilities satisfy the model's
requirements; otherwise build normally. Reconstructing unsupported physical
properties is outside the initial scope.

The first-release acceptance set is SQL tables, views, seeds, cached data tests
and one pinned incremental strategy, including initializing an incremental
model by copying from another schema. Snapshots, Python models and custom
materializations are deferred; unsupported cases use ordinary dbt execution
without State reuse. Profile-based prod/dev deferral is sufficient initially.
The Python client does not currently call the server's State selection or
deferred-relation lookup RPCs; integrating those is a separate feature, while
their server behavior remains covered through the protocol harness.

Source evidence: [Python adapter interface](https://github.com/dbt-labs/dbt-state/blob/b0f3030c90dd0766f2d1625b3305829d881fc2ae/clients/dbt_state/src/dbt_state/adapters/base.py),
[Python runner authentication guard](https://github.com/dbt-labs/dbt-state/blob/b0f3030c90dd0766f2d1625b3305829d881fc2ae/clients/dbt_state/src/dbt_state/runner.py#L312-L340),
[Python request models](https://github.com/dbt-labs/dbt-state/blob/b0f3030c90dd0766f2d1625b3305829d881fc2ae/libs/query_cache_common/src/query_cache_common/models/services/sql_service_models.py#L18-L50),
[DuckDB table metadata](https://duckdb.org/docs/2.0/sql/meta/duckdb_table_functions#duckdb_tables),
and [DuckDB CTAS](https://duckdb.org/docs/2.0/sql/statements/create_table).
