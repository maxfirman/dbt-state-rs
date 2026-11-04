# DuckDB result copying

The Python integration and production gRPC service support a first conservative
table copy slice. With one persistent DuckDB file, profile schemas named `prod`
and `dev`, and eligible model SQL, State can return CLONE containing CTAS, the
client executes and confirms it, and the next process can SKIP the target.
This is a local contract, not hosted DuckDB conformance.

Eligibility uses the validated [SQL formatting subset](duckdb-sql-identity.md),
fully qualified physical base-table inputs in the same catalog, known and unique
input evidence, and no view definitions. It substitutes only the root `prod`/`dev`
resolution context. Physical input names stay exact: `raw.input` and
`other.input` are distinct even if their rows are identical. Dependency graphs
are excluded, so swapped or ambiguous view bindings cannot authorize copying.
Unqualified inputs, other environment schemas, remote/attached inputs and
unsupported syntax use ordinary dbt execution. View graph expansion is documented separately; the narrower
[RegisterClone incremental contract](duckdb-incremental.md) validates template
identity before pre-compilation CTAS.

`fingerprint::copy_logic_hash` returns optional eligibility through the shared
Submit/Record pipeline. It is separate from the warehouse-wide Snowflake
namespace capability. DuckDB's new `dbt-state-duckdb-copy-v1` namespace identity
cannot reinterpret old history. Existing strict DuckDB identities and all
Snowflake fingerprint bytes remain unchanged. The initial policy permits one
copy hop: DuckDB copy-origin histories are excluded as automatic copy sources.

The client declares the `rows-schema-v1` capability only for ordinary table
models with no unsupported physical-property requirements. Contracts,
constraints, indexes, defaults, hooks, persisted documentation and unknown
adapter-specific requirements suppress copying. Before execution it checks the
actual source is a local base table without constraints, indexes or column
defaults/NOT NULL requirements. CTAS preserves rows and column names/types;
it does not reconstruct other physical properties. The generic TABLE label is
insufficient: recorded inspected sources carry `DUCKDB CTAS TABLE`.

Source evidence must exactly match the recorded source epoch, including known
existence. Source property inspection, fresh fingerprint/epoch validation and
CTAS read the same DuckDB transaction snapshot. A worker-local context carries
the correlated expected provenance; a separate early check is only an
optimization. The source schema must differ from the target schema, and the
target schema must equal the active profile environment. The client checks target correlation,
the allowed environments, and the exact server CTAS plan. Source changes,
deletion, unknown evidence and unsupported source properties take the ordinary
build fallback. Source validity is separate from upstream freshness: shared
SQL-submit fallback deliberately permits a physically valid recorded source
whose inputs advanced. Tests assert the explicitly allowed older result, not
an unconditional latest-data reference result in that case.

The client executes CTAS and records completion evidence after commit and before
confirmation. A returned copy proposal alone never publishes success. Failure
falls back through the ordinary client's materialization and confirms
`failed_to_clone=true` only after successful ordinary execution. SQL-submit
fallback keeps the requested SQL identity and clears false copy provenance.
Write-once outcomes and organization scoping remain shared.

## Snowflake confirmation compatibility assessment

The common ConfirmExecution previously ignored `failed_to_clone`. A failed
RegisterClone followed by an ordinary build could retain source-derived logic
identity and incorrectly adopt that build as the source model. The new behavior
clears copy provenance on this flag; for RegisterClone rows without submitted SQL
it also clears the unproven source-derived namespace identity. SQL-submit rows
retain their actual requested SQL identity. Duplicate confirmations cannot
change the first outcome.

This is an intentional individual correction for Snowflake as well as DuckDB.
The exact hosted failed-copy/fallback policy is uncharacterized; offline tests
are not new hosted evidence. Retaining an identity with no successful source
copy or submitted transformation would authorize wrong-logic reuse. Clearing it
has a small compatibility cost (the next request may rebuild or copy) and avoids
that correctness failure. Existing captured successful copies, response mirrors,
SQL generation and fingerprints remain the compatibility contract; no capture or
replay exemption changes. The production-router regression
`failed_clone_confirmation_does_not_certify_source_logic` demonstrates the
previous wrong SKIP and checks immutable duplicate confirmations.

## Validation

```sh
uv sync --project integrations/duckdb --locked
DBT_STATE_DUCKDB_PYTHON=integrations/duckdb/.venv/bin/python \
  cargo test -p dbt-state-harness --test duckdb_copy_python -- --ignored --nocapture
cargo test -p dbt-state-harness --test duckdb_copy --test clone_lifecycle
```

The real dbt gate uses profile-based prod/dev subprocesses, production gRPC and
isolated Postgres/DuckDB state. It checks independent SQL results, explicit
INTEGER column types, duplicates and NULLs, positive CTAS/confirmation/SKIP,
logic changes, copy-disabled execution, required defaults via normal dbt hooks,
source edits/deletion/index changes, a write after early validation, copy-plan
failure and successful fallback,
distinct physical inputs, and permitted stale-source copying. The precise
protocol gate additionally covers context allowlists, unknown/missing/duplicate
metadata, unsupported expiration properties and the actual fallback source
provenance. Existing Snowflake clone tests and golden replay remain required.
