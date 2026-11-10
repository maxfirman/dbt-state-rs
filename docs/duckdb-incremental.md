# DuckDB incremental models

The local integration supports incremental models with explicit
`incremental_strategy='delete+insert'`, a single identifier `unique_key`,
non-NULL keys and explicit `on_schema_change='fail'`, on the pinned versions
in [duckdb.md](duckdb.md#supported-versions). Cold initialization is wire
DELETE_INSERT (5); only an explicit full refresh uses FULL (1).

## Strategy semantics

The strategy deletes target rows whose key appears in the query result, then
inserts that result. Rows the query does not select stay in the target
relation, so a deleted source row is not deleted downstream, and an old key
changes only when the query selects it again. A timestamp or watermark
predicate must select updates to old keys. Duplicate result keys stay
duplicated. NULL keys accumulate under ordinary dbt execution; an existing
target relation with NULL keys bypasses State and cannot be a copy source.
The real-dbt test therefore checks literal expected rows for each update
sequence instead of a full-refresh result.

Schema evolution needs an explicit full refresh or ordinary execution with
another schema policy. With `fail`, dbt rejects added, removed or incompatible
columns; State does not migrate schemas.

These run as ordinary dbt with no State compile, submit or cached outcome
(`qualification.py::state_workload`, `incremental.py::supported_incremental`):
`predicates`/`incremental_predicates`, other or implicit strategies,
snapshots, Python models, custom materializations, and project overrides of
the delete+insert or incremental materialization macros. Their successful
writes still go through the completion and evidence lifecycle.

## Self-reading models

An incremental model that reads its own target relation is reused only when a
fixed-point proof holds (`incremental.py::reusable_self_read`). The supported
shape is a single base-table SELECT of unchanged, unqualified columns with
exactly `watermark > (SELECT MAX(watermark) FROM {{ this }})`, optionally with
MAX wrapped in COALESCE and a literal fallback. The watermark must be
projected unchanged under its own name. After a successful run, unchanged
inputs cannot pass this bound again. Joins, aliases, transformed projections,
extra predicates, view inputs and other direct or indirect self reads build
on every run. Both rendered branches must pass before initialization or
automatic copying is allowed. Models that read only external inputs keep the
normal reuse rules.

## Development initialization

With one file and `prod`/`dev` profile schemas, the client calls RegisterClone
before compiling an absent incremental target, so a CTAS can make
`is_incremental()` true in that compilation. This path has its own
source/target/epoch correlation and does not go through the client's
SQL-response handler.

Before CTAS the server requires the current confirmed source in the same
organization, file generation and DuckDB family, DELETE_INSERT execution type,
the `DUCKDB CTAS TABLE` source type, the exact current source epoch and a
matching initialization identity (`warehouse/duckdb.rs::incremental_copy_contract`).
Pending registrations, unknown source metadata, External modifications of the
source and later incompatible overwrites cannot supply this provenance. The
source must be an ordinary build, not itself a copy.

The identity (`duckdb-incremental-init-v2:` prefix,
`incremental.py::initialization_identity`) binds the SQL contract version, node
UID, template text, effective configuration other than physical location and
labels, and supplied macro contents. It travels in the local `node_hash`, in
RegisterClone labels and in SQL semantic extras. Binding it to automatic SQL
copies stops a rejected initialization template from re-entering through an
identical rendered cold branch. If the identity is unavailable, automatic
incremental copying is disabled; strict same-target reuse remains. A changed
template or configuration is rejected before CTAS, because copying first
would let the delta query keep obsolete source rows. No wire field or service
name changes, and Snowflake hashes and RegisterClone policy are unaffected.

Templates may use only `config`, `is_incremental()` and `this`. Both branches
must pass the [SQL eligibility subset](duckdb-evidence.md#sql-eligibility-subset)
and the worker execution-context check, with fully qualified local base-table
inputs and known evidence. Inputs in the `prod` or `dev` schemas, views,
`ref`/`source`, other Jinja, dynamic targets, variables or macros take
ordinary absent-target execution. Unchanged template text does not prove
unchanged input resolution or deterministic results.

The CTAS checks the exact plan, source and target schemas, source properties
and source evidence in the transaction that reads the source, records
completion evidence after commit, and confirms the registration separately.
Later SQL must still match its rendered, configuration and dependency
identity. A template with an `is_incremental()` branch may BUILD once after
creation or copy to record its new rendered identity; the next stable run
must SKIP. An unconditional template adopts the copied identity immediately.

A failed or uncertain compile-time copy leaves its registration pending. The
next SQL submission records the actual ordinary outcome under its own request
ID, and the uncertainty starts a fresh evidence generation at restart.
Copy-disabled settings, absent sources and unsupported properties keep
ordinary dbt behavior. Source validity is separate from upstream freshness:
the shared policy can permit a physically valid older source result.

## Validation

```sh
DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python" \
  cargo test -p dbt-state-harness --test duckdb_incremental_python -- --ignored --nocapture
cargo test -p dbt-state-harness --test duckdb_incremental
```

`duckdb_incremental_python` covers cold BUILD, unchanged SKIP, old-key updates,
retained deleted-source rows, profile-based RegisterClone/CTAS/confirmation,
restart reuse, logic mismatch before initialization, disabled copying,
edited and deleted sources, CTAS-plan failure with fallback, conditional
branch transitions, unsupported workloads, and self-read models
(`real_dbt_incremental_self_reads_require_reuse_proof`). Expected rows and
column types are literal, independent of the production fingerprint and
decision code.
