# DuckDB evidence and eligibility

How the Python integration (`integrations/duckdb/src/dbt_state_duckdb`)
observes a DuckDB file and decides which workloads can supply reusable
evidence. The decision itself stays in the Rust server. User guide:
[duckdb.md](duckdb.md). The evidence contract is decided in
[ADR 0003](adr/0003-duckdb-observed-change-evidence.md).

## Reserved schema `_dbt_state`

`recovery.py` creates these tables. Do not edit them or put project relations
in this schema.

| Table | Content |
| --- | --- |
| `database_identity` | Format version 1 and the generation UUID |
| `evidence_clock` | Format version 1 and one durable int64 ordinal for the file and generation |
| `relation_evidence` | Per relation: format version 1, schema and content SHA-256, ordinal |
| `invocation` | The dirty marker |

## Scope and namespace

The warehouse namespace is the SHA-256 of `duckdb-file-v1`, the canonical file
path, device, inode and generation (`recovery.py::Invocation`). A copied or
recreated file therefore cannot adopt another file's history, and a normal
restart keeps the same scope. Missing, incompatible or malformed metadata, an
ordinal outside `0 < ordinal <= clock`, or a dirty marker from an unfinished
invocation start a fresh generation before any State request. Old Postgres
history stays stored but cannot authorize reuse in the new scope. If the
metadata cannot be opened at all, the namespace is a random
`duckdb-unknown-<uuid>` and nothing can match.

## Ordering and Build age

Protocol `last_modified_epoch` values are DuckDB ordinals, not Unix
milliseconds (`evidence.py`). Freshness and target checks compare their order
only. One process-wide lock serializes transaction snapshots, scans and
evidence writes.

- An unchanged supported observation keeps its ordinal.
- A first observation, a changed schema or content, and every known committed
  build or copy take the next global ordinal, in the same transaction as the
  digest. A known write with identical rows still advances.
- A manual rewrite with identical contents and schema does not advance. Edits
  reversed between observations are invisible. This is observed-change
  evidence, not a write log.

Lag tolerance needs elapsed time, so the server measures Build age from when
it received the build's confirmation (`Warehouse::build_time_ms` in
`crates/server/src/warehouse.rs` returns `confirmed_at_ms` for DuckDB). The
integration bypasses the client's epoch caches and asynchronous epoch reads,
and a user override of a table's epoch (`table_overrides`) gives unknown
evidence rather than a wall-clock time.

## Table evidence format

Values use eight-byte unsigned big-endian length framing (`evidence.py::frame`).

- **Schema hash:** ASCII `duckdb-schema-v1`, then for each column of
  `DESCRIBE SELECT *` in order, the framed decimal column index followed by
  each framed DESCRIBE field (name, type, null, key, default, extra).
- **Content hash:** ASCII `duckdb-content-v1`, then all rows sorted as bytes.
  Each row is framed; each field is `N` for NULL or `V` + framed type + framed
  payload. Payloads are decimal integers, `True`/`False`, exact VARCHAR bytes,
  raw BLOB bytes, or decimal text at the column's fixed scale. Sorting keeps
  duplicate rows and ignores physical order.

Supported column types: BOOLEAN, signed and unsigned integers up to HUGEINT,
VARCHAR, BLOB and `DECIMAL(p,s)`. Any other type (FLOAT/DOUBLE, dates and
times, nested or extension types), a table whose stored DDL contains
`COLLATE`, and attached or external relations give unknown evidence, which
prevents reuse. A format change needs a new version tag.

## View evidence

A view observation hashes its committed catalog definition
(`duckdb-view-definition-v1`) and its bound DESCRIBE schema
(`duckdb-view-schema-v1`) in `views.py::view_fingerprint`. The view's ordinal
records definition or schema changes and known view DDL writes; it does not
timestamp the rows the view returns. Freshness comes from the leaf tables,
resolved recursively in one fresh transaction per request. Each physical
dependency view's name, definition and resolution context goes into the
request's `query_dependencies`, unlike the captured Snowflake view policy.
Compiled SQL never fills the view-definition cache. A wildcard view whose
stored schema no longer binds gets unknown evidence for its target relation
and rebuilds.

## SQL eligibility subset

`views.py::references` applies one gate to submitted models and tests, to
stored view definitions, and to both branches of incremental initialization
templates. Anything outside it adds an unknown marker input
(`_dbt_state.unresolved_view`), so the workload always builds.

Accepted: one SELECT or UNION over schema-qualified local relations, with
the functions CAST, TRY_CAST, SUM, COUNT, MIN, MAX, CONCAT, CHR, UNHEX,
COALESCE, NULLIF, LOWER and UPPER. Rejected, among everything else:

- LIMIT, OFFSET, SAMPLE, window functions, `COLLATE`, DISTINCT ON and special
  join methods (results can depend on row order);
- `/` and `//`, FLOAT/DOUBLE anywhere in a type tree, and scientific-notation
  literals (approximate intermediates);
- TIMESTAMPTZ and TIMETZ anywhere in a type tree (depend on worker timezone);
- named user-defined types (their definitions can change);
- column reads named `rowid`, qualified or quoted (physical row position);
- unqualified references, table functions, cycles and missing relations.

Function bindings: every callable name must be a built-in in both the dbt
worker session and the database file. If any built-in function or operator
name is overridden anywhere, including by a temporary macro, every query gets
unknown evidence.

Execution context (`views.py::supported_execution_context`): the actual
worker session must have `scalar_subquery_error_on_multiple_rows` true and an
empty `default_collation`, and the profile must not set `default_collation`.
Cursor plugins are included because the check reads the worker cursor.

SQL requests carry `duckdb_sql_contract = types-scalars-v5`, and incremental
initialization identities bind it too, so outcomes recorded before this
subset cold-miss.

## Hooks

Every SQL and seed request carries `duckdb_local_contract = functions-hooks-v1`
and its full pre/post-hook configuration, including empty lists
(`hooks.py::semantic_contract`). Changing or removing a hook rebuilds. Only
static post-hooks of the form `UPDATE {{ this }} SET col = <integer
column/literal arithmetic>` are eligible. Any pre-hook, and any other post-hook
form, adds the `_dbt_state.unsupported_execution_context` marker, so the model
always builds. Seed hooks have unknown evidence.

## Completion, recovery and the dirty marker

Evidence for a written relation is recorded after the materialization
returns, which follows dbt's commit and post-hooks, and before confirmation
(`completion.py`, `client.py`). A successful ordinary build advances the
ordinal once, also when the State submit failed and dbt built without a State
request. An error or partial outcome keeps the relation's evidence unknown for
the invocation, so neither confirmation nor write-only recording can certify
it.

### Recovery and the dirty marker

Each invocation commits a dirty marker before project execution and on-run
hooks. It is cleared only after successful CLI completion, adapter cleanup and
metadata workers finish. A failed or uncertain ConfirmExecution,
RecordExecutions or UpdateExecutionMetadata call, or uncertain write evidence,
leaves it dirty even when dbt succeeds. The next invocation then starts a
fresh generation and builds cold. This covers a crash between warehouse commit
and confirmation. It gives up reuse after uncertainty; there is no transaction
spanning DuckDB and Postgres.

`dbt-state-duckdb reset --database <file>` replaces only the reserved tables
and starts a new generation. It is required after restoring a backup, because
a restored marker, generation and clock can look valid.

## Seeds

Seeds use the client's SubmitValues request and ordinary `dbt seed`. A
successful load advances evidence before confirmation and before dependent
models submit. Unchanged CSV and configuration plus current evidence for the
target relation allow SKIP; a file hash alone does not. Seeds get no Lag
tolerance (`submit_decision.rs::submit_values`). Unsupported column types such
as DOUBLE build normally and never receive evidence.

## Cached data tests and stored failures

With `run_cache_enable_data_tests: true`, a supported test reuses its complete
`failures`, `should_error` and `should_warn` result. A cached error-severity
failure keeps dbt's failing status and exit code; a warning keeps its warning
status. A completed test failure counts as a clean invocation for recovery
(`data_tests.py::completed_invocation`); SQL errors and unconfirmed results
do not. Tests have no target relation; freshness comes from their recorded
upstream evidence.

Test requests carry a project ID derived from `duckdb-local-project-v1`, the
canonical project root and the project name (`client.py`). Same-named projects
at different roots have separate cached outcomes; moving a project starts new
test history.

`store_failures`, `store_failures_as` and `--store-failures` run through the
plain adapter, so dbt executes all DDL and test SQL ordinarily.

## Parallel workers

Normal dbt workers inside one invocation are supported. Scans and completion
writes share the process lock and the global ordinal, and each scan reads a
fresh committed snapshot. Completion evidence is written before control
returns to dbt's scheduler, so children observe their committed parents.
The file lock is held until workers and metadata jobs finish.

## Tests

Run with `DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python"`
and `cargo test -p dbt-state-harness --test <target> -- --ignored`.

| Target | Covers |
| --- | --- |
| `duckdb_python` | Table evidence, external edits, unknown types, Lag tolerance, views and collated tables |
| `duckdb_functions_python` | Function bindings, hooks, `rowid`, named types, scalar subqueries, worker collation, timezone casts, nested float casts, division |
| `duckdb_recovery` | Reset, copy/recreation, restore, damaged and old-format metadata (`integrations/duckdb/tests/fixtures/evidence-v1.sql`), lock exclusion |
| `duckdb_interruptions` | Process kills at each durability boundary; `INTERRUPT_ONLY_PHASE` selects one phase from `integrations/duckdb/tests/interruptions.py` |
| `duckdb_parallel` | Four-worker fan-out/fan-in ordering and worker SQL errors |
| `duckdb_seeds_python` | Seed reuse, config changes, failed loads, copied files, organization isolation |
| `duckdb_data_tests_python` | Cached pass/fail/warn, project isolation, stored-failure fallback |

Offline protocol regressions: `duckdb_outcomes`, `duckdb_data_tests` and
`data_test_freshness` (no `--ignored`). `VIEW_SCENARIO=collation` limits the
`duckdb_python` view test (`real_dbt_views_dependency_evidence`) to its
collated-table case.
