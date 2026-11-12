# DuckDB SQL identity and result copies

Server-side identity and result copy rules for DuckDB, and the client steps
that execute a copy. The shared direction is decided in
[ADR 0004](adr/0004-shared-sql-identity-with-dialect-rules.md); the copy scope
in [ADR 0002](adr/0002-duckdb-local-workflow.md). This is a local contract, not
hosted DuckDB conformance.

## Identity tags

Each fingerprint starts with a version tag. Changing identity semantics takes
a new tag, so older history cold-misses instead of being reinterpreted.

| Identity | DuckDB | Snowflake | Defined in |
| --- | --- | --- | --- |
| Strict SQL (rendered and template) | `dbt-state-duckdb-sql-v1` | `dbt-state-sql-v4` | `warehouse/duckdb.rs::SQL_IDENTITY_VERSION`, `warehouse.rs::SQL_IDENTITY_VERSION` |
| Namespace logic | `dbt-state-exact-ns-v1` (exact, no schema abstraction) | `dbt-state-ns-logic-v4` (schema-abstracted) | `warehouse.rs`, `warehouse/snowflake.rs` |
| Copy logic | `dbt-state-duckdb-copy-v2` (checked copy graph) | the namespace logic identity | `warehouse/duckdb.rs::COPY_IDENTITY_VERSION` |

Other dialects use `dbt-state-sql-v4`, the exact namespace identity and no
copy identity. Client-declared values in `semantic_extras` or RegisterClone
`labels` also enter the identity:

| Key | Value | Set by |
| --- | --- | --- |
| `duckdb_copy_contract` | `rows-schema-v1` | `qualification.py`, for CTAS-compatible nodes |
| `duckdb_incremental_contract` | `delete-insert-fixed-v1` | `qualification.py`, pinned incremental models |
| `duckdb_incremental_identity` | `duckdb-incremental-init-v2:…` | `incremental.py::initialization_identity` |
| `duckdb_sql_contract` | `types-scalars-v5` | `views.py::SQL_CONTRACT` |
| `duckdb_local_contract` | `functions-hooks-v1` | `hooks.py::LOCAL_CONTRACT` |

## Formatting subset

`sql_identity::normalized_sql` uses Snowflake's canonicalizer for Snowflake
and `sql_identity::validated_duckdb_sql` for DuckDB. The DuckDB subset is
small:

- one standard SELECT with at most one named input relation and an optional
  simple alias;
- projections that are identifiers, qualified identifiers, or explicitly
  aliased expressions built from identifiers, numeric literals, parentheses
  and `+`;
- no other clause, wildcard, function, join, CTE, set operation, cast,
  collection, implicit expression column name or dialect extension.

Only comments, whitespace and parser formatting normalize. Identifiers keep
their spelling, quoting and component boundaries: DuckDB resolves column names
case-insensitively, but their spelling is visible in the result schema.
Snowflake type and function synonyms are not applied. SQL outside the subset
keeps its exact bytes (`validated_duckdb_sql` returns `None`), so same-target
exact reuse still works under the normal freshness policy.

The strict fingerprint (`fingerprint.rs`) also covers the root catalog and
schema, each dependency's name, catalog, schema and definition, and the
semantic extras. Dependency bindings are sorted, so list order does not
matter, but swapping two definitions between names changes the identity.

## Copy eligibility

A validated formatting identity alone never grants copying.
`Warehouse::copy_identity` (`warehouse/duckdb.rs::copy_identity`) returns a
copy identity only when all of these hold:

- execution type FULL, or DELETE_INSERT with `delete-insert-fixed-v1`
  (`copy_admits`, which is also DuckDB's answer to
  `Warehouse::submit_copy_admits`: every other warehouse copies only FULL on
  submit, following hosted 2.0.11, #101, and Snowflake also a seed, #91;
  hosted does not engage DuckDB, so this subset keeps its delete+insert copy
  and reloads seeds);
- `default_schema` is `prod` or `dev`, and the target relation is in the
  request's default catalog and schema;
- `duckdb_copy_contract = rows-schema-v1`;
- `sql_identity::duckdb_copy_graph` builds a checked graph;
- every reported input other than the target relation has a known epoch, is a
  unique checked relation, and every physical graph input is among them.

`fingerprint::copy_logic_hash` frames that graph. In the graph:

- each level uses the formatting subset with one input, including checked
  `CREATE VIEW` wrappers;
- only views in the request's own Target schema are abstracted to a
  placeholder, so the same model in `prod` and `dev` matches. Physical base
  tables and views in other schemas keep exact names: `raw.input` and
  `other.input` differ even with identical rows;
- each view reference is bound to its definition and resolution context;
  quoted dots stay inside their component;
- one- and two-part names resolve only against explicit catalog/schema
  context. Duplicate, case-ambiguous, cyclic, unreachable, `_dbt_state` or
  other-catalog references reject the copy.

The client sets `rows-schema-v1` only for ordinary table models (and pinned
incrementals) with no contract, constraints, indexes, hooks, `sql_header`,
grants, persisted docs or unknown adapter configuration
(`qualification.py::ctas_satisfies_requirements`). It reports a source as
`DUCKDB CTAS TABLE` only after inspecting it as a local base table with no
constraints, indexes, defaults or NOT NULL columns (`copy.py::inspected_table`).

## Copy plan and chain depth

`warehouse/duckdb.rs::copy_ddl` emits
`CREATE OR REPLACE TABLE <target> AS SELECT * FROM <source>`. It requires the
`DUCKDB CTAS TABLE` source type, one catalog, different schemas, and both
schemas in `prod`/`dev`. The submit fallback also requires a recorded source
epoch (`copy_source_evidence`) and skips the volatile-SQL gate for requests
with a copy identity (`copy_identity_skips_volatility_gate`).

Copy chains stop at two ancestors (`MAX_COPY_DEPTH`); a client may request a
lower `clone_chain_depth_limit`. `Store::copy_chain_depth` follows confirmed
provenance as of each copy's submission, scoped by organization, namespace and
warehouse family. Missing ancestry is ineligible, and a later rebuild of an
ancestor cannot shorten an existing chain. A source at the limit means
ordinary execution. The client reserves one hop when it defers to its
configured production target, so the automatic two-hop test
(`integrations/duckdb/tests/graph_copy.py`) uses explicit physical references and `--no-defer`.

Incremental initialization through RegisterClone has its own narrower
contract: see [incremental models](duckdb-incremental.md).

## Client execution

`result_copy.py::ResultCopy` owns both entry paths. Before CTAS the client
checks that the source's current evidence equals the epoch the server
required, the target matches the node, the plan is exactly the expected CTAS,
and the target schema equals the active profile schema. Source inspection,
fingerprint re-check and CTAS run in one DuckDB transaction
(`result_copy.py::execute_copy`). Completion evidence is recorded after commit
and before confirmation. A copy proposal alone never publishes success.

If the source changed, disappeared, has unknown evidence or unsupported
properties, the client falls back to the ordinary materialization and
confirms with `failed_to_clone=true` only after that build succeeds. SQL-submit
fallback keeps the requested SQL identity and clears the copy provenance.

Source validity is separate from upstream freshness. The shared SQL-submit
fallback can copy a physically valid recorded source whose inputs have since
advanced; tests accept that permitted older result.

## Snowflake confirmation compatibility assessment

ConfirmExecution clears copy provenance when `failed_to_clone` is set. For
RegisterClone rows without submitted SQL it also clears the unproven
source-derived namespace identity; SQL-submit rows keep their requested SQL
identity. Duplicate confirmations cannot change the first outcome. Without
this, a failed RegisterClone followed by an ordinary build kept the source's
logic identity and could be adopted as that source model.

This applies to Snowflake as well as DuckDB. The hosted failed-copy fallback
policy is uncharacterized, and offline tests are not hosted evidence. Keeping
an identity with no successful copy or submitted transformation would
authorize wrong-logic reuse; clearing it costs at most a rebuild or copy on
the next request. Captured successful copies, response mirrors, SQL generation
and fingerprints stay the compatibility contract, and no capture or replay
exemption changes. `failed_clone_confirmation_does_not_certify_source_logic`
(`crates/harness/tests/clone_lifecycle.rs`) shows the earlier wrong SKIP and
checks immutable duplicate confirmations.

## Validation

```sh
uv sync --project integrations/duckdb --locked
export DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python"
cargo test -p dbt-state-harness --test duckdb_identity_python -- --ignored
cargo test -p dbt-state-harness --test duckdb_copy_python -- --ignored --nocapture
cargo test -p dbt-state-harness --test duckdb_identity --test duckdb_copy --test clone_lifecycle
```

| Target | Covers |
| --- | --- |
| `duckdb_identity_python` | BUILD, SKIP after a comment/format change, BUILD after a literal change; independent rows and types |
| `duckdb_copy_python` | Table copies and adoption; quoted/dotted view chains, prod→dev→prod hops, third-hop fallback, source edits/deletion/index changes, copy-plan failure, permitted stale sources |
| `duckdb_identity` | Submit/Record parity, context and configuration changes, binding swaps, dotted names, unknown syntax, disabled cross-target reuse |
| `duckdb_copy`, `clone_lifecycle` | Context allowlists, unknown/missing/duplicate metadata, expiration properties, fallback provenance, cycles, permutations |

Snowflake capture replay and existing clone tests stay required.
