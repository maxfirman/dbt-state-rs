# DuckDB SQL identity

Submission and recording share the existing versioned fingerprint framing,
semantic configuration, resolution context and dependency name-to-definition
bindings. `sql_identity` supplies the common parser/visitor/serialization path;
warehouse policy selects Snowflake's existing canonicalizer or DuckDB's strict
allowlist. The pure decision engine does not parse SQL or perform I/O.

DuckDB's initial formatting equivalence subset is deliberately small:

- One standard SELECT, with at most one named input relation and an optional
  simple table alias.
- Projection identifiers, qualified identifiers, or explicitly aliased
  expressions made from identifiers, numeric literals, parentheses and `+`.
- No other clauses, wildcards, functions, joins, CTEs, set operations, casts,
  collections, implicit expression column names or dialect extensions.

Only comments, whitespace and parser formatting within that subset normalize.
Identifiers retain spelling, quoting and component boundaries. In particular,
DuckDB case-insensitive resolution does not justify folding column names: their
spelling remains observable in a result's schema. Snowflake type and function
synonym transformations are not applied. Unsupported or unparseable SQL retains
exact bytes, allowing ordinary same-target exact reuse under the existing
freshness and execution policy. `validated_duckdb_sql` returns `None` for that
fallback, so callers can distinguish validation from preservation.

The strict fingerprint still includes root catalog/schema, dependency names,
each dependency's catalog/schema and definition, and semantic extras. Sorting
bindings makes dependency list order immaterial without conflating swapped
definitions. Exact dependency-name strings preserve quoted dots without
rejoining or erasing components. No schema substitution, namespace adoption or
automatic cross-target copying is enabled for DuckDB in this milestone. Those
require the separate checked graph resolution, source validation and execution
work in issue #13; a validated strict SQL string alone does not authorize copying.

DuckDB uses the new `dbt-state-duckdb-sql-v1` strict/template version tag. Older
DuckDB histories cold miss. Snowflake retains `dbt-state-sql-v4`,
`dbt-state-ns-logic-v4` and the same canonical SQL bytes; other dialects keep
existing exact-byte policy and tags. Existing compatibility entry points remain.

Validation crosses the public production gRPC router for Submit/Confirm,
Record/Submit parity, logic/context/configuration changes, dependency binding
swaps, dotted quoted names, unknown syntax, and disabled cross-target reuse.
The real dbt test independently executes a reference query and checks column
names/types and duplicate/NULL-bearing rows. It requires an actual BUILD, then
SKIP after a comment/format change, then BUILD after changing a numeric literal.
Run after installing the locked integration environment:

```sh
uv sync --project integrations/duckdb --locked
DBT_STATE_DUCKDB_PYTHON="$PWD/integrations/duckdb/.venv/bin/python" \
  cargo test -p dbt-state-harness --test duckdb_identity_python -- --ignored
cargo test -p dbt-state-harness --test duckdb_identity
```

These tests define a local DuckDB contract. Snowflake conformance replay and
frozen upgrade cases continue to establish their existing compatibility scope.
