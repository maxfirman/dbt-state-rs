# Share SQL identity machinery with explicit warehouse semantics

## Implementation status — 2026-11-07

Shared fingerprint framing and dependency binding are implemented with explicit
warehouse rules. [SQL identity](../duckdb-sql-identity.md) documents DuckDB's
validated subset and exact fallback; [result copying](../duckdb-result-copy.md)
documents the checked cross-environment subset and copy-to-SKIP lifecycle.
These retain the documented Snowflake compatibility scope.

The Python [Result copy module](../../integrations/duckdb/src/dbt_state_duckdb/result_copy.py)
now concentrates both DuckDB copy entry paths, provenance, transactional source
validation and path-specific recovery. Rust retains decision policy and State
history; Python executes the warehouse operation as decided in ADR 0002.

## Original decision

Use one fingerprinting and dependency-binding pipeline for Snowflake and
DuckDB, sharing traversal, provenance and execution lifecycle machinery.
Warehouse-specific rules supply parsing, identifier semantics and validated
SQL transformations. This creates a shared implementation exercised by cheap
DuckDB integration tests while preserving Snowflake behavior through its
existing conformance tests and explicit dialect rules.

Begin DuckDB cross-environment matching with a conservative, tested SQL subset
that recognizes supported prod/dev schema substitutions. Preserve the binding
between each dependency reference and its definition, structured identifier
components, and distinct physical inputs. Unknown syntax or uncertain
semantics must prevent automatic copying. A parser accepting syntax does not
establish equivalence. Do not enable Snowflake normalization wholesale for
DuckDB: for example, DuckDB's double-quoted identifiers are case-insensitive,
whereas Snowflake's are case-sensitive.

Copy eligibility, source validation, recording and subsequent reuse share the
same lifecycle. DuckDB CTAS and Snowflake CLONE remain warehouse-specific
execution capabilities. Adding CTAS DDL alone is insufficient: automatic
cross-environment selection and safe copy-to-SKIP adoption must also work.
Changes to persisted identity semantics require versioned fingerprints and
safe invalidation rather than reinterpretation of old history.

Evidence: current [SQL normalization](../../crates/server/src/sql_norm.rs),
[fingerprinting](../../crates/server/src/fingerprint.rs), and
[DuckDB identifier rules](https://duckdb.org/docs/2.0/sql/dialect/keywords_and_identifiers).
