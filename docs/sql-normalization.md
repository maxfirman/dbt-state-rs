# Snowflake SQL canonicalization

This page covers how SQL text enters the logic identity for **Snowflake**.
DuckDB uses a separate validated formatting subset
([duckdb-identity-and-copy.md](duckdb-identity-and-copy.md)). Databricks resolves
only its [evidenced identifier subset](databricks-identifiers.md), preserving
all other SQL bytes. Other dialects keep exact SQL bytes
(`crates/server/src/sql_identity.rs` `normalized_sql`).

## Hosted behavior

The hosted service parses SQL into a dialect-aware AST and compares a
canonical re-rendering. It canonicalizes names, operators, types and functions,
but does no semantic simplification (no constant folding, precedence or
parenthesis elimination, GROUP BY ordinal resolution or boolean algebra). This
was characterized with controlled A/B submits of one model on Snowflake, with a
clean confirmed baseline before each variant.

### Canonicalized (SKIP)

| Change |
|---|
| Line `--` and block `/* */` comments, optimizer hints `/*+ … */` |
| Whitespace between tokens |
| Keyword and unquoted-identifier case |
| Trailing comma before `FROM`; trailing semicolon |
| Optional `AS` in aliases (`x k` ≡ `x as k`) |
| Operator synonyms (`!=` ≡ `<>`) |
| Cast syntax (`x::t` ≡ `cast(x as t)`) |
| Type-name synonyms (`varchar` ≡ `text`; the number family) |
| Function-name synonyms (`coalesce` ≡ `nvl`/`ifnull`) |
| Physical schema of three-part names, for the cross-target identity only |

### Preserved (build)

| Change | Note |
|---|---|
| Whitespace or case inside a string literal | String content is significant |
| A quoted identifier whose resolved name differs (`"x"` vs `x`) | Identity-significant |
| Redundant or precedence-redundant parentheses | No precedence elimination |
| GROUP BY ordinal `1` vs column name | No ordinal resolution |
| CTE wrap vs inline | Structure preserved |
| `not (x is null)` vs `x is not null` | No boolean algebra |
| `1` vs `1.0`, `10` vs `1e1` | No literal folding |
| SELECT-list reorder; output alias rename | Order and output schema are significant |

## Server implementation (`crates/server/src/sql_norm.rs`)

`normalize_sql` parses with `sqlparser`'s Snowflake dialect, applies the
`Canonicalizer` AST pass and re-renders with `Display`
(`sql_identity::canonicalized`). Parsing and re-rendering remove comments,
whitespace, keyword case, optional `AS`, `!=`/`<>` differences, trailing commas
and semicolons, while keeping the structure the hosted service keeps. The AST
pass adds:

- **Identifiers** follow Snowflake resolution (`canonical_identifier`): an
  unquoted name folds to uppercase, a quoted name is case-sensitive. A resolved
  plain uppercase name is emitted unquoted, so `"X"` ≡ `X`; other names stay
  quoted. A quoted reserved word such as `"NULL"` stays quoted
  (`is_bare_uppercase_identifier`).
- **Casts**: `x::T` becomes `CAST(x AS T)`. Other cast kinds (such as
  `TRY_CAST`) stay distinct.
- **Types** (`canon_type`): the string family without a length becomes
  `VARCHAR`; the number family (`NUMBER`, `NUMERIC`, `DECIMAL`, `INT`, …)
  without precision becomes `NUMERIC`. A length, precision or scale is kept.
- **Functions** (`canon_function`): unqualified, unquoted `nvl` and `ifnull`
  become `coalesce`.

If parsing fails, the SQL is kept byte for byte, so an unsupported construct
can only cause an extra build. `normalize_sql_lexer` is a legacy helper and
does not feed persisted fingerprints.

The type and function catalogs cover the Snowflake aliases verified live and
are not exhaustive. An unlisted synonym over-executes, which is the safe
direction. The hosted backend may use a different parser, so agreement on
unusual SQL is not guaranteed. The Apache-2.0 `sqlparser` crate was chosen
because the dbt grammar crates are under ELv2 or unpublished.

### Cross-target identity

`normalize_sql_namespace_abstracted_with` replaces the schema of each
three-part relation name with a relative token (`__dbt_state_ns__0`, `…1`, …)
from a request-wide `SchemaMap`: each distinct schema gets its own token in
sorted order. The same model in two targets therefore shares an identity,
while two different schemas in one request stay distinct. Two-part and
one-part names are not abstracted.

`fingerprint::namespace_logic_hash` uses this with the Snowflake
`NAMESPACE_IDENTITY` version tag (`crates/server/src/warehouse/snowflake.rs`).
It binds each dependency's abstracted name to its abstracted definition, so
swapping two definitions between references changes the identity
(`fingerprint.rs::ns_distinguishes_swapped_dependency_bindings`). This identity
keys Snowflake cross-target result copy. Same-target reuse uses
`fingerprint::sql_logic_hash`, which hashes the non-abstracted normalized SQL.

Changing canonicalization requires bumping the version tag in
`crates/server/src/warehouse.rs` (`SQL_IDENTITY_VERSION`) or
`warehouse/snowflake.rs`, so persisted fingerprints from the old rules stop
matching.

## Tests

- `crates/server/src/sql_norm.rs` unit tests: positive equivalences, negative
  cases (cast kind, length and scale, qualified or quoted function names,
  quoted identifiers) and the `arbitrary_utf8_never_panics` property.
- `crates/harness/tests/sql_normalization.rs`: replays the captured
  skip/build pairs through the gRPC server.
- `crates/harness/tests/c2_rendered_sql.rs::quoted_reserved_word_change_forces_execute_same_target`.
