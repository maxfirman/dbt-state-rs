# dbt State server SQL comparison — mechanism (live-verified)

Hypothesis (user): the server parses SQL (ANTLR per-dialect, like dbt Fusion)
and compares something plan-like, so semantically-equivalent SQL skips.

**Refined conclusion over two rounds of ~25 controlled live A/B experiments**
(clean confirmed baseline re-established before every variant; model `sqlprobe`
on Snowflake via the recording proxy to api.state.dbt.com):

The server **parses SQL into a dialect-aware AST and compares a canonical
re-rendering** of it. It is NOT a raw token stream (round 1's conclusion was too
weak) and NOT a logical plan (the original hypothesis is too strong). It does
dialect-aware *name/operator/type/function* canonicalization, but performs **no
semantic simplification** (no constant folding, no precedence/paren elimination,
no group-by-ordinal resolution, no boolean algebra).

## What is CANONICALIZED (→ SKIP)

| change | evidence |
|---|---|
| line `--` and block `/* */` comments stripped | skip |
| optimizer hint `/*+ … */` stripped | skip |
| inter-token whitespace collapsed | skip |
| keyword + unquoted-identifier case folded | skip |
| trailing comma before `FROM` | skip |
| trailing semicolon | skip |
| optional `AS` in aliases (`x k` ≡ `x as k`) | skip |
| operator synonyms (`!=` ≡ `<>`) | skip |
| cast syntax (`x::t` ≡ `cast(x as t)`) | skip |
| type-name synonyms (`varchar` ≡ `text`) | skip |
| function-name synonyms (`coalesce` ≡ `nvl`) | skip |

## What is PRESERVED (→ EXECUTE)

| change | evidence | note |
|---|---|---|
| whitespace INSIDE a string literal | execute | string content significant |
| case INSIDE a string literal | execute | string content significant |
| quoted identifier vs unquoted / quoted case | execute | identity-significant |
| redundant parens `(x)` | execute | parens preserved |
| precedence-redundant parens `(a and b) or c` | execute | NO precedence elimination |
| group-by ordinal `1` vs column name | execute | NO ordinal resolution |
| CTE-wrap vs inline | execute | structure preserved |
| boolean rewrite `not(x is null)` vs `x is not null` | execute | NO boolean algebra |
| numeric form `1` vs `1.0`, `10` vs `1e1` | execute | NO literal/const folding |
| SELECT-list column reorder | execute | order significant |
| output alias rename | execute | output schema significant |

## The model

```
normalize(sql) = render_canonical( parse_dialect_ast(sql) )
```

where `parse_dialect_ast` resolves comments/hints away and the canonical render:
- folds case of keywords and unquoted identifiers; keeps string literals and
  quoted identifiers verbatim;
- canonicalizes optional `AS`, operator synonyms, cast shorthand, and
  type/function **synonyms** to a single spelling (dialect catalog);
- but renders the AST **structurally as written** — keeping parens, group-by
  ordinals, literal forms, boolean structure, CTEs, and ordering.

So it sits strictly between "token stream" and "logical plan": a **syntactic AST
canonicalization with dialect name resolution**.

## Implementation status in dbt-state-rs

`crate::sql_norm::normalize_sql` is **parser-backed** (apache/datafusion-
sqlparser-rs 0.63, Snowflake dialect): parse → a canonicalizing AST pass →
`Display`. From parse+Display we get, for free, canonicalization of comments,
whitespace, case, optional `AS`, `!=`↔`<>`, and trailing `;`, while the semantic
structure the hosted service preserves (parens, group-by ordinals, CTEs, literal
forms, boolean structure, ordering, string content) is preserved. The AST pass
adds: lowercase unquoted identifiers, `x::T` → `CAST(x AS T)`, and a small
documented catalog of type-name synonyms (string family → VARCHAR, number
family incl. Snowflake `NUMBER` → NUMERIC) and function-name synonyms
(`nvl`/`ifnull` → `coalesce`). All of comments/case/whitespace/AS/operators/
cast/type/function were re-verified live after implementation.

When `sqlparser` cannot parse the input (dialect features it doesn't support),
`normalize_sql` falls back to a conservative lexer-level normalizer
(`normalize_sql_lexer`) so robustness is never worse than before.

Remaining bounded gap: the synonym *catalogs* (type/function) cover the common
Snowflake aliases we verified live but are not exhaustive — an unlisted synonym
falls through and OVER-EXECUTES (safe-directional, never a wrong skip). The
hosted backend may also use a different parser (e.g. sqlglot if Python), so exact
agreement on exotic SQL is not guaranteed; that trade-off was accepted when
choosing an Apache-2.0 parser (the dbt grammar crates are ELv2 and unpublished —
incompatible with this project's Apache-2.0 licence and "no managed service"
clause). See the match-key notes in docs/protocol.md.
