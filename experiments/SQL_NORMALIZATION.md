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

`crate::sql_norm::normalize_sql` currently implements the LEXER subset (round 1):
comments, whitespace, case-folding, string/quoted-ident preservation. It does
NOT yet canonicalize optional-AS, operator synonyms, cast shorthand, or
type/function synonyms — so for those specific equivalences we OVER-EXECUTE
(safe-directional: we rebuild where the hosted service skips, never the
reverse). Reproducing them faithfully requires an actual SQL parser with a
per-dialect catalog of operator/type/function synonyms (a `sqlparser`-class
dependency). Tracked as a known, bounded gap; the lexer subset already captures
the highest-frequency equivalences (whitespace, comments, case) seen in real
dbt output. See the match-key notes in docs/protocol.md.
