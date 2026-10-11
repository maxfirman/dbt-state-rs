# dbt State SQL comparison — mechanism (live-verified)

The server's logic-identity match **parses SQL into a dialect-aware AST and
compares a canonical re-rendering** of it. It is neither a raw token stream nor a
logical plan: it does dialect-aware name/operator/type/function canonicalization,
but **no semantic simplification** (no constant folding, precedence/paren
elimination, group-by-ordinal resolution, or boolean algebra). Characterized via
controlled A/B experiments against `api.state.dbt.com` (model `sqlprobe` on
Snowflake, clean confirmed baseline re-established before each variant).

## Canonicalized → SKIP

| change |
|---|
| line `--` and block `/* */` comments stripped |
| optimizer hint `/*+ … */` stripped |
| inter-token whitespace collapsed |
| keyword + unquoted-identifier case folded |
| trailing comma before `FROM`; trailing semicolon |
| optional `AS` in aliases (`x k` ≡ `x as k`) |
| operator synonyms (`!=` ≡ `<>`) |
| cast syntax (`x::t` ≡ `cast(x as t)`) |
| type-name synonyms (`varchar` ≡ `text`; number family) |
| function-name synonyms (`coalesce` ≡ `nvl`/`ifnull`) |
| physical schema qualifier of 3-part names (cross-environment identity only — see `namespace_logic_hash`) |

## Preserved → EXECUTE

| change | note |
|---|---|
| whitespace / case INSIDE a string literal | string content significant |
| quoted identifier vs unquoted; quoted case | identity-significant |
| redundant / precedence-redundant parens | no precedence elimination |
| group-by ordinal `1` vs column name | no ordinal resolution |
| CTE-wrap vs inline | structure preserved |
| boolean rewrite `not(x is null)` vs `x is not null` | no boolean algebra |
| numeric form `1` vs `1.0`, `10` vs `1e1` | no literal folding |
| SELECT-list column reorder; output alias rename | order + output schema significant |

## Model

```
normalize(sql) = render_canonical( parse_dialect_ast(sql) )
```

A **syntactic AST canonicalization with dialect name resolution** — strictly
between a token stream and a logical plan.

## Implementation (`crate::sql_norm`)

Parser-backed via apache/datafusion-sqlparser-rs (Snowflake dialect): parse → a
canonicalizing AST pass → `Display`. Parse+Display gives comments, whitespace,
case, optional `AS`, `!=`↔`<>` and trailing `;` for free, while preserving the
structure the hosted service preserves (parens, ordinals, CTEs, literal forms,
boolean structure, ordering, string content). The AST pass adds: lowercased
unquoted identifiers, `x::T` → `CAST(x AS T)`, and small documented catalogs of
type synonyms (string family → VARCHAR; number family incl. Snowflake `NUMBER`
→ NUMERIC) and function synonyms (`nvl`/`ifnull` → `coalesce`).

`normalize_sql_namespace_abstracted` additionally replaces the schema component
of three-part names, giving the cross-environment logic identity used for clone
reuse (same model in prod vs a dev clone differs only by that qualifier).

On parse failure (unsupported dialect features) it falls back to a conservative
lexer-level normalizer, so robustness is never worse than exact-string matching.

Bounded gap: the synonym catalogs cover the common Snowflake aliases verified
live but are not exhaustive — an unlisted synonym falls through and OVER-EXECUTES
(safe-directional, never a wrong skip). The hosted backend may use a different
parser, so exact agreement on exotic SQL is not guaranteed; an Apache-2.0 parser
was chosen deliberately (the dbt grammar crates are ELv2/unpublished, incompatible
with this project's licence). See the match-key notes in
[docs/protocol.md](../docs/protocol.md).
