# Databricks SQL equivalence evidence

The pinned #137 campaigns characterize dbt 2.0.11 on serverless Unity Catalog with
managed Delta tables. Hosted decisions and independent physical results are
separate evidence: equal rows alone do not authorize a SQL identity equivalence.
The [dated acceptance report](reports/databricks-sql-equivalence-2026-11-14.md)
records the validated slice; it establishes no general Databricks support claim.

Two complete hosted captures contain 30 actual managed dbt invocations, 30 separate
State-disabled reference invocations and 205 certified RPCs. Each actual cell
checks the complete typed row multiset (duplicates and nulls included), ordered
column names/types, MANAGED/Delta relation properties and a complete first Explain.
A standalone exact unmodified-query control surrounds the public spacing/comment
regression. Full response/status/Explain replay and real Rust acceptance are
recorded in the dated acceptance report.

| SQL change | Observed hosted decision |
| --- | --- |
| Unchanged query, spacing/newlines, SELECT/FROM case | SKIP |
| Ordinary leading, inline and trailing comments | SKIP |
| Add a table alias to previously unaliased input | BUILD |
| Rename an existing table alias, with/without AS | SKIP |
| Change output column name | BUILD; schema change independently checked |
| Change numeric literal10→17 | BUILD |
| Numeric spelling10→010 with equal typed result | BUILD |
| Add real comment beside unchanged comment-like string contents | SKIP |
| Change string contents containing `-- /* ... */` | BUILD |
| CAST/NULL keyword case with explicit typed output alias | SKIP |
| CTE alpha rename, add unused definition, swap definitions | BUILD |
| Nested CTE shadow/unshadow with different bound result | BUILD |
| CTE unchanged, ordinary comment or spacing/root SELECT/FROM case | SKIP |

The checked forms are deliberately bounded:

- #136's direct identifier-only SELECT with one named input: existing identifier
  resolution supplies the physical input while retaining original source spans.
- A single named input with one table alias and projections qualified by that
  alias: its declaration and bound qualifiers are renamed together. Alias
  presence, physical source name and output columns remain distinct.
- Explicit named projections of identifiers or CAST literal expressions to INT
  or STRING: values, numeric token spelling and output alias names remain intact.
- A single simple CTE whose identifier projections read one fully qualified
  source, with the body reading that CTE: names and scoped binding tokens remain
  unchanged. Nested, recursive or multiple CTE definitions retain exact SQL.

All forms reuse the existing strict SELECT grammar guard. The latter three keep
original resolution context. Rendering uses raw token spelling, including quote
and backslash escapes; AST parsing validates grammar and does not serialize SQL
identity. This avoids widening equivalence by unescaping a string or erasing
punctuation. SELECT/FROM case folds in checked forms; CAST/AS/type/NULL case folds
only at checked literal syntax positions, with output identifiers protected.
Ordinary whitespace/comments are removed. Hint/directive comments, UDFs and
uncharacterized syntax retain exact fallback. Bounded public sequences require
reuse controls before checking changed values, column names, CTE bindings and
unsupported syntax; independent old binaries supply frozen v1/v2 histories.

The new versioned contract is `dbt-state-databricks-sql-v3`. Representative genuine
rendered-mode v1/v2 frozen histories invalidate, including pending rows confirmed
after upgrade. Frozen
bytes come from independently retained old binaries, not a current hash function
with a substituted tag. The restricted physical history key, original target
spelling, organization/family/namespace scope and #159 rendered-dialect admission
remain separate from SQL formatting. Snowflake and DuckDB identity tags and
policies are unchanged. No Result copy or freshness policy follows from these
SQL observations.


Canonicalization is confined to the rendered fingerprint path. The existing
raw-dialect template path, template body, dependency SQL and resolution context
remain unchanged; no template/configuration equivalence is inferred. Existing
`compare_unrendered_code` template alternatives can still qualify under the
preserved policy; these rendered-only upgrade regressions do not claim blanket
invalidation of all history. Escaped quote and
backslash spellings have local preservation regressions; no additional hosted
canonicalization policy is claimed for alternative escape spellings.
