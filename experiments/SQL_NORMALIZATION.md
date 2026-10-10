# dbt State server SQL comparison — mechanism (live-verified)

Hypothesis tested (user): the server parses SQL (ANTLR per-dialect, like dbt
Fusion) into a DataFusion **logical plan** and compares plan hashes, so
semantically-equivalent SQL would skip.

**Result: DISPROVEN.** The server does lexical **token-stream normalization**,
not logical-plan comparison. ~16 controlled live A/B experiments against
api.state.dbt.com (clean confirmed baseline re-established before each variant;
model `sqlprobe` on Snowflake):

## Discriminating evidence

| # | variant vs baseline | semantically equal? | logical-plan predicts | HOSTED actual |
|---|---|---|---|---|
| 1 | line comment `-- …` added | yes | skip | **skip** |
| C | block comment `/* … */` added | yes | skip | **skip** |
| 5 | UPPERCASE keywords | yes | skip | **skip** |
| D | unquoted identifier case (`CUSTOMER_ID`) | yes | skip | **skip** |
| 3 | `group by 1` vs `group by customer_id` | yes | skip | **execute** ✗plan |
| 4 | redundant parens `(x is not null)` | yes | skip | **execute** ✗plan |
| 6 | CTE wrap vs inline | yes | skip | **execute** ✗plan |
| 7 | `not (x is null)` vs `x is not null` | yes | skip | **execute** ✗plan |
| F | `1 = 1` vs `1 = 1.0` | yes | skip | **execute** ✗plan |
| G | SELECT-list column reorder | no (col order) | execute | **execute** |
| 2 | output alias rename (`id`→`cust_id`) | no (col name) | execute | **execute** |
| E | quoted `"customer_id"` vs unquoted | no (quoting) | execute | **execute** |
| 8 | add a column | no | execute | **execute** |
| 9 | different WHERE filter | no | execute | **execute** |
| A | whitespace INSIDE a string literal | no (data) | execute | **execute** |
| B | case INSIDE a string literal | no (data) | execute | **execute** |

The six "✗plan" rows are the proof: a logical-plan comparison would normalize
away parens, group-by ordinals, CTE wrapping, predicate rewrites, and numeric
literal forms — all of them SKIP under H1 but the hosted service EXECUTES. So it
is NOT comparing plans.

## The verified model: lexical token-stream normalization (lexer-aware)

The server tokenizes the SQL and compares a normalization of the token stream.
Specifically it:
1. **strips comments** — both `--` line and `/* */` block (rows 1, C skip);
2. **collapses whitespace BETWEEN tokens** (whitespace-only edits skip) but
   **preserves whitespace INSIDE string literals** (row A executes) — so it is a
   real lexer, not a naive string replace;
3. **case-folds keywords and UNQUOTED identifiers** (rows 5, D skip) but
   **preserves the case of string literals** (row B executes) and treats
   **quoted identifiers as distinct** (row E executes);
4. does **NOT** canonicalize semantics: parens, `group by <ordinal>` vs
   `<name>`, CTE-vs-inline, boolean-predicate rewrites, and numeric literal
   forms all change the token stream → EXECUTE (rows 3,4,6,7,F); token ORDER is
   significant (rows G, 2).

Equivalently: normalize = lex → drop comments → fold case of
keyword/bareword tokens → canonical single-space between tokens, keeping string
and quoted-identifier tokens verbatim → hash the resulting token sequence.

## Implication for dbt-state-rs

Our previous `split_whitespace()` normalization was too crude (it would, e.g.,
collapse whitespace inside string literals → wrongly skip row A, and it did not
strip comments or fold case → wrongly execute rows 1/C/5/D). Implement a proper
SQL lexer-level normalizer matching the four rules above. This gets us
materially closer to the hosted skip/execute boundary while staying far short of
a SQL engine / logical planner (which the evidence shows the server does NOT
use either).
