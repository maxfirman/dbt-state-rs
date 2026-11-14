# Databricks SQL-equivalence evidence — 2026-11-14

#137 now reproduces the captured rendered-SQL equivalences for the pinned dbt
2.0.11 client on serverless Unity Catalog with managed Delta tables. Whitespace,
ordinary comments, one table-alias rename and the captured simple CTE formatting
reuse; output names, literal values/spelling and CTE names/bindings retain their
observed distinctions. This is a bounded evidence claim, not general Databricks
support. The copied-target rebuild, snapshot deletion, #140 Safety, #158 logical
targetless and #159 namespace-fallback blockers remain explicit and unchanged.

## Captures and independent physical evidence

The two hosted phases ran on 2026-11-13: 26 main cells and four independent fresh
CTE controls, 60 dbt invocations including 30 State-disabled references, and 205
certified RPCs. They contain 18 confirmed builds, 12 required reuses and 31 complete
Explain responses (30 first checks and the existing unchanged reverse diagnostic).
The fixtures retain every invocation's validation, telemetry and speculative
calls as well as ordinary submits, confirmations and Explain. There is no hidden
State baseline. The external input is created before its first model submit;
all model builds and confirmations are present.

Each cell checks literal full typed row multisets, duplicate multiplicity, nulls,
ordered column schemas and MANAGED/Delta properties. Independent State-disabled
dbt executes the workload separately. The final artifact comparison also checks
all actual/reference properties, complete columns and rows across corresponding
hosted and Rust phases. The actual output-name change and the literal/CTE-binding
negative cases independently produce the declared different schema or values.

The fresh Rust run on 2026-11-14 passed the same two histories separately:
**60 dbt invocations, 30 cells, 18 builds and 12 mandatory SKIPs** in 458.92 seconds.
All physical and first Explain checks passed. Its 204 certified RPCs differ from
hosted only by one optional telemetry batch; ordinary/speculative submits,
confirmations, validation and Explain counts match. Both proxy sidecars certify
complete drained captures, and both owned proxy processes exited.

| Phase | RPCs | SHA-256 |
| --- | ---: | --- |
| Hosted main | 179 | `7b2da7cf0e190dacf2d72a86aa988d0ae224b4cfcd4d68514b0337e83221eba2` |
| Hosted CTE | 26 | `973e4b7e32e1f4385bb84e6b9098bd95cd9068dd734f19212c7c5159d3a2edc3` |
| Rust main | 178 | `212215a46738632dd089c35160a2ae2d87a9c83601b2f1aa306769c78d0c175e` |
| Rust CTE | 26 | `2909e5a191600a0a3c5bae07979a934811a1167231ec7b55a6ad876d869c2a65` |

The dbt executable SHA-256 is
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
All four phases report DBSQL `2026.44` with identical recorded build hashes;
`dbr_version` is null, not an inferred DBR version. Writes use only fresh exact
`q_<run>_{input,result}` allowlists in the evidenced demo schema lease. No warehouse schema
CREATE, prior-object cleanup or prior-object mutation occurred. Before/after
inventories preserve every prior object, including the first Rust phase's
relations during the second phase.

## Implementation and version safety

[The bounded rules](../databricks-sql-equivalence.md) use the existing Warehouse
boundary and SELECT grammar guard. Rendering tokenizers preserve raw escapes;
AST parsing validates admission rather than supplying additional semantic
canonicalization. One-table alias declarations and their bound qualifiers change
together, without rewriting physical source tokens. Simple CTE names, definition
order and references stay intact. Unknown grammar, functions, directives and
uncharacterized punctuation retain exactness. Physical history keys, raw target
spelling, organization/family/namespace scoping and current freshness floors are
unchanged. Snowflake/DuckDB identity versions remain unchanged.

The new SQL fingerprint prefix is `dbt-state-databricks-sql-v3`. Normalization is
confined to rendered fingerprints; #159's existing raw-dialect template path and
its exact body/context/dependency handling remain unchanged. No new template or
configuration equivalence is claimed. The representative genuine old rendered-mode fingerprints exercised here
invalidate without being interpreted under the new algorithm. Existing
`compare_unrendered_code` template alternatives can still qualify under the
preserved policy; this is not a blanket all-history invalidation claim.

Frozen v1 and v2 hashes come from retained immutable old servers, not a current
hash function with a different tag. V2 contains actual pending and confirmed row
exports, successful public confirmation and subsequent public reuse. Its dirty
candidate source/provenance and unrelated diagnostic limitations remain explicit;
the accepted v2 identity implementation matched that snapshot. Public upgrade
regressions restore those bytes into protocol-produced rows and require cold
misses for both confirmation orders. Existing frozen-v2 namespace/NULL-epoch
expectations now explicitly reflect the v3 boundary; new current-version positive
controls preserve empty/absent scope and NULL-epoch floor sensitivity. Late old
confirmation still prevents resurrection of an older current-version build.

Local bounded safety sequences cover comment/spacing combinations, source-name
and alias-name collisions, literal and column-name changes, CTE binding changes,
unsupported exactness and distinct quote/backslash spellings. They make no new
hosted-policy claim about alternative escape spellings.

## Validation and retained red evidence

- Full workspace: **645 passed, zero failed, 37 explicitly ignored**, 82 result
  groups (`sql137-workspace-final.log`).
- Exact replay: all **12 tests and 106 corpora** passed, including complete
  responses/statuses and every Explain from both #137 fixtures. New Databricks
  fixtures have zero gaps, masks, exclusions, partial histories or unknown baselines.
- Strict workspace/all-targets/fuzz Clippy, formatting and whitespace checks passed.
- Credential-free Python guard/oracle checks: **33 passed**.
- Supplementary cross-history comparison passed 60 actual/reference result pairs
  and 31 Explain structures. It maps only owned prefixes and temporal display
  text; opaque IDs, measured runtime values and different physical epochs are
  not cross-history equality claims. Exact replay applies no such substitutions.
  Every actual temporal line is separately checked for humanized text and the
  exact UTC timestamp of its captured source epoch, so the diagnostic substitution
  cannot conceal the former raw-millisecond fallback.

The durable archive is
`~/.local/state/dbt-state-rs/databricks-137-20261113/`. It retains both hosted and
both Rust phases, ordered commands/operations, before/after inventories, sidecars,
complete physical results, old-server exports and source/binary provenance.
`artifact-index.json` lists every retained file's size and digest.

Retained failures include the precise immutable-v2 public spacing/comment tracer
(four unchanged controls reused), the first full replay's alias/literal/CTE and
Explain time mismatches, individual literal/CTE red tests, and the escape-spelling
collision red test fixed by raw tokenization. A controller launch lacked PyYAML
before any cloud operation; its message and resolution are retained separately.
The initial strict-Clippy failure was a test-only unnecessary `format!` call,
corrected before the accepted final gate.

The separately validated #140 display-clock partial (`bf36034`) resolves the
identity-change Explain mismatch. Its clock is display-only and Databricks-only;
unknown physical epochs and freshness floors are not inferred from it.
The broader #140 package remains incomplete.

## Reproduction and limits

The opt-in public-TestServer seam is:

```bash
DBT_STATE_DATABRICKS_ARTIFACTS=<fresh-artifact-root> \
DBT_STATE_DATABRICKS_PYTHON=<python-with-PyYAML> \
DBT_STATE_DATABRICKS_DBT=<pinned-dbt-2.0.11> \
DBT_STATE_DATABRICKS_SCHEMA_LEASE=<verified-schema-ledger> \
DBT_STATE_DATABRICKS_QUERY_IDENTITY=20261113a137aa \
DBT_STATE_DATABRICKS_CTE_QUERY_IDENTITY=20261113b137bb \
cargo test -p dbt-state-harness --test databricks_sql_equivalence \
  real_dbt_sql_equivalences_match_independent_results -- --ignored --exact --nocapture
```

The runner's default plan is the 26-cell main history; `--only-cte` starts the
four-cell fresh supplement. `commands.json`, `rust-final-command.json`,
`compare-phases.py` and their logs in the archive preserve the exact accepted
arguments and supplementary verifier. Ordinary PR gates stay credential-free.
The extended real-DuckDB campaign was not rerun for this Databricks-only identity
slice; existing workspace Snowflake/DuckDB regressions passed. Broader #132 release
acceptance and its named capability/Safety blockers remain the integration owner's
responsibility. No PR readiness, issue closure or full-scope support is implied.
