# Prove a Databricks stored-failure output is not a direct test input

Accepted for #144, 2026-11-15, limited to the evidenced grammar below.

The dbt 2.0.11 native table and view audit histories in
`golden/fixtures/databricks_audit_upstream.jsonl` report both the genuine Delta
source and the configured audit alias once it exists. Hosted reuses unchanged
passing and failing results, rebuilds after source changes, and lists only the
source in Explain. The test SQL directly selects `id,v` from that source with
`WHERE v < 0`; the native output definition is the same query. The source is in
prod and both audit aliases are in a separately owned audit schema. This is
positive wire/SQL evidence, independent of Snowflake ADR 0008.

For Databricks data tests with `store_failures: true`, remove a reported audit
output from execution inputs only after proving all of these:

- There are exactly two reported fully qualified names in the characterized
  simple ASCII/backtick identifier subset, and no query dependencies.
- A single parsed direct SELECT has identifier projections and a WHERE made
  only from identifiers, numeric/NULL literals, parentheses, comparisons and
  AND/OR. Functions and subqueries are rejected.
- After removing that checked predicate from a separate AST, the unchanged
  direct-input resolver accepts the entire remaining SELECT. Its existing
  exclusions reject CTEs, joins, derived inputs, functions and other clauses.
- Exactly one reported relation is the resolved source. The other differs
  from that source and lies in the exact `default_catalog.default_schema`.

Existing pinned Databricks `databricks_identifiers.jsonl` and
`databricks_dependency_identity.jsonl` corroborate the leaf-input contract:
direct input views are sent as query dependencies with their definitions, and
`tables` contains their base leaves instead of the view. Such requests fail
both the no-dependencies condition and the direct-input match. The offline
fixture-derived control checks both refusals. Public Fusion source tree
`9977b6cbb1b761065536300037560d8e3c037011` agrees in
`run_cache_service.rs::collect_table_modified_infos` and
`collect_query_dependencies`, and `view_traversal.rs`: only leaves plus output
are physical tables; traversed definitions are separate, and incomplete
metadata prevents submission. That source is corroboration, not an assertion
that the published tree is the exact proprietary 2.0.11 binary. The pinned
wire captures establish the behavior actually used here. Clients that omit
required relation/definition evidence are outside this bounded claim.

The genuine source is retained even when it is itself in the audit schema.
The schema suffix is never evidence. Missing/unknown source epochs still
cannot justify reuse; organization, warehouse, namespace and full UID scope
remain unchanged. Raw protocol evidence is preserved; Submit, Record and
Explain consume the same filtered inputs. SQL fingerprints and resolution
context normalization are unchanged.

Other syntax, ambiguous/duplicate reports and additional dependencies retain
all reported inputs and may conservatively rebuild. Source-less Databricks
ordinary error/recovery contracts stay unchanged. This does not extend
Snowflake elimination to arbitrary Databricks tests or establish ADR 0009's
full hosted-conformance claim.
