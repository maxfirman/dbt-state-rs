# Recognize an aliased `store_failures` audit relation by elimination

## Status

Accepted (#123, maintainer decision 2026-11-11: option 3 of the issue,
limited as below). It loosens the dependency-set check for data tests; it
adds a rule next to #88's exact-name rule and changes nothing else in the
evidence a submit or a record writes.

## Context

A data test with `store_failures: true` writes its failing rows to an audit
relation, and client 2.0.11 reports that relation in `tables` once it exists:
dbt-fusion `collect_table_modified_infos` always adds the node's own relation
(`node.database()`.`node.schema()`.alias) to the relations it reports.
Hosted SKIPs the unchanged rerun anyway and lists only the tested source
(#88). The server drops the audit relation from the test's inputs when the
request names it exactly, as `default_catalog`.`default_schema`.`dbt_node_name`
(`execution.rs::is_own_audit_relation`, #88, #106).

Under a configured `alias` (`<test>_aud`) no request field carries the alias:
not `labels`, `semantic_extras`, `dbt_node_state` or the ConfirmExecution.
Hosted still SKIPs both unchanged reruns, for an audit table and an audit
view, and lists only the source
(`golden/fixtures/explain_store_failures_audit_alias.jsonl` entries 9–12, live
2026-11-11). Before this ADR the server kept the aliased relation as an
unrecorded input and rebuilt with `dependency_set_changed` (6 / 6).

## Decision

For a data test (execution type `DBT_DATA_TEST`) whose semantic extras set
`store_failures: true`, on Snowflake, a reported table is not an input when
all of these hold (`execution.rs::unnamed_audit_relation`):

1. it is in the test's own schema: `default_catalog`.`default_schema`
   compared with the Warehouse naming rules (an unquoted identifier folds to
   upper case, a quoted one is exact; `Warehouse::relation_in_schema`);
2. neither the test's SQL nor any of its query dependencies' definitions
   mentions its identifier, by the whole-word, case-insensitive check #109
   added (`mentions_identifier`);
3. it is the only reported table that meets 1 and 2. When two or more do,
   including one the exact-name rule recognizes or the same name reported
   twice, the request is ambiguous and none is dropped by this rule.

The exact-name rule (#88) is unchanged and checked first. Both rules are
applied where the execution evidence is built (`execution.rs::sql_inputs`),
so the decision, the pending and confirmed rows and Explain read the same
inputs. The Explain capture's fallback for decisions that record no inputs
uses the same function.

## Why a table the test's SQL never names cannot change its result

A data test's result (`failures`, `should_warn`, `should_error`) is its
compiled SQL evaluated against the relations that SQL reads, directly or
through views. The client sends the definition of each view it expands as a
query dependency. A relation that neither the SQL nor those definitions
names is not read by the query, so its contents cannot change the result.
The audit relation is written after the result is computed: it is the test's
output.

The client agrees with this. It reports a table only when the test's SQL
names it (parsed relations), when a view definition it sends as a query
dependency reaches it (view-traversal leaves), or when it is the node's own
relation (dbt-fusion `run_cache_service.rs` `collect_query_dependencies` and
`collect_table_modified_infos`, which keeps only those leaf tables plus the
target). So on client 2.0.11 the one reported table in the test's schema
that nothing names is the test's own relation.

## Limits

- **Snowflake only.** Other warehouses recognize nothing in a schema, so the
  rule never fires there. DuckDB keeps its own evidence rules, and hosted
  does not engage for DuckDB.
- **Data tests that store failures only.** `store_failures` false or unset,
  models, snapshots and every other node keep every reported table.
- **Ambiguity disables it.** Once the audit relation exists it is reported on
  every run, so any other unnamed table in the schema makes two qualifying
  tables and both stay inputs. A genuinely new upstream next to the audit
  relation therefore still rebuilds, whether the SQL names it (it fails 2) or
  not (it fails 3).
- **The mention check is textual.** It over-approximates a reference (a
  comment or a string literal counts), which only keeps tables. It cannot see
  a read that names no relation, such as a function body or a view whose
  definition the request omits; client 2.0.11 reports no such relation, so
  only a crafted or different client can send one. An identifier with an
  embedded quote is always kept, because SQL spells it doubled.
- **First execution.** Before the audit relation exists, a lone unnamed
  table in the schema is dropped. On client 2.0.11 that can only be the
  node's own relation left by an earlier run. If it were a read, the second
  run reports it next to the audit relation, the request is ambiguous, and
  the dependency-set change rebuilds.
- **Not hosted's rule.** Hosted's own mechanism is not observable: every
  capture is also explained by "compare only the inputs recorded at the
  build". This rule reproduces the captured outcomes, and stays on the
  conservative side wherever it cannot tell.
- **Not captured:** a long generic test name whose alias is dbt's truncated
  name. It is the same mechanism (the relation is in the test's schema and
  unnamed), so the rule covers it.

## Considered options

1. **Accept the divergence.** An aliased `store_failures` test re-runs on
   every invocation. Rejected by the maintainer: it is the default outcome
   for a common configuration.
2. **A client change** that sends the test's relation (for example
   `target_table` for data tests, or an alias label). Exact, but needs a
   client release, and data tests deliberately submit with no target
   (`run_cache_request.rs::build_test_sql_request`). Still preferable if the
   client ever sends it.
3. **Recognize the relation by elimination** (this ADR), limited to the one
   unnamed table in the test's schema.

## Consequences

- `explain_store_failures_audit_alias.jsonl` entries 9–12 replay exactly; the
  keyed gaps are removed.
- A data test now rebuilds for `dependency_set_changed` only when a genuinely
  new table is reported, which no capture shows, so its Explain tree still
  renders nothing.
- AGENTS.md reuse invariant 5 notes the rule.
