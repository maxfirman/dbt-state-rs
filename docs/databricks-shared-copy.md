# Shared analyzed workload and Result copy acceptance

The shared analysis and copy lifecycle implement the bounded contract in
[ADR 0004](adr/0004-shared-sql-identity-with-dialect-rules.md) and
[architecture](architecture.md#shared-analyzed-workload-contract-181186).
Both Databricks switches remain default off. This is a scoped reuse-safety
change; it does not establish arbitrary SQL equivalence or full hosted conformance.

| Contract | Evidence and practical limit |
|---|---|
| One analysis for SQL Submit/Record, strict fallback and versioned copy proof | `execution::analyzed_workloads_preserve_frozen_execution_identities` retains the pre-extraction identity corpus, with only reviewed v2 transitions. Snowflake/DuckDB identity domains remain unchanged. |
| Ordered operators, explicit binding and conservative effects | `databricks_submit_copy::analyzed_full_checked_forms_and_opaque_effects` and `analyzed_full_dependency_bindings_retain_context_and_definition` cover supported forms, context and dependency-definition swaps. Unsupported or uncertain operations retain ordinary strict fallback. |
| Current source and complete genuine inputs/producers | Public submit/pre-copy tests cover multiple inputs, changed producers, missing epochs, scoped identity, latest/pending source barriers and own relation existence/modification. `decision_proptest` retains pure freshness invariants. |
| Freeze, atomic publication, adoption and source build age | Public pre-copy/submit-copy tests cover source replacement, invalid origins, failed fallback, duplicate Confirm and within/outside Lag tolerance. Automatic FULL and direct MERGE/check use one shared proof and lifecycle with distinct entry protocols. |
| Upgrade and default-off behavior | Actual frozen v1 automatic/direct rows, including pending rows confirmed after upgrade, cold-miss v2 copying. Ordinary legacy strict reuse remains valid. Fresh ordinary source writes establish new v2 evidence; no SQL hydration/backfill. |
| Existing complete observable histories | The conformance suite retains the existing complete Snowflake/DuckDB and Databricks exact/boundary histories. Reviewed v2 persistence transitions are separate from unchanged wire expectations. |
| New representative physical grammar | `databricks_shared_copy.py` is the bounded #186 acceptance driver; its new native wrapper links the current library with both switches and the production System clock. Execution evidence is recorded separately below. |

The deleted Databricks token-count and direct SELECT-star analyzers are replaced
by shared `analyzed_workload` traversal. `CopyProof::for_origin` frames both v2
origins. Shared qualification/freezing/publication replaces parallel lifecycle
checks. No second optimizer or permanent shadow engine remains.

Dialect differences remain deliberate: Snowflake retains its characterized SQL
normalization, clone DDL and hosted policy; DuckDB retains checked local identity,
physical evidence and CTAS property limits. Databricks requires complete shared-v2
proofs, refuses uncertain function/context semantics and conservative stale-source
or copied-source adoption, and uses quiescent managed Delta SHALLOW CLONE.
Physical source races, positive chains, templates and new incremental/snapshot
modes are outside this change.

## Focused physical evidence

The exact fresh graph, source hashes and operation ledger are reviewed before
warehouse mutations. The bounded plan uses two physical inputs, FULL COUNT CTEs
with LEFT JOIN, broader MERGE CTE/COUNT/INNER JOIN and a source-clock check
snapshot. Independent literal expected bags and SCD histories, complete ordered
types, NULLs/duplicates/microseconds, UC provider/type, Delta history, properties
and separate State-disabled native references are required. Successful commands
are never repeated to repair an incidental guard stop.

The new driver is currently preparation evidence. It is not a completed hosted
or Rust physical acceptance claim until the complete retained captures and actual
native clocks have been verified.
