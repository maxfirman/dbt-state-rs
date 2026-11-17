# Shared analyzed workload and Result copy acceptance

The shared analysis and copy lifecycle implement the bounded contract in
[ADR 0004](adr/0004-shared-sql-identity-with-dialect-rules.md) and
[architecture](architecture.md#shared-analyzed-workload-contract-181186).
Databricks copying is enabled by default; [client controls and accepted limitations](databricks-copy-preview.md) apply. This is a scoped reuse-safety
change; it does not establish arbitrary SQL equivalence or full hosted conformance.

| Contract | Evidence and practical limit |
|---|---|
| One analysis for SQL Submit/Record, strict fallback and versioned copy proof | `execution::analyzed_workloads_preserve_frozen_execution_identities` retains the pre-extraction identity corpus, with only reviewed v2 transitions. Snowflake/DuckDB identity domains remain unchanged. |
| Ordered operators, explicit binding and conservative effects | `databricks_submit_copy::analyzed_full_checked_forms_and_opaque_effects` and `analyzed_full_dependency_bindings_retain_context_and_definition` cover supported forms, context and dependency-definition swaps. Named physical columns require complete SQL alias-list presence evidence; unknown column sets cannot establish absence or exclude session-variable fallback. Unsupported or uncertain operations retain ordinary strict fallback. |
| Current source and complete genuine inputs/producers | Public submit/pre-copy tests cover multiple inputs, changed producers, missing epochs, scoped identity, latest/pending source barriers and own relation existence/modification. `decision_proptest` retains pure freshness invariants. |
| Freeze, atomic publication, adoption and source build age | Public pre-copy/submit-copy tests cover source replacement, invalid origins, failed fallback, duplicate Confirm and within/outside Lag tolerance. Automatic FULL and direct MERGE/check use one shared proof and lifecycle with distinct entry protocols. |
| Upgrade behavior | Actual frozen v1 automatic/direct rows, including pending rows confirmed after upgrade, cold-miss v2 copying. Ordinary legacy strict reuse remains valid. Fresh ordinary source writes establish new v2 evidence; no SQL hydration/backfill. |
| Existing complete observable histories | The conformance suite retains the existing complete Snowflake/DuckDB and Databricks exact/boundary histories. Reviewed v2 persistence transitions are separate from unchanged wire expectations. |
| New representative physical grammar | `databricks_shared_copy.py` passed all fifteen hosted and fifteen Rust checkpoints described below, including actual automatic FULL copying and direct MERGE/check adoption, cumulative repair and modified-source refusal. |

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

Both campaigns completed on **2026-11-16 UTC**. Each used exactly eight new
permanent names in the two existing owned schemas, six State-disabled compiles,
fifteen managed commands and fifteen separate zero-RPC native references. Four
input CREATE/INSERT operations, one input value/source-clock UPDATE, one owned
production MERGE sentinel UPDATE and one owned development MERGE DROP executed
once per backend. No new schema, permanent reference, cleanup or repeated
successful command was needed. Final paged inventories were 97 production and
26 development relations; pre-existing relations were preserved.

Both runs copied FULL automatically and MERGE/check through RegisterClone, then
SKIPped. Independent literal bags, ordered INT/BIGINT/STRING/DATE/TIMESTAMP schemas,
NULLs/duplicates, microseconds and the complete three-version check SCD history
passed. After the declared input/policy change, MERGE and snapshot built and
reused their own results. The modified-source negative caused hosted to clone
then repair with MERGE; Rust refused the copy, built the correct result and
then SKIPped. Every checkpoint retained physical results, Delta history, UC
evidence and relevant properties.

The whole hosted **95-RPC** boundary replays with exactly six reviewed response
differences: two Explain query badges conservatively reflect changed opaque
body/config signatures when `pre_clone` switches to `never`; four responses are
causal consequences of refusing hosted's modified-source copy. The ungranted
Confirm returns NOT_FOUND and publishes nothing. Beyond existing opaque-ID
normalization, no field or timestamp is masked and no pending execution is invented. All other complete responses,
including dependency rows/timestamps, match. The Rust **94-RPC** history replays
exactly using all fifteen actual persisted audit clocks retained before local
State schema cleanup. Live requests carried no replay-clock headers; the native
server asserted the production System clock and both then-required experimental
switches, which have since been removed.

Source attribution is distinct: hosted ran clean `86b3898`; native ran clean
`184f85c` (only the replay tests differ). Its current-library executable SHA256
is `035ed6a1ffbe83e2191832d5a8390553aab0aa64139003f170e344170aa057e6`.
The subsequent lint/doc followups do not change the production library or driver.
Complete raw captures, immutable expected bags/ledger, tool/source hashes,
clearance/ownership receipts and exact replay outputs are retained under
`~/.local/state/dbt-state-rs/shared-copy-181-20261115/`.

This proves the specified quiescent managed-Delta slice. It does not prove
unknown physical-column bindings, arbitrary operators/session settings, native
Build-producer graphs, new strategies/snapshot modes, positive chains, physical
copy races or ordinary FULL replacement of an existing shallow clone. Public
protocol/property tests cover producer drift, unknown epochs, invalid publication
and fallback; those are not additional native feature claims.
