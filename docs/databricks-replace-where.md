# Databricks predicate replacement acceptance

The bounded workload uses dbt 2.0.11 on Free Edition serverless Unity Catalog,
managed Delta in `cdo_demo_db`, with zero lag tolerance and `pre_clone='never'`.
The pinned native path is `INSERT INTO … BY NAME REPLACE WHERE … TABLE` with
an unqualified temporary view. No custom macro, adapter override or substitute
strategy changes that behavior.

The [original thirteen-cell evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-replace-where-2026-11-14.md)
and `databricks_replace_where_history.jsonl` cover changed and unchanged
predicates and input, duplicate/NULL bags, outside input deletion retaining its
old target row, empty incoming replacement removing predicate-matching rows,
and full refresh. Both ordinary submits and full-refresh Record input use
DBT_CUSTOM11. The following ordinary type11 rebuild changes rendered SQL from
the full Record SELECT to its steady incremental SELECT before fresh reuse.
All 90 RPCs remain in mandatory replay.

The supplement adds schema and predicate failure/recovery. Four configurations
and their model SQL compile before DDL; the actual pinned materialization macro
is retained and inspected. Its input SELECT has no incremental filter, so an
out-of-range row reaches the warehouse predicate check.

| Operation | Independent expected result |
| --- | --- |
| First build/reuse | Five rows, including a duplicate, NULL predicate key and outside `B` row |
| Source gains `note`; schema policy `fail`, twice | Native exit1/dbt1501; target rows, schema and Delta history unchanged; neither attempt publishes a result |
| `append_new_columns`; reuse | Native ADD COLUMNS then replacement of `A` and NULL; retain outside `B` with NULL note; remove old inside rows absent from incoming data |
| Input now contains outside `B`, twice | Native `DELTA_REPLACE_WHERE_MISMATCH.INVARIANT_VIOLATION` and `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES` for `p : B`; existing complete target and history unchanged; no Confirm/Record |
| Widen predicate to include `B`; reuse | Replace all matching old rows, including the previously retained `B`; insert the complete incoming bag |

Expected bags are independent literal operation ledgers. The current source
alone does not establish the cumulative target. Each cell checks complete typed
source and target multisets, ordered four/five-column schema, MANAGED/Delta
properties and full Delta history. Two SELECT-only literal Reference executions
per cell use State-disabled `dbt show` with isolated zero-RPC canaries. Fault
probes detect fresh-source substitution, retained inside rows, missing outside
rows, duplicate loss, schema changes and incomplete Reference output. Native
output admission faults reject broad tokens, incomplete or wrong predicate/value
diagnostics and transport/auth/network failures. Each unchanged cell must SKIP with its own successful build's runtime and leave
physical history unchanged.

Only exact fresh `pr148_<UUID>_{inputs,consumer}` names in existing
`ds_20261112a134ef_prod` are writable. Hosted UUID:
`b3d9f69c0e324081a99d8fe775ac5f04`; Rust UUID:
`b4781b0e0d3444808e2ceab384dfcae0`. Full paged inventories and exact auxiliary
collision checks preserve every earlier graph. The native consumer staging and
backup names have one concurrent persistent slot; the exact unqualified
session temporary view is separately audited. No schema, persistent Reference,
cleanup, copy or previous-object mutation is part of the campaign.

Raw secret-free artifacts are retained under
`~/.local/state/dbt-state-rs/databricks-148-20261115/`.

The complete hosted fixture `databricks_predicate_recovery.jsonl` has 70 RPCs,
SHA256 `c213e95836c90e55787f8124e1edc2d52455e751853d8e3fc782653b664fc9b2`:
ten ordinary submits, ten speculative submits, ten validations, ten Explain
responses, three Confirms and 27 telemetry calls. All four failed attempts
remain in chronological history. Strict whole-history public replay passed
in 2.01 seconds, including every Explain, with no new gaps or masks.

The fresh Rust production-router process passed the same ten cells in 244.72
seconds. Its complete 70-RPC capture has SHA256
`d215e3870602c6ecfe68d3d8a99f724384bda01e9f11da382c41edd362188a99`.
The process executable SHA256 is
`05cb307d6d3a1562ea6aec7da5a20ebbb50614e2a5c33766ea3c26ca4a1fea4a`;
PID/executable, source and helper receipts are retained. There are no production
server changes. Native requests have no replay-clock header; original audit
clocks are retained before isolated State schema teardown for supplementary
strict replay of the unmodified native capture.

Final offline validation passes all twenty hosted/Rust cells, including actual
`success`/`reused`/`error` client statuses, complete native SQL, exact errors,
source and target epochs, own runtime and typed independent References. Each
backend has 24 zero-RPC canaries: four compiles and twenty References. The hosted
loaded snapshot initially admitted a broad `REPLACE_WHERE` diagnostic; after
observing the real error, only that admission guard was tightened to both exact
Delta codes, predicate and value. All four original errors pass the final guard;
transport/auth/network and incomplete/wrong diagnostics fail its fault probes.
Both loaded snapshots and final-source receipts are retained. No SQL, operational
scope or captured bytes were changed. Production inventory went 53→55→57;
the reference inventory remained 100. Observed persistent native auxiliary peak
was zero for both backends.

The final regression gate passed all 136 mandatory hosted corpora plus this
complete current-Rust history with its exact audit clocks in 261.17 seconds,
including the deferred Explain comparisons. Original and new Python fault
probes, focused harness tests, formatting and strict focused fuzz Clippy pass.

This acceptance bounds the shown predicate forms and additive schema policies.
It does not claim every predicate expression, column removal/type change,
legacy cluster mode or all strategy transitions. Broader node-kind transitions
and Result copy have their own acceptance work. Accepted conservative
UID/namespace/unknown-freshness differences remain in
[the preview](databricks-preview.md) and never authorize unsafe reuse.
