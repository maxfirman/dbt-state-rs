# Databricks insert-overwrite acceptance

The bounded workload uses dbt 2.0.11 on Free Edition serverless Unity Catalog,
managed Delta in `cdo_demo_db`, with State lag tolerance zero and
`pre_clone='never'`. The pinned client implements partition replacement with
`INSERT INTO TABLE … REPLACE ON`, rather than legacy dynamic partition overwrite.
Its emitted predicate compares each partition component with NULL-safe `<=>`.
No adapter behavior flag, custom macro or alternate strategy is forced.

The [original thirteen-cell evidence](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-insert-overwrite-2026-11-14.md)
and `databricks_insert_overwrite_history.jsonl` establish single DATE partition
replacement with duplicate/NULL bags, deletion inside a represented partition,
disappearance of a NULL partition, empty selected input retaining old target
partitions, restoration, and CLI full refresh. Full refresh publishes FULL1 via
RecordExecutions; the next ordinary type4 run builds before reusing. All 89 RPCs
and thirteen Explain responses remain in the mandatory replay.

The supplement exercises two STRING partition keys, NULL in either or both
components, duplicates and additive schema evolution. All three actual configs
and their model SQL compile before warehouse DDL; the pinned materialization
macros are retained and inspected. Complete native SQL establishes the two-key
predicate `t.p <=> s.p AND t.q <=> s.q` on successful replacements.

| Operation | Independent expected result |
| --- | --- |
| Initial build/reuse | Six four-column rows, including a duplicate and three NULL-key combinations |
| Source gains `note`; `on_schema_change='fail'`, twice | Native exit1/dbt1501; target rows/schema/Delta history unchanged; neither attempt Confirmed or Recorded |
| `append_new_columns`; reuse | ADD COLUMNS then replacement of `(A,x)`, `(NULL,x)`, `(A,NULL)`; retain `(A,y)` and `(NULL,NULL)` with NULL note |
| Change complementary partitions; reuse | Replace `(A,y)` and `(NULL,NULL)`; retain all rows of the other three partitions, including duplicates |
| Empty input; reuse | Retain the complete previous target bag |

Expected results are literal cumulative operation ledgers, established separately
from warehouse results. The current source alone is not the expected target.
Each checkpoint checks full typed source/target bags, ordered schemas,
managed/Delta properties, target partition metadata and complete Delta history.
Two separate literal Reference executions use State-disabled SELECT-only
`dbt show`, with isolated recorders proving zero RPCs. Fault probes reject lost
unrepresented siblings, fresh-source substitution, duplicate loss, invented
backfill, schema changes and incomplete Reference output. Unchanged runs must
SKIP, echo their own confirmed runtime and leave Delta history unchanged.

## Evidence and bounds

Raw secret-free artifacts are retained under
`~/.local/state/dbt-state-rs/databricks-147-20261115/`; the hosted graph is
`hosted/`, and the current production-router Rust graph is `rust/campaign/`.
The new complete 70-RPC hosted fixture is
`databricks_overwrite_recovery.jsonl`, SHA256
`fd23897e8c125c652e2c567beb0f00b628d2fa33656a2231aa2e78e0517e3eef`.
It retains ten ordinary submits, ten speculative submits, ten validations,
ten complete Explain responses, four Confirm calls and 26 telemetry calls.
Both native schema failures are retained with their actual client errors.
Strict public replay of this entire history passed, including every Explain;
there is no new gap, mask or partial-history exemption.

Rust passed the same ten cells in 216.53 seconds, with complete 70-RPC capture
SHA256 `a69a73c8a755d5077d71a1ce9f3092938cf0da0da5ff0a1eeb778543e6a96b73`.
Its test process runs the current production router, based on `c019873`, with
no production changes. The process executable SHA256 is
`4bacc4bcf14a513534a4e48cd6fa3386940df9b7b3b0914af803533a092644b6`;
PID/executable, helper hashes and source receipts are retained. Native requests
carry no replay-clock header. The test wrapper preserves the original persisted
audit clocks before removing its isolated State schema, so supplementary native
replay can use exact clocks without modifying the raw capture.

Final offline checks pass for all twenty hosted/Rust cells, including actual
`warn`/`reused`/`error` statuses and complete native/typed/Reference guards.
Each backend has 23 isolated zero-RPC canaries: three compiles and twenty
References. Both loaded driver snapshots are retained. A subsequent change only
saves the already-enforced final scope in `footprint.json`; original inherited
bookkeeping is preserved alongside a scope receipt from the exact operational
manifest. SQL, validation and operational allowlists are unchanged.

The final regression gate passed all 135 mandatory hosted corpora plus this
supplementary current-Rust history in 260.74 seconds, including complete Explain
comparisons. Focused harness tests, the original thirteen
and new two Python fault probes, formatting and strict focused fuzz Clippy pass.

Only exact fresh `or147_<UUID>_{inputs,consumer}` pairs in existing
`ds_20261112a134ef_prod` are writable. Hosted UUID:
`ca761f9ab62247e1a8b3fa5b0ed7d5b9`; Rust UUID:
`06d2a05954aa4e90ba3403cb189f849d`. Full paged inventories and collision checks
protect all earlier graphs. Native-only exact consumer staging/backup names and
session temporary views are audited by observed lifetime. Production inventory
went 49→51→53; the reference container stayed at 100. Observed native persistent
auxiliary peak was zero. No new schema,
cleanup, copy or old-object write is part of the campaign.

This establishes the two partition configurations and schema addition policies
shown above. It does not claim every partition type, column removal/type change,
liquid clustering, legacy cluster overwrite mode or restart scenario. Those are
outside this scoped acceptance, rather than inferred hosted policy. Broader
node-kind transitions and Result copy retain their separate acceptance work.
Accepted conservative UID/namespace/unknown-freshness differences remain in
[the preview](databricks-preview.md); they never permit unsafe reuse.
