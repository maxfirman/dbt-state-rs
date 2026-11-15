# Databricks APPEND acceptance

Issue #146 covers the pinned dbt 2.0.11 client (SHA256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`)
on serverless Unity Catalog managed Delta. The independent cumulative bag
models APPEND operations; a fresh SELECT of the source cannot substitute for it.

| Original criterion | Evidence |
| --- | --- |
| Duplicate-preserving append, supported settings, full refresh | Original eleven-cell hosted/Rust graph: duplicate and NULL rows, updates/inserts/deletes, changed projection, `unique_key=id` with `on_schema_change=ignore`, required unchanged SKIPs, CLI full refresh recorded as FULL, next APPEND rebuild and reuse. `databricks_append_history.jsonl` retains the whole 75-RPC hosted history. |
| Supported strategy/node-kind transitions reject incompatible old results | Supplemental fourteen-cell graph: APPEND→MERGE→APPEND→APPEND with `append_new_columns`→table→view→APPEND. Each boundary has a required unchanged repeat. Complete hosted history and independent native Rust results are required. |

The original graph's cumulative target grows 4→10→15→20 rows. Full refresh
resets it to five; the next ordinary APPEND produces ten and its repeat keeps
ten. Source updates retain old target values and source deletions retain old
target rows. The original native refresh sends RecordExecutions FULL (1),
`records_stored=1`; the next APPEND (2) rebuilds and then SKIPs. The retained
original archive is `~/.local/state/dbt-state-rs/databricks-146-20261114/`;
the detailed archived report is available at commit `d4ac553`,
`docs/reports/databricks-append-2026-11-14.md`.

The supplement uses one two-object graph per backend. MERGE receives unique
nonNULL keys before APPEND introduces repeated keys. MERGE updates key 1,
inserts key 3 and retains source-missing key 2; the return to APPEND accumulates
all selected rows. Adding `note` with `append_new_columns` preserves old rows
with NULL note and appends the new four-column rows. A table boundary replaces
the accumulated bag; the view boundary exposes the new source, and APPEND over
the view creates a new managed Delta table. No MERGE runs against the duplicated
APPEND target. Every repeat must preserve the entire bag and Delta history
(where the target is a table), catching unwanted additional appends.

The runner compiles all seven configurations before DDL. Typed SQL API reads
check the complete source and target bags, ordered schema and relation kind.
Each has an independent literal SQL reference plus an isolated State-disabled
`dbt show` with zero-RPC canary. Native SQL audits restrict writes to the exact
owned target and reserved native auxiliary spellings; session temporary views
and persistent object lifetimes are counted separately. Views require VIEW
properties and have no Delta history assertion.

The tested settings are `ignore` and additive `append_new_columns`; the graph
does not establish every schema removal/type conversion, every strategy pair,
NULL/duplicate-key MERGE, or other storage/client/deployment variants. Unknown
incompatible node kinds continue to rebuild conservatively. Accepted UID,
namespace and unknown-freshness differences are bounded by
[the preview](databricks-preview.md); they never authorize unsafe reuse.

The 2026-11-15 hosted supplement retains all 97 RPCs, including fourteen
complete Explain responses, in `databricks_append_transitions.jsonl` (SHA256
`abed8ff529a3550a91ceac625f68873beeb73fcb39f973a6f5c9e7e0f93cfe8f`).
Its primary 76-RPC session stopped after a successful native view confirmation
because the local SQL auditor initially rejected a staging view. That raw
session remains unchanged. A 21-RPC continuation verified the completed view,
then ran only its repeat and the final APPEND pair on the same graph. No initial
build was repeated and no replacement graph was allocated. The corrected audit
accepts only the exact reserved view staging/rename path; its default MERGE
auditing remains table-only.

Raw supplement evidence is retained at
`~/.local/state/dbt-state-rs/databricks-146-20261115/`. Exact UUIDs in existing
`ds_20261112a134ef_prod`: hosted `179c2e624d444d228a3b4d3de39b6e54`,
Rust `7f6ff17715bf48669e4ca883699b22b0`, prefix `at146_`, suffixes
`inputs`/`consumer`. Only the consumer's declared native staging/backup names
are additional writable objects. No new schemas, old-object writes, manual
cleanup or Result copies are part of this campaign.

Exact public replay initially exposed two transition rejection-code differences
and two Explain differences. The correction decorates an already forced BUILD
for the captured APPEND/MERGE pair, with zero Lag tolerance, ANY policy,
known positive epochs, unchanged current target and source relations with explicitly absent recorded
producers (`Producer::NoBuild`). Current physical source drift reports 6/6; no drift
reports the type mismatch 2/2. Reuse selection still rejects against the latest
build of the submit's own type. Its matched ID and decision-time inputs remain
unchanged. A separate latest-row display basis is persisted in the existing
unmatched slot for Explain; it never authorizes reuse or alters Lag tolerance
or producer evaluation. FULL refresh, other node kinds, incomplete evidence,
other policies and recorded upstream producers retain their existing behavior.
The table→view correction only empties the freshness section for the captured
Databricks FULL-prior, relation-reading view with no genuine reported inputs.

Native Rust completed fourteen cells in 305.96 seconds using the current
production-router TestServer, executable SHA256
`cae94817147739f0c679d9b5aa85dfaf104a863519a2de2ca556893b8e0abae6`.
Its complete 97-RPC capture has SHA256
`2831fc132973ae68ba1fe8252b03fd90f17016246aa256729ce6c1e2889ac892`.
Final offline guards pass all 28 hosted/Rust cells. The schema inventory grew
45→47 hosted and 47→49 Rust; the reference container stayed at 100. The native
view swap's observed extra persistent peak was one, with exact auxiliaries
removed by dbt itself. All previous objects were preserved.

The corrected whole 134-corpus replay passed in 250.56 seconds. Review then
narrowed display admission to known same UID and rendered comparison only;
every native request already meets both conditions. Final strict 97-RPC replay
and fourteen synthetic guard cases pass, including persisted old-anchor/input
checks and namespace/config controls. Nine focused public tests, 310 server
library tests and strict server/harness Clippy pass; CI verifies the final
combined corpus. These guard refinements and final offline validation are not
a second native campaign. ADR 0005 records the separate display ID contract.
