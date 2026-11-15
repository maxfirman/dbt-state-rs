# Databricks MERGE acceptance

The bounded MERGE workload is verified with dbt 2.0.11 on Free Edition
serverless Unity Catalog, using managed Delta in `cdo_demo_db`. The existing
[baseline report](https://github.com/maxfirman/dbt-state-rs/blob/d4ac553cdd09becb77c7555b143e5f4bedaeb2fb/docs/reports/databricks-merge-2026-11-14.md)
and `databricks_merge_history.jsonl` cover eleven actual hosted and Rust runs:
unique-key updates/inserts, retained source-missing rows, projection change,
reuse, CLI full refresh and the next MERGE/reuse. Full refresh bypassed Submit
and published one `RecordExecutions` FULL1 row; the next MERGE3 rebuilt before
reusing. Complete Explain and subsequent public calls establish persistence.
No process restart is claimed by that trace.

The 2026-11-15 supplement covers the remaining predicate and schema-addition
acceptance cases. Expected rows come from an independent cumulative operation
ledger, rather than equating the incremental target with the current source.
Source keys stay unique and non-NULL; the final predicate deliberately creates
a duplicate target key. No further MERGE runs against that duplicate target.

| Operation | Expected physical result and publication |
| --- | --- |
| Initial build/reuse | Two three-column rows; Confirm then SKIP |
| Source gains `note STRING`; `on_schema_change='fail'`, twice | Native CLI exit1, dbt1501 schema mismatch; unchanged target rows/schema/Delta history; no Confirm, Record or Clone |
| Switch to `append_new_columns`; reuse | Native ADD COLUMNS and MERGE; update key1, insert key3, retain source-missing key2 with NULL note; Confirm then SKIP |
| Replace source with keys3/4; reuse | Update key3, insert key4, retain keys1/2; Confirm then SKIP |
| Destination predicate `DBT_INTERNAL_DEST.id >= 2`; reuse | Actual predicate in native MERGE; update key3, insert key5, retain keys1/2/4; Confirm then SKIP |
| Reintroduce source key1 under that predicate; reuse | Old key1 is excluded from matching; insert a second key1, retain both complete rows; Confirm then SKIP |

Each checkpoint checks the complete typed source and target bags, ordered SQL
API schema and managed/Delta properties. Separate literal Reference executions
use State-disabled, SELECT-only `dbt show` with isolated recorders proving zero
RPCs. Those references validate cumulative expected target rows separately from
the current source rows. Fault probes reject fresh-source substitution, missing
retained rows, invented backfill, lost duplicates, schema drift and malformed
four-column Reference output.

## Evidence

Raw, secret-free evidence is retained under
`~/.local/state/dbt-state-rs/databricks-145-20261115/`. Hosted used
`hosted/` followed by the same-graph `hosted-predicate-effect/` continuation;
its original ten-cell capture was preserved. The continuation verified the
complete prior capture, ownership, inventory and source/target preconditions
before its one source overwrite. No UUID was abandoned or graph restarted.

The complete 84-RPC hosted history is
`golden/fixtures/databricks_merge_recovery.jsonl`, SHA256
`e7829028e2990e36422e94b0ff748d3ca5234135116dfcf33e989b68e677be03`.
It retains all validation, telemetry, speculative and ordinary submissions,
Confirm calls, twelve complete Explain responses, statuses and metadata.
It has no replay gap, missing baseline or partial-history exemption.

Rust completed all twelve cells in one current production-router TestServer
campaign (`rust/campaign/`), 246.84 seconds, with a complete 84-RPC capture:
SHA256 `a3eadb28f3851ae64fa485aedc74320310dbb1b536270a01f81e3be975d1fcd1`.
The running test process executable/PID/hash receipt is in `rust/`; no old
standalone server was used. Native requests supplied no replay-clock header.
The server library was based on `801cddd`; the subsequent #143 change affects
only data-test Explain display, with MERGE behavior unchanged.

The strict public replay passed the branch's original 131 corpora plus the new
MERGE history (132 total), including all twelve new Explain trees. The merged #143
fixture brings the final mandatory set to 133; CI verifies that combined set. Six Python
fault probes and both credential-free MERGE harness tests pass. Final native
and Reference guards were also checked against all 24 retained hosted/Rust
cells; this is offline validation of the original raw executions.

Only the exact fresh `mr145_<UUID>_{inputs,consumer}` pairs in existing
`ds_20261112a134ef_prod` were writable. Hosted UUID:
`ec26c5916ee14c25b1b476beb2fe2d49`; Rust UUID:
`74fdebab8dac4bb78d154940594e5c1a`. Native session temporary views and exact
consumer staging/backup spellings are audited separately; no persistent
auxiliary was inferred from its spelling. Full paged inventories protect every
preexisting object, including earlier retained graphs. No new schema, cleanup,
copy or old-object write is part of this campaign. The observed production
inventory went 41→43 hosted and 43→45 Rust; the reference container remained 100.
Native SQL created only each owned consumer and session temporary views; its
audited persistent auxiliary peak was zero.

The tested behavior bounds this acceptance; it does not establish NULL/composite
keys, ambiguous source matching, every predicate expression or every schema
removal/type-change setting. Accepted conservative UID, namespace and unknown
freshness differences remain documented in [the preview](databricks-preview.md).
They do not permit unsafe reuse. Broader lifecycle/upgrade, Result copy and
other incremental strategies retain their own acceptance tickets.
