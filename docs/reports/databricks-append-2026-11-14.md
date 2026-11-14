# Databricks APPEND operation history — 2026-11-14

This is a bounded independent #146 acceptance slice under #132. It covers a
stable rendered APPEND workload with duplicate rows and nullable values, actual
source updates/inserts/deletion, changed projection, and native full refresh
followed by ordinary APPEND. Native #146 and dependencies #138/#140 remain
incomplete. It does not establish full Databricks support.

The official dbt 2.0.11 executable is pinned to SHA256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`.
The workload uses SQL models, serverless Unity Catalog managed Delta tables,
rendered comparison (`compare_unrendered_code=false`), zero Lag tolerance,
`pre_clone=never`, `unique_key=id`, and `on_schema_change=ignore`. The body has
no `is_incremental` branch, so the first/repeat requests have identical rendered
SQL. No custom macros, client edits or behavior overrides are used.

## Independent physical results

The source initially holds `(1,10,'same')` twice, `(2,20,'old')` and
`(3,NULL,NULL)`. The declared source operations update both key-1 rows to
`(1,11,'changed')`, insert `(4,40,'new')` twice, then delete key 2. A later
projection adds 100 to selected amounts. The target oracle is an immutable
worked literal history, independent of State's identity/freshness code and the
actual target contents.

`R` is the entire expected cumulative target multiset; `F` is the rows selected
by the current query. Initial creation and full refresh replace `R` with `F`.
An ordinary successful APPEND adds `F` to the prior `R`; an eligible SKIP keeps
`R` unchanged. A fresh source SELECT alone cannot check this history.

| Checkpoint | R rows | F rows | Required physical meaning |
| --- | ---: | ---: | --- |
| First / unchanged | 4 | 4 | Exact duplicate rows and nullable amount/tag preserved |
| Source update+insert / repeat | 10 | 6 | Old key-1 values remain; changed rows and duplicate key-4 rows append |
| Source deletion / repeat | 15 | 5 | Deleted key-2 row remains twice in accumulated history |
| Projection change / repeat | 20 | 5 | New projections append; previous values and projections remain |
| Full refresh | 5 | 5 | Prior versions and deleted key-2 rows disappear |
| Next ordinary APPEND / repeat | 10 | 5 | Ordinary build appends to refreshed rows; repeat keeps all ten |

Every checkpoint compares complete typed rows, multiplicities, SQL NULL values,
ordered BIGINT/INT/STRING columns and MANAGED/Delta properties. A separate
literal cumulative `R` SELECT is executed through both the typed SQL API and
an isolated State-disabled `dbt show` invocation with a certified zero-RPC
canary. The selected `F` has its own independent literal SELECT and comparison
against the actual source query. No persistent reference graph is allocated.
The offline fault probes reject fresh-source substitution, deduplication, NULL
string coercion, schema/order changes, shallow-clone substitution and retaining
the refresh result after an ordinary APPEND build.

## Evidence

Private archive: `~/.local/state/dbt-state-rs/databricks-146-20261114/`.
Its evidence index and JSON report link complete captures, source/executable
pins, process receipts, per-invocation requests/responses/statuses, run results,
compiled/native SQL, typed result/reference envelopes, full Explain trees,
own-runtime echoes, Delta properties and inventories.

Hosted run `20261114a146aa` completed once: 11 managed invocations plus one
State-disabled parse, 11 complete `R`/`F` physical checks and 11 certified
zero-RPC reference canaries. Its complete 75-RPC capture has SHA256
`b1e2b078006d56768f1a8204f76ccd7c9ca48b7e0cc11c24aeefce73660f8ff2`.
The unfiltered fixture is `golden/fixtures/databricks_append_history.jsonl`.
The five SKIPs echo their own successful APPEND Confirm runtimes
5722/7930/6281/6228/6410ms; cross-backend runtimes are not expected to match.
All requests and responses retain their actual status, reasons, descriptions,
optional fields and payloads.

The native CLI full refresh sends no Submit. It emits one RecordExecutions
with FULL execution type 1, outcome epoch 1794685032000, runtime 3356ms and
`records_stored=1`. The next ordinary request carries that exact current target
epoch, submits APPEND type 2 and rebuilds with the hosted external-modification
description and reasons 6/6. Its successful INSERT appends the five selected
rows; its unchanged repeat SKIPs. An older APPEND result is not resurrected.

The fixture includes 11 nonempty complete Explain trees (five EXECUTE and six
SKIP, including the same-ID reverse-request control), with no new INFO or
unspecified marker/badge vocabulary. The complete hosted corpus replay passed all 124 fixtures. This new fixture
contributes 64 assertions with zero gaps, unknown baselines or partial history. Native Rust results are recorded in the companion JSON.

The first native attempt was interrupted during full refresh. Its eight completed
physical checkpoints and complete 59-RPC recording remain separate in the archive
(SHA256 `a44183be024acafc58de3bc282dc521a6687cac900c62c9068270c8f8064bd00`).
A read-only audit proved the CTAS committed five rows, but the client never
recorded that outcome or completed its invocation. This attempt did not pass.
Root reviewed exact table UUIDs, rows, history and ownership, retired only the
two interrupted Rust tables, and preserved the hosted pair and every other
object. The stopped preflight and completed retirement have separate receipts.

One cleared clean native retry used the same immutable acceptance executable
(SHA256 `18f4de3172ec0da44860bfa8edc70e81b1ac683f9c2fd3acf71cd8f17844873d`),
unchanged helper sources and tools, the same two allowed relation names, a
fresh evidence directory and independent State history. It passed in 169.11s:
11 managed invocations, 11 complete typed `R`/`F` checks and 11 certified zero-RPC
references. Its complete 75-RPC recording has SHA256
`42a5296c8bc4a48c9d2e1297a9de74e2f6a91f698b273f47c666b9aca23a4acb`.
All ten stable ordinary response payloads match hosted responses. Its five
SKIPs echo their own confirmed runtimes 5689/7425/6163/6162/6184ms. Native full
refresh records FULL1 with epoch 1794687578000, runtime 3591ms and one stored
record; the next APPEND carries that exact target epoch and rebuilds, then
reuses. No history reconstruction, capture concatenation or hosted rerun was
used. All 87 pinned source inputs and executable digests remained unchanged
through live use; all owned processes drained, and the interrupted archive's
file hashes still match.

The unmodified clean Rust capture also passed supplementary public replay
alongside all 124 hosted fixtures in 276.17s. This is a source-consistency check,
separate from the hosted conformance evidence. Four APPEND oracle fault probes,
five existing read-only Reference tests, the focused harness default test,
strict focused Clippy, formatting and Python syntax checks pass.

Production code is unchanged. All 159 production/protocol/DuckDB gate inputs
remain byte-identical to the prior validated source. The prior 675-workspace,
310-library and 25-DuckDB receipts apply to those identical inputs; these are
reused receipts, not newly executed full-suite gates for #146.

## Footprint and limits

Root allocated the fresh exact container `cdo_demo_db.state_20261114_append146`
and reviewed its UUID/owner/creation receipt and empty initial inventory.
Each backend owns only `a_<run>_inputs` and `a_<run>_consumer`. Four permanent
names total and one concurrently live persistent replacement slot are reserved.
Only each owned consumer's `__dbt_stg`/`__dbt_backup` spellings are admitted;
`<consumer>__dbt_tmp` must be an unqualified session temporary view. The actual
native path creates the first/refresh target directly and uses a temporary
view plus INSERT SELECT for ordinary APPEND. No persistent auxiliary peak was
observed. No prior object is writable by the runner. The separately reviewed root
retirement of the interrupted Rust pair is the only cleanup in this campaign.

This bounded trace uses known complete source/target epochs and requires actual
source epoch advancement. Missing, omitted, equal changed-data or delayed
freshness evidence stops this trace and remains #140 work. No replay mask,
known-gap allowance or unknown baseline is introduced. All organization,
warehouse-family, namespace, UID and latest-physical-history safety floors are
preserved; production code is unchanged.

Still required for whole #146: supported settings and schema evolution,
column-name binding/reordering, conditional `is_incremental` selection,
strategy/node-kind transitions, generated pending/failed/late-confirm histories,
restart/upgrade and reachable safer replacement. The configured unique key
is present but APPEND does not merge duplicate keys. Nullable non-key values
are exercised; this trace makes no NULL-key claim. Existing copy, snapshot,
identity and freshness blockers remain unresolved under the full #132 scope.
