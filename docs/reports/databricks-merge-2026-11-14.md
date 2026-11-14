# Databricks MERGE incremental history — 2026-11-14

The official dbt 2.0.11 client completed the bounded MERGE history below against
hosted State and the current Rust server, with exact hosted replay and independent
physical checks. **This is a partial of #145, not full ticket completion or a
Databricks support claim.** Native #138/#140 dependencies remain incomplete;
predicates, schema evolution and other coverage below remain required. No
production code, fingerprint, isolation, freshness or copy eligibility changed.

## Environment and evidence

The campaign used the pinned official dbt 2.0.11 executable, Databricks Free
Edition serverless, Unity Catalog and managed Delta. The original owned-container
lease mapped production to `cdo_demo_db.ds_20261112a134ef_ref` and inventory-only
reference to `ds_20261112e134ef_ref`. Every phase used a fresh SQL incremental
model with explicit `merge`, `unique_key='id'`, `file_format='delta'`,
`on_schema_change='ignore'`, rendered comparison, zero Lag tolerance and
`pre_clone='never'`. The actual namespace was
`f72415cee49eac9b2b28d9ad8e588507`. There were no hooks, conditional
`is_incremental()` branches, macro overrides or behavior-flag overrides.

The source/target pairs were exactly
`m_20261114a145aa_{inputs,consumer}` for hosted and
`m_20261114b145bb_{inputs,consumer}` for Rust. Root separately retired five
completed reference outputs before this campaign; this runner performed no
cleanup, prior-object writes or schema creation. Fresh inventory was a95/e100,
then a97/e100 after hosted and a99/e100 after Rust. All preexisting objects were
preserved. The twelve original incremental relations and unrelated graphs
remained protected.

Evidence is retained under
`~/.local/state/dbt-state-rs/databricks-145-20261114/`:

| Evidence | Location / SHA256 |
| --- | --- |
| Complete hosted capture, 75 RPCs | `hosted/golden/`, `98ba6e18fa71ae1d22c2ca53549c68bef0b66a858cea286f39a236ed28bb4aee` |
| Complete Rust capture, 75 RPCs | `rust/campaign/golden/`, `c2416363b57f5614e86c52a19443ae319139b47e8642a9e6d05537812b119343` |
| Immutable live server executable | `rust-launch/acceptance-test`, `7ed610fe889504afc166b2b847805b2706e40e79f83b4020d7312e6b8b8c0065` |
| Source/tool pins and actual PID/executable receipts | `live-tools/`, `rust-launch/` |
| Full physical checks, native SQL, inventory, disabled references | `hosted/`, `rust/campaign/` |
| Current Rust response/runtime/epoch audit | `rust/campaign/physical-response-audit.json` |
| Final ownership and unchanged pin audit | `rust/final-acceptance-proof.json` |

The Rust test used the production router and an isolated Postgres State schema;
actual dbt requests carried no replay-clock override header. Both full captures contain validation 11, telemetry 27, speculative submits 10,
ordinary submits 10, confirms 5, Explain 11 and Record 1. They retain all successful
statuses and application metadata, not a filtered decision-only trace. Explain 11
includes an extra same-decision unchanged Explain request. Each phase ran
11 managed dbt commands, one State-disabled parse and 11 separate State-disabled
SELECT-only Reference commands. All 11 reference recorders per phase drained
with complete certificates and zero RPCs. The single fresh Rust phase completed
in 170.52 seconds; no physical retry or second pilot graph was used.

## Observed history

Source columns were `id BIGINT, amount INT, tag STRING`. Source keys were unique
and non-NULL. Initial rows were `(1,10,'same'), (2,20,'old'), (3,10,'same'),
(4,NULL,NULL)`: non-key duplicate values and NULLs were intentional.

| Operation | Hosted and Rust behavior | Independent expected target |
| --- | --- | --- |
| First build | MERGE3 EXECUTE + successful Confirm | Exact four initial rows |
| Unchanged | Required SKIP | Same rows/schema/properties |
| Update key1 to `(11,'changed')`, insert key5 `(50,'new')` | MERGE3 EXECUTE + Confirm; then required SKIP | Updated key once, inserted key once, other rows retained |
| Delete source key2 | MERGE3 EXECUTE + Confirm; then required SKIP | Five target rows; deleted source key2 remains `(2,20,'old')` |
| Rendered projection changes to `amount + 100` | MERGE3 EXECUTE + Confirm; then required SKIP | Source-present amounts111/110/NULL/150; deleted key2 still20 |
| CLI `--full-refresh`, config `full_refresh` absent | No Submit; RecordExecutions FULL1, `records_stored=1` | Four rows from current source projection; key2 removed |
| Next ordinary invocation | MERGE3 EXECUTE + Confirm; then required SKIP | Same refreshed four-row result |

The source epoch advanced after actual update/insert and delete. Every ordinary
request carried exactly the source and, when present, target with known epochs.
The post-refresh ordinary request reported the newly recorded refresh target
epoch. This MERGE→FULL→MERGE history required a rebuild before reuse; an older
matching MERGE row could not satisfy the next ordinary invocation. Persistence
was observed through subsequent public calls, not by fabricating store rows.
Explicit process restart persistence is not established by this trace.

First creation kept State execution type MERGE3 even though materialization SQL
created a table. Full refresh actually emitted FULL1 recording after replacement;
this was observed rather than inferred from Snowflake behavior. The projection
changes rendered SQL and raw node body; it is not a template-only or config-only
identity control. Configured full-refresh true/false override behavior is untested.

## Native object footprint

Actual pinned default first-build and full-refresh SQL used fully qualified
`CREATE OR REPLACE TABLE <own consumer> USING DELTA ... AS SELECT ...`.
Executed steady MERGE used the unqualified session-only
`CREATE OR REPLACE TEMPORARY VIEW <own consumer>__dbt_tmp`, followed by
`MERGE ... ON DBT_INTERNAL_SOURCE.id <=> DBT_INTERNAL_DEST.id`, matched update
and unmatched insert. No source-missing delete clause was present. The changed
SQL used only the cleared source and target. Required Delta/managed properties
were checked independently; a shallow-clone subtype was not accepted.

The pinned macros also contain a safer replacement route with qualified
`__dbt_stg` and `__dbt_backup` names. Root reserved one extra concurrent UC slot,
including those exact phase-owned spellings, rather than disabling that route.
The runner's native SQL audit permits only its own consumer lifecycle and
simulates creation/rename/drop peaks. A staging table can coexist with the
consumer; renaming the consumer to backup does not add another object. Session
views have a separate allowlist and cannot be persistent/global temporary objects.

No staging/backup path was reached in either actual phase: every native mutation
window had zero extra persistent objects beyond the target. Final inventory
alone is not the basis of this peak conclusion; complete emitted DDL and its
ordered lifecycle were retained. The default SQL matches the pinned V1-shaped
materialization path. This does not validate V2 safer replacement, Python models,
other storage formats or alternative generated suffixes.

## Independent result checks

Before each operation, the runner saved a literal expected target snapshot from
the declared incremental history. It never derived expected rows from the current
source, target, State fingerprints or decision engine. Default MERGE retains
source-deleted keys; full refresh replaces the target. In particular, applying
the new projection to the absent key2 or substituting a fresh-source SELECT for
the MERGE history fails the oracle.

Each hosted/Rust target was compared independently with its expected complete
row multiset, ordered column types and managed Delta properties. A complete typed
API `VALUES` SELECT and isolated State-disabled dbt show independently rendered
the declared snapshot, with exact compiled/query SQL and a zero-RPC canary. This
Reference executes the independent operation model's state; it is not a second
persistent State-disabled MERGE graph. Hosted and Rust complete typed results
also matched each other at all 11 checkpoints.

Six offline fault probes reject wrong source-deletion/full-refresh history,
rewriting the retained deleted row, losing a duplicate-valued key, corrupting a
NULL, changing schema/subtype/provider, reference/old-object writes and unexpected
native persistent names. A legitimate staged rename/backup lifecycle is accepted.

All ten ordinary Rust decision payloads match primary stable response fields.
Cross-physical-run comparison removes only invocation UUID fields, maps the exact
phase spelling, and separately verifies runtime against the same phase's causal
Confirm/Record. Five Rust reuse runtimes were 5580/8081/8809/8567/8067ms; they are
not asserted equal to hosted warehouse runtimes. No field is removed from either
complete exact-replay capture, including full Explain trees and statuses.

## Validation and remaining scope

The complete hosted history is
[`databricks_merge_history.jsonl`](../../golden/fixtures/databricks_merge_history.jsonl).
Exact replay through the production router passed all 123 hosted corpora, with
64 assertions for this fixture and zero unknown-baseline, gap or partial-history
allowances. The existing production implementation already matched; no production
fix was needed. Source/tool receipts pin integration e5b2c5cc and show identical
production/protocol/dependency/runtime source to frozen af6937b.

Supplementary exact replay of the unmodified 75-RPC Rust trace alongside all
123 hosted corpora passed in 235.75 seconds. The focused harness passed one
offline test (six Python probes), leaving its live test opt-in; the separate
live invocation above passed once. Five existing strict Reference probes,
focused strict Clippy with fuzz enabled, syntax, format and whitespace checks
also passed. Evidence receipts and logs are indexed in the archive. Prior shared 675-workspace/310-library/strict
Clippy/format/25-DuckDB validation applies to the identical frozen production
inputs; an independent byte comparison of all 159 prior production/protocol/
DuckDB gate inputs found no difference. Those are prior receipts, not newly
executed #145 full-suite gates.

Whole #145 still requires predicates, supported schema evolution, config-only
update/exclude-column behavior, actual NULL unique-key semantics, intentionally
ambiguous-key negatives, conditional incremental SQL, generated lifecycle controls,
restart/upgrade evidence and broader client behavior. `<=>` was observed, but
NULL-key matching itself was not exercised. Native #145 remains incomplete and
its dependency graph is unchanged; no unsupported feature is waived by this slice.
