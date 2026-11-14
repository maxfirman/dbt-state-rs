# Databricks partition replacement — 2026-11-14 (bounded #147 partial)

The pinned dbt 2.0.11 client reached native null-safe `REPLACE ON` partition
replacement on the available serverless Unity Catalog warehouse. All thirteen
hosted managed cells have complete typed physical results, independent literal
cumulative R and selected F, and thirteen State-disabled SELECT-only references
with individually certified zero State RPCs. The entire chronological 89-RPC
hosted history passes exact public-router replay with all 124 previous corpora:
76 new assertions, zero unknown baselines, replay gaps or partial-history cases.
The independent fresh Rust counterpart passed all thirteen cells in271.31s,
with a complete89-RPC capture, six own-runtime reuse checks, thirteen zero-RPC
references and the same warned-build/reused/refresh client status distinctions.
Supplemental replay passes all19 tests with125 hosted corpora and the entire
original89-RPC native capture, using all12 exact original persisted audit clocks.
No capture entries, fields or live clocks changed; the initial replay red and its
WAL-based attribution remain separate evidence.
This evidence does not complete #147, its dependencies #138/#140, or #132.

## Operation semantics actually exercised

The model always selects partition `DATE '2026-11-01'` or NULL, on its first,
ordinary and full-refresh runs. Its source also contains an excluded B partition.
The target schema is exactly id BIGINT, v STRING, p DATE, ts TIMESTAMP; it is a
MANAGED Delta table partitioned by p. A configured unique_key=id deliberately
does not supply the replacement key. Actual emitted SQL replaces on
`t.p <=> s.p`; it does not emit legacy INSERT OVERWRITE or set an overwrite mode.
No feature, macro, client, timezone or session override was used.

Replacement removes all old rows in every partition represented in F, then
inserts the complete F multiset. Unrepresented partitions survive. Literal
expected rows were declared before execution and were not derived from actual
results or from a SELECT over current source rows. Initial and updated data
contain exact duplicates, NULL payloads, and a NULL partition. The history
removes a row while its A partition remains represented, changes the NULL
partition, removes that entire NULL partition from F (old target NULL rows
survive), empties F completely (the entire old target survives), then restores
A and NULL (both old partitions are replaced). The full refresh records FULL1,
then the next ordinary type4 rebuilds and its exact repeat reuses its own runtime.

| Cell | R rows | F rows | Hosted and Rust State | Client status |
|---|---:|---:|---|---|
| first | 4 | 4 | BUILD | warn |
| unchanged | 4 | 4 | SKIP | reused |
| upsert | 5 | 5 | BUILD | warn |
| upsert_repeat | 5 | 5 | SKIP | reused |
| remove_null | 5 | 4 | BUILD | warn |
| remove_null_repeat | 5 | 4 | SKIP | reused |
| empty | 5 | 0 | BUILD | warn |
| empty_repeat | 5 | 0 | SKIP | reused |
| restore | 2 | 2 | BUILD | warn |
| restore_repeat | 2 | 2 | SKIP | reused |
| full_refresh | 2 | 2 | FULL1 Record acknowledged | success |
| next_ordinary | 2 | 2 | BUILD | warn |
| next_repeat | 2 | 2 | SKIP | reused |

The ordinary warning is the pinned adapter's generic SQL-warehouse DBR17.1+
insert-overwrite warning (JinjaWarn dbt1071). It does not establish an unavailable
native branch: the actual query log, Delta history and physical rows establish
the reached REPLACE ON path. Reuse has no native adapter execution response;
ordinary warned builds and refresh both have adapter SUCCESS and native query IDs.

## Stops, continuation and provenance

A read-only temporal pilot stopped before any object mutation because dbt show
emits whole-second UTC timestamps while SQL API emits `.000Z`. Raw stdout,
typed schema, current_timezone=Etc/UTC, and both zero-RPC certifications are
retained. Root approved only the demonstrated lossless whole-second UTC spelling;
offset/local timestamps or unknown precision are refused. The original guard
failure remains separate from the offline revalidation and native cells.

The first native upsert completed successfully and was confirmed by hosted State,
then the reused footprint auditor falsely parsed TABLE in INSERT INTO TABLE as
a relation. The original failed helper summary, all22RPCs and source-v1 remain
immutable. A narrow optional-TABLE-token fix retains exact target/auxiliary floors;
its offline negative control still refuses the protected earlier APPEND target.
The complete SQL and raw request/Confirm pass offline revalidation.

Root separately permitted fresh read-only ownership validation. Initial API GET
versus CLI-list serialization differed only by empty column_masks objects; that
false guard is retained. The same original CLI serialization then verified all
four earlier APPEND objects unchanged. The new source/target UC IDs match their
original Delta creation IDs, actors and full query-linked histories. Source six
versions match the exact recorded API writes; target history links initial
creation and the successful upsert. Complete upsert R/F/source-B typed rows match
predeclared literals. No successful dbt invocation or source write was repeated.

A separately cleared continuation collected the missing upsert Explain/reference
once and ran the remaining ten managed cells and three source writes. The delayed
upsert Explain is explicitly a delayed first Explain with actual elapsed age;
no clock override or response mask was used. Individually certified raw captures
22+67 are concatenated byte for byte, preserving every status and Explain entry.
The complete trace SHA256 is
`964d4896aae9db8d8353602c6f5ab80c4057864e8b156ada0883a503210a5333`.
The promoted fixture is byte-identical. Public replay completed in237.79s.
The native capture SHA256 is
`3b69f7884da6677f923ee09ff9d6bb2fb67433a2485de6661b945f4f7bbb78fa`.
Native runs use an independent generated b147bb project, fresh source/target,
new isolated Postgres State schema and the production router. No hosted State
history or physical result was imported. Native source/target UC IDs match their
Delta creation IDs; final same-CLI metadata proves all four146 and two hosted147
objects unchanged. Final inventory is eight MANAGED objects, with zero observed
extra persistent staging objects (one reserved).

The bounded scope reuses only the exact owned146 container, preserves all four
completed APPEND objects, and owns fresh host/Rust input+consumer pairs with one
shared concurrent staging slot. It creates no reference relations or new schema.
No cleanup or original a99/e100/inc mutation is part of this campaign. Raw native
SQL lifetime accounting, per-cell inventories, complete source/target Delta
histories, source snapshots and strict typed references are retained privately.
Source-v1/v2, all source hashes, actual PID/argv/executable receipts, immutable
record-proxy/state-probe/acceptance binary hashes, read-only guards, clearances,
whole-history assembly and replay logs are indexed in the evidence package.

## Remaining required coverage

Multiple partition keys, unpartitioned/liquid-cluster settings when reachable,
column ordering/missing/new/type/schema-policy evolution, strategy/node-kind
transitions, failed/retry/restart/generated histories and other reachable paths
remain unverified. A pure composite-key oracle test is not live coverage.
Native #138/#140 and the full #147/#132 dependency graph remain incomplete.
No fixture exception, unavailable branch or generic execution enum closes them.


## Reproduction and validation

`experiments/databricks_insert_overwrite.py` requires an exact root-issued live
clearance, immutable pinned dbt/proxy/probe paths, a fresh artifact directory and
a fixed cleared backend run ID. An ordinary invocation cannot widen the names,
limits or session temporary allowlist. The held native harness is
`databricks_insert_overwrite.rs::real_dbt_insert_overwrite_matches_independent_operation_history`;
it uses the production router with an isolated State store and records its
actual process executable and address before warehouse use. It requires the
established DBT_STATE_DATABRICKS_{DBT,PYTHON,PROXY,PROBE,ARTIFACTS,RUN_ID,LIVE_CLEARANCE}
environment variables. Re-running these retained names is not authorized.

Credential-free checks include thirteen focused Python oracle/guard probes,
all86 Databricks Python tests, a passing Rust wrapper with the live test ignored,
formatting and focused strict Clippy. This slice changes no production Rust,
store migration or warehouse policy; existing whole-workspace/DuckDB production
results are inherited only under an explicit production-source equality proof,
not reported as fresh executions here. The exact hosted fixture increments the
ordinary corpus inventory to125. Whole-feature settings/schema/lifecycle gates
and fresh final release acceptance remain required.

The initial supplemental red is retained as
`ticket147-native-supplemental-replay.log` (238.64s). Its first/reverse unchanged
Explain used the actual handler clock1794690806996ms; the later UUID was minted
at1794690807001ms,5ms later across a whole-second boundary. Replaying a native
capture with the hosted UUID-time assumption produced29s instead of28s.
A fresh investigator decoded all12 original Postgres audit tuples from private
WAL. Independent review verified record CRCs, all38 attributes and four full-page
image duplicates. The hash-bound sidecar keys every org/decision and validates
complete coverage plus captured request intervals. No clock was guessed from
expected text, no warehouse call was repeated, and production code is unchanged.

The corrected full gate passed19/19 in239.88s with
`DBT_STATE_EXTRA_REPLAY_CAPTURE=<original-native89>` and
`DBT_STATE_EXTRA_REPLAY_CLOCKS=<archive>/clock-recovery/native-replay-clocks.json`.
The native trace asserts76 comparisons with zero unknown baselines, gaps or
partial histories. Hosted replay still uses its existing evidenced clock path.
The current ignored147 wrapper now calls `support::native_clock::retain` after
its proxy drains and before State schema cleanup for future runs. This is a
postflight-only addition: the actual271.31s native run used the retained old
04b30f65 executable and617-file source snapshot; it was not rerun with the hook.
Raw WAL and decoders remain private and are not committed.

Current source equality checks compare79 production/protocol/harness source
files byte for byte with aeb2e54. Of159 earlier gate inputs,157 are identical;
the other two are Cargo.lock and the harness manifest, changed only to declare
the clock verifier's sha2 test dependency. Existing full-workspace/DuckDB
receipts are prior runs, not fresh current-tree gate claims. The current fixture,
clock, Python, harness, formatting and strict workspace fuzz Clippy checks are
recorded separately in the evidence receipt.

The private evidence index contains4643 files, SHA256
`36a2133288b16b36179499d925e2545df170ba208e80db01352535a562a09043` (`artifact-manifest.json` in the retained147 archive).
