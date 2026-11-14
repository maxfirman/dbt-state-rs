# Actual Databricks freshness observations — 2026-11-14

**Validated evidence partial; #140 remains INCOMPLETE and does not unblock its
dependents.** No decision, identity, freshness or persistence behavior changes.
The independently unsafe hosted omitted-source SKIP remains a whole-ticket
blocker. Changed contents with equal submitted source epochs, delayed metadata
visibility and mutation inside the consumer warehouse query remain unestablished.
These bounded observations do not establish precision, exhaust reachability,
waive scope or justify a missing-epoch fallback.

## Reference execution without a relation

Pinned dbt 2.0.11 (`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`)
ran `show --inline <literal SELECT> --output json --limit -1 --no-manage-state`
with anonymous usage statistics/version checks disabled. The project, profile,
HOME, logs and artifacts were isolated; the project had no models, hooks,
headers, packages or user macros. The owned recording proxy was live throughout.
A contemporaneous read-only warehouse observation reports `dbsql_version`
`2026.44` and `dbr_version=null`; its full runtime/build metadata envelope is
retained in `warehouse-environment-current.json`. Managed targets remain
explicitly qualified inside `cdo_demo_db`.

Local command help and the pinned manifest's `get_show_sql` and limit macros
established SELECT-only intent before the authorized pilot.

The zero-object pilot returned exactly twelve literal rows, including two
identical rows and NULL cells. Separate stdout is one complete JSON array and
stderr is empty. Compiled SQL and the entire warehouse query log contain the
one exact SELECT, without LIMIT or extra statements. A complete independent
Statement Execution API read retains `JSON_ARRAY`, `truncated=false`, all chunks,
counts and data cells, with ordered column evidence:

| Position | Name | type_text | type_name |
| --- | --- | --- | --- |
| 0 | id | BIGINT | LONG |
| 1 | amount | INT | INT |
| 2 | tag | STRING | STRING |

The strict adapter compares literal expected multisets with both typed direct
rows and show rows. It rejects lost duplicates, changed NULLs, wrong JSON types,
missing or duplicate keys, noise/multiple JSON payloads, schema/order changes,
truncation and extra SQL. It intentionally supports only this evidenced
BIGINT/INT/STRING contract with exact small JSON integers. Show itself supplies
no typed schema. This reference establishes pure FULL SELECT results; it does
not replace independent incremental/snapshot operation semantics or copied
relation property checks.

The gracefully drained canary certified zero started/recorded RPCs of any
method; its empty capture SHA-256 is
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.
Fresh before/after inventories were identical: originalprod `ds_20261112a134ef_ref`
had 100 objects and originalref `ds_20261112e134ef_ref` had 89. No schema or
physical object was created, changed or removed by the pilot.

`experiments/databricks_reference_pilot.py` provides the guarded repeatable
entrypoint. The first exploratory pilot's exact reproduction script is retained
in the archive; the reusable entrypoint uses the same bounded query/result
contract, with the independent SELECT performed before show.

## Actual hosted observations

Both histories use the role-swapped, independently evidenced ownership lease.
Each phase admits only its two exact fresh names, a source and FULL consumer in
originalref; originalprod is inventory-only. No earlier campaign object is
writable. Every stable checkpoint compares literal operation-derived rows,
a full typed source transformation, a full typed consumer, consumer MANAGED/Delta
properties and a State-disabled no-object dbt reference. Each reference boundary
emits zero State RPCs. Each phase preserves its complete starting inventory.

| Complete hosted history | Managed dbt calls | Builds | Required reuses | RPCs | Capture SHA-256 |
| --- | --- | --- | --- | --- | --- |
| Bounded actual epochs | 8 | 5 | 3 | 56 | `a2731baacb8b67163cbb461cc9fb70deb53d14f3b277328235f488ae31256a82` |
| Owned Confirm interval | 3 | 2 | 1 | 21 | `2e6702c040fc7dfb0f29be889ef11d833bb23c35f1790cad2edca401428519ba` |
| Before-SQL and Confirm intervals, same fresh pair | 6 | 4 | 2 | 42 | `29d46ce6837656833ef6ade322f1b34c243ef965e8074b93a8a27d0a0453d1b2` |

The actual-epochs history first builds amount 10 and requires unchanged reuse.
Three bounded attempts update both duplicate source rows to 11, 12 and 13 and
invoke the actual client immediately after each UPDATE completes. Actual source
epochs recorded/submitted in order are `1794663374000`, `1794663412000`,
`1794663431000`, `1794663449000`: none of these changed-data attempts has equal
recorded/current evidence. The fixture preserves the original requests. It does
not manufacture an equal-epoch case by editing metadata.

A final UPDATE to 17 is followed by three bounded observations. Planned offsets
were 0, 1 and 3 seconds; actual observation starts were approximately 0, 19.16 and
34.24 seconds after mutation completion because metadata, dbt and independent
checks take time. All three client submissions report source epoch
`1794663470000`; the first rebuilds and subsequent calls reuse. No delayed
visibility was observed at these sampled instants. This sampling cannot rule
out shorter delays. History/catalog snapshots, UTC SQL boundaries and monotonic
durations remain in the evidence; the schedule is never reported as actual
visibility latency.

## Actual mutation in the Submit–Confirm interval

An owned loopback unary gRPC gate forwards original protobuf bytes to the
ordinary recording proxy. It recognizes ownership only from a real Submit for
the exact fresh consumer and its exact source, then correlates that response's
request ID to Confirm. A one-shot gate holds only that owned Confirm, releases
automatically at its bounded deadline, and preserves status/application metadata.
Transport tests cover ownership, one-shot behavior, byte preservation, metadata,
controller timeout and upstream errors. No request or epoch is edited.

The first actual consumer Confirm arrived at `13:40:54.814784 UTC` and was
forwarded at `13:41:17.091207 UTC`, approximately 22.276 seconds later. The same
raw request SHA-256 is retained at both boundaries:
`56ebc46aece215029314465e0ee3c672abdf3172302836cb555ed157e202adb9`.
The controller released it, not the timeout, and upstream returned OK/success.

At receipt the consumer warehouse SQL had already completed. Full independent
snapshots proved source and target amount 10. While Confirm remained held, one
owned UPDATE completed source amount 17; its full typed transformation and
State-disabled show agreed, while the target retained amount 10 and matched the
saved independent earlier reference. Only then was the original Confirm
forwarded. The next actual invocation rebuilt to 17 and its unchanged follow-up
reused. This proves a real operation between Submit and Confirm and the ensuing
freshness behavior. It does **not** prove a source operation before warehouse
execution or the precise stale-read interleaving. The separate combined history below establishes the before-execution case;
mutation inside SQL remains explicitly unverified.

## Combined before-SQL and Confirm history

After fresh inventory confirmed originalref had 93 objects, a newly owned pair
served both stages in one campaign lifetime. The transport gate first held the
original ready-to-execute response after the backend had received the source's
original epoch and returned its decision, before delivering the response to dbt.
Two fresh inventories proved the consumer absent throughout the controlled
source UPDATE from 10 to 17. Independent full typed/show source snapshots and
metadata were retained on each side. No response byte or epoch was edited.

The unchanged ready response was released at `13:55:01.736298 UTC`. The native
query log records model `CREATE OR REPLACE TABLE` starting at
`13:55:03.183470 UTC`, approximately 1.447 seconds later. Full target rows were
17, matching the post-update independent source/reference result. The next
actual submission's decision was observed, rather than preassigned: BUILD. Its
unchanged follow-up reused. This establishes a real input change after State's
Submit decision and before model SQL execution. It does not establish an
operation inside the warehouse's SQL snapshot/read interval.

Within this same still-owned campaign, the source was reset to 20 to trigger a
second build. The one-shot Confirm gate held its original confirmation after
output SQL completed. Independent rows then proved target/source 20. One UPDATE
completed source 23 while target 20 matched the saved earlier reference; only
then did the original Confirm continue. The next actual decision was observed
BUILD, produced 23, and its unchanged follow-up reused. Both gates were released
by the controller within their deadline. The complete 42-RPC capture retains
four builds, two required reuses and all six Explain trees/statuses.

Response-gate transport tests establish ownership, response byte preservation,
one-shot behavior and unchanged application metadata; the same forwarding
adapter's error/status and timeout tests remain green. Exact hashes of all five
launched combined-history helpers match the retained source provenance. The
subsequent campaign-wide footprint metadata correction changes only the total
authorized bound (8→10 objects), with an explicit addendum retaining the original
configuration; each phase's exact two-name allowlist is unchanged.

## Current-source Rust physical acceptance

Both Rust phases use the production router/isolated Postgres test store linked
from reviewed integration `45e8db8a30b04f237727d4e90f49917a29548506` (validated
#138 safe partial, source `12339079`). This follow-up has zero production source
or dependency delta against that integration. No #138 completion is implied.

| Rust phase | Managed dbt calls | Builds | Required reuses | State-disabled show references | RPCs | Capture SHA-256 |
| --- | --- | --- | --- | --- | --- | --- |
| Actual epochs | 8 | 5 | 3 | 8 | 56 | `b0294bf9863a26985a43e14544873d8f235466221495be394332462413544426` |
| Combined before-SQL / Confirm races | 6 | 4 | 2 | 9 | 42 | `940ea519f11aafc4e91b04680ab26a29d16f237b7b434b11a397593d84d17eef` |

These are **14 managed calls plus 17 reference show invocations**, not 14 total
client invocations. Two additional version-only setup calls are recorded. Both
phases pass full typed duplicate/NULL row multisets, ordered schemas and required
consumer MANAGED/Delta properties. All 17 exclusive reference recorders certify
zero started/recorded RPCs after clean shutdown. Both controlled gates release
unchanged bytes within their deadlines; the actual pre-SQL and post-SQL results
and next BUILD/reuse behavior match the independently checked hosted workloads.
A server that always builds fails the five required unchanged reuses.

Fresh phase inventories are 100/91→100/93, then 100/93→100/95. Every phase's
complete starting inventory is preserved. Both commands exit successfully in
166.61 and 135.27 seconds, with all owned proxies drained and stopped. Exact
current test/proxy/probe binaries and pre-run source hashes are retained.

Across the hosted histories, counts are separately **17 managed dbt calls and
21 State-disabled show references**, plus three version-only setup calls. The
zero-object pilot adds one show and one version-only setup call. The authoritative
`dbt-invocation-ledger.json` records every phase label: the whole follow-up has
70 warehouse dbt invocations (31 managed, 39 show references, including pilot)
and six version-only setup invocations. Inventory/typed SQL API reads, local gate
fault tests and proxy processes are separate from these client counts.

## Replay, Rust acceptance and retained artifacts

The first complete public replay already matches the new 56-RPC history exactly,
including Explain/status/metadata, with zero gaps, unknown baselines or partial
history. There is no new decision red and no production implementation change.
The final integrated replay and offline gates are recorded in the companion
JSON; the current-source physical acceptance above supplements exact replay.

The permanent evidence root is
`~/.local/state/dbt-state-rs/databricks-140-actual-20261114`, initially containing
`pilot`, `hosted-actual`, `hosted-race`, `hosted-combined`, exact source/reproduction files and a
SHA-256/size artifact index. All three initial artifact trees were scanned for
the injected credential bytes with zero matches. Native logs were redacted
before retention. Complete failures and setup diagnostics are retained; no
capture line, gap, baseline, fixture result or physical oracle is suppressed.

The expanded authorized footprint plan budgets ten total fresh physical objects
for hosted actual, hosted Confirm-only, hosted combined race, Rust actual and
Rust combined-race pairs in originalref. The initial 89→99 projection preceded
the separately authorized cleanup below; the remaining Rust +4 now project
91→95.
Every phase rechecks fresh inventory and reserves no capacity by stale count.
These #140 runners never delete or recycle objects. The originalprod 100-object schema remains
read-only. No complete #132 or #140 support claim follows from these partials.


## Separately authorized change between phases

After the hosted histories finished and before Rust physical acceptance, root
executed the user-authorized cleanup of four completed #137 reference outputs,
not #140 inputs or outputs. The exact manifest SHA-256 is
`db95166ab545d61443e092482fb929726a7a208285c02664f0e5e7ba8c1ee720`;
its receipt is retained under `external-between-phase-cleanup`, including the
explicit `execution-addendum.json` distinguishing the earlier read-only audit
limitations from the later four-DROP execution. Final result
records completion at `2026-11-14T14:30:21.447864 UTC`, with only these removals:

- `cdo_demo_db.ds_20261112e134ef_ref.q_20261113a137aa_result`
- `cdo_demo_db.ds_20261112e134ef_ref.q_20261113b137bb_result`
- `cdo_demo_db.ds_20261112e134ef_ref.q_20261114b66ecb_result`
- `cdo_demo_db.ds_20261112e134ef_ref.q_2026111418b892_result`

The receipt records all other 191 objects unchanged and counts 100/95→100/91.
This external action is an explicit chronological break; there is no claim
that every original object remained continuously present across the whole
multi-phase effort. Each phase retains its own complete before/after inventory.
The remaining two Rust pairs must inspect fresh inventory again; their projected
final counts are 100/95. Gross #140 creations remain bounded to ten, with no
#140 recycling, cleanup or writes to earlier campaign objects.

Future Rust references use an exclusive native recorder per reference process,
so concurrent managed telemetry or a held managed RPC cannot be attributed to
that reference. Each recorder is gracefully drained and must certify exactly
zero RPCs, including failures. A warehouse-free real-native self-test with an
empty HOME certifies zero calls and rejects an intentionally injected one-call
fault; the parent proxy/environment remain unchanged. Original hosted reference
boundaries and their complete captures remain unmodified and are not relabeled
as this later instrumentation. This changes observation isolation, not State
inputs or warehouse transformations.


The first final Rust launch failed before Python spawn because an ephemeral uv
interpreter path no longer existed; no campaign artifact directory, warehouse
calls or physical objects were created. The corrected launcher uses an owned
persistent environment. A credential-free capture-driver check initially found
system Python missing PyYAML and passed under that environment. Both setup
failures are retained. No physical campaign was blindly retried or discarded.


## Final integrated offline gates

On the exact current-source tree used by both physical phases, the complete
workspace reports **658 passed, zero failed, 41 opt-in tests ignored**, including
all **113 exact replay corpora**. The three new hosted histories have zero gaps,
unknown baselines, partial histories or response/tree masks. Strict workspace /
all-targets / fuzz Clippy, formatting and all 310 server library tests with an
invalid database URL pass. All eleven Python groups listed in `docs/testing.md`,
the four real-loopback gate tests and the exclusive-canary local fault checks
pass. The census now contains 775 trees in 124 files; these three histories add
11 FULL EXECUTE and seven FULL SKIP trees, with no new vocabulary.

The fresh 25-case DuckDB gate belongs to the validated #138 production source
`12339079`, integrated as `45e8db8`; see its dated dependency/template report.
This partial has **zero production or dependency delta** against that tested
source and does not claim another fresh 25-case run. Pre-run hashes are identical
across both Rust physical phases and every final Cargo gate. Current binary,
source, capture, invocation-ledger and per-phase inventory provenance is retained
with the final SHA-256/size index. #140 remains INCOMPLETE for its stated safety
and coverage blockers; no dependency promotion or broader support claim follows.
