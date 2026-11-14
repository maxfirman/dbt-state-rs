# Databricks target lifecycle observations — 2026-11-14

This is a bounded independently validated slice of #141. **#141 remains
INCOMPLETE and blocked by #140.** It neither promotes dependent tickets nor
changes the #132 release contract. The #138 UID, #140 omitted-source,
#158 targetless, #159 namespace, clone-rebuild and snapshot blockers remain.
Missing, unknown, equal-change or delayed metadata policy is not inferred.

Pinned dbt 2.0.11 (`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`)
runs against the same leased Free Edition serverless Unity Catalog workspace.
Only fresh source/consumer pairs in `cdo_demo_db.ds_20261112e134ef_ref` are
writable, two names per backend, four names for the whole campaign. The other
leased schema is read-only. No reference object, schema or cleanup is needed.
Every phase inventories both schemas and preserves all starting objects and
the original six protected relations in each schema. Own-name drop/recreation
and table/view transitions are explicitly planned lifecycle operations.

## Causal target history

The hosted table history contains 24 managed invocations: 12 builds and 12
required unchanged reuses. Every accepted checkpoint compares literal expected
full row multisets, complete independent typed transformation SELECT results,
complete target rows/schema, MANAGED/Delta properties, and a separate
State-disabled no-object dbt show reference. Duplicate rows and NULL cells
are intentional. Every show reference has an exclusive drained zero-RPC canary.

The continuous history covers absent target after DROP, external UPDATE,
INSERT and DELETE, CREATE OR REPLACE with distinguishable rows, added-column,
table-property and comment changes, external same-name recreation, then model
A→B→A over the same physical name. Each operation rebuilds, each unchanged
follow-up reuses. Returning to A rebuilds over B; the earlier matching A result
cannot resurrect. Warehouse UC identities, history and full properties are
retained before external changes and after every accepted checkpoint.

The complete hosted table capture contains 161 RPCs, including 25 Explain
requests (the initial unchanged Explain is also requested in reverse order).
SHA-256: `827b2f650a7c4a271a2a749278d2e0011c397d4e972e6a91b19c61918ca7c5fc`.
Initial public production-router replay asserts 136 applicable entries with
zero unknown baselines, gaps or partial histories, and compares complete
responses, statuses, application metadata and Explain. All prior 113 replay
corpora also pass. No decision implementation change is indicated by the table-only history.

## Continuation on the same names

A continuation admits only the exact two names, verifies the successful
prerequisite receipt, unchanged full inventory and final UC identities, and
retains the prerequisite capture. It is a continuation of the same causal
State history, never a fresh-history claim or retry. Final code additionally
revalidates each prerequisite capture's certification/count/digest; this guard
was added after hosted launch and changes no SQL or requests. Hosted launch
source and final current-source Rust provenance are retained separately.

A deterministic seed-141 sequence chooses INSERT→UPDATE→DELETE on the owned
target. Each deliberately wrong target must first fail the independent oracle
with `physical:rows`, then the actual client repairs it and must reuse its
unchanged follow-up. This checks the oracle as well as the State workflow.

Ordinary isolated project YAML emits `ignore_external_modifications=true`;
there is no client or macro override. Under this explicit policy, an external
UPDATE changes both duplicate target amounts to 999, while the independent
fresh model result stays 10. Hosted SKIP retains the declared external 999
snapshot twice. The target and fresh reference are checked against separate
literal expected results; policy-permitted external retention is not compared
to the fresh result as if it should rebuild. Dropping the target under the
same flag still rebuilds, followed by unchanged reuse. This does not relax the
legacy unknown-own-epoch safety floor.

A target externally replaced with a VIEW containing wrong rows reaches ordinary
Submit with its own current epoch present. Hosted rebuilds. The native query
log subsequently records DROP VIEW followed by CREATE OR REPLACE TABLE, and
full target checks establish MANAGED/Delta and correct contents before the
required unchanged reuse. This is a table-model repair over an external view;
the request carries no target relation-type field, so this does not establish a
server-side type-rejection rule or every view-model lifecycle policy.

Dropping the owned source produces the actual client's
`TABLE_OR_VIEW_NOT_FOUND` before ordinary Submit. It establishes reachability,
not a hosted missing-epoch decision. The complete failed invocation and its
RPCs remain in the causal history. Source same-name recreation (amount 17), replacement (23), table→literal-view
transition (31) and literal-view replacement (37) each rebuild and then reuse
unchanged. The literal view appears in `query_dependencies` with its complete
reported definition and default catalog/schema. It is omitted from the physical
`tables` list, which contains only the consumer's own epoch. These cells
establish definition-change handling after actual client preprocessing, not
missing-view-epoch freshness policy for an unresolved physical input.

The continuation has 22 managed invocations: 21 successful ordinary submits
(nine builds, twelve required reuses) and the one expected pre-Submit missing
source failure. Its 142 certified RPCs have SHA-256
`466609a70ff3fe342a4b14353ea91faaf572850a092b827304e6cbc6e9b1fb60`.
Together the stages retain 303 RPCs, 46 managed invocations (45 successful),
21 builds, 24 required reuses and 46 Explain requests. There are 45 additional
State-disabled show references and two version-only setup invocations, so hosted
warehouse dbt invocations total 91. The current runtime is independently read
as `dbsql_version=2026.44`, `dbr_version=null`.

The starting inventory is 100/95, table-stage end 100/97, continuation end
100/97. The final owned source is VIEW and consumer MANAGED. The source was
intentionally absent between its DROP completion at 15:34:57.040801 UTC and
same-name recreation completion at 15:35:05.881201 UTC; it was also absent
between the table→view DROP completion at 15:36:10.690715 UTC and VIEW creation
completion at 15:36:11.697553 UTC. An overlapping read-only campaign observed
this expected transient absence and retained its failed inventory summary;
that campaign's separate repeat after the stable boundary is distinct evidence.
No claim of continuously unchanged own-object inventory is made.

## Evidence and limits

Artifacts live at
`~/.local/state/dbt-state-rs/databricks-141-20261114`. Complete successes,
expected client failure, operation logs, typed SQL envelopes, native query logs,
source/binary provenance, inventory receipts and raw captures are retained.
Focused fixtures contain complete causal histories; no response/tree masks or
Databricks replay exemptions are permitted.

The complete first replay found four Explain-only differences for the literal
source views. Decisions, descriptions and responses match; the local renderer
omitted dependency lines or called a changed view definition a changed outer
model query. A focused public tracer reproduces all four in 1.28 seconds.
Auxiliary outer-query comparison evidence in the existing Databricks decision
JSON separates those displays. It does not change any candidate, producer,
copy, eligibility or fingerprint identity/version, and needs no migration.
Only the exact matched/unmatched execution's org-scoped request correlation
can supply the earlier display inputs. Missing legacy/audit evidence retains
the existing display. The new shape is fenced to FULL, false template mode,
literal query dependencies and no genuine physical upstreams; physical-view
freshness is not extrapolated. Equal body hashes with changed rendered outer
SQL retain the changed-model-query display.

This new production response behavior requires its own final-source 25-case
DuckDB gate. Earlier source-equivalence receipts are retained as pre-display
chronology and are superseded for final acceptance. Current-source Rust physical
acceptance and final offline gates must be read from the companion JSON,
rather than inferred from hosted success.


## Current-source Rust acceptance and final gates

The immutable Rust test/router, proxy and probe use the reviewed final Explain
production source. The complete command exits successfully in **797.90 seconds**.
Both current-source stages check the same operations and independent literal
results as hosted:

| Stage | Managed invocations | Builds | Required reuses | RPCs |
| --- | ---: | ---: | ---: | ---: |
| Hosted table | 24 | 12 | 12 | 161 |
| Hosted continuation | 22 | 9 | 12 | 142 |
| Rust table | 24 | 12 | 12 | 160 |
| Rust continuation | 22 | 9 | 12 | 142 |

Each continuation includes the one expected client failure before ordinary
Submit. There are **45 additional State-disabled show invocations and two
version-only setup calls per backend**: 91 warehouse dbt invocations per
backend, 182 across both, plus four version-only calls. All **90** exclusive
show canaries certify zero RPCs. Rust captures are
`f4833394aca52a204664f266bf3b360f073bcbb1328deefac368ad25840c38f9`
and `69a0af16c0c64cd25be0e13e8818092fb80ca7fda3a50698abe88cdd0cb5cf96`.
The single fewer Rust RPC is optional telemetry; ordinary/speculative submits,
Confirm, Explain and version-call counts agree.

The public hosted replay preserves the original requests, physical epochs and
Confirm runtimes, with zero Databricks gaps, unknown baselines, partial histories
or new field/tree exceptions. A **supplementary comparison of independent live
runs** also passes all 46 managed invocation boundaries and 46 Explain trees.
This supplementary check maps only the two explicitly corresponding physical
names and opaque IDs. Different measured runtimes are allowed only after every
actual SKIP runtime equals its own actual prior Confirm. It uses the existing
replay presentation slots for `last updated: …`, humanized build age and
`updated N … after … last executed` (the display-only **gap** line), plus the
five established hosted-ingress headers. Every original and exact mapped value
is retained in the 724-step normalization ledger. Raw request/Confirm epochs
are retained unchanged; complete emitted SQL, dependency definitions and their
contexts, semantic extras and policy flags are compared under only the declared
physical-name mapping. No eligibility timestamp, decision code, description,
branch, marker or badge is discarded. Four deliberate comparison faults
(description, marker, runtime and dependency definition) are rejected, and the
unmodified-copy control passes. This supplementary real-clock comparison is
separate from exact public hosted replay.

All 435 selected source-file hashes are identical across immutable Rust
acceptance, the original complete public replay, strict Clippy, 310 library
tests with an invalid database URL, the **661-pass / zero-failure / 42-ignored**
workspace gate, and the fresh **25/25 DuckDB** gate. The fresh DuckDB command
finishes in 1049.24 seconds. There is no reuse of the earlier production
source's 25-case result.

Subsequent integration syncs to `facbe84547d09a2f6c0c9127dd8661a72b41519e`
add only helper/report/fixture/tests. Production remains byte-identical to the
immutable live/DuckDB candidate. Separate post-sync checks pass: the two added
#158 targetless publication guards, the #143 cached-result persistence/scope
and runner controls, and the complete **16-test / 115-corpus** conformance gate.
Strict all-target/fuzz Clippy and formatting pass after the sync. The combined
Explain census is 825 trees in 126 files; this fixture contributes 21 FULL
EXECUTE and 25 FULL SKIP trees without new vocabulary.

The additional persisted-JSON controls create genuine current rows through
public Submit/Confirm, then alter only auxiliary audit data through the named
fault seam. A captured true-template body/config pair with changed rendered SQL
requires SKIP; removing only `rendered_query` from its stored baseline and SKIP
JSON, then restarting, preserves the complete hosted template Explain and
required reuse. Five false-mode restart cases cover the older JSON, a malformed
new field, null comparison, deleted audit row and a genuine foreign-org audit
with a deliberately colliding prior request UUID. Every case reuses the valid
execution and retains the legacy Explain fallback. Auxiliary evidence neither
cold-misses valid identity nor crosses organization boundaries. These test-only
additions require no further warehouse or DuckDB run.

## Separate later reference retirement

The successful campaign-end inventories remain **100/99** and are never
rewritten. After the whole #141 campaign and the subsequent #143 read-only
window completed, root separately retired seven completed old #140 reference
consumers at `2026-11-14T16:23:44.981637 UTC`. That later action changed counts
to 100/92, preserving every other object and the twelve protected originals.
It removes no #141 source or consumer. Receipt SHA-256 is
`6af2247f55f4d422b09cfd07e9029ccecb325d025d7054fdf75ab31a54390875`;
the exact receipt is retained under `external-after-campaign-cleanup`.
An 8,356-file scan of this campaign's SQL, logs, compiled artifacts, projects
and captures found no operational reference to those seven names; they occur
only in ten historical inventory files. This is a separate chronological
boundary, not a claim that starting inventories remain current indefinitely.

Full commands, source/binary hashes, per-stage capture certificates, original
and mapped comparison values, split gate attribution and retained artifact
paths are in the [companion JSON](databricks-target-lifecycle-2026-11-14.json).
#141 and the full #132 release remain incomplete for their stated blockers.
