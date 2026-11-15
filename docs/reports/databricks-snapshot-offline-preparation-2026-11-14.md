# Databricks snapshots: offline preparation for #151

This prepares a bounded timestamp/ignore campaign; it establishes no new hosted
conformance or native acceptance. No warehouse call, authentication, dbt command,
DDL, cleanup or schema allocation was performed. #151 and dependencies #138/#140
remain incomplete. The required `new_record` blocker remains: both strategies
produce NULL deleted business columns even with State disabled. The driver
refuses that mode; its output is not accepted semantics or a waiver.

`experiments/databricks_snapshots.py --plan` emits the immutable first-phase
operation ledger and exact proposed names without accessing credentials. A future
root-reviewed grant must match the entire footprint, operations digest, all
Databricks helper digests, allocation receipt, container identity and initial
inventory before initialization. The root later allocated the empty container (allocation only; no test-object grant):
`cdo_demo_db.state_20261114_snapshot151`. UUID `5bfc5332-ae2e-47ee-bb41-bd93fdd667ff`, owner `max.firman@gmail.com`, created_at `1794700751817`; its immutable private receipt is bound in the current proposal. Capacity and all eight absent names must be checked independently before the first phase, and four hosted objects protected before Rust.

The first phase proposes four independent source/target pairs, two per backend:
hosted managed and State-disabled native reference, then Rust managed and its
State-disabled native reference. This retains eight permanent relations after
both windows, with four added per window. Each window runs serially, reserving
one exact session temporary view at a time. Retained snapshot SQL establishes
CTAS for the first execution and temporary-view/MERGE/DROP-view for updates;
the driver grants zero persistent auxiliary names and no cleanup. The two
possible temporary names per backend are alternatives, never concurrent.
No existing survey, incremental or other campaign object is writable.

Each backend freezes 12 managed snapshot invocations, 12 State-disabled native
reference invocations, one disabled parse and 12 Explain requests. It exercises
first build, own-runtime positive reuse, update/insert, deletion under ignore,
reappearance, equal-timestamp business mutation and a source SQL projection
change, each followed by reuse. Sources are independent, with seven frozen
write operations each (create, insert initial rows, update, insert new row,
delete, reinsert, equal-timestamp update). Every result is checked against a
separate complete SCD ledger, including old business values, multiplicity,
validity intervals, updated_at and MD5 IDs. Complete source rows, original
physical schema, lossless timestamp projections, managed Delta properties,
full source/target Delta histories, CLI results, wire and Explain are retained.

The native wrapper is ignored by default. After the child drains the proxy it
retains actual org-scoped State audit clocks before TestServer cleanup, including
an attempted collection before asserting child success. These clocks are only
for Explain replay; they do not measure warehouse-generated SCD timestamps.
Whole hosted replay and an independent native Rust window are still required
before any first-phase acceptance claim. No fixture or replay exemption is added.

## Clock oracle preparation

`databricks_snapshot_oracle.py` also models the fixed `check_cols=['v']` check
strategy and invalidate semantics offline. One symbolic warehouse clock belongs
to each executed statement. The constraint checker nominates candidates from
lossless validity timestamp fields, requires independently acquired before/after
microsecond bounds from that same warehouse, then compares the entire expected
history and every MD5 under one shared assignment. It never recovers time from
a target hash and never treats each row's in-range clock independently.
A successful check is bounded clock consistency, not independent exact-time
verification. Bounds must be forward, nonoverlapping, at most ten minutes wide,
query-distinct and warehouse-identical; candidate combinations above 100,000
stop rather than weaken the comparison.

Native check/invalidate remain disabled. Their proposed projection uses six
fractional digits plus `unix_micros` and requires exact agreement; a millisecond
API display cannot pass. The timestamp-ignore driver must first validate the
codec against an independent submillisecond literal and native UTC timezone
before any object write. This SQL projection is proposed, not primary-validated
by offline tests. Future generated-clock work must independently obtain and
retain same-warehouse before/after observations around each consuming CTAS or
MERGE, including query identities and ordering; a client clock is insufficient.

## Verification and remaining scope

Fourteen warehouse-free behavioral/fault tests cover complete history, equal or
regressed source timestamps, check NULL transitions/noncheck columns, invalidate,
correctly hashed but inconsistent per-row clocks, closing/opening mismatches,
old-business mutation, precision loss, typed schema, duplicates, source rows,
call budgets, exact ownership, native lifetime, actual Confirm shape and CLI
status. Reuse also requires the target epoch from its own accepted Confirm,
the immediately previous source epoch/query/context and completely unchanged
Delta history/version. BUILD does not assume that a zero-row native MERGE commits
a new Delta version. Known MD5 literals were checked independently with `md5sum`, including
retained `1|2026-11-01 12:00:00` → `7917108250601dab0d0861c39f893716`.
The native SQL lifetime guard was also run offline against all four retained
zero-lag timestamp-ignore SQL logs, without changing them.

Config changes, relevant node-kind/strategy transitions, native check/invalidate,
new_record correctness, generated/restart/failure lifecycle, NULL or duplicate
keys, NULL timestamps, schema evolution, full refresh and copy remain required
follow-up scope. Existing conformance, enum coverage and this prepared driver do
not complete those boundaries or the ticket.

Primary evidence: retained pinned dbt 2.0.11 artifacts under
`~/.local/state/dbt-state-rs/databricks-134-20261112/{zero-lag,snapshot-reference}`;
macros, actual source/target SQL, wire/outcomes and precision limitations are
indexed in the coordinator's `ticket151-snapshots-preflight.md`. The original
capability report and known blocker remain authoritative; no stale survey
success label is promoted.

## Current offline refresh (2026-11-15)

The isolated source includes canonical `9e6919c`; historical freezes remain unchanged.
SKIP requires nested `explained_decision.is_stale` exactly false, including when a
misleading top-level field says false. The native wrapper retains its actual
PID, argv, executable SHA256, child PID/argv and extra-replay environment receipt.
The complete source/tool freeze and both JSON-to-Scope round trips are separate
coordinator artifacts. No #151 live call has run. Root must separately grant
each backend after #150 drains and the current source/tool pins are reviewed.

## Review correction 151-R1

The runner now requires `--result-ledger` and the grant's exact
`result_ledger_sha256`. The tracked `databricks_snapshot_results.json` preserves
the previously reviewed literal complete histories rather than regenerating
expected output at run time. Its version, ticket, strategy/delete mode, both
exact phase run IDs, exactly12 cells and complete source/managed/reference rows
are validated before Campaign initialization (credentials, proxy or DDL).
The dynamic oracle must agree with every literal history before initialization.
Each observed managed and independent native reference target is compared to
its frozen literal multiset as well as the existing dynamic oracle. Missing or
changed files, historical business/interval/MD5 faults, missing/extra cells,
wrong run IDs and disagreement with the dynamic oracle stop the run. No live
claim or new primary hosted evidence is introduced by these offline literals.

## Stopped first hosted invocation and bounded continuation preparation

The first hosted attempt on 2026-11-15 used canonical `4dc06d5` with its complete
672-file/tool freeze. The independent before-proxy/DDL Statement API projection
passed UTC six-digit timestamp and `unix_micros` agreement. Disabled parse and
first managed snapshot CLI succeeded; its ordinary type7 Build and Confirm were
accepted. The runner then stopped before Explain, physical comparison or native
reference because its RPC allowlist omitted successful telemetry and the pinned
client's source-only, unknown-epoch speculative submit returning `undecided`.
The complete seven-record attempt is retained unchanged, SHA256
`f2f8331d18a9f30f3da80a10e5488693a6172904dda8b096e597ee5b8dccefd5`.
Exactly three run-owned managed objects remained; no retry or cleanup occurred.
This is a runner defect, not a new hosted mismatch or completed snapshot slice.

The public `snapshot_submit` guard now admits successful canonical telemetry and
at most one preceding, source-only speculative request with NULL epoch and exact
`undecided` response. Its other request fields must match the ordinary submit.
All RPC service identities, status codes and request/response envelopes remain
validated; unknown methods, missing/failed status, speculative reuse/refusal,
duplicate speculative calls and malformed telemetry stop. Ordinary submits
still require current known positive source/target epochs and exact causal
Confirm or own-runtime non-stale Skip evidence. The retained actual seven calls
were the failing characterization before this narrow change and now pass.

The separate hosted-only continuation entrypoint is a concrete offline proposal,
not live authority. Its mandatory grant-bound anchor pins all original artifacts,
CLI success, full capture certificate, actual codec, first request/decision IDs,
source epoch1794710691000, Confirm epoch1794710706000/runtime8782, namespace and
UID, plus the complete original 672-file source archive and frozen executables.
The original first responses are read only for local postprocessing; they are
never sent to State. Fresh-run defaults still reject occupied targets.

Before starting its new proxy or invoking any dbt command, the continuation
requires the exact original three managed objects, same allocation UUID/owner/
creation and absent reference target. Both complete typed source contents and
Delta histories must match their original create/insert query identities; the
managed source epoch must still match its original submit. The complete first
managed SCD rows/schema/properties/history must match the frozen literal ledger,
native CTAS query identity and exact own Confirm target epoch. Any difference
stops before further mutation. It then requests the missing Explain using the
original decision ID, runs the first State-disabled native reference snapshot,
and continues the same shared loop for the remaining eleven managed/reference
pairs. There is no repeated parse, first managed execution, source CREATE or
initial INSERT. Exactly23 CLI calls remain (11managed,12native references), with
5Confirms,6Skips,12Explain requests and one new permanent target; no persistent
auxiliary, cleanup, clone, Record or generated clock mode is allowed.

After a complete physical pass and proxy drain, the original complete seven raw
lines and the separately certified continuation session are concatenated byte
exactly in actual chronology into a derived replay artifact with a composition
receipt. Both original raw sources and certificates remain separate. All130
existing hosted corpora plus this entire combined history must pass strict public
gRPC replay before fixture promotion or a separate Rust grant. A stopped attempt,
its prefix or an incomplete continuation is not promoted. Full#151 and every
required blocker/follow-up remain open.

## Bounded hosted timestamp-ignore acceptance (2026-11-15)

The separately cleared continuation completed23new CLI invocations and all
missing physical checks. Total12managed snapshots,12independent State-disabled
NATIVE references,6Confirms,6fresh own-runtime Skips and12Explains passed. Both
sources, every complete SCD row/null/ordered type/property/history and all known
epoch/runtime floors matched the reviewed contract. The original7 and complete
continuation77 retain separate raw files/certificates and compose byte-exact84
records, SHA256 `06ff6d582bbd0e08f6e2746a7bdac8bb777cc3fee9c07a27b7353b55acf9fc8e`.
Strict public replay of all130prior corpora plus this ENTIRE84 passed253.22s,
1passed/18filtered,72per-fixture assertions including12Validate, plus12separately checked Explains, zero new gaps,
unknown baselines or partial histories. Exact argv, explicit environment/unsets,
source/capture/composition/tool/PID/test-executable/exit/log bindings were recorded
contemporaneously; unrelated inherited environment is labelled unsnapshotted.
The complete hosted84 is now admitted as the131st corpus; the stopped prefix
is not admitted separately. No production change or fresh broad698/310/25gate
claim is made. Current production inputs remain byte-identical to canonical4b0.

Fresh Rust acceptance is still held for a separate fresh four-table graph,
current source/tool freeze and root inventory/capacity/grant. Its standard fresh
Snapshots path must not reuse first IDs or continuation bootstrapping. All earlier
full-ticket blockers and untested required scope remain open.

## Fresh Rust bounded acceptance addendum (2026-11-15)

The separately cleared fresh b151bb graph completed all12managed/native-reference
pairs, all physical/source/history/own-runtime checks and12actual pre-Drop audit
clocks. All131hosted corpora plus ENTIREnative84 and actualclocks passed257.55s,
1passed/18filtered; newhistory has0unknown/gaps/partial. See the
[dated hosted/native report](databricks-snapshot-timestamp-ignore-2026-11-15.md)
and its JSON provenance index. Full151 and all declared required scope remain open.
