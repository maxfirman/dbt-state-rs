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
