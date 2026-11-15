# Databricks direct State pre-copy

The experimental opt-in supports the tested quiescent dbt 2.0.11 MERGE and
check-snapshot development pre-copy flow on serverless Unity Catalog managed
Delta. Defaults remain [copy-disabled](databricks-copy-preview.md). Automatic
submit copying has its [separate switch](databricks-submit-copy.md).
The [copy recovery supplement](databricks-copy-recovery.md) tests source removal,
modification, chain refusals and later history; positive chains remain unsupported.
This is bounded physical acceptance, not the full ADR 0009 conformance claim.

## Tested flow

Ordinary `dbt run` and `dbt snapshot` use a development profile with
`defer_to_target: prod`. This is State `RegisterClone`, not `dbt clone` or a
manually substituted transport. Both source and development runs retain the
same full UID and exact physical inputs. Direct adoption now consumes the same
[checked structural query proof](architecture.md#shared-analyzed-workload-contract-181186)
as automatic FULL copying: projections, null filters, nonrecursive CTEs,
ordered joins, checked aggregates and complete query-dependency bindings.
Both paths retain distinct versioned origin identities and entry protocols.
Templates, uncertain effects and incomplete input/producers remain ineligible.
The broader forms currently have offline public-protocol coverage; focused
independent physical acceptance remains #186, not a new native claim here.

The snapshot uses `strategy='check'`, `check_cols=['v']`, `updated_at='ts'`,
`hard_deletes='ignore'` and a unique non-NULL scalar `id`. MERGE uses the default
lookback and schema policy with `unique_key='id'`. Both use zero Lag tolerance in the measured native flow. Exact node-kind,
semantic extras and body/config/macro/contract signatures bind adoption; opaque
config hashes establish equality, not the meaning of hidden snapshot options.
No new incremental strategy or snapshot mode is established by this change.

Direct-v2 registration freezes the persisted shared-v2 proof, source ID/key/epoch,
genuine input epochs/producers and inherited runtime. Confirm publishes it only
while that exact physical source remains current with no pending source write;
a valid completion epoch cannot precede its source. Reuse retains the original
source build-age anchor for Lag tolerance. DDL permission remains weaker: a
qualified legacy source can grant DDL without granting subsequent adoption.
Legacy NULL/v1/unknown proof and pending-v1 copies confirmed after upgrade cold-miss
v2 adoption; ordinary legacy strict reuse and v1 confirmation meaning survive.
Failed cloning discards the origin, and duplicate confirmation is immutable.

| Cell, for each node kind | Native result |
|---|---|
| First production source build | BUILD and successful confirmation |
| Absent development target | Granted copy, executed DDL, successful confirmation, same-invocation SKIP |
| Unchanged development repeat | SKIP |
| Update input key 1 and insert key 3 | Development BUILD and successful confirmation |
| Repeat after that build | SKIP of its own newly confirmed result |
| Set `pre_clone='always'` on source and development; refresh production | BUILD, with no production self-copy |
| Existing development target | Granted overwrite copy, executed DDL, successful confirmation, same-invocation SKIP |

All four captured RegisterClone requests omit source type and table properties.
Their two response mirrors and exact whitespace-preserving
`CREATE OR REPLACE TABLE ... SHALLOW CLONE ...` statements are compared in full.
The client executes the granted statements; physical checks independently
establish Delta, rather than inferring a provider from NULL protocol fields.

## Physical and protocol evidence

The independent oracle freezes complete expected source/MERGE bags and snapshot
business rows, SCD IDs, updated/from/to times and schema before execution.
Source timestamps retain six-digit microseconds; timestamp formatting and
integer microsecond projections must agree. Each cell also executes its literal
expected bag through a typed SQL read and an isolated, State-disabled native
`dbt show` with no reference objects or State RPCs.

Every target is checked through complete typed rows, `DESCRIBE TABLE EXTENDED`,
Unity Catalog metadata and Delta history. Sources are `MANAGED` Delta; copied
targets are `MANAGED_SHALLOW_CLONE` Delta in Unity Catalog, while DESCRIBE reports
`MANAGED`. Every source default property key must survive copying. The three
observed table/row-tracking identifiers differ from the source, must have their
exact UUID formats and must agree with Unity Catalog. This permits an existing
destination to retain its own identity; it does not require another allocation.

The hosted whole-session fixture is
[`databricks_precopy.jsonl`](../golden/fixtures/databricks_precopy.jsonl): 90 RPCs,
14 ordinary submits, four RegisterClone calls, ten successful confirmations and
14 complete Explain responses. SHA-256:
`ced1025c1d7e56f903f32c8dcc6b28651f540af599400124f3e8785c58d26452`.
It is a complete causal public-gRPC replay with no hydration, gaps, partial
history or timestamp masks. The older 16-call causal fixture stays unchanged.

The fresh Rust campaign runs the production-library TestServer with explicit
direct-copy opt-in and a separate five-object graph. Its actual decision audit
clocks are retained before the isolated Postgres schema is removed. The opt-in
harness and warehouse-free evidence fault probes live in
[`databricks_precopy.rs`](../crates/harness/tests/databricks_precopy.rs) and
[`databricks_precopy.py`](../experiments/databricks_precopy.py).

The source refresh also captures refused ordinary-submit optimizations:
MERGE reports 1/10 and a chain-limit description; SNAPSHOT reports 1/3,
`EXECUTION_TYPE_NOT_FULL`, with its ordinary tree. The narrow diagnostic lookup
selects the latest scoped other name and proves the same rendered direct input,
full UID and current source epoch. It returns only a frozen name for Explain.
The original own-build anchor still supplies query and freshness analysis;
the diagnostic grants no automatic copy and changes no Reuse qualification.

## Limits and retained safety floors

An unregistered physical source change during unversioned SHALLOW CLONE is
unobservable: confirmation and adoption contain no new source observation or
version. This evidence assumes quiescent copy sources. State-recorded pending
or newer source writes remain barriers; this does not solve physical concurrency.

Adoption binds provenance version 2, exact full UID, client body/config/
macro/contract evidence, actual source context, latest scoped source ID/epoch,
and frozen genuine input epochs/producers. A rebuilt source's observed own
target is excluded before validating and freezing genuine upstreams; unknown
or Unrecorded genuine input evidence still refuses adoption. New copy semantics
use versioned origin/core proofs without historical backfill; ordinary strict
identities are unchanged.

Copied FULL/seed replacement still has the independently observed Unity Catalog
replacement failure even with State disabled. Snapshot `new_record` retains its
independent NULL business-column client defect. Other formats, retention
properties, general SQL, other incremental strategies/snapshot modes, chains
and concurrent source modification are not accepted by this evidence. Default
refusal followed by a native cross-target build remains distinct from this
explicit opt-in campaign. Keep `pre_clone: never` for the supported preview.

Live artifacts are retained under
`~/.local/state/dbt-state-rs/databricks-152-20261115/`. The hosted 14- and 30-RPC
prefixes and final 46-RPC continuation belong to the same graph and hosted
State history. Two harness postprocessing corrections preserved all successful
commands and source writes: generated physical identifiers, then the native
MERGE's exact `ALTER TABLE ... CLUSTER BY NONE`. No successful command was
repeated, no graph reset and no new UUID substituted. Both backends use only
their five fresh owned objects inside existing demo schemas; no cleanup or
new schemas. Environment: pinned dbt 2.0.11, DBSQL 2026.44, UTC, 2026-11-15.
