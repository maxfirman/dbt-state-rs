# Databricks copy recovery and conservative boundaries

This dbt 2.0.11 supplement tests serialized FULL SQL-submit copies and MERGE
RegisterClone pre-copies on serverless Unity Catalog managed Delta. Both
historical experimental switches were off by default during this capture;
[current copy policy](databricks-copy-preview.md) now enables both paths. Positive copy chains, snapshots'
failure recovery and concurrent physical source changes are outside this scope.
This is bounded physical acceptance, not full ADR 0009 hosted conformance.

## Measured lifecycle

Each backend uses one fresh five-name graph in existing owned demo schemas:
one input, production FULL/MERGE results and development FULL/MERGE results.
The input has two rows, including a NULL and microsecond timestamps. Expected
complete typed bags and schema are fixed independently of State responses;
each successful result also has an independent State-disabled native literal
reference. All result mutations have an ordered budget, tracked created table
IDs, complete beforeimages and ownership checks. The nine intentional result
DROP operations are test removals; they are not product cleanup or automatic
failure recovery.

| Operation | Automatic FULL | Direct MERGE |
|---|---|---|
| Valid production result to absent development target | Copy, Confirm, then own SKIP | RegisterClone, Confirm, adoption SKIP, then own SKIP |
| Remove original production result | Existing development copy stays readable and SKIPs | Existing development copy stays readable and SKIPs |
| Attempt reverse copy from that copy at limit 1 | BUILD; no chain grant | RegisterClone refuses 10; ordinary BUILD |
| Modify copied target key 1 | Ordinary replacement fails with `CLUSTER_BY_AUTO_UNSUPPORTED_TABLE_TYPE_ERROR`, SQLSTATE 56038; no Confirm or physical/history/ID change | Ordinary MERGE repairs the complete result, then SKIP |
| Retry FULL after intentional fresh development-target removal | Valid source copy and subsequent SKIP | Not needed |
| Modify production result without changing its State history | Copy grant fails the client's required-source-epoch check; successful ordinary fallback confirms `failed_to_clone=true`, then SKIP | Hosted grants, confirms, then ordinary MERGE repairs. Local service refuses the stale source and builds normally |
| Remove production result and development target | Missing-source grant falls back to a successful ordinary build, then SKIP | Native client makes no RegisterClone call; ordinary BUILD, then SKIP |

The source-modified hosted MERGE executes the exact clone DDL, MERGE and
`ALTER TABLE ... CLUSTER BY NONE`. Its fresh target history is CLONE version 0,
MERGE version 1, CLUSTER BY version 2. All 22 default SHOW properties remain
accounted for: 18 equal values, three independently checked distinct physical
UUIDs, and the explicit `clusterByAuto=true` to `false` transition caused by
the ordinary materialization. An unmodified copy retains the existing strict
19-equal-values/three-distinct-identifiers rule.

## Whole hosted boundary

The complete raw [151-RPC capture](../crates/harness/tests/fixtures/databricks_copy_recovery.jsonl)
contains 25 ordinary submits, 25 Explain calls, 13 confirmations and three
RegisterClone calls. Its SHA-256 is
`873260e5e7a9360ef7672450d71589263a539239f3ca71b265cc78d0598407ae`.
It retains the failed FULL attempt and every causal prefix. Three incidental
postprocessing stops were corrected without repeating a successful command or
source/result mutation: exact platform error admission, a NULL legacy refusal
mirror, then the copy-followed-by-MERGE history/property checks.

Strict replay has an honest grant/refusal mismatch. Its first fatal assertion
is RegisterClone entry 127; earlier submit/Explain differences also exist.
The new named characterization executes **all 151 requests**, compares every
response field and pins the following complete local differences separately:

| Entries (zero based) | Retained difference |
|---|---|
| 26, 30 | Reverse FULL BUILD reason/Explain: hosted 1/10, local 4/7 |
| 115, 119 | Mutated MERGE target repair: hosted external-modification description, local copied-identity mismatch description; both BUILD 6/6 |
| 127, 128 | Hosted stale-source grant; local refusal 5. The subsequent raw hosted Confirm receives exact NotFound and publishes no history |
| 129, 133 | Following BUILD/Explain: hosted 8/7, local external-modification 6/6 |
| 141 | Missing development target BUILD reason: hosted 4/7, local 1/6 |

The [local expectation snapshot](../crates/harness/tests/fixtures/databricks_copy_recovery_local_differences.json)
pins entire differing payloads, including timestamps; it omits no fields.
Opaque IDs are correlated as in other public replays. No pending execution or
confirmation is invented for the refused grant. Existing exact corpora and
assertions are unchanged; this boundary is not added to their conformance claim.

## Native and safety gates

The native wrapper starts the production-library server with both switches
explicitly enabled, records the running executable and exact source attribution,
and retains all 25 actual decision audit clocks before isolated State cleanup.
Its whole capture must pass exact public replay with those clocks. Native has
one fewer successful copy/Confirm than hosted because it refuses the modified
direct source. The failed FULL attempt still leaves a pending ordinary execution.

The fresh native campaign passed in 550.74 seconds: 25 managed attempts
(including the expected failed FULL attempt), six compiles and 24 independent
native references.
Its complete 151 RPCs replay exactly in 3.73 seconds with all 25 bound persisted
audit clocks. Native capture SHA-256:
`15be964513316a011883d1beaa061dae3f94751de938e9013698d0490642f602`.
The process ran the recorded dirty source at `7600927`; production, driver and
physical-oracle bytes match frozen source `25f7577`. Later characterization and
documentation changes do not establish a different native executable claim.

Focused public tests also check copied-source refusal at limits 1/2/100,
unknown epochs and missing/empty UID barriers, successful automatic fallback
publication, retry after a missing outcome, immutable duplicate confirmation
and own-result reuse with the fallback's measured runtime. The sole production
change is the measured direct chain-limit refusal diagnostic at limit 1 for a
positive current source epoch and matching nonempty full UID; no grant expands.

An external source write between the client's check and unversioned clone DDL
remains unobservable. Captured Databricks direct grants have no required-source-epoch
value; no timing hook or physical race is claimed here. Normal direct rejection/build is distinct
from a granted direct DDL failure, which this serialized native flow does not
establish. FULL/seed replacement of an existing shallow clone remains a client/
platform limitation. Preserve the conservative source-epoch qualification rather
than copying externally modified contents merely to match hosted permission.

Live receipts: `~/.local/state/dbt-state-rs/databricks-154-20261115/`.
