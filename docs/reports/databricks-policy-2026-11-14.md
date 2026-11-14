# Databricks producer and Lag tolerance evidence — 2026-11-14

**Validated independent ordinary-FULL/known-epoch partial; native #142 remains
INCOMPLETE and blocked by #138 and #140.** This does not promote dependencies,
waive scope or claim complete Databricks support. Eligibility, producer identity,
freshness anchors and persisted epochs are unchanged. The production correction
is confined to Databricks Explain duration text; the separately owned #141
decision-audit publication repair is attributed in its
[own report](databricks-anchor-capture-2026-11-14.md).

The pinned client is dbt 2.0.11, SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`, on the
existing Free Edition serverless Unity Catalog/managed Delta environment.
Ordinary FULL models use template comparison false and pre_clone never.
Missing/omitted/ambiguous/equal-change/delayed metadata, data-test UID isolation
and exact hosted Lag-tolerance equality remain outside this bounded slice.

## Actual configuration and complete history

The initial structured `lag_tolerance.updates_on: all` candidate parsed into the
manifest but emitted wire ANY0 at 180 seconds. Its five-command, 30-RPC attempt
stopped at the wire gate and remains intact. It is failed reachability evidence,
not an ALL observation or production mismatch. It includes both initial producer
BUILD/Confirm histories, consumer BUILD/Confirm and positive unchanged reuse.
The failed candidate's Explain was fetched during continuation.

The [official configuration reference](https://docs.getdbt.com/reference/resource-configs/dbt-state-configs?version=2)
and [require_fresh_data_from documentation](https://docs.getdbt.com/reference/resource-configs/require-fresh-data-from)
identify the separate setting. Corrected ordinary properties YAML uses:

```yaml
config:
  state:
    lag_tolerance: 3m
    require_fresh_data_from: all
    compare_unrendered_code: false
    pre_clone: never
```

The pinned manifest and actual Submit prove ALL1, lag180, FULL1 and template
comparison false. ANY emits policy0. No client or macro was modified. The same
four-name graph continued after a fresh unchanged inventory; explicit consumer
write-only rebuilds published each new reference/epoch anchor. Configuration
switches are recorded separately from elapsed-age checks.

The unfiltered hosted lifetime contains **261 RPCs**: 29 ordinary submits,
11 successful confirmations, 19 accepted one-record publications, 29 speculative
UNDECIDED responses, 28 Explain calls containing 29 trees, 47 validations and
98 telemetry calls. Every ordinary decision has its complete Explain tree.
All statuses are OK0. Every causal prerequisite is retained; this slice adds no
unknown baseline, gap, field mask or fixture exclusion.

## Data policy and genuine producer changes

One raw source with labeled partitions feeds two recorded producers. The FULL
consumer combines explicit BIGINT/INT/STRING projections with UNION ALL,
preserving duplicate rows, NULL amounts and distinguishable branch labels.
Only the selected producer is rebuilt after each partition mutation.

These hosted ages use the service-issued UUIDv7 replay-clock witness, checked
within the captured submit/response interval, against the recorded warehouse
completion epoch. They establish large margins, not exact raw service-time
or Lag-tolerance equality. Inside/outside data-policy requests have identical
protobuf bodies and unchanged 180-second configuration.

| Hosted cell | Age ms | Decision | is_stale | Description class |
| --- | ---: | --- | --- | --- |
| ANY partial inside | 27527 | SKIP | true | within lag tolerance |
| ANY partial outside | 198290 | BUILD | false | outdated/query mismatch |
| ANY both inside | 37784 | SKIP | true | within lag tolerance |
| ANY both outside | 198150 | BUILD | false | outdated/query mismatch |
| ALL partial inside | 27085 | SKIP | true | within lag tolerance |
| ALL partial outside | 198852 | SKIP | true | at least one unchanged input |
| ALL B after earlier partial SKIPs | 219681 | BUILD | false | outdated/query mismatch |
| ALL both inside | 37660 | SKIP | true | within lag tolerance |
| ALL both outside | 198026 | BUILD | false | outdated/query mismatch |
| Changed producer ANY inside/outside | 24181 / 217448 | BUILD / BUILD | false / false | outdated/query mismatch |
| Changed producer ALL inside/outside | 24192 / 218271 | BUILD / BUILD | false / false | outdated/query mismatch |

Unchanged outside controls reuse at 198398 ms (ANY) and 198194 ms (ALL), with
is_stale=false and the up-to-date description. SKIP is decision 0; BUILD is 1.
Each policy/producer rebuild above reports rejection codes 6/6 and the captured
full description `model was executed because either its query didn't match or its upstream data is out of date`.
The fixture/indexed contract preserve every full response, runtime and tree;
table phrases abbreviate descriptions.

Same-logic producer Records preserve rendered SQL, node-state identities,
dependencies, type, configuration and context. Changed-logic Records really
change rendered arithmetic (+0→+1 etc.) and independent output values. In all four
actual changed-producer cells, the consumer request body changes only table
epochs: its SQL, query_dependencies and node-state identity are byte-identical
to the preceding unchanged Submit. These isolate producer logic changes rather
than mixed consumer dependency-identity changes. Policy switches change the
emitted policy/node-state configuration fields and are labeled accordingly.

ALL partial SKIPs retain their original BUILD input anchors. Publishing B after
the earlier A-only SKIPs causes BUILD against those original anchors; SKIPs did
not advance the read ledger. Crafted public-router regressions additionally
pin local T−1/T/T+1, zero tolerance and producer precedence, plus eight bounded
Record histories with policy switches. Exact T equality there is local-only.

## Immutable references and physical Rust acceptance

Every consumer BUILD saves an immutable operation-derived reference R with
known input/output epochs. An independent SELECT derives both producer
expressions directly from raw input and computes current F. Each checkpoint
checks the complete typed direct result and an isolated State-disabled
`dbt show --inline ... --limit -1`; an exclusive gracefully drained native
recording canary certifies zero RPCs. Consumer rows never manufacture their own
expected result.

SKIP compares against saved R, while intended drift cells independently prove
R differs from current F. BUILD requires F. Full rows, duplicate multiplicity,
NULLs, branch labels, ordered schema and MANAGED/Delta properties are checked.
The unchanged target epoch is required on SKIP. Six artifact fault probes reject
fresh-for-old SKIP, old-for-fresh BUILD, duplicate/NULL/branch loss, wrong schema
or properties and mutable snapshots; existing no-object reference probes pass.

The **one fresh Rust phase passed** all 26 consumer checkpoints and 47 driver
dbt commands (46 State-managed plus one State-disabled parse), with another
26 zero-object reference executions. The copied test exits 0 after 1499.47s.
Its gracefully certified **254 RPCs** contain 28 ordinary submits, 11 confirms,
19 Records, 28 speculative UNDECIDED, 27 Explain calls, 46 validations and
95 telemetry calls. All 26 physical checks and 26 zero-RPC canaries pass.
The hosted 261/Rust 254 difference is retained explicitly: Rust uses the corrected
ALL setting directly, so it does not repeat the hosted legacy ignored-config
attempt or its later Explain; optional telemetry batching contributes the
remaining difference. Hosted validation/speculative/ordinary/Explain counts are
47/29/29/28 versus 46/28/28/27 locally, while telemetry is 98 versus 95. Confirm
and Record counts are identical. Neither capture is filtered to manufacture
equal invocation totals.
Complete stable response payloads match the named primary cells; opaque IDs vary
per invocation, and all 17 runtime echoes are verified against this run's own
causal Confirm/Record outcomes rather than asserting cross-run runtime equality.
Every declared known-input drift pattern and inside/outside margin is audited.
All four changed-producer consumer deltas remain only tables. Final hosted/Rust
full typed multisets and schemas agree.

Root separately retired seven completed old State-disabled #140 reference
outputs before this campaign; this implementer performed no cleanup. Fresh
inventories were original-a 100/e92 before hosted, 100/96 before Rust and 100/100
after Rust. The exact eight new names are the four suffixes
`inputs,producer_a,producer_b,consumer` under
`cdo_demo_db.ds_20261112e134ef_ref.p_20261114{a142aa,b142bb}_`.
All 192 pre-host objects and all 196 pre-Rust objects remain preserved, including
both containers' twelve protected initial incremental relations. Original-a and
the hosted graph were read-only during Rust. There was no new schema, auxiliary
persistent object, reference relation, cleanup or physical retry.

## Captured Explain correction

Initial exact replay found eight duration-only tree mismatches: gaps of
212–236 seconds rendered four minutes on hosted and three locally. Decisions,
descriptions, statuses and history agreed. Repeated unchanged requests retained
the same gap, ruling out a different clock/anchor explanation. A focused public
regression replays four exact captured Records plus Submit/Explain and compares
the complete message; it went red in 2.39s before correction.

Complete crafted known-epoch Record→Submit→Explain controls use only admitted
names in fresh synthetic named namespaces. They make no warehouse invocation,
object or physical-result claim. Their original full trees remain unfiltered:

- 34 gap cells (102 RPCs): 59999 ms→59 seconds; 60000/60001→a minute;
  89999→a minute; 90000→2 minutes; 150000/150001/150999→2 minutes;
  151000→3 minutes; 210000→4 minutes; 270000→4 minutes;
  58m30s→58 minutes; 59m29s→59 minutes; 59m30s/59m31s/59m59s→an hour;
  60m→an hour.
- 12 relative-hour cells (36 RPCs): bounded whole-second buckets distinguish
  90-minute, 2-hour and 2.5-hour cases and the hour→day carry. For example,
  age [9000005,9000203] ms→2 hours, while [9001005,9001212] ms→3 hours;
  23h59m59s→a day and >=24h→a day.
- Four adjacent relative-minute cells (12 RPCs): age [150003,150218] ms→2 minutes
  and [270004,270196] ms→4 minutes; 90 seconds→2 minutes and 151→3 minutes.

Intervals establish whole-second buckets; they do not assert equality at an
independently observed raw service instant. The observed rule is whole-second
truncation followed by nearest-unit rounding with even ties. The scoped
`explain::gap_line` minute interval and `anchored_rel_time_line` minute/hour
intervals now apply it only to anchored Databricks builds. Shared humanization,
subminute text, gap>=hour, relative ages>=day and other dialect behavior remain
unchanged. Other dialects retain the previous relative-minute rounding.
The boundary regression went red in 0.94s; the adjacent-minute red took 6.30s;
all six public/oracle/generated tests then passed in 10.93s.

## Frozen source, gates and provenance

The live source is frozen `af6937bee4e1d6f7c5c5422e5fe967100d04fb12`, with
docs-only receipt commit `e505e8a768278fbd214c48e82b0e49f5587c2e0c`.
Explain SHA-256 is
`90c43e251dc0aa164f747507753355b1353eb5579c7aaf432defea71b7aa2f35`.
All production/helpers/tests/fixtures match the shared 457-file freeze; the local
golden README difference is documentation only. Test/proxy/probe and source
receipts were pinned outside Cargo before live use, with unchanged pre/post
hashes. A retained argv-only launcher substitutes byte-identical pinned helper
copies for the frozen test's embedded Cargo paths. Official dbt, models and RPC
inputs are unchanged; no Cargo mutation occurs while the live child runs.

The [shared final-source gate receipt](databricks-anchor-capture-2026-11-14.md)
records 675 workspace tests, 0 failures/45 ignored; 121-corpus public 17;
310 invalid-database library tests; strict Clippy/format; and the one fresh locked
DuckDB 25/25 gate. Its source hashes match this live candidate. This report reuses
those exact shared receipts; it does not claim another 25-case execution.
All 15 credential-free Python experiment groups pass locally. The updated census
has 920 trees in 132 files, 59 INFO lines and no empty/unmapped/unspecified tree.
Supplementary exact replay also passes the unmodified 254-RPC local Rust trace
plus the standard 121 corpora (228.47 seconds), with no new gaps or masks.
This is local Rust consistency evidence, separate from hosted conformance.

Permanent evidence is `~/.local/state/dbt-state-rs/databricks-142-20261114/`:
original/continuation captures, every command/operation, full typed/reference
artifacts, immutable R snapshots, manifest/wire config proof, producer/consumer
identity audits, all failure logs, zero-RPC certifications and pinned binaries.
`fixture-parts.json`, `gap-fixture-parts.json`, the indexed tracked
`experiments/databricks_policy_contract.json` and final Rust offline audits
record causal attribution. The original first failed reachability attempt and
every formatter red remain preserved. No post-completion cloud call was made.

The machine-readable [receipt](databricks-policy-2026-11-14.json) records exact
fixture hashes, source attribution, physical proofs and reused gate receipts.
