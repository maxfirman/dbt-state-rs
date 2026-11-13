# Databricks upstream freshness — 2026-11-12

**Partial implementation; #140 remains incomplete and does not unblock its
dependents.** Actual-client source-operation and same-logic recorded-rebuild
cells have exact public replay and independent result checks. A crafted
omitted-source request produces a physically wrong hosted SKIP. Conservative
Reuse safety is preserved; there are no Databricks replay gaps, exclusions or
temporal masks. The broader #132 copied-target rebuild and snapshot deletion
blockers remain unchanged.

Scope: official dbt **2.0.11**, SHA-256
`f47bcb39cbe325beff9e1e19324266bc5cbb674ab6f879bccae2f2f43db68e31`,
Free Edition serverless DBSQL **2026.44**, Unity Catalog, managed Delta,
warehouse `47fd3fa312624d91`. Every cloud write targets a new `f_<run>_…`
relation in the two explicitly leased prior run-owned `cdo_demo_db` reference
schemas. Before/after inventories preserve every preexisting object. There
are no schema CREATE/DROP or writes outside the demo catalog.

Durable evidence root (**A**):
`~/.local/state/dbt-state-rs/databricks-140-20261112`.
The accompanying [JSON index](databricks-freshness-2026-11-12.json) pins capture
and artifact digests, archive paths, commands, classifications and validation.
The [runner documentation](../databricks-freshness.md) gives repeatable entry
points. Successes and failed hypotheses are retained.

## Actual pinned-client operations

`A/hosted` contains **118 certified RPCs / 36 dbt invocations**: 18 ordinary
submissions, eight successful confirmations, 19 Explain calls, 18 speculative
calls and all validation/telemetry. The stable consumer selects explicit
BIGINT/INT/STRING columns from a managed Delta source. SQL/configuration stay
fixed, Lag tolerance is zero and policy is ANY. There are **eight required
builds and ten required SKIPs**. Always building cannot satisfy this campaign.

| Source operation | Delta version | Actual wire source epoch (ms) | Hosted consumer / unchanged follow-up |
| --- | --- | --- | --- |
| Initial populated source | 1 | 1794492697000 | BUILD / SKIP |
| Explicit unchanged control | 1 | 1794492697000 | SKIP / SKIP |
| INSERT | 2 | 1794492781000 | BUILD / SKIP |
| UPDATE | 3 | 1794492822000 | BUILD / SKIP |
| DELETE | 4 | 1794492862000 | BUILD / SKIP |
| No-op UPDATE amount=amount | 5 | 1794492901000 | BUILD / SKIP |
| ADD COLUMN extra STRING | 6 | 1794492940000 | BUILD / SKIP |
| SET TBLPROPERTIES freshness.note | 7 | 1794492980000 | BUILD / SKIP |
| COMMENT ON TABLE | 8 | 1794493020000 | BUILD / SKIP |

Each step checks complete typed row multisets, duplicate multiplicity, NULLs,
ordered schema and MANAGED/Delta properties. A separate State-disabled dbt
project/profile target, independent SELECT and literal declared rows establish
the reference. References emit zero State RPCs. Changed values, lost duplicates,
changed NULLs and schema errors are rejected by credential-free fault probes.
Both current and reused results are checked, including operations whose logical
output remains unchanged despite advancing freshness evidence.

Catalog and Delta history are sampled before/immediately after each operation,
after the build and after unchanged reuse. Every wire source epoch equals the
latest sampled Delta history timestamp, at **second precision**; catalog
`last_altered` retains different subsecond timestamps and is not substituted
for the wire evidence. Immediate samples already show each new history version;
later samples and the repeated client's source epoch remain stable. This
bounded observation measures these operations, not a general convergence
latency guarantee. The initial hosted runner retained SQL start observations;
the current runner and Rust phase additionally retain explicit SQL completion
times. `A/metadata-convergence.json` keeps all sample times and values.

A diagnostic's two close-spaced updates had history timestamps
14:21:31.000 and 14:21:38.000 UTC, with complete source snapshots and SQL timing.
No equal-epoch changed result or delayed metadata visibility was observed.
There was no client submit between those two writes, so those history samples
alone cannot establish equal-change wire reachability. Actual unchanged
client epochs do establish reachable equal-evidence reuse. Equal-second changes,
real delayed visibility and an actual mutation between client submit and
confirmation remain unverified; crafted cases below are labeled separately.

## Same-logic recorded upstream rebuild

`A/recorded-hosted` contains **36 certified RPCs / ten dbt invocations**,
including one actual `RecordExecutions`. Initial producer/consumer builds,
unchanged reuse, source UPDATE, producer `--state-mode write-only`, consumer
BUILD, and unchanged producer/consumer SKIPs are all captured. The producer's
recorded SQL, FULL execution type and semantic identity match its earlier
submit; only its data and physical epoch change. Full typed results and
required properties match separate reference producer/consumer executions and
an independent SELECT. Lag tolerance stays zero; policy/changed-logic expansion
belongs to #142.

`A/recorded-full-refresh-hypothesis` retains the unsuccessful recording
hypothesis: table `--full-refresh` emitted ordinary submit/confirm and **no
RecordExecutions**. It is not counted as that acceptance cell.

## Crafted boundaries and unresolved Safety conflict

`A/boundaries` is a **38-RPC crafted public-gRPC campaign**, not actual pinned
client input. Every cell has unique SQL/namespace and its own complete prior
submit/confirmation. Equal and older known epochs SKIP; +1ms rebuilds. The
consumer confirmation is 10 seconds later than its recorded source epoch, so
the +1ms rebuild verifies comparison against the recorded source, even when
the source is older than the consumer confirmation. Delayed-old evidence SKIPs,
then later-new evidence rebuilds. These four independent histories form the
**18-RPC explicitly crafted accepted fixture**. Their exact responses/Explain
match the shared conservative engine.

The remaining cells stay diagnostic and keep #140 open:

| Crafted condition | Hosted observation | Rust conservative behavior / remaining issue |
| --- | --- | --- |
| Known recorded epoch, missing current epoch | BUILD | BUILD; Explain differs |
| Both epochs missing | BUILD | BUILD; Explain differs |
| Missing recorded epoch, known current epoch | SKIP | BUILD; unknown evidence cannot prove freshness |
| Source entry omitted while SQL still reads source | SKIP | BUILD; independently demonstrated wrong hosted reuse |
| Two identical known current source entries | SKIP | BUILD; conservative duplicate handling differs |

Identical known duplicate evidence is a separate safe-normalization opportunity;
it is not the same as conflicting or missing epochs. No ambiguous duplicate
rule, Snowflake audit-relation rule or conservative missing-evidence floor is
changed here. The initial boundary campaign (`A/boundaries-initial`) also
exposed cross-cell copy-candidate descriptions because it shared SQL/namespace;
the final isolated campaign supersedes that confound.

`A/unsafe-diagnostic` contains **27 certified RPCs / seven dbt invocations**.
An actual client builds two amount-10 rows. An external source UPDATE makes
both rows amount 17. A deliberately crafted request preserves that actual
build's SQL/identity/own-target epoch but omits the source entry. Hosted SKIPs.
Independent complete reads show target amount 10 versus source/reference 17;
the checker rejects exactly **`physical:rows`**. The actual client then supplies
complete current evidence, BUILDs and repairs the result. This proves the
crafted request's Safety conflict on real warehouse data; it **does not prove
that the pinned client emits that request**.

Dropping that run's source subsequently makes the actual client fail with
`TABLE_OR_VIEW_NOT_FOUND` before any ordinary SubmitEnrichedSQL (zero ordinary
submits). That failure is retained and does not establish client-emitted null
epochs. No warehouse permissions are changed to manufacture missing metadata.

The final complete boundary replay is retained red, including full status,
description and Explain comparisons, in
`A/databricks-freshness-140-boundaries-final-red.log`. For this diagnostic only,
the external capture temporarily occupied the accepted client fixture's runtime
path in a copied test executable; the accepted fixture was restored byte for
byte (`312d1aaf…666a`) afterward. No diagnostic is added as a golden exclusion,
known gap, accepted mismatch or passing conformance fixture.

## Exact Explain timestamps

Initial red replay showed raw numeric epoch placeholders and non-humanized gap
durations. Hosted freezes relative ages at the original submit decision,
including when Explain follows a completed build. The shared pure generator
now recovers that instant from the matched build clock plus persisted build
age, renders absolute UTC/relative age text and humanizes the exact upstream
epoch gap. No decision policy, fingerprints, storage schema or Warehouse rule
changes.

Proxy response `recorded_at` can cross a second after the hosted decision;
client `x-submitted-at` can precede it. Using either alone created different
one-second mismatches. Hosted UUIDv7 decision IDs expose their creation instant:
all **25** matched decisions in the actual/recorded campaigns reproduce their
exact relative text with that instant. `A/uuidv7-clock-verification.json`
retains the 25 observations. The test-only new-Databricks replay clock validates
v7, RFC4122 variant and captured request→response bounds, and fails unsupported
or timing-invalid IDs. Negative tests cover invalid UUIDs, versions, variants
and instants outside the interval. Production keeps IDs opaque; other corpus
clocks are unchanged. Full Explain text and timestamps remain exact, with no
time comparator or mask. Unanchored copy rendering remains separate unfinished
coverage.

## Validation

The standalone Explain/clock commit is `70347a7`. At that point all **11
conformance tests / 73 corpora** passed; the two actual-client fixtures asserted
99 and 31 exchanges respectively with **zero unknown baselines, zero gaps and
zero partial histories**. The final exact replay passed **11 tests / 74 corpora**:
the actual-client fixtures assert 99/31 exchanges and the crafted known-epoch fixture asserts 13,
all with zero unknown baselines, gaps or partial histories. Three focused
freshness tests, 309 server-library tests, strict targeted Clippy, formatting
and diff checks passed. The accompanying JSON report pins their logs. A
redundant full workspace run was deferred to the coordinator's final combined
#136/#140 validation. Real dbt against isolated Rust production routers
passed both campaigns: **120 certified RPCs / 36 invocations** for source
operations and **37 certified RPCs / ten invocations** for recording. Telemetry
batching differs from hosted, while model submission/confirmation/Record counts
match. All required decisions and physical reference checks pass. Across the
hosted/Rust phases, 21 complete accepted result snapshots have equal typed row
multisets and ordered schemas; each phase separately checks MANAGED/Delta
properties. All owned proxy processes are stopped and test servers clean up
only their isolated Postgres schemas.

The whole ticket remains incomplete on the documented missing-evidence Safety
conflict and remaining boundary/reachability coverage. These validated partial
cells do not establish general Databricks support or remove any #132 blocker.
