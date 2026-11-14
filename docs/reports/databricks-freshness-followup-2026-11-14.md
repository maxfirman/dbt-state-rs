# Bounded Databricks freshness follow-up — 2026-11-14

**Validated partial; #140 remains INCOMPLETE and does not unblock dependents.**
The omitted-source hosted SKIP still serves a physically stale result in the
retained diagnostic. No scope waiver, dependency promotion, new replay gap or
exclusion is introduced. #158's untargeted qualification and #159's unresolved
boundaries remain explicit, as do #132's copied-target and snapshot blockers.

The bounded change collapses identical known **current** upstream metadata for
Databricks rendered FULL submits without query dependencies and with a supported
physical Target relation. Equality is checked on complete raw entries before
identifier resolution. Every entry sharing that physical name must have the
same raw spelling and known epoch. Conflicting or missing epochs, mixed physical
aliases and duplicate Target relation evidence remain ambiguous. Producer
resolution follows normalization, so equal names/epochs resolve together.
The pending execution retains uncollapsed wire inputs; recorded and legacy
duplicates are never normalized. No identity fingerprint or version changes.

Migration 0022 adds nullable `display_submitted_at_ms` to executions and node
decisions. Databricks SQL submits record their server/replay-clock instant;
RecordExecutions and old rows retain NULL. Explain uses the current submit clock
for relative/absolute timestamp text even on an identity-change rebuild without
a reuse anchor. For captured FULL unknown-current cells it displays that instant;
when the recorded epoch is also missing, the gap uses the original build's submit
clock only when retained. Epochs remain unknown and the shared freshness verdict
remains unprovable/BUILD. No display timestamp enters freshness, producer
resolution, physical completion ordering or build-age policy; legacy clocks are
never backfilled from completion or receipt timestamps.

## Captured evidence before implementation

The complete preserved archive is
`~/.local/state/dbt-state-rs/databricks-140-followup-20261113`:

| Capture | RPCs | SHA-256 |
| --- | --- | --- |
| Protocol boundaries and duplicate order controls | 54 | `e1d97d8c34be188a5a92c5f1164e9d4a0c2f67d6fbefd6eb7d7b7330852d35b0` |
| Real repeated-source query plus crafted duplicates | 32 | `6ff28f23f8d768732569ae0b78244647db8979253a8cd30f5d7da5185badb03e` |
| Delayed missing-current and both-missing Explain | 10 | `ef4f206de14313520d62f600e632c25fa448e48bea1e26e7eb54d056af49e75a` |

The protocol calls are explicitly crafted public-gRPC evidence, not a claim
that dbt emits those requests. Hosted order controls exhibit last-entry behavior:
known/new-known and known/missing differ when reversed. The implementation
retains the conservative BUILD floor for every conflicting/missing duplicate.
The 32-RPC physical control's actual dbt query references its source twice but
emits one unique source entry. Its two additional duplicate reuses are crafted
and labeled; this does not establish that client-emitted duplicates are
unreachable in every other query.

Delayed Explain calls separate first submit, confirmation, repeat submit and
Explain instants. Both missing epochs use first-submit `19:24:42.141` versus
repeat-submit `19:24:44.563`, producing the literal `2 seconds` gap. The synthetic
consumer completion epoch is about 50 seconds older and cannot explain it.
The known-recorded/current-missing case displays `19:24:37 UTC` at both delayed
Explain calls and a minute gap from its recorded source epoch. No fresh cloud
recapture was needed to establish these display defaults.

The immutable #136 public duplicate red is retained unchanged. Fresh public
regressions establish red then green for duplicate reuse, missing-current tree
and both-missing tree, including full response/status/metadata and both repeated
Explain calls. The original complete 38-RPC boundary red and all unsafe evidence
remain intact in `databricks-140-20261112`; passing fixtures do not replace them.

The identity-change timestamp case comes from #137's existing certified 179-RPC
hosted discovery capture (`databricks-137-20261113/hosted-discovery`). Its public
red is retained in `/tmp/dbt-state-132-context/sql137-replay-slice1.log`.
The new six-RPC fixture keeps original entries 0/3/5/48/49/51: validation,
initial build/confirmation, identity-change build/confirmation and Explain.
Only intervening calls that publish no execution are omitted. It requires no
unmerged SQL equivalence behavior and compares the complete causal history.

## Validation and physical scope

Current-source evidence is retained in
`~/.local/state/dbt-state-rs/databricks-140-followup-20261114`.
The actual-client campaign uses pinned dbt 2.0.11, serverless Unity Catalog,
managed Delta and only fresh exact `f_<run>_...` objects in the two explicitly
leased `cdo_demo_db` schema containers. Before/after inventories preserve every
preexisting object; there are no schema creates or cross-run writes.

Two actual builds, two mandatory actual unchanged reuses and two crafted known
duplicate reuses each check complete typed row multisets, duplicate multiplicity,
NULLs, ordered schema and MANAGED/Delta properties. Both a State-disabled dbt
execution and independent SELECT establish the declared literal rows. Reference
invocations emit zero State RPCs. An amount-10 to amount-17 UPDATE must rebuild;
its unchanged follow-up must reuse. An always-build implementation cannot pass.

The first physical pass succeeds with 10 pinned dbt invocations, 32 certified
RPCs and six complete independent result checks. Its capture SHA-256 is
`e8fef5f1322a00b1e22a7f99288ca4d6ad7437ab680f3722ea6aa29ddce406e3`.
The final pass validates the conservative supported-target qualification guard
with the same 10 invocations, 32 certified RPCs and six independent checks; its
capture SHA-256 is
`9935a1a8225aa9d015d9891356ed8c622a14d4008e742785ac482122b1dc038d`.
Local setup failures (an ephemeral Python path and invalid run-ID alphabet)
occurred before cloud calls and are retained separately. Build failures from
new nullable fields missing in two test constructors are retained; only fixture
initializers changed, with no behavioral assertion waived.

The full workspace passes 634 tests with zero failures and 35 opt-in tests
ignored, including exact replay of all 103 corpora and the six new public
regressions. Workspace/all-targets/fuzz Clippy, 310 no-database library tests,
format/diff checks and six Python oracle groups pass.

Ordinary workspace checks cover existing Snowflake/DuckDB contracts, physical
scope isolation, changed/unknown evidence and frozen/current unknown physical
epoch floors. The full 25-case DuckDB acceptance at integration base `a2330b0`
is prior release-gate evidence; this follow-up does not claim a fresh 25-case
cloud-independent campaign. The final integration release gate remains required.

Four new complete fixtures enter generic replay with no Databricks gaps,
exclusions, unknown baselines or temporal masks. The accompanying JSON report
records final command results, capture hashes and limits.

After those commands exited, validated seed integration `4a27642` was merged.
It adds a seed fixture and tests with zero production/proto delta. Corpus and
census pins were reconciled to 104 exact replay corpora and 650 Explain trees
in 115 files. The merged seed oracle, six public freshness regressions and
complete 104-corpus replay pass: 19 tests, zero failures, two opt-in tests
ignored. All production-source digests still match the successful final physical
run. The full 634-test workspace/Clippy attribution remains the pre-seed harness
with this same production code; it was not rerun solely for the harness addition.
