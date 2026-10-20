# Conformance and safety test plan

This is the handoff plan for turning the current collection of captured examples
and offline regressions into an explicit, versioned conformance claim. It does
not authorize a claim of hosted parity. The work through milestone 3 needs only
Rust, Postgres and the vendored protocol; do not contact a warehouse or the
hosted service while access is unavailable.

## Objective and evidence rules

There are two different questions:

1. **Safety:** can a SKIP or CLONE serve the wrong physical result? An independent
   model of controlled, simple workloads can check this offline. It must not
   call the production fingerprinting, freshness or decision functions to
   decide what result *should* exist.
2. **Hosted parity:** does the server return the same gRPC status and complete
   response as dbt State for the same starting state and request? Only a
   causally complete hosted capture for a pinned client/version/warehouse can
   establish this. Offline safety tests, published syntax and a matching
   decision variant are weaker evidence.

Maintain separate parity and safety fields for every capability, using the
labels `hosted-verified`, `offline-safe`, `intentional-fallback`,
`known-mismatch`, and `unknown` where applicable. Attach a fixture/test or a
precise documented reason to each label. Public
[dbt State behavior](https://docs.getdbt.com/docs/deploy/dbt-state-about) and
[configuration descriptions](https://docs.getdbt.com/reference/resource-configs/dbt-state-configs)
define the advertised surface, but do not determine exact wire responses.
Record the source date and client version for evidence; do not reinterpret an
older capture after an upstream client change.

The first supported slice should be the already captured Fusion client and
Snowflake protocol behavior. Keep BigQuery, Databricks and Redshift marked as
unverified against real warehouses/hosted State. Their current conservative
same-target behavior can still be safety-tested offline. Pin exact client
versions/commits when known; leave them `unknown` rather than implying a broad
dbt v1/v2 compatibility range.

## Current starting point

- [`conformance.rs`](../crates/harness/tests/conformance.rs) replays 26 causal
  fixtures. The separate `CHARACTERIZATION_ONLY` captures need dedicated tests;
  [`replay_gaps.json`](../golden/replay_gaps.json) pins known response gaps by
  fixture entry. Unknown initial hosted history is not recreated.
- [`support.rs`](../crates/harness/tests/support.rs) starts the production gRPC
  router against an isolated migrated Postgres schema. Reuse this boundary for
  the new runner, including organization metadata.
- [`decision.rs`](../crates/server/src/decision.rs) is a pure policy engine for
  a *prevalidated* candidate. Its properties cannot prove store selection,
  physical state, clone provenance or correct request/confirmation lifecycles.
- [`concurrency.rs`](../crates/harness/tests/concurrency.rs) pins an offline
  confirmation-order rule. That rule is not proven to equal physical warehouse
  write order or hosted behavior; generated traces must flag ambiguous ordering
  instead of silently treating it as known parity.
- [`correctness-handoff.md`](correctness-handoff.md) records unresolved
  policies, including lag tolerance, volatile transformation values, clone
  time/chain limits, cross-environment data-test reuse and response-code cases.

## Milestone 1: coverage ledger, before more policy changes

Create a small, reviewable machine-readable ledger, for example
`golden/conformance_coverage.json`, plus a human-readable summary in this doc or
[`testing.md`](testing.md). Each row needs a stable capability ID, client
version/commit, warehouse family, node/RPC/decision branch, parity and safety
statuses, evidence paths, known limitation, and next validation action. Include
the intended behavior on unsupported requests. Start from actual public RPCs
and the current test/gap inventory rather than expanding the full Cartesian
product of every field. A CI test should reject duplicate IDs, missing evidence
files, invalid statuses, and an unclassified fixture or exception. A downgrade
or removed gap needs an explicit reason in the ledger and a reviewed change;
it must never disappear merely because a test stopped checking it.

Seed rows for: model/seed/snapshot/data-test submit and confirm, Record,
RegisterClone, selector, deferral, speculative/version/Explain/telemetry behavior,
same-target reuse, cross-name clone, freshness ANY/ALL and lag, template mode,
volatile SQL, physical scope, and errors. Separate request/decision coverage
from response-field coverage. Mark the six causal mismatches and field-scoped
exemptions in `replay_gaps.json` explicitly; do not count them as passing parity.

**Acceptance:** a reviewer can answer which client and warehouse are claimed,
which branches are supported, what evidence each claim has, and which failures
are deliberate without reading all test source files. Existing tests continue
to pass; the ledger validator works without Postgres.

## Milestone 2: deterministic independent physical-state runner

Add a test-only trace runner under `crates/harness/tests/` using the existing
in-process gRPC/Postgres helper. Define typed operations such as `Submit`,
`Confirm`, `Record`, `ExternalReplace`, `Drop`, `RegisterClone`, `Restart`, and
`Probe`; preserve org, warehouse family, namespace, target, source, epochs and
returned request IDs. The model tracks the current *physical* object in each
scope, its independently assigned content token, upstream revisions, and the
pending/confirmed operation history. Use a tiny controlled SQL vocabulary
(for example `SELECT 1 AS id` versus `SELECT 2 AS id`) whose expected content
token is declared by the scenario. Do not implement a second general SQL
parser or hash function. An explicit simulated build/clone step updates the
model's physical object; a mere `ready_to_execute` response does not. For
freshness scenarios, the declared allowed result can include policy-tolerated
stale data; it is not automatically the newest upstream revision.

Check safety implications rather than guessing all hosted decisions:

- SKIP requires an existing current target with the expected content/result
  token and valid cached outcome. A previous matching *historical* row is not
  enough after a replacement, drop or failed/unconfirmed operation.
- CLONE requires an existing source in the same permitted warehouse connection
  and a source token allowed by the scenario's logic and reuse policy. A clone
  from a different namespace/family or a changed, non-equivalent definition is
  a safety failure. Source data age alone does not prove a violation: the
  hosted service has been observed cloning a source older than its upstream.
- A confirm must correlate to a pending request; duplicate confirms must not
  change the first durable outcome. A rejected Record batch publishes neither
  execution nor selector/deferral state.
- Different orgs and physical warehouse scopes cannot affect one another.

Begin with fixed traces: build→confirm→skip; SQL change; overwrite followed by
resubmit of old logic; missing/unknown upstream epoch; dropped or externally
replaced target; clone→confirm→resubmit; source replacement before clone;
same relation text in another connection; duplicate confirm; Record failure;
restart between submit and confirm. Where the protocol lacks enough evidence
to establish physical order or contents, label the trace `unknown` and assert
only the conservative property that is justified. Do not make a server response
its own safety oracle.

**Acceptance:** the runner executes those traces through gRPC against fresh
migrated Postgres state, prints the seed/step/scope on failure, and detects a
deliberately introduced wrong-SKIP or wrong-source-CLONE mutation. Its oracle
imports no production decision/fingerprint/store code.

## Milestone 3: generated traces and upgrade boundary

Generate bounded sequences from the same typed operations, initially with a
small reproducible seed set in CI. Prefer useful transitions over random
protobuf bytes: pending→confirm, confirmed→replace, source→clone→replace,
template/rendered edits, metadata missing/duplicate, cross-org/scope switches,
and restart. Persist any failing seed and a minimal replayable trace. Shrink by
deleting operations while preserving their prerequisites; never drop the source
build or confirmation that makes a clone meaningful. Make fixture/seed additions
reviewable. Keep expensive generated suites opt-in until runtime is measured;
fixed regressions must run in ordinary CI.

Add upgrade traces using the frozen old-server rows in
[`golden/upgrade/`](../golden/upgrade/) plus current migrations, including both
confirmed and pending states and both rendered/template comparison. Cover
physical-scope migration and selector/deferral state as well as fingerprint
invalidation. Do not use a current hasher to manufacture an alleged historical
fingerprint.

**Acceptance:** repeated runs with the same seed produce the same trace and
result; failures have a replay command; a mutation that resurrects an old row
is detected; migration tests use frozen historical inputs.

## Milestone 4: hosted characterization, when access returns

For each unresolved behavior, build a controlled client-driven experiment with
a clean baseline in the permitted demo database. Capture the full causal chain
(including source build, confirmations, physical metadata and final warehouse
contents), not only a final decision. The existing recording proxy and
`experiments/` oracles should reject missing or uncorrelated prerequisites.
Run the same scenario against this server with equivalent initial state; map
opaque IDs and compare complete responses *and* gRPC errors, exempting only
explicit field-scoped differences. Save minimized captures in
`golden/fixtures/` and update the ledger. Live work must only mutate
`CDO_DEMO_DB`; no live call is part of milestones 1–3.

Characterize lag tolerance first: the current server intentionally does not
apply it, while the public config describes a rebuild window. Test a truth
table with unchanged/changed logic, upstream before/after the own build, build
age inside/outside tolerance, and ANY/ALL policy. Do not implement a guessed
formula from documentation alone. Then tackle volatile transformation values,
clone time/chain limits, missing-history data-test cases, and rejection-code
differences. Test non-Snowflake metadata and DDL only when those warehouses are
actually available.

**Acceptance:** every policy change has a versioned, causally complete hosted
example, a failing local regression before the fix, a passing comparison after
it, and an updated ledger. If hosted and public documentation disagree, record
the version and observed behavior rather than generalizing either one.

## Release gate and ordering

Conformance is claimed for a **named client/version × warehouse × feature
slice**, never for an unbounded product. For that slice, require: no unknown or
unexplained parity cells; no wrong-result SKIP/CLONE from fixed, generated and
upgrade safety traces; zero unclassified causal replay mismatches; complete
response/error comparisons where hosted history is reproducible; and an
explicit, tested fallback for every intentional gap. Report exact-match,
partial-history, known-gap and unknown-baseline counts separately so a rising
fixture count cannot mask weaker assertions. Offline safety evidence cannot
promote an item to hosted-verified.

Prioritize wrong-result reuse or cross-tenant/warehouse contamination first,
then false rebuilds and missing features, then response fidelity and
performance. Operational deployment readiness (authentication, availability,
observability, backup/retention and resource limits) is a separate release gate
even after decision conformance is established.

The next implementation change should be **milestone 1 only**, followed by the
fixed-trace part of milestone 2. It should leave `decision.rs` and production
behavior untouched.
