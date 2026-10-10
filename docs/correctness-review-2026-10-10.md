# Correctness and conformance review — 2026-10-10

> Historical pre-hardening review. See [correctness-handoff.md](correctness-handoff.md)
> for completed fixes, current behavior and outstanding work.

Reviewed repository commit: `e31d33aa38e187de434a5f0d83c56e9904c36c9f`.

## Assessment

The project has a useful foundation: preserved protobuf names, a small pure
decision engine, organization-scoped execution queries, transactional batch
recording, immutable duplicate confirmations, isolated integration databases,
and captured hosted traffic. However, it is **not yet a conformant drop-in
replacement**. Several current paths can skip required work, and the existing
conformance tests do not establish equivalence with the hosted service.

The most important change is to require evidence that a matching execution
still describes a usable physical object. Finding a matching historical row is
not sufficient. SQL hashing, dependency handling, and test-result reuse also
need correctness work before expanding the UI or adding MCP and observability.

This review separates three kinds of evidence:

- **Reproduced:** behavior demonstrated against the actual implementation in
  isolated local probes. These probes deliberately assert the current defective
  behavior; their passing does not mean that behavior is correct.
- **Code/client evidence:** implementation omissions or incompatibilities
  demonstrated by reading the server and official clients.
- **Needs hosted characterization:** documentation implies a different rule,
  but the exact current hosted behavior needs a controlled capture before
  changing the implementation.

No new requests were sent to the metered hosted service. Server implementation
and existing tests were left unchanged; this document is the review artifact.

## Research and validation

I reviewed the official State configuration, reuse, deferral, and explain docs,
and two official public client codebases:

- [dbt-labs/dbt Rust client](https://github.com/dbt-labs/dbt/tree/a0fc53e6797b135ce8d4fea99c581ea5116803ac),
  including request construction, metadata collection, data-test result
  consumption, clone fallback, selectors, and deferred relation resolution.
- [dbt-labs/dbt-state Python client](https://github.com/dbt-labs/dbt-state/tree/b0f3030c90dd0766f2d1625b3305829d881fc2ae),
  including seed configuration, SQL enrichment, and result confirmation/reuse.

The vendored `query_cache` protobuf directory exactly matches the reviewed Rust
client commit. This is strong evidence for schema compatibility, not behavioral
conformance. The older Python client's messages are materially different: for
example, test identity is carried in labels without `dbt_node_state`.

Validation performed with Rust 1.98.0 and a disposable Postgres 17 instance:

- `cargo +1.98.0 test --workspace`: **106 tests passed**.
- `cargo +1.98.0 test -p dbt-state-server --lib`: **48 tests passed**.
- `cargo +1.98.0 fmt --all --check`: passed.
- `cargo +1.98.0 clippy --workspace --all-targets --features fuzz -- -D warnings`:
  passed; this compiles the fuzz tool without running hosted requests.
- Additional local probes reproduced the findings listed below. Their source
  is available for this session at `/tmp/dbt-state-review-probes/src/lib.rs`.
- A separate replay run measured the assertions made by
  `conformance_replay_all_corpora`; see the testing findings below.

The default installed Rust 1.93.1 cannot build the entire workspace because the
UI dependency requires 1.98. This is a reproducibility issue, not a server logic
failure. The documented minimum is already 1.98, but no repository toolchain
file selects it.

## Findings requiring correctness work

### 1. P1 — Historical matches can refer to overwritten, missing, or modified targets

**Reproduced.** In [store.rs](../crates/server/src/store.rs), lines 95–156,
lookups filter by the requested fingerprint *before* choosing the newest row.
After confirming SQL A, then SQL B against the same target, requesting A again
returns SKIP. The physical table now contains B. Seed lookup has the same
historical selection pattern at lines 210–229.

The existing [hardening test](../crates/harness/tests/hardening.rs), lines
235–272, explicitly requires the original version to keep skipping after the
second version is confirmed. This locks in unsafe behavior unless there is an
independent, still-valid object from which A can be recovered.

Separately, [decision.rs](../crates/server/src/decision.rs), lines 206–219,
excludes the target from upstream checks but never performs the target's own
existence or external-modification check. Local probes returned SKIP for both
a target with no modification epoch and a target modified after confirmation.
`ignore_external_modifications=false` makes no difference.

The target metadata is meaningful protocol evidence. The
[Python client](https://github.com/dbt-labs/dbt-state/blob/b0f3030c90dd0766f2d1625b3305829d881fc2ae/clients/dbt_state/src/dbt_state/run_cache.py#L1152)
explicitly includes it to detect unobserved modifications. The
[Rust client](https://github.com/dbt-labs/dbt/blob/a0fc53e6797b135ce8d4fea99c581ea5116803ac/crates/dbt-tasks-core/src/run_cache/run_cache_service.rs#L3795)
omits the target table entry when its epoch is absent. The replay's comment
that physical existence is not represented in the protocol is therefore too
strong.

**Improve:** separate execution history from the current state of each physical
relation. Validate current target existence and epoch before SKIP. Retain old
executions as candidate provenance, but establish that their source object
still exists and still represents that execution before reuse. Update current
relation state atomically with confirmation/recording; characterize late,
out-of-order confirmations rather than assuming receipt order is build order.

**Tests:** A→B→A, changed materialization on the same target, dropped/recreated
targets, external edits with both flag values, seed reversion, server restart,
and concurrent builds with reversed confirmation order. Capture the hosted
decisions and complete response fields for each sequence.

### 2. P1 — SQL normalization merges expressions with different behavior

**Reproduced.** [sql_norm.rs](../crates/server/src/sql_norm.rs), lines 54–138,
produces identical canonical SQL for these pairs:

| First expression | Second expression | Lost distinction |
| --- | --- | --- |
| `cast(x as integer)` | `try_cast(x as integer)` | Error versus null on conversion failure |
| `cast(x as varchar(1))` | `cast(x as varchar(100))` | String length |
| `cast(x as numeric(10,0))` | `cast(x as numeric(10,2))` | Scale |
| `my_schema.nvl(x, 0)` | `my_schema.coalesce(x, 0)` | Distinct qualified function identities |

All cast kinds are replaced with `Cast`, type arguments are discarded, and
function synonym rewriting applies to qualified names. These collisions can
skip changed model logic. Snowflake documents the distinct behavior of
[TRY_CAST](https://docs.snowflake.com/en/sql-reference/functions/try_cast)
and precision/scale in [CAST](https://docs.snowflake.com/en/sql-reference/functions/cast).

The normalizer always uses `SnowflakeDialect` regardless of request dialect.
Its claim that imperfect normalization can only cause additional execution is
not true for these rewrites.

There is also a reproduced **P2 robustness bug** in the lexer fallback:
`normalize_sql("select é @@")` panics at line 256 because byte slicing crosses a
UTF-8 character boundary. Parser failure should not make a valid protobuf string
panic the request handler. Use character-safe tokenization or preserve raw SQL
when safe normalization is unavailable; add arbitrary UTF-8 and parser-failure
fuzz inputs with a no-panic property.

**Improve:** preserve cast kind and type parameters. Apply only equivalences
supported by dialect-specific evidence; do not rewrite arbitrary qualified
functions as builtins. Pass dialect and resolution context into normalization.
Version persisted fingerprints so a normalization change cannot silently
reinterpret previous evidence.

**Tests:** negative distinctness cases alongside synonym cases, generated
precision/scale/length combinations, builtin versus UDF resolution, and
supported-dialect fixtures. For proposed equivalences, compare actual warehouse
results and failure behavior as well as hosted decisions.

### 3. P1 — Data-test matching ignores changed logic and cached results are lost

**Reproduced.** [services.rs](../crates/server/src/services.rs), lines 434–452,
uses an identity-only lookup for data tests. The query in
[store.rs](../crates/server/src/store.rs), lines 179–195, ignores rendered SQL,
semantic configuration, and logical namespace. A test with the same unique ID
but changed SQL/configuration returned SKIP.

If `dbt_node_state` is absent, the fallback matches the empty target without a
SQL restriction. A Python-style request with an unrelated test unique ID in
labels skipped against a different previously confirmed test.

`ConfirmExecution` and `RecordExecutions` discard `execution_results`, and
`skip_response` always returns an empty Struct (services.rs:43–48, 725–740,
758–810). A confirmed failing test's result was returned with zero fields.

The [Python client](https://github.com/dbt-labs/dbt-state/blob/b0f3030c90dd0766f2d1625b3305829d881fc2ae/clients/dbt_state/src/dbt_state/run_cache.py#L633)
confirms and consumes `failures`, `should_error`, and `should_warn`. The reviewed
[Rust client](https://github.com/dbt-labs/dbt/blob/a0fc53e6797b135ce8d4fea99c581ea5116803ac/crates/dbt-tasks-core/src/run_cache/run_cache_service.rs#L5101)
protects itself by executing a test when a SKIP lacks a cached result. Thus this
currently prevents proper reuse with that Rust client; it should not be
described as silently passing failing tests in every client version.

**Improve:** match scoped test identity *and* relevant logic/configuration,
support the identity representation used by each supported client, and persist
and return the complete result Struct through both recording paths. If identity
or result evidence is insufficient, do not advertise a reusable test result.

**Tests:** pass/warn/fail result round trips, changed singular and generic tests,
same ID with changed severity/thresholds, unrelated tests with empty targets,
identical IDs in different projects, and actual Python/Rust client consumption.
Some committed captures already contain test results; replay them intact.

### 4. P1 — Dependency SQL and SQL resolution context do not participate in reuse

**Reproduced for dependency SQL; code evidence for context.** SQL identity in
services.rs:409–418 includes only the root SQL/template and semantic extras.
`query_dependencies` are captured as names for the UI, but their definitions
are not used by the decision/store paths. Changing a supplied view definition
from `select 1` to `select 2` while keeping the root SQL unchanged returned SKIP.

The [Rust client](https://github.com/dbt-labs/dbt/blob/a0fc53e6797b135ce8d4fea99c581ea5116803ac/crates/dbt-tasks-core/src/run_cache/run_cache_service.rs#L3775)
deliberately publishes views through `query_dependencies` so the server can
recurse through their DDL and detect transitive changes. Default catalog/schema
and dialect also affect how unqualified names and functions resolve; hashing
raw SQL without that context cannot establish semantic equivalence.

**Improve:** build and retain a resolved dependency graph, including view
definitions, resolution context, and transitive logic fingerprints. Keep logic
changes distinct from data freshness changes. Define conservative behavior for
cycles, incomplete definitions, and unresolved references, then characterize
`lenient_dependencies` rather than ignoring it.

**Tests:** nested views, unchanged root SQL with changed view SQL, raw references
outside the dbt DAG, unqualified references under different catalogs/schemas,
logic changes inside a lag window, and `select *` with changing upstream column
sets. Test views with explicit projections separately from `select *`.

### 5. P1 — Unknown freshness becomes fresh, and schema stripping aliases distinct inputs

**Reproduced.** `input_tables_of` (decision.rs:264–270) and batch recording
(services.rs:797–803) convert missing modification epochs to zero. An unknown
upstream then appears older than confirmed history and returned SKIP. The
official Rust client's metadata collection describes missing upstream epochs
as conservatively treated as “now”, not zero.

`logical_relation_key` (decision.rs:173–202) drops schemas and lowercases quoted
identifiers. Freshness lookup takes the first matching entry (lines 222–229).
With two genuine inputs `DB.A.T` and `DB.B.T`, a change to the second input was
hidden by the first input's higher epoch, and the model skipped. This is not a
cross-environment mapping: both objects occur in the same query.

**Improve:** preserve known/unknown/missing metadata explicitly. Use real
physical identities within each dependency graph, and establish cross-target
equivalence through explicit node mappings. Preserve dialect-specific quoted
identifier semantics and escaping. Avoid first-match ambiguity.

**Tests:** absent upstream epochs through Submit and Record, duplicate table
names across schemas, shuffled input order, quoted case-sensitive names,
embedded dots/escaped quotes, metadata errors, and recreated inputs. Useful
properties include input-order invariance and distinct physical inputs never
sharing freshness evidence accidentally.

### 6. P1 — Seed reuse ignores configuration and physical metadata

**Reproduced.** SubmitValues matches only target, type, and `values_hash`
(services.rs:599–610). It ignores semantic extras and the target's supplied
epoch. Changing `column_types` without changing the data hash returned SKIP.
Batch recording also omits a semantic seed fingerprint.

The [Python seed request builder](https://github.com/dbt-labs/dbt-state/blob/b0f3030c90dd0766f2d1625b3305829d881fc2ae/clients/dbt_state/src/dbt_state/run_cache.py#L987)
sends seed data and semantic configuration separately. A matching data hash
does not prove equivalent column types, quoting, or delimiter interpretation.

**Improve:** include canonical semantic configuration in seed identity, validate
the current object, and make Submit/Confirm and Record use the same evidence
representation. Do not rely on a newer client's aggregate node hash to repair
compatibility with older clients.

**Tests:** changed types, quoting, delimiter, relevant persisted descriptions,
drop/recreate, external edits, A→B→A data, recording-path parity, and seed clone
policy across targets.

### 7. P1 — Namespace reuse can SKIP a target that needs building or cloning

**Reproduced.** services.rs:461–493 chooses a matching namespace execution even
when it belongs to another physical target. The pure engine has only SKIP and
EXECUTE verdicts, so ordinary SubmitEnrichedSQL cannot select CLONE. A confirmed
production model followed by a never-built development target with deferral
disabled and `allow_clones=false` returned SKIP.

Namespace matching is useful for discovering candidates; it is not sufficient
to choose the action. The [official allow_clones contract](https://docs.getdbt.com/reference/resource-configs/allow-clones)
requires a full build when cloning is disabled and only another target offers
a usable match. The [State reuse docs](https://docs.getdbt.com/docs/deploy/dbt-state-about)
distinguish skipping a current object from cloning an object in another schema.

The physical fallback also runs after a supplied namespace misses, rather than
only when the namespace is absent. This can reuse an execution from a different
logical identity sharing the same physical target.

**Improve:** discover candidates, validate their physical state, and then choose
SKIP/BUILD/CLONE using target presence, freshness, clone eligibility, and defer
policy. Preserve the requested logical scope. Characterize candidate ranking
and the exact semantics of `defer_enabled`/`is_defer_to_profile`.

**Tests:** missing versus existing dev target, clones enabled/disabled/omitted,
multiple candidates with different data freshness, same target with a changed
namespace, and already-dropped candidate tables. Compare the complete clone
response and execute its DDL in supported warehouse integration tests.

### 8. P1 — Volatile SQL evaluation is unimplemented

**Code/client evidence.** `tolerate_nondeterminism` is unused by decisions;
transformed function mappings are always empty, and confirmed execution results
are discarded. The [official volatile SQL configuration](https://docs.getdbt.com/reference/resource-configs/evaluate-volatile-sql)
requires runtime function outputs to participate in reuse when evaluation is
enabled. The [Rust client maps that setting inversely](https://github.com/dbt-labs/dbt/blob/a0fc53e6797b135ce8d4fea99c581ea5116803ac/crates/dbt-tasks-core/src/run_cache/run_cache_service.rs#L3446)
to `tolerate_nondeterminism`.

An unchanged SQL hash and unchanged table epochs cannot validate a node whose
meaning depends on a changed `current_date()` value under this setting.

**Improve/tests:** capture the full transformation/confirmation/reuse lifecycle
for the supported volatile functions. Preserve function mappings and results;
test same-day reuse, date rollover, multiple calls and nested dependencies.
Until implemented, decline unsafe reuse when this evidence is required. That
is a temporary correctness fallback, not full hosted conformance.

### 9. P1/P2 — State selectors and state-backed deferral return empty successes

**Code/client evidence.** SelectorService returns no IDs for every request
(services.rs:1035–1042). Its “conservative default” comment is incorrect: an
empty response to `state:new` or `state:modified` can suppress all required
builds. The [official Rust selector](https://github.com/dbt-labs/dbt/blob/a0fc53e6797b135ce8d4fea99c581ea5116803ac/crates/dbt-state/src/selector.rs#L66)
uses those returned IDs directly.

ResolveDeferredRelations always returns an empty map (services.rs:864–871).
The [Rust deferral client](https://github.com/dbt-labs/dbt/blob/a0fc53e6797b135ce8d4fea99c581ea5116803ac/crates/dbt-state/src/run_cache_defer.rs#L179)
uses actual state-backed FQNs to patch target references; falling back to
rendered profile names does not reproduce every historical custom naming rule.
These are documented State features, not optional UI enhancements. See the
[deferral and State selector documentation](https://docs.getdbt.com/docs/deploy/dbt-state-deferral).

**Improve/tests:** retain the project/target/node history needed to answer these
RPCs. Capture every selector criterion, empty history, modified hashes,
relation changes, batch boundaries, and project/organization isolation. Test
custom schema/alias/database macros and renamed targets for deferred resolution.
Unsupported functionality should be explicit, with client fallback verified;
do not report a fabricated successful answer.

### 10. P2 — Registered clones lack eligibility checks and useful provenance

**Code evidence.** RegisterClone always produces ReadyToClone
(services.rs:880–921), ignoring source epoch, table properties, and chain depth.
The response always omits `clone_required_last_modified_epoch` (line 116).
The pending record has no source logic hash or input evidence, so confirmation
does not make that cloned object reusable by normal nonempty SQL matching.

The Snowflake generator emits `CREATE OR REPLACE VIEW ... CLONE ... COPY GRANTS`
for a view source (clone.rs:35–44). This is not valid
[Snowflake CREATE VIEW syntax](https://docs.snowflake.com/en/sql-reference/sql/create-view).
Tests exercise table kinds and string templates, not view execution or warehouse
eligibility.

`failed_to_clone` is also ignored. Do **not** simply reject every confirmation
with that flag: the official client can set it after a failed clone followed by
a successful normal build. The source invalidation and fallback provenance
need lifecycle characterization.

**Improve/tests:** preserve usable source provenance and enforce observed source
kind, epoch, expiration, chain-depth and time-travel rules. Capture source edits
between decision and execution, missing source, unsupported kinds, successful
clone→subsequent submit, and failed clone→successful build→confirmation. Validate
DDL against each supported warehouse rather than assuming syntax equivalence.

### 11. P2 — Template comparison is implemented as an exclusive hash switch

**Reproduced; exact hosted rule needs characterization.** services.rs:409–418
stores either rendered SQL or the unrendered body as one combined hash. With
`compare_unrendered_code=true`, changing the template while keeping identical
SQL caused execution. The [documented rule](https://docs.getdbt.com/reference/resource-configs/compare-unrendered-code)
requires both template and rendered SQL changes to trigger a logic rebuild.

The current fixture establishes only one half of that behavior: same template,
changed rendered SQL. It does not establish that a changed template with equal
rendered SQL must rebuild. RecordExecutions also always hashes rendered SQL,
making its history incompatible with template-mode matching.

**Improve/tests:** retain independent rendered, template, macro/dependency, and
config evidence. Capture all four changed/unchanged combinations, missing body
hashes, toggling the setting, macro changes, and Record→Submit parity.

### 12. P1 candidate — Lag tolerance models timestamp distance, not elapsed build age

**Needs hosted characterization.** decision.rs:206–255 checks whether an
upstream epoch exceeds its recorded epoch plus the tolerance. It never uses
the current time. The [current lag_tolerance documentation](https://docs.getdbt.com/reference/resource-configs/lag-tolerance)
describes a build-age gate together with changed upstream data, and requires
upstream logic changes to bypass that gate.

For a model built at 08:00, data changed at 08:20, and a 45-minute tolerance,
the documented 09:00 run rebuilds. The current rule can continue skipping
indefinitely if no further source update crosses its timestamp-distance
threshold. Dependency logic changes are not represented either.

The overview docs themselves contain differing descriptions of this setting.
Do not replace the existing rule solely from prose: capture this timeline with
fixed inputs and a known client/service version.

**Improve/tests:** settle the rule experimentally, then pass an explicit clock
or evaluation time into the pure engine if needed. Test the exact boundary,
old source watermark with a recent build, elapsed window without new data,
ANY/ALL policies, view propagation, and upstream logic changes during tolerance.

### 13. P2 — Organization isolation stops at the gRPC execution store

**Code evidence; relevant to shared deployments.** Core execution queries retain
organization filters, which is good. However, [UI queries](../crates/ui/src/db.rs)
at lines 93–149 and 198–230 aggregate globally and fetch invocation details by
ID without organization scope. Sharing the UI against a multi-organization
database exposes execution metadata across organizations.

The gRPC organization header is accepted without authentication, consistent
with local development but insufficient to establish an authenticated tenant
boundary. Make the deployment mode explicit before claiming multi-tenant
support. Also remove the complete database URL from startup logging
([main.rs](../crates/server/src/main.rs):13), since it can include a password.

**Improve/tests:** carry organization scope through every exposed query and
verify cross-organization access denial with colliding project/node identities.
Keep local operation straightforward; avoid building a broad authentication
system before the supported deployment boundary is defined.

## Why the passing tests overstate conformance

### Golden replay omits important comparisons and changes the recorded scenario

The main [conformance replay](../crates/harness/tests/conformance.rs) names five
of the fourteen fixture files: 57 of 102 total captured RPC entries. Other files
have separate tests, but there is no universal exact replay guarantee.

Measured output from `conformance_replay_all_corpora`:

| Corpus | SQL EXECUTE assertions | SQL SKIP assertions | Baseline SKIPs hydrated | Clone entries characterized without comparing the decision |
| --- | ---: | ---: | ---: | ---: |
| `golden_20261008T222744.061Z` | 1 | 3 | 5 | 0 |
| `golden_20261009T083754.534Z` | 5 | 0 | 5 | 0 |
| `golden_20261009T100637.316Z` | 1 | 0 | 0 | 0 |
| `clone_happy_path` | 0 | 1 | 1 | 0 |
| `clone_failed_fallback` | 0 | 0 | 0 | 1 |

That is **11 directly compared SQL decisions**, plus validation/speculative
checks and confirmation-success checks. The failed-clone corpus can pass
without any SQL decision comparison.

Specific weaknesses:

- The causal fingerprint omits rendered SQL, configuration, test identity,
  namespace, and organization (lines 76–95).
- Hosted EXECUTE after a fingerprint was confirmed is not asserted
  (lines 202–213). These are precisely the transitions needed to catch changed
  SQL and freshness invalidation.
- Unknown baseline SKIPs and CLONEs cause synthetic history injection rather
  than validation of a recorded precondition (lines 223–244). Synthetic outcome
  epochs are inferred from inputs; recording errors are ignored.
- Confirmations are associated by last node name/fingerprint, not a bijection
  of original request IDs. The replay replaces results, table type, failed-clone
  flag, labels and metadata instead of replaying them (lines 249–283).
- Most assertions compare only the response variant. Rejection reasons,
  descriptions, results, optional-field presence, clone epoch, transformed
  nodes, and legacy/new clone fields need full comparison.
- Some characterization tests inspect expected fixture values or clone strings
  without requiring the implementation to produce those responses.

**Replace this with a causal trace contract:** include explicit initial state
and setup operations; preserve every request and relevant metadata; map generated
IDs consistently across RPCs; compare full decoded responses while normalizing
only fields proven nondeterministic. A trace whose initial state is unknown can
be retained for research, but must not count as a passing exact conformance case.
Preserve independent/concurrent operations as a partial order when recording
completion order cannot establish request order.

Discover fixtures through a checked manifest or automatic enumeration and fail
when any supported recorded RPC is unhandled. Report assertion coverage, not
just corpus size or final test count. Deliberately replace the engine with
always-SKIP/always-EXECUTE and disable result storage to ensure the contract suite
fails for the expected reasons.

### Differential fuzzing rarely exercises the cache it is supposed to test

[diff_fuzz.rs](../crates/harness/src/bin/diff_fuzz.rs), lines 114–159, submits
mutants without confirming or recording executions into the local service.
It therefore probes an empty confirmed cache. Upstream history differences are
accepted as expected, and many mutations cannot exercise local reuse at all.

The oracle compares only a decision variant (lines 196–221), skips decoding or
transport errors, and exits successfully even when divergences are reported.
The current mutation axes do not change rendered SQL or semantic extras.

**Replace/add:** stateful differential scenarios which prime both services with
an equivalent isolated baseline and require a cache hit before mutation. Mutate
logic, config, source data, own target, identity, and policy independently; then
test confirm/record/clone/fallback/retry/restart sequences. Use dedicated oracle
identities, preserve complete traces, and shrink divergences into fixtures.
Return a failure status for divergences and a distinct inconclusive status when
successful comparisons are below the required count. Keep hosted runs explicitly
opt-in with a metering budget.

### Properties prove implementation assumptions rather than independent safety

[decision_proptest.rs](../crates/server/src/decision_proptest.rs)
duplicates freshness and schema-stripping assumptions in its oracle. It starts
with a supposedly valid confirmed row, so it cannot detect an invalid historical
candidate supplied by the store. Some “changed hash” properties simply pass no
confirmed row rather than exercise a changed hash through storage.

Keep the useful pure tests, but add a state-machine model with physical relation
contents, logical history, dependencies, and outcomes. Assert that SKIP/CLONE
uses valid evidence under the characterized policies, not merely that its
arithmetic agrees with a second copy of the same implementation. Add metamorphic
properties for harmless SQL edits and negative properties for meaningful edits,
identifier distinctions, unknown metadata, and input order.

## Feature coverage against the official clients

The 102 committed entries contain 52 SQL submits, 29 confirmations, nine
version checks, four speculative submits, three deferred resolutions, two clone
registrations, and three values submits. All 59 submit-family requests are
Snowflake. There are no hosted captures for RecordExecutions, selectors, Explain,
telemetry, or health; synthetic tests are useful but are different evidence.

| Surface | Current implementation | Correctness/conformance work remaining |
| --- | --- | --- |
| Protobuf services/messages | Matches reviewed public Rust schema | Automate descriptor drift checks; retain old-client wire tests |
| Submit SQL: build/skip | Implemented subset | Physical validity, complete logic/dependency/context evidence, policies |
| Submit SQL: automatic clone | Missing | Candidate selection, eligibility, response and lifecycle |
| Seeds | Data-hash subset | Config, object validity, cross-target reuse |
| Data tests | Identity-only matching; empty results | Scoped logic identity and lossless cached outcomes |
| ConfirmExecution | Idempotent and org-scoped | Results, clone/fallback provenance, late-order behavior |
| RecordExecutions | Transactional insertion | Full outcome/dependency evidence and parity with submit/confirm |
| RegisterClone | DDL templates, both response forms | Eligibility, source tracking, view handling, limits/properties |
| Speculative SQL | Always Undecided | Safe fallback, but no early reuse functionality; capture other branches |
| Deferred relations | Always empty | Project/profile/target scoped actual relations |
| State selectors | Always empty | Every criterion and history scope; empty response is unsafe |
| Volatile evaluation | Missing | Transformations, results, runtime comparison |
| Lag tolerance / ANY / ALL | Timestamp-distance implementation | Resolve documented time rule and direct-parent versus leaf behavior |
| Template comparison | Exclusive hash mode | Full truth table, macro effects, mixed-mode history |
| External modifications | Ignored | Own-object metadata and flag semantics |
| Lenient dependencies | Ignored | Characterize allowable stale deferred inputs |
| Clone limits/properties | Largely ignored | Time travel, depth, expiration, warehouse-specific execution |
| Explain RPCs | Empty | Real messages and dependency changes; `dbt state explain` compatibility |
| Client validation | Always supported | Characterize versions and unsupported-client behavior |
| ClientTelemetry | Not registered | Match expected acknowledgement/failure behavior |
| Health | Overall SERVING | Named-service behavior and readiness, if clients use them |
| Other warehouses | SQL normalization is Snowflake; clone strings guessed | Actual dialect/client/warehouse evidence |
| Python/custom/unit-test/other execution types | Sparse or no hosted evidence | Verify eligibility and client bypass/write-only behavior by version |

Not every user-facing configuration needs server implementation. For example,
[pre_clone](https://docs.getdbt.com/reference/resource-configs/pre-clone),
hook execution, and metadata warehouse selection have substantial client-side
behavior. Test the requests and lifecycle they produce rather than adding server
settings with the same names without evidence.

The current docs say Python and custom materializations are not reused, while
the repository contains a captured custom-materialization SKIP. Retain that
observation, record its client/version/context, and resolve the discrepancy
experimentally. Do not turn either one isolated trace or product prose into an
unconditional rule for every client and execution type.

## Recommended foundation and delivery order

1. **Make the oracle trustworthy.** Define the supported client/dialect/version
   matrix and fixture provenance. Replace partial replay with complete causal
   contracts, including the test-result captures already available. Clearly
   distinguish exact matches, known divergences, and unknown preconditions.
2. **Eliminate unsafe reuse.** Capture and fix historical-target validity,
   normalization collisions, test identity/results, unknown metadata, dependency
   definitions, and seed configuration. Add each minimal regression through the
   actual store/service boundary, then retain focused pure tests.
3. **Implement reuse actions and core history features.** Add automatic clone,
   state-backed deferral, and selectors with candidate provenance. Resolve lag,
   template, volatile and lenient policies with controlled hosted experiments.
4. **Exercise complete clients and warehouses.** Run pinned Python and Rust
   clients against the replacement with a small warehouse project covering
   tables, views, incremental models, snapshots, seeds, passing/failing tests,
   custom names and raw references. Compare resulting objects, rows, schemas,
   and test outcomes with a full-build reference under the same policies.
   Require current-output equivalence where the policy promises it; for
   permitted lag or tolerated volatility, assert the characterized reuse policy
   rather than unconditional equality with a fresh full build.
5. **Add stateful generated coverage and operational contracts.** Test retries,
   out-of-order confirmations, batch atomicity, migrations, restarts and
   organization isolation; introduce differential trace shrinking and mutation
   testing. Expand warehouse support only with corresponding evidence.

The architecture can remain small. Introduce explicit domain evidence for
logical identity, versioned rendered/template/dependency/config fingerprints,
physical relation state, and recorded outcomes. Keep candidate discovery and
validation separate from a pure action decision, and encode the response in one
place. Use the same evidence builders for Submit/Confirm and RecordExecutions.
Do not put the decision's source of truth in best-effort UI capture tables.

A reasonable readiness gate is: every supported trace replays exactly; every
unsafe local reproduction has an enforced regression; real-client scenarios
agree on both actions and resulting objects/outcomes; stateful differential
traces have comparable initial state and no unexplained divergence; and no
supported RPC silently returns an invented empty success.

Secondary cleanup: pin the Rust toolchain and declare `rust-version`; either
keep the core correctness checks independent of UI dependencies or provision
the documented toolchain consistently. The advertised no-database `--lib`
command now includes a database-backed capture test and should be split or
documented accurately. Bound or isolate best-effort capture work so it cannot
exhaust the decision store's pool. Add matching lookup indexes only after the
correct identity/state model is settled. Update comments and research docs that
describe lexer-only hashing, all-corpus replay, or gaps as uniformly safe.
