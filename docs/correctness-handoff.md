# Correctness handoff after offline hardening

Updated 2026-10-10. Read this before changing decisions. The goal remains a
conformant drop-in dbt State replacement; **the current server does not yet meet
that goal**. There is no Snowflake access, and this work made no new hosted
requests. Passing local tests establish the contracts below, not complete
hosted equivalence.

The [original review](correctness-review-2026-10-10.md) describes commit
`e31d33aa38e187de434a5f0d83c56e9904c36c9f` before these changes. Its line numbers,
status descriptions and test counts are historical. This document is the current
handoff; experimental notes remain evidence about their individual scenarios,
not proof of general rules.

## Completed changes and evidence

| Review finding | Offline change | Main regression coverage |
| --- | --- | --- |
| 1: stale historical matches and own-target validity | Select the newest confirmed physical target before checking its fingerprint or materialization. Require one known current-target epoch. Reject observed external changes unless explicitly ignored. | A→B→A, materialization replacement, missing/changed target, explicit ignore flag |
| 2: unsafe normalization and UTF-8 panic | Preserve TRY/SAFE cast kind, type parameters, qualified function identity. Preserve exact unsupported SQL; normalize only Snowflake. Version the fingerprints. | Negative SQL pairs, arbitrary UTF-8 no-panic property, parser failure |
| 3: test identity, logic, lost results | Match project/namespace/UID plus SQL/config/context; support Python label identity. Persist complete protobuf results through Confirm and Record. Require usable failures/error/warn fields before SKIP. | Passing/failing/warning tests, missing results, changed SQL/severity/project/UID, nested results, int64 MAX, infinity, immutable duplicate confirm |
| 4: dependency definitions and resolution context | Fingerprint all supplied dependency SQL and its catalog/schema, root dialect/catalog/schema, and sorted semantic extras. Empty/ambiguous supplied dependency definitions prevent reuse. Share submit/record fingerprint code. | Changed view definition, changed schema, Record/Submit parity |
| 5: unknown freshness and schema collisions | Keep optional epochs; exact physical dependency names; incomplete or duplicate metadata forces BUILD even with ALL. Corrupt stored evidence returns INTERNAL rather than becoming empty/fresh. | Unknown epochs, two same-named inputs in different schemas, missing/duplicate inputs, invalid JSON/protobuf |
| 6: seed configuration | Match latest physical state, seed bytes and semantic configuration/context; require known current object metadata. | Column types, missing object, content reversion, Record/Submit parity |
| 7: cross-target SKIP without provenance | Remove unconditional namespace reuse. Rebuild another physical target until object/provenance mapping is implemented. | Different target/schema with same namespace cannot SKIP |
| 8: volatile evaluation omitted | BUILD whenever evaluation is requested (`tolerate_nondeterminism=false`) until transformations are implemented. | Flag transition rejects reuse |
| 9: empty selectors/deferral | Return UNIMPLEMENTED for both services rather than an empty successful selection/map. | Public gRPC error assertions |
| 10: invalid clone DDL | Reject view sources and unknown dialects before inserting pending history. Preserve the captured Snowflake table DDL and both response mirrors. | Exact captured table DDL, view/unknown-dialect errors and no pending rows |
| 13: tenant leaks/logged password | Scope every UI database read to configured `DBT_STATE_UI_ORG_ID` (default `local`); remove startup DSN logging. | Two organizations with identical project names; all UI read methods including direct foreign IDs |
| Testing/toolchain | Pin Rust 1.98; move capture DB test out of `--lib`; reject malformed trace lines; replay all 14 fixtures with full captured confirms and ID correlation; fail discovery fuzz tool on transport/decode failures or divergences. | Workspace tests, no-DB library tests, strict trace parse test, full causal response comparisons, fuzz compilation |

`decision::decide` remains pure. Store queries remain organization scoped.

## Compatibility and upgrade effects

Migration `0007_execution_results.sql` adds nullable `project_id`, binary
`execution_results`, and an index on current confirmed targets. Results use the
custom protobuf Struct bytes, preserving int64 and non-finite double fields
without JSON conversion. Existing numeric input epochs deserialize as `Some`;
new null epochs preserve unknown evidence. Corrupt rows cause an explicit error.

SQL/seed hashes now have a versioned, length-framed SHA-256 domain. Existing
fingerprints will cold miss and rebuild; **do not rehash old rows in place**:
root SQL, dependency definitions and complete outcomes were not stored there.
Legacy test rows without recorded results also rebuild.

Current-target validation uses `current_epoch <= confirmed_epoch`, with presence
required. The seed capture has a current metadata epoch
`1791624374547` and confirmation `1791624374910` (363 ms later), so exact equality
would reject a real unchanged object. This assumes the recorded confirmation is
an upper bound for that build's warehouse modification time; timestamps alone
cannot prove object lineage. Stronger lineage and late-confirmation handling
remain below. The explicit ignore-external-modifications flag still requires
known existence evidence.

More work may execute: missing metadata, changed physical input names,
dependency representation differences (SELECT versus full view DDL), another
physical target, unsupported syntax, or requested volatile evaluation. These
are intentional conservative fallbacks, **not claims of hosted conformance**.
Selector/deferral callers now receive UNIMPLEMENTED; this is an explicit feature
gap and may abort client operations that previously succeeded incorrectly.

## Remaining work, in priority order

### A. Physical state, concurrency and clone provenance (review 1, 7, 10)

Offline work: introduce a current-relation/provenance model distinct from
append-only executions. Store enough source identity, logic, freshness and
outcome evidence to validate cross-environment reuse. Test concurrent A/B builds,
reversed confirms, dropped/recreated sources, cloned-target re-submission,
source drift between decision and clone, duplicate confirms, server restart,
and Record batches with conflicting targets. State selectors/deferral will need
project/profile/environment/node history, currently mostly capture metadata
and insufficient as authoritative state.

Hosted/warehouse questions: SKIP versus CLONE versus DEFER eligibility; source
epoch requirements; clone row/time limits, allow_clones, defer_enabled and
is_defer_to_profile; late-confirmation ordering; warehouse clock semantics;
source freshness enforcement; TABLE types other than the captured ordinary and
transient types. Do not reinstate schema stripping or namespace-only SKIP.
`failed_to_clone=true` is not automatically a failed build: the public Rust
client can confirm a successful normal build after clone fallback. Characterize
that lifecycle before rejecting confirmations based on the flag alone.

BigQuery/Databricks/Spark clone templates remain unverified. Snowflake view
cloning now reports unsupported; a correct view-copy strategy needs actual view
definition/context, not `CREATE VIEW ... CLONE`.

### B. Dependency semantics and SQL equivalence (review 2, 4, 5)

Offline work: retain a resolved dependency graph; distinguish view DDL from its
query; handle cycles, duplicates, incomplete definitions, raw references outside
the DAG, quoted identifiers, and default resolution context. Add nested views,
transitive definition changes, missing/added columns with SELECT *, and input
ordering invariance. Exact fingerprinting of supplied definitions does not
establish completeness of the supplied graph. `lenient_dependencies` is still
ignored; define its behavior from primary client/docs evidence and then capture
controlled cases. Builtin synonym rules should gain more negative cases and
warehouse validation before being expanded; unqualified UDF shadowing remains
a resolution question.

Hosted/warehouse questions: actual equivalence catalog, parameterized numeric
and string casts, session settings/collation, SQL header effects, view-definition
representations. See existing SQL normalization experiments for individual
observations. A parser round trip alone is not a semantic oracle.

### C. Lag tolerance and template comparison (review 11, 12)

**Policies intentionally not changed without an oracle.** Freshness currently
compares `current_input > recorded_input + tolerance_seconds*1000`; official docs
suggest elapsed age since build can matter instead. Introduce an injectable
clock and characterize unchanged/changed inputs before/at/after the lag window,
negative/extreme tolerance, logic changes within the window, ANY/ALL with mixed
changes, and old builds with small timestamp deltas. Do not change the rule based
only on prose or an oracle that copies the current implementation.

`compare_unrendered_code=true` remains exclusive template-body comparison,
while false uses rendered SQL. Fingerprints separate both modes, include extras,
context and dependency definitions, and reject empty body evidence. Existing
capture proves one changed-rendered/unchanged-template SKIP, not the complete
truth table. Test all four template/rendered changes with the flag both ways,
mode transitions, macros/vars and actual Python/Rust clients. `SQLExecution` in
Record has no comparison flag: it currently records rendered mode; decide how
template reuse should consume batch history once hosted behavior is established.

### D. Full public client feature set (review 8, 9 and coverage matrix)

- Implement selectors (criteria enums and project/target scoping) and deferral
  (profile/project/target/node mapping) using authoritative state; require
  nonempty response fixtures. Both currently return UNIMPLEMENTED.
- Implement volatile-function evaluation and `transformed_nodes_by_query`,
  including client execution/confirmation of transformed expressions. Current
  BUILD fallback does not reproduce the transformation protocol.
- Implement SQL-submit automatic `ready_to_clone`, query hash metadata,
  `clone_required_last_modified_epoch`, meaningful rejection reasons and complete
  description strings across all branches. Submit currently only BUILD/SKIP.
- Explain RPCs still return empty defaults; ClientValidation accepts every
  version; speculative SQL always returns Undecided. Establish exact supported
  client versions and behavior instead of advertising general coverage.
- ClientTelemetry is not served; investigate whether supported clients require
  its RPCs (including session methods). Health reports serving but is not a
  database-readiness check. Treat telemetry/observability as lower priority
  unless required for client compatibility.
- Header-based org selection is isolation, **not authentication**. Auth/TLS/
  trusted identity mapping remain deployment work. UI uses one configured org,
  not authenticated multi-user tenant selection. Bound asynchronous capture
  work/retention before enabling it under heavy load.

### E. Independent testing foundation

The pure freshness properties encode current policy assumptions and only test
an already selected candidate. They do not prove store selection, physical
validity or official behavior. Build an independent state machine at the gRPC
boundary with modelled physical contents/version, scopes, builds, confirms,
records, failures and clone transitions. Generate operation sequences; assert
that SKIP preserves the independently expected contents/results. Include
metadata permutations, restart, malformed requests, nulls, future/extreme
epochs, and migration from old confirmed rows. Extend arbitrary UTF-8 testing to
the public gRPC path and numeric/cast negative cases.

Replay now loads all fixture files and preserves outcome results/table type/
failed-clone/runtime fields, correlating confirms by request ID. It never
manufactures pre-capture history. Leading SKIPs and uncorrelated confirms are
reported as unknown baseline; clone/deferral gaps are counted. Complete causal
SKIP responses are compared after stripping only opaque IDs. Rebuild variants
are always checked, including after previous confirms. Organization metadata is preserved for stateful submit/confirm calls. Full rebuild response
fields still need equivalent initial preconditions; first-build descriptions
cannot be compared to a hosted prior-history rebuild without that history.
Six current causal mismatches are pinned in [golden/replay_gaps.json](../golden/replay_gaps.json)
(zero-based entry indices): c1_config_vs_logic entries 2/3 change dependency
SELECT text to view DDL (and use lag-specific descriptions); the second general
corpus entries 13–16 reuse data tests across environments. They must currently
BUILD locally; the replay checks that fallback and fails if an exemption becomes
unused or resolved. They are not counted as hosted agreement. Keep any explicit known mismatch narrowly keyed to a fixture entry with its
reason, never exempt a whole response family silently.

The opt-in differential fuzz tool remains **empty-history discovery**, comparing
variants only. It does not establish stateful conformance, even at zero
reported divergences. Build equivalent scenario state in both services,
compare complete responses/errors after ID remapping, save seeds and outcomes,
shrink stateful divergences, and distinguish history mismatch from genuine
policy mismatch. Transport/decode failures now fail the run. Do not run hosted
fuzzing while Snowflake/hosted validation is unavailable.

## Research sources for the next agent

The [review](correctness-review-2026-10-10.md) includes detailed source links and
feature coverage. Re-read primary sources when changing behavior:

- [Lag tolerance](https://docs.getdbt.com/reference/resource-configs/lag-tolerance)
  and [dbt State](https://docs.getdbt.com/docs/deploy/dbt-state-about).
- [Public Rust client at reviewed commit](https://github.com/dbt-labs/dbt/tree/a0fc53e6797b135ce8d4fea99c581ea5116803ac):
  `crates/dbt-tasks-core/src/run_cache/run_cache_service.rs` (metadata, view
  traversal, transformations, cached tests, clone fallback), `run_cache_request.rs`,
  and the vendored protobuf definitions.
- [Public Python client at reviewed commit](https://github.com/dbt-labs/dbt-state/tree/b0f3030c90dd0766f2d1625b3305829d881fc2ae):
  `clients/dbt_state/src/dbt_state/run_cache.py` (label identity, target metadata,
  seed semantic extras and test outcomes).

Wire schemas match the reviewed Rust client exactly; older Python schemas and
identity representations differ. Pin the supported client versions in a future
compatibility matrix, not just the proto commit.

## Validation

Run the following with Postgres at DATABASE_URL (default documented in
[development.md](development.md)); none require Snowflake or hosted access:

```bash
cargo test --workspace
DATABASE_URL=postgres://invalid:invalid@127.0.0.1:1/invalid cargo test -p dbt-state-server --lib
cargo fmt --all --check
cargo clippy --workspace --all-targets --features fuzz -- -D warnings
```

Final checks passed with Rust 1.98.0 and disposable Postgres 17:

- Workspace: **125 tests passed**, including **14 new offline gRPC regressions**.
- Server library: **50 tests passed** with DATABASE_URL pointing to an unreachable
  port, verifying that this target does not require a database.
- Formatting and Clippy (all targets, including the fuzz feature): passed.
- Replay was also rerun after adding a confirmation-ID echo assertion. All 14
  files / 102 entries were accounted for: 76 checks against hosted evidence
  (rebuild variants or complete applicable responses), six pinned local
  fallbacks, 14 entries with unknown baseline, and six clone/deferral entries
  outside lifecycle comparison. RegisterClone's captured shape/DDL is checked
  separately. The six fallbacks are not hosted agreement.
- No Snowflake or hosted requests were made. The disposable database was removed.

Re-run the commands above against a new local Postgres
instance before extending behavior; do not use the removed temporary database
or untracked `/tmp` research clones as required inputs.
