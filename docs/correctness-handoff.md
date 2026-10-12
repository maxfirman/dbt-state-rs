# Correctness handoff after offline hardening

Updated 2026-10-12. Read this before changing decisions. The goal remains a
conformant drop-in dbt State replacement; **the current server does not yet meet
that goal**, but a growing set of behaviors is now live-verified.

An earlier phase hardened the server offline (unsafe-skip fixes, fingerprinting,
test/seed identity, org isolation — summarized in the table below). A later
phase (2026-10-11) characterized the **clone** and **lag/freshness** paths
against the REAL hosted service (api.state.dbt.com, dbt Fusion client, Snowflake
`prod_demo`/`dev_clone`) via the recording proxy, and implemented/fixed them.
Those live-verified sections are marked inline (see §A and §C). Passing local
tests establish the contracts below; live-verified items additionally reproduce
captured hosted responses, but complete hosted equivalence is still not claimed.

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
| 9: empty selectors/deferral | **Selectors IMPLEMENTED (live-verified 2026-10-11, see §D).** `GetStateSelection` reproduces the hosted `state:*` verdicts via a recorded `node_states` store + pure `selector::classify`. **Deferral IMPLEMENTED (live-verified 2026-10-12, see §D):** `ResolveDeferredRelations` resolves each node to its defer-to-env physical FQN from `node_states` (migration 0010 adds `target_table`), omitting unbuilt nodes. | `selector_conformance.rs`, `selector_offline.rs`, `selector.rs` unit tests; `deferral.rs` + fixture `deferral_resolve.jsonl` |
| 10: invalid clone DDL | Reject view sources and unknown dialects before inserting pending history. Preserve the captured Snowflake table DDL and both response mirrors. | Exact captured table DDL, view/unknown-dialect errors and no pending rows |
| 13: tenant leaks/logged password | Scope every UI database read to configured `DBT_STATE_UI_ORG_ID` (default `local`); remove startup DSN logging. | Two organizations with identical project names; all UI read methods including direct foreign IDs |
| 12: lag tolerance (live-verified 2026-10-11) | Anchor data-freshness drift on the node's OWN last-build epoch (`upstream_epoch > own_build_epoch`), replacing the `recorded_upstream + tolerance` timestamp-distance rule. `freshness_tolerance_seconds` verified NOT to absorb the gap and intentionally unapplied. Data tests have no own object, so they anchor on the RECORDED UPSTREAM epoch instead (review #2 finding 1, 2026-10-12). See §C. | `decision.rs` `freshness_*` unit tests, proptest oracle, `golden/fixtures/lag_freshness_own_build.jsonl`, `crates/harness/tests/lag_freshness.rs` |
| 10 (extended, live-verified 2026-10-11): clone lifecycle, rejection, cross-name fallback | Reproduce register→confirm→cross-environment reuse (schema-abstracted, logic-sensitive); reject `allow_clones=false` (`clone_chain_depth_limit=0` → `unable_to_clone`, crr=10); emit SQL-submit `ready_to_clone` for an equivalent under another name. Migration 0008 adds `node_namespace_logic_hash` + `clone_source_table`. See §A. | `clone_lifecycle.rs`, `clone.rs::register_clone_rejected_when_allow_clones_false`, `clone_submit_fallback.rs`, fixtures `clone_rejected_allow_clones_false.jsonl` + `clone_submit_equivalent_fallback.jsonl`, `sql_norm`/`fingerprint` unit tests |
| 12b (live-verified 2026-10-11): require_fresh_data_from + is_stale | Verify ALL rebuilds only when every direct parent drifted (SKIP otherwise, even at lag 0); fix `is_stale` to be false on every execute and true only on a tolerated-stale skip. Record evidence that `lag_tolerance` CAN gate under ANY policy (exact rule not yet isolated; engine keeps the safe own-build anchor). See §C. | `decision.rs` `*_is_stale_*` tests, `crates/harness/tests/lag_policy.rs`, fixture `lag_multi_upstream_policy.jsonl` |
| Testing/toolchain | Pin Rust 1.98; capture DB test out of `--lib`; reject malformed trace lines; replay all fixtures with full captured confirms and ID correlation; fail discovery fuzz tool on transport/decode failures or divergences; add live `experiments/` capture drivers (clone/lag). | Workspace tests, no-DB library tests, strict trace parse test, full causal response comparisons, fuzz compilation |
| **#2 finding 1 (live-verified 2026-10-12): data-test upstream freshness** | A data test (execution_type=8) is confirmed with NO own object epoch; the engine returned `should_rebuild=false` for it BEFORE comparing upstreams, reusing a stale cached pass/fail after the tested data changed. Now `decision.rs::data_test_upstream_freshness` anchors a data test's freshness on the RECORDED UPSTREAM epoch (per input, strict `>`, ANY-style; unknown/absent baseline rebuilds). VERIFIED LIVE (`experiments/data_test_freshness{,_control}.sh`): `dt_test` over `dt_up` executes when `dt_up`'s epoch advances and skips when unchanged. | 6 `decision.rs` unit tests, `crates/harness/tests/data_test_freshness.rs`, fixture `data_test_upstream_freshness.jsonl` (CHARACTERIZATION-ONLY) |
| **#2 finding 5 (2026-10-12): RecordExecutions logical-state atomicity** | `RecordExecutions` wrote each record's `node_states` (selector/deferral) row inline — before full batch validation and outside the batch transaction — so a rejected (INVALID_ARGUMENT) or DB-failed batch leaked logical state. Now node-state upserts are collected during row build (fail-fast, no writes) and published only AFTER the executions batch commits. Error-path atomicity only; every successful path is behavior-preserving (conformance replay still passes). The companion publication-point question (an UNCONFIRMED/failed submit being immediately deferrable) is left unchanged and documented open — it needs live pending/failed-build characterization. | `crates/harness/tests/correctness.rs::rejected_record_batch_does_not_leak_node_state` |

`decision::decide` remains pure. Store queries remain organization scoped.

## Compatibility and upgrade effects

Migration `0007_execution_results.sql` adds nullable `project_id`, binary
`execution_results`, and an index on current confirmed targets. Results use the
custom protobuf Struct bytes, preserving int64 and non-finite double fields
without JSON conversion. Existing numeric input epochs deserialize as `Some`;
new null epochs preserve unknown evidence. Corrupt rows cause an explicit error.

Migration `0008_cross_env_reuse.sql` adds nullable `node_namespace_logic_hash`
(the schema-abstracted cross-environment logic identity) and `clone_source_table`
(clone provenance), plus a namespace+logic index. Both are populated on new
executions only; pre-0008 confirmed rows have them NULL, so cross-environment
clone adoption simply does not fire for them (they fall back to the existing
strict same-target path — safe, no behavior change for legacy rows).

Migration `0009_node_states.sql` adds a new `node_states` table (keyed by
org/project_id/`target` env/node_unique_id) for the `state:*` selector
comparison store — SEPARATE from the `executions` decision ledger. It is
populated going forward on every Submit/Record that carries a `dbt_node_state`;
it has no effect on the decision path and legacy executions are unaffected. A
`state:*` selector against an env with no recorded node state simply reports
every node as `new`/`modified` until a build (in that `defer_to_target` env)
seeds it.

Migration `0010_node_state_fqn.sql` adds a nullable `target_table` column to
`node_states`, populated going forward on every Submit/Record that carries both
a `dbt_node_state` and a physical target. It powers deferral
(`ResolveDeferredRelations` resolves a node to its defer-to-env FQN); legacy
rows have it NULL and simply do not resolve. No effect on the decision path.

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
State selectors are now implemented (see §D). Deferral callers still receive
UNIMPLEMENTED; this is an explicit feature gap and may abort client operations
that previously succeeded incorrectly.

## Remaining work, in priority order

### A. Physical state, concurrency and clone provenance (review 1, 7, 10)

**Clone lifecycle, eligibility, rejection and the SQL-submit cross-name fallback —
LIVE-VERIFIED + IMPLEMENTED (2026-10-11).** Characterized against the real service
(api.state.dbt.com, Fusion client, Snowflake `prod_demo`/`dev_clone`) via the
recording proxy; see `experiments/clone_experiment.sh`,
`experiments/clone_logic_sensitivity.sh`, and the fixtures
`clone_rejected_allow_clones_false.jsonl` + `clone_submit_equivalent_fallback.jsonl`.

- **Clone eligibility SPLITS by materialization.** Incremental models and
  snapshots clone via the `Clone.RegisterClone` RPC (the `pre_clone` path, default
  `if_missing`). Table materializations clone via `SQL.SubmitEnrichedSQL` →
  `ready_to_clone` (the cross-name fallback below). Clones require profile
  `defer_to_target` set and the dev object absent.
- **Clone lifecycle (register → confirm → reuse) now reproduced.** After
  `RegisterClone` + `ConfirmExecution`, a later submit for the cloned target
  SKIPS when its logic matches — and reuse is CROSS-ENVIRONMENT (schema-
  abstracted) and LOGIC-SENSITIVE: a dev submit differing from the prod source
  only in the schema qualifier skips; a genuine SQL change executes
  (`LATEST_QUERY_HASH_NOT_MATCH`=8). Implemented via a schema-abstracted
  `node_namespace_logic_hash` (`fingerprint::namespace_logic_hash`,
  `sql_norm::normalize_sql_namespace_abstracted`) recorded on every execution,
  plus clone provenance columns (`clone_source_table`, migration 0008). The
  clone-origin row inherits the source's namespace logic identity at
  RegisterClone (`store::find_source_ns_logic_hash`) and is adopted by a later
  submit only when it is clone-origin and the abstracted logic matches
  (`store::find_latest_confirmed_target`, gated to clone-origin). Pinned by
  `crates/harness/tests/clone_lifecycle.rs`.
- **`allow_clones: false` rejection.** The client encodes it as
  `clone_chain_depth_limit = 0`; `RegisterClone` returns `unable_to_clone`
  (decision=1 READY_TO_EXECUTE, `clone_rejection_reason=10`
  CLONE_CHAIN_LIMIT_EXCEEDED, empty deprecated oneof, no pending row) and the
  client builds normally. Implemented in `services.rs::register_clone`; pinned by
  `clone.rs::register_clone_rejected_when_allow_clones_false`.
- **SQL-submit cross-name `ready_to_clone` fallback.** A submit for an absent
  target with an equivalent (same namespace + schema-abstracted logic +
  `execution_type`) under another physical name returns `ready_to_clone` cloning
  it in (decision=3, `skip_rejection_reason=1` TARGET_TABLE_MISMATCH,
  `clone_required_last_modified_epoch`=source build epoch, source runtime echoed).
  Implemented via `store::find_clone_equivalent` + `services::
  submit_ready_to_clone_response`; gated to real SQL submits with a known
  namespace, `allow_clones != false`, and `execution_type` match. Pinned by
  `crates/harness/tests/clone_submit_fallback.rs`.
- **Historical clone-state must reflect CURRENT physical contents — FIXED
  (review issue #1 P0#1/P0#2, 2026-10-12).** Two clone-state lookups could treat
  a historical execution as evidence of an object's current contents:
  `store::find_clone_equivalent` filtered by (namespace, logic, type) BEFORE
  reducing to the newest row per source, so an obsolete A-logic row for a source
  since rebuilt with B could resurrect stale SQL as a clone source (A→B→A); and
  `store::find_latest_confirmed_target` applied `clone_source_table IS NOT NULL`
  BEFORE selecting the latest row, so a cloned-then-rebuilt target still resolved
  to the old clone-origin row and could be ADOPTED into an unsafe SKIP. Fixed:
  `find_clone_equivalent` now reduces to the latest confirmed row PER target
  (`DISTINCT ON (target_table)`) and only then filters by logic+type;
  `find_latest_confirmed_target` selects the latest confirmed target row of any
  provenance (caller still requires clone-origin + logic match to adopt) and
  omits `execution_results` from that lookup (adoption never serves cached
  results, so a corrupt-but-irrelevant blob can't fail a cold miss). Pinned by
  `crates/harness/tests/clone_history_provenance.rs`.
- **The cross-name fallback fires OVER an existing target — LIVE-VERIFIED
  (2026-10-12, `experiments/p0_fallback_over_existing.sh`).** A table target
  submitted with logic L1 (absent → clone), rebuilt with L2 (execute), then
  submitted with L1 again while physically holding L2 is answered
  `ready_to_clone` AGAIN. The fallback discriminator is purely "does an
  equivalent for THIS logic exist under another name", NOT target existence or
  prior build history. An inferred `!target_has_confirmed_history` gate was
  therefore REMOVED (it contradicted hosted). The P0#2 fix only prevents the
  unsafe stale-clone-origin SKIP; the subsequent clone is correct. Fixture
  `golden/fixtures/clone_fallback_over_existing_target.jsonl` (characterization-
  only), pinned by `clone_submit_fallback.rs::hosted_fallback_fires_over_existing_target`.
- **Current physical state before ALL metadata filters — HARDENED (review
  issue #1 follow-up, 2026-10-12).** The P0 fixes picked the latest row per
  source before matching logic, but three lookups still applied SOME metadata
  filters before `ORDER BY confirmed_at`: a target's current row could be
  skipped and an older matching one returned. All three now select the current
  physical state FIRST, then filter:
  - `find_latest_confirmed_target` applied `execution_type`/`table_namespace`/
    `dialect` before the ORDER BY. **LIVE-VERIFIED client-reachable
    (`experiments/physical_state_exec_type.sh`):** a model switched
    `table`→`incremental` sends `execution_type` 1→3 for the same target; after
    the target holds an et=3 build, re-submitting et=1 of the same logic is
    answered `ready_to_clone`, NOT a skip reusing the stale et=1 clone-origin
    row. Fixed via a `current_state` CTE (latest row, any type) then post-filter.
    Fixture `golden/fixtures/clone_rebuild_execution_type.jsonl`
    (characterization-only).
  - `find_source_ns_logic_hash` applied `node_namespace_logic_hash IS NOT NULL`
    before the ORDER BY, so a source whose newest build stores no logic hash
    (unrendered-comparison or empty SQL — see the `ns_logic_hash` gate in
    `services.rs`) would inherit an OBSOLETE hash onto the clone-origin row. Now
    reads the source's latest row's hash directly (may be NULL → inherit none →
    later submit EXECUTEs; safe).
  - `find_clone_equivalent` reduced to latest-per-target WITHIN the requested
    namespace, so a target whose newest state moved to another namespace could
    surface its older same-namespace row as current. Now computes
    current-state-per-target across ALL namespaces, then filters by namespace.
    `table_namespace` is a stable adapter-level id (no target changes it across
    captured traffic), so this is an OFFLINE invariant with no hosted-parity
    claim (do-not-infer rule).

  All three pinned by `crates/harness/tests/clone_history_provenance.rs`
  (`clone_adoption_must_not_resurrect_row_of_different_execution_type` —
  live-verified; `clone_source_logic_inherit_must_use_current_source_state` and
  `clone_equivalent_must_reflect_current_namespace_of_source` — offline).

Remaining in this area: dropped/recreated sources mid-decision, server restart;
clone row/time limits (`clone_time_travel_limit`, `OUTSIDE_TIME_TRAVEL_
WINDOW`=9) and deeper chain limits (>0); `defer_enabled`/`is_defer_to_profile`
nuances. **Concurrency / late-confirmation / conflicting-batch determinism is
now PINNED (2026-10-12, `crates/harness/tests/concurrency.rs`).** Hosted behavior
under true concurrency is not establishable from a sequential capture (so these
are not hosted-parity claims), but the server's rule is made explicit and
deterministic: CURRENT PHYSICAL STATE = the execution with the newest
`confirmed_at` (tie-broken by id). Confirmation order — NOT submission order —
defines current state (reversed-confirm test); a late confirm deterministically
becomes current (late-confirm test); conflicting confirmed builds for one target
both persist and the last-confirmed wins (conflicting-rows test); and
Submit↔Record agree on the model fingerprint (parity test). This models "the
last build to finish writing the object wins" — the only provenance signal
available without stable warehouse object IDs (stronger lineage remains open).
**`allow_clones=false` suppression of the SQL-submit fallback is now
LIVE-VERIFIED (2026-10-12, `experiments/clone_experiment.sh table --no-clones`):**
the client encodes it as `clone_chain_depth_limit=0` on the SUBMIT (NOT via
`allow_clones`), and the hosted service, instead of cloning the equivalent,
returns `ready_to_execute` with `skip_rejection_reason=1` (TARGET_TABLE_MISMATCH),
`clone_rejection_reason=10` (CLONE_CHAIN_LIMIT_EXCEEDED) and
`decision_description="model was executed because the maximum number of nested
clones has been reached"`. The server now reproduces this exact shape (it
previously gated only on `allow_clones != Some(false)`, which the real client
never sets, and emitted a generic execute). Fixture
`clone_submit_allow_clones_false.jsonl`; pinned by
`clone_submit_fallback.rs::allow_clones_false_suppresses_fallback_with_chain_limit_shape`.
`failed_to_clone=true` (observed with "clone source freshness
unavailable") is still confirmed as a normal build — do not reject on the flag
alone. State selectors and deferral are implemented (§D).

Hosted/warehouse questions: late-confirmation ordering; warehouse clock
semantics; source-freshness enforcement; TABLE types beyond ordinary/transient.
Do not reinstate schema stripping for the STRICT same-target identity or a
namespace-only SKIP (the cross-env path CLONES, it never skips without proven
physical provenance). BigQuery/Databricks/Spark clone templates remain
unverified. Snowflake view cloning reports unsupported; a correct view-copy
needs the actual view definition/context, not `CREATE VIEW ... CLONE`.

### B. Dependency semantics and SQL equivalence (review 2, 4, 5)

**Negative coverage ADDED (2026-10-12, offline).** Dependency-identity
properties are now pinned in `crates/server/src/fingerprint.rs` tests: the
STRICT identity distinguishes a changed dependency definition and the physical
dependency name / resolution catalog / schema (same-target reuse cannot conflate
different physical inputs); the NAMESPACE-ABSTRACTED identity excludes the
physical dependency name/catalog/schema (so the same model's view in prod vs a
dev clone shares identity) BUT still distinguishes a genuine dependency LOGIC
change, a different dependency-set size, and a root projection change; dependency
ORDER is irrelevant in both; and template vs rendered modes never collide. SQL
equivalence negatives are pinned in `crates/server/src/sql_norm.rs` tests:
quoted/qualified function names and UDFs that shadow a builtin are NOT
synonym-folded; unknown functions, cast precision/scale and target-type
differences, TRY_CAST vs CAST, COLLATE, DISTINCT/LIMIT/ORDER-direction and JOIN
kind all stay distinct (the normalizer is syntactic, over-executing is the safe
default). Only the tiny verified synonym set folds.

Offline work remaining: retain a resolved dependency graph; distinguish view DDL
from its query; handle cycles, raw references outside
the DAG, and default resolution context. Add nested views,
transitive definition changes, missing/added columns with SELECT *. Exact
fingerprinting of supplied definitions does not
establish completeness of the supplied graph. `lenient_dependencies` is still
ignored; define its behavior from primary client/docs evidence and then capture
controlled cases. Builtin synonym rules should gain warehouse validation before
being expanded.

Hosted/warehouse questions: actual equivalence catalog, parameterized numeric
and string casts, session settings/collation, SQL header effects, view-definition
representations. See existing SQL normalization experiments for individual
observations. A parser round trip alone is not a semantic oracle.

#### Review #2 findings assessment (2026-10-12)

Review #2 raised 9 findings. Two were genuine and provable to the project's
standard and are FIXED (see the completed-changes table): finding 1 (data-test
upstream freshness, LIVE-VERIFIED) and finding 5's error-path atomicity (rejected
`RecordExecutions` batch must not leak logical state). The rest were triaged
against the AGENTS.md do-not-infer rule and are **documented OPEN** rather than
changed, because a clean live characterization could not be constructed without
guessing hosted policy:

- **Finding 2 (clone fallback freshness / stale same-target suppresses clone).**
  The SQL-submit cross-name clone fallback is already LIVE-VERIFIED as driven by
  schema-abstracted logic identity, NOT target existence
  (`clone_submit_equivalent_fallback.jsonl`, `clone_fallback_over_existing_target.jsonl`,
  §A). The review's freshness-guard claim is a local counterexample only ("hosted
  confirmation still needed"); isolating it needs two differently-named objects
  with controlled, differing upstream epochs in ONE adapter namespace — the
  decision there is governed by warehouse physical state the protocol does not
  expose. Open until a controlled live capture exists.
- **Finding 3 (namespace abstraction erases dependency identity: BLUE/RED,
  swapped V1/V2).** Schema abstraction is a DELIBERATE, live-verified
  cross-environment identity (§A; `fingerprint.rs`/`sql_norm.rs` live-comment
  tests). The counterexamples are within-namespace; the `table_namespace` is
  adapter-supplied (one value per warehouse/account), so a clean live
  contradiction is not constructible in jaffle-shop's single account. Open:
  needs a controlled multi-account/namespace capture to decide whether hosted
  binds each dependency to a resolved graph node.
- **Finding 4 (RegisterClone provenance / age reset / external source edit).**
  Offline invariant counterexamples; the real meaning of namespace transitions
  and clone-chain provenance needs live clone-chain characterization. Open.
- **Finding 5A (publication point: an UNCONFIRMED/failed submit is immediately
  deferrable).** The successful-build captures do not establish pending/failed
  policy; the publication point must be characterized live, not guessed. Open.
- **Finding 6 (template truth table; wildcard-view column evidence).** The review
  states "reproduced locally; hosted confirmation still needed"; `fingerprint.rs`
  already notes the template truth table is unresolved. Open (see §C).
- **Finding 7 (lag/graph/ordering, physical identity across accounts).** Already
  documented open / characterization-only in §C. No new genuine bug.
- **Finding 8 (recording-proxy fidelity).** A test-oracle improvement (forward
  redacted client metadata, capture errors/trailers/raw bytes, fail on capture
  loss), not a server reuse-safety or conformance defect. Tracked as harness
  work; it does not change any decision.
- **Finding 9 (wire/client compatibility holes, auth = trusted-header scoping).**
  Explicitly deployment requirements and known documented gaps (see §D).

### C. Lag tolerance and template comparison (review 11, 12)

**Lag/data freshness — RESOLVED (live-verified 2026-10-11).** The freshness
rule was characterized against the real service (api.state.dbt.com, dbt Fusion
client, Snowflake `prod_demo`) via the recording proxy. A model with
`lag_tolerance=5m` over an upstream whose physical table was recreated in place
with identical content (advancing its modification epoch WITHOUT any dbt SQL
change, isolating pure data freshness from the upstream-logic-change bypass)
rebuilds iff:

    max_upstream_epoch > own_last_build_epoch

That is: an upstream newer than the object we last built forces a rebuild. The
comparison anchor is the node's OWN last-build epoch — NOT the upstream epoch
recorded at that build (the previous implementation's anchor), and NOT a
wall-clock build-age gate (the lag_tolerance doc's prose and the pre-hardening
review both hypothesized the latter; neither matched the observed behavior).
Critically, `freshness_tolerance_seconds` did NOT absorb the gap: a 300s
tolerance still rebuilt on an upstream only tens of seconds newer than the
build, and `is_stale=false` on every captured entry (staleness is a separate
axis). The previous `current_input > recorded_input + tolerance` rule could SKIP
when the upstream-delta was within tolerance even though the hosted service
rebuilt — the UNSAFE (stale-serving) direction.

`decision.rs::is_stale` now anchors on `prev.last_modified_epoch` (own build
epoch), with ANY/ALL preserved and `freshness_tolerance_seconds` intentionally
NOT applied (applying it would widen the skip window and reintroduce the unsafe
skip; the only safe direction when uncertain is to execute more). Data-test
nodes (`execution_type=8`) have no own object and no build epoch; they are
governed by identity/logic match, not this axis. Pinned by the unit tests in
`decision.rs` (`freshness_*`), the `decision_proptest.rs` oracle, the golden
fixture `golden/fixtures/lag_freshness_own_build.jsonl`, and the characterization
test `crates/harness/tests/lag_freshness.rs`.

Remaining lag questions (not blocking):

- **`require_fresh_data_from` ANY/ALL — VERIFIED LIVE 2026-10-11 (multi-upstream).**
  A model cross-joining two independent upstreams, each recreated in place to
  advance only its epoch, was characterized under both policies (fixture
  `golden/fixtures/lag_multi_upstream_policy.jsonl`,
  `experiments/lag_experiment.sh`). ALL rebuilds only when EVERY direct parent
  drifted; with one-of-two drifted it SKIPS. This holds even at
  `lag_tolerance=0`, proving the ALL skip is PURE POLICY (not lag absorption).
  Matches the engine's `drifted == considered` rule. Pinned by
  `crates/harness/tests/lag_policy.rs` and `decision.rs` unit tests.

- **`is_stale` reporting — VERIFIED LIVE + FIXED 2026-10-11.** The hosted service
  reports `is_stale=false` on EVERY execute (including an upstream-drift
  rebuild — lag fixture entry 5 and the ANY captures), and `is_stale=TRUE` only
  on a tolerated-stale SKIP (ALL policy with some parents fresh: the reused
  result knowingly serves slightly stale data). The engine previously hardcoded
  `is_stale=false` on skips and set `is_stale=true` on drift executes — both
  wrong. `decision.rs::evaluate_freshness` now returns `{should_rebuild,
  drift_present}`; `Verdict::Skip` carries `is_stale = drift_present` and every
  execute reports false. Pinned by `decision.rs` (`*_is_stale_*` tests) and
  `lag_policy.rs`.

- **`lag_tolerance` gating — NOW PROVEN TO EXIST (open: exact rule).** The prior
  conclusion that `lag_tolerance` NEVER gates was overturned. The multi-upstream
  capture contains ANY-policy SKIPS with `is_stale=true` whose
  `decision_description` is *"model was a no-op because its query is up to date
  and its data is within lag tolerance"*, where an upstream is newer than the
  own build by LESS than `lag_tolerance` (e.g. +53.7s under a 300s tolerance).
  So `lag_tolerance` CAN suppress a rebuild under ANY policy. However, multiple
  controlled single-run experiments (`experiments/lag_isolate.sh`,
  `lag_buildage.sh`) could NOT reproduce it: a fresh build followed by an
  upstream bump always rebuilt on the first drifted submit regardless of
  tolerance or build-age, because each drifted submit immediately re-advances
  the build epoch past the upstream. The confounding variable (which clock the
  tolerance is measured against — wall-clock since upstream change, wall-clock
  build-age, or a server-side "last rebuild" ledger that persists across submits
  without a client confirm) is NOT yet isolated. The engine therefore STILL uses
  the own-build anchor (rebuild on any ANY-drift) — the SAFE direction (it
  executes more, never serves staler data than hosted). DO NOT implement a lag
  gate until the exact trigger is isolated from a clean, un-confounded capture;
  a wrong gate reintroduces unsafe skips. The evidence is pinned by the
  `hosted_policy_is_stale_truth_table` assertion in `lag_policy.rs` (the
  ANY-within-lag-tolerance skip must remain in the corpus).

- Direct-parent vs leaf propagation of the upstream-logic-change bypass remains
  uncharacterized.

The own-build anchor itself remains verified for the ANY path:
`golden/fixtures/lag_freshness_own_build.jsonl`, `decision.rs` `freshness_*`
tests, `decision_proptest.rs` oracle, `crates/harness/tests/lag_freshness.rs`.

`compare_unrendered_code=true` remains exclusive template-body comparison,
while false uses rendered SQL. Fingerprints separate both modes, include extras,
context and dependency definitions, and reject empty body evidence. Existing
capture proves one changed-rendered/unchanged-template SKIP, not the complete
truth table. Test all four template/rendered changes with the flag both ways,
mode transitions, macros/vars and actual Python/Rust clients. `SQLExecution` in
Record has no comparison flag: it currently records rendered mode; decide how
template reuse should consume batch history once hosted behavior is established.

### D. Full public client feature set (review 8, 9 and coverage matrix)

- **State selectors — LIVE-VERIFIED + IMPLEMENTED (2026-10-11).**
  `SelectorService.GetStateSelection` (dbt State-powered `state:*` selectors) is
  implemented and conformant. Characterized against the real service
  (`api.state.dbt.com`, jaffle-shop, Snowflake `prod_demo` baseline, selector
  queried from `dev_clone` whose `defer_to_target: prod_demo`) via the recording
  proxy (`experiments/selector_experiment.sh`); fixture
  `golden/fixtures/selector_state_selection.jsonl` (67 distinct queries across
  all 10 criteria + every change type), pinned by
  `crates/harness/tests/selector_conformance.rs` (set-equal replay) and
  `selector_offline.rs` (record-path + org/project/target scoping). Verified
  semantics (see protocol.md): the server records the client-supplied
  `DbtNodeData` hashes verbatim on every Submit/Record (migration 0009
  `node_states`, keyed by org/project_id/`target` env/node_unique_id) and does a
  pure hash comparison. `new`/`old` and `modified`/`unmodified` partition the
  sent set; `modified = new ∪ body ∪ configs ∪ persisted_descriptions ∪ macros ∪
  contract` with **`relation` excluded**; a new node matches every `modified.*`;
  untracked node types are always new/modified; `node_body_hash` is a raw
  (whitespace-sensitive) file hash. Pure engine in `crates/server/src/selector.rs`
  (`classify`, 13 unit tests). Open: the client batches ≤10,000 nodes/request —
  the server handles a single large request; multi-batch union is client-side.
  **Baseline is LAST-WRITE-WINS — LIVE-VERIFIED (2026-10-12,
  `experiments/selector_last_write_wins.sh`).** Re-recording a node in the
  defer-to env moves the comparison baseline to the NEWEST recorded execution: a
  node rebuilt there is then reported `modified` against its own previous
  content. This confirms `store::upsert_node_state`'s latest-wins
  `ON CONFLICT DO UPDATE` is hosted-faithful for sequential semantics. Fixture
  `golden/fixtures/selector_last_write_wins.jsonl`; pinned by
  `selector_offline.rs::selector_baseline_is_last_write_wins` and
  `selector_conformance.rs::last_write_wins_baseline_reproduces_hosted`.
  Concurrent/racing-record ordering is NOT establishable from a capture and is
  intentionally not implemented as a guard (would be speculative); the
  last-arriving write wins by design. **Best-effort write is now OBSERVABLE:**
  `record_node_state` increments `AppState::selector_write_failures` on a store
  error (still never failing the decision path), so a silent baseline-staleness
  divergence is a surfaceable metric. Pinned by
  `selector_offline.rs::selector_state_write_failures_are_observable`.
  **Deferral** (`ResolveDeferredRelations`) is now IMPLEMENTED + LIVE-VERIFIED
  (2026-10-12, `experiments/deferral_experiment.sh`). Building from `dev_clone`
  (`defer_to_target: prod_demo`) and selecting only a downstream node defers its
  upstreams; the hosted service answers
  `ResolveDeferredRelations(profile, target=prod_demo, project, [uids])` with a
  map `node_unique_id -> "CATALOG"."SCHEMA"."OBJECT"` for each node built in that
  env (the submit `target_table`). The earlier empty-map captures were simply
  the not-yet-built case. The server records `target_table` in `node_states`
  (migration 0010) and resolves (org, project_id, target env, uids) → FQN,
  omitting unbuilt nodes. Pinned by `crates/harness/tests/deferral.rs` and the
  fixture `golden/fixtures/deferral_resolve.jsonl`.
- Implement volatile-function evaluation and `transformed_nodes_by_query`,
  including client execution/confirmation of transformed expressions. Current
  BUILD fallback does not reproduce the transformation protocol.
- SQL-submit `ready_to_clone` is now IMPLEMENTED for the cross-name equivalence
  case (see §A: `clone_required_last_modified_epoch`, TARGET_TABLE_MISMATCH
  rejection, source runtime echo). Still remaining: `query_hash_metadata_info`
  on `ready_to_execute`, `transformed_nodes_by_query`, and the complete rejection
  reason / description matrix across every submit branch (we populate the
  verified ones; others are the conservative BUILD/SKIP/CLONE set).
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
  **Hardening ADDED (2026-10-12):** `DBT_STATE_REQUIRE_ORG_HEADER=true` makes the
  server REJECT (`UNAUTHENTICATED`) any request lacking a non-empty
  `x-organization-id` header instead of silently bucketing it into the shared
  `local` tenant. Off by default (preserves the insecure local-dev flow where
  the client omits the header). This enforces org SCOPING PRESENCE — it still
  does NOT authenticate the claimed org against a trusted bearer token, which
  remains the deployment-side work (OAuth/TLS/trusted-identity). Pinned by
  `crates/harness/tests/auth.rs` and a `config.rs` unit test.

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

Re-read primary sources when changing behavior:

- [dbt State overview](https://docs.getdbt.com/docs/deploy/dbt-state-about),
  [dbt State configs](https://docs.getdbt.com/reference/resource-configs/dbt-state-configs)
  (the reuse → clone → build ladder; `lag_tolerance`, `require_fresh_data_from`,
  `pre_clone`, `allow_clones`, `defer_to_target`, `compare_unrendered_code`,
  `evaluate_volatile_sql`), and
  [deferral](https://docs.getdbt.com/docs/deploy/dbt-state-deferral).
- [Public Rust client at reviewed commit](https://github.com/dbt-labs/dbt/tree/a0fc53e6797b135ce8d4fea99c581ea5116803ac):
  `crates/dbt-tasks-core/src/run_cache/run_cache_service.rs` (metadata, view
  traversal, transformations, cached tests, clone fallback), `run_cache_request.rs`,
  `crates/dbt-state/src/run_cache_defer.rs`, and the vendored protobuf definitions.
- [Public Python client at reviewed commit](https://github.com/dbt-labs/dbt-state/tree/b0f3030c90dd0766f2d1625b3305829d881fc2ae):
  `clients/dbt_state/src/dbt_state/run_cache.py` (label identity, target metadata,
  seed semantic extras and test outcomes).
- Live capture workflow: [harness.md](harness.md) + the `experiments/` drivers
  (`clone_experiment.sh`, `clone_logic_sensitivity.sh`, `lag_experiment.sh`,
  `lag_isolate.sh`). Each isolates one behavior and only mutates `CDO_DEMO_DB`.

Wire schemas match the reviewed Rust client exactly; older Python schemas and
identity representations differ. Pin the supported client versions in a future
compatibility matrix, not just the proto commit.

## Validation

The offline suite needs only Postgres at DATABASE_URL (default in
[development.md](development.md)); none of it requires Snowflake or hosted access:

```bash
cargo test --workspace
DATABASE_URL=postgres://invalid:invalid@127.0.0.1:1/invalid cargo test -p dbt-state-server --lib
cargo fmt --all --check
cargo clippy --workspace --all-targets --features fuzz -- -D warnings
```

Last run (Rust 1.98, disposable Postgres 17; 2026-10-12):

- Workspace: **214 tests passed** across the workspace (adds this session's
  review-#2 fixes: 6 data-test-freshness decision unit tests, the
  `data_test_freshness.rs` harness test, and the
  `rejected_record_batch_does_not_leak_node_state` atomicity test — on top of
  the earlier clone-provenance, selector last-write-wins + observability,
  deferral, allow_clones=false, concurrency determinism, dependency/SQL
  negatives, auth-hardening, and physical-state-before-filters work).
- Server library: **95 tests passed** with DATABASE_URL pointing to an
  unreachable port, verifying the engine needs no database (adds the 6 new
  `decision.rs` data-test-freshness unit tests).
- Formatting and Clippy (all targets, fuzz feature, `-D warnings`): passed.
- Conformance replay: 17 fixtures causally replayed; the characterization-only
  captures (`lag_multi_upstream_policy`, `selector_state_selection`,
  `clone_fallback_over_existing_target`, `selector_last_write_wins`,
  `deferral_resolve`, `clone_submit_allow_clones_false`,
  `clone_rebuild_execution_type`, `data_test_upstream_freshness`) are excluded
  and validated by their dedicated tests. Clone rejection and SQL-submit clone
  fallback entries are counted as replay gaps and asserted by their dedicated
  tests (`clone.rs`, `clone_submit_fallback.rs`). The six pinned local fallbacks
  in [golden/replay_gaps.json](../golden/replay_gaps.json) remain exercised
  exactly once.

Live verification (clone + lag paths, §A and §C) was performed against the real
service and Snowflake demo warehouse via the recording proxy; those captures are
promoted into `golden/fixtures/`. The offline suite above does **not** make
hosted/Snowflake requests. Re-run the live `experiments/` drivers only against a
disposable demo schema (`CDO_DEMO_DB`) when re-characterizing.

Re-run the commands above against a new local Postgres
instance before extending behavior; do not use the removed temporary database
or untracked `/tmp` research clones as required inputs.
