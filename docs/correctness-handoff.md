# Open questions and known divergences

This page lists what is not settled: hosted behavior we have not characterized,
differences from hosted that the tests pin, and gaps that block a production
deployment. Read it before changing decision logic. The wire contract and the
server's current behavior are in [protocol.md](protocol.md); validation commands
and the evidence rules are in [testing.md](testing.md).

Two goals are kept apart (see [GLOSSARY.md](../GLOSSARY.md)):

- **Hosted conformance**: agreement with the official dbt State service's
  observable behavior for a given client, warehouse and feature scope.
- **Reuse safety**: reusing a result satisfies the workload's declared logic,
  freshness and isolation requirements.

When the two conflict, or hosted behavior is unknown, the server chooses reuse
safety: it builds. Over-execution is a conformance gap; serving a wrong result
is a defect.

## Reuse invariants

Do not weaken these without new evidence.

- **Current state before identity.** Within the physical scope (organization,
  warehouse family, `table_namespace`, target relation), the server selects the
  latest confirmed build first and only then compares fingerprints, execution
  type and dialect. An older matching build never stands in for a relation
  overwritten since. When the latest build has the submit's identity under
  another execution type, the rebuild is judged against the latest build of
  the submit's type, which is never reused
  ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md#execution-type-change)). See
  [architecture.md](architecture.md#physical-scope-rule).
- **Confirmation order defines current state.** The current build is the one
  with the newest confirmation (ties broken by row id), not the newest submit
  (`crates/harness/tests/concurrency.rs`).
- **Target relation as built.** Reuse needs exactly one known epoch for the
  target relation with `current_epoch <= confirmed_epoch` (a later epoch is
  accepted only with `ignore_external_modifications`). Missing, duplicated
  or unknown metadata builds. This assumes the confirmation epoch is an upper
  bound for the build's warehouse write time; timestamps alone cannot prove
  object lineage.
- **Unknown evidence builds.** Unknown or duplicated upstream epochs, a changed
  set of physical input names, empty or duplicated dependency definitions and
  corrupt stored evidence (`INTERNAL`) never become a reuse. A node's own
  relation is not an input: a model's own target, and a `store_failures`
  data test's audit relation when the request identifies it exactly (#88).
- **Versioned fingerprints.** Each fingerprint starts with a version tag
  (`crates/server/src/warehouse.rs`, `fingerprint.rs`). A change to identity
  semantics takes a new tag, so older State history cold-misses and rebuilds
  once. Never rehash old rows in place
  ([ADR 0004](adr/0004-shared-sql-identity-with-dialect-rules.md)).
- **Freshness.** Drift is measured against the epoch recorded for each upstream,
  an Upstream producer change always rebuilds, and Lag tolerance compares Build
  age only ([ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md)).
- **No cross-target SKIP.** A `table_namespace` match alone never justifies a
  SKIP. Cross-target reuse is a Result copy on Snowflake (and the DuckDB copy
  subset) only, and a submit-time Result copy is FULL only (#101).
- **Copy adoption needs the copy's logic.** A later submit reuses a
  RegisterClone copy only when its schema-abstracted logic equals the logic
  the copy inherited from its source build, directly or with the submit's
  Target relation schema read as the copy source's schema (Snowflake only,
  `fingerprint::rehomed_copy_logic_hash`, #93). Either way each dependency's
  name stays bound to its definition
  (`fingerprint.rs::rehomed_identity_keeps_dependency_bindings`). No
  persisted fingerprint changed. A submit of another execution type with that
  logic is never reused: it is an execution type change (2 / 2) judged
  against the copy, as hosted does (`explain_copy_logic_mismatch.jsonl`
  `kf4`, #104).
- **Submit-time equivalents read in another schema.** The Result copy
  fallback finds an equivalent by the submit's schema-abstracted logic, or by
  that logic with the submit's Target relation schema read as another schema
  the request names (the #93 rule, `fingerprint::rehomed_copy_logic_hashes`,
  Snowflake only, #107). A candidate found the second way must have its
  Target relation in that schema: one elsewhere read other relations. Each
  dependency's name stays bound to its definition, so a dev dependency whose
  definition differs from the prod build's is not an equivalent
  (`clone_submit_fallback.rs::a_submit_reading_a_fixed_prod_schema_copies_only_a_same_logic_prod_build`).
  Copying stays FULL only; other kinds only report the equivalent in their
  codes. A candidate in a schema the request does not name is found only by
  the plain logic, which is conservative (not captured).

## Open hosted questions

Each item needs a controlled capture before the server changes. Until then the
server keeps the conservative behavior described.

- **Resolution context of dependencies.** Whether hosted keys dependency
  equivalence on anything beyond the definition body and freshness. A
  context-only change could not be isolated from upstream freshness through the
  client (`experiments/resolution_context.sh`). The schema-abstracted identity
  is unchanged (`fingerprint.rs::ns_uniform_schema_rename_is_cross_env_positive`).
- **Volatile evaluation.** Hosted returns `transformed_nodes_by_query` with
  runtime values (for example `CURRENT_TIMESTAMP()`). The server returns it
  empty, leaves `query_hash_metadata_info` unset (hosted use uncharacterized),
  and never reuses under `tolerate_nondeterminism=false`. Keyed gaps: `clone_fallback_volatile.jsonl`
  entries 0 and 2 in `golden/replay_gaps.json`.
- **RegisterClone source-age chain, entry 4.** Hosted SKIPs a post-copy submit
  that differs from the chain's first submit in schema, an added incremental
  predicate and epochs at once; the local server does not. The cause is not
  isolated
  (`clone_chain_provenance.rs::registerclone_source_age_chain_exact_replay`,
  fixture `clone_chain_source_age.jsonl`). It is not the #93 cause: its
  schema difference is one the copy adoption already matched before #93
  (the upstream resolved per target). What remains is the added
  `is_incremental()` self-reference predicate, the Target relation's epoch
  being newer than the copy's confirmation, and a runtime (4074 ms) that is
  not the source build's (2600 ms).
- **Explain verdicts over a copy (#93).** In the post-copy SKIP tree hosted
  gives a FRESH verdict only to an upstream the copy's source build recorded,
  and lists a per-target upstream with its `last updated` line only
  (`explain_post_copy_identity.jsonl` `c93st`, `c93it`). The server keeps the
  source build's upstream names on the copy and renders the same. Not
  isolated: in that capture the per-target upstream was itself a submit-time
  copy, so "the upstream is a copy" fits the capture too.
- **An older build of the submit's type that would qualify (#83, #90).**
  Hosted judges a submit whose latest build has its identity under another
  execution type against the latest build of its own type, RecordExecutions
  rows included (resolved, [ADR 0007](adr/0007-lag-tolerance-and-upstream-producers.md#execution-type-change),
  `explain_execution_type_change.jsonl`). Every capture had that build's
  Target relation modified since. Whether hosted would reuse it when its
  Target relation is unchanged since is not characterized; the server
  rebuilds, as the execution type change against the latest build (2 / 2,
  `offline_regressions.rs::execution_type_change_is_judged_against_the_latest_build_of_that_type`).
- **Incremental CLONE (#97, resolved by #73).** Hosted 2.0.11 has no
  explainable incremental CLONE decision: its `RegisterClone` copy has an
  empty `execution_decision_id`, and it rebuilds incrementals on submit. This
  server now copies only FULL on submit (#101). Still open (#101 question 3):
  whether hosted 2.0.11 copies an incremental over an existing relation that
  has no State history of its own, the situation of the 2.0.6 copies
  (`clone_failed_fallback.jsonl`, `clone_submit_equivalent_fallback.jsonl`,
  whose target histories are not captured; keyed divergences below).
- **Non-FULL submits with an equivalent, uncaptured cases (#86, #92, #101).**
  The server reports 1 / 3, 1 / 6 or 1 / 2 only where the captures show them
  ([protocol.md](protocol.md#result-copy-on-submit)). Not captured: a view,
  snapshot or custom node whose newest equivalent has another execution type
  (the server sends 1 / 2); a non-FULL submit whose own reuse candidate must
  rebuild for an Upstream producer change (the server keeps the ordinary
  codes; for upstream data drift hosted reports the equivalent, 1 / 3 or
  1 / 6, captured since in `explain_freshness_cells.jsonl` `fd_sn` and
  `explain_rebuild_tolerance_line.jsonl`, and so does the server); a non-FULL
  submit that forbids copies (the server sends its non-FULL codes, not
  1 / 10; a FULL one is `copy_disabled`, 1 / 10, "cloning is disabled",
  captured, `explain_copy_logic_mismatch.jsonl` `cf4`); and a view or
  snapshot rebuild over an existing Target relation that has an equivalent
  elsewhere (#92 question 2). Captured since (#104,
  `explain_freshness_cells.jsonl` `fr_*`): when the target's own older build
  matches and would be reused, hosted reports that older match (8 / 7,
  `is_stale` as it would be reused) and not the equivalent elsewhere, for
  the five incrementals and a snapshot. The server does the same
  (`submit_decision::older_match` goes ahead of a refused copy).
- **Seed table externally modified.** Hosted skips an unchanged-values seed
  whose own table was edited, serving the edited table; the server reloads it
  as a safety floor. Keyed gaps: `lag_seed_upstream.jsonl` entries 22–23.
- **External modification of the target relation in older captures.** In
  `lag_multi_upstream_policy.jsonl` (client 2.0.6) hosted skipped a target
  relation modified after its build; current hosted rebuilds it
  (`lag_tolerance_matrix.jsonl` `ot*_u`). The server follows current hosted, so
  that fixture stays characterization-only (`lag_policy.rs`).
- **First-build rejection codes with prior hosted history.** Hosted reports
  4 / 7 from builds in sessions the capture lacks. The rule is implemented; the
  replay lacks the history. Keyed gaps: `lag_external_modification_linked.jsonl`
  entries 3–4.
- **Stale reuse versus a fresh alternative, and copies with Upstream
  producers.** Uncharacterized: whether hosted prefers copying a fresh
  equivalent over a stale same-target reuse within tolerance; copying an
  equivalent from another target after a same-upstream one was rejected on
  freshness (the server rebuilds); and the full clone chain with the upstream's
  own copies. `clone_chain_upstream_history.jsonl` is characterization-only
  because every copy source predates the capture
  (`lag_tolerance.rs::clone_chain_external_modification_of_consumed_upstream_rebuilds`).
- **Copy limits.** `clone_time_travel_limit` and `OUTSIDE_TIME_TRAVEL_WINDOW`
  (code 9) are never applied. Outside DuckDB, a `clone_chain_depth_limit`
  above 0 is ignored.
- **When node state is published.** The server publishes selector and deferral
  node state at submit time, before confirmation, so an unconfirmed or failed
  build is immediately deferrable. Hosted timing is unobserved
  (`submit_decision.rs::record_node_state`).
- **Wildcard views.** Column evidence for `SELECT *` over views with added or
  removed columns, nested views and transitive definition changes.
- **`lenient_dependencies`.** The server ignores the field. Define it from
  client source or docs, then capture.
- **Template comparison matrix.** The four cells with the flag both ways,
  transitions of `compare_unrendered_code`, macro/var changes, and how
  `RecordExecutions` (no flag) should seed the template identity.
- **Copy with no recorded inputs.** A submit-path copy that recorded an empty
  input list rebuilds when a later submit has upstreams; hosted is unobserved
  (`reuse_candidate.rs::sql_submit_copy_with_no_recorded_upstreams_rebuilds_when_upstreams_appear`).
- **Data tests and Lag tolerance.** Clients send tolerance 0 for data tests, so
  data tests as Lag tolerance subjects are uncharacterized.
- **Data-test EXECUTE trees not captured.** Hosted's data-test EXECUTE tree
  (#75) is the same for a first execution, a logic change, upstream data
  drift and an upstream whose logic changed, for passing and failing tests,
  over one or two upstreams; a test without upstreams says `no upstream
  dependencies` (`explain_rebuild_reason_gaps.jsonl`, #40 code review). A
  test that fails reuse qualification (`evaluate_volatile_sql`, #67) was not
  captured; the server renders nothing for it
  ([explain-catalog.md](explain-catalog.md#data-test)).
- **`store_failures` audit relation recognition (#88).** Hosted's own rule is
  not observable: every capture is also explained by "compare only the
  inputs recorded at the build". The server recognizes the audit relation
  from the request alone and drops it from the test's inputs
  (`execution.rs::is_own_audit_relation`): a Snowflake data test whose
  semantic extras set `store_failures: true`, and a relation exactly
  `default_catalog`.`default_schema`.`dbt_node_name`. Every other unrecorded
  table still changes the dependency set and rebuilds. The request does not
  carry the test's alias, so a relation the test reads can have exactly that
  name (test `orders` aliased `orders_audit`, reading `orders`; Codex review
  on #109): a name that the test's SQL or one of its query dependencies
  mentions as a word is never the audit relation and stays an input. Assumed, not
  captured: that client 2.0.11's `default_schema` is the test's own schema
  (dbt-fusion builds `default_catalog` from `node.database()` and parses
  unqualified names against `node.database()`/`node.schema()`; the 2.0.11
  request builder is not in the local dbt-fusion checkout, and jaffle-shop's
  `prod_demo` target puts the audit schema in the target schema, so the
  captures cannot tell the two apart). When it is not (the usual
  `<target>_dbt_test__audit` schema), or the test has a configured `alias`
  (a long generic test name), the relation is not recognized and the test
  keeps rebuilding with `dependency_set_changed`, the conservative side.
  Other warehouses recognize nothing.
- **Unit tests.** Hosted reuse of unit tests is not characterized; Explain shows
  "details unavailable" ([hosted-behavior.md](hosted-behavior.md)).
- **Explain evidence gaps (#78).** Resolved by #76: VIEW × CLONE is fenced as
  unreachable on 2.0.11 (hosted answers a non-FULL submit copy candidate with
  1 / 3, `explain_copy_probes.jsonl` entry 13). The SHAPE 7 query-changed
  tree is replayed from `explain_snapshot_history.jsonl` (FULL, VIEW and
  snapshot, client 2.0.11).
- **SHAPE 7 without a matched build (resolved).** Hosted judges an
  identity-mismatch rebuild's freshness listing against the latest build in
  scope, whose identity did not match: `<FRESH>` and `upstream data is up to
  date` when the upstream epoch is unchanged
  (`explain_incremental_append_merge_history.jsonl`,
  `explain_incremental_microbatch.jsonl` entry 40, #89), `<FRESH>` and no
  summary when it moved back (`explain_snapshot_history.jsonl`), `<OUTDATED>`
  with the `updated N seconds after …` line and `upstream data is outdated`
  when it drifted (`explain_rebuild_reason_gaps.jsonl` entry 33). The
  decision persists that build's id (`node_decisions.unmatched_execution_id`,
  migration 0017) and Explain re-derives the listing from it. A rebuild
  recorded before the migration has no such build: its listing keeps the
  default `<FRESH>` badges and omits the summary, since unknown evidence
  never proves freshness
  ([explain-catalog.md](explain-catalog.md#execute-node-query-changed-shape-7)).
- **Reason × node kind cells without a capture (#104, settled).** Every
  Decision reason × node kind that renders a tree is backed by a tracked
  capture; the cells without one render nothing, and each has a status in
  [explain-catalog.md](explain-catalog.md#decision-reason-coverage) (no cell
  is left → #104). The freshness cells are captured
  (`explain_freshness_cells.jsonl`): the
  ALL-policy reuse with partial drift for the incrementals and DBT_CUSTOM,
  drift past the Lag tolerance for insert_overwrite, delete+insert,
  microbatch and a snapshot, and the stale SHAPE 7 rebuild with its Lag
  tolerance line, whose age is the older matching build's (the decision now
  records it). The last cell, `copy_logic_mismatch` over a dropped
  RegisterClone copy, is captured for every model kind (crafted
  `explain_copy_logic_crafted.jsonl` `x11`–`x19`): hosted rebuilds with
  6 / 6 and the first-build tree, as this server does.
- **A view that reads no relation (#117).** Client 2.0.11 reports no upstreams
  for any view, yet hosted's VIEW tree says `no upstream dependencies` when
  the view's SQL reads no relation (`explain_reason_kind_cells.jsonl` `vc0`,
  `cv0`) and `upstream data is up to date` when it reads a `ref`
  (`explain_snapshot_history.jsonl` `s72vl`). Neither the decision nor its
  matched build records which, so the server renders nothing for the two
  captured VIEW cells (`own_target_modified`, `execution_type_changed`; keyed
  `empty_tree` gaps, entries 246 and 377) and its VIEW SKIP arm would use the
  second wording for a view reading no relation (no capture of that SKIP).
- **Snapshot SKIP wording past the Lag tolerance.** Hosted words every
  observed snapshot SKIP "both … up to date", including drift within the
  tolerance (`explain_snapshot_history.jsonl` entry 40). The `ALL`-policy
  reuse past the tolerance is unreachable for snapshots on 2.0.11: the
  client reads `state` config (`require_fresh_data_from`, `lag_tolerance`)
  from models only (dbt-fusion `run_cache_service.rs::model_state_for_node`),
  so a snapshot always gets the ANY policy. The server keeps the
  same string ([protocol.md](protocol.md#decision-descriptions)), a local
  choice pinned by
  `decision.rs::unobserved_all_policy_snapshot_skip_past_tolerance_keeps_the_observed_wording`.
- **Non-Snowflake copy DDL.** BigQuery and Databricks templates follow published
  syntax but were never run against hosted or the warehouses. View copies need
  the view definition, not `CREATE VIEW ... CLONE`.
- **Client and warehouse compatibility.** Wire schemas match the reviewed Rust
  client; older Python client schemas and identity representations differ.
  Supported client versions and warehouses need a pinned matrix.
- **Proxy fidelity.** The recording proxy forwards client metadata but does not
  record error responses, trailers or raw bytes, and does not fail on capture
  loss (`crates/harness/src/bin/record_proxy.rs`).

## Known divergences

Every replay divergence is pinned per fixture entry in
[`golden/replay_gaps.json`](../golden/replay_gaps.json), with its reason and,
where only part of a response differs, the `ignore_fields` it exempts. The
replay fails when an entry becomes unused or resolved
([testing.md](testing.md#goldenreplay_gapsjson)). The categories are:

- **Missing history.** The capture copies from, or depends on, a build made in
  an earlier session, so an empty local store builds instead.
- **Conservative identity.** A dependency sent as `SELECT` text in one submit
  and as view DDL in another, and data-test reuse across targets, rebuild
  locally where hosted reused.
- **Volatile evaluation.** `transformed_nodes_by_query` is empty locally.
- **Safety floor.** The externally modified seed table is reloaded.
- **Speculative SKIP (#103).** Hosted SKIPs an unchanged data test with no
  upstreams on the `SubmitEnrichedSQLSpeculative` call, so the client sends
  no ordinary submit; the server answers every speculative call `undecided`,
  and the client then sends the ordinary submit, which the server SKIPs with
  hosted's payload. The final class and the Explain tree match. Keyed gap:
  `explain_rebuild_reason_gaps.jsonl` entry 39 (`local_variant: "undecided"`;
  the replay drives the follow-up submit and asserts it).
- **Copies from `prod_demo` into `dev_clone` (#76 probes, client 2.0.11).**
  - Seed copy (#91). Hosted copies a seed loaded with the same values under
    another name on submit (`ready_to_clone`, 1 / -, `an equivalent seed
    exists under another name so we cloned that one`); the server never copies
    seeds and loads it. Keyed gaps: `explain_copy_probes.jsonl` entries 12
    (submit) and 18 (Explain: the server explains the load it decided).
- **Snapshot and custom node over a modified Target relation (#115, client
  2.0.11).** Hosted reuses a snapshot whose Target relation a later
  `--full-refresh` (FULL) build rewrote or an External modification changed,
  and a custom node whose relation was modified externally or replaced by a
  view build in between (`explain_reason_kind_cells.jsonl` `sn_c`, `sn_cz`,
  `sn_e`, `sn_ez`, `vc`, `cv0`). The incrementals, FULL and VIEW rebuild in
  the same situations. The server keeps reuse invariant 3 and rebuilds them
  with `own_target_modified` (6 / 6); its Explain tree for that rebuild of a
  snapshot or custom node renders nothing. Keyed gaps: entries 171, 187,
  297, 311, 313, 323 (submits) and 225, 231, 369, 375, 376, 379 (Explain,
  `empty_tree`).
- **View ↔ custom node over an upstream (#116, client 2.0.11).** With the
  same SQL and semantic extras (`lookback`), hosted treats the switch as an
  execution type change only when the SQL reads no relation (`vc0`, `cv0`,
  2 / 2). Over an upstream, which the view does not report and the custom
  node does, it answers 6 / 6, and 8 / 7 for the custom node over view and
  custom builds (`vc`, `cv`). The server answers 2 / 2 and judges the last
  one as `own_target_modified` (6 / 6) against the older custom build. Both
  rebuild; codes and the last description differ, and the server's trees
  for them render nothing. Keyed gaps: entries 163, 182, 322 (field-scoped)
  and 243, 245, 378 (Explain, `empty_tree`).
- **2.0.6 submit copies of a MERGE incremental (deliberate, #101).** Client
  2.0.6 got `ready_to_clone` for a MERGE incremental over an existing target
  (`clone_failed_fallback.jsonl` entry 2, `clone_submit_equivalent_fallback.jsonl`
  entry 1). The server follows hosted 2.0.11, which rebuilt every incremental
  strategy on submit (`explain_incremental_copy_over.jsonl`,
  `explain_incremental_submit_copy.jsonl`), and copies only FULL on submit
  (maintainer decision 2026-11-11), so it builds there. Whether 2.0.11 would
  copy over a relation with no State history of its own is open (#101
  question 3, above). The copy sources' histories are not in those captures
  either. Keyed gaps: those two entries (local `ready_to_execute`).
- **Malformed requests (#114, crafted only,** `experiments/malformed_requests.py`).
  For every node kind, hosted fails a submit with empty SQL with
  `INVALID_ARGUMENT` "Failed to process SQL". It fails a model whose query
  dependency has an empty name or query the same way. It builds a request
  whose query dependency is listed twice, then SKIPs the identical resubmit.
  The server accepts all of these, builds them, and on every resubmit fails
  reuse qualification (`no_reuse_candidate.failed_reuse_qualification`,
  6 / 6, no tree). Keyed gaps: `explain_malformed_requests.jsonl` entries 2,
  6, 10, 14, 19, 23 and 27 (the resubmits) and the Explain entry after each.
  The rejected submits have no fixture rows (see "Proxy fidelity"). No
  captured client request has these shapes; #114 asks whether to keep the
  safety rule as an accepted divergence.
- **Crafted ambiguous Target relation evidence** (#71 probes,
  `experiments/ambiguous_rebuilds.py`). Client 2.0.11 sends none of these
  requests ([explain-catalog.md](explain-catalog.md#ambiguous-rebuild-reasons)).
  The Target relation listed twice or with no epoch (#80) and the
  ConfirmExecution without an epoch (#82) are accepted divergences (see
  [below](#accepted-divergences)). A model submit with no target (#81) is
  rejected with `InvalidArgument` before it is decided; hosted fails it too
  (`Internal` when unset, `InvalidArgument` "Failed to process SQL" when
  `""`). Pinned by
  `offline_regressions.rs::model_submit_without_target_is_invalid_argument_not_a_result_copy`.

  The proxy does not record failed calls (see "Proxy fidelity"), so the
  no-target submits and the no-epoch confirm have no fixture rows and no keyed
  gap. The driver's stdout is their only record.

Characterization-only fixtures, which the generic replay excludes, are listed
in `CHARACTERIZATION_ONLY` in `crates/harness/tests/conformance.rs` with the
test that pins each.

### Accepted divergences

Settled differences from hosted: each needs a request client 2.0.11 never
sends, and the server's rule is the conservative one, so it stays. Their
keyed gaps stay in `golden/replay_gaps.json`, with reasons that point here.
Each rebuild below is local-only and renders no Explain tree.

- **Ambiguous Target relation evidence (#80, crafted d1 / d2).** A Target
  relation listed twice in `tables` with the build's epoch (hosted SKIPs), or
  listed with no epoch (hosted rebuilds it as missing, codes 4 / 7, "did not
  exist"): the server rebuilds with `own_target_changed_or_missing` (6 / 6,
  generic description). Rationale: crafted-only (client 2.0.11 sends one
  entry per relation and drops a Target relation of unknown epoch), and the
  "Target relation as built" invariant above, which needs exactly one known
  epoch, is the conservative rule. Keyed gaps:
  `explain_ambiguous_rebuilds.jsonl` entries 2, 3, 6 and 7 (`empty_tree`).
- **ConfirmExecution without an epoch for a target-bearing build (#82,
  crafted d4).** Hosted fails it (`Internal`, "Database operation failed");
  the server records it and a later submit rebuilds with `no_build_anchor`.
  Rationale: crafted-only (client 2.0.11 never sends that confirm for a
  non-test node), and a build with no epoch never proves the Target relation
  is as built, so rebuilding is the conservative rule. No keyed gap: the
  failed hosted call was not recorded.
- **Data test with incomplete cached results (#87, crafted,
  `experiments/incomplete_test_results.py`).** A data test confirmed with
  `execution_results: null`, or with only `failures`, then resubmitted
  unchanged: hosted SKIPs and echoes the confirmed results (`{"fields": {}}`
  or `failures` alone); the server rebuilds with
  `no_reuse_candidate.incomplete_cached_test_results`. Rationale:
  crafted-only (client 2.0.11 always confirms `failures`, `should_warn` and
  `should_error`), and AGENTS.md invariant 5 requires complete cached
  results. Keyed gaps: `explain_incomplete_test_results.jsonl` entries 2, 6
  (decision) and 3, 7 (`empty_tree`).
- **Upstream with an unknown epoch (#113, crafted,
  `experiments/unknown_epoch_probes.py`).** A FULL model built and
  resubmitted with an upstream whose `last_modified_epoch` is `null` at the
  build, at the resubmit, or both: hosted SKIPs within the Lag tolerance
  (`is_stale` unless only the build's epoch was unknown) and explains it with
  the ordinary SKIP tree, reading an unknown current epoch as `now`. The
  server rebuilds with `upstream_outdated` (6 / 6) and renders no tree,
  because SHAPE 9 needs a proven drift. Rationale: crafted-only (client
  2.0.11 leaves a table of unknown epoch out of `tables` and skips the State
  submit), and AGENTS.md invariant 2 says unknown epochs never prove
  freshness. Keyed gaps: `explain_unknown_epoch.jsonl` entries 2, 6, 10, 14
  (decision) and 3, 7, 11, 15 (`empty_tree`).
- **Unset target on a model submit (#81, crafted d3).** Hosted answers
  `Internal` "Internal server error"; the server answers `InvalidArgument`,
  the code hosted gives for `""` (d3e). Rationale: copying hosted's internal
  error is not worth it; the request is the caller's error and is rejected
  before anything is decided or recorded. No keyed gap: failed calls are not
  recorded.

## Deployment gaps

- **Auth and TLS.** Header-based organization selection is isolation, not
  authentication. `DBT_STATE_REQUIRE_ORG_HEADER=true` rejects requests without
  `x-organization-id` but does not verify it against a trusted token. OAuth,
  TLS and identity mapping are deployment work. The UI reads one configured
  organization (`DBT_STATE_UI_ORG_ID`).
- **ClientTelemetry** is not served. Check whether supported clients need it.
- **Health** always reports `SERVING`; it is not a database readiness check.
- **Capture** runs as unbounded background tasks (`services.rs::spawn_capture`)
  with no retention policy. Bound both before heavy load.
- **ClientValidation** accepts every client version.
- **Speculative submits** always return `Undecided` (hosted decides some,
  #103).

## Primary sources

- dbt docs: [dbt State overview](https://docs.getdbt.com/docs/deploy/dbt-state-about),
  [dbt State configs](https://docs.getdbt.com/reference/resource-configs/dbt-state-configs),
  [deferral](https://docs.getdbt.com/docs/deploy/dbt-state-deferral).
- Clients: [Rust client](https://github.com/dbt-labs/dbt/tree/a0fc53e6797b135ce8d4fea99c581ea5116803ac)
  (`crates/dbt-tasks-core/src/run_cache/`, `crates/dbt-state/`),
  [Python client](https://github.com/dbt-labs/dbt-state/tree/b0f3030c90dd0766f2d1625b3305829d881fc2ae)
  (`clients/dbt_state/src/dbt_state/run_cache.py`).
