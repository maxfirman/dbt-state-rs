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
  overwritten since. See
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
  corrupt stored evidence (`INTERNAL`) never become a reuse.
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
  subset) only.

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
  fixture `clone_chain_source_age.jsonl`). A cleaner capture of the same
  outcome, with identical SQL and aligned epochs, is the divergence #95
  ([Known divergences](#known-divergences)).
- **What a RecordExecutions row means to hosted decisions (#90).** After a
  `--full-refresh` incremental run, which client 2.0.11 records as a FULL
  execution, hosted judges the next APPEND/MERGE submit against the last
  incremental build. Whether hosted leaves recorded executions out of
  decisions, or prefers the latest row of the submit's execution type, is not
  isolated; #83 shows hosted does consider another execution type for a
  submit.
- **Incremental CLONE (#97).** No APPEND/MERGE CLONE tree is captured on
  client 2.0.11. Hosted answers `RegisterClone` with an empty
  `execution_decision_id`, so the dev-clone copy decision is never explained;
  one submit-time probe (`explain_incremental_submit_copy.jsonl`) was rebuilt
  rather than copied, while client 2.0.6 did get an incremental submit copy
  (`clone_failed_fallback.jsonl`). The cells stay fenced.
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
- **Unit tests.** Hosted reuse of unit tests is not characterized; Explain shows
  "details unavailable" ([hosted-behavior.md](hosted-behavior.md)).
- **Explain evidence gaps (#78).** The VIEW × CLONE Explain cell is open
  (`explain.rs::is_characterized`) but no view copy was ever captured; the
  live attempt is part of #76. The SHAPE 7 query-changed tree is replayed from
  `explain_snapshot_history.jsonl` (FULL, VIEW and snapshot, client 2.0.11).
- **SHAPE 7 without a matched build.** An identity-mismatch rebuild has no
  Explain anchor, so its per-upstream listing renders `<FRESH>`. The
  `upstream data is up to date` summary follows the epochs recorded at the
  latest build in scope, persisted with the decision
  (`node_decisions.unmatched_upstreams_unchanged`, migration 0017): present
  when they are unchanged (`explain_incremental_append_merge_history.jsonl`),
  absent when one moved back (`explain_snapshot_history.jsonl`). Hosted's tree
  for a query change whose upstream also drifted is unobserved
  ([explain-catalog.md](explain-catalog.md#execute-node-query-changed-shape-7)).
- **Snapshot SKIP wording past the Lag tolerance.** Hosted words every
  observed snapshot SKIP "both … up to date", including drift within the
  tolerance (`explain_snapshot_history.jsonl` entry 40). The `ALL`-policy
  reuse past the tolerance is unobserved for snapshots; the server keeps the
  same string ([protocol.md](protocol.md#decision-descriptions)).
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
- **Execution-type change (#83).** A snapshot run under `--full-refresh` is
  submitted as FULL over its SNAPSHOT State history. Hosted rebuilds with
  rejection codes 2 / 2 (`EXECUTION_TYPE_MISMATCH`) and explains it with the
  node-query-unchanged tree. The server rebuilds with the same class and
  description but codes 6 / 6, records the Decision reason
  `no_reuse_candidate.execution_type_changed`, and renders no Explain tree.
  Keyed gaps: `explain_snapshot_history.jsonl` entries 58 (rejection codes)
  and 60 (`empty_tree`).
- **Incremental rebuild after a `--full-refresh` run (#90).** Client 2.0.11
  records the full refresh with `RecordExecutions` as a FULL execution, then
  resubmits the model as APPEND/MERGE. Both rebuild with 6 / 6; hosted says
  "the table was modified after the cached execution" (it judges against the
  last incremental build), the server takes the recorded FULL row as the
  latest, records `no_reuse_candidate.execution_type_changed` with the
  generic rebuild description, and renders no Explain tree. Keyed gaps:
  `explain_incremental_append_merge_history.jsonl` entries 74–75
  (description) and 78–79 (`empty_tree`).
- **Post-copy incremental submit (#95).** After the dev-clone pre-step copies
  an append or merge incremental (`RegisterClone`, confirmed with the copy's
  epoch), the client submits it with byte-identical SQL. Hosted skips it. The
  server does not adopt the copy: the copy inherits the prod build's
  schema-abstracted logic, computed under the prod default schema, where the
  submit's explicit prod-schema source reference was abstracted; under the dev
  default schema it is not. It rebuilds with
  `no_reuse_candidate.copy_logic_mismatch` (wire unchanged, 6 / 6) and renders
  no Explain tree. Keyed gaps: `explain_incremental_copy_history.jsonl`
  entries 12–13 (whole response) and 14–15 (`empty_tree`). The same root
  cause gives `explain_incremental_submit_copy.jsonl` entries 9–10 skip code
  6 where hosted, which recognizes the prod equivalent, reports 1
  (field-scoped). `clone_happy_path.jsonl` entry 7 is the missing-history
  form: its copy source's build is not in the fixture.
- **Crafted ambiguous Target relation evidence** (#71 probes,
  `experiments/ambiguous_rebuilds.py`). Client 2.0.11 sends none of these
  requests ([explain-catalog.md](explain-catalog.md#ambiguous-rebuild-reasons)).
  Each rebuild below is local-only and its Explain tree is empty.
  - A Target relation listed twice with the build's epoch: hosted reuses it,
    the server rebuilds with `own_target_changed_or_missing` (#80). Keyed gaps:
    `explain_ambiguous_rebuilds.jsonl` entries 2 and 3 (`empty_tree`).
  - A Target relation with no epoch: hosted rebuilds it as missing (codes
    4 / 7, "did not exist"), the server as ambiguous (6 / 6, generic
    description) (#80). Keyed gaps: entries 6 and 7 (`empty_tree`).
  - A model submit with no target: hosted fails it (`Internal` when unset,
    `InvalidArgument` "Failed to process SQL" when `""`), the server answers a
    Result copy of the node's own build (#81).
  - A ConfirmExecution without an epoch for a target-bearing build: hosted
    fails it (`Internal`, "Database operation failed"), the server records it
    and later rebuilds with `no_build_anchor` (#82).

  The proxy does not record failed calls (see "Proxy fidelity"), so the last
  two have no fixture rows and no keyed gap. The driver's stdout is their only
  record.

Characterization-only fixtures, which the generic replay excludes, are listed
in `CHARACTERIZATION_ONLY` in `crates/harness/tests/conformance.rs` with the
test that pins each.

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
- **Speculative submits** always return `Undecided`.

## Primary sources

- dbt docs: [dbt State overview](https://docs.getdbt.com/docs/deploy/dbt-state-about),
  [dbt State configs](https://docs.getdbt.com/reference/resource-configs/dbt-state-configs),
  [deferral](https://docs.getdbt.com/docs/deploy/dbt-state-deferral).
- Clients: [Rust client](https://github.com/dbt-labs/dbt/tree/a0fc53e6797b135ce8d4fea99c581ea5116803ac)
  (`crates/dbt-tasks-core/src/run_cache/`, `crates/dbt-state/`),
  [Python client](https://github.com/dbt-labs/dbt-state/tree/b0f3030c90dd0766f2d1625b3305829d881fc2ae)
  (`clients/dbt_state/src/dbt_state/run_cache.py`).
