# Golden traffic

Captured request/response pairs from the hosted dbt State service
(`api.state.dbt.com`), used as test fixtures. How they are replayed and what a
pass proves is in [docs/testing.md](../docs/testing.md#captured-traffic-replay).

## Layout

`databricks_explain_visibility.jsonl` records a warehouse-free hosted
submit followed immediately by its first Explain (2026-11-13). The returned
decision has a complete tree. `explain_visibility.rs` reproduces a local
capture-lag race at the public gRPC seam and verifies scoped causal visibility
without client retries. The fixture is the unchanged first pair from a
certified twelve-RPC campaign; no execution or warehouse mutation is claimed.

- `fixtures/`: the curated, git-tracked corpus. Treat it as read-only test data.
- `replay_gaps.json`: keyed known divergences for the replay
  ([semantics](../docs/testing.md#goldenreplay_gapsjson)).
- `upgrade/`: frozen rows from an old server revision (below).
- Top level: the recording proxy's output directory. `record-proxy` writes
  `golden_<timestamp>_<session>.jsonl` and a `.capture.json` integrity sidecar here each session. These files are gitignored
  (`/golden/golden_*.jsonl`) and ephemeral. Promote a useful capture by copying
  it into `fixtures/`; do not leave session files at the top level.

Session files at the top level are never tracked. Tests read only
`fixtures/`, `replay_gaps.json` and `upgrade/`.

## Fixtures

`crates/harness/tests/conformance.rs` replays every fixture except those in its
`CHARACTERIZATION_ONLY` list ("char-only" below), which the named dedicated
tests drive instead. Test paths are under `crates/harness/tests/`.

| File | Replay | What it captures; which test pins it |
|---|---|---|
| `databricks_client_auxiliary.jsonl` | yes | Pinned 2.0.11 validation, session start/end including exact durations and client failure, plus query-preparation telemetry; `client_auxiliary.rs` and generic conformance. [Provenance](../docs/reports/databricks-client-auxiliary-2026-11-12.md). |
| `databricks_table_view_baseline.jsonl` | yes | Complete pinned 2.0.11 managed Delta table/view build, unchanged reuse, changed-query rebuild and subsequent reuse, confirmations, speculative submits, telemetry and full Explain including opposite request orders; no history/gap exceptions. [Runner](../docs/databricks-baseline.md). |
| `golden_20261008T222744.061Z.jsonl` | yes | Build, confirm and skip lifecycle with speculative submits and version checks; `differential.rs`. |
| `golden_20261009T083754.534Z.jsonl` | yes | Prod build then dev skip through namespace reuse, with cross-target data-test entries keyed in `replay_gaps.json`; `conformance.rs`. |
| `golden_20261009T100637.316Z.jsonl` | yes | `RegisterClone` to `ready_to_clone` plus an empty `ResolveDeferredRelations`; `clone.rs::register_clone_matches_real_shape`, `deferred_relations_empty_when_nothing_built`. |
| `clone_happy_path.jsonl` | yes | Profile-deferral result copy: cross-target skip plus `RegisterClone`; `clone_lifecycle.rs::clone_happy_path_register_confirm_shapes`. |
| `clone_failed_fallback.jsonl` | yes | Client 2.0.6 MERGE incremental SQL submit answered `ready_to_clone` from a source not in the capture; this server copies only FULL on submit and builds (keyed divergence, #101 question 3); `conformance.rs::submit_enriched_sql_clone_fallback_is_characterized`. |
| `c1_config_vs_logic.jsonl` | yes | A no-op rebuild, a config-only `meta` edit and a SQL change: hosted skips the first two, but the server rebuilds them because the dependency definition changes from SELECT text to view DDL and its exact dependency-definition fingerprint misses (keyed in `replay_gaps.json`); `c1_probe.rs`. |
| `c1_config_semantics.jsonl` | yes | One config change at a time: hosted skips `tags`/`meta`/`post_hook` and rebuilds on `grants`/`persist_docs`, and the recorded `pre_hook` rebuild did not reproduce from a clean baseline; `c1_config_semantics.rs`. |
| `c1_test_node_identity.jsonl` | yes | Data tests sharing one body hash are told apart by `node_unique_id`; `c1_test_node_identity.rs`. |
| `rendered_sql_change.jsonl` | yes | Rendered SQL changes under an unchanged body hash, which rebuilds; `c2_rendered_sql.rs::rendered_sql_change_forces_execute`. |
| `compare_unrendered_code.jsonl` | yes | With `compare_unrendered_code`, a rendered-only change skips; `c2_rendered_sql.rs::compare_unrendered_code_skips_rendered_change`. |
| `template_compare_unrendered.jsonl` | yes | With `compare_unrendered_code`, a template-only change skips; `template_comparison.rs::hosted_template_changed_rendered_same_shape_is_pinned`. |
| `data_test_compare_unrendered.jsonl` | yes | The same template rule for a data test; `data_test_template.rs::hosted_data_test_cuc_template_change_skips_shape_is_pinned`. |
| `data_test_upstream_freshness.jsonl` | yes | A data test reruns when its upstream epoch moves past the recorded one, with the externally replaced upstream keyed as missing history; `data_test_freshness.rs`. |
| `custom_materialization_reuse.jsonl` | yes | A custom materialization is reused when unchanged; `conformance_materializations.rs::custom_materialization_is_reused_like_any_node`. |
| `snapshot_reuse.jsonl` | yes | An unchanged snapshot is reused; `conformance_materializations.rs::snapshot_is_reused_when_unchanged`. |
| `incremental_merge_reuse.jsonl` | yes | An unchanged incremental merge model is reused; `conformance_incremental_seed.rs::incremental_merge_reuse_reproduced`. |
| `seed_values_hash.jsonl` | yes | Seed build, reuse and rebuild on a `values_hash` change; `conformance_incremental_seed.rs::seed_values_hash_lifecycle_reproduced`. |
| `explain_happy_history.jsonl` | yes | History-bearing Explain trees for model, view and data-test SKIPs; `conformance.rs` Explain arm. |
| `explain_clone_history.jsonl` | yes | History-bearing Explain tree for a result copy; `conformance.rs` and `crates/server/src/explain.rs` tests. |
| `explain_tolerance_history.jsonl` | yes | History-bearing Explain trees for a within-tolerance SKIP and an outdated-upstream build; `conformance.rs` Explain arm. |
| `explain_custom_history.jsonl` | yes | Client 2.0.11 custom materialization (DBT_CUSTOM) Explain trees (first build, SKIPs, source drift under default and `0s` Lag tolerance, upstream query and logic changes, `--full-refresh`), from `experiments/explain_capture.py custom_explain`; `conformance.rs` Explain arm. |
| `explain_custom_copy.jsonl` | yes | Client 2.0.11 custom node and FULL control built in `dev_clone` over prod equivalents: hosted copies only the FULL one and rebuilds the custom node with codes 1 / 3, as this server does (#86), from `experiments/explain_capture.py custom_copy`; `conformance.rs`. |
| `explain_snapshot_history.jsonl` | yes | Client 2.0.11 snapshot Explain trees (first build, SKIPs, source drift, upstream and logic changes, `--full-refresh`) plus FULL and VIEW logic-change rebuilds, from `experiments/explain_capture.py snapshot_explain`; `conformance.rs` Explain arm. |
| `explain_incremental_append_merge_history.jsonl` | yes | Client 2.0.11 APPEND and MERGE incremental Explain trees (first build, SKIP, logic change, upstream drift at the default and a 0s Lag tolerance, `{{ this }}` self-read), an unset strategy sent as APPEND, and a `--full-refresh` run recorded with `RecordExecutions` then rebuilt against the last incremental build, from `experiments/explain_capture.py incremental_append_merge`; `conformance.rs`. |
| `explain_execution_type_change.jsonl` | yes | Client 2.0.11, the same SQL under another execution type (table ↔ append, merge ↔ delete+insert, table → view → table, an append model fully refreshed then made a table): which build hosted judges the submit against (#83, #90; ADR 0007). Two windows concatenated: `experiments/explain_capture.py execution_type_change`, then `execution_type_change_append --run-id` of that run; `conformance.rs`, `offline_regressions.rs::execution_type_change_is_judged_against_the_latest_build_of_that_type`. |
| `explain_reason_kind_cells.jsonl` | yes | Client 2.0.11 (#104 group b), one node per model kind and situation: no upstreams (SKIP, logic change), its Target relation modified after the build (an External modification or a later `--full-refresh`), execution type changes over a `--full-refresh` build and view ↔ custom switches, an upstream query change, and a logic change over a dropped Target relation. Two windows concatenated: `experiments/explain_capture.py reason_kind_cells`, then `reason_kind_cells_more --run-id … --prior-window …`; all explains but entry 350 sent by `state-probe` for the client's decisions. Keyed accepted divergence #115 (hosted reuses a snapshot or custom node over a modified Target relation); the view ↔ custom switches over an upstream (#116) and the VIEW cells of a view that reads no relation (#117) replay without gaps; `conformance.rs`. |
| `explain_reason_kind_cells_custom.jsonl` | yes | Client 2.0.11 (#104 group b): a custom node with `on_schema_change` set and a table with the same SQL swapping materializations over an upstream; the custom node still sends only `lookback`, so they rebuild as other identities (SHAPE 7), from `experiments/explain_capture.py reason_kind_cells_custom`; `conformance.rs`. |
| `explain_view_reads_no_relation.jsonl` | yes | Client 2.0.11 (#117): the SKIP and logic-change rebuild of four views that read no relation (no FROM, a CTE without FROM, VALUES, FLATTEN) and of a view over a `ref`, from `experiments/explain_capture.py view_reads_no_relation`; the first SKIP explained by `dbt state explain`, the rest by `state-probe`; `conformance.rs`. |
| `explain_incremental_copy_history.jsonl` | yes | Client 2.0.11 dev-clone pre-step copy of an append and a merge incremental (`RegisterClone`, confirm) and the post-copy SKIP (reproduced by #93), from `experiments/explain_capture.py incremental_copy`; `conformance.rs` (`RegisterClone` arm). |
| `explain_incremental_copy_over.jsonl` | yes | Client 2.0.11, one model per incremental strategy (append, merge, insert_overwrite, delete+insert, microbatch; `state.pre_clone: never`, upstream `RAW.RAW_ORDERS`) built in `prod_demo`, in `dev_clone` with other logic, then in `dev_clone` with the prod logic over the existing dev relation: hosted rebuilds all five (1 / 6, or 1 / 2 for append and delete+insert) with SHAPE 7 trees, as this server does (#101). One attempt, from `experiments/explain_capture.py incremental_copy_over`; `conformance.rs`. |
| `explain_incremental_submit_copy.jsonl` | yes | Client 2.0.11 append and merge with `state.pre_clone: never` submitted into absent dev relations while their prod builds exist: hosted builds them with skip code 1 (the prod build is the equivalent, as this server finds by reading the submit in the fixed prod schema, #107) and copies nothing. One probe, from `experiments/explain_capture.py incremental_submit_copy`; `conformance.rs`. |
| `explain_copy_probes.jsonl` | yes | Client 2.0.11, `experiments/explain_capture.py copy_probes` (#76, #78): a seed, a snapshot and a view built in `prod_demo` then `dev_clone`. Hosted copies the seed, as this server does on Snowflake (#91), and builds the view and snapshot with codes 1 / 3 (#92, matched since #107); replays exactly; `conformance.rs`. |
| `explain_seed_copy_existing.jsonl` | yes | Client 2.0.11, `experiments/explain_capture.py seed_copy_existing` (#91): two seeds loaded in `prod_demo`, then built in `dev_clone`. One is copied into its absent dev table, then rebuilt after its dev table was modified outside dbt: hosted SKIPs it, keyed accepted divergence (#129, reuse invariant 3). The other is first loaded in dev with other values, then copied over its dev table once its values match prod's ("but it's old"). Three explains sent by `state-probe`, one by `dbt state explain`; `conformance.rs`, `submit_decision.rs` seed copy tests. |
| `explain_seed_copy_disabled.jsonl` | yes | Client 2.0.11, `experiments/explain_capture.py seed_copy_disabled`: two seeds loaded in `prod_demo`, then built with the same values in a private `dev_noclone` target (`dev_clone` with `allow_clones: false`; the client sends `allow_clones: false` and `clone_chain_depth_limit: 0`), one into its absent dev table, one over its dev table loaded with other values. Hosted loads both with codes 1 / 10 and the ordinary seed descriptions, and its trees name the prod load ("but cloning is disabled"); replays exactly. One explain sent by `state-probe`, one by `dbt state explain`; `conformance.rs`, `submit_decision.rs::a_seed_is_not_copied_when_clones_are_disallowed`. |
| `explain_post_copy_identity.jsonl` | yes | Client 2.0.11, `experiments/explain_capture.py post_copy_identity` (#93): post-copy SKIPs after the `RegisterClone` pre-step for snapshots over a fixed prod source, a ref'd table and a ref'd view resolved per target, and an append over the ref'd table; the dev submits of the table and view find the prod build as their equivalent (#107); `conformance.rs` RegisterClone and Explain arms. |
| `explain_snapshot_register_clone.jsonl` | yes | Client 2.0.11, `experiments/explain_capture.py snapshot_register_clone` (#76): a snapshot copied into `dev_clone` by `RegisterClone`, then its post-copy SKIP (reproduced by #93); `conformance.rs` RegisterClone and Explain arms. |
| `explain_data_test_history.jsonl` | yes | Client 2.0.11 data-test Explain trees (first execution, unchanged rerun, logic change, upstream data drift) for a passing, a failing and a `store_failures` singular test, from `experiments/explain_capture.py data_test_execute`; the `store_failures` test reports its audit relation, which is not an input (#88); `conformance.rs` Explain arm and `data_test_explain.rs`. |
| `explain_store_failures_audit.jsonl` | yes | Client 2.0.11 failing `store_failures` test (audit table) and `store_failures_as: view` test, first execution and unchanged rerun: hosted SKIPs both with the audit relation in `tables` and lists only the source (#88), from `experiments/explain_capture.py store_failures_audit`; `conformance.rs` Explain arm and `data_test_explain.rs`. |
| `explain_store_failures_audit_schema.jsonl` | yes | Client 2.0.11 `store_failures` tests (audit table, audit view) placed by the run's schema macro in `PROD_STATE_DEMO_DBT_TEST__AUDIT`, first execution and unchanged rerun: `default_schema` is that schema and hosted SKIPs both, listing only the source (#106), from `experiments/explain_capture.py store_failures_audit_schema`; replays exactly; `conformance.rs` Explain arm and `data_test_explain.rs`. |
| `explain_store_failures_audit_alias.jsonl` | yes | Client 2.0.11 `store_failures` tests (audit table, audit view) with a configured `alias`, first execution and unchanged rerun: no request field carries the alias and hosted SKIPs both, listing only the source (#106), from `experiments/explain_capture.py store_failures_audit_alias`; the server recognizes the aliased relation as the only table in the test's schema its SQL never mentions (#123, ADR 0008), so every entry replays exactly; `conformance.rs` Explain arm and `data_test_explain.rs`. |
| `explain_incremental_insert_overwrite.jsonl` | yes | Client 2.0.11 insert_overwrite incremental (INSERT_OVERWRITE=4) Explain trees: first build, unchanged SKIP, source drift within Lag tolerance, logic change, and a `{{ this }}` self-read's first incremental run and rerun, from `experiments/explain_capture.py incremental_insert_overwrite`; `conformance.rs` Explain arm. |
| `explain_incremental_delete_insert.jsonl` | yes | The same six trees for a delete+insert incremental (DELETE_INSERT=5), from `experiments/explain_capture.py incremental_delete_insert`; `conformance.rs` Explain arm. |
| `explain_incremental_microbatch.jsonl` | yes | The same six trees for a microbatch incremental (MICROBATCH=6) built with a fixed `--event-time-start/--event-time-end` window, plus three builds with the default moving window (the second rebuilds because the window moved, the third SKIPs), from `experiments/explain_capture.py incremental_microbatch`; `conformance.rs` Explain arm (the moving-window rebuild's SHAPE 7 summary line, #89). |
| `explain_tolerance.jsonl` | char-only | Explain trees seeded programmatically from its first submit; `conformance.rs::explain_skip_tree_matches_hosted_oracle_when_history_seeded`, `explain_outdated_execute_tree_matches_hosted_oracle_when_history_seeded`. |
| `lag_freshness_own_build.jsonl` | yes | `lp_model` (5m Lag tolerance) over an upstream recreated in place outside dbt with identical content; `lag_freshness.rs`. |
| `lag_multi_upstream_policy.jsonl` | char-only | Two upstreams under ANY and ALL policies with per-upstream epoch bumps; `lag_policy.rs`. |
| `lag_tolerance_matrix.jsonl` | yes | Crafted synthetic-epoch sequences varying one Lag tolerance input at a time; `lag_tolerance.rs::crafted_matrix_decisive_outcomes`. |
| `lag_tolerance_docs_example.jsonl` | yes | The public Lag tolerance worked example, scaled down, through the real client; `lag_tolerance.rs::docs_worked_example_reproduces_on_the_real_client`. |
| `lag_external_modification_linked.jsonl` | yes | An External modification of a consumed upstream build rebuilds even under ALL; `lag_tolerance.rs::external_modification_of_a_consumed_build_rebuilds_under_all_policy`. |
| `lag_upstream_query_listing.jsonl` | yes | Explain's per-upstream query-change listing and an ALL-policy result copy marked `is_stale`; `lag_tolerance.rs::upstream_query_change_arms`. |
| `lag_copy_candidates.jsonl` | yes | Which build hosted reuses or copies when the latest build no longer qualifies, with its rejection codes; `lag_tolerance.rs::copy_candidate_arms`. |
| `lag_dropped_targets.jsonl` | yes | Dropped target relations with stale equivalents and stale own builds; `lag_tolerance.rs::copy_candidate_arms`. |
| `lag_seed_upstream.jsonl` | yes | Seeds as upstreams under a Lag tolerance plus seed Explain trees, with a keyed safety-floor divergence for an externally modified seed; `lag_tolerance.rs::seed_upstream_arms`. |
| `explain_ambiguous_rebuilds.jsonl` | yes | Crafted `state-probe` requests only (`experiments/ambiguous_rebuilds.py`, #71): a model resubmitted with its Target relation listed twice (hosted SKIP) or with no epoch (hosted first-build rebuild), keyed as accepted divergences (#80); the builds of the no-target and no-epoch-confirm probes, whose failed hosted calls were not recorded; `conformance.rs`. |
| `explain_rebuild_reason_gaps.jsonl` | yes | Client 2.0.11 (#40 code review): a FULL model whose logic changed while its source drifted (SHAPE 7 with an OUTDATED listing against the unmatched build), and data tests over a model whose logic changed, with no upstreams (first execution and logic change, both `undecided` on the speculative submit, and an unchanged rerun hosted SKIPs on the speculative submit, #103) and over two upstreams (drift, SKIP), from `experiments/explain_capture.py rebuild_reason_gaps`; `conformance.rs` (the speculative arm asserts the SKIP and its Explain), `speculative_submit.rs`. |
| `explain_incomplete_test_results.jsonl` | yes | Crafted `state-probe` requests only (`experiments/incomplete_test_results.py`, #75): a data test confirmed with no or partial `execution_results`, then resubmitted; hosted SKIPs and serves them, keyed as accepted divergences (#87); `conformance.rs` and `data_test_explain.rs`. |
| `explain_freshness_cells.jsonl` | yes | Client 2.0.11 (#104): ALL-policy reuse with one of two sources drifted (five incrementals, custom), drift past a `0s` Lag tolerance (insert_overwrite, delete+insert, microbatch), a stale older match after a logic revert under drift (every kind but view), and a snapshot rebuilt past the default tolerance; two windows concatenated, `experiments/explain_capture.py freshness_cells`, then `freshness_cells_snapshot --run-id`; `conformance.rs`. |
| `explain_rebuild_tolerance_line.jsonl` | yes | Crafted `state-probe` requests only (`experiments/rebuild_tolerance_line_probes.py`, #104): data-only rebuilds past a 20 s Lag tolerance for FULL, SNAPSHOT and INSERT_OVERWRITE, with and without a current equivalent elsewhere; only the ones reporting an equivalent (1 / 3, 1 / 6) carry a Lag tolerance line; `conformance.rs`. |
| `explain_unknown_epoch.jsonl` | yes | Crafted `state-probe` requests only (`experiments/unknown_epoch_probes.py`, #104): a FULL model whose upstream epoch is unknown at the build, the resubmit or both; hosted SKIPs within the Lag tolerance, keyed as accepted divergences (#113); `conformance.rs`. |
| `explain_copy_logic_mismatch.jsonl` | yes | Client 2.0.11, `experiments/explain_capture.py copy_logic_mismatch` (#104): rebuilds over a `RegisterClone` pre-step copy after a logic change (five incrementals, a snapshot) or a materialization change (a table: 2 / 2; a view and a custom node: 6 / 6), and a table with a prod equivalent under `allow_clones: false` (1 / 10, "cloning is disabled"); `conformance.rs` RegisterClone and Explain arms. |
| `explain_copy_logic_crafted.jsonl` | yes | Crafted `state-probe` requests only (`experiments/copy_logic_probes.py`, #104), including `RegisterClone`: a table over an append copy with other logic, every model kind over a copy without upstreams, and every model kind over a dropped copy (`x11`–`x19`); `conformance.rs`. |
| `rpc_success_and_rejection.jsonl` | yes | Warehouse-free hosted calls (2026-11-12, #133): version validation succeeds; a selector without `project_id` returns `INVALID_ARGUMENT: project_id must be provided`. Status/response metadata and field presence replay in `rpc_capture_replay.rs` and `conformance.rs`; no history exemption. |
| `explain_malformed_requests.jsonl` | yes | Crafted `state-probe` requests only (`experiments/malformed_requests.py`, #104): a node of each kind whose query dependency is listed twice, built then resubmitted; hosted SKIPs, keyed as accepted divergences (#114). The empty-SQL and empty-dependency probes failed with `INVALID_ARGUMENT` and were not recorded; `conformance.rs`. |
| `clone_chain_upstream_history.jsonl` | char-only | A copy chain whose consumed upstream copy is externally modified, which rebuilds within tolerance; `lag_tolerance.rs::clone_chain_external_modification_of_consumed_upstream_rebuilds`. |
| `clone_chain_upstream_drift.jsonl` | yes | A copied target rebuilds once its upstream advances past the copy's build epoch; `clone_chain_provenance.rs::hosted_clone_rebuilds_when_upstream_advances_past_clone`. |
| `clone_chain_source_age.jsonl` | char-only | A `RegisterClone` chain whose final resubmit hosted skips and the server rebuilds (pinned divergence); `clone_chain_provenance.rs::registerclone_source_age_chain_exact_replay`. |
| `clone_rejected_allow_clones_false.jsonl` | yes | `RegisterClone` with `clone_chain_depth_limit=0` answered `unable_to_clone`; `clone.rs::register_clone_rejected_when_allow_clones_false`. |
| `clone_submit_equivalent_fallback.jsonl` | yes | Client 2.0.6 SQL submits answered `ready_to_clone` from an equivalent under another name: a MERGE incremental over an existing target (entry 1, which this server builds: keyed divergence, #101 question 3) and a FULL model (entry 2); `clone_submit_fallback.rs::hosted_submit_fallback_shape_is_pinned`. |
| `clone_submit_allow_clones_false.jsonl` | char-only | The same fallback suppressed by `clone_chain_depth_limit=0`; `clone_submit_fallback.rs::hosted_allow_clones_false_suppression_shape_is_pinned`. |
| `clone_fallback_over_existing_target.jsonl` | char-only | The cross-name fallback firing over a target relation that already has its own history; `clone_submit_fallback.rs::hosted_fallback_fires_over_existing_target`. |
| `clone_rebuild_execution_type.jsonl` | char-only | Copy, incremental rebuild, then copy again over one target relation; no test loads it, and `clone_history_provenance.rs::clone_adoption_must_not_resurrect_row_of_different_execution_type` reproduces the scenario. |
| `clone_fallback_xenv_fresh.jsonl` | char-only | A cross-target fallback copy from an equivalent over a differently named upstream; `clone_fallback_xenv.rs::hosted_cross_env_clone_fallback_shape_is_pinned`. |
| `clone_fallback_swapped_bindings.jsonl` | yes | Swapping two dependency bindings builds instead of copying; `clone_submit_fallback.rs::hosted_swapped_bindings_execute_not_clone`. |
| `clone_fallback_dep_binding_swap.jsonl` | yes | The same swap through two view dependency definitions, with a keyed cross-run source leak; `clone_submit_fallback.rs::hosted_dep_binding_swap_executes_shape_is_pinned`. |
| `clone_fallback_stale_source.jsonl` | yes | Hosted copies a source older than the current upstream; `clone_eligibility.rs::hosted_clones_stale_source_with_captured_provenance`. |
| `clone_fallback_zero_tolerance.jsonl` | yes | The same stale-source copy with zero tolerance; `clone_eligibility.rs::hosted_zero_tolerance_clones_stale_source_shape_is_pinned`. |
| `clone_fallback_volatile.jsonl` | yes | A volatile model under volatile evaluation builds instead of copying (field-scoped gap); `clone_eligibility.rs::hosted_volatile_executes_not_clones_shape_is_pinned`. |
| `selector_state_selection.jsonl` | char-only | A baseline build then `GetStateSelection` queries for every criterion; `selector_conformance.rs::get_state_selection_reproduces_hosted_responses`. |
| `selector_last_write_wins.jsonl` | char-only | The selector baseline follows the newest recorded build; `selector_conformance.rs::last_write_wins_baseline_reproduces_hosted`. |
| `deferral_resolve.jsonl` | char-only | `ResolveDeferredRelations` mapping deferred nodes to defer-to target relations; `deferral.rs::hosted_resolution_shape_is_pinned`. |

`crates/harness/tests/fixture_guard.rs` checks that every fixture epoch is in
milliseconds.

## `upgrade/`

Frozen bytes captured from server revision `366be09`, independent of current
hashing code:

- `prefix_clone_366be09.json`: confirmed and pending result-copy rows plus the
  requests that produced them;
  `clone_submit_fallback.rs::upgrade_rejects_frozen_366be09_clone_fixture_all_cases`.
- `incorrect_clone_366be09.json`: an unsafe result copy for a swapped
  dependency binding;
  `clone_submit_fallback.rs::upgrade_rejects_frozen_366be09_incorrect_clone_fixture_all_cases`.

Both tests assert the current server rebuilds instead of reusing these rows.
Regenerate only from the recorded revision with the `#[ignore]`d
`zz_capture_prefix_clone_fixture` / `zz_capture_incorrect_clone_fixture` drivers.
