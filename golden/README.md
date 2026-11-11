# Golden traffic

Captured request/response pairs from the hosted dbt State service
(`api.state.dbt.com`), used as test fixtures. How they are replayed and what a
pass proves is in [docs/testing.md](../docs/testing.md#captured-traffic-replay).

## Layout

- `fixtures/`: the curated, git-tracked corpus. Treat it as read-only test data.
- `replay_gaps.json`: keyed known divergences for the replay
  ([semantics](../docs/testing.md#goldenreplay_gapsjson)).
- `upgrade/`: frozen rows from an old server revision (below).
- Top level: the recording proxy's output directory. `record-proxy` writes
  `golden_<timestamp>.jsonl` here each session. These files are gitignored
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
| `golden_20261008T222744.061Z.jsonl` | yes | Build, confirm and skip lifecycle with speculative submits and version checks; `differential.rs`. |
| `golden_20261009T083754.534Z.jsonl` | yes | Prod build then dev skip through namespace reuse, with cross-target data-test entries keyed in `replay_gaps.json`; `conformance.rs`. |
| `golden_20261009T100637.316Z.jsonl` | yes | `RegisterClone` to `ready_to_clone` plus an empty `ResolveDeferredRelations`; `clone.rs::register_clone_matches_real_shape`, `deferred_relations_empty_when_nothing_built`. |
| `clone_happy_path.jsonl` | yes | Profile-deferral result copy: cross-target skip plus `RegisterClone`; `clone_lifecycle.rs::clone_happy_path_register_confirm_shapes`. |
| `clone_failed_fallback.jsonl` | yes | SQL submit answered `ready_to_clone` from a source not in the capture (keyed gap); `conformance.rs::submit_enriched_sql_clone_fallback_is_characterized`. |
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
| `explain_custom_copy.jsonl` | yes | Client 2.0.11 custom node and FULL control built in `dev_clone` over prod equivalents: hosted copies only the FULL one (keyed divergence #86), from `experiments/explain_capture.py custom_copy`; `conformance.rs`. |
| `explain_snapshot_history.jsonl` | yes | Client 2.0.11 snapshot Explain trees (first build, SKIPs, source drift, upstream and logic changes, `--full-refresh`) plus FULL and VIEW logic-change rebuilds, from `experiments/explain_capture.py snapshot_explain`; `conformance.rs` Explain arm. |
| `explain_incremental_append_merge_history.jsonl` | yes | Client 2.0.11 APPEND and MERGE incremental Explain trees (first build, SKIP, logic change, upstream drift at the default and a 0s Lag tolerance, `{{ this }}` self-read), an unset strategy sent as APPEND, and a `--full-refresh` run recorded with `RecordExecutions` then rebuilt (divergence #90), from `experiments/explain_capture.py incremental_append_merge`; `conformance.rs`. |
| `explain_incremental_copy_history.jsonl` | yes | Client 2.0.11 dev-clone pre-step copy of an append and a merge incremental (`RegisterClone`, confirm) and the post-copy submit hosted skips (divergence #93), from `experiments/explain_capture.py incremental_copy`; `conformance.rs` (`RegisterClone` arm). |
| `explain_incremental_copy_over.jsonl` | yes | Client 2.0.11, one model per incremental strategy (append, merge, insert_overwrite, delete+insert, microbatch; `state.pre_clone: never`, upstream `RAW.RAW_ORDERS`) built in `prod_demo`, in `dev_clone` with other logic, then in `dev_clone` with the prod logic over the existing dev relation: hosted rebuilds all five (1 / 6, or 1 / 2 for append and delete+insert) where this server copies (keyed gaps, #101). One attempt, from `experiments/explain_capture.py incremental_copy_over`; `conformance.rs`, `explain.rs::incremental_clone_emits_nothing`. |
| `explain_incremental_submit_copy.jsonl` | yes | Client 2.0.11 append and merge with `state.pre_clone: never` submitted into absent dev relations while their prod builds exist: hosted builds them (skip code 1, #93) and copies nothing. One probe, from `experiments/explain_capture.py incremental_submit_copy`; `conformance.rs`. |
| `explain_copy_probes.jsonl` | yes | Client 2.0.11, `experiments/explain_capture.py copy_probes` (#76, #78): a seed, a snapshot and a view built in `prod_demo` then `dev_clone`. Hosted copies the seed (#91) and builds the view and snapshot with codes 1 / 3 (#92), all keyed; `conformance.rs`. |
| `explain_snapshot_register_clone.jsonl` | yes | Client 2.0.11, `experiments/explain_capture.py snapshot_register_clone` (#76): a snapshot copied into `dev_clone` by `RegisterClone`, then its post-copy SKIP, which this server rebuilds (#93, keyed); `conformance.rs` RegisterClone and Explain arms. |
| `explain_data_test_history.jsonl` | yes | Client 2.0.11 data-test Explain trees (first execution, unchanged rerun, logic change, upstream data drift) for a passing, a failing and a `store_failures` singular test, from `experiments/explain_capture.py data_test_execute`; the `store_failures` audit relation decisions are keyed divergences (#88); `conformance.rs` Explain arm and `data_test_explain.rs`. |
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
| `explain_ambiguous_rebuilds.jsonl` | yes | Crafted `state-probe` requests only (`experiments/ambiguous_rebuilds.py`, #71): a model resubmitted with its Target relation listed twice (hosted SKIP) or with no epoch (hosted first-build rebuild), keyed as divergences (#80); the builds of the no-target and no-epoch-confirm probes, whose failed hosted calls were not recorded; `conformance.rs`. |
| `explain_rebuild_reason_gaps.jsonl` | yes | Client 2.0.11 (#40 code review): a FULL model whose logic changed while its source drifted (SHAPE 7 with an OUTDATED listing against the unmatched build), and data tests over a model whose logic changed, with no upstreams (first execution, logic change, a SKIP hosted decides on the speculative submit, keyed divergence #103) and over two upstreams (drift, SKIP), from `experiments/explain_capture.py rebuild_reason_gaps`; `conformance.rs` (speculative-gap arm). |
| `explain_incomplete_test_results.jsonl` | yes | Crafted `state-probe` requests only (`experiments/incomplete_test_results.py`, #75): a data test confirmed with no or partial `execution_results`, then resubmitted; hosted SKIPs and serves them, keyed as divergences (#87); `conformance.rs` and `data_test_explain.rs`. |
| `clone_chain_upstream_history.jsonl` | char-only | A copy chain whose consumed upstream copy is externally modified, which rebuilds within tolerance; `lag_tolerance.rs::clone_chain_external_modification_of_consumed_upstream_rebuilds`. |
| `clone_chain_upstream_drift.jsonl` | yes | A copied target rebuilds once its upstream advances past the copy's build epoch; `clone_chain_provenance.rs::hosted_clone_rebuilds_when_upstream_advances_past_clone`. |
| `clone_chain_source_age.jsonl` | char-only | A `RegisterClone` chain whose final resubmit hosted skips and the server rebuilds (pinned divergence); `clone_chain_provenance.rs::registerclone_source_age_chain_exact_replay`. |
| `clone_rejected_allow_clones_false.jsonl` | yes | `RegisterClone` with `clone_chain_depth_limit=0` answered `unable_to_clone`; `clone.rs::register_clone_rejected_when_allow_clones_false`. |
| `clone_submit_equivalent_fallback.jsonl` | yes | SQL submit for an absent target answered `ready_to_clone` from an equivalent under another name; `clone_submit_fallback.rs::hosted_submit_fallback_shape_is_pinned`. |
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
