# Golden traffic

Captured real request/response pairs from the hosted dbt State service
(`api.state.dbt.com`), used as differential-test fixtures.

## Layout
- `fixtures/` — the curated, **git-tracked** corpus the tests replay. Treat as
  read-only test data.
- `golden/` (this dir, top level) — the recording proxy's live output dir. The
  `record-proxy` binary writes `golden_<timestamp>.jsonl` here each session.
  These are **gitignored** (`/golden/golden_*.jsonl`) and ephemeral. Promote a
  useful capture by copying it into `fixtures/`:

      cp golden/golden_<ts>.jsonl golden/fixtures/

Do not leave session files duplicated at the top level — they are ignored and
only clutter the tree. Only `fixtures/` is referenced by the test suite.

## Fixture corpus (all referenced by the conformance replay)

| File | Entries | What it exercises |
|---|---|---|
| `golden_20261008T222744.061Z.jsonl` | 22 | execute→confirm→skip lifecycle, speculative=undecided, baseline skips |
| `golden_20261009T083754.534Z.jsonl` | 17 | cross-environment namespace reuse (prod execute → dev skip) |
| `golden_20261009T100637.316Z.jsonl` | 6  | `RegisterClone` → ready_to_clone + ResolveDeferredRelations |
| `clone_happy_path.jsonl` | 8 | profile-deferral clone: cross-env skip + RegisterClone |
| `clone_failed_fallback.jsonl` | 4 | **characterized gap**: `SubmitEnrichedSQL` → `ready_to_clone` ("an equivalent model exists under another name") — pinned by `submit_enriched_sql_clone_fallback_is_characterized`, NOT reproduced from an empty store (SKIP/EXECUTE/CLONE routing there depends on physical warehouse state absent from the protocol) |
| `c1_config_vs_logic.jsonl` | 6 | **characterized divergence** (live-captured): same node across a no-op rebuild, a config-only `config(meta=…)` edit, and a genuine SQL change. Proves the hosted service fingerprints SQL *semantics* server-side (skips the config-only change despite a changed `node_body_hash`) while our body-hash match over-executes. Pinned by `crates/harness/tests/c1_probe.rs` |
| `c1_config_semantics.jsonl` | 12 | **characterized per-config-key policy** (live-captured): one config change at a time on the same node. The hosted service skips `tags`/`meta`/`post_hook` (incl. a warehouse-mutating `ALTER TABLE … SET COMMENT`, verified not to run) but executes on `grants`/`pre_hook`/`persist_docs`. Pinned by `crates/harness/tests/c1_config_semantics.rs` |
| `c1_test_node_identity.jsonl` | 5 | **implemented behavior** (live-captured): dbt data-test nodes (`execution_type=8`) share one `node_body_hash` per generic type and have an empty `target_table`; the hosted service distinguishes them by `node_unique_id`. Adding a new column test executes it despite the shared body hash. Drives `find_confirmed_by_unique_id`; pinned by `crates/harness/tests/c1_test_node_identity.rs` |
| `lag_freshness_own_build.jsonl` | 10 | **implemented behavior** (live-captured): `lp_model` (`lag_tolerance=5m`) over `lp_upstream`, whose physical table was recreated in place with identical content to advance its modification epoch WITHOUT any dbt SQL change (isolating pure data freshness). The hosted service rebuilds iff `upstream_epoch > own_last_build_epoch` — comparing against the node's own build epoch, with `freshness_tolerance_seconds` NOT absorbing the gap. Drives `decision.rs::is_stale`; pinned by `crates/harness/tests/lag_freshness.rs` and docs/correctness-handoff.md §C |
| `lag_multi_upstream_policy.jsonl` | 84 | **CHARACTERIZATION-ONLY** (live-captured, `experiments/lag_experiment.sh`): `lp_model2` cross-joins two independent upstreams; `require_fresh_data_from` ANY vs ALL with per-upstream epoch bumps. Proves ALL rebuilds only when EVERY direct parent drifted (SKIP otherwise, even at `lag_tolerance=0`); `is_stale=true` only on a tolerated-stale SKIP; and — overturning the prior conclusion — that `lag_tolerance` CAN gate a rebuild under ANY policy (ANY skips with "…its data is within lag tolerance"). An interleaved multi-run capture: EXCLUDED from the generic causal replay and driven by `crates/harness/tests/lag_policy.rs`. See handoff §C. |
| `clone_rejected_allow_clones_false.jsonl` | 2 | **implemented behavior** (live-captured): `allow_clones: false` on the target makes the client send `RegisterClone` with `clone_chain_depth_limit=0`; the hosted service answers `unable_to_clone` (decision=1 READY_TO_EXECUTE, `clone_rejection_reason=10` CLONE_CHAIN_LIMIT_EXCEEDED, empty deprecated oneof) and writes no history. Drives `services.rs::register_clone` gating; pinned by `crates/harness/tests/clone.rs::register_clone_rejected_when_allow_clones_false` |
| `clone_submit_equivalent_fallback.jsonl` | 3 | **implemented behavior** (live-captured): a `SubmitEnrichedSQL` for an absent target, when an equivalent model (same `table_namespace` + schema-abstracted logic + `execution_type`) exists under a DIFFERENT physical name, returns `ready_to_clone` cloning that equivalent in (decision=3, `skip_rejection_reason=1` TARGET_TABLE_MISMATCH, "an equivalent model exists under another name so we cloned that one", `clone_required_last_modified_epoch`=source build epoch). Drives `services.rs` SQL-submit cross-name fallback (`fingerprint::namespace_logic_hash`, `store::find_clone_equivalent`); pinned by `crates/harness/tests/clone_submit_fallback.rs` |
| `clone_fallback_over_existing_target.jsonl` | 3 | **CHARACTERIZATION-ONLY** (live-captured, `experiments/p0_fallback_over_existing.sh`): the SAME `clone_probe` table target submitted three times — logic L1 with the target absent (`ready_to_clone`), logic L2 added-column (`ready_to_execute`, no equivalent under another name), then logic L1 again with the target NOW EXISTING and holding L2 (`ready_to_clone`). Proves the cross-name clone fallback fires over a target that already has its own confirmed build history: the discriminator is purely "does an equivalent for THIS logic exist under another name", NOT target existence. Overturned an inferred `!target_has_confirmed_history` gate; underpins review issue #1 P0#2's `ready_to_clone` outcome. EXCLUDED from the generic causal replay; pinned by `crates/harness/tests/clone_submit_fallback.rs::hosted_fallback_fires_over_existing_target` and `clone_history_provenance.rs` |
| `selector_state_selection.jsonl` | 159 | **implemented behavior** (live-captured, `experiments/selector_experiment.sh`): dbt State-powered `state:*` selectors (`SelectorService.GetStateSelection`). A jaffle-shop baseline built in `prod_demo` (49 `SubmitEnrichedSQL`/`SubmitValues` + their `ConfirmExecution`s record each node's `dbt_node_state`), then 67 distinct `GetStateSelection` queries from `dev_clone` (`defer_to_target: prod_demo`, so `target=prod_demo`) across all 10 `SelectorCriteria` and every change type (body/configs/contract/macros/persisted_descriptions/relation, metadata-only, whitespace, add-node). Proves: `new`/`old` and `modified`/`unmodified` are exact complements of the sent set; `modified = new ∪ body ∪ configs ∪ persisted_descriptions ∪ macros ∪ contract` (**excluding** `relation`); each `modified.X` isolates its own component hash; a new node (no recorded state) matches every `modified.*`; untracked node types (unit_test/source/semantic_model/metric/saved_query) are always `new`/`modified`; the server does a PURE comparison of the client-supplied `DbtNodeData` hashes against the recorded ones (no recompute), keyed by (`target` env, `project_id`, `node_unique_id`). An interleaved multi-mutation capture: EXCLUDED from the generic causal replay (`CHARACTERIZATION_ONLY`) and driven by `crates/harness/tests/selector_conformance.rs` |
| `selector_last_write_wins.jsonl` | 10 | **CHARACTERIZATION-ONLY** (live-captured, `experiments/selector_last_write_wins.sh`): `stg_customers` recorded in `prod_demo` (V1), `state:modified` queried from `dev_clone` (not selected), then `stg_customers` re-recorded with a changed body (V2), and `state:modified` queried again (now selected). Proves the selector comparison baseline is LAST-WRITE-WINS — the newest recorded execution in the defer-to env defines the comparison base. Validates `store::upsert_node_state`'s latest-wins upsert. EXCLUDED from the generic causal replay; pinned by `crates/harness/tests/selector_conformance.rs::last_write_wins_baseline_reproduces_hosted` and `selector_offline.rs::selector_baseline_is_last_write_wins` |

The fixtures in this directory are auto-discovered and replayed by
`crates/harness/tests/conformance.rs` (`conformance_replay_all_corpora`), which
asserts only the causally-reproducible transitions and hydrates/characterizes
the rest. Interleaved multi-run captures (e.g. `lag_multi_upstream_policy.jsonl`)
are in the replay's `CHARACTERIZATION_ONLY` exclusion list and validated by their
own dedicated tests instead.
