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

All five are replayed by `crates/harness/tests/conformance.rs`
(`conformance_replay_all_corpora`), which asserts only the causally-reproducible
transitions and hydrates/characterizes the rest.
