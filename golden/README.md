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

All five are replayed by `crates/harness/tests/conformance.rs`
(`conformance_replay_all_corpora`), which asserts only the causally-reproducible
transitions and hydrates/characterizes the rest.
