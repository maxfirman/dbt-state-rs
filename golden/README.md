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
