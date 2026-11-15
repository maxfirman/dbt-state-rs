# Databricks MICRO rebuild description correction — 2026-11-15

## Captured behavior and correction

The complete official-client 2.0.11 hosted MICRO6 campaign passed all ten
physical checkpoints. Its 70-RPC capture exposed two local description
mismatches: ordinary Submit entry 17 and Explain entry 21. Both services already
returned BUILD 1, rejection codes 6/6, `is_stale=false`, and the same complete
Explain tree. The source and Target relation independently advanced known
positive epochs. Hosted described this as `model was executed because either
its query didn't match or its upstream data is out of date`; local described
only the Target's External modification.

A fresh-schema public gRPC replay of actual entries 3→5→17→21 reproduced both
mismatches in 0.90s and 0.92s. Removing Confirm eliminated that symptom; restoring
the intervening actual SKIP changed neither mismatch. Resetting only the
Target epoch to its confirmed epoch made the entire replay match. Focused
inspection confirmed own-target qualification selected `OwnTargetModified`
before the independently stale source could affect description wording.

The correction changes only `DecisionRecord.description`. A named Warehouse
rule dispatches Databricks MICRO6; shared evidence checks require an ordinary
non-copy row with equal nonempty rendered identity, prior reuse qualification,
`OwnTargetModified`, zero Lag tolerance/ANY, a strictly advanced positive
Target epoch, and independently advanced genuine upstream evidence. Every
evaluated upstream must have known-positive current/recorded epochs, with no
unknown evidence or producer change. Decisions, reason, rejection codes,
`is_stale`, freshness, eligibility and Explain anchors/tree remain unchanged.
Other kinds/dialects and Target-only policy are outside this correction.

The public regression compares complete captured responses with transport IDs
alone normalized. Eighteen adjacent evidence controls cover unchanged,
unknown, older and zero epochs (including a mixed valid+zero second source),
other kind/dialect, Lag/ALL, and changed SQL/config. The captured regression
failed before the fix while adjacent controls passed. Final focused tests
passed 2/2 in 1.82s.

## Entire capture admission

Strict replay of all 129 prior corpora plus the original entire70 passed in
249.26s: 60 new assertions, zero new gaps, unknown baselines or partial-history
allowances. No masks, selected prefixes or replay exceptions were introduced.
The whole file was then copied byte for byte into
[`databricks_microbatch_hosted_history.jsonl`](../../golden/fixtures/databricks_microbatch_hosted_history.jsonl),
and the corpus count advanced to 130. Its SHA-256 is
`cdc76f6a3a47188bcb3026885ca14040a14ca53de474a0a75ad6a37ca583e6a0`.
The original integrity sidecar is 70 started/70 recorded/complete=true/failed=false.
The trace includes ten ordinary submits (five BUILD/Confirm, five SKIP), ten
Explain, ten speculative, ten validation and 25 telemetry calls.

Exact first replay command, from the correction worktree:

```sh
env -u DBT_STATE_EXTRA_REPLAY_CLOCKS \
  CARGO_TARGET_DIR=/home/maxfirman/.cache/dbt-state-150desc-target \
  CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 \
  CARGO_BUILD_JOBS=3 \
  DBT_STATE_EXTRA_REPLAY_CAPTURE=/home/maxfirman/.local/state/dbt-state-rs/databricks-150-20261115/replay/databricks_microbatch_hosted_history.jsonl \
  cargo test -p dbt-state-harness --test conformance \
  conformance_replay_all_corpora -- --exact --test-threads=2 --nocapture
```

## Uniform current-source verification

All gates below passed against the same unchanged 665-file source, including
the admitted 130 fixtures, based on canonical
`9e6919c1ae6d82c1c6d127cc5cefc379c27fc500`. The structured gate source freeze
SHA-256 is `e1f51b6b0ed6bd4d0dac08bf1a7f25824ab70179d078da337ccf480bc2c7040f`.
The earlier line-based 665-file manifest SHA-256 is
`6a0175c72f2a41def17d3d7f50e0439e0b442dc353876e65c3c7892db05079a5`;
these hash different manifest encodings of the same source inputs.

| Gate | Command | Result | Command seconds |
|---|---|---|---:|
| Workspace | `cargo test --workspace` | 698 passed/0 failed/54 ignored; all 19 conformance tests, 130 corpora |777.04 |
| No-DB library | `cargo test -p dbt-state-server --lib` | 310 passed with invalid DSN; test runtime 0.78s |28.22 |
| Strict Clippy | `cargo clippy --workspace --all-targets --features fuzz -- -D warnings` | passed |63.12 |
| Format | `cargo fmt --all -- --check` | passed |4.86 |
| Python | `integrations/duckdb/.venv/bin/python -m unittest discover -s experiments -p '*_test.py'` | 149 passed |5.81 |
| Fresh DuckDB | `./scripts/duckdb-ci.sh --jobs 3 --artifacts <gate-dir>/duckdb-artifacts` | 25/25; setup 156.61s, acceptance 710.77s |867.54 |

Exact shared overrides were `CARGO_TARGET_DIR=/home/maxfirman/.cache/dbt-state-150desc-target`,
`CARGO_BUILD_JOBS=3`, `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`,
`CARGO_INCREMENTAL=0`, `RUST_TEST_THREADS=2`, and
`DATABASE_URL=postgres://dbtstate:dbtstate@127.0.0.1:55441/dbtstate`.
DuckDB used the separate target `/home/maxfirman/.cache/dbt-state-150duck-target`;
library used `DATABASE_URL=postgres://invalid:invalid@127.0.0.1:1/invalid`.
The runner removed `DBT_STATE_EXTRA_REPLAY_CAPTURE`, `DBT_STATE_EXTRA_REPLAY_CLOCKS`,
`COPY_SCENARIO`, `VIEW_SCENARIO`, `SEED_SCENARIO`, `INTERRUPT_ONLY_PHASE`.
Other inherited environment was not snapshotted; credential-bearing values
were intentionally excluded. `<gate-dir>` is the retained directory below.

Gate JSON receipts, exact commands/environment and hash-verified logs remain at
`/home/maxfirman/.local/state/dbt-state-rs/databricks-150-20261115/description-fix-gates/`.
Receipt SHA-256 values:

| Receipt | SHA-256 |
|---|---|
| `workspace.json` | `230914e8dcbcbbc299befd047a80fcf705f3dc5621e351e12ef7640a16e1c159` |
| `library.json` | `e5cb9b4c2556ecaa6ff9e91416be813ee991d5539be2bc67a6f9bfa4639bddf5` |
| `clippy.json` | `a5e29abbb7c729010198d519d48e95a20ab36c72ecc767bdf3511e1367bcada2` |
| `fmt.json` | `eed4c6ef4375b8d82167a3c498c91560368538c31786ee563372e33112fe532b` |
| `python.json` | `d418a2d1add8d4af628dc13c0835d3e97af002f8259db90b71b5636b336006ac` |
| `duckdb.json` | `938926225347124dcff5d762b7f42856192acf28f8076f3c81c198a7263a7a84` |

Diagnosis, exact red/green logs and first strict-replay command/output are
retained under `/tmp/dbt-state-132-context/ticket150-description-*`.
Hosted physical evidence remains in the campaign's `hosted-audit-receipt.json`;
this correction performed no new cloud or warehouse calls.

## Verification boundary

This report and its link from `golden/README.md` were added **after** the
uniform source freeze and gates. They are documentation-only additions; this
report was not part of the gated source. All 665 frozen files were verified
unchanged before writing them. No production/test/fixture changes followed
the gates.

Hosted physical checkpoints and complete hosted replay pass. The fresh Rust
physical counterpart is **pending**. Whole #150 and #138/#140 remain open;
this descriptor correction does not establish Target-only modification policy
or grant native execution.
