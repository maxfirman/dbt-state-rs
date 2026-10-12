# Testing

Correctness comes first. The offline suite (below) needs only Postgres and makes
no hosted/Snowflake calls; it is the day-to-day gate. Behaviors are additionally
characterized live against the real service via the recording proxy and the
`experiments/` drivers (see [harness.md](harness.md)) — capture first, promote a
golden fixture, then assert. Do not invent hosted behavior; document open
questions in the [handoff](correctness-handoff.md), which also lists remaining
work and the independent state-machine testing plan.

## Local checks

```bash
cargo test --workspace                          # requires Postgres
cargo test -p dbt-state-server --lib             # no database required
cargo fmt --all --check
cargo clippy --workspace --all-targets --features fuzz -- -D warnings
```

Rust 1.98 is selected by rust-toolchain.toml. Integration tests create isolated
Postgres schemas and run all migrations. Capture's database test lives in
`crates/server/tests/capture.rs`, keeping library tests independent of Postgres.

## Offline regression tests

`crates/harness/tests/offline_regressions.rs` exercises public gRPC decisions and
persistent state: overwritten history, target existence/external edits, scoped
cached test outcomes, dependency/context fingerprints, unknown/ambiguous
freshness, seed configuration, duplicate confirms, Record/Submit parity,
corruption, old fingerprints and unsupported RPC/clone errors. UI read tests
verify organization isolation across every database read operation.

SQL normalization combines positive captured equivalences with negative cases
(cast kind, length/scale, qualified function identity) and an arbitrary UTF-8
no-panic property. Pure decision properties test determinism and existing
freshness/policy assumptions for already selected candidates; they are not an
independent oracle for the complete service or hosted lag semantics.

## Captured traffic replay

`crates/harness/tests/conformance.rs` auto-discovers the `golden/fixtures/`
corpus (24 causally-replayed fixtures; interleaved multi-run captures such as
`lag_multi_upstream_policy.jsonl` are excluded via a `CHARACTERIZATION_ONLY` list
and validated by dedicated tests). Invalid trace lines fail with a line number.
It preserves complete captured confirmation outcomes and correlates hosted/local
request IDs directly. It asserts hosted rebuild variants even after previous
confirms, and compares complete causal SKIP responses after stripping only
opaque IDs. Version/speculative responses are also compared. Dedicated tests
cover captured materializations, seed/test lifecycles, both RegisterClone
response mirrors, the clone register→confirm→reuse lifecycle
(`clone_lifecycle.rs`), the `allow_clones=false` `unable_to_clone` rejection
(`clone.rs`), the SQL-submit cross-name clone fallback
(`clone_submit_fallback.rs`), the multi-upstream ANY/ALL + `is_stale`
behavior (`lag_policy.rs`), and the dbt State-powered `state:*` selectors
(`selector_conformance.rs` replays 67 captured `GetStateSelection` queries
across all 10 criteria and asserts set-equality; `selector_offline.rs` exercises
the record path and org/project/target scoping; plus the pure
`selector::classify` unit tests in the server crate).

Leading hosted SKIPs and uncorrelated confirms depend on unknown initial
history; they are counted, never hydrated with guessed epochs/results. Automatic
cloning and deferral are counted gaps. Known causal mismatches must be explicitly
identified by fixture entry and reason, with the rest of the response checked
where possible. See the handoff for the exact remaining replay limitations.
Passing these tests is not proof that every captured response or feature is
implemented.

## Hosted discovery (differential fuzz)

The opt-in differential tool uses real credentials/metering:

```bash
cargo run -p dbt-state-harness --features fuzz --bin diff-fuzz -- \
  --seeds golden/fixtures/<file>.jsonl --iterations 200
```

It needs a dbt Cloud credential in `~/.dbt/dbt_cloud.yml` and burns dbt State
metering, so run it deliberately. Its local side has empty history and it
compares variants only; zero divergences does not prove reuse conformance.
Decode/transport failures, missing comparisons and reported
divergences now fail the process. Future work must establish equivalent state,
compare complete responses/errors, save reproducible scenarios and shrink
stateful divergences. Promote useful controlled cases into golden/fixtures;
keep raw session captures out of the tree.
