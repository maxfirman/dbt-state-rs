# Testing strategy — faithful reproduction & regression guards

Three layers beyond example unit tests, each targeting the core risk: that our
hand-written decision engine diverges from the hidden hosted service in input
regions we haven't observed.

## #1 Exhaustive golden conformance replay  (crates/harness/tests/conformance.rs)
Replays EVERY captured real request/response pair (all corpora) against our
server in recorded order and asserts the **causally-reproducible** transitions
match the hosted service:
- real `ready_to_execute` on an unseen fingerprint => we must execute;
- real `skip_execution` on a fingerprint we have already confirmed in this
  replay => we must skip;
- leading "baseline" skips (pre-capture state) are hydrated, not asserted;
- `ValidateClientVersion` / `SubmitEnrichedSQLSpeculative` asserted exactly.
Runs in CI (needs Postgres). This is the strongest regression gate.

### Cross-environment reuse (CLOSED — was a gap surfaced by #1)
`cross_environment_namespace_skip`: the hosted service matches a node across
environments by `table_namespace` + `node_body_hash`, ignoring physical
`default_schema` (prod execute -> dev skip, corpus 2). IMPLEMENTED:
store::find_confirmed_by_namespace matches confirmed state by table_namespace +
node_body_hash + execution_type (org-scoped); decision::logical_relation_key
compares upstream freshness by schema-stripped logical identity. Verified by unit
tests, the conformance replay (now un-ignored), and live across two Snowflake
schemas (prod_demo -> analytics_dev).

## #3 Property-based decision invariants  (crates/server/src/decision_proptest.rs)
`proptest` over the pure `decide()` function asserting PROTOCOL INVARIANTS that
must hold for all inputs (not random outputs):
1. Determinism.
2. No confirmed match => Execute (always).
3. Self-table immunity — the node's own target-table epoch never flips a verdict.
4. Freshness monotonicity — no upstream drift => Skip.
5. Policy ordering — ANY is at least as strict as ALL (ANY skip => ALL skip).
Runs in CI, fast, no DB.

## #2 Differential fuzzing vs the REAL service  (crates/harness/src/bin/diff_fuzz.rs)
OPT-IN tool (NOT CI — burns dbt State metering + needs ~/.dbt/dbt_cloud.yml).
Semantic/structural fuzzing: mutates real seed requests along meaningful axes
(stale_upstream_policy, per-input epoch around the freshness boundary,
execution_type, add/drop upstreams, forced body-hash miss, tolerance), sends
each to BOTH the real service and our in-process server (empty state), and
classifies:
- agree (execute): reproducible agreement — the high-signal "we match" result;
- real-skip/we-execute: expected (real has prior history we lack);
- DIVERGENCE: real executed but we skipped, or structural mismatch — a real
  bug / candidate new golden case.

Run:
    cargo run -p dbt-state-harness --features fuzz --bin diff-fuzz -- \
      --seeds golden/fixtures/golden_20261008T222744.061Z.jsonl --iterations 200

Last smoke run (20 mutants): 7 agree-execute, 13 real-skip/we-execute, 0
divergences. Promote interesting real-skip cases into golden/fixtures/ to grow
the #1 conformance corpus.

## Why NOT certain techniques
- Naive protobuf byte-fuzzing: tests prost/tonic decoding, not our logic.
- Coverage-guided whole-server fuzzing: the interesting logic is a tiny pure
  function; property tests (#3) cover it far more cheaply than fuzzing the async/
  DB plumbing.
- Load/perf testing: irrelevant to faithful-reproduction goal.
- Cross-dialect clone-DDL fuzzing: we've only observed Snowflake output; fuzzing
  BigQuery/Databricks would assert against our own guesses, not the real service.
