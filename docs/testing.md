# Testing

The offline suite needs only Postgres and makes no hosted or Snowflake calls;
it is the day-to-day gate. Hosted behavior is characterized live through the
recording proxy and the `experiments/` drivers ([harness.md](harness.md)):
capture first, promote a golden fixture, then assert. Do not invent hosted
behavior. Record open questions in [correctness-handoff.md](correctness-handoff.md).

## Validation

CI (`.github/workflows/ci.yml`) runs the first, third, fourth and fifth
commands below. Run all of them before a change that touches decision logic.

```bash
cargo test --workspace                                   # needs Postgres at $DATABASE_URL
DATABASE_URL=postgres://invalid:invalid@127.0.0.1:1/invalid \
  cargo test -p dbt-state-server --lib                   # proves library tests need no database
cargo fmt --all --check
cargo clippy --workspace --all-targets --features fuzz -- -D warnings
./scripts/duckdb-ci.sh                                   # CI passes --jobs 4
python3 experiments/trace_validate_test.py               # warehouse-free experiment-oracle tests
python3 experiments/explain_census_test.py               # warehouse-free Explain census tests
python3 experiments/explain_capture_test.py              # warehouse-free capture driver tests
python3 experiments/databricks_survey_test.py            # offline Databricks campaign guards/results
```

`rust-toolchain.toml` pins the Rust version. `cargo test --workspace` does not
build the `fuzz` feature, so it never contacts the hosted service.

## Explain tree census

`experiments/explain_census.py` counts the Explain trees in the tracked
fixtures per node kind × decision class: trees by origin (real client and its
version, or crafted `state-probe`), empty trees, and each marker and badge. It
names the fixture and entry of any `INFO`, `EM_UNSPECIFIED`, `EB_UNSPECIFIED`
or unmapped tree. Method and output:
[explain-catalog.md](explain-catalog.md#explain-tree-census).

```bash
python3 experiments/explain_census.py                    # tracked fixtures only
python3 experiments/explain_census.py "$GOLDEN_DIR" more.jsonl   # plus capture files or directories
python3 experiments/explain_census.py --no-fixtures "$GOLDEN_DIR"  # one campaign alone
```

Every #40 capture ticket reruns it after promoting its fixtures. It then
updates, in `docs/explain-catalog.md`, the census table, the vocabulary line
counts, the "not observed across N trees (date)" entries and the evidence
column of the coverage table. A finding (an `INFO` line, an explicit
unspecified value, or a tree no submit minted) needs a catalog entry before the
fixture is promoted.

## Evidence rules and release gate

Two questions need different evidence (GLOSSARY: Hosted conformance, Reuse safety):

- **Reuse safety**: can a SKIP or CLONE serve the wrong physical result? Offline
  tests with an independent oracle can show this. The oracle must not call the
  production fingerprint, freshness or decision code to decide what result
  should exist.
- **Hosted conformance**: does the server return the same gRPC status and full
  response as dbt State for the same starting state and request? Only a
  causally complete hosted capture for a pinned client and warehouse shows this.

Conformance is claimed per **client/version × warehouse × feature slice**, never
for the product as a whole. A slice needs: no wrong-result SKIP or CLONE in the
fixed, generated and upgrade safety tests; zero unclassified mismatches in the
captured-traffic replay; and a tested, documented fallback for every
intentional gap. Offline evidence never promotes an item to hosted-verified.
Fix wrong-result reuse and cross-tenant or cross-warehouse contamination first,
then false rebuilds and missing features, then response fidelity.

## Test support

Integration tests share `crates/harness/tests/support.rs` and its modules in
`crates/harness/tests/support/`.

- `resources.rs` owns `TestServer` and `TestDatabase`. Each creates an isolated
  Postgres schema, runs all migrations and removes the schema on drop, including
  after a panic or failed server startup. `TestServer` also stops and joins its
  server runtime. Tests that call server modules directly (such as
  `submit_decision.rs`) take state from `TestDatabase::state`; the database
  owner must outlive that state.
- `scenario.rs` builds requests that fill in the reuse identity (`model`,
  `view`, `data_test`, `duckdb_model`, `seed`, `clone_request`), outcomes
  (`outcome`, `built_at`, `test_results`), and a `Scenario` client with
  `expect_execute`/`expect_skip`/`expect_clone`, `verdict`, `speculative`,
  `build` and
  `record`. `Submit::body` changes the rendered SQL as well as the body hash,
  because reuse ignores `node_body_hash`. Seed State history through the
  protocol with `Scenario::build` or `Scenario::record`.
- `rows.rs` reads `executions` and `node_states` back for assertions.
- `legacy.rs` (legacy fingerprints, frozen rows, corrupt encodings) and
  `fault.rs` (fault-injection DDL) are the only raw-SQL State writers.
- `python.rs` provides `python_project(...)` for real-dbt cases. Successful
  projects are removed; a failed one is kept and its path reported.

`scenario_guard.rs` fails when a file in its `MIGRATED` list builds requests by
hand, destructures decisions, runs SQL, or when raw State writes appear outside
`legacy.rs`/`fault.rs`. The capture database test lives in
`crates/server/tests/capture.rs`, so server library tests stay independent of
Postgres.

## Offline regressions

- `offline_regressions.rs` and `correctness.rs`: public gRPC decisions and
  persistent state, including overwritten history, target relation existence
  and External modifications, scoped cached test outcomes, dependency and
  context fingerprints, unknown freshness, seeds, duplicate confirms,
  Record/Submit parity, corruption, old fingerprints and unsupported RPCs.
- `warehouse_portability.rs`: independent physical histories across connection
  namespaces and conservative same-target reuse for unverified warehouses. It
  does not show hosted or warehouse parity.
- `speculative_submit.rs`: `SubmitEnrichedSQLSpeculative` SKIPs only an
  unchanged data test without upstreams, with the ordinary SKIP's payload and
  an explainable decision id, and stays `undecided` for a data test with an
  upstream, incomplete cached results, a model without upstreams and another
  organization's history (#103).
- `crates/ui/src/db.rs::every_read_is_scoped_to_the_configured_organization`:
  organization isolation for every UI read.
- `crates/server/src/decision_proptest.rs`: properties of the pure freshness
  policy for an already selected candidate. They are a regression oracle, not
  an independent model of store selection or hosted lag semantics.
- `crates/server/src/sql_norm.rs` tests and `sql_normalization.rs`: captured
  positive equivalences, negative cases and an arbitrary-UTF-8 no-panic
  property ([sql-normalization.md](sql-normalization.md)).
- `lag_tolerance.rs`: pins the hosted Lag tolerance rule (ADR 0007) by fixture
  inspection, including `seed_upstream_arms` (seed upstreams) and
  `copy_candidate_arms` (which build hosted reuses or copies when the latest
  build no longer qualifies). Most of its fixtures also replay in
  `conformance.rs`.
- `data_test_explain.rs`: pins what the data-test Explain captures show by
  fixture inspection: passing and failing tests render the same tree, the
  EXECUTE tree does not depend on the rebuild reason, a `store_failures` test
  reports its audit relation (a table or a view, passing or failing) and
  hosted does not list it (#88), the client names a separate audit schema
  as `default_schema` and carries a configured alias nowhere (#106), and
  hosted serves incomplete cached results
  (#87). All five fixtures also replay in `conformance.rs`.
  `data_test_freshness.rs` checks offline that only the test's own audit
  relation is dropped from its inputs: a new upstream next to it, or a
  table that only resembles it, still rebuilds; an audit relation in a
  separate audit schema is dropped, and so is an aliased one as the only
  table in the test's schema its SQL never mentions, while a new upstream
  next to it (named by the SQL, or a second unnamed table) still rebuilds
  (#123). It also checks that the Explain tree of a test without upstreams
  never lists its audit relation.

## DuckDB acceptance gate

`./scripts/duckdb-ci.sh` installs the locked Python environment, builds all
targets once, and runs every fixed real-dbt case plus bounded generated traces
with independent-oracle mutation probes. Each case gets its own Postgres schema
and DuckDB file; logs land in `artifacts/duckdb/`. A scheduled or manual CI job
runs a larger generated set (`--generated-only`). DuckDB checks establish local
Reuse safety only; Snowflake captures remain the hosted-conformance evidence.
See [duckdb.md](duckdb.md).

## Captured traffic replay

`crates/harness/tests/conformance.rs::conformance_replay_all_corpora` loads
every `golden/fixtures/*.jsonl` not named in its `CHARACTERIZATION_ONLY` list
and replays it through the gRPC server against an empty store. The replayed
corpus size is pinned there. Invalid trace lines fail with a line number.

- Confirms are correlated by mapping hosted request IDs to local ones, and keep
  the captured outcome. A confirm with no local pending request is counted as
  an unknown baseline.
- Hosted `ready_to_execute` responses are compared in full after removing
  opaque IDs. A first-build-versus-rebuild rationale difference is tolerated
  only when the local store has no confirmed history for that workload, and
  only for the exact (fixture, entry) set listed in the test.
- A hosted SKIP is compared in full when the replay has confirmed history for
  that workload. Otherwise it depends on unknown hosted history and is counted
  as an unknown baseline, never hydrated with guessed epochs.
- A hosted SQL-submit `ready_to_clone` is compared in full and its confirm is
  correlated. A `RegisterClone` is driven and its full response compared
  after removing opaque IDs (both the top-level copy and the deprecated
  `ready_to_clone_v1` mirror). A granted copy's confirm is correlated, so the
  copy is State history for the next submit, only when the copy's source was
  confirmed in the same fixture; otherwise the confirm is an unknown baseline.
  `RecordExecutions` (sent by client 2.0.11 instead of a submit for an
  incremental run under `--full-refresh`) is driven and its response compared;
  each record counts as confirmed history for its workload.
  `ResolveDeferredRelations` entries are counted but not driven here;
  `deferral.rs` covers them.
- `ValidateClientVersion`, speculative submits and `GetExplainMessages` are
  compared too. A speculative submit is compared in full after removing
  opaque IDs, and a speculative SKIP's decision id is correlated for the
  Explain that follows, like a submit's (#103).

`CHARACTERIZATION_ONLY` holds interleaved multi-run captures whose decisions
depend on warehouse state or history outside the capture. Each entry has a
comment naming the dedicated test that pins it.

### `golden/replay_gaps.json`

Each entry names a `fixture` and `entry` index, the `local_variant` the server
returns, and a `reason`. Kinds:

- **Whole-response gap** (no `ignore_fields`): the local response differs, for
  example missing source history for a hosted clone or a deliberate
  safety-floor divergence. For a submit, the local variant must equal
  `local_variant`. A hosted rebuild that the server decides as another class
  (a decision divergence, spec #40) needs one; its hosted confirm then has no
  local counterpart.
- **Field-scoped gap** (`ignore_fields`): the named fields are stripped from
  both responses and the rest must match exactly. The named fields must still
  differ.
- **Empty-tree Explain gap** (`local_variant: "empty_tree"`): a hosted
  decision this server makes differently (spec #40). The local Explain tree
  for it must be empty, and must differ from hosted's.

The replay fails if any gap is unused, if a whole-response gap now matches
hosted ("gap is resolved; remove its exemption"), or if a field-scoped gap's
fields no longer differ. Divergences are explained in
[correctness-handoff.md](correctness-handoff.md), keyed by these entries.

Passing the replay is evidence for the captured cases only. It does not show
that every feature or response field is implemented.

### `golden/upgrade/`

Frozen rows captured from an old server revision. `clone_submit_fallback.rs`
loads them (`upgrade_rejects_frozen_366be09_*`) and asserts the current server
does not reuse them. Do not regenerate them with current hashing code.

## Hosted discovery (differential fuzz)

```bash
cargo run -p dbt-state-harness --features fuzz --bin diff-fuzz -- \
  --seeds golden/fixtures/<file>.jsonl --iterations 200
```

Needs `~/.dbt/dbt_cloud.yml` and uses dbt State metering. Its local side starts
with empty history and compares decision variants only, so it is discovery,
not a conformance gate. Failed comparisons and divergences fail the process.
Promote useful controlled cases into `golden/fixtures/`.
