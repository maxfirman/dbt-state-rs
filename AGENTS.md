# AGENTS.md

Compact orientation for coding agents working in this repo. Read the linked docs
in [`docs/`](docs/) before non-trivial changes.

## What this is
An open-source reimplementation of the **dbt State** ("query cache") gRPC
decision service (`api.state.dbt.com`) in Rust + Postgres. The server decides
BUILD / SKIP / CLONE / DEFER for each dbt node and records outcomes.
→ [docs/overview.md](docs/overview.md)

## Layout
- `crates/proto`: generated tonic stubs (server+client) from `proto/`.
- `crates/server`: the server. Modules to know:
  - `decision.rs`: the pure decision engine (`decide`), freshness, descriptions.
  - `reuse_candidate.rs`: pure Reuse candidate selection and Reuse qualification.
  - `submit_decision.rs`: how one submit is decided, end to end.
  - `result_copy.rs`: pure Result copy planning (submit fallback and RegisterClone).
  - `explain.rs`: pure re-derivation of `dbt state explain` trees.
  - `warehouse.rs` + `warehouse/`: dialect policy and the Warehouse rules.
  - `store.rs` + `store/history.rs`: Postgres writes and scoped State history reads.
  - `services.rs` (gRPC handlers), `lib.rs` (`AppState`, `build_router`), `main.rs`.
- `crates/harness`: recording proxy, `state-probe`, golden fixtures, differential
  and fuzz tools, integration tests.
- `crates/ui`: optional read-only web console.
→ full module table in [docs/architecture.md](docs/architecture.md)

## Build / test
```bash
cargo build
cargo test                              # needs Postgres at $DATABASE_URL
cargo test -p dbt-state-server --lib    # server library tests, no DB
cargo clippy --workspace --all-targets --features fuzz -- -D warnings   # CI lint
./scripts/duckdb-ci.sh                  # DuckDB acceptance cases (needs uv, Python 3.12, Postgres)
```
- `DATABASE_URL` default `postgres://dbtstate:dbtstate@localhost:55441/dbtstate`.
- `protoc` is required and must be on `PATH` (or set `PROTOC`). `tonic-prost-build`
  invokes it at build time.
→ [docs/development.md](docs/development.md)

## Critical gotchas
- **Preserve on-wire proto service/method names.** The service literally named
  `Clone` collides with `std::clone::Clone`; `crates/proto/build.rs` patches the
  generated std-derive impl. Do not rename the service to "fix" it.
- **Decision engine is pure** (`decision.rs::decide`). Keep it I/O-free so the
  property tests (`decision_proptest.rs`) and conformance replay stay valid.
- **Reuse requires current evidence.** Invariants
  (→ [docs/correctness-handoff.md](docs/correctness-handoff.md), ADR 0007):
  1. Physical State history is scoped by organization, warehouse family,
     connection namespace (`table_namespace`) and Target relation. Only the
     latest row in that scope can be reused, and its versioned
     SQL/dependency/context/config fingerprints must match. When the latest
     row has the submit's identity under another execution type, the rebuild
     is judged against the latest row of the submit's type, never reused
     (ADR 0007 "Execution type change").
  2. Upstream drift is measured against the epoch recorded for each upstream at
     the build, by exact physical name; unknown epochs never prove freshness. A
     changed Upstream producer always rebuilds. Lag tolerance only turns a
     policy rebuild into a reuse.
  3. The Target relation's existence and External modifications are checked
     separately from upstream freshness.
  4. Cross-target Result copy: Snowflake matches by `table_namespace` +
     schema-abstracted logic + execution type, copies only FULL on submit
     (`Warehouse::submit_copy_admits`, #101), and that logic identity binds
     each dependency's schema-abstracted name to its definition (swapping two
     dependency definitions between references is not equivalent). DuckDB has
     its own checked copy subset ([docs/duckdb.md](docs/duckdb.md)). Other
     dialects keep exact SQL and do no automatic cross-target reuse. A namespace
     alone never justifies SKIP.
  5. Data tests require scoped UID identity and complete cached results. Under
     `compare_unrendered_code`, models and data tests both use the dual
     rendered/template identity.
- **Org isolation.** Every store query is scoped by `org_id` (from the
  `x-organization-id` metadata header, default `local`). Never weaken this.
- **Replay clock is test-only.** `Clock::Replay` lets a request's
  `x-dbt-state-replay-now-ms` header set the server clock
  (`crates/server/src/lib.rs`). Never enable it in a deployment: a client could
  choose its own build age.
- **Response shapes must match the hosted service exactly** (decision ints,
  rejection reasons, `decision_description` strings, both `ready_to_clone` and
  the deprecated `ready_to_clone_v1` oneof). → [docs/protocol.md](docs/protocol.md)
- **Hosted dbt State does not engage for DuckDB**, so live hosted validation
  needs Snowflake. Local DuckDB runs go through `dbt-state-duckdb` against this
  server ([docs/duckdb.md](docs/duckdb.md)).

## Validating faithfulness (do this when touching decision logic)
1. `cargo test`: includes the conformance replay of real captured traffic
   (`crates/harness/tests/conformance.rs`) and property invariants.
2. Opt-in differential fuzz vs the REAL service (burns dbt State metering, needs
   `~/.dbt/dbt_cloud.yml`):
   ```bash
   cargo run -p dbt-state-harness --features fuzz --bin diff-fuzz -- \
     --seeds golden/fixtures/<file>.jsonl --iterations 200
   ```
   This is empty-history discovery, not a conformance gate. New real-skip cases can be promoted into
   `golden/fixtures/` to grow the conformance corpus.
→ [docs/testing.md](docs/testing.md), [docs/harness.md](docs/harness.md)

## Conventions
- TDD: characterize behavior against the REAL service first when possible
  (recording proxy + `experiments/` drivers, see `docs/harness.md`), promote a
  golden fixture, add the assertion, then implement. Live captures require a dbt
  Cloud credential in `~/.dbt/dbt_cloud.yml` and a Snowflake target; **only ever
  mutate the demo database (`CDO_DEMO_DB`)**. When live access is unavailable,
  use primary client/protocol evidence and offline regressions; do not infer
  unresolved hosted policy. Record open questions in
  [docs/correctness-handoff.md](docs/correctness-handoff.md).
- Keep golden session files out of the tree; only `golden/fixtures/` is tracked
  (see `golden/README.md`).
- Don't commit unless asked; prefer small, focused commits.

## Agent guidance

- Before GitHub issue operations, read [issue-tracker.md](docs/agents/issue-tracker.md).
- Before triaging issues, read [triage-labels.md](docs/agents/triage-labels.md).
- Before domain exploration or design, read [domain.md](docs/agents/domain.md).
