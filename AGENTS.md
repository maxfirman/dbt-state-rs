# AGENTS.md

Compact orientation for coding agents working in this repo. Read the linked docs
in [`docs/`](docs/) before non-trivial changes.

## What this is
An open-source reimplementation of the **dbt State** ("query cache") gRPC
decision service (`api.state.dbt.com`) in Rust + Postgres. The server decides
BUILD / SKIP / CLONE / DEFER for each dbt node and records outcomes.
→ [docs/overview.md](docs/overview.md)

## Layout
- `crates/proto` — generated tonic stubs (server+client) from `proto/`.
- `crates/server` — the server: `decision.rs` (pure engine), `store.rs` (sqlx),
  `clone.rs` (DDL gen), `services.rs` (gRPC), `lib.rs` (`build_router`), `main.rs`.
- `crates/harness` — recording proxy, golden fixtures, differential + fuzz tools, tests.
→ [docs/architecture.md](docs/architecture.md)

## Build / test
```bash
cargo build
cargo test                              # needs Postgres at $DATABASE_URL
cargo test -p dbt-state-server --lib    # pure engine tests, no DB
```
- `DATABASE_URL` default `postgres://dbtstate:dbtstate@localhost:55441/dbtstate`.
- `protoc` is required and must be on `PATH` (or set `PROTOC`). `tonic-prost-build`
  invokes it at build time.
→ [docs/development.md](docs/development.md)

## Critical gotchas
- **Preserve on-wire proto service/method names.** The service literally named
  `Clone` collides with `std::clone::Clone`; `crates/proto/build.rs` patches the
  generated std-derive impl — do not rename the service to "fix" it.
- **Decision engine is pure** (`decision.rs::decide`). Keep it I/O-free so the
  property tests (`decision_proptest.rs`) and conformance replay stay valid.
- **Reuse requires current evidence.** Select latest physical history before
  checking versioned SQL/dependency/context/config fingerprints. Compare exact
  physical upstream names and preserve unknown epochs. Own-target existence and
  external edits are checked separately from upstream freshness. Namespace-only
  cross-target reuse is disabled pending provenance. Data tests require scoped
  UID identity and complete cached results. → [docs/correctness-handoff.md](docs/correctness-handoff.md)
- **Org isolation.** Every store query is scoped by `org_id` (from the
  `x-organization-id` metadata header, default `local`). Never weaken this.
- **Response shapes must match the hosted service exactly** (decision ints,
  rejection reasons, `decision_description` strings, both `ready_to_clone` and
  the deprecated `ready_to_clone_v1` oneof). → [docs/protocol.md](docs/protocol.md)
- **DuckDB does not trigger dbt State** — use a supported warehouse (Snowflake)
  for live validation.

## Validating faithfulness (do this when touching decision logic)
1. `cargo test` — includes the conformance replay of real captured traffic
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
- TDD: use primary client/protocol evidence and offline regressions for obvious
  correctness fixes. No Snowflake access is available: do not make live calls or
  infer unresolved hosted policy. Document remaining questions in the handoff.
- Keep golden session files out of the tree; only `golden/fixtures/` is tracked
  (see `golden/README.md`).
- Don't commit unless asked; prefer small, focused commits.
