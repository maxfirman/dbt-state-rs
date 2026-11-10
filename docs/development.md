# Development

## Toolchain

- **Rust** 1.98 (pinned by `rust-toolchain.toml`; server/proto/harness use
  edition 2021, the UI edition 2024).
- **protoc** (protobuf compiler), required by `tonic-prost-build`. Install it
  and put it on `PATH` (e.g. `brew install protobuf`, `apt-get install
  protobuf-compiler`), or set `PROTOC`.
- **Postgres** reachable at `DATABASE_URL`. CI uses `postgres:17-alpine`.
- **uv** and **Python 3.12** for the DuckDB acceptance cases
  (`scripts/duckdb-ci.sh`).

## Build, lint and test

```bash
cargo build --workspace --all-targets
cargo fmt --all --check
cargo clippy --workspace --all-targets --features fuzz -- -D warnings   # same as CI
cargo test --workspace                     # unit + integration (needs Postgres)
cargo test -p dbt-state-server --lib       # server library tests (no DB)
./scripts/duckdb-ci.sh                     # every fixed and generated DuckDB case
```

Integration tests create a unique, isolated Postgres schema per test and run the
migrations into it, so they are independent and repeatable. CI
(`.github/workflows/ci.yml`) runs the same commands; `duckdb-ci.sh` runs there
with `--jobs 4`. Validation strategy is in [testing.md](testing.md).

## Pre-commit hook

A tracked hook, [`.githooks/pre-commit`](../.githooks/pre-commit), runs
`cargo fmt --all --check` when at least one staged file ends in `.rs`, and
exits immediately otherwise. Enable it once per clone:

```bash
git config core.hooksPath .githooks
```

It checks formatting only; clippy and tests stay in CI. Bypass once with
`git commit --no-verify`.

## Postgres

Any reachable instance works. A disposable container:

```bash
docker run -d --name dbt-state-pg \
  -e POSTGRES_USER=dbtstate -e POSTGRES_PASSWORD=dbtstate -e POSTGRES_DB=dbtstate \
  -p 55441:5432 postgres:17-alpine
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
```

The server applies the migrations (`crates/server/migrations/`) on startup.

## Running the server

```bash
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
export DBT_STATE_LISTEN=127.0.0.1:50051        # optional, this is the default
export DBT_STATE_REQUIRE_ORG_HEADER=false      # optional; true rejects requests
                                               # without x-organization-id
RUST_LOG=info cargo run -p dbt-state-server
```

## Running the UI

```bash
HOST=127.0.0.1 PORT=4000 DBT_STATE_UI_ORG_ID=local cargo run -p dbt-state-ui
```

It reads the same `DATABASE_URL`. `HOST`/`PORT` default to `127.0.0.1:3000`;
`DBT_STATE_UI_ORG_ID` defaults to `local`. See [ui.md](ui.md).

## Pointing the dbt client at the server

The dbt Fusion client (dbt v2) uses an insecure channel, and therefore sends
**no OAuth**, when `RUN_CACHE_API_SECURE=false`:

```bash
export DBT_ENGINE_MANAGE_STATE=true        # enable dbt State
export RUN_CACHE_API_URL=127.0.0.1:50051   # our server
export RUN_CACHE_API_SECURE=false          # insecure => no OAuth
dbt build --profile <profile> --target <target> --skip-semantic-manifest-validation
```

Client auth, metadata headers and configuration are described in
[protocol.md](protocol.md).

### Warehouse note

Hosted dbt State does not engage for DuckDB, so Fusion validation against the
hosted service needs Snowflake; local DuckDB runs use the
[DuckDB workflow](duckdb.md).

## Capturing real traffic

See [harness.md](harness.md) (recording proxy) and [testing.md](testing.md).
Those flows need a dbt Cloud credential in `~/.dbt/dbt_cloud.yml` and consume
dbt State metering.

Server internals are in [architecture.md](architecture.md).
