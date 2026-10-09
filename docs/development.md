# Development

## Toolchain

- **Rust** 1.98+ (edition 2021).
- **protoc** (protobuf compiler) — required by `tonic-prost-build`. Install it
  and ensure it is on `PATH` (e.g. `brew install protobuf`, `apt-get install
  protobuf-compiler`, or `arduino/setup-protoc` in CI). `tonic-prost-build` finds
  it via `PATH` or the `PROTOC` env var.
- **Postgres** 15+ reachable at `DATABASE_URL`.

## Build & test

```bash
cargo build                 # whole workspace
cargo test                  # unit + integration (needs Postgres running)
cargo test -p dbt-state-server --lib   # pure decision-engine tests (no DB)
```

Integration tests create a unique, isolated Postgres schema per test and run the
migrations into it, so they are independent and repeatable.

## Postgres

Any reachable instance works. A disposable container:

```bash
docker run -d --name dbt-state-pg \
  -e POSTGRES_USER=dbtstate -e POSTGRES_PASSWORD=dbtstate -e POSTGRES_DB=dbtstate \
  -p 55441:5432 postgres:17-alpine
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
```

The server applies migrations (`crates/server/migrations/`) automatically on
startup.

## Running the server

```bash
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
export DBT_STATE_LISTEN=127.0.0.1:50051        # optional, this is the default
RUST_LOG=info cargo run -p dbt-state-server
```

## Pointing the dbt client at the server

The dbt Fusion client (dbt v2) selects an insecure channel — and therefore sends
**no OAuth** — when `RUN_CACHE_API_SECURE=false`. That is how you talk to a local
server without auth:

```bash
export DBT_ENGINE_MANAGE_STATE=true        # enable dbt State
export RUN_CACHE_API_URL=127.0.0.1:50051   # our server
export RUN_CACHE_API_SECURE=false          # insecure => no OAuth
dbt build --profile <profile> --target <target> --skip-semantic-manifest-validation
```

### Warehouse note

dbt State only engages for its supported warehouses (Snowflake, BigQuery,
Databricks, Redshift). **DuckDB does not trigger dbt State at all** — verified by
running the Fusion client against a DuckDB target and observing zero State
activity. Use a supported warehouse (this project was developed and validated
against Snowflake) to exercise the protocol end-to-end.

## Capturing real traffic / validating against the hosted service

See [harness.md](harness.md) (recording proxy) and [testing.md](testing.md)
(conformance replay, property tests, differential fuzzing). Those flows need a
dbt Cloud credential in `~/.dbt/dbt_cloud.yml` and will consume dbt State trial
metering.

## Key client/auth facts

- Enable: `DBT_ENGINE_MANAGE_STATE=true`. Config via `RUN_CACHE_*` env vars.
- Hosted auth: token-exchange of the dbt Cloud credential at
  `https://auth.state.dbt.com/token` (client_id `2fd87cd5-...`) → `id_token`
  sent as `Bearer` + `x-organization-id` (org id from the token scope).
- Per-request metadata the client sends: `x-request-id`, `x-session-id`,
  `x-submitted-at-epoch`, `x-system-user-id`, `x-os-name`, `x-dbt-invocation-id`.

See [protocol.md](protocol.md) for the full protocol and [architecture.md](architecture.md)
for the server internals.
