# dbt-state-rs

An open-source, self-hostable reimplementation of the **dbt State** decision
service — the hosted, metered gRPC "query cache" at `api.state.dbt.com` that
lets dbt skip, clone, or defer model builds when nothing meaningful has changed.

Written in Rust (tonic + prost) with a Postgres backend (sqlx). The protocol was
reverse-engineered from the open-source `.proto` definitions and from real
traffic captured against the hosted service using the official dbt Fusion client.

- **Core idea:** the dbt client sends each node's precomputed hashes and upstream
  freshness; the server decides BUILD / SKIP / CLONE and records outcomes so
  future runs can skip. The decision engine is the novel part this project
  reproduces.
- **Validated** against the real service three ways: differential replay of
  captured traffic, property-based invariants, and live end-to-end runs with the
  official dbt Fusion client on Snowflake.

## Quick start

```bash
# 1. Postgres (any reachable instance; a container is fine)
docker run -d --name dbt-state-pg -e POSTGRES_USER=dbtstate \
  -e POSTGRES_PASSWORD=dbtstate -e POSTGRES_DB=dbtstate -p 55441:5432 postgres:17-alpine

# 2. Build + run the server (applies migrations on start)
export DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate
cargo run -p dbt-state-server            # listens on 127.0.0.1:50051

# 3. Point the dbt Fusion client at it (insecure => no OAuth)
export DBT_ENGINE_MANAGE_STATE=true
export RUN_CACHE_API_URL=127.0.0.1:50051
export RUN_CACHE_API_SECURE=false
dbt build --profile <snowflake-profile> --target <target>
```

## Workspace layout

| Crate | Purpose |
|---|---|
| `crates/proto` | Generated tonic server+client stubs from the `.proto` files. |
| `crates/server` | The dbt State server: decision engine + Postgres store + gRPC services. |
| `crates/harness` | Test harness: recording proxy, golden fixtures, differential + fuzz tools. |

## Documentation

Full documentation lives in [`docs/`](docs/). Start with
[`docs/README.md`](docs/README.md). See [`AGENTS.md`](AGENTS.md) for a compact
orientation aimed at coding agents.

## License

Apache-2.0. The `.proto` definitions are the Apache-2.0 contracts published by
dbt Labs; this server is an independent implementation of the decision logic.
