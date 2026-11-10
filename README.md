# dbt-state-rs

An open-source, self-hostable reimplementation of the **dbt State** decision
service, the hosted, metered gRPC "query cache" at `api.state.dbt.com` that lets
dbt skip, copy or defer model builds when nothing meaningful has changed.

Written in Rust (tonic + prost) with a Postgres backend (sqlx). The protocol was
reverse-engineered from the open-source `.proto` definitions and from real
traffic captured against the hosted service using the official dbt Fusion client.

The dbt client sends each node's rendered SQL, config, hashes and upstream
freshness. The server derives a logic identity, matches it against recorded
State history, evaluates freshness and Lag tolerance, and decides BUILD / SKIP /
CLONE, recording outcomes so later runs can reuse them.

- **Status:** selected hosted behavior is reproduced and pinned by captured
  traffic replay; it is not yet a fully conformant drop-in replacement (see
  [open questions and known divergences](docs/correctness-handoff.md)).
- **Warehouse scope:** Snowflake is the characterized hosted warehouse, DuckDB
  has a separately tested local workflow, and other dialects use conservative
  same-target reuse.

## Quick start

Details and options are in [docs/development.md](docs/development.md).

```bash
# 1. Postgres
docker run -d --name dbt-state-pg -e POSTGRES_USER=dbtstate \
  -e POSTGRES_PASSWORD=dbtstate -e POSTGRES_DB=dbtstate -p 55441:5432 postgres:17-alpine

# 2. The server (applies migrations on start; listens on 127.0.0.1:50051)
DATABASE_URL=postgres://dbtstate:dbtstate@localhost:55441/dbtstate cargo run -p dbt-state-server

# 3. The dbt Fusion client, pointed at it without OAuth
DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL=127.0.0.1:50051 RUN_CACHE_API_SECURE=false \
  dbt build --profile <snowflake-profile> --target <target>
```

For credential-free local runs with a real Python dbt project, see the
[DuckDB workflow](docs/duckdb.md).

## Workspace layout

| Crate | Purpose |
|---|---|
| `crates/proto` | Generated tonic server and client stubs. |
| `crates/server` | The server: decision engine, Postgres store, gRPC services. |
| `crates/harness` | Recording proxy, golden fixtures, differential and fuzz tools, tests. |
| `crates/ui` | Optional read-only web console. |

## Documentation

Start with [`docs/README.md`](docs/README.md). [`AGENTS.md`](AGENTS.md) is a
compact orientation for coding agents.

## License

Apache-2.0. The `.proto` definitions are the Apache-2.0 contracts published by
dbt Labs; this server is an independent implementation of the decision logic.
