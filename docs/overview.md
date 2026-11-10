# Overview

## What dbt State is

dbt State (also called "state-aware orchestration") makes dbt build on *change*
instead of on schedule. On every run, for each node, dbt consults a decision
service: has the node's **logic** (its SQL) or its **upstream data** changed
since a prior recorded build? If not, dbt reuses the previous result: it
**skips** the build, **copies** a result built under another Target (the wire
decision is CLONE), or **defers** to it.

dbt Labs ships this as a hosted, metered gRPC service at `api.state.dbt.com`.
The dbt client (dbt Core's Python client and the Rust "Fusion"/dbt v2 engine) is
only a thin client; with no credentials it disables itself and dbt runs vanilla.
The decision engine is server-side and closed-source.

`dbt-state-rs` is an independent, self-hostable reimplementation of that server.

## The client/server split

The protocol is internally called the **"query cache"** (proto package
`com.fivetran.query_cache`).

- **The client** computes most inputs and sends them pre-digested:
  - `node_body_hash`, `node_hash`, `node_configs_hash`, `node_contract_hash`
    (fingerprints of the model) inside a `dbt_node_state` message;
  - a `table_namespace` grouping hash (an adapter-level warehouse id);
  - per-input-table freshness (`last_modified_epoch`);
  - the **raw compiled SQL** and a `semantic_extras` map of config;
  - for seeds, an md5 `values_hash` of the seed bytes.
- **The server** owns the decision. It derives the node's logic identity from
  the rendered SQL (or the template, under `compare_unrendered_code`),
  `semantic_extras` and dependency/context evidence, and matches it against
  State history. It then checks upstream freshness against what the build
  recorded, applies the stale-upstream policy and Lag tolerance, and returns
  BUILD / SKIP / CLONE. It records confirmed outcomes so later runs can reuse
  them, answers `state:*` selector and deferral queries, and re-derives
  `dbt state explain` trees on demand.

Snowflake SQL fingerprints use a restricted `sqlparser` AST pass; unsupported
syntax is kept exactly ([sql-normalization.md](sql-normalization.md)).
Snowflake cross-target Result copy uses the captured schema-abstracted matching
contract. DuckDB has its own validated formatting subset and checked copy
subset ([duckdb.md](duckdb.md)). Other dialects keep exact rendered SQL for
same-target reuse and do no automatic cross-target copy. BigQuery and
Databricks copy DDL follows published syntax but has not been run against
those warehouses; Redshift copies are unsupported.

## Project goals

1. Faithfully reproduce the hosted service's decisions (skip/execute/clone/defer).
2. Keep state in a backend you control (Postgres here).
3. Be validatable against the real service, not just against our own assumptions.

## Correctness status

Selected hosted behavior is pinned by captured traffic replay, offline
regressions and property tests ([testing.md](testing.md)). Open hosted
questions and known divergences are tracked in
[correctness-handoff.md](correctness-handoff.md).
