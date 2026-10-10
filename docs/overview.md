# Overview

## What dbt State is

dbt State (GA ~2026, also called "state-aware orchestration") makes dbt build on
*change* instead of on schedule. On every run, for each node, dbt consults a
decision service: has the node's **logic** (its SQL) or its **upstream data**
changed since a prior recorded build? If not, dbt reuses the previous result —
it **skips** the build (a "no-op"), **clones** an existing object from another
environment, or **defers** to it.

dbt Labs ships this as a hosted, metered gRPC service at `api.state.dbt.com`.
The dbt client (dbt Core's Python client and the Rust "Fusion"/dbt v2 engine) is
only a thin client; with no credentials it disables itself and dbt runs vanilla.
The decision engine — the interesting part — is server-side and closed-source.

`dbt-state-rs` is an independent, self-hostable reimplementation of that server.

## The client/server split (and where SQL is parsed)

The protocol is internally called the **"query cache"** (proto package
`com.fivetran.query_cache`). The split is deliberate and important:

- **The client** computes most things and sends them pre-digested:
  - `node_body_hash`, `node_hash`, `node_configs_hash`, `node_contract_hash`
    (fingerprints of the model) inside a `dbt_node_state` message;
  - a `table_namespace` grouping hash (an adapter-level id);
  - per-input-table freshness (`last_modified_epoch`);
  - the **raw compiled SQL** and a `semantic_extras` map of config;
  - for seeds, an md5 `values_hash` of the seed bytes.
- **The server** owns the decision: it derives the node's *logic identity* from
  the **rendered SQL** by default, with a separate template comparison mode,
  plus `semantic_extras` and dependency/context evidence, matches that against recorded
  history, evaluates upstream freshness, and returns BUILD / SKIP / CLONE. It
  then records confirmed outcomes so future runs skip.

The implementation uses a restricted `sqlparser` AST pass for Snowflake SQL
fingerprints. Existing experiments capture individual positive equivalences;
they do not prove a complete semantic catalog or the hosted implementation's
algorithm. Unsupported syntax is preserved exactly. See [protocol.md](protocol.md)
and [`experiments/SQL_NORMALIZATION.md`](../experiments/SQL_NORMALIZATION.md).
Clone DDL is generated from dialect templates, with unsupported operations
reported explicitly.

## Project goals

1. Faithfully reproduce the hosted service's decisions (skip/execute/clone/defer).
2. Keep state in a backend you control (Postgres here).
3. Be validatable against the real service, not just against our own assumptions.

## Correctness status

The goal is full hosted conformance, but the implementation has known gaps.
Offline regressions, captured traffic replay and pure properties guard selected
contracts; the hosted discovery tool has unequal history and is not a conformance
oracle. See [testing.md](testing.md).

Recent offline hardening prevents several unsafe skips and preserves cached test
results. Some unsupported reuse paths conservatively rebuild; selectors and
state-backed deferral return UNIMPLEMENTED. Automatic SQL cloning, volatile
transformations, provenance, dependency resolution, lag semantics and complete
client compatibility still need work. Read [correctness-handoff.md](correctness-handoff.md)
for completed fixes, upgrade effects, evidence and concrete next tasks.
