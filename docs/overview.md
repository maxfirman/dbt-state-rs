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

## The client/server split (why the server needs no SQL engine)

The protocol is internally called the **"query cache"** (proto package
`com.fivetran.query_cache`). The split is deliberate and important:

- **The client** computes everything SQL-related and sends it pre-digested:
  - `node_body_hash`, `node_hash`, `node_configs_hash`, `node_contract_hash`
    (semantic fingerprints of the model) inside a `dbt_node_state` message;
  - a `table_namespace` grouping hash;
  - per-input-table freshness (`last_modified_epoch`);
  - the raw compiled SQL (for the hosted service's own fingerprinting; our
    server does not need to parse it);
  - for seeds, an md5 `values_hash` of the seed bytes.
- **The server** owns the decision: match the incoming fingerprint against
  recorded history, evaluate upstream freshness, and return
  BUILD / SKIP / CLONE. It then records confirmed outcomes so future runs skip.

Because the client sends precomputed hashes, **the server does not need a SQL
engine** for the core decision. The only place SQL is *generated* is the CLONE
DDL, which is a simple dialect-specific `CREATE ... CLONE ...` template, not a
query transformation. See [protocol.md](protocol.md) and
[architecture.md](architecture.md).

## Project goals

1. Faithfully reproduce the hosted service's decisions (skip/execute/clone/defer).
2. Keep state in a backend you control (Postgres here).
3. Be validatable against the real service, not just against our own assumptions.

## How faithfulness is ensured

Three independent checks (see [testing.md](testing.md)):

- **Conformance replay** of real captured traffic (regression gate).
- **Property-based invariants** over the pure decision function.
- **Differential fuzzing** that sends mutated requests to both the real service
  and ours and diffs the decisions (opt-in discovery tool).

## Scope and known limits

Implemented and validated: `SubmitEnrichedSQL` (skip/execute), `SubmitValues`
(seeds), `ConfirmExecution`, `RecordExecutions`, `RegisterClone` (with clone-DDL
generation), cross-environment reuse by `table_namespace`, `ClientValidation`,
`Health`, and conservative defaults for `Explain`, `SelectorService`,
`ResolveDeferredRelations`, and speculative submits.

Not yet reproduced (valid empty/default responses, pending more captured
traffic): rich `Explain` text, `transformed_nodes_by_query` population,
`query_hash_metadata_info`, and non-Snowflake clone DDL verified against the real
service. See [protocol.md](protocol.md#coverage) for the current matrix.
